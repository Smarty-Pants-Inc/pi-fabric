import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { setWindowsVerifiedRemoval, verifiedRemove, verifiedRemoveTestHooks, windowsPrivate, windowsRemoveHelper } from "../src/storage/verified-remove.js";

// smarty-dev#7858: Windows verified removal through native/fabric-win-remove.c (built and digest-pinned by the build;
// the Windows CI job builds it with the pinned Windows zig). Owner SID, volume serial and file index are checked on
// the HANDLE, reparse points are opened as themselves, and the delete is by handle.
const dirs: string[] = [];
// The helper is on for the operator's retention sweep only (the CLI switches it on); these tests switch it on too.
beforeEach(() => setWindowsVerifiedRemoval(true));
afterEach(() => {
  setWindowsVerifiedRemoval(false);
  delete verifiedRemoveTestHooks.beforeHelper;
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const tempDir = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-win-remove-")); dirs.push(dir); return dir; };
const ids = (file: string) => { const stat = fs.lstatSync(file, { bigint: true }); return [stat.dev.toString(16), stat.ino.toString(16)]; };
/** One helper entry: KIND VOLUME INDEX MTIME (FILETIME) SIZE PATH, all from one bigint lstat. */
const entryArgs = (file: string, kind?: "f" | "d") => { const stat = fs.lstatSync(file, { bigint: true });
  return [kind ?? (stat.isDirectory() ? "d" : "f"), stat.dev.toString(16), stat.ino.toString(16),
    (stat.mtimeNs / 100n + 116_444_736_000_000_000n).toString(16), stat.size.toString(16), path.toNamespacedPath(file)]; };

// Foreign-owner fixture (smarty-dev#7858 / #4664): bounded Windows PowerShell 5.1 children, inputs by environment only.
// PSModulePath is dropped (Windows env keys are case-insensitive): node spawned from pwsh inherits pwsh 7's module
// path, and 5.1 then fails to load Microsoft.PowerShell.Security (Get-Acl). 5.1 rebuilds its default path itself.
const withoutModulePath = (env: Record<string, string | undefined>) =>
  Object.fromEntries(Object.entries(env).filter(([key]) => key.toLowerCase() !== "psmodulepath"));
const powershell = (script: string, env: Record<string, string> = {}) => spawnSync("powershell.exe",
  // -EncodedCommand (UTF-16LE base64): no command-line quoting of the script at all.
  ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
  { encoding: "utf8", timeout: 90_000, windowsHide: true, env: { ...withoutModulePath(process.env), ...withoutModulePath(env) } });
const brief = (result: { status: number | null; error?: Error | undefined; stdout: unknown; stderr: unknown }): string =>
  `exit ${String(result.status)}${result.error ? ` ${result.error.message}` : ""}: ${`${String(result.stdout)} ${String(result.stderr)}`.replace(/\s+/g, " ").trim().slice(0, 300)}`;
/** This process's token SIDs, as the helper reads them: TokenUser and TokenOwner (WindowsIdentity.User / .Owner). */
const tokenSids = (): { user: string; owner: string } => {
  const result = powershell("$i = [Security.Principal.WindowsIdentity]::GetCurrent(); \"$($i.User.Value) $($i.Owner.Value)\"");
  const match = /^(S-1-[\d-]+) (S-1-[\d-]+)$/.exec(String(result.stdout).trim());
  if (result.status !== 0 || !match) throw new Error(`token SID query failed (${brief(result)})`);
  return { user: match[1]!, owner: match[2]! };
};
/** The owner SID Get-Acl reads now (sid undefined when it cannot be read), with that child's brief() as trace. */
const aclOwner = (file: string): { sid: string | undefined; trace: string } => {
  const result = powershell("(Get-Acl -LiteralPath $env:FABRIC_TEST_FILE).GetOwner([Security.Principal.SecurityIdentifier]).Value", { FABRIC_TEST_FILE: file });
  const sid = String(result.stdout).trim();
  return { sid: result.status === 0 && /^S-1-[\d-]+$/.test(sid) ? sid : undefined, trace: `Get-Acl ${brief(result)}` };
};
/** Sets only the owner section, with SeRestorePrivilege enabled in THIS child's own token (process-local, gone at its
 * exit; it is only enabled if already held: nothing global changes). Exit 5 when the privilege is not held. */
const PRIVILEGED_SET_OWNER = `$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @'
using System; using System.Runtime.InteropServices;
public static class FabricTestRestorePrivilege {
  [StructLayout(LayoutKind.Sequential, Pack = 4)] struct Luid { public uint Low; public int High; }
  [StructLayout(LayoutKind.Sequential, Pack = 4)] struct Privileges { public int Count; public Luid Luid; public int Attributes; }
  [DllImport("advapi32.dll", SetLastError = true)] static extern bool OpenProcessToken(IntPtr process, uint access, out IntPtr token);
  [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode)] static extern bool LookupPrivilegeValue(string system, string name, out Luid luid);
  [DllImport("advapi32.dll", SetLastError = true)] static extern bool AdjustTokenPrivileges(IntPtr token, bool disableAll, ref Privileges state, int length, IntPtr previous, IntPtr returned);
  [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  public static int Enable() {
    IntPtr token;
    if (!OpenProcessToken(GetCurrentProcess(), 0x28, out token)) return Marshal.GetLastWin32Error();
    try {
      Privileges state = new Privileges(); state.Count = 1; state.Attributes = 2;
      if (!LookupPrivilegeValue(null, "SeRestorePrivilege", out state.Luid)) return Marshal.GetLastWin32Error();
      if (!AdjustTokenPrivileges(token, false, ref state, 0, IntPtr.Zero, IntPtr.Zero)) return Marshal.GetLastWin32Error();
      return Marshal.GetLastWin32Error();
    } finally { CloseHandle(token); }
  }
}
'@
$enabled = [FabricTestRestorePrivilege]::Enable()
if ($enabled -ne 0) { "SeRestorePrivilege not enabled (Win32 error $enabled)"; exit 5 }
$security = New-Object System.Security.AccessControl.FileSecurity
$security.SetOwner((New-Object System.Security.Principal.SecurityIdentifier $env:FABRIC_TEST_SID))
[System.IO.File]::SetAccessControl($env:FABRIC_TEST_FILE, $security)
'owner set'`;
/** Make `sid` the owner: icacls first, then the privileged child; each attempt confirmed by Get-Acl. */
const setOwner = (file: string, sid: string): { changed: boolean; trace: string[] } => {
  const trace: string[] = [];
  const icacls = spawnSync("icacls", [file, "/setowner", `*${sid}`], { encoding: "utf8", timeout: 30_000, windowsHide: true });
  let now = aclOwner(file);
  trace.push(`icacls /setowner *${sid}: ${brief(icacls)}; Get-Acl owner ${String(now.sid)} (${now.trace})`);
  if (now.sid === sid) return { changed: true, trace };
  const privileged = powershell(PRIVILEGED_SET_OWNER, { FABRIC_TEST_FILE: file, FABRIC_TEST_SID: sid });
  now = aclOwner(file);
  trace.push(`SetAccessControl (SeRestorePrivilege) ${sid}: ${brief(privileged)}; Get-Acl owner ${String(now.sid)} (${now.trace})`);
  return { changed: now.sid === sid, trace };
};

describe.runIf(process.env.GITHUB_ACTIONS === "true" && process.platform === "win32")("Windows verified removal (smarty-dev#7858)", () => {
  it("has the digest-verified helper in this build", () => {
    expect(windowsRemoveHelper()).toMatch(/fabric-win-remove\.exe$/);
  });

  it("removes our own file and a directory tree by handle", () => {
    const dir = tempDir();
    const file = path.join(dir, "old.bak");
    fs.writeFileSync(file, "x".repeat(10));
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test" })).toEqual({ removed: true, bytes: 10 });
    expect(fs.existsSync(file)).toBe(false);
    const tree = path.join(dir, "run");
    fs.mkdirSync(path.join(tree, "nested"), { recursive: true });
    fs.writeFileSync(path.join(tree, "nested", "events.jsonl"), "{}\n");
    expect(verifiedRemove(tree, fs.lstatSync(tree), { deleter: "test" })).toMatchObject({ removed: true });
    expect(fs.existsSync(tree)).toBe(false);
  });

  // pi-fabric#913 Windows CI: a caller's final check runs with the target off its name, as on Linux, so a compaction
  // can publish its new tail there; a kept target goes back to its name.
  it("runs the final check with the target moved off its name, and moves a kept target back", () => {
    const dir = tempDir();
    const file = path.join(dir, "events.jsonl");
    fs.writeFileSync(file, "old events\n");
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test", freed: 4,
      proceed: () => { fs.writeFileSync(file, "tail\n", { flag: "wx" }); return true; } })).toEqual({ removed: true, bytes: 4 });
    expect(fs.readFileSync(file, "utf8")).toBe("tail\n");
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test", proceed: () => false }))
      .toMatchObject({ removed: false, reason: "kept by the final check" });
    expect(fs.readFileSync(file, "utf8")).toBe("tail\n");
    expect(fs.readdirSync(dir)).toEqual(["events.jsonl"]);
  });

  // pi-fabric#913 round 4 (Astra P1): a file refreshed between the scan and the helper's open keeps its id; its
  // last-write time or size no longer matches the judged ones, so the helper keeps it.
  it("keeps a file refreshed after the scan with the same id (new time, or new size)", () => {
    const dir = tempDir();
    const file = path.join(dir, "events.jsonl");
    fs.writeFileSync(file, "judged\n");
    const id = ids(file);
    // Same size, later time.
    verifiedRemoveTestHooks.beforeHelper = () => {
      fs.writeFileSync(file, "fresh!\n");
      const later = new Date(Date.now() + 10_000);
      fs.utimesSync(file, later, later);
    };
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test" }))
      .toEqual({ removed: false, reason: `kept by the final check (${file} modified since judgment)` });
    expect(fs.readFileSync(file, "utf8")).toBe("fresh!\n");
    expect(ids(file)).toEqual(id);
    // Larger, through the final-check path (moved off its name first): kept and moved back.
    verifiedRemoveTestHooks.beforeHelper = () => { fs.appendFileSync(file, "more\n"); };
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "test", proceed: () => true }))
      .toMatchObject({ removed: false, reason: expect.stringContaining("modified since judgment") });
    expect(fs.readFileSync(file, "utf8")).toBe("fresh!\nmore\n");
    expect(fs.readdirSync(dir)).toEqual(["events.jsonl"]);
  });

  // While the helper holds a judged file (validated, before the deletes), nobody can write it.
  it("denies a write to a judged file between validation and the delete", async () => {
    const dir = tempDir();
    const file = path.join(dir, "events.jsonl");
    fs.writeFileSync(file, "judged\n");
    const pause = tempDir();
    const child = spawn(windowsRemoveHelper()!, entryArgs(file), { env: { ...process.env, FABRIC_WIN_REMOVE_TEST_PAUSE: pause }, windowsHide: true });
    let stdout = "";
    child.stdout!.on("data", (data: Buffer) => { stdout += String(data); });
    const exited = new Promise<number | null>(resolve => child.once("exit", resolve));
    const started = Date.now();
    while (!fs.existsSync(path.join(pause, "paused"))) {
      if (Date.now() - started > 20_000) throw new Error(`the helper never paused: ${stdout}`);
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    expect(() => fs.writeFileSync(file, "refresh\n")).toThrow();
    fs.writeFileSync(path.join(pause, "go"), "");
    expect(await exited, stdout).toBe(0);
    expect(fs.existsSync(file)).toBe(false);
  }, 60_000);

  it("keeps a file whose id changed after it was judged", () => {
    const dir = tempDir();
    const file = path.join(dir, "old.bak");
    fs.writeFileSync(file, "judged");
    const judged = fs.lstatSync(file);
    const judgedArgs = entryArgs(file);
    fs.unlinkSync(file);
    fs.writeFileSync(file, "replacement");
    expect(verifiedRemove(file, judged, { deleter: "test" })).toMatchObject({ removed: false, reason: expect.stringContaining("replaced since judgment") });
    // The helper itself, given the judged id for the replaced name, refuses on the handle.
    const helper = spawnSync(windowsRemoveHelper()!, judgedArgs, { encoding: "utf8", windowsHide: true });
    expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 file id changed since judgment" });
    expect(fs.readFileSync(file, "utf8")).toBe("replacement");
  });

  // pi-fabric#913 (Astra P1): an ancestor in the judged tree swapped for a junction AFTER the walk and before the
  // helper opens the entries: the helper opens the junction itself and refuses; nothing is deleted anywhere.
  it("refuses an ancestor swapped for a junction after the walk, and deletes nothing", () => {
    const dir = tempDir();
    const tree = path.join(dir, "run");
    const nested = path.join(tree, "nested");
    fs.mkdirSync(nested, { recursive: true });
    fs.writeFileSync(path.join(nested, "events.jsonl"), "{}\n");
    verifiedRemoveTestHooks.beforeHelper = () => {
      fs.renameSync(nested, `${nested}.real`);
      fs.symlinkSync(`${nested}.real`, nested, "junction");
    };
    expect(verifiedRemove(tree, fs.lstatSync(tree), { deleter: "test" })).toMatchObject({ removed: false, reason: expect.stringContaining("reparse point") });
    expect(fs.readFileSync(path.join(`${nested}.real`, "events.jsonl"), "utf8")).toBe("{}\n");
    expect(fs.existsSync(tree)).toBe(true);
  });

  // pi-fabric#913 round 2 (Astra P1): after the helper validated the tree, neither an entry nor an ancestor of the
  // target can be moved before the deletes: all are held without FILE_SHARE_DELETE. A move tried in that window fails,
  // and the delete happens exactly where the tree was judged.
  it("holds the tree and its ancestors against a rename between validation and the deletes", async () => {
    const parent = tempDir();
    const tree = path.join(parent, "run");
    fs.mkdirSync(path.join(tree, "nested"), { recursive: true });
    fs.writeFileSync(path.join(tree, "nested", "events.jsonl"), "{}\n");
    const args = [path.join(tree, "nested", "events.jsonl"), path.join(tree, "nested"), tree].flatMap(file => entryArgs(file));
    const pause = tempDir();
    const child = spawn(windowsRemoveHelper()!, args, { env: { ...process.env, FABRIC_WIN_REMOVE_TEST_PAUSE: pause }, windowsHide: true });
    let stdout = "";
    child.stdout!.on("data", (data: Buffer) => { stdout += String(data); });
    const exited = new Promise<number | null>(resolve => child.once("exit", resolve));
    const started = Date.now();
    while (!fs.existsSync(path.join(pause, "paused"))) {
      if (Date.now() - started > 20_000) throw new Error(`the helper never paused: ${stdout}`);
      await new Promise(resolve => setTimeout(resolve, 5));
    }
    const elsewhere = path.join(tempDir(), "moved");
    expect(() => fs.renameSync(path.join(tree, "nested"), elsewhere)).toThrow();   // an entry
    expect(() => fs.renameSync(tree, elsewhere)).toThrow();                         // the target
    expect(() => fs.renameSync(parent, `${parent}.moved`)).toThrow();               // an ancestor
    fs.writeFileSync(path.join(pause, "go"), "");
    expect(await exited, stdout).toBe(0);
    expect(stdout.trim()).toBe("ok 3 removed");
    expect(fs.existsSync(tree)).toBe(false);
    expect(fs.existsSync(elsewhere)).toBe(false);
    expect(fs.existsSync(parent)).toBe(true);
  }, 60_000);

  // pi-fabric#913 (Astra P1): a record we own whose DACL lets another SID write it is not private.
  it("accepts our private record and refuses one that another SID may write", () => {
    const file = path.join(tempDir(), "approval.json");
    fs.writeFileSync(file, "{}");
    expect(windowsPrivate(file, fs.lstatSync(file, { bigint: true }))).toBe(true);
    const granted = spawnSync("icacls", [file, "/grant", "*S-1-1-0:(W)"], { encoding: "utf8", windowsHide: true });
    expect(granted.status, granted.stdout + granted.stderr).toBe(0);
    expect(windowsPrivate(file, fs.lstatSync(file, { bigint: true }))).toBe(false);
    const [volume, index] = ids(file);
    const helper = spawnSync(windowsRemoveHelper()!, ["--private", volume!, index!, path.toNamespacedPath(file)], { encoding: "utf8", windowsHide: true });
    expect(helper.stdout.trim()).toMatch(/^refused 0 writable by another SID; ace=\d+ sid=S-1-[0-9-]+ mask=0x[0-9a-f]+ mapped=0x[0-9a-f]+ flags=0x[0-9a-f]+ sddl=D:/i);
  });

  // smarty-dev#7858 / #4664: a real file in our temp dir whose OWNER is a real well-known SID that is neither this
  // token's TokenUser nor its TokenOwner. The DACL is left as created (we keep access), so only the owner differs.
  // No real unreadable-owner fixture is claimed: denying READ_CONTROL makes the helper fail at open first,
  // not at GetSecurityInfo(OWNER). A DACL refusal is not a substitute for that missing evidence.
  it("refuses to remove a file owned by a foreign SID, and the file, its id and contents survive", () => {
    const { user, owner } = tokenSids();
    // Users, then SYSTEM, then Administrators: the first that is neither token SID (an elevated token's owner is
    // often Administrators, so it is never assumed to differ).
    const foreign = ["S-1-5-32-545", "S-1-5-18", "S-1-5-32-544"].find(sid => sid !== user && sid !== owner);
    expect(foreign, `token user ${user}, owner ${owner}`).toBeDefined();
    const dir = tempDir();
    const file = path.join(dir, "foreign.bak");
    const contents = "foreign owner\n";
    fs.writeFileSync(file, contents);
    const original = aclOwner(file);
    expect(original.sid, `a fresh file's owner (${original.trace})`).toBeDefined();
    const set = setOwner(file, foreign!);
    try {
      // Never a silent pass: if neither attempt (nor its Get-Acl confirmation) gives the file away, this hosted test
      // fails loudly with why; the finally below still restores the owner.
      expect(set.changed, `cannot make ${foreign!} the owner (token user ${user}, owner ${owner}): ${set.trace.join(" | ")}`).toBe(true);
      console.info(`[foreign-owner fixture] token user ${user}, owner ${owner}, file owner ${foreign!}: ${set.trace.join(" | ")}`);
      const id = ids(file);
      const judged = fs.lstatSync(file);
      expect(verifiedRemove(file, judged, { deleter: "test" }))
        .toEqual({ removed: false, reason: `not owned by this user (${file}); nothing is deleted` });
      // The helper itself refuses on the handle, by the owner check and nothing earlier.
      const helper = spawnSync(windowsRemoveHelper()!, entryArgs(file), { encoding: "utf8", timeout: 60_000, windowsHide: true });
      expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 not owned by this user" });
      expect(fs.readFileSync(file, "utf8")).toBe(contents);
      expect(ids(file)).toEqual(id);
      expect(fs.lstatSync(file).size).toBe(Buffer.byteLength(contents));
      expect(fs.readdirSync(dir)).toEqual(["foreign.bak"]);
      const surviving = aclOwner(file);
      expect(surviving.sid, `the surviving file's owner (${surviving.trace})`).toBe(foreign);
    } finally {
      // Give the fixture its original owner back before the temp dir teardown (best effort; the DACL entry made for us
      // at creation still allows the delete). Only this one temp file is ever touched.
      if (aclOwner(file).sid !== original.sid) {
        const back = setOwner(file, original.sid!);
        if (!back.changed) console.warn(`[foreign-owner fixture] owner not reverted: ${back.trace.join(" | ")}`);
      }
    }
  }, 240_000);

  it("never follows a reparse point: a junction in the tree refuses the removal, and its target is untouched", () => {
    const dir = tempDir();
    const outside = path.join(tempDir(), "keep.txt");
    fs.writeFileSync(outside, "keep");
    const tree = path.join(dir, "run");
    fs.mkdirSync(tree);
    const junction = path.join(tree, "link");
    fs.symlinkSync(path.dirname(outside), junction, "junction");
    expect(verifiedRemove(tree, fs.lstatSync(tree), { deleter: "test" })).toMatchObject({ removed: false, reason: expect.stringContaining("reparse point") });
    // The helper opens the junction itself (FILE_FLAG_OPEN_REPARSE_POINT) and refuses it.
    const helper = spawnSync(windowsRemoveHelper()!, entryArgs(junction, "d"), { encoding: "utf8", windowsHide: true });
    expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 a reparse point" });
    expect(fs.readFileSync(outside, "utf8")).toBe("keep");
    expect(fs.existsSync(junction)).toBe(true);
  });
});
