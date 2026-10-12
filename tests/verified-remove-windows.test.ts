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
  delete verifiedRemoveTestHooks.beforeOpen;
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const tempDir = (): string => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-win-remove-")); dirs.push(dir); return dir; };
const ids = (file: string) => { const stat = fs.lstatSync(file, { bigint: true }); return [stat.dev.toString(16), stat.ino.toString(16)]; };
/** One helper entry: KIND VOLUME INDEX MTIME (FILETIME) SIZE PATH, all from one bigint lstat. */
const entryArgs = (file: string, kind?: "f" | "d") => { const stat = fs.lstatSync(file, { bigint: true });
  return [kind ?? (stat.isDirectory() ? "d" : "f"), stat.dev.toString(16), stat.ino.toString(16),
    (stat.mtimeNs / 100n + 116_444_736_000_000_000n).toString(16), stat.size.toString(16), path.toNamespacedPath(file)]; };
// Latest platform decision: verifiedRemove refuses before any deletion or helper dispatch where getuid/geteuid are
// absent, as on real Windows. The helper itself stays covered below by direct invocations.
const NO_UID = "file ownership cannot be proven on this platform (no getuid); nothing is deleted";
/** verifiedRemove on real Windows: the no-getuid refusal, with no hook dispatched and no final check run. */
const refusesWithoutUid = (target: string, judged: fs.Stats, options: { freed?: number; proceed?: () => boolean } = {}) => {
  const dispatched: string[] = [];
  verifiedRemoveTestHooks.beforeOpen = () => { dispatched.push("beforeOpen"); };
  verifiedRemoveTestHooks.beforeHelper = () => { dispatched.push("beforeHelper"); };
  let checked = 0;
  const proceed = options.proceed ? () => { checked++; return options.proceed!(); } : undefined;
  expect(verifiedRemove(target, judged, { deleter: "test", ...(options.freed === undefined ? {} : { freed: options.freed }),
    ...(proceed ? { proceed } : {}) })).toEqual({ removed: false, reason: NO_UID });
  expect({ dispatched, checked }).toEqual({ dispatched: [], checked: 0 });
};

describe.skipIf(process.platform !== "win32")("Windows verified removal (smarty-dev#7858)", () => {
  it("has the digest-verified helper in this build", () => {
    expect(windowsRemoveHelper()).toMatch(/fabric-win-remove\.exe$/);
  });

  // verifiedRemove refuses (no getuid) and leaves both untouched; the helper itself removes them by handle.
  it("verifiedRemove refuses our own file and tree without getuid; the helper removes them by handle", () => {
    const dir = tempDir();
    const file = path.join(dir, "old.bak");
    fs.writeFileSync(file, "x".repeat(10));
    refusesWithoutUid(file, fs.lstatSync(file));
    expect(fs.readFileSync(file, "utf8")).toBe("x".repeat(10));
    const fileHelper = spawnSync(windowsRemoveHelper()!, entryArgs(file), { encoding: "utf8", windowsHide: true });
    expect({ status: fileHelper.status, line: fileHelper.stdout.trim() }).toEqual({ status: 0, line: "ok 1 removed" });
    expect(fs.existsSync(file)).toBe(false);
    const tree = path.join(dir, "run");
    fs.mkdirSync(path.join(tree, "nested"), { recursive: true });
    fs.writeFileSync(path.join(tree, "nested", "events.jsonl"), "{}\n");
    refusesWithoutUid(tree, fs.lstatSync(tree));
    expect(fs.readFileSync(path.join(tree, "nested", "events.jsonl"), "utf8")).toBe("{}\n");
    const args = [path.join(tree, "nested", "events.jsonl"), path.join(tree, "nested"), tree].flatMap(entry => entryArgs(entry));
    const treeHelper = spawnSync(windowsRemoveHelper()!, args, { encoding: "utf8", windowsHide: true });
    expect({ status: treeHelper.status, line: treeHelper.stdout.trim() }).toEqual({ status: 0, line: "ok 3 removed" });
    expect(fs.existsSync(tree)).toBe(false);
  });

  // Without getuid, verifiedRemove never moves the target off its name and never runs the caller's final check.
  it("refuses without getuid before the final check: the target stays on its name, unchanged", () => {
    const dir = tempDir();
    const file = path.join(dir, "events.jsonl");
    fs.writeFileSync(file, "old events\n");
    refusesWithoutUid(file, fs.lstatSync(file), { freed: 4, proceed: () => { fs.writeFileSync(file, "tail\n", { flag: "wx" }); return true; } });
    refusesWithoutUid(file, fs.lstatSync(file), { proceed: () => false });
    expect(fs.readFileSync(file, "utf8")).toBe("old events\n");
    expect(fs.readdirSync(dir)).toEqual(["events.jsonl"]);
  });

  // pi-fabric#913 round 4 (Astra P1): a file refreshed between the scan and the helper's open keeps its id; its
  // last-write time or size no longer matches the judged ones, so the helper keeps it.
  it("keeps a file refreshed after the scan with the same id (new time, or new size)", () => {
    const dir = tempDir();
    const file = path.join(dir, "events.jsonl");
    fs.writeFileSync(file, "judged\n");
    const id = ids(file);
    // verifiedRemove refuses without getuid, before the helper; the helper itself keeps refreshed files (exit 4).
    refusesWithoutUid(file, fs.lstatSync(file));
    // Same size, later time.
    const judgedArgs = entryArgs(file);
    fs.writeFileSync(file, "fresh!\n");
    const later = new Date(Date.now() + 10_000);
    fs.utimesSync(file, later, later);
    const timed = spawnSync(windowsRemoveHelper()!, judgedArgs, { encoding: "utf8", windowsHide: true });
    expect({ status: timed.status, line: timed.stdout.trim() }).toEqual({ status: 4, line: "kept 0 modified since judgment" });
    expect(fs.readFileSync(file, "utf8")).toBe("fresh!\n");
    expect(ids(file)).toEqual(id);
    // Larger.
    const sizedArgs = entryArgs(file);
    fs.appendFileSync(file, "more\n");
    const sized = spawnSync(windowsRemoveHelper()!, sizedArgs, { encoding: "utf8", windowsHide: true });
    expect({ status: sized.status, line: sized.stdout.trim() }).toEqual({ status: 4, line: "kept 0 modified since judgment" });
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
    refusesWithoutUid(file, judged);
    // The helper itself, given the judged id for the replaced name, refuses on the handle.
    const helper = spawnSync(windowsRemoveHelper()!, judgedArgs, { encoding: "utf8", windowsHide: true });
    expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 file id changed since judgment" });
    expect(fs.readFileSync(file, "utf8")).toBe("replacement");
  });

  // pi-fabric#913 (Astra P1): an ancestor in the judged tree swapped for a junction AFTER the walk and before the
  // helper opens the entries: the helper opens the junction itself and refuses; nothing is deleted anywhere.
  // verifiedRemove refuses without getuid first; the swap is then proven against the helper directly.
  it("refuses an ancestor swapped for a junction after the walk, and deletes nothing", () => {
    const dir = tempDir();
    const tree = path.join(dir, "run");
    const nested = path.join(tree, "nested");
    fs.mkdirSync(nested, { recursive: true });
    fs.writeFileSync(path.join(nested, "events.jsonl"), "{}\n");
    refusesWithoutUid(tree, fs.lstatSync(tree));
    const args = [path.join(nested, "events.jsonl"), nested, tree].flatMap(entry => entryArgs(entry));
    fs.renameSync(nested, `${nested}.real`);
    fs.symlinkSync(`${nested}.real`, nested, "junction");
    const helper = spawnSync(windowsRemoveHelper()!, args, { encoding: "utf8", windowsHide: true });
    expect(helper.status, helper.stdout).toBe(3);
    expect(helper.stdout.trim()).toMatch(/^refused \d+ .*reparse point/);
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
    expect(helper.stdout.trim()).toBe("refused 0 writable by another SID");
  });

  it("never follows a reparse point: a junction in the tree refuses the removal, and its target is untouched", () => {
    const dir = tempDir();
    const outside = path.join(tempDir(), "keep.txt");
    fs.writeFileSync(outside, "keep");
    const tree = path.join(dir, "run");
    fs.mkdirSync(tree);
    const junction = path.join(tree, "link");
    fs.symlinkSync(path.dirname(outside), junction, "junction");
    refusesWithoutUid(tree, fs.lstatSync(tree));
    // The helper opens the junction itself (FILE_FLAG_OPEN_REPARSE_POINT) and refuses it.
    const helper = spawnSync(windowsRemoveHelper()!, entryArgs(junction, "d"), { encoding: "utf8", windowsHide: true });
    expect({ status: helper.status, line: helper.stdout.trim() }).toEqual({ status: 3, line: "refused 0 a reparse point" });
    expect(fs.readFileSync(outside, "utf8")).toBe("keep");
    expect(fs.existsSync(junction)).toBe(true);
  });
});
