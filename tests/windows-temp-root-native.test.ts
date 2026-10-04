import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { createScratch } from "../src/storage/scratch.js";
import { createRunTmpDirectory } from "../src/storage/run-scratch.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { fabricDataRoot } from "../src/storage/temp-root.js";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";
import { windowsDataRoot } from "../src/storage/windows-temp-root.js";

// A normal Windows drive root may allow Users to create directories. Do not weaken
// ancestor checks or rewrite the runner's C:/D: ACLs to get a positive control.
// windows-latest runs elevated: use an isolated, disposable 64 MiB NTFS VHD instead.
// Setup failure on Windows is a failure, never a silent skip of native ACL evidence.
const native = (source: string, values: Record<string, string | number> = {}): string => {
  const command = windowsSecurityPowerShell(`
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$p = ConvertFrom-Json -InputObject $env:FABRIC_ACL_TEST_VALUES
${source}
`, { ...process.env, FABRIC_ACL_TEST_VALUES: JSON.stringify(values) });
  return childProcess.execFileSync(command.file, command.args,
    { env: command.env, encoding: "utf8", windowsHide: true, timeout: 20_000, stdio: ["ignore", "pipe", "pipe"] },
  ).trim();
};

const PRIVATE_ACL = String.raw`
$user = [System.Security.Principal.WindowsIdentity]::GetCurrent().User
$acl = [System.Security.AccessControl.DirectorySecurity]::new()
$acl.SetOwner($user)
$acl.SetAccessRuleProtection($true, $false)
foreach ($sid in @($user.Value, 'S-1-5-18', 'S-1-5-32-544')) {
  $identity = [System.Security.Principal.SecurityIdentifier]::new($sid)
  $rule = [System.Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl', 'ContainerInherit,ObjectInherit', 'None', 'Allow')
  $acl.AddAccessRule($rule)
}
Set-Acl -LiteralPath $p.path -AclObject $acl
`;

// Set an actual foreign owner without creating accounts. Setting an arbitrary SID
// requires SeRestorePrivilege; enable it explicitly and fail if the token lacks it.
const FOREIGN_OWNER = String.raw`
Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
public static class FabricAclTestPrivilege {
  [StructLayout(LayoutKind.Sequential)] struct Luid { public uint Low; public int High; }
  [StructLayout(LayoutKind.Sequential)] struct TokenPrivileges { public uint Count; public Luid Id; public uint Attributes; }
  [DllImport("kernel32.dll")] static extern IntPtr GetCurrentProcess();
  [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr handle);
  [DllImport("advapi32.dll", SetLastError=true)] static extern bool OpenProcessToken(IntPtr process, uint access, out IntPtr token);
  [DllImport("advapi32.dll", CharSet=CharSet.Unicode, SetLastError=true)] static extern bool LookupPrivilegeValue(string system, string name, out Luid id);
  [DllImport("advapi32.dll", SetLastError=true)] static extern bool AdjustTokenPrivileges(IntPtr token, bool disable, ref TokenPrivileges privileges, uint length, IntPtr previous, IntPtr returned);
  public static void EnableRestore() {
    IntPtr token;
    if (!OpenProcessToken(GetCurrentProcess(), 0x28, out token)) throw new Win32Exception();
    try {
      Luid id;
      if (!LookupPrivilegeValue(null, "SeRestorePrivilege", out id)) throw new Win32Exception();
      var privileges = new TokenPrivileges { Count = 1, Id = id, Attributes = 2 };
      if (!AdjustTokenPrivileges(token, false, ref privileges, 0, IntPtr.Zero, IntPtr.Zero)) throw new Win32Exception();
      int error = Marshal.GetLastWin32Error();
      if (error != 0) throw new Win32Exception(error);
    } finally { CloseHandle(token); }
  }
}
'@
[FabricAclTestPrivilege]::EnableRestore()
$acl = Get-Acl -LiteralPath $p.path
$acl.SetOwner([System.Security.Principal.SecurityIdentifier]::new('S-1-5-32-545'))
Set-Acl -LiteralPath $p.path -AclObject $acl
(Get-Acl -LiteralPath $p.path).GetOwner([System.Security.Principal.SecurityIdentifier]).Value
`;

let backing = "";
let volume = "";
let letter = "";
let diskpartSequence = 0;
const diskpart = (commands: string[]) => {
  const script = path.join(backing, `diskpart-${diskpartSequence++}.txt`);
  fs.writeFileSync(script, commands.join("\r\n") + "\r\nexit\r\n");
  return childProcess.execFileSync(path.join(process.env.SystemRoot!, "System32", "diskpart.exe"), ["/s", script], { encoding: "utf8", timeout: 90_000, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
};
const detach = () => {
  if (!backing) return;
  const vhd = path.join(backing, "acl-fixture.vhd");
  // Setup may already have detached the fixture. Ignore that diskpart error,
  // but still fail below if the volume remains mounted after the command.
  if (fs.existsSync(vhd)) diskpart([`select vdisk file="${vhd}"`, "detach vdisk noerr"]);
  if (volume && fs.existsSync(volume)) throw new Error(`Native ACL test VHD did not detach: ${volume}`);
};
const privateDirectory = (name = "private") => {
  const directory = fs.mkdtempSync(path.join(volume, name + "-"));
  native(PRIVATE_ACL, { path: directory });
  return directory;
};
const sddl = (directory: string) => native("(Get-Acl -LiteralPath $p.path).Sddl", { path: directory });
const grant = (directory: string, sid: string, mask: number) => native(String.raw`
$acl = Get-Acl -LiteralPath $p.path
$identity = [System.Security.Principal.SecurityIdentifier]::new($p.sid)
$rule = [System.Security.AccessControl.FileSystemAccessRule]::new($identity, [System.Security.AccessControl.FileSystemRights][int]$p.mask, 'None', 'None', 'Allow')
$acl.AddAccessRule($rule)
Set-Acl -LiteralPath $p.path -AclObject $acl
`, { path: directory, sid, mask });

describe.skipIf(process.platform !== "win32")("native Windows temp-root ACL contract (requires elevated Windows runner)", () => {
  beforeAll(() => {
    backing = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-acl-"));
    if (/["\r\n]/.test(backing)) throw new Error("Unsafe diskpart fixture path");
    const drives = JSON.parse(native("ConvertTo-Json -Compress -InputObject @([System.IO.Directory]::GetLogicalDrives())")) as string[];
    letter = [..."ZYXWVUTSRQPONMLKJIHGFE"].find(candidate => !drives.some(drive => drive[0]!.toUpperCase() === candidate))!;
    if (!letter) throw new Error("No unused drive letter for native ACL fixture");
    volume = `${letter}:\\`;
    const vhd = path.join(backing, "acl-fixture.vhd");
    try {
      const output = diskpart([
        `create vdisk file="${vhd}" maximum=64 type=expandable`, `select vdisk file="${vhd}"`,
        "attach vdisk", "create partition primary", 'format fs=ntfs label="fabric-acl-test" quick', `assign letter=${letter}`,
      ]);
      // diskpart can exit 0 on a command failure, so prove the actual fixture exists.
      if (!fs.existsSync(volume)) throw new Error(`Native NTFS fixture setup failed: ${output}`);
      native(PRIVATE_ACL, { path: volume });
      vi.stubEnv("PI_FABRIC_TMPDIR", volume);
      expect(fabricDataRoot()).toBe(volume);
    } catch (error) {
      detach();
      throw error;
    } finally { vi.unstubAllEnvs(); }
  }, 120_000);

  afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });
  afterAll(() => {
    detach();
    if (backing) fs.rmSync(backing, { recursive: true, force: true });
  }, 120_000);

  it("D5 launcher fixtures use a natively accepted private OS-temp namespace", () => {
    // This is the namespace inherited by worker-e2e/process-utils/actor tests,
    // not this suite's separate adversarial ACL volume.
    expect(windowsDataRoot(os.tmpdir(), { private: true })).toBe(path.resolve(os.tmpdir()));
  }, 30_000);

  it("reuses the native inspector but never reuses an ACL decision after a real grant", () => {
    const directory = privateDirectory("warm-inspector");
    const spawn = vi.spyOn(childProcess, "spawn");
    expect(windowsDataRoot(directory, { private: true })).toBe(directory);
    grant(directory, "S-1-1-0", 0x1);
    const before = sddl(directory);
    expect(() => windowsDataRoot(directory, { private: true })).toThrow(/not private/);
    expect(sddl(directory)).toBe(before);
    // beforeAll already opened this process's inspector. Changing policy on the
    // filesystem must not require spawning/compiling another PowerShell process.
    expect(spawn).not.toHaveBeenCalled();
  }, 30_000);

  it("accepts a private per-user directory and places actual scratch data there", () => {
    const directory = privateDirectory("quote'$;日本語");
    const before = sddl(directory);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory + path.sep);
    expect(fabricDataRoot()).toBe(directory);
    const scratch = createScratch("output");
    expect(scratch.startsWith(directory + path.sep)).toBe(true);
    expect(fs.existsSync(scratch)).toBe(true);
    expect(sddl(directory)).toBe(before);
  }, 30_000);

  it("exports private run scratch only under a natively proven private explicit run root", async () => {
    const directory = privateDirectory("explicit-run");
    const before = sddl(directory);
    const worker = path.join(directory, "worker.mjs");
    fs.writeFileSync(worker, `import fs from "node:fs";
fs.writeFileSync(${JSON.stringify(path.join(directory, "environment.json"))}, JSON.stringify({tmpdir:process.env.TMPDIR,tmp:process.env.TMP,temp:process.env.TEMP}));`);
    const handle = await new ProcessTransport().launch({ id: "private", name: "private", cwd: directory, workerPath: worker,
      workerArguments: ["--status-file", path.join(directory, "status.json")] });
    try {
      await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false), { timeout: 10000 });
      const tmp = path.join(directory, "tmp");
      expect(JSON.parse(fs.readFileSync(path.join(directory, "environment.json"), "utf8"))).toEqual({tmpdir:tmp,tmp:tmp,temp:tmp});
      expect(createRunTmpDirectory(directory)).toBe(tmp);
      expect(sddl(directory)).toBe(before);
    } finally {
      await handle.stop();
      await vi.waitFor(async () => expect(await handle.isAlive()).toBe(false), { timeout: 10000 });
    }
  }, 60000);

  it.each([0x1, 0x3])("rejects explicit run roots with another ordinary user's read/read-write grant (%s) before spawn", async mask => {
    const directory = privateDirectory("foreign-grant");
    const sid = native("([System.Security.Principal.WindowsIdentity]::GetCurrent().User.AccountDomainSid.Value) + '-424242'");
    grant(directory, sid, mask);
    const before = sddl(directory);
    await expect(new ProcessTransport().launch({ id: "unsafe", name: "unsafe", cwd: directory,
      workerPath: path.join(directory, "must-not-launch.mjs"), workerArguments: ["--status-file", path.join(directory, "status.json")] })).rejects.toThrow(/not private|untrusted principal/);
    expect(fs.existsSync(path.join(directory, "tmp"))).toBe(false);
    expect(sddl(directory)).toBe(before);
  }, 30000);

  it("rejects an existing read-public scratch directory even inside a private explicit run root", () => {
    const directory = privateDirectory("existing-scratch");
    const tmp = path.join(directory, "tmp");
    fs.mkdirSync(tmp);
    grant(tmp, "S-1-1-0", 0x1);
    const before = sddl(tmp);
    expect(() => createRunTmpDirectory(directory)).toThrow(/not private/);
    expect(sddl(tmp)).toBe(before);
  }, 30000);

  it("rejects a foreign-owned root without changing ownership or ACLs", () => {
    const directory = privateDirectory();
    expect(native(FOREIGN_OWNER, { path: directory })).toBe("S-1-5-32-545");
    const before = sddl(directory);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/owned by another user/);
    expect(sddl(directory)).toBe(before);
  }, 30_000);

  it.each(["S-1-1-0", "S-1-5-32-545"])("rejects a writable ancestor for SID %s even when the child has a private ACL", sid => {
    const ancestor = privateDirectory("ancestor");
    const directory = path.join(ancestor, "private-data");
    fs.mkdirSync(directory);
    native(PRIVATE_ACL, { path: directory });
    grant(ancestor, sid, 0x2);
    const before = sddl(ancestor);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/untrusted principal/);
    expect(sddl(ancestor)).toBe(before);
    expect(fs.readdirSync(directory)).toEqual([]);
    const missing = path.join(ancestor, "must-not-create");
    vi.stubEnv("PI_FABRIC_TMPDIR", missing);
    expect(() => fabricDataRoot()).toThrow(/PI_FABRIC_TMPDIR/);
    expect(fs.existsSync(missing)).toBe(false);
  }, 30_000);

  it("rejects an untrusted writable volume root even with a private descendant", () => {
    const directory = privateDirectory();
    try {
      grant(volume, "S-1-1-0", 0x2);
      vi.stubEnv("PI_FABRIC_TMPDIR", directory);
      expect(() => fabricDataRoot()).toThrow(/untrusted principal/);
    } finally { native(PRIVATE_ACL, { path: volume }); }
  }, 30_000);

  it("rejects a real SUBST drive that conceals the underlying ancestors", () => {
    const directory = privateDirectory();
    const drives = JSON.parse(native("ConvertTo-Json -Compress -InputObject @([System.IO.Directory]::GetLogicalDrives())")) as string[];
    const alias = [..."ZYXWVUTSRQPONMLKJIHGFE"].find(candidate => !drives.some(drive => drive[0]!.toUpperCase() === candidate));
    if (!alias) throw new Error("No unused drive letter for SUBST regression");
    const subst = path.join(process.env.SystemRoot!, "System32", "subst.exe");
    childProcess.execFileSync(subst, [`${alias}:`, directory], { timeout: 10_000, windowsHide: true });
    try {
      vi.stubEnv("PI_FABRIC_TMPDIR", `${alias}:\\`);
      expect(() => fabricDataRoot()).toThrow(/could not prove native Windows ACL/);
    } finally { childProcess.execFileSync(subst, [`${alias}:`, "/D"], { timeout: 10_000, windowsHide: true }); }
  }, 30_000);

  it.each([["delete child", 0x40], ["delete", 0x10000], ["change permissions", 0x40000], ["take ownership", 0x80000]] as const)("rejects native untrusted %s rights", (_name, mask) => {
    const directory = privateDirectory();
    grant(directory, "S-1-1-0", mask);
    vi.stubEnv("PI_FABRIC_TMPDIR", directory);
    expect(() => fabricDataRoot()).toThrow(/untrusted principal/);
  }, 30_000);

  it.each([false, true])("rejects a real junction at root or ancestor, including a trailing separator (ancestor=%s)", ancestor => {
    const parent = privateDirectory("junction");
    const target = path.join(parent, "target");
    const link = path.join(parent, "link");
    fs.mkdirSync(target);
    fs.mkdirSync(path.join(target, "data"));
    fs.symlinkSync(target, link, "junction");
    vi.stubEnv("PI_FABRIC_TMPDIR", (ancestor ? path.join(link, "data") : link) + path.sep);
    expect(() => fabricDataRoot()).toThrow(/not a real directory/);
    expect(fs.readdirSync(path.join(target, "data"))).toEqual([]);
  }, 30_000);

  it("rejects a missing private root without creating it, and preserves the unset fallback", () => {
    const parent = privateDirectory();
    const missing = path.join(parent, "missing", "data");
    vi.stubEnv("PI_FABRIC_TMPDIR", missing);
    expect(() => fabricDataRoot()).toThrow(/could not prove native Windows ACL/);
    expect(fs.existsSync(path.dirname(missing))).toBe(false);
    vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
    expect(fabricDataRoot()).toBe(os.tmpdir());
  }, 30_000);
});
