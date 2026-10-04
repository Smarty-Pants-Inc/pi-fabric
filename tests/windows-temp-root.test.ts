import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fabricDataRoot } from "../src/storage/temp-root.js";
import { windowsDataRoot } from "../src/storage/windows-temp-root.js";
import { windowsSecurityPowerShell } from "../src/storage/windows-powershell.js";

// Policy unit tests supply raw snapshots; transport/cold-vs-warm checks live in
// windows-acl-inspector.test.ts, and native ACL mutations still use the real bridge.
vi.mock("../src/storage/windows-acl-inspector.js", () => ({
  inspectWindowsAclChain: (source: string, env: NodeJS.ProcessEnv) => {
    const command = windowsSecurityPowerShell(source, env);
    return childProcess.execFileSync(command.file, command.args, {
      env: command.env, encoding: "utf8", windowsHide: true, timeout: 15_000, maxBuffer: 1024 * 1024,
      stdio: ["ignore", "pipe", "pipe"],
    });
  },
}));

type Ace = { type: number; flags: number; sid: string; mask: number };
type Directory = { path: string; attributes: number; owner: string; dacl: Ace[] | null };
const userSid = "S-1-5-21-111-222-333-1001";
const everyone = "S-1-1-0";
const users = "S-1-5-32-545";
const root = "R:\\private\\data";
const paths = ["R:\\", "R:\\private", root];
const allow = (sid = userSid, mask = 0x1f01ff, flags = 0): Ace => ({ type: 0, flags, sid, mask });
let directories: Directory[];
let normalTemp: string | undefined;
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;

beforeEach(() => {
  normalTemp = undefined;
  directories = paths.map(path => ({ path, attributes: 0x10, owner: userSid, dacl: [allow()] }));
  vi.stubEnv("SystemRoot", "C:\\Windows");
  vi.stubEnv("PI_FABRIC_TMPDIR", undefined);
  vi.spyOn(childProcess, "execFileSync").mockImplementation(() => JSON.stringify({ userSid, device: "\\Device\\HarddiskVolume7", normalTemp, directories }));
});
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

describe("Windows file-data namespace ACL policy", () => {
  it("accepts a private user namespace through fabricDataRoot without using mode bits or writing", () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    vi.stubEnv("PI_FABRIC_TMPDIR", root + "\\");
    const lstat = vi.spyOn(fs, "lstatSync");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const chmod = vi.spyOn(fs, "chmodSync");
    expect(fabricDataRoot()).toBe(root);
    expect(lstat).not.toHaveBeenCalled();
    expect(mkdir).not.toHaveBeenCalled();
    expect(chmod).not.toHaveBeenCalled();
  });

  it.each(["S-1-5-18", "S-1-5-32-544", "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"])("trusts system SID %s for ownership and grants", sid => {
    for (const directory of directories) { directory.owner = sid; directory.dacl = [allow(sid)]; }
    expect(windowsDataRoot(root)).toBe(root);
  });

  it.each([0, 1, 2])("rejects foreign ownership at chain index %s", index => {
    directories[index]!.owner = users;
    expect(() => windowsDataRoot(root)).toThrow(/owned by another user/);
  });

  it.each([
    ["write data", 0x2], ["append/create directories", 0x4], ["write EA", 0x10],
    ["delete child", 0x40], ["write attributes", 0x100], ["delete", 0x10000],
    ["change permissions", 0x40000], ["take ownership", 0x80000],
    ["generic all", 0x10000000], ["generic write", 0x40000000],
    ["maximum allowed", 0x02000000], ["unknown", 0x800],
  ])("rejects untrusted %s rights (%s) at every level", (_name, mask) => {
    for (const index of [0, 1, 2]) {
      directories[index]!.dacl!.push(allow(everyone, mask as number));
      expect(() => windowsDataRoot(root)).toThrow(/untrusted principal/);
      directories[index]!.dacl!.pop();
    }
  });

  it.each([everyone, users, "S-1-5-11", "S-1-5-19", "S-1-5-20", "S-1-3-0", "S-1-5-21-111-222-333-1002"])("does not trust SID %s merely because it is a group, service or creator", sid => {
    directories[1]!.dacl!.push(allow(sid));
    expect(() => windowsDataRoot(root)).toThrow(/untrusted principal/);
  });

  it.each([0, 0x10, 0x0b])("rejects unsafe explicit, inherited and inherit-only grants (flags=%s)", flags => {
    directories[0]!.dacl!.push(allow(users, 0x2, flags));
    expect(() => windowsDataRoot(root)).toThrow(/untrusted principal/);
  });

  it("permits sibling creation and inherit-only grants only above native normal temp", () => {
    normalTemp = paths[1];
    directories[0]!.dacl!.push(allow(users, 0x06), allow("S-1-3-0", 0x1f01ff, 0x0b));
    expect(windowsDataRoot(root, { private: true })).toBe(root);
    directories[1]!.dacl!.push(allow(users, 0x04));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/untrusted principal/);
  });

  it.each([0x10, 0x40, 0x100, 0x10000, 0x40000, 0x80000, 0x40000000])("rejects ancestor mutation/replacement above normal temp (%s)", mask => {
    normalTemp = paths[1]; directories[0]!.dacl!.push(allow(users, mask));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/untrusted principal/);
  });

  it.each([0, 1, 2])("keeps ownership and reparse checks above/at/below normal temp (%s)", index => {
    normalTemp = paths[1]; directories[index]!.owner = users;
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/owned by another user/);
    directories[index]!.owner = userSid; directories[index]!.attributes |= 0x400;
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/not a real directory/);
  });

  it("does not infer normal temp from caller-controlled environment paths", () => {
    normalTemp = "R:\\unrelated";
    vi.stubEnv("TMP", paths[1]); vi.stubEnv("TEMP", paths[1]);
    directories[0]!.dacl!.push(allow(users, 0x04));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/untrusted principal/);
  });

  it("checks inherited grants at/below normal temp despite harmless grants above it", () => {
    normalTemp = paths[1]; directories[0]!.dacl!.push(allow(users, 0x1f01ff, 0x0b));
    directories[1]!.dacl!.push(allow(users, 0x2, 0x0b));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/untrusted principal/);
    directories[1]!.dacl!.pop(); directories[2]!.dacl!.push(allow(users, 0x1, 0x0b));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/not private/);
  });
  it("fails closed even when a deny could cancel an unsafe allow", () => {
    directories[1]!.dacl!.unshift({ ...allow(everyone), type: 1 });
    directories[1]!.dacl!.push(allow(everyone));
    expect(() => windowsDataRoot(root)).toThrow(/untrusted principal/);
  });

  it.each([0x1, 0x80, 0x20000, 0x80000000, 0xa01200a9])("private scratch rejects untrusted read grants (%s)", mask => {
    directories[2]!.dacl!.push(allow(everyone, mask));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/not private/);
  });

  it("private scratch still allows read/traverse-only ancestors, not writable ancestors", () => {
    directories[0]!.dacl!.push(allow(everyone, 0xa01200a9));
    expect(windowsDataRoot(root, { private: true })).toBe(root);
    directories[0]!.dacl!.push(allow(everyone, 0x2));
    expect(() => windowsDataRoot(root, { private: true })).toThrow(/untrusted principal/);
  });

  it("permits known untrusted read/traverse rights and standard denies", () => {
    for (const directory of directories) directory.dacl!.push(allow(everyone, 0xa01200a9), { ...allow(users), type: 1 });
    expect(windowsDataRoot(root)).toBe(root);
  });

  it.each([0, 1, 2])("rejects a junction, symlink or any reparse point at index %s", index => {
    directories[index]!.attributes |= 0x400;
    expect(() => windowsDataRoot(root + "\\")).toThrow(/not a real directory/);
  });

  it("rejects files even with a private ACL", () => {
    directories[2]!.attributes = 0x80;
    expect(() => windowsDataRoot(root)).toThrow(/not a real directory/);
  });

  it.each([0, 1, 2])("rejects a null DACL at index %s", index => {
    directories[index]!.dacl = null;
    expect(() => windowsDataRoot(root)).toThrow(/null or unproven DACL/);
  });

  it.each([5, 6, 9, 10, 17])("rejects unsupported object, callback or other ACE type %s", type => {
    directories[2]!.dacl!.push({ ...allow(userSid), type });
    expect(() => windowsDataRoot(root)).toThrow(/unsupported or unproven ACE/);
  });

  it.each([
    { mask: -1 }, { mask: 0x100000000 }, { mask: 1.5 }, { mask: "2" },
    { flags: 0x80 }, { flags: -1 }, { sid: "Everyone" }, { type: "0" },
  ])("rejects malformed native ACE %j", change => {
    directories[2]!.dacl!.push({ ...allow(), ...change } as Ace);
    expect(() => windowsDataRoot(root)).toThrow(/unsupported or unproven ACE/);
  });

  it.each(["null", "{}", "not json", JSON.stringify({ userSid, directories: [] }), JSON.stringify({ userSid: "user", directories: [] })])("fails closed on unproven output %s", output => {
    vi.mocked(childProcess.execFileSync).mockReturnValue(output);
    expect(() => windowsDataRoot(root)).toThrow(/PI_FABRIC_TMPDIR.*(?:invalid|could not prove)/);
  });

  it("requires the complete ordered chain, with valid owners and attributes", () => {
    directories.reverse();
    expect(() => windowsDataRoot(root)).toThrow(/invalid native Windows directory snapshot/);
    directories.reverse();
    directories[0]!.owner = "BUILTIN\\Administrators";
    expect(() => windowsDataRoot(root)).toThrow(/unproven owner/);
    directories[0]!.owner = userSid;
    directories[0]!.attributes = -1;
    expect(() => windowsDataRoot(root)).toThrow(/invalid native Windows directory snapshot/);
  });

  it.each(["relative", "C:relative", "\\relative", "\\\\server\\share\\data", "\\\\?\\C:\\data", "\\\\.\\C:\\data", "C:\\data:stream", "C:\\private.\\data", "C:\\private \\data", "C:\\..\\data", "C:\\NUL", "C:\\con.txt", "C:\\bad\u0000name", "C:\\wild*card"])("rejects unsupported Windows namespace %j before invocation", directory => {
    expect(() => windowsDataRoot(directory)).toThrow(/PI_FABRIC_TMPDIR.*(?:absolute local drive|ambiguous Windows)/);
    expect(childProcess.execFileSync).not.toHaveBeenCalled();
  });

  it("uses the absolute OS executable and transports names as data, with bounded execution", () => {
    vi.stubEnv("PSModulePath", "C:\\Program Files\\PowerShell\\7\\Modules");
    const unusual = "R:\\private\\quote'$;日本語";
    directories[2]!.path = unusual;
    expect(windowsDataRoot(unusual)).toBe(unusual);
    const [exe, args, options] = vi.mocked(childProcess.execFileSync).mock.calls[0]!;
    expect(exe).toBe("C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe");
    expect(args).toEqual(["-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", expect.any(String)]);
    const source = Buffer.from((args as string[])[4]!, "base64").toString("utf16le");
    expect(source.startsWith("$ErrorActionPreference = 'Stop'\nImport-Module Microsoft.PowerShell.Security -ErrorAction Stop\n")).toBe(true);
    expect(source).toContain("RawSecurityDescriptor");
    expect(source).toContain("[System.IO.Directory]::GetAccessControl($directory)");
    expect(source).toContain("[System.IO.File]::GetAttributes($directory)");
    expect(source).not.toContain(unusual);
    expect(options).toMatchObject({ timeout: 15000, windowsHide: true, encoding: "utf8", env: { PSModulePath: "C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\Modules", PI_FABRIC_ACL_CHAIN: JSON.stringify([paths[0], paths[1], unusual]) } });
  });

  it.each(["\\\\??\\\\C:\\\\hidden", "\\\\Device\\\\LanmanRedirector\\\\share", "\\\\Device\\\\CdRom0", "unknown", ""])("rejects unproven, mapped or substituted device %j", device => {
    vi.mocked(childProcess.execFileSync).mockReturnValue(JSON.stringify({ userSid, device, directories }));
    expect(() => windowsDataRoot(root)).toThrow(/not a proven direct local volume/);
  });

  it.each(["ENOENT", "ETIMEDOUT", "EACCES", "CouldNotAutoloadMatchingModule"])("fails closed on native command error %s, without creating anything", code => {
    vi.mocked(childProcess.execFileSync).mockImplementation(() => { throw Object.assign(new Error(code), { code }); });
    const mkdir = vi.spyOn(fs, "mkdirSync");
    expect(() => windowsDataRoot(root)).toThrow(/could not prove native Windows ACL/);
    expect(mkdir).not.toHaveBeenCalled();
  });

  it("does not cache ACL decisions across calls", () => {
    expect(windowsDataRoot(root)).toBe(root);
    directories[1]!.dacl!.push(allow(everyone));
    expect(() => windowsDataRoot(root)).toThrow(/untrusted principal/);
    expect(childProcess.execFileSync).toHaveBeenCalledTimes(2);
  });

  it("does not inspect ACLs when the override is unset or empty", () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    expect(fabricDataRoot()).toBe(os.tmpdir());
    vi.stubEnv("PI_FABRIC_TMPDIR", "");
    expect(fabricDataRoot()).toBe(os.tmpdir());
    expect(childProcess.execFileSync).not.toHaveBeenCalled();
  });
});
