import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { privateWindowsTestTemp } from "../scripts/windows-test-temp.js";
import { windowsDataRoot } from "../src/storage/windows-temp-root.js";

vi.mock("../src/storage/windows-temp-root.js", () => ({ windowsDataRoot: vi.fn() }));
vi.mock("../src/storage/windows-powershell.js", () => ({
  windowsSecurityPowerShell: (source: string, env: NodeJS.ProcessEnv) => ({ file: "powershell.exe", args: [source], env }),
}));

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); vi.mocked(windowsDataRoot).mockReset(); });

// The real directory provisioning code runs on Linux with filesystem/native IO
// simulated. Real ACL isolation/adversarial tests remain native Windows gates.
const fixture = () => {
  vi.stubEnv("CI", "1");
  vi.stubEnv("RUNNER_TEMP", path.resolve("runner-temp"));
  vi.stubEnv("GITHUB_RUN_ID", "369");
  vi.stubEnv("GITHUB_RUN_ATTEMPT", "1");
  const temp = path.resolve("normal-temp");
  const directories = new Set<string>();
  vi.spyOn(fs, "mkdirSync").mockImplementation(file => {
    if (directories.has(String(file))) throw Object.assign(new Error("exists"), { code: "EEXIST" });
    directories.add(String(file)); return undefined;
  });
  vi.spyOn(fs, "mkdtempSync").mockImplementation(prefix => {
    const directory = `${prefix}unique`; directories.add(directory); return directory;
  });
  vi.spyOn(fs, "lstatSync").mockReturnValue({ dev: 1, ino: 2, isDirectory: () => true, isSymbolicLink: () => false } as fs.Stats);
  const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  const native = vi.spyOn(childProcess, "execFileSync").mockImplementation((_file, args) =>
    (String(args![0]).includes("GetFolderPath") ? temp : "") as never);
  return { temp, directories, native, remove };
};

describe("private Windows test directory lifecycle", () => {
  it("provisions owner-only inheritable ACLs under normal temp without disk provisioning", () => {
    const f = fixture(); const namespace = privateWindowsTestTemp();
    expect(path.dirname(namespace.directory)).toBe(f.temp);
    expect(f.native).toHaveBeenCalledTimes(2);
    const [file, args, options] = f.native.mock.calls[1]!;
    expect(file).toBe("powershell.exe");
    expect(String(args![0])).toContain("SetAccessRuleProtection($true, $false)");
    expect(String(args![0])).toContain("ContainerInherit,ObjectInherit");
    expect(String(args![0])).toContain("Unowned test directory");
    expect(String(args![0])).toContain("$adminOwner = $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)");
    expect(String(args![0])).toContain("if ($adminOwner) { $allowedOwners += 'S-1-5-32-544' }");
    expect(String(args![0])).toContain("WindowsBuiltInRole]::Administrator");
    expect(String(args![0])).toContain("$prior.GetOwner([System.Security.Principal.SecurityIdentifier]).Value -notin $allowedOwners");
    expect(options!.env!.FABRIC_TEST_DIRECTORY).toBe(namespace.directory);
    expect(f.native.mock.calls.every(([file, args]) => !/diskpart|vdisk|format fs|assign letter/i.test(`${file} ${args}`))).toBe(true);
    expect(windowsDataRoot).toHaveBeenCalledExactlyOnceWith(namespace.directory, { private: true });
    namespace.close(); expect(f.remove).not.toHaveBeenCalled();
  });

  it("reuses the job directory only after a fresh ACL check and never repairs it", () => {
    const f = fixture(); const first = privateWindowsTestTemp();
    f.native.mockClear(); vi.mocked(windowsDataRoot).mockClear();
    const second = privateWindowsTestTemp();
    expect(second.directory).toBe(first.directory);
    expect(f.native).toHaveBeenCalledTimes(1); // known folder only, no ACL writes
    expect(windowsDataRoot).toHaveBeenCalledExactlyOnceWith(first.directory, { private: true });
    second.close(); expect(f.remove).not.toHaveBeenCalled();
  });

  it("separates CI jobs and attempts", () => {
    fixture(); const first = privateWindowsTestTemp();
    vi.stubEnv("GITHUB_RUN_ATTEMPT", "2");
    expect(privateWindowsTestTemp().directory).not.toBe(first.directory);
  });

  it("fails closed on unproven existing custody without repair or removal", () => {
    const f = fixture(); privateWindowsTestTemp(); f.native.mockClear();
    vi.mocked(windowsDataRoot).mockImplementation(() => { throw new Error("unsafe ACL"); });
    expect(() => privateWindowsTestTemp()).toThrow("unsafe ACL");
    expect(f.native).toHaveBeenCalledTimes(1); expect(f.remove).not.toHaveBeenCalled();
  });

  it("removes only its fresh directory on setup failure", () => {
    const f = fixture(); vi.mocked(windowsDataRoot).mockImplementation(() => { throw new Error("unsafe ACL"); });
    expect(() => privateWindowsTestTemp()).toThrow("unsafe ACL");
    expect(f.remove).toHaveBeenCalledExactlyOnceWith([...f.directories][0], expect.objectContaining({ recursive: true }));
  });

  it("collects a non-CI directory on close, but refuses a replaced identity", () => {
    const f = fixture(); vi.stubEnv("CI", undefined); const namespace = privateWindowsTestTemp();
    namespace.close(); expect(f.remove).toHaveBeenCalledOnce();
    vi.mocked(fs.lstatSync).mockReturnValue({ dev: 1, ino: 3, isDirectory: () => true, isSymbolicLink: () => false } as fs.Stats);
    expect(() => namespace.close()).toThrow("was replaced"); expect(f.remove).toHaveBeenCalledOnce();
  });
});
