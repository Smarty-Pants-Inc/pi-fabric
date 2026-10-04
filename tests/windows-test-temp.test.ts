import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { privateWindowsTestTemp, WINDOWS_TEST_VOLUME_MAX_MIB } from "../scripts/windows-test-temp.js";
import { windowsDataRoot } from "../src/storage/windows-temp-root.js";

vi.mock("../src/storage/windows-temp-root.js", () => ({ windowsDataRoot: vi.fn() }));
vi.mock("../src/storage/windows-powershell.js", () => ({
  windowsSecurityPowerShell: (source: string, env: NodeJS.ProcessEnv) => ({ file: "powershell.exe", args: [source], env }),
}));

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); vi.mocked(windowsDataRoot).mockClear(); });

// Native mounting and ACL inspection remain Windows gates. These tests execute
// the real provisioning/reuse code on Linux, simulating only filesystem/native IO.
const fixture = () => {
  vi.stubEnv("CI", "1");
  vi.stubEnv("RUNNER_TEMP", path.resolve("runner-temp"));
  vi.stubEnv("SystemRoot", path.resolve("windows"));
  const backing = path.join(process.env.RUNNER_TEMP!, "fabric-private-test-volume");
  const vhd = path.join(backing, "tests.vhd");
  const manifest = path.join(backing, "namespace.json");
  // The real launcher uses the host path API; emulate its Windows volume
  // joins without changing the Linux runner's ordinary fixture/backing paths.
  const join = path.join, windowsJoin = path.win32.join;
  vi.spyOn(path, "join").mockImplementation((...parts) => parts[0]?.[1] === ":" ? windowsJoin(...parts) : join(...parts));
  const files = new Map<string, string>();
  const directories = new Set<string>();
  vi.spyOn(fs, "existsSync").mockImplementation(file => files.has(String(file)) || directories.has(String(file)));
  vi.spyOn(fs, "mkdirSync").mockImplementation(file => { directories.add(String(file)); return undefined; });
  vi.spyOn(fs, "writeFileSync").mockImplementation((file, text) => { files.set(String(file), String(text)); });
  vi.spyOn(fs, "readFileSync").mockImplementation(file => files.get(String(file)) as never);
  const remove = vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  const commands: string[] = [];
  const native = vi.spyOn(childProcess, "execFileSync").mockImplementation((file, args) => {
    if (String(file).endsWith("diskpart.exe")) {
      const source = files.get(String(args![1]))!;
      commands.push(source);
      files.set(vhd, "virtual disk");
      const letter = source.match(/assign letter=([A-Z])/)?.[1];
      if (letter) directories.add(`${letter}:\\`);
      return "DiskPart successfully executed" as never;
    }
    return (String(args![0]).includes("GetLogicalDrives") ? '["C:\\\\","Z:\\\\"]' : "") as never;
  });
  return { vhd, manifest, files, directories, commands, native, remove };
};

describe("private Windows test volume capacity and lifecycle", () => {
  it("provisions an expandable 64 GiB NTFS disk, leaving headroom above the unchanged 8 GB fixture", () => {
    const f = fixture();
    const namespace = privateWindowsTestTemp();
    expect(WINDOWS_TEST_VOLUME_MAX_MIB).toBe(65_536);
    expect(WINDOWS_TEST_VOLUME_MAX_MIB * 1024 ** 2).toBeGreaterThan(8_000_000_000 * 2);
    expect(f.commands).toEqual([expect.stringContaining(`create vdisk file="${f.vhd}" maximum=65536 type=expandable\r\n`)]);
    expect(f.commands[0]).toContain('format fs=ntfs label="fabric-private-tests" quick');
    expect(f.commands[0]).toContain("assign letter=Y");
    expect(f.native.mock.calls.some(([, args]) => String(args![0]).includes("Set-Acl -LiteralPath"))).toBe(true);
    expect(windowsDataRoot).toHaveBeenCalledWith("Y:\\", { private: true });
    expect(windowsDataRoot).toHaveBeenCalledWith(namespace.directory, { private: true });
    expect(JSON.parse(f.files.get(f.manifest)!)).toEqual({ volume: "Y:\\", vhd: f.vhd, directory: namespace.directory });
    namespace.close();
    expect(f.commands).toHaveLength(1);
    expect(f.remove).not.toHaveBeenCalled(); // the job owns the shared MSYS mount
  });

  it("reuses the job volume only after rechecking private directory custody", () => {
    const f = fixture();
    const first = privateWindowsTestTemp();
    f.native.mockClear();
    vi.mocked(windowsDataRoot).mockClear();
    const second = privateWindowsTestTemp();
    expect(second.directory).toBe(first.directory);
    expect(windowsDataRoot).toHaveBeenCalledExactlyOnceWith(first.directory, { private: true });
    expect(f.native).not.toHaveBeenCalled();
    second.close();
    expect(f.remove).not.toHaveBeenCalled();
  });

  it("refuses unconfirmed disks and receipts outside the test-owned namespace", () => {
    const f = fixture();
    f.files.set(f.vhd, "old disk");
    expect(() => privateWindowsTestTemp()).toThrow("Unconfirmed private test volume setup");
    f.files.set(f.manifest, JSON.stringify({ volume: "Y:\\", vhd: "foreign.vhd", directory: "Y:\\tmp" }));
    expect(() => privateWindowsTestTemp()).toThrow("Invalid private test volume receipt");
    expect(f.native).not.toHaveBeenCalled();
    expect(f.remove).not.toHaveBeenCalled();
  });
});
