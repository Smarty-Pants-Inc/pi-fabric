import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  forgetWindowsMeshCapability, prepareWindowsMeshCapability, releaseWindowsMeshLock, windowsMeshReleaseTestHooks, type WindowsMeshCapability,
} from "../src/storage/windows-mesh-release.js";

// Keep: data safety at the native trust boundary. Recorded filesystem/child receipts exercise the real wrapper,
// not NTFS: no file is written and no child starts. Hosted kill/share evidence remains in the Windows-only suite.
const { bootstrapMock } = vi.hoisted(() => ({ bootstrapMock: vi.fn() }));
vi.mock("../src/storage/verified-remove.js", () => ({ windowsHelperBootstrap: bootstrapMock }));
vi.mock("node:child_process", async original => ({ ...await original<typeof import("node:child_process")>(), spawnSync: vi.fn() }));
vi.mock("node:path", async original => {
  const actual = await original<typeof import("node:path")>();
  return { ...actual, default: actual.win32 };
});

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const lock = "C:\\recorded\\.lock";
const originalRecord = "11111111-2222-3333-4444-555555555555\n42\n1\n";
const bytes = Buffer.from(originalRecord);
const executable = Buffer.from("recorded exact executable");
const capability: WindowsMeshCapability = {
  absolutePath: "C:\\private-cache\\helper.exe", digest: createHash("sha256").update(executable).digest("hex"), meshAbi: 1,
};
const stat = (directory: boolean) => ({
  dev: 1n, ino: directory ? 2n : 3n, mtimeNs: 100n, size: BigInt(bytes.length),
  isSymbolicLink: () => false, isDirectory: () => directory, isFile: () => !directory,
});
const absent = () => { throw Object.assign(new Error("absent"), { code: "ENOENT" }); };
const receipt = (stdout: string, status = 0) => ({ pid: 17, status, signal: null, stdout: Buffer.from(stdout), stderr: Buffer.alloc(0) });
const childMock = vi.mocked(spawnSync);
const queue = (...results: ReturnType<typeof receipt>[]) => {
  for (const result of results) childMock.mockReturnValueOnce(result as ReturnType<typeof spawnSync>);
};
const requests = (): Buffer[] => childMock.mock.calls.map(call => (call[2] as { input: Buffer }).input);
const originalField = (message: Buffer): string => {
  let at = 5;
  for (let field = 0; field < 10; field++) {
    const length = parseInt(message.subarray(at, at + 8).toString("ascii"), 16);
    at += 8;
    if (field === 8) return message.subarray(at, at + length).toString("utf8");
    at += length;
  }
  throw new Error("record field absent");
};

beforeEach(() => {
  forgetWindowsMeshCapability();
  bootstrapMock.mockReturnValue({ file: capability.absolutePath, digest: capability.digest, bytes: executable });
  Object.defineProperty(process, "platform", { ...platform, value: "win32" });
  vi.spyOn(os, "type").mockReturnValue("Windows_NT");
  vi.spyOn(fs.realpathSync, "native").mockReturnValue("C:\\recorded");
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
    if (String(file) === capability.absolutePath) return { isFile: () => true, isSymbolicLink: () => false };
    if (String(file).includes("\\r.")) return absent();
    return stat(String(file) === lock);
  }) as typeof fs.lstatSync);
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor) =>
    String(file) === capability.absolutePath ? executable : bytes) as typeof fs.readFileSync);
  vi.spyOn(fs, "readdirSync").mockReturnValue(["owner"] as never);
  childMock.mockReset();
});
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  forgetWindowsMeshCapability();
  childMock.mockReset();
  for (const key of Object.keys(windowsMeshReleaseTestHooks)) delete (windowsMeshReleaseTestHooks as Record<string, unknown>)[key];
});

describe("Windows release unknown reconciliation, recorded calls (#9569 T4)", () => {
  it("unknown then refusal stays unknown with both histories; explicit same-record retry releases only", () => {
    queue(receipt("", 124), receipt("FMR1 refused owner record-unreadable\n", 3));
    let operationCalls = 0;
    const operation = () => { operationCalls++; return "already completed"; };
    expect(operation()).toBe("already completed");
    const retryRelease = () => releaseWindowsMeshLock(lock, originalRecord, capability);
    const result = retryRelease();
    expect(result).toMatchObject({ outcome: "release-outcome-unknown", reason: expect.stringContaining("reconciliation: refused-before-retirement") });
    expect(result.history).toEqual([
      expect.stringContaining("attempt 1: release-outcome-unknown"),
      expect.stringContaining("attempt 2: refused-before-retirement (owner: record-unreadable)"),
    ]);
    expect(childMock).toHaveBeenCalledTimes(2); // no third child and no implicit application replay
    expect(requests().map(originalField)).toEqual([originalRecord, originalRecord]);
    queue(receipt("FMR1 released cleanup clean\n"));
    expect(retryRelease()).toMatchObject({ outcome: "released", cleanup: "clean" });
    expect(childMock).toHaveBeenCalledTimes(3); // explicit retry, not part of the bounded reconciliation
    expect(requests().map(originalField)).toEqual([originalRecord, originalRecord, originalRecord]);
    expect(operationCalls).toBe(1);
  });

  it.each([
    ["FMR1 not-owner guard absent\n", 5, "not-owner"],
    ["FMR1 released cleanup clean\n", 0, "released"],
    ["FMR1 released cleanup pending-record-unreadable\n", 0, "released"],
  ] as const)("unknown can reconcile only to a canonical-safe terminal receipt: %s", (line, code, outcome) => {
    queue(receipt("", 124), receipt(line, code));
    const result = releaseWindowsMeshLock(lock, originalRecord, capability);
    expect(result.outcome).toBe(outcome);
    expect(result.history).toEqual(expect.arrayContaining([
      expect.stringContaining("attempt 1: release-outcome-unknown"), `attempt 2: ${outcome}`,
    ]));
    expect(childMock).toHaveBeenCalledTimes(2);
    expect(requests().map(originalField)).toEqual([originalRecord, originalRecord]);
  });

  it.each([
    ["", 124], ["FMR1 not-owner cleanup absent\n", 5], ["FMR1 refused cleanup x\n", 3],
  ] as const)("another unknown/invalid second receipt retains uncertainty and the two-child bound: %s", (line, code) => {
    queue(receipt("", 124), receipt(line, code));
    const result = releaseWindowsMeshLock(lock, originalRecord, capability);
    expect(result.outcome).toBe("release-outcome-unknown");
    expect(result.history).toHaveLength(2);
    expect(result.history?.[1]).toContain("attempt 2: release-outcome-unknown");
    expect(childMock).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["FMR1 released cleanup pending-unrecognized\n", 0],
    ["FMR1 not-owner guard unrecognized\n", 5],
    ["FMR1 not-owner owner unrecognized\n", 5],
  ] as const)("CR-A-01: actual wrapper unknown + invalid success cannot resolve reconciliation: %s", (line, code) => {
    queue(receipt("", 124), receipt(line, code));
    const result = releaseWindowsMeshLock(lock, originalRecord, capability);
    expect(result).toMatchObject({ outcome: "release-outcome-unknown", reason: expect.stringContaining("reconciliation: release-outcome-unknown") });
    expect(result.history).toEqual([
      expect.stringContaining("attempt 1: release-outcome-unknown"),
      expect.stringContaining("attempt 2: release-outcome-unknown"),
    ]);
    expect(childMock).toHaveBeenCalledTimes(2);
    expect(requests().map(originalField)).toEqual([originalRecord, originalRecord]);
  });

  it("not-started reconciliation cannot erase an earlier unknown", () => {
    queue(receipt("", 124));
    childMock.mockReturnValueOnce({ pid: 0, status: null, signal: null, stdout: Buffer.alloc(0), stderr: Buffer.alloc(0), output: [null, null, null],
      error: Object.assign(new Error("missing"), { code: "ENOENT" }) } as ReturnType<typeof spawnSync>);
    const result = releaseWindowsMeshLock(lock, originalRecord, capability);
    expect(result).toMatchObject({ outcome: "release-outcome-unknown", reason: expect.stringContaining("helper-unavailable/not-started") });
    expect(result.history?.[1]).toContain("attempt 2: helper-unavailable/not-started");
    expect(childMock).toHaveBeenCalledTimes(2);
  });

  it("refuses an oversized logical release path before a native child or mutation", () => {
    const root = `C:\\${"界".repeat(162)}\u{1F600}`; // 167 units; retired owner would be 240
    vi.mocked(fs.realpathSync.native).mockReturnValue(root);
    const longLock = `${root}\\.lock`;
    vi.mocked(fs.lstatSync).mockImplementation(((file: fs.PathLike) => {
      if (String(file) === capability.absolutePath) return { isFile: () => true, isSymbolicLink: () => false };
      if (String(file).includes("\\r.")) return absent();
      return stat(String(file) === longLock);
    }) as typeof fs.lstatSync);
    expect(releaseWindowsMeshLock(longLock, originalRecord, capability).outcome).toBe("refused-before-retirement");
    expect(childMock).not.toHaveBeenCalled();
  });

  it("rejects an embedded NUL in the original record without a child", () => {
    expect(releaseWindowsMeshLock(lock, "full-uuid\0suffix", capability).outcome).toBe("refused-before-retirement");
    expect(childMock).not.toHaveBeenCalled();
  });

  it("strips inherited test-only environment before a production release child", () => {
    const key = "FABRIC_WIN_MESH_TEST_STAGING";
    const old = process.env[key];
    process.env[key] = "untrusted-inherited-hook";
    try {
      queue(receipt("FMR1 released cleanup clean\n"));
      expect(releaseWindowsMeshLock(lock, originalRecord, capability).outcome).toBe("released");
      expect((childMock.mock.calls[0]?.[2] as { env: NodeJS.ProcessEnv }).env).not.toHaveProperty(key);
    } finally {
      if (old === undefined) delete process.env[key]; else process.env[key] = old;
    }
  });
});

describe("Windows retained cache location policy, recorded preparation (#9569 H2)", () => {
  const local = "C:\\Users\\me\\AppData\\Local";
  const temp = `${local}\\Temp`;
  beforeEach(() => {
    vi.stubEnv("LOCALAPPDATA", local);
    vi.stubEnv("TEMP", "D:\\Temp");
    vi.stubEnv("TMP", "E:\\Temp");
    vi.spyOn(os, "tmpdir").mockReturnValue(temp);
    vi.mocked(fs.realpathSync.native).mockImplementation(((file: fs.PathLike) => String(file)) as typeof fs.realpathSync.native);
    vi.mocked(fs.lstatSync).mockImplementation(((file: fs.PathLike) => ({
      isSymbolicLink: () => false, isDirectory: () => !String(file).endsWith(".exe"), isFile: () => String(file).endsWith(".exe"),
    })) as typeof fs.lstatSync);
    vi.mocked(fs.readFileSync).mockReturnValue(executable as never);
    // No fixture filesystem writes/deletes or real children, even when probing the old implementation.
    vi.spyOn(fs, "mkdirSync").mockReturnValue(undefined);
    for (const method of ["writeFileSync", "renameSync", "unlinkSync", "rmdirSync"] as const) {
      vi.spyOn(fs, method).mockImplementation(() => { throw new Error(`unexpected recorded mutation: ${method}`); });
    }
    queue(receipt("ok private\n"), receipt("FMRCAP1 mesh-retire-v1 abi=1\n"));
  });

  it.each([
    ["exact os.tmpdir", temp, {}],
    ["child of os.tmpdir", `${temp}\\cache`, {}],
    ["case-insensitive child", `${temp.toUpperCase()}\\cache`, {}],
    ["namespaced exact temp", `\\\\?\\${temp}`, {}],
    ["LOCALAPPDATA alias to temp", "C:\\Alias", { "C:\\Alias": temp }],
    ["TEMP alias to selected base", "D:\\Temp", { "D:\\Temp": "C:\\ResolvedTemp" }],
    ["exact TMP independently of os.tmpdir and TEMP", "E:\\Temp", {}],
    ["child of TEMP independently of os.tmpdir", "D:\\Temp\\private", {}],
  ] as const)("CR-A-02: refuses %s before any cache creation/publication", (_name, selected, aliases) => {
    vi.stubEnv("LOCALAPPDATA", selected);
    vi.mocked(fs.realpathSync.native).mockImplementation(((file: fs.PathLike) =>
      (aliases as Readonly<Record<string, string>>)[String(file)] ?? String(file)) as typeof fs.realpathSync.native);
    expect(() => prepareWindowsMeshCapability()).toThrow(/must not be in a temporary location/);
    expect(fs.mkdirSync).not.toHaveBeenCalled();
    expect(fs.writeFileSync).not.toHaveBeenCalled();
    expect(fs.renameSync).not.toHaveBeenCalled();
    expect(fs.unlinkSync).not.toHaveBeenCalled();
    expect(fs.rmdirSync).not.toHaveBeenCalled();
    expect(childMock).not.toHaveBeenCalled();
  });

  it("CR-A-02: resolves a missing suffix below an existing LOCALAPPDATA alias before refusing", () => {
    vi.stubEnv("LOCALAPPDATA", "C:\\Alias\\not-created");
    vi.mocked(fs.realpathSync.native).mockImplementation(((file: fs.PathLike) => {
      if (String(file) === "C:\\Alias\\not-created") return absent();
      return String(file) === "C:\\Alias" ? temp : String(file);
    }) as typeof fs.realpathSync.native);
    expect(() => prepareWindowsMeshCapability()).toThrow(/must not be in a temporary location/);
    expect(fs.mkdirSync).not.toHaveBeenCalled();
    expect(childMock).not.toHaveBeenCalled();
  });

  it("CR-A-02: canonicalizes a temp alias even when LOCALAPPDATA uses the real destination", () => {
    vi.stubEnv("LOCALAPPDATA", `${temp}\\cache`);
    vi.stubEnv("TEMP", "C:\\TempAlias");
    vi.spyOn(os, "tmpdir").mockReturnValue("D:\\OtherTemp");
    vi.mocked(fs.realpathSync.native).mockImplementation(((file: fs.PathLike) =>
      String(file) === "C:\\TempAlias" ? temp : String(file)) as typeof fs.realpathSync.native);
    expect(() => prepareWindowsMeshCapability()).toThrow(/must not be in a temporary location/);
    expect(fs.mkdirSync).not.toHaveBeenCalled();
    expect(childMock).not.toHaveBeenCalled();
  });

  it("CR-A-02: rejects the actual cache below TEMP even when LOCALAPPDATA is its parent", () => {
    vi.stubEnv("TEMP", path.join(local, "pi-fabric"));
    expect(() => prepareWindowsMeshCapability()).toThrow(/must not be in a temporary location/);
    expect(fs.mkdirSync).not.toHaveBeenCalled();
    expect(childMock).not.toHaveBeenCalled();
  });

  it.each([local, `${temp}-other`])("CR-A-02: permits parent Local and prefix-lookalike outside Temp: %s", selected => {
    vi.stubEnv("LOCALAPPDATA", selected);
    const result = prepareWindowsMeshCapability();
    const base = path.join(selected, "pi-fabric", "native");
    expect(result).toEqual({ absolutePath: path.join(base, capability.digest, "fabric-win-remove.exe"), digest: capability.digest, meshAbi: 1 });
    expect(fs.mkdirSync).toHaveBeenCalledWith(base, { recursive: true });
    expect(childMock.mock.calls.map(call => call[1]?.[0])).toEqual(["--cache-private-v1", "--capability-v1"]);
    expect(fs.writeFileSync).not.toHaveBeenCalled();
  });

  it("CR-A-02: keeps explicit in-process cacheRoot available as a private temp fixture override", () => {
    vi.stubEnv("LOCALAPPDATA", temp);
    windowsMeshReleaseTestHooks.cacheRoot = `${temp}\\fixture-cache`;
    const result = prepareWindowsMeshCapability();
    expect(result.absolutePath).toBe(path.join(windowsMeshReleaseTestHooks.cacheRoot, capability.digest, "fabric-win-remove.exe"));
    expect(childMock.mock.calls.map(call => call[1]?.[0])).toEqual(["--cache-private-v1", "--capability-v1"]);
  });
});
