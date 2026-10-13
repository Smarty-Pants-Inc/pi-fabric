import * as childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { FileLockBusy, lockFile } from "../src/residency/file-lock.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";
import type { ResidentHostConfig } from "../src/residency/protocol.js";
vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});
afterEach(() => { vi.restoreAllMocks(); vi.mocked(childProcess.spawn).mockReset(); });

describe("lockFile ownership across platforms", () => {
  it.skipIf(process.platform !== "linux")("retains the real shared fence for a verified POSIX owner", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-posix-uid-"));
    const file = path.join(root, "host-fence-establish.lock");
    let first: number | undefined;
    let successor: number | undefined;
    try {
      first = await lockFile(file, 0);
      const inode = fs.statSync(file).ino;
      await expect(lockFile(file, 0)).rejects.toBeInstanceOf(FileLockBusy);
      fs.closeSync(first); first = undefined;
      successor = await lockFile(file, 0);
      expect(fs.statSync(file).ino).toBe(inode);
    } finally {
      if (first !== undefined) fs.closeSync(first);
      if (successor !== undefined) fs.closeSync(successor);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.skipIf(process.getuid === undefined)("rejects a foreign POSIX owner before spawning the fence helper", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-foreign-uid-"));
    const file = path.join(root, "host-fence-establish.lock");
    fs.writeFileSync(file, "");
    vi.spyOn(process, "getuid").mockReturnValue(fs.statSync(file).uid + 1);
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    const close = vi.spyOn(fs, "closeSync");
    try {
      await expect(lockFile(file, 0)).rejects.toThrow(/not a regular file owned/);
      expect(spawn).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledOnce();
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});

describe("lockFile safety without POSIX UIDs", () => {
  it.each([true, false])("rejects unknown POSIX ownership and closes its descriptor before invoking the fence (regular=%s)", async regular => {
    vi.spyOn(process, "platform", "get").mockReturnValue("linux");
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-nonregular-"));
    const file = path.join(root, "host-fence-establish.lock");
    const getuid = Object.getOwnPropertyDescriptor(process, "getuid");
    Object.defineProperty(process, "getuid", { configurable: true, writable: true, value: undefined });
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number) => {
      const stat = fstat(fd); stat.uid = 0; stat.isFile = () => regular; return stat;
    }) as typeof fs.fstatSync);
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    const close = vi.spyOn(fs, "closeSync");
    try {
      await expect(lockFile(file, 0)).rejects.toThrow(/not a regular file owned/);
      expect(spawn).not.toHaveBeenCalled();
      expect(close).toHaveBeenCalledOnce();
    } finally {
      if (getuid) Object.defineProperty(process, "getuid", getuid);
      else Reflect.deleteProperty(process, "getuid");
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});

describe("Windows lockFile boundary", () => {
  it("never spawns flock or creates a POSIX lock on Windows", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-windows-file-"));
    const file = path.join(root, "host-fence-establish.lock");
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const spawn = vi.mocked(childProcess.spawn);
    spawn.mockClear();
    try {
      await expect(lockFile(file, 0)).rejects.toThrow(/POSIX flock is unavailable on Windows/);
      expect(spawn).not.toHaveBeenCalled();
      expect(fs.existsSync(file)).toBe(false);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
describe("unsupported native residency versus in-process test contract", () => {
  it("explicit test fence excludes a second owner until exact descriptor close and never replaces the inode", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-test-fence-"));
    const file = path.join(root, "host.lock");
    installInProcessResidentFence(true);
    let first: number | undefined;
    let following: number | undefined;
    try {
      first = await lockFile(file, 0);
      const inode = fs.statSync(file).ino;
      await expect(lockFile(file, 0)).rejects.toBeInstanceOf(FileLockBusy);
      fs.closeSync(first); first = undefined;
      following = await lockFile(file, 0);
      expect(fs.statSync(file).ino).toBe(inode);
    } finally {
      if (first !== undefined) fs.closeSync(first);
      if (following !== undefined) fs.closeSync(following);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("Windows host fence excludes a second holder without POSIX UIDs or spawning flock, then admits a successor", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-windows-claim-"));
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:windows", sessionId: "windows", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const first = new ResidentHost(config);
    const second = new ResidentHost(config);
    const successor = new ResidentHost(config);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const getuid = Object.getOwnPropertyDescriptor(process, "getuid");
    Object.defineProperty(process, "getuid", { configurable: true, writable: true, value: undefined });
    const spawn = vi.mocked(childProcess.spawn).mockImplementation(() => {
      throw Object.assign(new Error("flock must never spawn on Windows"), { code: "ENOENT" });
    });
    const lock = path.join(config.residencyRoot, "host.lock");
    try {
      const starts = await Promise.allSettled([first.start(), second.start()]);
      expect(starts[0]!.status).toBe("fulfilled");
      expect(starts[1]!.status).toBe("rejected");
      if (starts[1]!.status === "rejected") expect(starts[1]!.reason.constructor.name).toBe("ResidentHostAlreadyRunning");
      expect(second.actors).toBeUndefined();
      expect(second.agents).toBeUndefined();
      expect(spawn).not.toHaveBeenCalled();
      expect(fs.existsSync(path.join(config.residencyRoot, "host-fence-establish.lock"))).toBe(false);
      expect(fs.existsSync(path.join(config.residencyRoot, "host-fence.json"))).toBe(false);
      const claim = fs.readFileSync(lock, "utf8");
      await second.close();
      expect(fs.readFileSync(lock, "utf8")).toBe(claim);
      await first.close();
      expect(fs.existsSync(lock)).toBe(false);
      await successor.start();
      expect(spawn).not.toHaveBeenCalled();
      await successor.close();
      expect(fs.existsSync(lock)).toBe(false);
    } finally {
      await Promise.all([first.close(), second.close(), successor.close()]);
      if (getuid) Object.defineProperty(process, "getuid", getuid);
      else Reflect.deleteProperty(process, "getuid");
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["", "{", "null", JSON.stringify({ pid: -1, token: "legacy-dead" })])(
    "Windows host refuses an uncertain existing claim (%j) instead of reclaiming it", async record => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-windows-legacy-"));
      const config: ResidentHostConfig = {
        format: 1, rootId: "session:windows", sessionId: "windows", cwd: root, projectRoot: root,
        meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
        fullCodeMode: false, agents: DEFAULT_FABRIC_CONFIG.agents, mesh: DEFAULT_FABRIC_CONFIG.mesh,
        retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
      };
      fs.mkdirSync(config.residencyRoot);
      const lock = path.join(config.residencyRoot, "host.lock");
      fs.writeFileSync(lock, record);
      const host = new ResidentHost(config);
      vi.spyOn(process, "platform", "get").mockReturnValue("win32");
      const spawn = vi.mocked(childProcess.spawn);
      spawn.mockClear();
      try {
        await expect(host.start()).rejects.toThrow(/legacy\/uncertain.*verify drain/);
        expect(host.actors).toBeUndefined();
        expect(host.agents).toBeUndefined();
        expect(fs.readFileSync(lock, "utf8")).toBe(record);
        expect(spawn).not.toHaveBeenCalled();
        expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
      } finally { await host.close(); fs.rmSync(root, { recursive: true, force: true }); }
    },
  );
});
