import * as childProcess from "node:child_process";
import fs from "node:fs";
import { EventEmitter } from "node:events";
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

describe("Windows lockFile validation", () => {
  it.each([true, false])("uses Windows file access rather than an unavailable POSIX UID, but still checks regular files (regular=%s)", async regular => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-windows-file-"));
    const file = path.join(root, "host-fence-establish.lock");
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const getuid = Object.getOwnPropertyDescriptor(process, "getuid");
    Object.defineProperty(process, "getuid", { configurable: true, writable: true, value: undefined });
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number) => {
      const stat = fstat(fd); stat.uid = 0; stat.isFile = () => regular; return stat;
    }) as typeof fs.fstatSync);
    const spawn = vi.mocked(childProcess.spawn).mockImplementation(() => {
      const child = new EventEmitter();
      queueMicrotask(() => child.emit("exit", 0));
      return child as childProcess.ChildProcess;
    });
    spawn.mockClear();
    const close = vi.spyOn(fs, "closeSync");
    let fd: number | undefined;
    try {
      if (regular) {
        fd = await lockFile(file, 0);
        expect(spawn).toHaveBeenCalledOnce();
        expect(spawn.mock.calls[0]?.[0]).toBe("flock");
        expect(close).not.toHaveBeenCalled(); // caller still owns the lock descriptor
      } else {
        await expect(lockFile(file, 0)).rejects.toThrow(/not a regular file owned/);
        expect(spawn).not.toHaveBeenCalled();
        expect(close).toHaveBeenCalledOnce();
      }
    } finally {
      if (fd !== undefined) fs.closeSync(fd);
      if (getuid) Object.defineProperty(process, "getuid", getuid);
      else Reflect.deleteProperty(process, "getuid");
      fs.rmSync(root, { recursive: true, force: true });
    }
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

  it.skipIf(process.platform !== "linux")("product Windows start with no POSIX UID fails closed before managers and owner publication", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-windows-unsupported-"));
    const config: ResidentHostConfig = {
      format: 1, rootId: "session:unsupported", sessionId: "unsupported", cwd: root, projectRoot: root,
      meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"),
      fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, mesh: DEFAULT_FABRIC_CONFIG.mesh,
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: "unused", fabricExtensionPath: "unused", piBinary: "unused", claudeBinary: "unused", vedaBinary: "unused",
    };
    const host = new ResidentHost(config);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const getuid = Object.getOwnPropertyDescriptor(process, "getuid")!;
    Object.defineProperty(process, "getuid", { configurable: true, writable: true, value: undefined });
    const fstat = fs.fstatSync.bind(fs);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number) => {
      const stat = fstat(fd); stat.uid = 0; return stat;
    }) as typeof fs.fstatSync);
    const spawn = vi.mocked(childProcess.spawn).mockImplementation(() => {
      throw Object.assign(new Error("flock unavailable"), { code: "ENOENT" });
    });
    try {
      await expect(host.start()).rejects.toThrow("flock unavailable");
      expect(spawn).toHaveBeenCalledOnce();
      expect(spawn.mock.calls[0]?.[0]).toBe("flock");
      expect(host.actors).toBeUndefined();
      expect(host.agents).toBeUndefined();
      expect(fs.existsSync(path.join(config.residencyRoot, "owner.json"))).toBe(false);
    } finally {
      await host.close();
      Object.defineProperty(process, "getuid", getuid);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
