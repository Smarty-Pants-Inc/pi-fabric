import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import { waitResidentChange } from "../src/residency/wake.js";

const exit = (child: ChildProcess) => new Promise<void>(resolve => {
  if (child.exitCode !== null || child.signalCode !== null) resolve();
  else child.once("exit", () => resolve());
});

describe("resident native startup outcomes", () => {
  it.each(["silent", "throw", "error"] as const)("uses child IPC ready when the filesystem watcher is %s", async mode => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-child-ready-"));
    const close = vi.fn();
    const watcher = Object.assign(new EventEmitter(), { close }) as unknown as fs.FSWatcher;
    vi.spyOn(fs, "watch").mockImplementation(() => {
      if (mode === "throw") throw new Error("inotify exhausted");
      if (mode === "error") queueMicrotask(() => watcher.emit("error", new Error("network fs")));
      return watcher;
    });
    const interval = vi.spyOn(globalThis, "setInterval");
    const ready = vi.fn(() => false);
    const child = spawn(process.execPath, ["-e", `process.send({ event: 'resident-ready', root: ${JSON.stringify(root)}, token: 'owned' }); process.disconnect();`],
      { stdio: ["ignore", "ignore", "ignore", "ipc"] });
    try {
      await waitResidentChange(root, ready, 5_000, "pending", child);
      expect(ready).toHaveBeenCalledTimes(mode === "throw" ? 0 : 1);
      expect(interval).not.toHaveBeenCalled();
      expect(child.listenerCount("message")).toBe(0);
      expect(close).toHaveBeenCalledTimes(mode === "throw" ? 0 : 1);
    } finally {
      if (child.connected) child.disconnect();
      await exit(child);
      vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it("learns early native child exit immediately, even with a silent watcher", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-child-exit-"));
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
    vi.spyOn(fs, "watch").mockReturnValue(watcher);
    const child = spawn(process.execPath, ["-e", "process.exit(17)"], { stdio: ["ignore", "ignore", "ignore", "ipc"] });
    const ready = vi.fn(() => false);
    const started = performance.now();
    try {
      await expect(waitResidentChange(root, ready, 30_000, "pending", child)).rejects.toMatchObject({
        code: "RESIDENT_WAKE_STARTUP_FAILED", root, message: expect.stringContaining("17"),
      });
      expect(performance.now() - started).toBeLessThan(5_000);
      expect(ready).toHaveBeenCalledTimes(2);
      expect(child.listenerCount("message")).toBe(0);
      expect(watcher.close).toHaveBeenCalledOnce();
    } finally { await exit(child); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does exactly one final re-read and retains typed pending when the owned child has not signaled", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-child-pending-"));
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
    const child = Object.assign(new EventEmitter(), { exitCode: null, signalCode: null }) as unknown as ChildProcess;
    vi.spyOn(fs, "watch").mockReturnValue(watcher);
    const ready = vi.fn(() => false);
    try {
      await expect(waitResidentChange(root, ready, 10, "pending", child)).rejects.toMatchObject({ code: "RESIDENT_WAKE_PENDING", root });
      expect(ready).toHaveBeenCalledTimes(2);
      expect(child.listenerCount("exit")).toBe(0);
      expect(child.listenerCount("message")).toBe(0);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
