import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { describe, expect, it, vi } from "vitest";
import { waitResidentChange } from "../src/residency/wake.js";
import { canonicalResidentWakeConfig } from "../src/residency/wake-index.js";

const exit = (child: ChildProcess) => new Promise<void>(resolve => {
  if (child.exitCode !== null || child.signalCode !== null) resolve();
  else child.once("exit", () => resolve());
});

describe("resident native startup outcomes", () => {
  it.skipIf(process.platform === "win32")("propagates a typed config refusal from the compiled wake launcher without starting an owner", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-config-native-"));
    const config = { rootId: "session:fenced", residencyRoot: root, cwd: root, actorRoot: path.join(root, "actors") };
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1, rootId: config.rootId,
      hostId: "host:fenced", configJson: canonicalResidentWakeConfig(config), actors: [] }));
    fs.writeFileSync(path.join(root, "config.json"), JSON.stringify({ ...config, actorRoot: path.join(root, "foreign-actors") }));
    const child = spawn(process.execPath, [path.resolve("dist/residency/launcher.js"), "--config", path.join(root, "config.json"), "--wake"],
      { stdio: ["ignore", "ignore", "ignore", "ipc"] });
    try {
      await expect(waitResidentChange(root, () => false, 10_000, "pending", child)).rejects.toMatchObject({
        code: "RESIDENT_WAKE_CONFIG_MISMATCH", root,
      });
      await exit(child);
      expect(child.exitCode).toBe(1);
      expect(JSON.parse(fs.readFileSync(path.join(root, "error.json"), "utf8"))).toMatchObject({ code: "RESIDENT_WAKE_CONFIG_MISMATCH" });
      expect(fs.existsSync(path.join(root, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(root, "foreign-actors"))).toBe(false);
    } finally { await exit(child); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.skipIf(process.platform === "win32")("freezes all config fields before a native wake without a full launch spec", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-config-frozen-"));
    const pi = path.join(root, "fake-pi.mjs");
    const configFile = path.join(root, "config.json");
    const report = path.join(root, "snapshot-report.json");
    const config = { rootId: "session:frozen", residencyRoot: root, cwd: root, actorRoot: path.join(root, "actors"), piBinary: pi };
    fs.writeFileSync(pi, `import fs from 'node:fs';
const file = process.env.PI_FABRIC_RESIDENT_CONFIG;
const before = JSON.parse(fs.readFileSync(file, 'utf8'));
fs.writeFileSync(${JSON.stringify(configFile)}, JSON.stringify({...before, actorRoot: '/changed-after-fence'}));
const after = JSON.parse(fs.readFileSync(file, 'utf8'));
fs.writeFileSync(${JSON.stringify(report)}, JSON.stringify({file, before, after}));
`);
    fs.writeFileSync(configFile, JSON.stringify(config));
    fs.writeFileSync(path.join(root, "wake-routes.json"), JSON.stringify({ format: 1, rootId: config.rootId,
      hostId: "host:frozen", configJson: canonicalResidentWakeConfig(config), actors: [] }));
    const child = spawn(process.execPath, [path.resolve("dist/residency/launcher.js"), "--config", configFile, "--wake"], { stdio: "ignore" });
    try {
      await exit(child);
      const receipt = JSON.parse(fs.readFileSync(report, "utf8"));
      expect(receipt.file).not.toBe(configFile);
      expect(receipt.before).toEqual(config);
      expect(receipt.after).toEqual(config);
      expect(JSON.parse(fs.readFileSync(configFile, "utf8")).actorRoot).toBe("/changed-after-fence");
    } finally { await exit(child); fs.rmSync(root, { recursive: true, force: true }); }
  });

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
      expect(ready).not.toHaveBeenCalled();
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
    // A receipt written before host startup completes must NOT hide a child failure.
    const ready = vi.fn(() => true);
    const started = performance.now();
    try {
      await expect(waitResidentChange(root, ready, 30_000, "pending", child)).rejects.toMatchObject({
        code: "RESIDENT_WAKE_STARTUP_FAILED", root, message: expect.stringContaining("17"),
      });
      expect(performance.now() - started).toBeLessThan(5_000);
      expect(ready).not.toHaveBeenCalled();
      expect(child.listenerCount("message")).toBe(0);
      expect(watcher.close).toHaveBeenCalledOnce();
    } finally { await exit(child); vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("uses one direct deadline read as a fallback when a child ready message is lost", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-child-reread-"));
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
    const child = Object.assign(new EventEmitter(), { exitCode: null, signalCode: null }) as unknown as ChildProcess;
    vi.spyOn(fs, "watch").mockReturnValue(watcher);
    const ready = vi.fn(() => true);
    try {
      await waitResidentChange(root, ready, 10, "pending", child);
      expect(ready).toHaveBeenCalledOnce();
      expect(child.listenerCount("message")).toBe(0);
      expect(watcher.close).toHaveBeenCalledOnce();
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does exactly one final re-read and retains typed pending when the owned child has not signaled", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-wake-child-pending-"));
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
    const child = Object.assign(new EventEmitter(), { exitCode: null, signalCode: null }) as unknown as ChildProcess;
    vi.spyOn(fs, "watch").mockReturnValue(watcher);
    const ready = vi.fn(() => false);
    try {
      await expect(waitResidentChange(root, ready, 10, "pending", child)).rejects.toMatchObject({ code: "RESIDENT_WAKE_PENDING", root });
      expect(ready).toHaveBeenCalledOnce();
      expect(child.listenerCount("exit")).toBe(0);
      expect(child.listenerCount("message")).toBe(0);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
