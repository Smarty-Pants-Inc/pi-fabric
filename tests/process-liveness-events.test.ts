import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentTransportHandle } from "../src/agents/types.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  vi.useRealTimers();
  for (const manager of managers.splice(0)) await manager.close().catch(() => undefined);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const root = () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-event-liveness-"));
  roots.push(directory);
  return directory;
};
const tick = () => new Promise<void>(resolve => setImmediate(resolve));
const deferred = () => {
  let resolve!: () => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
};

async function nativeWorker(run: (handle: AgentTransportHandle, child: ChildProcess) => Promise<void>) {
  const directory = root();
  const workerPath = path.join(directory, "worker.mjs");
  fs.writeFileSync(workerPath, "setInterval(() => {}, 1000);\n");
  const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
  let child!: ChildProcess;
  vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => { child = actual.spawn(...args); return child; });
  const handle = await new ProcessTransport().launch({ id: "events", name: "events", cwd: directory, workerPath, workerArguments: [] });
  try { await run(handle, child); }
  finally { await handle.stop(); await handle.waitForClose?.(); }
}

describe("native process event liveness", () => {
  it("arms no recurring liveness timer for an active process transport", async () => {
    const interval = vi.spyOn(globalThis, "setInterval");
    const timeout = vi.spyOn(globalThis, "setTimeout");
    await nativeWorker(async handle => {
      expect(handle.liveness).toBe("events");
      expect(handle.livenessPollIntervalMs).toBeUndefined();
      expect(interval).not.toHaveBeenCalled();
      expect(timeout).not.toHaveBeenCalled();
      expect(await handle.isAlive()).toBe(true);
    });
  });

  it("latches exit/close by the next tick, without a timer or numeric-PID re-probe", async () => {
    await nativeWorker(async (handle, child) => {
      const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
      child.kill("SIGTERM");
      await closed;
      await tick();
      const kill = vi.spyOn(process, "kill");
      expect(await handle.isAlive()).toBe(false);
      expect(kill).not.toHaveBeenCalled();
      await expect(handle.closed).resolves.toBeUndefined();
    });
  });
});

describe.skipIf(process.platform !== "linux")("native descendant tree-empty delivery", () => {
  it("settles a real process run at the later descendant exit, not the deadline", async () => {
    const directory = root();
    const workerPath = path.join(directory, "descendant-worker.mjs");
    const descendant = `const fs = require("node:fs");
      const timer = setInterval(() => {
        if (fs.existsSync("exit-descendant")) {
          fs.writeFileSync("descendant-exited", String(Date.now()));
          clearInterval(timer); process.exit(0);
        }
      }, 10);
      setTimeout(() => process.exit(2), 8000);`;
    fs.writeFileSync(workerPath, `import fs from "node:fs";
      import { spawn } from "node:child_process";
      process.on("message", message => {
        if (message.type !== "fabric-execution-custody-ack") return;
        const descendant = spawn(process.execPath, ["-e", ${JSON.stringify(descendant)}], { detached: true, stdio: "ignore" });
        descendant.unref();
        const fields = fs.readFileSync(\`/proc/\${descendant.pid}/stat\`, "utf8");
        const started = fields.slice(fields.lastIndexOf(")") + 2).trim().split(/\\s+/)[19];
        process.send({ type: "fabric-execution-started", pid: descendant.pid, started }, () => {
          fs.writeFileSync("descendant-ready", String(descendant.pid));
          setInterval(() => { if (fs.existsSync("exit-parent")) process.exit(0); }, 10);
        });
      });
      process.send({ type: "fabric-execution-custody" });
      setTimeout(() => process.exit(2), 8000);`);
    const actualSpawn = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    let child!: ChildProcess;
    vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => { child = actualSpawn.spawn(...args); return child; });
    const actualLaunch = ProcessTransport.prototype.launch;
    let transport!: AgentTransportHandle;
    const stop = vi.fn<AgentTransportHandle["stop"]>();
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function(this: ProcessTransport, request) {
      transport = await actualLaunch.call(this, request);
      stop.mockImplementation(transport.stop);
      return { ...transport, stop, relaunchable: false };
    });
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000, budgetUsd: 0, sessionExport: false }, {
      workerPath, runRoot: path.join(directory, "runs"),
    });
    managers.push(manager);
    const handle = await manager.spawn({ task: "native descendant closes later", transport: "process" });
    try {
      await vi.waitFor(() => expect(fs.existsSync(path.join(directory, "descendant-ready"))).toBe(true), { timeout: 3_000 });
      const closed = new Promise<void>(resolve => child.once("close", () => resolve()));
      fs.writeFileSync(path.join(directory, "exit-parent"), "");
      await closed; await tick();
      expect(await transport.isAlive()).toBe(true);
      expect(manager.status(handle.id).status).toBe("running");
      let treeEmpty = false;
      void transport.treeClosed?.then(() => { treeEmpty = true; });
      await new Promise<void>(resolve => setTimeout(resolve, 100));
      expect(treeEmpty).toBe(false);
      expect(stop).not.toHaveBeenCalled();
      const releasedAt = Date.now();
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      let timer: ReturnType<typeof setTimeout> | undefined;
      let result;
      try {
        result = await Promise.race([manager.wait(handle.id), new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("descendant exit did not settle before the run deadline")), 2_000);
        })]);
      } finally { clearTimeout(timer); }
      expect(result.status).toBe("failed");
      expect(result.error).toContain("exited without a result");
      expect(fs.existsSync(path.join(directory, "descendant-exited"))).toBe(true);
      expect(Date.now() - releasedAt).toBeLessThan(2_000);
      expect(transport.treeClosed).toBeDefined();
      expect(treeEmpty).toBe(true);
      expect(await transport.isAlive()).toBe(false);
      expect(stop).not.toHaveBeenCalled();
    } finally {
      fs.writeFileSync(path.join(directory, "exit-parent"), "");
      fs.writeFileSync(path.join(directory, "exit-descendant"), "");
      await transport.stop(); await transport.waitForClose?.();
    }
  });
});

const statusRecord = (id: string, task: string): AgentRunRecord => ({
  id, name: task, task, status: "running", runner: "pi", transport: "process", cwd: process.cwd(),
  startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
});

function monitored(relaunchable = false) {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
  const directory = root();
  const close = deferred();
  const tree = deferred();
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as fs.FSWatcher;
  vi.spyOn(fs, "watch").mockReturnValue(watcher);
  let alive = true;
  const read = vi.fn(async () => alive);
  const stop = vi.fn(async () => { alive = false; close.resolve(); tree.resolve(); });
  const launch = vi.spyOn(ProcessTransport.prototype, "launch").mockResolvedValue({
    kind: "process", liveness: "events", closed: close.promise, treeClosed: tree.promise,
    isAlive: read, stop, relaunchable,
  });
  const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 1_000, budgetUsd: 0, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(directory, "runs"),
  });
  managers.push(manager);
  return { manager, close, tree, watcher, read, stop, launch, die: () => { alive = false; } };
}

describe("manager event-only process monitoring", () => {
  it("does not wake or query on 250 ms/1 s ticks; only native close triggers a liveness check", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "event census", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const readFile = vi.spyOn(fs, "readFileSync");
    const timeout = vi.spyOn(globalThis, "setTimeout");
    f.watcher.emit("change", "unrelated-file");
    await vi.advanceTimersByTimeAsync(1_500);
    expect(f.read).not.toHaveBeenCalled();
    expect(readFile.mock.calls.filter(([file]) => file === status)).toHaveLength(0);
    expect(timeout).not.toHaveBeenCalled();
    f.die(); f.close.resolve(); f.tree.resolve();
    await tick();
    expect(f.read).toHaveBeenCalledOnce();
    expect((await f.manager.wait(handle.id)).status).toBe("failed");
  });

  it.each(["closed", "treeClosed"])("wakes on %s by the next tick", async event => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: event, transport: "process" });
    await vi.advanceTimersByTimeAsync(500);
    expect(f.read).not.toHaveBeenCalled();
    if (event === "closed") f.close.resolve(); else f.tree.resolve();
    await tick();
    expect(f.read).toHaveBeenCalledOnce();
    // A notification while custody still says alive is not exit permission.
    expect(f.manager.status(handle.id).status).toBe("running");
    f.die(); if (event === "closed") f.tree.resolve(); else f.close.resolve();
    await tick();
    expect((await f.manager.wait(handle.id)).status).toBe("failed");
  });

  it("catches a missed event with exactly one deadline safety read", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "missed native event", transport: "process" });
    f.die(); // Neither captured promise delivers its notification.
    await vi.advanceTimersByTimeAsync(1_999);
    expect(f.read).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect((await f.manager.wait(handle.id)).error).toContain("exited without a result");
    expect(f.read).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.read).toHaveBeenCalledOnce();
    expect(f.watcher.close).toHaveBeenCalledOnce();
    f.close.resolve(); f.tree.resolve();
  });

  it("observes atomic status publication without probing process liveness", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "status events", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const temporary = `${status}.tmp`;
    fs.writeFileSync(temporary, JSON.stringify({ ...statusRecord(handle.id, "status events"), turns: 3, toolCalls: 1 }));
    fs.renameSync(temporary, status);
    f.watcher.emit("rename", "status.json");
    await tick();
    expect(f.manager.status(handle.id)).toMatchObject({ status: "running", turns: 3, toolCalls: 1 });
    expect(f.read).not.toHaveBeenCalled();
    f.die(); f.close.resolve(); f.tree.resolve();
    await tick();
    expect((await f.manager.wait(handle.id)).status).toBe("failed");
  });

  it("does not let a terminal file erase a failed native watcher", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "terminal without exit proof", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const record = statusRecord(handle.id, "terminal without exit proof");
    f.close.reject(new Error("native close lost"));
    await tick();
    fs.writeFileSync(status, JSON.stringify({ ...record, status: "completed", finishedAt: Date.now() }));
    f.watcher.emit("rename", "status.json");
    await tick();
    expect(f.read).not.toHaveBeenCalled();
    expect(f.stop).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.read).toHaveBeenCalledOnce();
    f.tree.resolve();
  });

  it.each(["native", "tree", "records"])("keeps a %s watcher failure latched across a later wake and dead process", async source => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "dead after watcher failure", transport: "process" });
    const status = path.join(f.manager.runDirectory(handle.id)!, "status.json");
    const failure = new Error("event watcher unavailable");
    if (source === "native") f.close.reject(failure);
    else if (source === "tree") f.tree.reject(failure);
    else f.watcher.emit("error", failure);
    await tick();
    // A separate status wake must not clear the failure or grant retry admission.
    f.watcher.emit("rename", "status.json");
    await tick();
    f.die();
    fs.writeFileSync(status, JSON.stringify({ ...statusRecord(handle.id, "dead after watcher failure"), status: "completed", finishedAt: Date.now() }));
    if (source === "native") f.tree.resolve(); else f.close.resolve();
    await tick();
    expect(f.manager.status(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.launch).toHaveBeenCalledOnce();
    expect(f.stop).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
    f.close.resolve(); f.tree.resolve();
  });

  it("vetoes a startup retry when a watcher fails during the exit join", async () => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "watcher fails during retry", transport: "process" });
    f.die(); f.close.resolve();
    await tick();
    f.watcher.emit("error", new Error("retry watcher unavailable"));
    f.tree.resolve();
    await tick();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(f.launch).toHaveBeenCalledOnce();
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
  });

  it("joins a replacement admitted just before the old watcher fails, without clearing the failure", async () => {
    const f = monitored(true);
    const handle = await f.manager.spawn({ task: "watch failure races native retry admission", transport: "process" });
    const replacementStop = vi.fn(async () => undefined);
    f.launch.mockImplementationOnce(async request => {
      expect(request.authorize?.()).toBe(true);
      f.watcher.emit("error", new Error("old watch failed during admission"));
      expect(request.authorize?.()).toBe(false);
      return { kind: "process", liveness: "events", closed: Promise.resolve(), treeClosed: Promise.resolve(),
        isAlive: async () => false, stop: replacementStop };
    });
    f.die(); f.close.resolve(); f.tree.resolve();
    await tick();
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.launch).toHaveBeenCalledTimes(2);
    expect(replacementStop).toHaveBeenCalledOnce();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
  });

  it("fails closed when the single deadline safety read rejects, without replacing it with polling", async () => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "unreadable deadline", transport: "process" });
    f.read.mockRejectedValueOnce(new Error("exit observation unreadable"));
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await f.manager.wait(handle.id)).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(f.read).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(f.read).toHaveBeenCalledOnce();
    f.close.resolve(); f.tree.resolve();
  });

  it.each(["native", "tree", "records"])("fails closed at the deadline with a typed reason after a %s watcher failure", async source => {
    const f = monitored();
    const handle = await f.manager.spawn({ task: "broken watcher", transport: "process" });
    const failure = new Error("event watcher unavailable");
    if (source === "native") f.close.reject(failure);
    else if (source === "tree") f.tree.reject(failure);
    else f.watcher.emit("error", failure);
    await tick();
    await vi.advanceTimersByTimeAsync(1_999);
    expect(f.read).not.toHaveBeenCalled();
    expect(f.manager.status(handle.id).status).toBe("running");
    await vi.advanceTimersByTimeAsync(1);
    const result = await f.manager.wait(handle.id);
    expect(result).toMatchObject({ status: "failed", errorCode: "PROCESS_LIVENESS_WATCH_FAILED" });
    expect(result.error).toContain("event watcher unavailable");
    expect(f.read).toHaveBeenCalledOnce();
    expect(f.stop).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(f.manager.runDirectory(handle.id)!, "unresolved-worker.json"))).toBe(true);
    await expect(f.manager.cleanup(handle.id)).rejects.toThrow(/lost track|unresolved|exit is unconfirmed/);
    f.close.resolve(); f.tree.resolve();
  });
});
