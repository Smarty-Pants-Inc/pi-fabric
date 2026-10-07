import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { launchLog, same, stopOwned, type Owned } from "./helpers/owned-processes.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { canRemoveTerminalRun, hasUnresolvedWorker } from "../src/storage/retention.js";
import * as launcherOwner from "../src/residency/launcher-owner.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});
afterEach(async () => {
  vi.useRealTimers(); vi.restoreAllMocks(); vi.unstubAllEnvs();
  vi.mocked(spawn).mockImplementation((await vi.importActual<typeof import("node:child_process")>("node:child_process")).spawn);
});
const record = (id: string) => ({
  id, name: "stop-bound", task: "progress", status: "running", runner: "pi", transport: "process",
  sessionId: "2147483647", cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(),
  turns: 1, toolCalls: 1, text: "progress", usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
});

describe.skipIf(process.platform === "win32")("POSIX stop deadline (#360 Astra R1)", () => {
  it.each(["alive", "exit-without-close"] as const)("bounds a captured worker that never closes (%s)", async mode => {
    vi.useFakeTimers();
    const child = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const unconfirmed = vi.fn();
    const handle = await spawnDetached("worker.mjs", [], process.cwd(), { onUnconfirmedExit: unconfirmed });
    const capture = process.platform === "linux" ? vi.spyOn(launcherOwner, "captureDescendants") : undefined;
    const descendants = process.platform === "linux"
      ? vi.spyOn(launcherOwner, "stopObservedDescendants").mockImplementation(async () => {
        // Even without native close, cleanup starts after the original bound
        // and the custody fence is already published before it can return.
        expect(handle.lostContact()).toBeDefined();
        expect(unconfirmed).toHaveBeenCalledOnce();
      }) : undefined;
    let stopped = false;
    const first = handle.stop();
    if (capture) expect(capture.mock.invocationCallOrder[0]).toBeLessThan(kill.mock.invocationCallOrder[0]!);
    expect(handle.stop()).toBe(first);
    const pending = first.then(() => { stopped = true; });
    try {
      expect(kill).toHaveBeenCalledWith(-child.pid, "SIGTERM");
      if (mode === "exit-without-close") child.emit("exit", 0);
      await vi.advanceTimersByTimeAsync(5_999);
      expect(kill).not.toHaveBeenCalledWith(-child.pid, "SIGKILL");
      await vi.advanceTimersByTimeAsync(1);
      if (mode === "alive") expect(kill).toHaveBeenCalledWith(-child.pid, "SIGKILL");
      else expect(kill).not.toHaveBeenCalledWith(-child.pid, "SIGKILL");
      expect(descendants?.mock.calls.length ?? 0, "worker gets its native-close grace before descendants are signalled").toBe(0);
      vi.setSystemTime(new Date(0)); // Deadline is a native timer, not adjustable wall time.
      await vi.advanceTimersByTimeAsync(1_000);
      if (descendants) expect(descendants).toHaveBeenCalledOnce();
      expect(stopped, "native-close join has a finite deadline").toBe(true);
      expect(handle.lostContact()).toMatch(/unconfirmed|did not confirm/);
      expect(unconfirmed).toHaveBeenCalledOnce();
      expect(vi.getTimerCount()).toBe(0);
      child.emit("close", 0);
      expect(handle.lostContact(), "a late parent close cannot silently remove the persisted fence").toBeDefined();
    } finally { child.emit("exit", 0); child.emit("close", 0); await pending; }
  });

  it.each(["stop", "deadline", "close"] as const)("a worker that never exits fences custody but completes %s", async action => {
    vi.useFakeTimers();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stop-bound-"));
    const child = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
    let exited = false;
    vi.spyOn(process, "kill").mockImplementation(() => {
      if (exited) throw Object.assign(new Error("gone"), { code: "ESRCH" });
      return true; // Even SIGKILL cannot confirm this captured worker's exit.
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: action === "deadline" ? 1_000 : 60_000,
      maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
    }, { runRoot: root, workerPath: path.join(root, "worker.mjs") });
    let pending: Promise<unknown> | undefined;
    try {
      const handle = await manager.spawn({ task: "progress", transport: "process" });
      const run = manager.runDirectory(handle.id)!;
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record(handle.id)));
      let done = false;
      pending = (action === "stop" ? manager.stop(handle.id) : action === "deadline" ? manager.wait(handle.id) : manager.close())
        .then(result => {
          if (action !== "close") expect(result).toMatchObject({ status: action === "stop" ? "stopped" : "timed_out" });
          done = true; return result;
        });
      await vi.advanceTimersByTimeAsync(30_000);
      expect(done, "stop, run deadline, and host shutdown must not hang on close").toBe(true);
      expect(hasUnresolvedWorker(run)).toBe(true);
      expect(manager.retentionReferences().has(handle.id)).toBe(true);
      expect(canRemoveTerminalRun(run)).toBe(false);
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
      if (action !== "close") {
        await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|unconfirmed/);
        const next = await manager.spawn({ task: "admission probe", transport: "process" });
        expect(next.status).toBe("queued");
      }
      const closing = manager.close();
      await vi.advanceTimersByTimeAsync(10_000);
      await closing;
      expect(fs.existsSync(run), "host close must retain the uncertain worker's files").toBe(true);
    } finally {
      exited = true; child.emit("exit", 0); child.emit("close", 0);
      await vi.advanceTimersByTimeAsync(30_000);
      await pending;
      await manager.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["stop", "deadline", "close"] as const)("lets the real worker join its separate TERM-ignoring native group before %s completes", async action => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-native-stop-order-"));
    const nativePath = path.join(root, "native.mjs");
    const workerPath = path.join(root, "worker.mjs");
    // Execute the production worker, but delay its TERM delivery to reproduce
    // ordinary scheduling latency before its own five-second child grace starts.
    fs.writeFileSync(workerPath, `const once = process.once;
process.once = function(event, listener) {
  return once.call(this, event, event === "SIGTERM" ? (...args) => setTimeout(() => listener(...args), 200) : listener);
};
await import(${JSON.stringify(pathToFileURL(path.resolve("dist/worker.js")).href)});`);
    fs.writeFileSync(nativePath, String.raw`import fs from "node:fs";
process.on("SIGTERM", () => fs.writeFileSync("native-term", "ignored"));
process.stdin.resume();
setInterval(() => {}, 1000);
process.stdout.write(JSON.stringify({type:"agent_start"}) + "\n");
process.stdout.write(JSON.stringify({type:"message_end",message:{role:"assistant",content:"native progress"}}) + "\n");
fs.writeFileSync("native-ready", String(process.pid));`);
    const launches = launchLog(root);
    // This log is exclusive to this fixture. Use the child's own launch
    // receipt rather than a wall-clock start window (which can drift under load).
    const nativeLaunch = (): Owned | undefined => fs.existsSync(launches.file)
      ? fs.readFileSync(launches.file, "utf8").trim().split("\n").map(line => JSON.parse(line) as Owned)
        .find(entry => entry.argv[0] === nativePath)
      : undefined;
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    let child: ChildProcess | undefined;
    let closed: Promise<void> | undefined;
    vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => {
      child = actual.spawn(...args);
      closed = new Promise(resolve => child!.once("close", () => resolve()));
      return child;
    });
    const manager = new AgentManager(root, {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: action === "deadline" ? 10_000 : 60_000,
      maxConcurrent: 1, retainRuns: false, budgetUsd: 0, sessionExport: false,
    }, { runRoot: path.join(root, "runs"), workerPath, piBinary: nativePath });
    let native: Owned | undefined;
    try {
      const handle = await manager.spawn({ task: "native progress", transport: "process", extensions: false });
      const run = manager.runDirectory(handle.id)!;
      await vi.waitFor(() => expect(fs.existsSync(path.join(root, "native-ready"))).toBe(true), { timeout: 10_000 });
      const nativePid = Number(fs.readFileSync(path.join(root, "native-ready"), "utf8"));
      native = nativeLaunch();
      expect(native, "native records its owned launch identity").toBeDefined();
      expect(native!.pid).toBe(nativePid);
      expect(same(native!), "native launch identity is still live before stop").toBe(true);
      expect(nativePid).not.toBe(child!.pid);
      // The live child leads a detached group distinct from the worker's.
      expect(process.kill(-nativePid, 0)).toBe(true);
      const started = performance.now();
      if (action === "stop") expect((await manager.stop(handle.id)).status).toBe("stopped");
      else if (action === "deadline") expect((await manager.wait(handle.id)).status).toBe("timed_out");
      else await manager.close();
      expect(performance.now() - started).toBeLessThan(action === "deadline" ? 25_000 : 10_000);
      await closed;
      expect(fs.existsSync(path.join(root, "native-term")), "native ignored TERM before KILL escalation").toBe(true);
      expect(same(native!), "native child must exit, not merely its worker").toBe(false);
      expect(child!.signalCode, "worker must finish the child-close sequence cooperatively").toBeNull();
      expect(hasUnresolvedWorker(run)).toBe(false);
      // The shell gate now contains runtime preloads on scoped hosts. With
      // native teardown joined, both scoped custody and main-compatible
      // unscoped collection permit checkpoint/cleanup immediately.
      expect(fs.existsSync(path.join(run, "unresolved-scratch.json"))).toBe(false);
      if (action !== "close") await expect(manager.checkpointForRelease()).resolves.toBeUndefined();
      await manager.close();
      expect(fs.existsSync(run)).toBe(false);
    } finally {
      // Revalidate native launch identity before cleanup; reap it while its
      // owning worker is alive, then join the exact captured worker instance.
      native ??= nativeLaunch();
      if (native) await stopOwned(native, 0, 5_000);
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      await closed;
      await manager.close();
      if (native) expect(same(native), "fixture must leave no native child running").toBe(false);
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
    }
  }, 40_000);

  it.each(["stop", "deadline", "close"] as const)("escalates a real TERM-ignoring worker and completes %s", async action => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-term-ignore-"));
    const worker = path.join(root, "worker.mjs");
    fs.writeFileSync(worker, `import fs from "node:fs";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
process.on("SIGTERM", () => {});
setInterval(() => {}, 1000);
fs.writeFileSync(args.get("--status-file"), JSON.stringify({
  id: args.get("--id"), name: args.get("--name"), task: "progress", status: "running", runner: "pi",
  transport: "process", sessionId: String(process.pid), cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(),
  turns: 1, toolCalls: 1, text: "TERM handler installed", usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 }
}));`);
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    let child: ChildProcess | undefined;
    let closed: Promise<void> | undefined;
    let didClose = false;
    vi.mocked(spawn).mockImplementation((...args: Parameters<typeof spawn>) => {
      child = actual.spawn(...args);
      closed = new Promise(resolve => child!.once("close", () => { didClose = true; resolve(); }));
      return child;
    });
    const manager = new AgentManager(root, {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: action === "deadline" ? 10_000 : 60_000,
      retainRuns: false, budgetUsd: 0,
    }, { runRoot: path.join(root, "runs"), workerPath: worker });
    try {
      const handle = await manager.spawn({ task: "progress", transport: "process" });
      const run = manager.runDirectory(handle.id)!;
      await vi.waitFor(() => expect(fs.existsSync(path.join(run, "status.json"))).toBe(true), { timeout: 10_000 });
      const started = performance.now();
      if (action === "stop") expect((await manager.stop(handle.id)).status).toBe("stopped");
      else if (action === "deadline") expect((await manager.wait(handle.id)).status).toBe("timed_out");
      else await manager.close();
      expect(performance.now() - started).toBeLessThan(action === "deadline" ? 25_000 : 10_000);
      expect(didClose).toBe(true);
      expect(child!.signalCode).toBe("SIGKILL");
      // Primary close confirms escalation, not teardown of native Pi groups
      // that a stuck/TERM-ignoring worker could not join. Keep owner custody.
      expect(hasUnresolvedWorker(run)).toBe(true);
      expect(fs.existsSync(run)).toBe(true);
      if (action !== "close") {
        await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|unconfirmed/);
        await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
      }
    } finally {
      // No numeric identity, leaked native child, or unfinished teardown on assertion failure.
      if (child && child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
      if (closed) await closed;
      await manager.close();
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 });
    }
  }, 40_000);
});
