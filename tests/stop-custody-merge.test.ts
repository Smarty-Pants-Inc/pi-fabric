import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import type { AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { hasUnresolvedWorker } from "../src/storage/retention.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
afterEach(() => {
  vi.useRealTimers(); vi.restoreAllMocks();
  vi.mocked(spawn).mockReset();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-stop-merge-")); roots.push(root);
  return new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000,
    maxConcurrent: 1, retainRuns: false, budgetUsd: 0, notifyOnComplete: false },
  { runRoot: path.join(root, "runs"), workerPath: path.join(root, "worker.mjs") });
};
const stillRunning = async (manager: AgentManager, id: string) => {
  const waiting = expect(manager.wait(id, { timeoutMs: 0 })).rejects.toThrow(/still running/);
  await vi.advanceTimersByTimeAsync(0); await waiting;
};
const unconfirmedClose = async (manager: AgentManager) => {
  const closing = expect(manager.close()).rejects.toThrow(/execution exit unconfirmed/);
  await vi.advanceTimersByTimeAsync(10_000); await closing;
};
const terminal = (request: AgentTransportLaunch) => {
  const status = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
  fs.writeFileSync(status, JSON.stringify({ id: request.id, name: request.name, task: "terminal",
    status: "completed", runner: "pi", transport: "process", sessionId: "2147483647", cwd: request.cwd,
    startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 1, toolCalls: 0,
    text: "done", usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } }));
};

describe.skipIf(process.platform !== "linux")("absent native worker stop fence", () => {
  it.each(["unreadable", "new-birth", "late-birth"] as const)("never signals an unowned birth (%s)", async mode => {
    vi.useFakeTimers();
    const pid = 2147483647;
    const child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), kill: vi.fn() });
    vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
    const nativeRead = fs.readFileSync.bind(fs);
    const stat = nativeRead("/proc/self/stat", "utf8");
    let readCount = 0; let appeared = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (String(file) !== `/proc/${pid}/stat`) return nativeRead(file, options as never);
      if (mode === "unreadable") throw Object.assign(new Error("unreadable birth"), { code: "EACCES" });
      if ((mode === "new-birth" && readCount++ > 0) || appeared) return stat;
      throw Object.assign(new Error("absent"), { code: "ENOENT" });
    }) as typeof fs.readFileSync);
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const handle = await spawnDetached("worker.mjs", [], process.cwd());
    try {
      if (mode === "late-birth") {
        const stopping = handle.stop();
        expect(kill).toHaveBeenCalledWith(-pid, "SIGTERM");
        appeared = true;
        await vi.advanceTimersByTimeAsync(7_000); await stopping;
        expect(kill).not.toHaveBeenCalledWith(-pid, "SIGKILL");
      } else {
        await expect(handle.stop()).rejects.toThrow(/ownership\/exit/);
        expect(kill).not.toHaveBeenCalled();
      }
      expect(handle.stopDebt?.(), "failed identity checks are never bounded-stop authority").toBeUndefined();
      expect(handle.lostContact()).toBeDefined();
      expect(vi.getTimerCount()).toBe(0);
    } finally { child.emit("exit", 0); child.emit("close", 0); }
  });
});

// Inject only the parent's product branch. These are deterministic contract
// probes, not native Windows process-tree certification.
describe.each(["linux", "win32"] as const)("merge stop custody (%s)", platform => {
  it.each([false, true])("does not settle an unchecked terminal handle (close hook=%s)", async closeHook => {
    vi.useFakeTimers(); vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    let gone = false;
    const stop = vi.fn(async () => {});
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop, isAlive: async () => !gone,
        lostContact: () => gone ? undefined : "unknown execution tree",
        ...(closeHook ? { waitForClose: async () => {}, stopDebt: () => undefined } : {}) };
    });
    const manager = setup();
    const handle = await manager.spawn({ task: "terminal" });
    try {
      await vi.advanceTimersByTimeAsync(2_000);
      await stillRunning(manager, handle.id);
      await expect(manager.stop(handle.id)).rejects.toThrow(/execution exit unconfirmed/);
      expect(stop).not.toHaveBeenCalled();
      expect(hasUnresolvedWorker(manager.runDirectory(handle.id)!)).toBe(true);
      expect((await manager.spawn({ task: "must stay queued" })).status).toBe("queued");
      await unconfirmedClose(manager);
    } finally {
      gone = true;
      await manager.stop(handle.id);
      await manager.close();
    }
  });

  it("keeps a cancelled launch strict even with an exact native stop debt", async () => {
    vi.useFakeTimers(); vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    let enter!: () => void; const entered = new Promise<void>(resolve => { enter = resolve; });
    let finish!: () => void; const gate = new Promise<void>(resolve => { finish = resolve; });
    let debt: string | undefined;
    const transport: AgentTransportHandle = { kind: "process", sessionId: "2147483647",
      isAlive: async () => debt !== undefined, stop: async () => { debt = "native close deadline expired"; },
      waitForClose: async () => {}, lostContact: () => debt, stopDebt: () => debt };
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async () => { enter(); await gate; return transport; });
    const manager = setup(); const abort = new AbortController();
    const spawning = manager.spawn({ task: "cancel during launch" }, abort.signal);
    await entered; abort.abort(); finish();
    const handle = await spawning;
    try {
      await expect(manager.stop(handle.id)).rejects.toThrow(/execution exit unconfirmed/);
      await stillRunning(manager, handle.id);
      expect(hasUnresolvedWorker(manager.runDirectory(handle.id)!)).toBe(true);
      await unconfirmedClose(manager);
    } finally {
      transport.stop = async () => { debt = undefined; };
      debt = undefined;
      await manager.stop(handle.id); await manager.close();
    }
  });

  it("releases confirmed custom process admission and evicts settled handles", async () => {
    vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop: async () => {}, isAlive: async () => false };
    });
    const manager = setup();
    let first: string | undefined;
    try {
      for (let index = 0; index < 1_005; index++) {
        const result = await manager.run({ task: `eviction pressure ${index}` });
        first ??= result.id;
        expect(result.status).toBe("completed");
      }
      expect(manager.runDirectory(first!), "confirmed execution can really release and prune on either platform").toBeUndefined();
    } finally { await manager.close(); }
  }, 20_000);

  it("retains a failed release observation after execution was already confirmed", async () => {
    vi.useFakeTimers(); vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    let hung = false; let finish!: () => void;
    const query = new Promise<boolean>(resolve => { finish = () => resolve(false); });
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
      terminal(request);
      return { kind: "process", sessionId: "2147483647", stop: async () => {},
        isAlive: async () => hung ? query : false };
    });
    const manager = setup();
    try {
      const handle = await manager.spawn({ task: "confirmed exit" });
      await manager.wait(handle.id);
      const run = manager.runDirectory(handle.id)!;
      hung = true;
      const checking = expect(manager.checkpointForRelease(Date.now() + 80)).rejects.toThrow(/unconfirmed/);
      await vi.advanceTimersByTimeAsync(100); await checking;
      expect(hasUnresolvedWorker(run)).toBe(true);
      finish(); hung = false;
      await manager.close();
      expect(fs.existsSync(run)).toBe(true);
      expect(hasUnresolvedWorker(run)).toBe(true);
    } finally { finish(); hung = false; await manager.close(); }
  });
});
