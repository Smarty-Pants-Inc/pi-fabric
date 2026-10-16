import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import principalDelivery from "../src/worker/principal-delivery.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { canRemoveTerminalRun, hasUnresolvedWorker } from "../src/storage/retention.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn() };
});

const platform = Object.getOwnPropertyDescriptor(process, "platform")!;

async function joinTreeStopEntry(killer: EventEmitter): Promise<void> {
  // Public stop first joins a possible relaunch. Flush that join without
  // advancing the helper deadline, then prove event listeners are installed.
  await vi.advanceTimersByTimeAsync(0);
  expect(spawn).toHaveBeenCalledTimes(2);
  expect(vi.mocked(spawn).mock.calls[1]?.[0]).toBe("taskkill");
  expect(killer.listenerCount("error")).toBe(1);
  expect(killer.listenerCount("close")).toBe(1);
}
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.useRealTimers(); vi.restoreAllMocks(); vi.clearAllMocks(); vi.unstubAllEnvs();
  vi.mocked(spawn).mockReset();
});

// No real workers or descendants: taskkill and the captured worker are native-event mocks.
// The unconfirmed native descendant intentionally has no Fabric status/identity file.
describe("Windows tree-stop owner custody (#360 security R1)", () => {
  it.each((["delivered", "cancelled", "queued"] as const).flatMap(state =>
    [false, true].map(leftover => ({ state, leftover })),
  ))("checks release immediately after proven stop with $state follow-up (leftover envelope: $leftover)", async ({ state, leftover }) => {
    vi.useFakeTimers();
    vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tree-follow-up-"));
    const worker = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
    const killer = Object.assign(new EventEmitter(), { pid: 2147483646, kill: vi.fn() });
    let exited = false;
    vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess).mockReturnValueOnce(killer as unknown as ChildProcess);
    vi.spyOn(process, "kill").mockImplementation(() => {
      if (exited) throw Object.assign(new Error("worker absent"), { code: "ESRCH" });
      return true;
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
    }, { runRoot: root, workerPath: path.join(root, "mock-worker.mjs") });
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const handle = await manager.spawn({ task: "follow-up release", transport: "process" });
      const run = manager.runDirectory(handle.id)!;
      const receipt = manager.followUp(handle.id, "unique follow-up", undefined, undefined, { deadlineMs: 60_000 });
      const directory = path.join(run, "deliveries");
      fs.mkdirSync(directory, { recursive: true });
      vi.stubEnv("PI_FABRIC_DELIVERY_DIR", directory);
      const envelope = path.join(directory, receipt.messageId + ".json");
      const content = JSON.stringify({
        message: "unique follow-up", delivery: "followUp", followUpId: receipt.messageId,
        provenance: { v: 1, channel: "fabric", via: "followUp", sender: { id: "sender", kind: "main", verified: "mesh" } },
      });
      fs.writeFileSync(envelope, content);
      const handlers = new Map<string, (...args: any[]) => any>();
      const sendUserMessage = vi.fn();
      principalDelivery({
        registerCommand: (_name: string, command: any) => handlers.set("command", command.handler),
        on: (name: string, handler: any) => handlers.set(name, handler), sendUserMessage,
      } as unknown as ExtensionAPI);
      await handlers.get("command")!(receipt.messageId, { isIdle: () => false });
      if (state === "delivered") {
        await handlers.get("turn_end")!({ outcome: "completed" }, { isIdle: () => false });
        const text = sendUserMessage.mock.calls[0]![0];
        await handlers.get("context")!({ messages: [{ role: "user", content: [{ type: "text", text }] }] }, {});
      } else if (state === "cancelled") manager.cancelFollowUp(handle.id, receipt.messageId);
      expect(manager.status(handle.id).followUpDeliveries?.[0]?.state).toBe(state);
      if (leftover) fs.writeFileSync(envelope, content);
      else fs.rmSync(envelope, { force: true });
      const stopping = manager.stop(handle.id);
      await joinTreeStopEntry(killer);
      killer.emit("close", 0); exited = true; worker.emit("exit", 0); worker.emit("close", 0);
      await stopping;
      // No timer/microtask flush: public stop has already joined positive host exit.
      expect(hasUnresolvedWorker(run)).toBe(false);
      if (state === "queued") {
        await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved run tree/);
      } else {
        expect(manager.retentionReferences().has(handle.id)).toBe(false);
        await expect(manager.checkpointForRelease()).resolves.toBeUndefined();
      }
    } finally {
      exited = true; killer.emit("close", 0); worker.emit("exit", 0); worker.emit("close", 0);
      const closing = manager.close();
      await vi.advanceTimersByTimeAsync(30_000);
      await closing;
      Object.defineProperty(process, "platform", platform);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
  it.each(["confirmed", "timeout", "stop-confirmed", "stop-failure"] as const)(
    "joins normal terminal cleanup to captured close without releasing uncertain custody (%s)", async outcome => {
      vi.useFakeTimers();
      vi.spyOn(process, "emitWarning").mockImplementation(() => {});
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-normal-close-"));
      const worker = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
      const killer = Object.assign(new EventEmitter(), { pid: 2147483646, kill: vi.fn() });
      let exited = false;
      vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess).mockReturnValueOnce(killer as unknown as ChildProcess);
      vi.spyOn(process, "kill").mockImplementation(() => {
        if (exited) throw Object.assign(new Error("worker absent"), { code: "ESRCH" });
        return true;
      });
      const manager = new AgentManager(process.cwd(), {
        ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
      }, { runRoot: root, workerPath: path.join(root, "mock-worker.mjs") });
      let cleaning: Promise<void> | undefined;
      let stopping: Promise<unknown> | undefined;
      try {
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        const handle = await manager.spawn({ task: "successful terminal cleanup", transport: "process", actorId: "normal-close-owner" });
        const run = manager.runDirectory(handle.id)!;
        const record: AgentRunRecord = {
          id: handle.id, name: handle.name, task: "successful terminal cleanup", status: "completed", runner: "pi",
          transport: "process", sessionId: String(worker.pid), actorId: "normal-close-owner", cwd: process.cwd(),
          startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 1, toolCalls: 0, text: "done",
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
        };
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
        let reported = false;
        const waiting = manager.wait(handle.id).then(result => { reported = true; return result; });
        await vi.advanceTimersByTimeAsync(250);
        expect(reported).toBe(false);
        expect(manager.status(handle.id).status).toBe("finishing");
        let collected = false;
        let cleanupError: unknown;
        await expect(manager.cleanup(handle.id)).rejects.toThrow(/running/);
        expect(fs.existsSync(run)).toBe(true);
        expect(manager.retentionReferences().has("normal-close-owner")).toBe(true);
        await expect(manager.checkpointForRelease()).rejects.toThrow(/pending|unresolved/);
        const probe = await manager.spawn({ task: "native capacity stays owned", transport: "process" });
        expect(probe.status).toBe("queued");
        await manager.stop(probe.id);
        const stoppingDuringJoin = outcome.startsWith("stop-");
        if (stoppingDuringJoin) {
          stopping = manager.stop(handle.id).catch(error => error);
          await vi.advanceTimersByTimeAsync(0);
          expect(spawn).toHaveBeenCalledTimes(2);
          // Explicit teardown still vetoes a second cleanup call immediately.
          await expect(manager.cleanup(handle.id)).rejects.toThrow(/pending|teardown|running/);
        }
        exited = true;
        worker.emit("exit", 0);
        if (outcome !== "timeout") worker.emit("close", 0);
        await vi.advanceTimersByTimeAsync(100);
        if (stoppingDuringJoin) {
          expect(collected).toBe(false);
          expect(cleanupError).toBeUndefined();
          expect(fs.existsSync(run)).toBe(true);
          await expect(manager.checkpointForRelease()).rejects.toThrow(/pending/);
          killer.emit("close", outcome === "stop-confirmed" ? 0 : 1);
        }
        // A failed tree helper deliberately stays unresolved; the transport's
        // existing seven-second stop bound must expire without faking tree exit.
        if (outcome === "timeout" || outcome === "stop-failure") await vi.advanceTimersByTimeAsync(7_000);
        await vi.advanceTimersByTimeAsync(0);
        await stopping;
        const uncertain = outcome === "timeout" || outcome === "stop-failure";
        expect(await waiting).toMatchObject(uncertain
          ? { status: "failed", errorCode: "CUSTODY_UNCONFIRMED" } : { status: "completed" });
        cleaning = manager.cleanup(handle.id).then(() => { collected = true; }, error => { cleanupError = error; });
        await cleaning;
        expect(collected).toBe(!uncertain);
        expect(fs.existsSync(run)).toBe(uncertain);
        if (uncertain) {
          expect(cleanupError).toBeInstanceOf(Error);
          expect((cleanupError as Error).message).toMatch(/lost track/);
          expect(hasUnresolvedWorker(run)).toBe(true);
          expect(canRemoveTerminalRun(run)).toBe(false);
          expect(manager.retentionReferences().has("normal-close-owner")).toBe(true);
          // Even late native close cannot erase the persisted uncertainty fence.
          worker.emit("close", 0);
          await vi.advanceTimersByTimeAsync(0);
          await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
          await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|unconfirmed/);
          const blocked = await manager.spawn({ task: "uncertain capacity stays owned", transport: "process" });
          expect(blocked.status).toBe("queued");
          await manager.stop(blocked.id);
        } else expect(cleanupError).toBeUndefined();
      } finally {
        exited = true; killer.emit("close", 0); worker.emit("exit", 0); worker.emit("close", 0);
        const closing = manager.close();
        await vi.advanceTimersByTimeAsync(30_000);
        await Promise.allSettled([cleaning, stopping, closing]);
        Object.defineProperty(process, "platform", platform);
        fs.rmSync(root, { recursive: true, force: true });
      }
    },
  );
  it.each((["confirmed", "worker-close-last", "failure", "timeout"] as const).flatMap(outcome =>
    [false, true].map(closeDuringStop => [outcome, closeDuringStop] as const),
  ))("fences teardown started while finishing (%s, close=%s)", async (outcome, closeDuringStop) => {
    vi.useFakeTimers();
    vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-settled-tree-"));
    const worker = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
    const killer = Object.assign(new EventEmitter(), { pid: 2147483646, kill: vi.fn() });
    let exited = false;
    vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess).mockReturnValueOnce(killer as unknown as ChildProcess);
    vi.spyOn(process, "kill").mockImplementation(() => {
      if (exited) throw Object.assign(new Error("worker absent"), { code: "ESRCH" });
      return true;
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
    }, { runRoot: root, workerPath: path.join(root, "mock-worker.mjs") });
    let stopping: Promise<unknown> | undefined;
    let closing: Promise<void> | undefined;
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const handle = await manager.spawn({ task: "terminal before native close", transport: "process", actorId: "settled-owner" });
      const run = manager.runDirectory(handle.id)!;
      const record: AgentRunRecord = {
        id: handle.id, name: handle.name, task: "terminal before native close", status: "completed", runner: "pi",
        transport: "process", sessionId: String(worker.pid), actorId: "settled-owner", cwd: process.cwd(),
        startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 1, toolCalls: 1, text: "done",
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
      };
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
      let reported = false;
      const waiting = manager.wait(handle.id).then(result => { reported = true; return result; });
      await vi.advanceTimersByTimeAsync(250);
      expect(reported).toBe(false);
      expect(manager.status(handle.id).status).toBe("finishing");
      let stopped = false;
      stopping = manager.stop(handle.id).then(result => { stopped = true; return result; }, error => { stopped = true; return error; });
      await vi.advanceTimersByTimeAsync(0);
      expect(spawn).toHaveBeenCalledTimes(2);
      if (outcome === "worker-close-last") killer.emit("close", 0);
      else { exited = true; worker.emit("exit", 0); worker.emit("close", 0); }
      await vi.advanceTimersByTimeAsync(100);
      expect(stopped, "both helper and captured worker close are required").toBe(false);
      expect(hasUnresolvedWorker(run), "pending is not failure yet").toBe(false);
      expect(manager.retentionReferences().has(handle.id)).toBe(true);
      expect(manager.retentionReferences().has("settled-owner")).toBe(true);
      await expect(manager.checkpointForRelease()).rejects.toThrow(/pending|unresolved/);
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/pending|teardown|running/);
      const next = await manager.spawn({ task: "capacity probe", transport: "process" });
      expect(next.status, "settlement must not release the pending native permit").toBe("queued");
      let closed = false;
      if (closeDuringStop) {
        closing = manager.close().then(() => { closed = true; });
        await vi.advanceTimersByTimeAsync(100);
        expect(closed, "close joins teardown even for a logically settled run").toBe(false);
        expect(fs.existsSync(run)).toBe(true);
      } else await manager.stop(next.id);
      if (outcome === "worker-close-last") { exited = true; worker.emit("exit", 0); worker.emit("close", 0); }
      else if (outcome === "confirmed") killer.emit("close", 0);
      else if (outcome === "failure") killer.emit("close", 1);
      else { await vi.advanceTimersByTimeAsync(1_000); killer.emit("close", 0); }
      await vi.advanceTimersByTimeAsync(10_000);
      const uncertain = outcome === "failure" || outcome === "timeout";
      expect(await stopping).toMatchObject(uncertain
        ? { status: "failed", errorCode: "CUSTODY_UNCONFIRMED" } : { status: "completed" });
      expect(await waiting).toMatchObject(uncertain
        ? { status: "failed", errorCode: "CUSTODY_UNCONFIRMED" } : { status: "completed" });
      expect(stopped).toBe(true);
      if (uncertain) {
        expect(hasUnresolvedWorker(run)).toBe(true);
        expect(manager.retentionReferences().has(handle.id)).toBe(true);
        expect(manager.retentionReferences().has("settled-owner")).toBe(true);
        expect(canRemoveTerminalRun(run)).toBe(false);
        await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
        if (!closeDuringStop) {
          await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|unconfirmed/);
          const blocked = await manager.spawn({ task: "failed-tree capacity probe", transport: "process" });
          expect(blocked.status).toBe("queued");
          await manager.stop(blocked.id);
        }
      } else if (!closeDuringStop) {
        expect(hasUnresolvedWorker(run)).toBe(false);
        expect(manager.retentionReferences().has(handle.id)).toBe(false);
        await expect(manager.checkpointForRelease()).resolves.toBeUndefined();
        await expect(manager.cleanup(handle.id)).resolves.toMatchObject({ cleaned: true });
        vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess);
        const admitted = await manager.spawn({ task: "confirmed-tree capacity probe", transport: "process" });
        expect(admitted.status).toBe("running");
        worker.emit("exit", 0); worker.emit("close", 0);
        fs.writeFileSync(path.join(manager.runDirectory(admitted.id)!, "status.json"), JSON.stringify({ ...record, id: admitted.id }));
        await vi.advanceTimersByTimeAsync(250);
      }
      closing ??= manager.close();
      await vi.advanceTimersByTimeAsync(10_000);
      await closing;
      expect(fs.existsSync(run)).toBe(uncertain);
    } finally {
      exited = true; killer.emit("close", 0); worker.emit("exit", 0); worker.emit("close", 0);
      await vi.advanceTimersByTimeAsync(30_000);
      await Promise.allSettled([stopping, closing]);
      await manager.close();
      Object.defineProperty(process, "platform", platform);
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["confirmed", "timeout"] as const)("joins a pending tree stop before dead-worker settlement when polling overtakes the helper (%s)", async outcome => {
    vi.useFakeTimers();
    vi.spyOn(process, "emitWarning").mockImplementation(() => {});
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tree-pending-"));
    const worker = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
    const killer = Object.assign(new EventEmitter(), { pid: 2147483646, kill: vi.fn() });
    let exited = false;
    vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess).mockReturnValueOnce(killer as unknown as ChildProcess);
    vi.spyOn(process, "kill").mockImplementation(() => {
      if (exited) throw Object.assign(new Error("worker absent"), { code: "ESRCH" });
      return true;
    });
    const manager = new AgentManager(process.cwd(), {
      ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000, maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
    }, { runRoot: root, workerPath: path.join(root, "mock-worker.mjs") });
    // Hold only the helper's timeout continuation. Its timer becomes overdue,
    // but the earlier monitor poll gets to run before that callback is serviced.
    const schedule = globalThis.setTimeout;
    let helperTimeout: (() => void) | undefined;
    let helperOverdue = false;
    let timerSpy: ReturnType<typeof vi.spyOn> | undefined;
    let stopping: Promise<unknown> | undefined;
    let waiting: Promise<unknown> | undefined;
    let closing: Promise<void> | undefined;
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const handle = await manager.spawn({ task: "work already produced", transport: "process" });
      const run = manager.runDirectory(handle.id)!;
      const record: AgentRunRecord = {
        id: handle.id, name: handle.name, task: "work already produced", status: "running", runner: "pi",
        transport: "process", sessionId: String(worker.pid), cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(),
        turns: 1, toolCalls: 1, text: "progress", usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
      };
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
      await vi.advanceTimersByTimeAsync(100); // monitor observes real progress
      timerSpy = vi.spyOn(globalThis, "setTimeout").mockImplementation((callback, delay, ...args) => {
        if (delay === 1_000 && !helperTimeout) {
          helperTimeout = () => callback(...args);
          return schedule(() => { helperOverdue = true; }, delay);
        }
        return schedule(callback, delay, ...args);
      });
      stopping = manager.stop(handle.id);
      await joinTreeStopEntry(killer);
      expect(helperTimeout, "the pending helper must own its timeout before worker exit").toBeTypeOf("function");
      exited = true; worker.emit("exit", 0); worker.emit("close", 0);
      let reported = false;
      waiting = manager.wait(handle.id).then(() => { reported = true; });
      await vi.advanceTimersByTimeAsync(1_350);
      expect(helperOverdue).toBe(true);
      expect(reported, "dead-worker reporting must join the outstanding tree stop").toBe(false);
      expect(hasUnresolvedWorker(run), "uncertainty callback has not run yet").toBe(false);
      expect(manager.retentionReferences().has(handle.id)).toBe(true);
      expect(canRemoveTerminalRun(run)).toBe(false);
      await expect(manager.checkpointForRelease()).rejects.toThrow(/pending|unresolved/);
      await expect(manager.cleanup(handle.id)).rejects.toThrow(/running/);
      const next = await manager.spawn({ task: "capacity probe", transport: "process" });
      expect(next.status, "pending tree join retains its one native admission permit").toBe("queued");
      let closed = false;
      closing = manager.close().then(() => { closed = true; });
      await vi.advanceTimersByTimeAsync(100);
      expect(closed, "close must not skip a run whose tree stop is pending").toBe(false);
      expect(fs.existsSync(run)).toBe(true);
      if (outcome === "confirmed") killer.emit("close", 0);
      else { helperTimeout!(); killer.emit("close", 0); }
      helperTimeout = undefined; // A confirmed/failed helper no longer has a pending callback.
      await vi.advanceTimersByTimeAsync(10_000);
      await Promise.all([stopping, waiting, closing]);
      expect(reported).toBe(true); expect(closed).toBe(true);
      expect(spawn, "pending join cannot launch a replacement").toHaveBeenCalledTimes(2);
      if (outcome === "timeout") {
        expect(hasUnresolvedWorker(run)).toBe(true);
        expect(manager.retentionReferences().has(handle.id)).toBe(true);
        expect(canRemoveTerminalRun(run)).toBe(false);
        await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
        expect(fs.existsSync(run)).toBe(true);
      } else expect(fs.existsSync(run)).toBe(false);
    } finally {
      timerSpy?.mockRestore();
      helperTimeout?.(); killer.emit("close", 0); worker.emit("exit", 0); worker.emit("close", 0);
      Object.defineProperty(process, "platform", platform);
      await vi.advanceTimersByTimeAsync(20_000);
      await Promise.allSettled([stopping, waiting, closing]);
      await manager.close();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each(["error", "terminal-before-error", "failed-before-error", "resume-error", "nonzero", "timeout", "spawn-throw", "confirmed"] as const)(
    "fences all owner-release paths until tree exit is confirmed (%s)", async outcome => {
      vi.useFakeTimers();
      vi.spyOn(process, "emitWarning").mockImplementation(() => {});
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tree-fence-"));
      const worker = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
      const killer = Object.assign(new EventEmitter(), { pid: 2147483646, kill: vi.fn() });
      let run = "";
      let markerAtFallback = false;
      let workerExited = false;
      worker.kill.mockImplementation(() => {
        markerAtFallback = hasUnresolvedWorker(run);
        workerExited = true;
        worker.emit("exit", null); worker.emit("close", null);
        return true;
      });
      vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess);
      if (outcome === "spawn-throw") vi.mocked(spawn).mockImplementationOnce(() => { throw new Error("helper failed to start"); });
      else vi.mocked(spawn).mockReturnValueOnce(killer as unknown as ChildProcess);
      vi.spyOn(process, "kill").mockImplementation(() => {
        if (workerExited) throw Object.assign(new Error("captured worker absent"), { code: "ESRCH" });
        return true;
      });
      const manager = new AgentManager(process.cwd(), {
        ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1, retainRuns: false, budgetUsd: 0,
      }, { runRoot: root, workerPath: path.join(root, "mock-worker.mjs") });
      try {
        Object.defineProperty(process, "platform", { ...platform, value: "win32" });
        const handle = await manager.spawn({ task: "work already produced", transport: "process" });
        run = manager.runDirectory(handle.id)!;
        const record: AgentRunRecord = {
          id: handle.id, name: handle.name, task: "work already produced", status: "running",
          runner: "pi", transport: "process", sessionId: String(worker.pid), cwd: process.cwd(),
          startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 1, text: "progress",
          usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 },
        };
        fs.writeFileSync(path.join(run, "status.json"), JSON.stringify(record));
        let stopped = false;
        if (outcome === "resume-error") fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({ ...record, status: "stopped", finishedAt: Date.now() }));
        const stopping = (outcome === "resume-error" ? manager.wait(handle.id) : manager.stop(handle.id)).then(() => { stopped = true; });
        if (outcome !== "resume-error" && outcome !== "spawn-throw") await joinTreeStopEntry(killer);
        if (outcome === "resume-error") {
          // The automatic resume path also invokes stop. Its bounded return is
          // not exit proof: an uncertain prior tree must never launch a replacement.
          await vi.advanceTimersByTimeAsync(30_000);
        } else if (outcome === "confirmed") {
          killer.emit("close", 0); workerExited = true; worker.emit("exit", 0); worker.emit("close", 0);
          await stopping;
        } else {
          if (outcome === "terminal-before-error" || outcome === "failed-before-error") {
            // A terminal file may race the helper outcome. Do not release its
            // permit or publish a collectible result while stop still owes a join.
            fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({
              ...record, status: outcome === "failed-before-error" ? "failed" : "completed",
              ...(outcome === "failed-before-error" ? { error: "real worker failure" } : {}), finishedAt: Date.now(),
            }));
            let reported = false;
            void manager.wait(handle.id).then(() => { reported = true; });
            await vi.advanceTimersByTimeAsync(250);
            expect(reported, "terminal reporting cannot bypass an in-flight process stop").toBe(false);
          }
          if (outcome === "error" || outcome === "terminal-before-error" || outcome === "failed-before-error") { killer.emit("error", new Error("helper failure")); killer.emit("close", 0); }
          if (outcome === "nonzero") killer.emit("close", 1);
          if (outcome === "timeout") { await vi.advanceTimersByTimeAsync(1_000); killer.emit("close", 0); }
          await vi.advanceTimersByTimeAsync(0);
          expect(stopped, "closed failed attempt must not add seven seconds to logical stop").toBe(true);
          expect(hasUnresolvedWorker(run)).toBe(true);
          expect(manager.retentionReferences().has(handle.id)).toBe(true);
          await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track|running/);
          // Later monitoring must preserve the custody failure without transferring custody.
          await vi.advanceTimersByTimeAsync(10_000);
        }
        // Keep Windows active through the later admission/close probe too;
        // restoring Linux here used to hide that launch's captured-close debt.
        const result = await manager.wait(handle.id);
        const failedCandidate = outcome === "terminal-before-error" || outcome === "failed-before-error";
        expect(result.status).toBe(failedCandidate ? "failed" : "stopped");
        if (failedCandidate) expect(result.errorCode).toBe("CUSTODY_UNCONFIRMED");
        if (outcome === "confirmed") {
          expect(hasUnresolvedWorker(run)).toBe(false);
          expect(manager.retentionReferences().has(handle.id)).toBe(false);
          await expect(manager.checkpointForRelease()).resolves.toBeUndefined();
          await expect(manager.cleanup(handle.id)).resolves.toMatchObject({ cleaned: true });
        } else {
          expect(markerAtFallback, "persist uncertainty before killing only the worker").toBe(true);
          expect(hasUnresolvedWorker(run)).toBe(true);
          expect(manager.retentionReferences().has(handle.id)).toBe(true);
          expect(canRemoveTerminalRun(run)).toBe(false);
          await expect(manager.checkpointForRelease()).rejects.toThrow(/unresolved|unconfirmed/);
          await expect(manager.cleanup(handle.id)).rejects.toThrow(/lost track/);
        }
        expect(stopped, "the bounded stop must return without transferring custody").toBe(true);
        expect(spawn, "uncertain tree exit cannot launch a replacement worker").toHaveBeenCalledTimes(2);
        if (outcome === "resume-error") expect(result.error).toMatch(/unconfirmed|unknown|not relaunched/);
        // A terminal report must not free the uncertain tree's one native permit.
        vi.mocked(spawn).mockReturnValueOnce(worker as unknown as ChildProcess);
        const next = await manager.spawn({ task: "capacity probe", transport: "process" });
        if (outcome === "confirmed") {
          expect(next.status).toBe("running");
          const nextRun = manager.runDirectory(next.id)!;
          fs.writeFileSync(path.join(nextRun, "status.json"), JSON.stringify({ ...record, id: next.id, status: "completed", finishedAt: Date.now() }));
          await vi.advanceTimersByTimeAsync(1_000);
          // This launch owns a new captured-close promise even though the mock
          // reuses the same EventEmitter. Native Windows must close it too.
          worker.emit("exit", 0); worker.emit("close", 0);
          await vi.advanceTimersByTimeAsync(0);
        } else expect(next.status).toBe("queued");
        await manager.close();
        expect(fs.existsSync(run)).toBe(outcome !== "confirmed");
      } finally {
        Object.defineProperty(process, "platform", platform);
        // Finish every mock helper's timer; never leave an owned native process running.
        killer.emit("close", 0); worker.emit("exit", null); worker.emit("close", null);
        await vi.advanceTimersByTimeAsync(10_000);
        await manager.close();
        fs.rmSync(root, { recursive: true, force: true });
      }
    });
});
