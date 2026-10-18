import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";

vi.mock("node:child_process", async importOriginal => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(() => { throw new Error("Unexpected real child launch in recorded probe"); }) };
});

// Recorded-call probe: never launch, signal, or delete a real process/tree locally.
const managers: AgentManager[] = [];
const exits: (() => void)[] = [];
afterEach(async () => {
  exits.splice(0).forEach(exit => exit());
  await vi.advanceTimersByTimeAsync(500);
  try { await Promise.all(managers.splice(0).map(manager => manager.close())); }
  finally {
    vi.useRealTimers();
    vi.restoreAllMocks();
  }
});

async function fixture(options: { finalAnswer?: boolean; killOnStop?: boolean; legacy?: boolean; legacyNoEvents?: boolean; workerFinishing?: boolean; milestone?: boolean; stopFailures?: number; workerRunning?: boolean; timeoutMs?: number; recordedNative?: boolean } = {}) {
  // Keep the host platform native: POSIX scoped launches publish treeClosed;
  // Windows launches expose only native close and bounded waitForClose.
  const win32 = process.platform === "win32";
  vi.useFakeTimers();
  vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  vi.spyOn(fs.promises, "rm").mockResolvedValue();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-finishing-events-"));
  const child = Object.assign(new EventEmitter(), { pid: 2147483647, unref: vi.fn(), kill: vi.fn() });
  vi.mocked(spawn).mockClear();
  const kill = options.recordedNative ? vi.spyOn(process, "kill").mockImplementation(() => { throw new Error("Unexpected process signal in native-close probe"); }) : undefined;
  if (options.recordedNative) vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
  let nativeHandle: AgentTransportHandle | undefined;
  let alive = true;
  let primaryDone!: () => void;
  const primaryExit = new Promise<void>(resolve => { primaryDone = resolve; });
  const emitPrimaryExit = () => {
    alive = false;
    primaryDone();
    if (options.recordedNative) child.emit("exit", 0, null);
  };
  let close!: () => void;
  let treeExit!: () => void;
  let rejectTree!: (error: Error) => void;
  let lost: string | undefined;
  const closed = new Promise<void>(resolve => { close = resolve; });
  const treeClosed = new Promise<void>((resolve, reject) => { treeExit = resolve; rejectTree = reject; });
  void treeClosed.catch(() => {});
  const isAlive = vi.fn(async () => alive);
  let statusFile: string | undefined;
  const exit = () => {
    if (options.workerFinishing && statusFile) {
      const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
      fs.writeFileSync(statusFile, JSON.stringify({ ...record, status: "completed" }));
    }
    emitPrimaryExit();
    close(); treeExit();
    if (options.recordedNative) child.emit("close", 0, null);
  };
  let stopAttempts = 0;
  const stop = vi.fn(async () => {
    if (options.killOnStop) exit();
    if (++stopAttempts <= (options.stopFailures ?? 0)) throw new Error("injected transient stop failure");
    await (win32 ? closed : treeClosed);
  });
  exits.push(exit);
  const realLaunch = ProcessTransport.prototype.launch;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    const args = new Map<string, string>();
    for (let i = 0; i < request.workerArguments.length; i += 2) args.set(request.workerArguments[i]!, request.workerArguments[i + 1]!);
    statusFile = args.get("--status-file")!;
    const receipt = options.finalAnswer ? saveFinalAnswerReceipt(path.dirname(statusFile), request.id, "QUIESCENT") : undefined;
    fs.writeFileSync(statusFile, JSON.stringify({ id: request.id, name: request.name, task: "probe", runner: "pi", transport: "process", cwd: request.cwd,
      startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(), turns: 1, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      status: options.workerRunning ? "running" : options.workerFinishing ? options.milestone ? "running" : "finishing" : "completed",
      ...(options.milestone ? { finishingAt: Date.now() } : {}), text: "QUIESCENT", exitCode: 0,
      ...(receipt ? { finalAnswerReceipt: { id: receipt.id, recordedAt: receipt.recordedAt } } : {}) }));
    if (options.recordedNative) {
      // Exercise the actual adapter and legacy native wait/stop deadlines, not a
      // fixture reimplementation that could conceal the passive-wait race.
      nativeHandle = await realLaunch.call(this, request);
      vi.spyOn(nativeHandle, "stop");
      vi.spyOn(nativeHandle, "waitForClose");
      return nativeHandle;
    }
    // An older adapter without close events has neither closed nor waitForClose.
    const nativeClose = options.legacyNoEvents ? {} : { closed, waitForClose: async () => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        await Promise.race([closed, new Promise<void>(resolve => {
          timer = setTimeout(() => { lost ??= "Owned process worker did not confirm native close within 7000ms"; resolve(); }, 7_000);
        })]);
      } finally { clearTimeout(timer); }
    } };
    return { kind: "process",
      ...(options.legacy ? { liveness: "poll" as const } : win32 ? {} : { liveness: "events" as const }),
      ...nativeClose,
      ...(win32 ? { primaryExit } : {}),
      ...(options.legacy || win32 ? {} : { treeClosed }), isAlive, stop, finishingGraceMs: 7_000,
      ...(win32 ? {} : { forcedCleanupCount: () => options.killOnStop && !alive ? 1 : 0 }),
      lostContact: () => lost } satisfies AgentTransportHandle;
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 30_000, maxConcurrent: 1, ...(options.timeoutMs !== undefined ? { timeoutMs: options.timeoutMs } : {}) }, { workerPath: path.join(root, "worker.mjs"), runRoot: path.join(root, "runs") });
  managers.push(manager);
  const handle = await manager.spawn({ task: "child outlives worker", transport: "process" });
  return { manager, handle, isAlive, stop: nativeHandle?.stop ?? stop,
    close: options.recordedNative ? () => { child.emit("close", 0, null); } : close,
    primaryExit: emitPrimaryExit, nativeHandle, child, kill, exit, statusFile: statusFile!,
    failTree: () => { lost = "owned tree watcher failed"; close(); rejectTree(new Error(lost)); } };
}

// The descendant-tree exit barrier is a POSIX scoped-launch contract; Windows
// has no process groups or tree receipt and is covered by native-close tests below.
it.skipIf(process.platform === "win32").each([false, true])("keeps status, UI, wait and join finishing until the owned tree exits (receipt %s)", async finalAnswer => {
  const f = await fixture({ finalAnswer });
  let waited = false;
  let joined = false;
  const wait = f.manager.wait(f.handle.id).then(result => { waited = true; return result; });
  const join = f.manager.join(f.handle.id).then(() => { joined = true; });
  await vi.advanceTimersByTimeAsync(0);
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "finishing", text: "QUIESCENT" });
  expect(f.manager.listForUi().find(run => run.id === f.handle.id)?.status).toBe("finishing");
  f.close(); // Worker close is not descendant/tree exit.
  await vi.advanceTimersByTimeAsync(1_000);
  expect(waited).toBe(false);
  expect(joined).toBe(false);
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  const next = await f.manager.spawn({ task: "must not overlap finishing", transport: "process" });
  expect(next.status).toBe("queued");
  await f.manager.stop(next.id);
  f.exit();
  await vi.advanceTimersByTimeAsync(0);
  expect(await wait).toMatchObject({ status: "completed", text: "QUIESCENT" });
  await join;
  expect(f.manager.status(f.handle.id).status).toBe("completed");
});

it.each([
  { workerFinishing: false, finalAnswer: false },
  { workerFinishing: true, finalAnswer: false },
  { workerFinishing: true, finalAnswer: true },
  { workerFinishing: true, finalAnswer: true, milestone: true },
])("does not poll liveness or arm a repeating status timer while finishing (worker $workerFinishing, receipt $finalAnswer)", async options => {
  const f = await fixture(options);
  await vi.advanceTimersByTimeAsync(0);
  const calls = f.isAlive.mock.calls.length;
  const timerCount = vi.getTimerCount();
  const timeout = vi.spyOn(globalThis, "setTimeout");
  const interval = vi.spyOn(globalThis, "setInterval");
  await vi.advanceTimersByTimeAsync(1_000);
  expect(f.isAlive.mock.calls.length).toBe(calls);
  expect(timeout).not.toHaveBeenCalled();
  expect(interval).not.toHaveBeenCalled();
  expect(vi.getTimerCount()).toBe(timerCount);
  expect(f.stop).not.toHaveBeenCalled();
  f.exit();
  await vi.advanceTimersByTimeAsync(0);
  expect((await f.manager.wait(f.handle.id)).status).toBe("completed");
});

it("preserves worker cleanup diagnostics appended after the sealed answer snapshot", async () => {
  const f = await fixture({ finalAnswer: true });
  await vi.advanceTimersByTimeAsync(0);
  const record = JSON.parse(fs.readFileSync(f.statusFile, "utf8"));
  fs.writeFileSync(f.statusFile, JSON.stringify({ ...record, warnings: ["finished with forced cleanup of 1 descendants"] }));
  f.exit();
  await vi.advanceTimersByTimeAsync(0);
  expect(await f.manager.wait(f.handle.id)).toMatchObject({ status: "completed", warnings: ["finished with forced cleanup of 1 descendants"] });
});

// Forced descendant counts and tree receipts are POSIX-only; Windows taskkill
// owns native close instead and cannot provide this tree-specific assertion.
it.skipIf(process.platform === "win32")("joins the killed child-tree receipt after the existing bounded cleanup grace", async () => {
  const f = await fixture({ killOnStop: true });
  await vi.advanceTimersByTimeAsync(0);
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  f.close();
  await vi.advanceTimersByTimeAsync(6_999);
  expect(f.stop).not.toHaveBeenCalled();
  expect(f.isAlive).not.toHaveBeenCalled();
  expect(f.manager.isSettled(f.handle.id)).toBe(false);
  await vi.advanceTimersByTimeAsync(1);
  expect(f.stop).toHaveBeenCalledOnce();
  expect(await f.manager.wait(f.handle.id)).toMatchObject({ status: "completed", warnings: ["finished with forced cleanup of 1 descendants"] });
});

it("uses one bounded safety check, not periodic scans, for an older adapter without close events", async () => {
  const f = await fixture({ legacy: true, legacyNoEvents: true });
  await vi.advanceTimersByTimeAsync(0);
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  expect(f.isAlive).toHaveBeenCalledOnce();
  const timeout = vi.spyOn(globalThis, "setTimeout");
  await vi.advanceTimersByTimeAsync(1_000);
  expect(f.isAlive).toHaveBeenCalledOnce();
  expect(timeout).not.toHaveBeenCalled();
  f.exit();
  await vi.advanceTimersByTimeAsync(6_000);
  expect((await f.manager.wait(f.handle.id)).status).toBe("completed");
  expect(f.isAlive).toHaveBeenCalledTimes(2);
  expect(f.stop).not.toHaveBeenCalled();
});

// Failure of the owned tree watcher is specific to POSIX event/tree custody;
// native Windows close has no descendant-tree watcher to fail.
it.skipIf(process.platform === "win32").each([false, true])("retains failed custody and cannot revive from a late terminal write (receipt %s)", async finalAnswer => {
  const f = await fixture({ finalAnswer });
  await vi.advanceTimersByTimeAsync(0);
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  f.failTree();
  await vi.advanceTimersByTimeAsync(0);
  const result = await f.manager.wait(f.handle.id);
  expect(result).toMatchObject({ status: "failed", text: "QUIESCENT", errorCode: "CUSTODY_UNCONFIRMED" });
  const lateWorkerWrite = JSON.stringify({ ...result, status: "completed", text: "late worker write" });
  fs.writeFileSync(f.statusFile, lateWorkerWrite);
  // Neither worker progress nor the older unauthenticated custody snapshot may
  // override main's manager-owned, authenticated settlement publication.
  fs.writeFileSync(path.join(path.dirname(f.statusFile), "custody-failure.json"), lateWorkerWrite);
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "failed", text: "QUIESCENT", errorCode: "CUSTODY_UNCONFIRMED" });
  expect(await f.manager.wait(f.handle.id)).toMatchObject({ status: "failed", text: "QUIESCENT", errorCode: "CUSTODY_UNCONFIRMED" });
  expect(f.manager.listForUi().find(run => run.id === f.handle.id)?.status).toBe("failed");
  const next = await f.manager.spawn({ task: "must not overlap failed custody", transport: "process" });
  expect(next.status).toBe("queued");
  await f.manager.stop(next.id);
  await expect(f.manager.close()).rejects.toThrow(/execution exit unconfirmed/);
  managers.splice(managers.indexOf(f.manager), 1);
});

// This probe seeds POSIX descendant births while the live leader can attest
// parentage. Windows has no descendant census; its native-close path must not
// perform this entry probe (the native Windows contract is asserted below).
it.skipIf(process.platform === "win32")("captures legacy custody once before native close without polling", async () => {
  const f = await fixture({ legacy: true });
  await vi.advanceTimersByTimeAsync(1_000);
  expect(f.isAlive).toHaveBeenCalledOnce();
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  f.exit();
  await vi.advanceTimersByTimeAsync(0);
  expect((await f.manager.wait(f.handle.id)).status).toBe("completed");
  expect(f.isAlive).toHaveBeenCalledTimes(2);
  expect(f.stop).not.toHaveBeenCalled();
});

// Windows waits for natural native close, then runs taskkill before joining
// waitForClose; SIGTERM/group ordering cannot be simulated by changing platform.
it.runIf(process.platform === "win32")("joins Windows stop before a native-close wait can latch timeout debt", async () => {
  const f = await fixture({ killOnStop: true });
  await vi.advanceTimersByTimeAsync(6_999);
  expect(f.stop).not.toHaveBeenCalled();
  expect(f.isAlive).not.toHaveBeenCalled();
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  await vi.advanceTimersByTimeAsync(1);
  expect(f.stop).toHaveBeenCalledOnce();
  expect(await f.manager.wait(f.handle.id)).toMatchObject({ status: "completed" });
});

// Windows-only native event contract: the recorded config can model it on
// Linux, but this file never changes process.platform. Keep reason: Luna's
// recorded custody failure; primary exit is not captured close or stop proof.
it.runIf(process.platform === "win32")("does not let a passive Windows close deadline poison a later exact stop", async () => {
  const f = await fixture({ recordedNative: true });
  let waited = false;
  let joined = false;
  let stopped = false;
  const wait = f.manager.wait(f.handle.id).then(result => { waited = true; return result; });
  const join = f.manager.join(f.handle.id).then(() => { joined = true; });
  f.primaryExit(); // t=0: terminal record + exit, deliberately no captured close.
  await vi.advanceTimersByTimeAsync(0);
  expect(f.nativeHandle?.primaryExit).toBeDefined();
  expect(f.nativeHandle?.waitForClose).toHaveBeenCalledOnce();
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  await vi.advanceTimersByTimeAsync(6_000);
  const stop = f.manager.stop(f.handle.id).then(result => { stopped = true; return result; });
  await vi.advanceTimersByTimeAsync(0);
  expect(f.stop).toHaveBeenCalledOnce();
  expect(spawn).toHaveBeenCalledOnce(); // Already exited: no taskkill helper.
  await vi.advanceTimersByTimeAsync(1_000); // t=7000: old passive bound only.
  expect(f.nativeHandle?.lostContact?.()).toBeUndefined();
  expect(f.manager.isSettled(f.handle.id)).toBe(false);
  expect([waited, joined, stopped]).toEqual([false, false, false]);
  await vi.advanceTimersByTimeAsync(200);
  f.close(); // t=7200, inside the exact stop's own absolute t=13000 deadline.
  await vi.advanceTimersByTimeAsync(0);
  expect(await stop).toMatchObject({ status: "completed", text: "QUIESCENT" });
  expect(await wait).toMatchObject({ status: "completed", text: "QUIESCENT" });
  await join;
  expect(f.nativeHandle?.lostContact?.()).toBeUndefined();
  expect(f.manager.isSettled(f.handle.id)).toBe(true);
  expect(f.kill).not.toHaveBeenCalled();
  expect(f.child.kill).not.toHaveBeenCalled();
});

it.runIf(process.platform === "win32")("still bounds passive Windows close custody at 7000ms without an explicit stop", async () => {
  const f = await fixture({ recordedNative: true });
  let waited = false;
  const wait = f.manager.wait(f.handle.id).then(result => { waited = true; return result; });
  f.primaryExit();
  await vi.advanceTimersByTimeAsync(6_999);
  expect(f.nativeHandle?.waitForClose).toHaveBeenCalledOnce();
  expect(f.nativeHandle?.lostContact?.()).toBeUndefined();
  expect(waited).toBe(false);
  expect(f.stop).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(1);
  expect(f.nativeHandle?.lostContact?.()).toMatch(/native close within 7000ms/);
  expect(await wait).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  expect(spawn).toHaveBeenCalledOnce();
  expect(f.kill).not.toHaveBeenCalled();
  expect(f.child.kill).not.toHaveBeenCalled();
  f.close();
  await vi.advanceTimersByTimeAsync(0);
  expect(f.manager.status(f.handle.id).status).toBe("failed");
});

it("retries exact transient stop custody after close without freezing an early failure", async () => {
  const f = await fixture({ workerFinishing: true, killOnStop: true, stopFailures: 1 });
  await vi.advanceTimersByTimeAsync(0);
  await expect(f.manager.stop(f.handle.id)).resolves.toMatchObject({ status: "completed", text: "QUIESCENT" });
  await vi.advanceTimersByTimeAsync(0);
  expect(await f.manager.wait(f.handle.id)).toMatchObject({ status: "completed", text: "QUIESCENT" });
  expect(f.stop).toHaveBeenCalledTimes(2);
});

it("keeps repeated stop failure terminal even when the primary worker closed", async () => {
  const f = await fixture({ workerFinishing: true, killOnStop: true, stopFailures: Infinity });
  await vi.advanceTimersByTimeAsync(0);
  await expect(f.manager.stop(f.handle.id)).resolves.toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  await vi.advanceTimersByTimeAsync(0);
  const result = await f.manager.wait(f.handle.id);
  expect(result).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  fs.writeFileSync(f.statusFile, JSON.stringify({ ...result, status: "completed" }));
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  await f.manager.close(); // Exact native exit can clear custody, not revive the outcome.
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  managers.splice(managers.indexOf(f.manager), 1);
});

it("never retries a deadline stop through #stop: a transient timeout stop failure stays failed, not stopped", async () => {
  const f = await fixture({ workerRunning: true, timeoutMs: 100, killOnStop: true, stopFailures: 1 });
  await vi.advanceTimersByTimeAsync(2_000);
  const result = await f.manager.wait(f.handle.id);
  expect(result).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  expect(f.stop).toHaveBeenCalledOnce();
  await f.manager.close(); // Exact native exit (second stop succeeds) cannot revive the outcome.
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "failed", errorCode: "CUSTODY_UNCONFIRMED" });
  managers.splice(managers.indexOf(f.manager), 1);
});

// The Windows barrier is captured native close, not a POSIX tree receipt.
// Keep the same public status/UI/wait/join and admission assertions on Windows.
it.runIf(process.platform === "win32").each([false, true])("keeps status, UI, wait and join finishing until native Windows close (receipt %s)", async finalAnswer => {
  const f = await fixture({ finalAnswer });
  let waited = false;
  let joined = false;
  const wait = f.manager.wait(f.handle.id).then(result => { waited = true; return result; });
  const join = f.manager.join(f.handle.id).then(() => { joined = true; });
  await vi.advanceTimersByTimeAsync(1_000);
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "finishing", text: "QUIESCENT" });
  expect(f.manager.listForUi().find(run => run.id === f.handle.id)?.status).toBe("finishing");
  expect(waited).toBe(false);
  expect(joined).toBe(false);
  expect(f.isAlive).not.toHaveBeenCalled();
  const next = await f.manager.spawn({ task: "must not overlap finishing", transport: "process" });
  expect(next.status).toBe("queued");
  await f.manager.stop(next.id);
  f.exit();
  await vi.advanceTimersByTimeAsync(0);
  expect(await wait).toMatchObject({ status: "completed", text: "QUIESCENT" });
  await join;
  expect(f.manager.status(f.handle.id).status).toBe("completed");
});

// This is the actual Windows native-close contract, not a POSIX process with
// process.platform rewritten: taskkill/close ordering is platform-owned.
it.runIf(process.platform === "win32")("keeps the Windows native-close contract: no pre-close probe, one checked probe after close", async () => {
  const f = await fixture();
  await vi.advanceTimersByTimeAsync(1_000);
  // Windows isAlive() does not enumerate descendants, so there is nothing to seed.
  expect(f.isAlive).not.toHaveBeenCalled();
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  f.exit();
  await vi.advanceTimersByTimeAsync(0);
  expect((await f.manager.wait(f.handle.id)).status).toBe("completed");
  expect(f.isAlive).toHaveBeenCalledOnce();
  expect(f.stop).not.toHaveBeenCalled();
});
