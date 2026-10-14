import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentTransportHandle } from "../src/agents/types.js";
import { saveFinalAnswerReceipt } from "../src/worker/terminal-answer.js";

// Recorded-call probe: never launch, signal, or delete a real process/tree locally.
const managers: AgentManager[] = [];
const exits: (() => void)[] = [];
// Existing process.platform descriptor seam (tests/agent-manager.test.ts): the
// fixture pins the platform whose real ProcessTransport handle it represents,
// so hosted Windows and Linux runs exercise the same contract.
const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
afterEach(async () => {
  exits.splice(0).forEach(exit => exit());
  await vi.advanceTimersByTimeAsync(500);
  try { await Promise.all(managers.splice(0).map(manager => manager.close())); }
  finally {
    Object.defineProperty(process, "platform", nativePlatform);
    vi.useRealTimers();
    vi.restoreAllMocks();
  }
});

async function fixture(options: { finalAnswer?: boolean; killOnStop?: boolean; legacy?: boolean; legacyNoEvents?: boolean; workerFinishing?: boolean; milestone?: boolean; platform?: "linux" | "win32"; stopFailures?: number; workerRunning?: boolean; timeoutMs?: number } = {}) {
  // Only Linux scoped launches publish treeClosed/events liveness. Real Windows
  // ProcessTransport keeps its native-close contract: closed plus a bounded
  // waitForClose, never a tree receipt (process-transport.ts launch()).
  const win32 = options.platform === "win32";
  Object.defineProperty(process, "platform", { ...nativePlatform, value: win32 ? "win32" : "linux" });
  vi.useFakeTimers();
  vi.spyOn(fs, "rmSync").mockImplementation(() => {});
  vi.spyOn(fs.promises, "rm").mockResolvedValue();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-finishing-events-"));
  let alive = true;
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
    alive = false; close(); treeExit();
  };
  let stopAttempts = 0;
  const stop = vi.fn(async () => {
    if (options.killOnStop) exit();
    if (++stopAttempts <= (options.stopFailures ?? 0)) throw new Error("injected transient stop failure");
    await treeClosed;
  });
  exits.push(exit);
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
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
      ...(options.legacy || win32 ? {} : { treeClosed }), isAlive, stop, finishingGraceMs: 7_000,
      forcedCleanupCount: () => options.killOnStop && !alive ? 1 : 0,
      lostContact: () => lost } satisfies AgentTransportHandle;
  });
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0, deniedModels: [], timeoutMs: 30_000, maxConcurrent: 1, ...(options.timeoutMs !== undefined ? { timeoutMs: options.timeoutMs } : {}) }, { workerPath: path.join(root, "worker.mjs"), runRoot: path.join(root, "runs") });
  managers.push(manager);
  const handle = await manager.spawn({ task: "child outlives worker", transport: "process" });
  return { manager, handle, isAlive, stop, close, exit, statusFile: statusFile!,
    failTree: () => { lost = "owned tree watcher failed"; close(); rejectTree(new Error(lost)); } };
}

it.each([false, true])("keeps status, UI, wait and join finishing until the owned tree exits (receipt %s)", async finalAnswer => {
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

it("joins the killed child-tree receipt after the existing bounded cleanup grace", async () => {
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

it.each(["linux", "win32"] as const)("uses one bounded safety check, not periodic scans, for an older adapter without close events (%s)", async platform => {
  const f = await fixture({ legacy: true, legacyNoEvents: true, platform });
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

it.each([false, true])("retains failed custody and cannot revive from a late terminal write (receipt %s)", async finalAnswer => {
  const f = await fixture({ finalAnswer });
  await vi.advanceTimersByTimeAsync(0);
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  f.failTree();
  await vi.advanceTimersByTimeAsync(0);
  const result = await f.manager.wait(f.handle.id);
  expect(result).toMatchObject({ status: "failed", text: "QUIESCENT", errorCode: "CUSTODY_UNCONFIRMED" });
  fs.writeFileSync(f.statusFile, JSON.stringify({ ...result, status: "completed", text: "late worker write" }));
  expect(f.manager.status(f.handle.id)).toMatchObject({ status: "failed", text: "QUIESCENT", errorCode: "CUSTODY_UNCONFIRMED" });
  expect(f.manager.listForUi().find(run => run.id === f.handle.id)?.status).toBe("failed");
  const next = await f.manager.spawn({ task: "must not overlap failed custody", transport: "process" });
  expect(next.status).toBe("queued");
  await f.manager.stop(next.id);
  await expect(f.manager.close()).rejects.toThrow(/execution exit unconfirmed/);
  managers.splice(managers.indexOf(f.manager), 1);
});

it("captures legacy custody once before native close without polling", async () => {
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

it("joins Windows stop before a native-close wait can latch timeout debt", async () => {
  const f = await fixture({ platform: "win32", killOnStop: true });
  await vi.advanceTimersByTimeAsync(6_999);
  expect(f.stop).not.toHaveBeenCalled();
  expect(f.isAlive).not.toHaveBeenCalled();
  expect(f.manager.status(f.handle.id).status).toBe("finishing");
  await vi.advanceTimersByTimeAsync(1);
  expect(f.stop).toHaveBeenCalledOnce();
  expect(await f.manager.wait(f.handle.id)).toMatchObject({ status: "completed", warnings: ["finished with forced cleanup of 1 descendants"] });
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

it("keeps the Windows native-close contract: no pre-close probe, one checked probe after close", async () => {
  const f = await fixture({ platform: "win32" });
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
