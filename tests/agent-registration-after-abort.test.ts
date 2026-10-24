import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AGENT_STATUS_POLL_INTERVAL_MS } from "../src/agents/constants.js";
import { terminalAgentStatuses } from "../src/agents/lifecycle.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentHandleInfo, AgentRunRecord, AgentRunResult, AgentTransportHandle, AgentTransportLaunch } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { UNRESOLVED_WORKER_FILE, hasUnresolvedWorker } from "../src/storage/retention.js";
import { recordManagerLaunchRemovals } from "./fixtures/manager-launch-heap-probe.js";

// A caller abort can land while ProcessTransport.launch is still pending, so the
// worker registers with an already-aborted signal. Force exactly that boundary
// through the public launch seam and the real run -> onSpawned -> wait joins.
// Recorded handles only (no real workers). Manager removals are recorded, never
// executed; only the test's own private root is removed afterwards.

const PID = "2147483647";
// Fake time only, never wall-clock: the budget the existing close-custody fixture
// advances, covering the manager's bounded drain/observation windows.
const BOUND_MS = 10_000;

const roots: string[] = [];
let removals: ReturnType<typeof recordManagerLaunchRemovals> | undefined;
afterEach(() => {
  vi.useRealTimers(); vi.restoreAllMocks();
  removals?.restore(); removals = undefined;
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-registration-abort-")); roots.push(root);
  removals = recordManagerLaunchRemovals();
  const settled = vi.fn();
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", timeoutMs: 60_000,
    maxConcurrent: 1, retainRuns: true, budgetUsd: 0, notifyOnComplete: false, sessionExport: false },
  { runRoot: path.join(root, "runs"), workerPath: path.join(root, "worker.mjs"), onSettled: settled });
  return { manager, settled, recorded: removals.recorded };
};
const task = (text: string) => ({ task: text, transport: "process" as const, extensions: false });
const settledStatuses = (settled: ReturnType<typeof vi.fn>, id: string) =>
  settled.mock.calls.filter(([result]) => result.id === id).map(([result]) => result.status);

const writeStatus = (request: AgentTransportLaunch, status: "running" | "completed"): void => {
  const now = Date.now();
  const record: AgentRunRecord = { id: request.id, name: request.name, task: "registration boundary", status,
    runner: "pi", transport: "process", sessionId: PID, cwd: request.cwd, startedAt: now, updatedAt: now,
    ...(status === "completed" ? { finishedAt: now } : {}), turns: status === "completed" ? 1 : 0, toolCalls: 0,
    text: status === "completed" ? "done" : "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
  writeJsonAtomic(request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!, record);
};

/** The first launch is the run under test, parked at transport completion. Later
 * launches are admission probes whose recorded handle exits when stopped. */
const gatedLaunch = (first: (request: AgentTransportLaunch) => AgentTransportHandle) => {
  let enter!: () => void; const entered = new Promise<void>(resolve => { enter = resolve; });
  let finish!: () => void; const gate = new Promise<void>(resolve => { finish = resolve; });
  let probe!: () => void; const probed = new Promise<void>(resolve => { probe = resolve; });
  const launched: string[] = [];
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    launched.push(request.id);
    if (launched.length > 1) {
      probe();
      let alive = true;
      return { kind: "process", sessionId: PID, isAlive: async () => alive, stop: async () => { alive = false; } };
    }
    writeStatus(request, "running");
    enter(); await gate;
    return first(request);
  });
  return { entered, finish, probed, launched };
};
type Lab = ReturnType<typeof gatedLaunch>;

type Join<T> = { outcome: { status: "pending" } | { status: "fulfilled"; value: T } | { status: "rejected"; reason: unknown } };
/** Attach to the original promise at once: never an unhandled or lost outcome. */
const observe = <T>(promise: Promise<T>): Join<T> => {
  const join: Join<T> = { outcome: { status: "pending" } };
  promise.then(value => { join.outcome = { status: "fulfilled", value }; },
    reason => { join.outcome = { status: "rejected", reason }; });
  return join;
};
/** Drive only existing fake-clock timers, stopping as soon as every join settled. */
const advanceUntilSettled = async (...joins: Join<unknown>[]): Promise<void> => {
  for (let elapsed = 0; elapsed < BOUND_MS && joins.some(join => join.outcome.status === "pending");
    elapsed += AGENT_STATUS_POLL_INTERVAL_MS) await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
};
const fulfilled = <T>(join: Join<T>): T => {
  expect(join.outcome.status, "the run/wait join must settle instead of hanging").toBe("fulfilled");
  return (join.outcome as { value: T }).value;
};
const rejected = (join: Join<unknown>): Error => {
  expect(join.outcome.status, "the run/wait join must fail honestly instead of hanging").toBe("rejected");
  const { reason } = join.outcome as { reason: unknown };
  expect(reason).toBeInstanceOf(Error);
  return reason as Error;
};

/** run -> spawn -> onSpawned (an independent unbounded public wait join) -> wait. With
 * actorId it is the actor drain's own internal run: the existing actorId and the
 * drain's activation-generation predicate, through the same public run API. */
const runWithSpawnedWait = (manager: AgentManager, signal: AbortSignal, actorId?: string) => {
  const spawned: { handle?: AgentHandleInfo; waited?: Join<AgentRunResult> } = {};
  const ran = observe(manager.run({ ...task("registration boundary"), ...(actorId ? { actorId } : {}) }, signal, handle => {
    spawned.handle = handle; spawned.waited = observe(manager.wait(handle.id));
  }, actorId ? () => !signal.aborted : undefined));
  return { ran, spawned };
};

/** One permit returns: the queued probe launches once and a further spawn still queues. */
const expectOnePermitReturned = async (manager: AgentManager, lab: Lab, id: string, blocked: AgentHandleInfo) => {
  await lab.probed;
  expect(lab.launched).toEqual([id, blocked.id]);
  const next = await manager.spawn(task("second admission probe"));
  expect(next.status).toBe("queued");
  expect(await manager.stop(next.id)).toMatchObject({ status: "stopped" });
  expect(await manager.stop(blocked.id)).toMatchObject({ status: "stopped" });
  expect(lab.launched).toEqual([id, blocked.id]);
};

describe("caller abort before the transport launch returns (registration after abort)", () => {
  it("observes the exact exit of an exact-contact worker, then settles one stopped result", async () => {
    vi.useFakeTimers();
    const { manager, settled, recorded } = setup();
    let exited = false;
    // Contact stays exact; the stop request itself is never acknowledged.
    const stop = vi.fn(async () => { throw new Error("close acknowledgment lost"); });
    const lab = gatedLaunch(() => ({ kind: "process", sessionId: PID, stop,
      isAlive: async () => !exited, lostContact: () => undefined }));
    const abort = new AbortController();
    const { ran, spawned } = runWithSpawnedWait(manager, abort.signal);
    try {
      await lab.entered;
      abort.abort(); // caller abort while the transport launch is pending
      lab.finish(); // then the transport returns: registration sees an aborted signal
      await vi.advanceTimersByTimeAsync(0);
      expect(spawned.handle, "a late registration returns a custody handle").toBeDefined();
      const id = spawned.handle!.id, waited = spawned.waited!;
      const directory = manager.runDirectory(id)!, marker = path.join(directory, UNRESOLVED_WORKER_FILE);
      // The owned cancelled-launch stop ran and was refused while the worker lives.
      expect(stop).toHaveBeenCalled();
      expect(hasUnresolvedWorker(directory)).toBe(true);
      // Control: a run whose signal aborted before transport creation fails unlaunched.
      await expect(manager.run(task("aborted before transport creation"), abort.signal)).rejects.toThrow();
      expect(lab.launched).toEqual([id]);
      const blocked = await manager.spawn(task("admission probe"));
      expect(blocked.status).toBe("queued");

      // Exit unknown but observable: no rejection and no terminal publication.
      for (let elapsed = 0; elapsed < BOUND_MS; elapsed += AGENT_STATUS_POLL_INTERVAL_MS) {
        await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
      }
      expect([ran.outcome.status, waited.outcome.status]).toEqual(["pending", "pending"]);
      expect(manager.isSettled(id)).toBe(false);
      expect(manager.hasRunCustody(id)).toBe(true);
      expect(terminalAgentStatuses.has(manager.status(id).status)).toBe(false);
      expect(settledStatuses(settled, id)).toEqual([]);
      expect(recorded).not.toContain(marker);

      // The worker exits by itself after the refused stop; only ongoing
      // observation of this exact handle can prove it.
      exited = true;
      await advanceUntilSettled(ran, waited);
      const result = fulfilled(ran);
      expect(result).toMatchObject({ id, status: "stopped" });
      expect(result.errorCode).toBeUndefined();
      expect(fulfilled(waited)).toMatchObject({ id, status: "stopped" });
      expect(manager.isSettled(id)).toBe(true);
      expect(settledStatuses(settled, id)).toEqual(["stopped"]);
      expect(recorded).toContain(marker); // exact exit discharged its marker (recorded only)
      await expectOnePermitReturned(manager, lab, id, blocked);
    } finally {
      exited = true; lab.finish();
      if (spawned.handle) await manager.stop(spawned.handle.id).catch(() => undefined);
      await manager.close();
      await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
    }
  });

  it("fails the active run/wait joins promptly on unobservable exit, keeps custody unsettled, then recovers once", async () => {
    vi.useFakeTimers();
    const { manager, settled, recorded } = setup();
    let debt: string | undefined, recovered = false;
    // Strict counterexample handle: the exact native stop leaves immutable tree
    // debt (a cancelled launch never completes logically on it), and false
    // primary liveness is not exit proof.
    const stop = vi.fn(async () => { if (!recovered) debt ??= "native tree exit observation unavailable"; });
    const lab = gatedLaunch(() => ({ kind: "process", sessionId: PID, stop, waitForClose: async () => {},
      isAlive: async () => false, lostContact: () => debt, stopDebt: () => debt }));
    const abort = new AbortController();
    const { ran, spawned } = runWithSpawnedWait(manager, abort.signal);
    try {
      await lab.entered;
      abort.abort();
      lab.finish();
      await vi.advanceTimersByTimeAsync(0);
      expect(spawned.handle, "a late registration returns a custody handle").toBeDefined();
      const id = spawned.handle!.id, waited = spawned.waited!;
      const directory = manager.runDirectory(id)!, marker = path.join(directory, UNRESOLVED_WORKER_FILE);
      // The owned cancelled-launch stop ran once and its refusal has closed.
      expect(stop).toHaveBeenCalledTimes(1);
      expect(debt).toBeDefined();
      expect(hasUnresolvedWorker(directory)).toBe(true);

      await advanceUntilSettled(ran, waited);
      for (const join of [ran, waited]) {
        const error = rejected(join);
        expect(error.message).toMatch(/unconfirmed/i);
        expect(error.message).not.toMatch(/still running|timed out/i);
      }
      // Separately from the error: nonterminal, truthful custody; no synthetic success.
      expect(manager.isSettled(id)).toBe(false);
      expect(manager.hasRunCustody(id)).toBe(true);
      expect(terminalAgentStatuses.has(manager.status(id).status)).toBe(false);
      expect(hasUnresolvedWorker(directory)).toBe(true);
      expect(recorded).not.toContain(marker);
      expect(settledStatuses(settled, id)).toEqual([]);
      await expect(manager.cleanup(id)).rejects.toThrow(/Cannot clean up/);
      await expect(manager.stop(id)).rejects.toThrow(/execution exit unconfirmed/);
      const blocked = await manager.spawn(task("admission probe"));
      expect(blocked.status).toBe("queued");
      expect(lab.launched).toEqual([id]);

      // Fixture-only exact recovery of the same handle: exit proof is now available.
      recovered = true; debt = undefined;
      expect(await manager.stop(id)).toMatchObject({ id, status: "stopped" });
      expect(manager.isSettled(id)).toBe(true);
      expect(settledStatuses(settled, id)).toEqual(["stopped"]);
      expect(recorded).toContain(marker);
      await expectOnePermitReturned(manager, lab, id, blocked);
    } finally {
      recovered = true; debt = undefined; lab.finish();
      if (spawned.handle) await manager.stop(spawned.handle.id).catch(() => undefined);
      await manager.close();
      await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
    }
  });

  it("keeps an actor's internal run joined while its public waits fail on unobservable exit, then recovers once", async () => {
    vi.useFakeTimers();
    const { manager, settled, recorded } = setup();
    let debt: string | undefined, recovered = false;
    // The same strict counterexample handle as the ordinary fallback above.
    const stop = vi.fn(async () => { if (!recovered) debt ??= "native tree exit observation unavailable"; });
    const lab = gatedLaunch(() => ({ kind: "process", sessionId: PID, stop, waitForClose: async () => {},
      isAlive: async () => false, lostContact: () => debt, stopDebt: () => debt }));
    const abort = new AbortController();
    // The actor drain's internal run; onSpawned attaches a public wait on the same handle.
    const { ran, spawned } = runWithSpawnedWait(manager, abort.signal, "registration-actor");
    try {
      await lab.entered;
      abort.abort();
      lab.finish();
      await vi.advanceTimersByTimeAsync(0);
      expect(spawned.handle, "a late registration returns a custody handle").toBeDefined();
      const id = spawned.handle!.id, waited = spawned.waited!;
      const directory = manager.runDirectory(id)!, marker = path.join(directory, UNRESOLVED_WORKER_FILE);
      expect(stop).toHaveBeenCalledTimes(1);
      expect(debt).toBeDefined();
      expect(hasUnresolvedWorker(directory)).toBe(true);

      // The public observation fails honestly ...
      await advanceUntilSettled(waited);
      const error = rejected(waited);
      expect(error.message).toMatch(/unconfirmed/i);
      expect(error.message).not.toMatch(/still running|timed out/i);
      // ... and a public wait attached after the refusal fails too instead of hanging.
      const late = observe(manager.wait(id));
      await advanceUntilSettled(late);
      expect(rejected(late).message).toMatch(/unconfirmed/i);

      // The actor's internal run stays joined to the lifecycle for the whole window:
      // its drain must not unwind while the worker may still be live.
      for (let elapsed = 0; elapsed < BOUND_MS; elapsed += AGENT_STATUS_POLL_INTERVAL_MS) {
        await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
      }
      expect(ran.outcome.status, "the actor's internal run must stay joined to the lifecycle").toBe("pending");
      // Separately from the errors: nonterminal, truthful custody; no synthetic success.
      expect(manager.isSettled(id)).toBe(false);
      expect(manager.hasRunCustody(id)).toBe(true);
      expect(terminalAgentStatuses.has(manager.status(id).status)).toBe(false);
      expect(hasUnresolvedWorker(directory)).toBe(true);
      expect(recorded).not.toContain(marker);
      expect(settledStatuses(settled, id)).toEqual([]);
      await expect(manager.cleanup(id)).rejects.toThrow(/Cannot clean up/);
      await expect(manager.stop(id)).rejects.toThrow(/execution exit unconfirmed/);
      const blocked = await manager.spawn(task("admission probe"));
      expect(blocked.status).toBe("queued");
      expect(lab.launched).toEqual([id]);
      expect(ran.outcome.status).toBe("pending");

      // Fixture-only exact recovery of the same handle: exit proof is now available.
      recovered = true; debt = undefined;
      expect(await manager.stop(id)).toMatchObject({ id, status: "stopped" });
      await advanceUntilSettled(ran);
      expect(fulfilled(ran)).toMatchObject({ id, status: "stopped" });
      expect(manager.isSettled(id)).toBe(true);
      expect(settledStatuses(settled, id)).toEqual(["stopped"]);
      expect(recorded).toContain(marker);
      await expectOnePermitReturned(manager, lab, id, blocked);
    } finally {
      recovered = true; debt = undefined; lab.finish();
      if (spawned.handle) await manager.stop(spawned.handle.id).catch(() => undefined);
      await manager.close();
      await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
    }
  });

  it("control: an unaborted launch through the same boundary completes once", async () => {
    vi.useFakeTimers();
    const { manager, settled } = setup();
    const lab = gatedLaunch(request => {
      writeStatus(request, "completed");
      return { kind: "process", sessionId: PID, isAlive: async () => false, stop: async () => {} };
    });
    const { ran, spawned } = runWithSpawnedWait(manager, new AbortController().signal);
    try {
      await lab.entered;
      lab.finish();
      await vi.advanceTimersByTimeAsync(0);
      expect(spawned.handle).toBeDefined();
      const id = spawned.handle!.id, waited = spawned.waited!;
      await advanceUntilSettled(ran, waited);
      expect(fulfilled(ran)).toMatchObject({ id, status: "completed" });
      expect(fulfilled(waited)).toMatchObject({ id, status: "completed" });
      expect(lab.launched).toEqual([id]);
      expect(settledStatuses(settled, id)).toEqual(["completed"]);
      expect(hasUnresolvedWorker(manager.runDirectory(id)!)).toBe(false);
    } finally {
      lab.finish();
      await manager.close();
      await vi.advanceTimersByTimeAsync(AGENT_STATUS_POLL_INTERVAL_MS);
    }
  });
});
