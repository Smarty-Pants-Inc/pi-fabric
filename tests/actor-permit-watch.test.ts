import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentRunResult, AgentTransportLaunch } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import * as processIdentity from "../src/residency/process-identity.js";

// Keep / Decision C (#9031): failure path not cheaply reachable in a user flow,
// plus the worker-to-host trust boundary. Real managers, queues and semaphore;
// only host process identity/native custody are scripted. No real launch, signal
// or product deletion. This is supplemental integration proof, not installed-system E2E.
const eventually = (assertion: () => void) => vi.waitFor(assertion, { timeout: 5_000, interval: 20 });
// Native close is the normal wake. This bounded observation window also gives
// the existing status/liveness monitor time to reject a forged terminal claim.
const observe = () => new Promise(resolve => setTimeout(resolve, 1_500));
const deletionCalls: string[] = [];
const fixtures: Fixture[] = [];
const fixtureRoots: string[] = [];
const receiptDrains: Array<() => void> = [];
const workers = new Map<number, Worker>();
const claimedGonePid = 899_999_999;
let nextPid = 900_000_000;
let closing = false;
const settledDebtScanPlatforms = process.platform === "win32" ? (["win32"] as const) : (["linux"] as const);
interface Worker {
  launch: AgentTransportLaunch;
  pid: number;
  alive: boolean;
  contactLost?: string;
  identityAlive?: boolean;
  birth?: string;
  probeError?: string;
  descendants: boolean;
  debt?: string;
  record: AgentRunRecord;
  statusFile: string;
  nativeClose: () => void;
  // Optional independent native/tree joins; absent preserves the original fake.
  closeJoin?: Promise<void>;
  stopJoin?: Promise<void>;
  closeStarted: boolean;
  closeJoined: boolean;
  primaryProofScans: number;
}
interface Fixture {
  root: string;
  admission: string;
  agents: AgentManager;
  actors: ActorManager;
  createActors: () => ActorManager;
}

beforeEach(() => {
  closing = false;
  workers.clear();
  fixtureRoots.length = 0;
  deletionCalls.length = 0;
  receiptDrains.length = 0;
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR"]) vi.stubEnv(key, undefined);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (signal !== 0) throw new Error("No real or scripted worker signals allowed");
    const worker = workers.get(pid);
    if (worker) worker.primaryProofScans++;
    const code = pid === claimedGonePid ? "ESRCH"
      : worker?.probeError ?? (worker && !(worker.identityAlive ?? worker.alive) ? "ESRCH" : undefined);
    if (code) throw Object.assign(new Error(code), { code });
    return true; // Other identities (e.g. this fixture's registry lock) stay live.
  });
  const birth = processIdentity.processStartTime;
  vi.spyOn(processIdentity, "processStartTime").mockImplementation(pid => workers.has(pid) ? workers.get(pid)!.birth : birth(pid));
  const quarantine = (file: fs.PathLike) => {
    let target = path.resolve(String(file));
    // Linux lock release addresses our directory through /proc/self/fd/<fd>.
    // Resolve that pinned parent before applying the fixture-only write fence.
    try { target = path.join(fs.realpathSync(path.dirname(target)), path.basename(target)); } catch { /* absent parent */ }
    deletionCalls.push(target);
    if (!fixtureRoots.some(root => target === root || target.startsWith(root + path.sep))) {
      throw new Error(`Deletion outside this test's fixture: ${target}`);
    }
    // Release metadata locks without deleting bytes. Leave all artifacts in TMPDIR.
    if (fs.existsSync(target)) fs.renameSync(target, `${target}.recorded-removal-${deletionCalls.length}`);
  };
  vi.spyOn(fs, "rmSync").mockImplementation(quarantine);
  vi.spyOn(fs, "rmdirSync").mockImplementation(quarantine);
  vi.spyOn(fs, "unlinkSync").mockImplementation(quarantine);
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(() => { deletionCalls.push("actor-log-prune"); });
  vi.spyOn(ActorChildCompletionStore.prototype, "prune").mockImplementation(() => { deletionCalls.push("child-completion-prune"); });
  vi.spyOn(AgentManager.prototype, "cleanup").mockImplementation(async id => {
    deletionCalls.push(`agent-cleanup:${id}`);
    return { cleaned: false };
  });
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async launch => {
    if (launch.authorize?.() === false) throw new Error("Launch not authorized");
    const arg = (key: string) => launch.workerArguments[launch.workerArguments.indexOf(`--${key}`) + 1]!;
    const pid = nextPid++;
    let nativeClose!: () => void;
    const closed = new Promise<void>(resolve => { nativeClose = resolve; });
    const record: AgentRunRecord = {
      id: launch.id, name: launch.name, task: fs.readFileSync(arg("task-file"), "utf8"), status: "running",
      runner: "pi", transport: "process", sessionId: String(pid), cwd: launch.cwd,
      startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    const worker: Worker = { launch, pid, alive: true, birth: "12345", descendants: false,
      record, statusFile: arg("status-file"), nativeClose, closeStarted: false, closeJoined: false, primaryProofScans: 0 };
    workers.set(pid, worker);
    fs.writeFileSync(worker.statusFile, JSON.stringify(record));
    if (closing) complete(worker); // Failed-test teardown also drains any queued launches.
    return {
      kind: "process", sessionId: String(pid), closed,
      stopDebt: () => worker.debt, lostContact: () => worker.contactLost ?? worker.debt,
      waitForClose: async () => {
        // Like the native transport, a healthy live worker owes a close join,
        // not an immediate uncertainty failure at logical settlement.
        worker.closeStarted = true;
        await closed;
        if (worker.closeJoin) await worker.closeJoin;
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Native custody still open");
        worker.closeJoined = true;
      },
      isAlive: async () => !worker.contactLost && (worker.alive || worker.descendants),
      stop: async () => {
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Owned tree exit unconfirmed");
        await closed;
        if (worker.stopJoin) await worker.stopJoin;
      },
    };
  });
});

afterEach(async () => {
  closing = true;
  // Release synthetic native/tree joins even when fixture preparation asserts
  // before a test's try/finally. Never strand custody or contaminate later cases.
  for (const drain of receiptDrains) drain();
  for (const worker of workers.values()) complete(worker);
  // Keep deletion probes installed until every manager has relinquished custody.
  for (const f of [...fixtures].reverse()) {
    // A failed native join can end the baseline monitor before it settles the
    // actor. Explicit fixture stop rejoins only our already-completed fakes.
    for (const id of new Set([...workers.values()].map(worker => worker.launch.id))) {
      await f.agents.stop(id, { consume: false });
    }
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    await f.actors.close();
    await f.agents.close();
  }
  fixtures.length = 0;
  expect(deletionCalls.length).toBeGreaterThan(0);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function fixture(onSettled?: (result: AgentRunResult) => void): Fixture {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-permit-watch-"));
  fixtureRoots.push(root); // Constructors can release metadata locks before fixture() returns.
  const admission = path.join(root, "actors", "admission.json");
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents,
    enabled: true, runner: "pi", transport: "process", maxConcurrent: 1, budgetUsd: 0,
    deniedModels: [], timeoutMs: 24 * 60 * 60_000, retainRuns: true, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    meshRoot: path.join(root, "mesh"), admissionSnapshotPath: admission, ...(onSettled ? { onSettled } : {}),
  });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:permit-test", name: "main", kind: "main", sessionId: "permit-test" };
  const createActors = () => new ActorManager("permit-test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, retentionSweepMs: 20,
    });
  const f = { root, admission, agents, actors: createActors(), createActors };
  fixtures.push(f);
  return f;
}
const snapshot = (f: Fixture) => JSON.parse(fs.readFileSync(f.admission, "utf8")) as {
  held: Array<{ runId: string }>; waiting: Array<{ runId: string }>;
};
const launched = async (f: Fixture, actorId: string): Promise<Worker> => {
  let worker!: Worker;
  await eventually(() => {
    const runId = f.actors.status(actorId).inFlightRun?.id;
    worker = [...workers.values()].find(candidate => candidate.launch.id === runId)!;
    expect(worker).toBeDefined();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
  });
  return worker;
};
const save = (worker: Worker, patch: Partial<AgentRunRecord> & Record<string, unknown>) => {
  worker.record = { ...worker.record, ...patch };
  fs.writeFileSync(worker.statusFile, JSON.stringify(worker.record));
};
function complete(worker: Worker) {
  worker.alive = false;
  worker.descendants = false;
  delete worker.debt;
  delete worker.contactLost;
  delete worker.probeError;
  worker.nativeClose();
  save(worker, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
}
// Durable activation evidence, not an inference from permit counters: releasing
// RUN must not acknowledge or restart the actor's still-owned native-session turn.
const queueEvidence = (f: Fixture, actorId: string) => {
  const directory = path.join(f.root, "actors", actorId);
  const files = fs.readdirSync(directory).filter(file => /^queue-.*\.json$/.test(file));
  expect(files).toHaveLength(1);
  return JSON.parse(fs.readFileSync(path.join(directory, files[0]!), "utf8")) as {
    items: Array<{ id: string; executionStarted: boolean }>;
    latestActivationSequence: number;
  };
};
const create = (f: Fixture, name: string) => f.actors.create({ name, instructions: "Respond", responseMode: "text", coalesce: false });
async function blockedSuccessor(f: Fixture) {
  const actor = await create(f, "successor");
  const answer = f.actors.ask(actor.id, "next independent event");
  void answer.catch(() => undefined);
  await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
  expect(workers.size).toBe(1);
  return { actor, answer };
}

// Recorded native calls let the real retry path reach its launch await while
// host PID proof is deliberately still alive. stop() joins only this fake's
// captured close; no process is created or signalled.
function deferredRetry() {
  const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
  const calls: Array<{ operation: string; id: string; pid?: number }> = [];
  let firstId: string | undefined;
  let attempts = 0;
  let release!: () => void;
  const gate = new Promise<void>(resolve => { release = resolve; });
  vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
    firstId ??= request.id;
    const attempt = request.id === firstId ? ++attempts : 0;
    if (attempt === 2) {
      calls.push({ operation: "launch-pending", id: request.id });
      await gate;
    }
    const handle = await launch(request);
    const worker = workers.get(Number(handle.sessionId))!;
    calls.push({ operation: "launch-resolved", id: request.id, pid: worker.pid });
    return { ...handle,
      isAlive: async () => {
        calls.push({ operation: "isAlive", id: request.id, pid: worker.pid });
        return handle.isAlive();
      },
      stop: async () => {
        calls.push({ operation: "stop", id: request.id, pid: worker.pid });
        if (attempt === 1) worker.nativeClose();
        return handle.stop();
      },
    };
  });
  return { calls, release };
}

// Test-side statement of the existing manager stop/close contract, NOT a
// shortened product timeout (manager.ts: TRANSPORT_EXIT_GRACE_MS * 7).
const TRANSPORT_EXIT_GRACE_MS = 1_000;
const nativeJoinBoundMs = TRANSPORT_EXIT_GRACE_MS * 7;
type NativeJoin = "close" | "stop";
function joinGate() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

// Decorate the EXISTING launch fake, not another manager/queue harness. Native
// close and the admitted scope's tree-empty receipt are independent, as in
// process-transport.ts. waitForClose joins native close ONLY; scoped stop joins
// checked cleanup too. No real process, scope, daemon, signal or deletion exists.
function scopedExitReceipts(rejectMonitor = false) {
  const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
  const tree = joinGate(), cleanup = joinGate(), monitor = joinGate();
  const calls: string[] = [];
  let firstId: string | undefined;
  let monitorHeld = false;
  let nativeDelivered: Promise<void> | undefined;
  vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
    const handle = await launch(request);
    firstId ??= request.id;
    if (request.id !== firstId) return handle;
    const worker = workers.get(Number(handle.sessionId))!;
    return { ...handle, liveness: "events", treeClosed: tree.promise,
      waitForClose: async () => {
        worker.closeStarted = true;
        calls.push("native-join-started");
        await handle.closed;
        worker.closeJoined = true;
        calls.push("native-joined");
      },
      isAlive: async () => {
        calls.push("scope-probe");
        if (rejectMonitor && !monitorHeld) {
          monitorHeld = true;
          calls.push("monitor-held");
          // Registered during the monitor's liveness await, AFTER its real
          // closed callback. This receipt proves that callback was delivered.
          nativeDelivered = handle.closed!.then(() => { calls.push("native-delivered"); });
          await monitor.promise;
        }
        return handle.isAlive();
      },
      stop: async () => {
        calls.push("scope-stop-started");
        await cleanup.promise;
        worker.descendants = false; // Recorded owned-scope cleanup, never a signal.
        calls.push("scope-cleanup-ack");
        await handle.stop();
        await tree.promise; // ACK/path absence is not the checked tree-empty join.
        calls.push("scope-stop-joined");
      },
    };
  });
  const drain = () => { monitor.resolve(); cleanup.resolve(); tree.resolve(); };
  receiptDrains.push(drain);
  return { tree, cleanup, monitor, calls, nativeDelivered: () => nativeDelivered, drain };
}

// Real same-actor drain and durable queue restore must consume each activation
// once. Keep the second successor queued while late old-run events/stop joins
// exercise the real once-only admission release, not just a promise counter.
async function drainTwoSameActorSuccessors(
  f: Fixture, actorId: string, first: Worker, settlements: AgentRunResult[],
) {
  await eventually(() => {
    expect(f.actors.status(actorId).inFlightRun).toBeDefined();
    expect(f.actors.status(actorId).inFlightRun?.id).not.toBe(first.launch.id);
  });
  const second = await launched(f, actorId);
  expect(second.launch.id).not.toBe(first.launch.id);
  expect(second.record.task).toContain("scope successor one");
  expect(workers.size).toBe(2);
  expect(f.actors.status(actorId).queued).toBe(1);
  expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
  expect(settlements.filter(result => result.id === first.launch.id)).toHaveLength(1);
  const firstStatus = f.agents.status(first.launch.id).status;
  // Same-actor single-flight alone could hide a duplicate raw semaphore release.
  // Queue an independent admission probe while successor one owns RUN; cancel
  // its receipt afterwards, without ever creating another worker on the good path.
  const guard = await create(f, "once-only-admission-probe");
  const guardAnswer = f.actors.ask(guard.id, "must not launch through a duplicate old RUN release");
  void guardAnswer.catch(() => undefined);
  await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
  const guardRunId = snapshot(f).waiting[0]!.runId;
  const settledActivations = () => settlements.filter(result => result.id !== guardRunId).map(result => result.id);
  first.nativeClose(); first.nativeClose();
  await f.agents.stop(first.launch.id, { consume: false });
  await f.agents.stop(first.launch.id, { consume: false });
  await observe();
  expect(workers.size, "late old-run joins cannot admit the second queued activation").toBe(2);
  expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
  expect(f.actors.status(actorId).queued).toBe(1);
  expect(f.agents.status(first.launch.id).status).toBe(firstStatus);
  expect(settlements.filter(result => result.id === first.launch.id)).toHaveLength(1);
  expect(snapshot(f).waiting, "old settlement/stop cannot admit the independent guard").toHaveLength(1);
  await f.actors.stop(guard.id);
  await eventually(() => expect(snapshot(f).waiting).toEqual([]));

  complete(second);
  await expect(f.agents.wait(second.launch.id)).resolves.toMatchObject({ status: "completed", text: "finished" });
  await eventually(() => {
    expect(f.actors.status(actorId).inFlightRun).toBeDefined();
    expect(f.actors.status(actorId).inFlightRun?.id).not.toBe(second.launch.id);
  });
  const third = await launched(f, actorId);
  expect(third.launch.id).not.toBe(second.launch.id);
  expect(third.record.task).toContain("scope successor two");
  expect(workers.size).toBe(3);
  complete(third);
  await expect(f.agents.wait(third.launch.id)).resolves.toMatchObject({ status: "completed", text: "finished" });
  await eventually(() => {
    expect(f.actors.inFlightCount()).toBe(0);
    expect(snapshot(f).held).toEqual([]);
    expect(snapshot(f).waiting).toEqual([]);
  });
  expect(settledActivations()).toEqual([first.launch.id, second.launch.id, third.launch.id]);
  expect([...workers.values()].filter(worker => worker.record.task === first.record.task)).toHaveLength(1);
  await f.actors.close();
  f.actors = f.createActors();
  await observe();
  expect(workers.size, "the completed activation must not replay on durable queue restore").toBe(3);
  expect(f.actors.status(actorId).queued).toBe(0);
  expect(f.actors.status(actorId).inFlightRun).toBeUndefined();
  expect(settledActivations()).toEqual([first.launch.id, second.launch.id, third.launch.id]);
}

// Leave polling, snapshot I/O and vi.waitFor on REAL timers. Only the exact
// native 7000 ms deadline is deliverable by this clock; no broad timer advance.
function nativeJoinClock() {
  const schedule = globalThis.setTimeout;
  const cancel = globalThis.clearTimeout;
  const started = Date.now();
  let now = started;
  const deadlines = new Map<ReturnType<typeof setTimeout>, () => void>();
  const date = vi.spyOn(Date, "now").mockImplementation(() => now);
  const timeout = vi.spyOn(globalThis, "setTimeout").mockImplementation(((
    callback: (...args: unknown[]) => void, milliseconds?: number, ...args: unknown[]
  ) => {
    if (milliseconds !== nativeJoinBoundMs) return schedule(callback, milliseconds, ...args);
    const timer = schedule(() => { deadlines.delete(timer); callback(...args); }, milliseconds);
    deadlines.set(timer, () => { cancel(timer); deadlines.delete(timer); callback(...args); });
    return timer;
  }) as typeof setTimeout);
  const clear = vi.spyOn(globalThis, "clearTimeout").mockImplementation(timer => {
    deadlines.delete(timer as ReturnType<typeof setTimeout>);
    cancel(timer);
  });
  return {
    beforeBound: () => { now = started + nativeJoinBoundMs - 1; },
    expire: () => {
      now = started + nativeJoinBoundMs;
      // Snapshot: callbacks may schedule other work, which is NOT auto-fired.
      for (const deliver of [...deadlines.values()]) deliver();
    },
    restore: () => {
      for (const timer of deadlines.keys()) cancel(timer);
      deadlines.clear();
      clear.mockRestore(); timeout.mockRestore(); date.mockRestore();
    },
  };
}

// Normal terminal settlement captures BOTH independent native obligations. A
// stop ACK can carry stopDebt, just like the native transport; it is not a
// whole-tree exit receipt. Only the first worker's first stop is gated.
async function withTerminalJoins(
  fault: NativeJoin,
  priorGone: boolean,
  check: (context: {
    f: Fixture; actor: Awaited<ReturnType<typeof create>>; first: Worker;
    next: Awaited<ReturnType<typeof blockedSuccessor>>;
    third: Awaited<ReturnType<typeof create>>; thirdAnswer: ReturnType<ActorManager["ask"]>;
    close: ReturnType<typeof joinGate>; stop: ReturnType<typeof joinGate>;
    calls: string[]; scan: () => void; clock: ReturnType<typeof nativeJoinClock>;
    stopping: Promise<unknown>;
  }) => Promise<void>,
) {
  const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
  const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
  const close = joinGate(), stop = joinGate();
  const calls: string[] = [];
  let firstPid: number | undefined;
  let stopCaptured = false;
  let draining = false;
  vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
    const handle = await launch(request);
    const worker = workers.get(Number(handle.sessionId))!;
    firstPid ??= worker.pid;
    if (worker.pid !== firstPid) return handle;
    worker.closeJoin = close.promise;
    return { ...handle,
      waitForClose: async () => {
        calls.push("close-started");
        try { await handle.waitForClose!(); calls.push("close-joined"); }
        catch (error) { calls.push("close-rejected"); throw error; }
      },
      stop: async () => {
        if (draining || stopCaptured) return handle.stop();
        stopCaptured = true;
        calls.push("stop-started");
        try { await stop.promise; calls.push("stop-joined"); }
        catch (error) { calls.push("stop-rejected"); throw error; }
      },
    };
  });
  const f = fixture();
  let first: Worker | undefined;
  let stopping: Promise<unknown> | undefined;
  let clock: ReturnType<typeof nativeJoinClock> | undefined;
  try {
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    const actor = await create(f, "terminal-native-reconciliation");
    const answer = f.actors.ask(actor.id, "terminal result, independent captured native joins");
    void answer.catch(() => undefined);
    first = await launched(f, actor.id);
    const next = await blockedSuccessor(f);
    const third = await create(f, "terminal-native-second-successor");
    const thirdAnswer = f.actors.ask(third.id, "must not launch on a duplicate old release");
    void thirdAnswer.catch(() => undefined);
    await eventually(() => expect(snapshot(f).waiting).toHaveLength(2));

    clock = nativeJoinClock();
    first.alive = false;
    first.identityAlive = !priorGone;
    save(first, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
    await eventually(() => expect(first!.closeStarted).toBe(true));
    await expect(answer).resolves.toMatchObject({ text: "finished" });
    // Passive close is distinct from the gated waitForClose receipt.
    first.nativeClose();
    stopping = f.agents.stop(first.launch.id, { consume: false });
    void stopping.catch(() => undefined);
    await eventually(() => expect(calls).toContain("stop-started"));
    expect(calls).toContain("close-started");
    if (fault === "stop") {
      close.resolve();
      await eventually(() => expect(first!.closeJoined).toBe(true));
    }
    // Debt appears AFTER normal terminal admission has captured both joins.
    // Neither a rejected join nor primary death can erase this tree custody.
    first.descendants = true;
    first.debt = "recorded native tree close unconfirmed";
    if (fault === "close") {
      stop.resolve();
      await stopping;
    }
    expect(workers.size).toBe(1);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(2);
    const scan = () => first!.launch.onUnconfirmedExit!(first!.debt ?? "recorded late native observation");
    expect(first.launch.onUnconfirmedExit).toBeTypeOf("function");
    await check({ f, actor, first, next, third, thirdAnswer, close, stop, calls, scan, clock, stopping });
  } finally {
    clock?.restore();
    draining = true;
    close.resolve();
    stop.resolve();
    if (first) { delete first.closeJoin; complete(first); }
    await stopping?.catch(() => undefined);
    try {
      // A RED leaked fence must not strand the real queued actor receipts.
      for (const waiting of snapshot(f).waiting) await f.agents.stop(waiting.runId, { consume: false }).catch(() => undefined);
    } finally { Object.defineProperty(process, "platform", platform); }
  }
}

async function reconciledExactlyOnce(
  context: Parameters<Parameters<typeof withTerminalJoins>[2]>[0], proofScansBefore: number,
) {
  const { f, actor, first, next, third, thirdAnswer, close, stop, scan, stopping } = context;
  const second = await launched(f, next.actor.id);
  // Check BEFORE retention queries, repeated scans or late explicit stops can
  // themselves probe this PID and disguise a stale cached proof at release.
  expect(first.primaryProofScans, "reconciliation must obtain fresh host proof before admission").toBeGreaterThan(proofScansBefore);
  await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
  expect(workers.size, "reconciliation admits only ONE of two queued successors").toBe(2);
  expect(f.agents.heldPermits()).toBe(0);
  expect(first.descendants).toBe(true);
  expect(first.debt).toBeDefined();
  expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
  expect(f.agents.retentionCustodyVeto(actor.id)).toBe(true);
  expect(f.agents.status(first.launch.id).status).toBe("completed");

  scan(); scan();
  // Now supply the separate tree exit/late successful join, not merely RUN
  // proof. Its settlement/stop continuations must not release admission twice.
  first.descendants = false;
  delete first.debt;
  close.resolve(); stop.resolve();
  await stopping.catch(() => undefined);
  first.nativeClose(); first.nativeClose();
  await f.agents.stop(first.launch.id, { consume: false });
  await f.agents.stop(first.launch.id, { consume: false });
  scan(); scan();
  await observe();
  expect(workers.size, "late joins and repeated proof cannot discharge old RUN twice").toBe(2);
  expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
  expect(snapshot(f).waiting).toHaveLength(1);
  complete(second);
  await expect(next.answer).resolves.toMatchObject({ text: "finished" });
  const last = await launched(f, third.id);
  expect(workers.size).toBe(3);
  complete(last);
  await expect(thirdAnswer).resolves.toMatchObject({ text: "finished" });
  await eventually(() => expect(snapshot(f).held).toEqual([]));
}

describe("actor worker permit release (#9031)", () => {
  it.each(["absent", "reused"] as const)("host-proven %s worker releases real admission and launches the next activation", async mode => {
    const f = fixture();
    const actor = await create(f, "permit-owner");
    await f.actors.tell(actor.id, "lost event");
    const first = await launched(f, actor.id);
    const answer = f.actors.ask(actor.id, "next activation on the same actor");
    void answer.catch(() => undefined);
    await eventually(() => expect(f.actors.status(actor.id).queued).toBe(1));
    expect(workers.size).toBe(1);
    // A worker-writable live PID cannot override the host's launch identity.
    save(first, { sessionId: String(process.pid) });
    first.alive = false;
    if (mode === "reused") { first.identityAlive = true; first.birth = "67890"; }
    first.nativeClose(); // Independently checked whole-tree/native exit, not just leader absence.
    await eventually(() => expect(f.agents.status(first.launch.id).status).toBe("failed"));
    let second!: Worker;
    await eventually(() => {
      const runId = f.actors.status(actor.id).inFlightRun?.id;
      expect(runId).not.toBe(first.launch.id);
      second = [...workers.values()].find(candidate => candidate.launch.id === runId)!;
      expect(second).toBeDefined();
      expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
    });
    expect(workers.size).toBe(2);
    complete(second);
    await expect(answer).resolves.toMatchObject({ text: "finished" });
    await eventually(() => expect(snapshot(f).held).toEqual([]));
  });

  it.each(["running", "startup-failed", "stopped-after-progress"] as const)("lost %s activation is not automatically retried or relaunched", async mode => {
    const f = fixture();
    const actor = await create(f, "no-replay");
    const answer = f.actors.ask(actor.id, "unique lost event");
    // Observe rejection immediately, without creating an unhandled-rejection race.
    const outcome = answer.then(value => ({ value }), error => ({ error }));
    const worker = await launched(f, actor.id);
    if (mode === "stopped-after-progress") {
      save(worker, { turns: 2, toolCalls: 1, updatedAt: Date.now() });
      await eventually(() => expect(f.agents.status(worker.launch.id)).toMatchObject({ toolCalls: 1 }));
      save(worker, { status: "stopped", error: "Agent transport exited without a result" });
    } else if (mode === "startup-failed") {
      save(worker, { status: "failed", error: "Agent transport exited without a result" });
    }
    worker.alive = false;
    worker.nativeClose();
    // Plain failed semantics only: no new public error code, notice or API required.
    await eventually(() => expect(f.agents.status(worker.launch.id).status).toBe("failed"));
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect(await outcome).toHaveProperty("error");
    await observe(); // Also covers the existing startup/resume retry delay.
    expect(workers.size).toBe(1);
    expect(snapshot(f).held).toEqual([]);
    await f.actors.close();
    f.actors = f.createActors();
    await observe();
    expect(workers.size).toBe(1); // No replay by the persistent queue restore path either.
    expect(f.actors.status(actor.id).queued).toBe(0);
  });

  it.each(["ESRCH", "different-birth"] as const)("old-attempt %s proof during deferred retry cannot release RUN or kill the live replacement", async proof => {
    const trace = deferredRetry();
    const f = fixture();
    const actor = await create(f, "retry-owner");
    const answer = f.actors.ask(actor.id, "one activation across a bounded startup retry");
    const outcome = answer.then(value => ({ value }), error => ({ error: String(error) }));
    const first = await launched(f, actor.id);
    // Transport exit is known, but host death proof is not available yet. Keep
    // this identity live through every pre-launch #actorWorkerGone check.
    first.alive = false;
    first.identityAlive = true;
    save(first, { status: "failed", error: "Agent transport exited without a result" });
    let next!: Awaited<ReturnType<typeof create>>;
    let nextAnswer!: ReturnType<ActorManager["ask"]>;
    try {
      await eventually(() => expect(trace.calls.filter(call => call.operation === "launch-pending")).toHaveLength(1));
      expect(trace.calls.some(call => call.operation === "stop" && call.pid === first.pid)).toBe(true);
      expect(f.agents.workerIdentity(first.launch.id)?.pid).toBe(first.pid);
      expect(process.kill(first.pid, 0)).toBe(true);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(workers.size).toBe(1); // Second launch is awaiting, not installed.

      if (proof === "ESRCH") first.identityAlive = false;
      else first.birth = "67890";
      // Independent spawn is the event-driven proof hook. The previous native
      // close was already joined while its host identity still appeared alive.
      next = await create(f, "queued-during-retry");
      nextAnswer = f.actors.ask(next.id, "independent actor must wait for replacement");
      void nextAnswer.catch(() => undefined);
      await eventually(() => expect(snapshot(f).waiting.length +
        [...workers.values()].filter(worker => worker.launch.id !== first.launch.id).length).toBe(1));
      await eventually(() => {
        const admission = snapshot(f);
        expect(admission.waiting.length === 1 || admission.held.some(held => held.runId !== first.launch.id)).toBe(true);
      }); // Wait for the real snapshot writer, not a stale pre-spawn image.
      console.info("deferred-retry pending evidence", JSON.stringify({ proof, admission: snapshot(f), calls: trace.calls }));
      expect.soft(snapshot(f).held.map(held => held.runId), "old proof must not release raw RUN while relaunch is pending").toEqual([first.launch.id]);
      expect.soft(snapshot(f).waiting).toHaveLength(1);
      expect.soft(workers.size, "unrelated actor cannot launch during deferred replacement").toBe(1);
    } finally {
      trace.release(); // Even a RED assertion cannot strand teardown at launch.
    }

    let replacement!: Worker;
    await eventually(() => {
      replacement = [...workers.values()].find(worker => worker.launch.id === first.launch.id && worker.pid !== first.pid)!;
      expect(replacement).toBeDefined();
      expect(f.agents.workerIdentity(first.launch.id)?.pid).toBe(replacement.pid);
    });
    expect(replacement.alive).toBe(true);
    expect(process.kill(replacement.pid, 0)).toBe(true);
    // Existing bounded monitor observation, not a new production poller: the
    // replacement must survive a full liveness pass before it publishes success.
    await observe();
    console.info("deferred-retry replacement evidence", JSON.stringify({ proof, admission: snapshot(f), calls: trace.calls }));
    expect.soft(snapshot(f).held.map(held => held.runId), "live replacement inherits the same sole admission").toEqual([first.launch.id]);
    expect.soft(snapshot(f).waiting).toHaveLength(1);
    expect.soft(trace.calls.filter(call => call.operation === "stop" && call.pid === replacement.pid), "old workerGone cannot stop the replacement").toEqual([]);
    expect.soft(f.actors.status(actor.id).inFlightRun?.id).toBe(first.launch.id);
    expect.soft(f.agents.status(first.launch.id).status).toBe("running");
    expect.soft([...workers.values()].filter(worker => worker.launch.id !== first.launch.id)).toHaveLength(0);
    complete(replacement);
    expect(await outcome).toMatchObject({ value: { text: "finished" } });
    const successor = await launched(f, next.id);
    expect(f.agents.status(first.launch.id).status).toBe("completed");
    complete(successor);
    await expect(nextAnswer).resolves.toMatchObject({ text: "finished" });
    await eventually(() => expect(snapshot(f).held).toEqual([]));
    expect([...workers.values()].filter(worker => worker.launch.id === first.launch.id)).toHaveLength(2);
  });

  it("ordinary task startup retry remains bounded and owns RUN across a deferred launch", async () => {
    const trace = deferredRetry();
    const f = fixture();
    const run = await f.agents.spawn({ task: "ordinary retry counterexample" });
    const first = [...workers.values()].find(worker => worker.launch.id === run.id)!;
    first.alive = false;
    first.identityAlive = true;
    save(first, { status: "failed", error: "Agent transport exited without a result" });
    try {
      await eventually(() => expect(trace.calls.filter(call => call.operation === "launch-pending")).toHaveLength(1));
      first.identityAlive = false;
      expect(snapshot(f).held.map(held => held.runId)).toEqual([run.id]);
    } finally { trace.release(); }
    let replacement!: Worker;
    await eventually(() => {
      replacement = [...workers.values()].find(worker => worker.launch.id === run.id && worker.pid !== first.pid)!;
      expect(replacement).toBeDefined();
      expect(f.agents.workerIdentity(run.id)?.pid).toBe(replacement.pid);
    });
    await observe();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([run.id]);
    expect(trace.calls.filter(call => call.operation === "stop" && call.pid === replacement.pid)).toEqual([]);
    complete(replacement);
    await expect(f.agents.wait(run.id)).resolves.toMatchObject({ status: "completed", text: "finished" });
    expect(workers.size).toBe(2);
  });

  it("success committed during the native/tree join is not overwritten by the earlier running snapshot", async () => {
    const f = fixture();
    const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
    vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
      const handle = await launch(request);
      const worker = [...workers.values()].find(candidate => candidate.launch.id === request.id)!;
      return { ...handle, stop: async () => { complete(worker); } };
    });
    const actor = await create(f, "close-race");
    const answer = f.actors.ask(actor.id, "finish during native join");
    const outcome = answer.then(value => ({ value }), error => ({ error }));
    const worker = await launched(f, actor.id);
    worker.alive = false;
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect(await outcome).toMatchObject({ value: { text: "finished" } });
    expect(f.agents.status(worker.launch.id).status).toBe("completed");
    expect(workers.size).toBe(1);
    await eventually(() => expect(snapshot(f).held).toEqual([]));
  });

  it.each([false, true])("preserves the original window refusal with retryable diagnostics after primary exit (progress=%s)", async progress => {
    const f = fixture();
    const actor = await create(f, "window-refusal");
    await f.actors.tell(actor.id, "one terminal refusal, never replay");
    const worker = await launched(f, actor.id);
    const error = "Context exceeds window: request is too large; No API key found; Agent transport exited without a result";
    if (progress) save(worker, { turns: 2, toolCalls: 1, updatedAt: Date.now() });
    worker.alive = false;
    save(worker, { status: "failed", error, finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 1 });
    worker.nativeClose();
    await expect(f.agents.wait(worker.launch.id)).resolves.toMatchObject({ status: "failed", error,
      turns: progress ? 2 : 0, toolCalls: progress ? 1 : 0 });
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect(f.agents.status(worker.launch.id)).toMatchObject({ error });
    expect(JSON.parse(fs.readFileSync(worker.statusFile, "utf8")).error).toBe(error);
    await observe();
    expect(workers.size).toBe(1);
    expect(snapshot(f).held).toEqual([]);
  });

  it.each(["active", "abandoned", "stopped"] as const)("primary death releases only RUN while a live descendant keeps wait and the same-session drain fenced (%s actor)", async mode => {
    const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
    const stops: number[] = [];
    vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
      const handle = await launch(request);
      const worker = workers.get(Number(handle.sessionId))!;
      return { ...handle, stop: async () => {
        stops.push(worker.pid);
        // A real owned-tree stop can kill a descendant. Record that destructive
        // effect instead of having the fake throw and hide premature draining.
        worker.descendants = false;
        worker.nativeClose();
        await handle.stop();
      } };
    });
    const f = fixture();
    const actor = await create(f, "natural-tree-exit");
    await f.actors.tell(actor.id, "primary will crash, descendant keeps writing");
    const first = await launched(f, actor.id);
    await f.actors.tell(actor.id, "next same-session event must wait");
    if (mode === "stopped") {
      save(first, { turns: 3, toolCalls: 1, updatedAt: Date.now() });
      await eventually(() => expect(f.agents.status(first.launch.id)).toMatchObject({ toolCalls: 1 }));
      await f.actors.stop(actor.id); // Abandons the progressed run, without stopping its writer.
      expect(f.actors.status(actor.id)).toMatchObject({ status: "stopped", inFlightRun: { id: first.launch.id } });
    }
    const before = queueEvidence(f, actor.id);
    let settled = false;
    const result = f.agents.wait(first.launch.id).then(value => { settled = true; return value; });
    const next = await blockedSuccessor(f);
    if (mode === "abandoned") f.agents.abandon(first.launch.id);
    first.descendants = true;
    first.alive = false;
    first.nativeClose(); // Primary close is not the native-tree exit receipt.
    const second = await launched(f, next.actor.id);
    await observe();
    expect(stops).not.toContain(first.pid);
    expect(first.descendants).toBe(true);
    expect(settled).toBe(false);
    expect(f.agents.status(first.launch.id).status).toBe("running");
    expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(first.launch.id);
    expect(queueEvidence(f, actor.id)).toEqual(before);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
    expect(workers.size).toBe(2);

    first.descendants = false; // The writer exits naturally; no manager stop drove it.
    await expect(result).resolves.toMatchObject({ status: "failed", error: expect.stringContaining("Actor worker is proven gone") });
    const finalActor = mode === "stopped" ? await create(f, "after-stopped-writer") : actor;
    let finalAnswer: ReturnType<ActorManager["ask"]> | undefined;
    if (mode === "stopped") {
      await eventually(() => expect(f.actors.status(actor.id).inFlightRun).toBeUndefined());
      expect(f.actors.status(actor.id).status).toBe("stopped");
      finalAnswer = f.actors.ask(finalActor.id, "independent event after natural tree exit");
      void finalAnswer.catch(() => undefined);
    }
    await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
    await observe();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
    expect(workers.size).toBe(2); // Old settlement cannot release RUN twice.
    complete(second);
    await expect(next.answer).resolves.toMatchObject({ text: "finished" });
    const sameSession = await launched(f, finalActor.id);
    expect(sameSession.launch.id).not.toBe(first.launch.id);
    complete(sameSession);
    if (finalAnswer) await expect(finalAnswer).resolves.toMatchObject({ text: "finished" });
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect([...workers.values()].filter(worker => worker.record.task === first.record.task)).toHaveLength(1);
  });

  it.each(["live", "unknown-launch-birth", "empty-launch-birth", "unreadable-current-birth", "EPERM", "EIO"] as const)("%s worker retains its permit and blocks the next actor", async mode => {
    if (mode === "unknown-launch-birth" || mode === "empty-launch-birth") {
      const birth = vi.mocked(processIdentity.processStartTime).getMockImplementation()!;
      const unknownBirth = mode === "empty-launch-birth" ? "" : undefined;
      let captured = false;
      vi.mocked(processIdentity.processStartTime).mockImplementation(pid => {
        if (!workers.has(pid)) return birth(pid);
        if (!captured) { captured = true; return unknownBirth; }
        return "67890"; // A later readable birth cannot fill a missing launch identity.
      });
    }
    const f = fixture();
    const actor = await create(f, "retained");
    await f.actors.tell(actor.id, "still running");
    const worker = await launched(f, actor.id);
    await blockedSuccessor(f);
    if (mode === "unreadable-current-birth") delete worker.birth;
    if (mode === "EPERM" || mode === "EIO") { worker.probeError = mode; worker.birth = "67890"; }
    await observe();
    expect(f.agents.status(worker.launch.id).status).toBe("running");
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(workers.size).toBe(1);
  });

  it.each(["sessionId", "gone", "errorCode"] as const)("worker-written %s claim cannot release a live host-owned worker's permit", async claim => {
    const f = fixture();
    const actor = await create(f, "untrusted-status");
    await f.actors.tell(actor.id, "event");
    const worker = await launched(f, actor.id);
    await blockedSuccessor(f);
    if (claim === "sessionId") save(worker, { sessionId: String(claimedGonePid), processStartTime: "99999" });
    if (claim === "gone") save(worker, { workerGone: true, settledLost: true, error: "Actor worker is proven gone" });
    // Deliberately untyped, untrusted disk data; does not require a public WORKER_GONE symbol.
    if (claim === "errorCode") fs.writeFileSync(worker.statusFile, JSON.stringify({ ...worker.record,
      status: "failed", errorCode: "WORKER_GONE", error: "Actor worker is proven gone; execution was lost" }));
    await observe();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(workers.size).toBe(1);
    // Windows may publish the failed logical result before native close. The
    // live host-owned worker still owns RUN, regardless of actor inFlight state.
    expect(f.agents.workerIdentity(worker.launch.id)?.pid).toBe(worker.pid);
    expect(process.kill(worker.pid, 0)).toBe(true);
  });

  it.each(["descendants", "native-debt"] as const)("lost-contact publication fence registers RUN before settlement and releases it once despite %s", async fault => {
    const f = fixture();
    const actor = await create(f, "lost-contact-root");
    await f.actors.tell(actor.id, "unique stranded activation");
    const first = await launched(f, actor.id);
    await f.actors.tell(actor.id, "same actor writer must wait");
    const before = queueEvidence(f, actor.id);
    expect(before.items.map(item => item.executionStarted)).toEqual([true, false]);
    let logicalSettled = false;
    void f.agents.wait(first.launch.id).then(() => { logicalSettled = true; }, () => { logicalSettled = true; });
    const next = await blockedSuccessor(f);

    // ROOT: transport contact fails while the exact host-owned PID is ALIVE.
    // The second variant makes the host probe uncertain (EPERM); a changed
    // birth must not override that failure and authorize early admission.
    if (fault === "native-debt") { first.probeError = "EPERM"; first.birth = "67890"; }
    // Native close normally wakes the monitor; its existing 1 s liveness grace
    // is the bounded safety net needed to reach #markLost -> #settle's fence.
    const contactLostAt = Date.now();
    first.contactLost = "recorded transport contact lost; native PID still alive";
    await eventually(() => {
      const marker = path.join(path.dirname(first.statusFile), "unresolved-worker.json");
      expect(fs.existsSync(marker)).toBe(true);
      expect(JSON.parse(fs.readFileSync(marker, "utf8")).reason).toBe(first.contactLost);
    });
    expect(Date.now() - contactLostAt).toBeGreaterThanOrEqual(1_000);
    expect(first.alive).toBe(true);
    if (fault === "native-debt") expect(() => process.kill(first.pid, 0)).toThrow("EPERM");
    else expect(process.kill(first.pid, 0)).toBe(true);
    expect(logicalSettled).toBe(false);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(first.launch.id);
    expect(queueEvidence(f, actor.id)).toEqual(before);
    expect(workers.size).toBe(1);
    expect(f.agents.heldPermits()).toBe(1); // Fails on 03ac: registration is after the actor fence.

    // Exact primary death frees only RUN, not the native/session writer lease.
    first.alive = false;
    delete first.probeError; // The exact host probe now returns ESRCH, not EPERM.
    if (fault === "descendants") first.descendants = true;
    else first.debt = "native tree close unconfirmed";
    first.nativeClose();
    const second = await launched(f, next.actor.id);
    expect(second.launch.id).not.toBe(first.launch.id);
    expect(f.agents.heldPermits()).toBe(0);
    expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(first.launch.id);
    expect(f.actors.status(actor.id).queued).toBe(1);
    expect(logicalSettled).toBe(false);
    expect(queueEvidence(f, actor.id)).toEqual(before);
    expect(workers.size).toBe(2);

    // A later ordinary native join/settlement must not release that old RUN a
    // second time and admit two writers while the independent second is alive.
    const thirdActor = await create(f, "once-only-successor");
    const thirdAnswer = f.actors.ask(thirdActor.id, "third independent event");
    void thirdAnswer.catch(() => undefined);
    await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
    complete(first);
    await f.agents.stop(first.launch.id, { consume: false });
    await eventually(() => expect(snapshot(f).waiting).toHaveLength(2));
    await observe();
    expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
    expect(workers.size).toBe(2);
    complete(second);
    await expect(next.answer).resolves.toMatchObject({ text: "finished" });
    const third = await launched(f, thirdActor.id);
    await observe();
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(workers.size).toBe(3);
    complete(third);
    await expect(thirdAnswer).resolves.toMatchObject({ text: "finished" });
    const sameActorNext = await launched(f, actor.id);
    expect(sameActorNext.launch.id).not.toBe(first.launch.id);
    expect([...workers.values()].filter(worker => worker.record.task === first.record.task)).toHaveLength(1);
    complete(sameActorNext);
    await eventually(() => expect(snapshot(f).held).toEqual([]));
  });

  it.each(settledDebtScanPlatforms)("settled actor with native debt releases retained RUN only on host proof (%s scan)", async scanPlatform => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const f = fixture();
    try {
      const actor = await create(f, "settled-debt-owner");
      const answer = f.actors.ask(actor.id, "terminal result with retained native custody");
      void answer.catch(() => undefined);
      const first = await launched(f, actor.id);
      first.descendants = true;
      first.debt = "recorded native tree close debt";
      save(first, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now() });

      // Reach real logical settlement, not Linux's unsatisfied actor-publication
      // fence. Windows permits the terminal result while native debt retains RUN.
      // Then exercise that settled entry in BOTH platform branches of the scan.
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      await f.agents.stop(first.launch.id, { consume: false });
      await expect(f.agents.wait(first.launch.id)).resolves.toMatchObject({ status: "completed", text: "finished" });
      await expect(answer).resolves.toMatchObject({ text: "finished" });
      await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
      Object.defineProperty(process, "platform", { ...platform, value: scanPlatform });
      expect(first.alive).toBe(true);
      expect(f.agents.heldPermits()).toBe(1);
      expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
      expect(f.agents.workerIdentity(first.launch.id)?.pid).toBe(first.pid);
      const next = await blockedSuccessor(f); // Admission scans the already-settled owner while alive.
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);

      first.probeError = "EPERM";
      first.birth = "67890"; // A different birth cannot override an uncertain PID probe.
      save(first, { sessionId: String(claimedGonePid), processStartTime: "99999", workerGone: true, settledLost: true });
      // Deliberately untyped worker-writable cause, not a new public error code.
      fs.writeFileSync(first.statusFile, JSON.stringify({ ...first.record, errorCode: "WORKER_GONE" }));
      expect(first.launch.onUnconfirmedExit).toBeTypeOf("function");
      first.launch.onUnconfirmedExit!(first.debt!); // Recorded native observation, not worker-writable proof.
      await observe();
      expect(() => process.kill(first.pid, 0)).toThrow("EPERM");
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(1);
      expect(f.agents.heldPermits()).toBe(1);
      expect(workers.size).toBe(1);

      // The current host-owned PID is now absent. No ENODEV marker, native close
      // or tree exit is supplied: only actual admission may be discharged.
      first.alive = false;
      delete first.probeError;
      first.launch.onUnconfirmedExit!(first.debt!);
      const second = await launched(f, next.actor.id);
      expect(f.agents.heldPermits()).toBe(0);
      expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
      expect(first.descendants).toBe(true);
      expect(first.debt).toBeDefined();
      const thirdActor = await create(f, "settled-debt-third");
      const thirdAnswer = f.actors.ask(thirdActor.id, "wait behind successor, despite repeated old proof");
      void thirdAnswer.catch(() => undefined);
      await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
      expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
      expect(workers.size).toBe(2);

      // This ordinary no-debt actor really settles and gives RUN to the third.
      // A later scan must ignore its already-discharged admission as well.
      complete(second);
      await expect(next.answer).resolves.toMatchObject({ text: "finished" });
      const third = await launched(f, thirdActor.id);
      await expect(f.agents.wait(second.launch.id)).resolves.toMatchObject({ status: "completed" });
      const fourthActor = await create(f, "settled-debt-fourth");
      const fourthAnswer = f.actors.ask(fourthActor.id, "must remain queued through late native close and stop");
      void fourthAnswer.catch(() => undefined);
      await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
      expect(snapshot(f).held.map(held => held.runId)).toEqual([third.launch.id]);
      expect(workers.size).toBe(3);

      complete(first); // Late native close re-scans both settled entries.
      await f.agents.stop(first.launch.id, { consume: false }); // Late settlement/stop is once-only too.
      await observe();
      expect(snapshot(f).held.map(held => held.runId)).toEqual([third.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(1);
      expect(workers.size).toBe(3);
      complete(third);
      await expect(thirdAnswer).resolves.toMatchObject({ text: "finished" });
      const fourth = await launched(f, fourthActor.id);
      complete(fourth);
      await expect(fourthAnswer).resolves.toMatchObject({ text: "finished" });
      await eventually(() => expect(snapshot(f).held).toEqual([]));
    } finally {
      // RED must not strand actor waiters behind the baseline's leaked permit.
      // Cancelling queued receipts never signals a worker or deletes run files.
      try {
        for (const waiting of snapshot(f).waiting) await f.agents.stop(waiting.runId, { consume: false });
      } finally { Object.defineProperty(process, "platform", platform); }
    }
  });

  it.each(["ESRCH", "different-birth"] as const)("Windows terminal confirmed close keeps RUN fenced across %s primary-death proof until the captured native close joins", async primaryProof => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const f = fixture();
    let first: Worker | undefined;
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const actor = await create(f, "terminal-proof-race");
      const answer = f.actors.ask(actor.id, "terminal result while native close is pending");
      void answer.catch(() => undefined);
      first = await launched(f, actor.id);
      const next = await blockedSuccessor(f);

      // Publish the terminal result and host death in one turn, before the
      // monitor's pre-terminal proof hook. Captured native close is still open.
      first.primaryProofScans = 0;
      save(first, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
      first.alive = false;
      if (primaryProof === "ESRCH") first.identityAlive = false;
      else { first.identityAlive = true; first.birth = "67890"; }
      await eventually(() => {
        expect(first!.primaryProofScans).toBeGreaterThan(0);
        // Either the proper close join began, or the regression admitted a
        // successor. Do not let a worker-writable terminal file alone pass.
        expect(first!.closeStarted || workers.size > 1).toBe(true);
      });
      expect(first.closeJoined).toBe(false);
      expect(workers.size, "primary death cannot bypass the pending Windows close").toBe(1);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(1);
      expect(first.closeStarted).toBe(true);
      await expect(answer).resolves.toMatchObject({ text: "finished" });

      first.nativeClose();
      await eventually(() => expect(first!.closeJoined).toBe(true));
      const second = await launched(f, next.actor.id);
      expect(workers.size, "confirmed native close admits exactly one successor").toBe(2);
      expect([...workers.values()].filter(worker => worker.launch.id === second.launch.id)).toHaveLength(1);
      complete(second);
      await expect(next.answer).resolves.toMatchObject({ text: "finished" });
      await eventually(() => expect(snapshot(f).held).toEqual([]));
    } finally {
      if (first) {
        first.alive = false;
        first.descendants = false;
        delete first.debt;
        first.nativeClose();
      }
      // Cancel queued receipts; a RED early launch may have promoted one
      // before the snapshot flush, so leave that fake to afterEach's drain.
      try {
        for (const waiting of snapshot(f).waiting) await f.agents.stop(waiting.runId, { consume: false }).catch(() => undefined);
      } finally { Object.defineProperty(process, "platform", platform); }
    }
  });

  it.each(["close", "stop"] as const)("Windows terminal uncertain %s join reconciles fresh host death exactly once without releasing tree custody", async fault => {
    await withTerminalJoins(fault, false, async context => {
      const { f, first, close, stop, calls, scan } = context;
      first.probeError = "EPERM";
      first.birth = "67890"; // Birth mismatch cannot override an uncertain PID probe.
      (fault === "close" ? close : stop).reject(new Error("recorded captured join uncertain"));
      await eventually(() => expect(calls).toContain(`${fault}-rejected`));
      scan(); scan();
      await observe();
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(2);
      expect(workers.size).toBe(1);

      delete first.probeError;
      first.identityAlive = false;
      const probes = first.primaryProofScans;
      scan(); scan();
      await reconciledExactlyOnce(context, probes);
    });
  }, 15_000);

  it.each(["close", "stop"] as const)("Windows terminal hung %s join releases RUN once on fresh gone proof at the existing native deadline", async fault => {
    await withTerminalJoins(fault, false, async context => {
      const { f, first, clock, scan } = context;
      first.identityAlive = false;
      clock.beforeBound();
      scan(); scan();
      await observe();
      expect(workers.size, "host death cannot bypass a still-pending captured join before 7000 ms").toBe(1);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(2);
      const probes = first.primaryProofScans;
      clock.expire(); // ONLY the actual native deadline callback, no external scan.
      await reconciledExactlyOnce(context, probes);
    });
  }, 15_000);

  it.each([
    ["close", "alive", false], ["stop", "alive", false],
    ["close", "alive", true], ["stop", "alive", true],
    ["close", "unknown", true], ["stop", "unknown", true],
  ] as const)("Windows terminal hung %s join re-probes %s host proof after the bound without a busy loop (prior gone=%s)", async (fault, proof, priorGone) => {
    await withTerminalJoins(fault, priorGone, async context => {
      const { f, first, clock, scan } = context;
      first.identityAlive = true;
      first.birth = proof === "alive" ? "12345" : "67890";
      if (proof === "unknown") first.probeError = "EPERM";
      const probes = first.primaryProofScans;
      clock.expire();
      await eventually(() => expect(first.primaryProofScans, "timeout must re-probe, not reuse a prior gone receipt").toBeGreaterThan(probes));
      expect(workers.size).toBe(1);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(2);

      const atBound = first.primaryProofScans;
      scan(); // A later EXTERNAL event must retry the exact PID/birth proof.
      await eventually(() => expect(first.primaryProofScans).toBeGreaterThan(atBound));
      const afterExternalScan = first.primaryProofScans;
      await observe();
      expect(first.primaryProofScans, "alive/unknown reconciliation must not self-reschedule a proof loop").toBe(afterExternalScan);
      expect(workers.size).toBe(1);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(2);

      delete first.probeError;
      first.identityAlive = false;
      const beforeRecovery = first.primaryProofScans;
      scan();
      await reconciledExactlyOnce(context, beforeRecovery);
    });
  }, 15_000);

  it.each([
    ["ESRCH", false],
    ["different-birth", false],
    ["ESRCH", true],
    ["different-birth", true],
  ] as const)("Windows close callback before terminal read retains RUN through captured close (proof=%s, pending stop=%s)", async (primaryProof, pendingStop) => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
    const calls: Array<{ operation: string; pid: number }> = [];
    let resumeMonitor!: () => void;
    const monitorGate = new Promise<void>(resolve => { resumeMonitor = resolve; });
    let releaseClose!: () => void;
    const closeJoin = new Promise<void>(resolve => { releaseClose = resolve; });
    let releaseStop!: () => void;
    const stopJoin = new Promise<void>(resolve => { releaseStop = resolve; });
    let firstId: string | undefined;
    let monitorHeld = false;
    let closeDelivered!: Promise<void>;
    vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
      const handle = await launch(request);
      const worker = workers.get(Number(handle.sessionId))!;
      firstId ??= request.id;
      const firstAttempt = request.id === firstId;
      if (firstAttempt) {
        worker.closeJoin = closeJoin;
        if (pendingStop) worker.stopJoin = stopJoin;
      }
      calls.push({ operation: "launch", pid: worker.pid });
      return { ...handle,
        isAlive: async () => {
          if (firstAttempt && !monitorHeld) {
            // The monitor already read RUNNING and installed its closed callback.
            // Hold its existing liveness await so that callback MUST run before
            // the next terminal read/#settleGoneActorWorker can install a fence.
            const alive = await handle.isAlive();
            monitorHeld = true;
            calls.push({ operation: "monitor-liveness-held", pid: worker.pid });
            // Registered after the real monitor's callback: FIFO promise delivery
            // makes this a receipt that the real callback has already run.
            closeDelivered = handle.closed!.then(() => {
              calls.push({ operation: "closed-callback-delivered", pid: worker.pid });
            });
            await monitorGate;
            calls.push({ operation: "monitor-liveness-resumed", pid: worker.pid });
            return alive;
          }
          return handle.isAlive();
        },
        waitForClose: async () => {
          calls.push({ operation: "close-join-started", pid: worker.pid });
          await handle.waitForClose!();
          calls.push({ operation: "close-join-settled", pid: worker.pid });
        },
        stop: async () => {
          calls.push({ operation: "stop-started", pid: worker.pid });
          await handle.stop();
          calls.push({ operation: "stop-joined", pid: worker.pid });
        },
      };
    });
    const f = fixture();
    let first: Worker | undefined;
    let stopping: Promise<unknown> | undefined;
    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      const actor = await create(f, "closed-before-terminal-read");
      const answer = f.actors.ask(actor.id, "terminal result before captured native joins");
      void answer.catch(() => undefined);
      first = await launched(f, actor.id);
      await eventually(() => expect(monitorHeld).toBe(true));
      const next = await blockedSuccessor(f);
      const thirdActor = await create(f, "second-queued-successor");
      const thirdAnswer = f.actors.ask(thirdActor.id, "only one successor may acquire RUN");
      void thirdAnswer.catch(() => undefined);
      await eventually(() => expect(snapshot(f).waiting).toHaveLength(2));

      // Terminal publication, host death and passive close occur in one turn.
      // Passive closed is deliberately NOT the captured waitForClose/stop join.
      calls.push({ operation: "terminal-published", pid: first.pid });
      save(first, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
      first.alive = false;
      if (primaryProof === "ESRCH") first.identityAlive = false;
      else { first.identityAlive = true; first.birth = "67890"; }
      first.nativeClose();
      await closeDelivered;
      expect(calls.filter(call => call.pid === first!.pid).map(call => call.operation)).toEqual([
        "launch", "monitor-liveness-held", "terminal-published", "closed-callback-delivered",
      ]);
      expect(first.closeStarted).toBe(false); // Monitor is still before the terminal read.
      expect(first.closeJoined).toBe(false);
      await observe(); // Let real semaphore promotion/snapshot I/O expose an early release.
      console.info("closed-before-terminal-read pending evidence", JSON.stringify({ primaryProof, pendingStop, admission: snapshot(f), calls }));
      expect(workers.size, "P1: closed callback cannot admit a successor before the terminal read/native join").toBe(1);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(2);

      resumeMonitor();
      await eventually(() => expect(first!.closeStarted).toBe(true));
      await expect(answer).resolves.toMatchObject({ text: "finished" });
      expect(first.closeJoined).toBe(false);
      if (pendingStop) {
        // A stop begun after logical settlement adds a SEPARATE tree-helper join.
        // Even successful waitForClose cannot discharge that pending obligation.
        stopping = f.agents.stop(first.launch.id, { consume: false });
        void stopping.catch(() => undefined);
        await eventually(() => expect(calls.some(call => call.operation === "stop-started" && call.pid === first!.pid)).toBe(true));
      }
      const scan = first.launch.onUnconfirmedExit!;
      expect(scan).toBeTypeOf("function");
      scan("recorded repeated host-death observation while captured close is pending");
      scan("recorded repeated host-death observation while captured close is pending");
      await observe();
      expect(first.closeJoined).toBe(false);
      expect(workers.size, "pending captured close retains actual RUN despite repeated proof").toBe(1);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(2);

      releaseClose();
      await eventually(() => expect(first!.closeJoined).toBe(true));
      if (pendingStop) {
        await observe();
        expect(calls.some(call => call.operation === "stop-joined" && call.pid === first!.pid)).toBe(false);
        expect(workers.size, "joined close cannot bypass the separately pending stop join").toBe(1);
        expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
        expect(snapshot(f).waiting).toHaveLength(2);
        releaseStop();
        await stopping;
      }
      const second = await launched(f, next.actor.id);
      await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
      expect(workers.size, "settled native joins admit exactly ONE of two queued successors").toBe(2);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);

      // Repeated proof, repeated passive close, and late/repeated explicit stop
      // must not release the old admission again while the successor owns RUN.
      first.nativeClose();
      first.nativeClose();
      scan("recorded late host-death observation after native joins");
      scan("recorded late host-death observation after native joins");
      await f.agents.stop(first.launch.id, { consume: false });
      await f.agents.stop(first.launch.id, { consume: false });
      await observe();
      expect(workers.size).toBe(2);
      expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
      expect(snapshot(f).waiting).toHaveLength(1);
      expect([...workers.values()].filter(worker => worker.launch.id === first!.launch.id)).toHaveLength(1);
      console.info("closed-before-terminal-read evidence", JSON.stringify({ primaryProof, pendingStop, admission: snapshot(f), calls }));
      complete(second);
      await expect(next.answer).resolves.toMatchObject({ text: "finished" });
      const third = await launched(f, thirdActor.id);
      expect(workers.size).toBe(3);
      complete(third);
      await expect(thirdAnswer).resolves.toMatchObject({ text: "finished" });
      await eventually(() => expect(snapshot(f).held).toEqual([]));
    } finally {
      // RED must also return every synthetic join to afterEach; no signal/delete.
      resumeMonitor();
      releaseClose();
      releaseStop();
      if (first) complete(first);
      await stopping?.catch(() => undefined);
      try {
        for (const waiting of snapshot(f).waiting) await f.agents.stop(waiting.runId, { consume: false }).catch(() => undefined);
      } finally { Object.defineProperty(process, "platform", platform); }
    }
  });

  it.each(["completed", "failed"] as const)("terminal %s result cleans its surviving scope before settling and drains two same-actor successors without replay (#16970)", async status => {
    const trace = scopedExitReceipts();
    const settlements: AgentRunResult[] = [];
    const f = fixture(result => { settlements.push(result); });
    const actor = await create(f, "terminal-scope-daemon");
    await f.actors.tell(actor.id, "unique terminal event with a surviving cache daemon");
    const first = await launched(f, actor.id);
    await f.actors.tell(actor.id, "scope successor one");
    await f.actors.tell(actor.id, "scope successor two");
    await eventually(() => expect(f.actors.status(actor.id).queued).toBe(2));
    const before = queueEvidence(f, actor.id);
    expect(before.items.map(item => item.executionStarted)).toEqual([true, false, false]);
    let settled = false;
    const result = f.agents.wait(first.launch.id).then(value => { settled = true; return value; });
    try {
      first.alive = false; // Host-owned launch identity now proves primary ESRCH.
      first.descendants = true; // e.g. op daemon; scope is still populated.
      save(first, { status, text: "terminal answer", turns: 2, toolCalls: 1,
        ...(status === "failed" ? { error: "Permanent worker task failure" } : {}),
        finishedAt: Date.now(), updatedAt: Date.now(), exitCode: status === "completed" ? 0 : 1 });
      first.nativeClose();
      await eventually(() => expect(trace.calls, "terminal verdict must reach bounded owned-scope cleanup").toContain("scope-stop-started"));
      expect(first.primaryProofScans).toBeGreaterThan(0);
      expect(() => process.kill(first.pid, 0)).toThrow("ESRCH");
      expect(first.descendants).toBe(true);
      expect(settled, "a terminal file plus primary close cannot bypass checked cleanup").toBe(false);
      expect(settlements).toEqual([]);
      expect(workers.size).toBe(1);
      expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
      expect(queueEvidence(f, actor.id)).toEqual(before);

      trace.cleanup.resolve();
      await eventually(() => expect(trace.calls).toContain("scope-cleanup-ack"));
      await observe(); // A stop ACK/scope-like absence still owes captured tree proof.
      expect(first.descendants).toBe(false);
      expect(trace.calls).not.toContain("scope-stop-joined");
      expect(settled).toBe(false);
      expect(settlements).toEqual([]);
      expect(workers.size).toBe(1);
      expect(queueEvidence(f, actor.id)).toEqual(before);
      trace.tree.resolve();
      await eventually(() => expect(settled, "checked scope exit must settle the terminal activation").toBe(true));
      await expect(result).resolves.toMatchObject({ id: first.launch.id, status, text: "terminal answer",
        ...(status === "failed" ? { error: "Permanent worker task failure" } : {}) });
      expect(trace.calls.filter(call => call === "scope-stop-started")).toHaveLength(1);
      expect(trace.calls.filter(call => call === "scope-stop-joined")).toHaveLength(1);
      trace.tree.resolve(); // Duplicate receipt delivery cannot discharge RUN twice.
      await drainTwoSameActorSuccessors(f, actor.id, first, settlements);
    } finally {
      // RED also unblocks a pending checked stop; afterEach completes any fake
      // successor launched during failure unwinding, with deletion probes intact.
      trace.drain();
      complete(first);
    }
  }, 20_000);

  it("rejected monitor with native close already delivered settles once on a later checked tree exit and drains two same-actor successors (#16970)", async () => {
    const trace = scopedExitReceipts(true);
    trace.cleanup.resolve();
    const settlements: AgentRunResult[] = [];
    const f = fixture(result => { settlements.push(result); });
    const actor = await create(f, "late-tree-after-monitor-failure");
    await f.actors.tell(actor.id, "unique completed event before monitor rejection");
    const first = await launched(f, actor.id);
    await f.actors.tell(actor.id, "scope successor one");
    await f.actors.tell(actor.id, "scope successor two");
    await eventually(() => {
      expect(trace.calls).toContain("monitor-held");
      expect(f.actors.status(actor.id).queued).toBe(2);
    });
    const before = queueEvidence(f, actor.id);
    let settled = false;
    const result = f.agents.wait(first.launch.id).then(value => { settled = true; return value; });
    try {
      first.alive = false;
      save(first, { status: "completed", text: "terminal answer", turns: 2, toolCalls: 1,
        finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
      first.nativeClose();
      await trace.nativeDelivered();
      expect(trace.calls).toContain("native-delivered");
      trace.monitor.reject(new Error("recorded monitor liveness rejection after native close"));
      const marker = path.join(path.dirname(first.statusFile), "unresolved-worker.json");
      await eventually(() => {
        expect(fs.existsSync(marker)).toBe(true);
        expect(JSON.parse(fs.readFileSync(marker, "utf8")).reason).toContain("recorded monitor liveness rejection");
      });
      expect(() => process.kill(first.pid, 0)).toThrow("ESRCH");
      expect(first.descendants).toBe(false); // Scope/path absence is not treeClosed.
      expect(settled).toBe(false);
      expect(settlements).toEqual([]);
      expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
      expect(queueEvidence(f, actor.id)).toEqual(before);
      await observe();
      const probes = trace.calls.filter(call => call === "scope-probe").length;
      const primaryScans = first.primaryProofScans;
      await observe();
      expect(trace.calls.filter(call => call === "scope-probe"), "failed monitor continuation must be event-driven, not a new liveness loop").toHaveLength(probes);
      expect(first.primaryProofScans, "no self-rescheduled primary-proof loop").toBe(primaryScans);
      expect(settled).toBe(false);
      expect(workers.size).toBe(1);
      expect(queueEvidence(f, actor.id)).toEqual(before);

      // The already-delivered native close cannot wake this failed monitor.
      // This independent, exact admitted-scope receipt is the ONLY new event.
      trace.tree.resolve();
      await eventually(() => expect(settled, "late tree exit must re-drive settlement after monitor rejection").toBe(true));
      await expect(result).resolves.toMatchObject({ id: first.launch.id, status: "completed", text: "terminal answer" });
      expect(trace.calls.filter(call => call === "native-delivered")).toHaveLength(1);
      expect(fs.existsSync(marker), "checked late exit clears unresolved custody").toBe(false);
      trace.tree.resolve();
      await drainTwoSameActorSuccessors(f, actor.id, first, settlements);
    } finally {
      trace.drain();
      complete(first);
    }
  }, 20_000);

  it.each(["neither", "native-only", "tree-only"] as const)("unconfirmed exit stays fenced with %s captured receipt despite primary and scope absence (#16970)", async receipt => {
    const trace = scopedExitReceipts(true);
    trace.cleanup.resolve();
    const settlements: AgentRunResult[] = [];
    const f = fixture(result => { settlements.push(result); });
    const actor = await create(f, "unconfirmed-scope-exit");
    await f.actors.tell(actor.id, "unconfirmed execution must retain same-session custody");
    const first = await launched(f, actor.id);
    await f.actors.tell(actor.id, "same-session successor stays queued");
    await eventually(() => {
      expect(trace.calls).toContain("monitor-held");
      expect(f.actors.status(actor.id).queued).toBe(1);
    });
    const before = queueEvidence(f, actor.id);
    let settled = false;
    void f.agents.wait(first.launch.id).then(() => { settled = true; });
    try {
      first.alive = false;
      first.descendants = false; // isAlive=false models missing PID AND empty/missing scope.
      if (receipt === "native-only") {
        first.nativeClose();
        await trace.nativeDelivered();
      }
      if (receipt === "tree-only") trace.tree.resolve();
      trace.monitor.reject(new Error("recorded monitor rejection without whole-execution proof"));
      await eventually(() => expect(fs.existsSync(path.join(path.dirname(first.statusFile), "unresolved-worker.json"))).toBe(true));
      expect(() => process.kill(first.pid, 0)).toThrow("ESRCH");
      first.launch.onUnconfirmedExit!("recorded PID and scope absent, exit still unconfirmed");
      await observe();
      const probes = trace.calls.filter(call => call === "scope-probe").length;
      await observe();
      expect(trace.calls.filter(call => call === "scope-probe"), "missing proof must not start another polling loop").toHaveLength(probes);
      expect(settled, "PID/scope absence and one receipt cannot certify whole-execution exit").toBe(false);
      expect(settlements).toEqual([]);
      expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
      expect(f.actors.status(actor.id).inFlightRun?.id).toBe(first.launch.id);
      expect(f.actors.status(actor.id).queued).toBe(1);
      expect(queueEvidence(f, actor.id)).toEqual(before);
      expect(workers.size).toBe(1);
    } finally {
      trace.drain();
      complete(first); // Supply BOTH receipts only for safe fixture teardown.
    }
  }, 15_000);

  it("lost-contact alive worker retains actual RUN despite forged primary-death claims", async () => {
    const f = fixture();
    const actor = await create(f, "alive-lost-contact");
    await f.actors.tell(actor.id, "still owns native session");
    const first = await launched(f, actor.id);
    await blockedSuccessor(f);
    first.contactLost = "contact is lost, host worker is not";
    await eventually(() => expect(fs.existsSync(path.join(path.dirname(first.statusFile), "unresolved-worker.json"))).toBe(true));
    save(first, { sessionId: String(claimedGonePid), processStartTime: "99999", workerGone: true, settledLost: true });
    // Intentionally untyped worker-writable claim, not a new public error code.
    fs.writeFileSync(first.statusFile, JSON.stringify({ ...first.record, errorCode: "WORKER_GONE" }));
    await observe();
    expect(process.kill(first.pid, 0)).toBe(true);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([first.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(1);
    expect(f.agents.retentionCustodyVeto(first.launch.id)).toBe(true);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(first.launch.id);
    expect(workers.size).toBe(1);
    expect(f.agents.heldPermits()).toBe(1);
  });

  it.each(["descendants", "native-debt"] as const)("gone leader with %s frees RUN but not whole-execution or same-actor custody", async fault => {
    const f = fixture();
    const actor = await create(f, "tree-custody");
    await f.actors.tell(actor.id, "lost event");
    const worker = await launched(f, actor.id);
    await f.actors.tell(actor.id, "same native session must wait");
    const before = queueEvidence(f, actor.id);
    const next = await blockedSuccessor(f);
    worker.alive = false;
    if (fault === "descendants") worker.descendants = true;
    else worker.debt = "native tree close unconfirmed";
    worker.nativeClose();
    const second = await launched(f, next.actor.id);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]);
    expect(snapshot(f).waiting).toHaveLength(0);
    expect(workers.size).toBe(2);
    expect(f.agents.retentionCustodyVeto(worker.launch.id)).toBe(true);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
    expect(f.actors.status(actor.id).queued).toBe(1);
    expect(queueEvidence(f, actor.id)).toEqual(before);
  });
});
