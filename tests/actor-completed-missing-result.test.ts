import { spawn, type ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { spawnDetached } from "../src/agents/transports/process-utils.js";
import type { AgentNativeExitDiagnostic, AgentRunResult, AgentTransportHandle } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { writeJsonAtomic } from "../src/core/atomic-write.js";
import { MeshStore } from "../src/mesh/store.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

// Keep: recorded incident smarty-dev#17990 and the worker/manager custody
// boundary. Real serial drain, stop/archive and automatic recovery; native
// launch/PID/procfs/exit are recorded, never a live resident-owner replacement.
// The sticky-close case runs the REAL unscoped transport's deadline/debt code.
vi.mock("node:child_process", async importOriginal => ({
  ...await importOriginal<typeof import("node:child_process")>(),
  spawn: vi.fn(() => { throw new Error("No native launch authorized by missing-result fixture"); }),
}));

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>(done => { resolve = done; });
  return { promise, resolve };
};
const eventually = (check: () => void) => vi.waitFor(check, { timeout: 5_000, interval: 20 });
const answer = "WORKER_COMPLETION_REQUIRES_NATIVE_TREE_JOIN";
const roots: string[] = [];
const cleanups: Array<() => Promise<void>> = [];
const workers = new Map<number, { gone: boolean; parent?: number; group?: number; started?: string }>();
let censusUnknown = false;
const recordedSignals = new Map<number, (signal: "SIGTERM" | "SIGKILL") => void>();
let nextPid = 900_000_000;
let removalSequence = 0;

beforeEach(() => {
  roots.length = 0;
  workers.clear();
  censusUnknown = false;
  recordedSignals.clear();
  removalSequence = 0;
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "PI_FABRIC_ACTOR_SESSION_FILE"]) vi.stubEnv(key, undefined);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (signal !== 0) {
      const recorded = recordedSignals.get(pid);
      if (!recorded || (signal !== "SIGTERM" && signal !== "SIGKILL")) throw new Error("No signals authorized by missing-result fixture");
      recorded(signal);
      return true;
    }
    if (workers.get(pid)?.gone) throw Object.assign(new Error("recorded worker exit"), { code: "ESRCH" });
    return true;
  });
  const birth = processIdentity.processStartTime;
  vi.spyOn(processIdentity, "processStartTime").mockImplementation(pid => workers.has(pid) ? "12345" : birth(pid));
  // Feed only recorded workers to native tree census. Never inspect/adopt/signal
  // a host process while exercising the real legacy transport below.
  const read = fs.readFileSync, list = fs.readdirSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    const match = /^\/proc\/(\d+)\/stat$/.exec(String(file));
    const worker = match && workers.get(Number(match[1]));
    if (!worker) return Reflect.apply(read, fs, [file, ...args]);
    if (worker.gone) throw Object.assign(new Error("recorded procfs absence"), { code: "ENOENT" });
    const fields = Array<string>(20).fill("0");
    fields[0] = "S"; fields[1] = String(worker.parent ?? process.pid); fields[2] = String(worker.group ?? match![1]!);
    fields[3] = match![1]!; fields[19] = worker.started ?? "12345";
    return `${match![1]} (recorded-worker) ${fields.join(" ")}`;
  }) as typeof fs.readFileSync);
  vi.spyOn(fs, "readdirSync").mockImplementation(((file: fs.PathLike, ...args: unknown[]) => {
    if (String(file) !== "/proc") return Reflect.apply(list, fs, [file, ...args]);
    if (censusUnknown) throw Object.assign(new Error("recorded unreadable census"), { code: "EACCES" });
    return [...workers].filter(([, worker]) => !worker.gone).map(([pid]) => String(pid));
  }) as typeof fs.readdirSync);
  // Preserve all fixture bytes, including lock/marker removals. Resolve pinned
  // /proc/self/fd parents before enforcing the test-owned-directory boundary.
  const quarantine = (file: fs.PathLike) => {
    let target = path.resolve(String(file));
    try { target = path.join(fs.realpathSync(path.dirname(target)), path.basename(target)); } catch { /* absent parent */ }
    if (!roots.some(root => target === root || target.startsWith(root + path.sep))) {
      throw new Error(`Removal outside missing-result fixture: ${target}`);
    }
    if (fs.existsSync(target)) fs.renameSync(target, `${target}.preserved-${++removalSequence}`);
  };
  vi.spyOn(fs, "rmSync").mockImplementation(quarantine);
  vi.spyOn(fs, "unlinkSync").mockImplementation(quarantine);
  vi.spyOn(fs, "rmdirSync").mockImplementation(quarantine);
  vi.spyOn(fs.promises, "rm").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(fs.promises, "unlink").mockImplementation(async file => { quarantine(file); });
  vi.spyOn(fs.promises, "rmdir").mockImplementation(async file => { quarantine(file); });
});

afterEach(async () => {
  try { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); }
  finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); }
  // No fixture/trace deletion. The test environment owns these private roots.
});

type Settlement = "valid" | "malformed" | "mismatched";
async function actorFixture(settlement: Settlement = "valid", stickyCloseDebt = false, forcedTermination = false) {
  // The parent runs this suite with TMPDIR on /var/tmp, never its inherited /srv
  // runtime scratch root. Every manager, mesh, queue and artifact stays here.
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-completed-missing-result-"));
  roots.push(fs.realpathSync(root));
  const residencyRoot = path.join(root, "resident");
  const actorRoot = path.join(root, "actors");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity = { id: "session:missing-result", name: "main", kind: "main" as const, sessionId: "missing-result" };
  const pid = nextPid++, worker = { gone: false };
  workers.set(pid, worker);
  const primary = deferred(), tree = deferred();
  let treeGone = false;
  let exit: AgentNativeExitDiagnostic | undefined;
  let statusFile = "", runId = "", launches = 0;
  const descendants: number[] = [];
  let child: EventEmitter | undefined;
  let transport!: AgentTransportHandle;
  let skew = 0;
  const now = Date.now.bind(Date);
  vi.spyOn(Date, "now").mockImplementation(() => now() + skew);
  const primaryExit = () => {
    worker.gone = true;
    exit ??= { scope: "primary", pid, processStartTime: "12345", code: 0, signal: null, exitedAt: Date.now(),
      cgroup: { state: "unsupported", reason: "recorded native fixture" } };
    child?.emit("exit", 0, null);
    primary.resolve();
  };
  const confirmExit = () => { primaryExit(); treeGone = true; child?.emit("close", 0, null); tree.resolve(); };
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async request => {
    launches++;
    if (runId) throw new Error("The interrupted activation must not launch a replacement");
    runId = request.id;
    const handle = await fakeManagerHeapWorker(request, 0, false, {
      closed: primary.promise, alive: () => !treeGone,
      stop: async () => { if (!treeGone) throw new Error("recorded owned-tree exit unconfirmed"); await tree.promise; },
    });
    statusFile = request.workerArguments[request.workerArguments.indexOf("--status-file") + 1]!;
    writeJsonAtomic(statusFile, { ...JSON.parse(fs.readFileSync(statusFile, "utf8")),
      status: "running", finishedAt: undefined, sessionId: String(pid), turns: 1 });
    if (stickyCloseDebt) {
      child = Object.assign(new EventEmitter(), { pid, unref: vi.fn(), kill: vi.fn() });
      vi.mocked(spawn).mockReturnValueOnce(child as unknown as ChildProcess);
      // No scope => production selects process-utils-legacy. Its own timeout
      // latches lostContact and reports onUnconfirmedExit; no invented debt.
      const native = await spawnDetached(request.workerPath, request.workerArguments, request.cwd,
        request, undefined, undefined, forcedTermination ? 1 : 7_000, true);
      transport = { ...handle, ...native, kind: "process", sessionId: String(native.pid) };
    } else transport = { ...handle, sessionId: String(pid), primaryExit: primary.promise, treeClosed: tree.promise,
      nativeExit: () => exit, waitForClose: () => tree.promise };
    return transport;
  });
  const settlements: AgentRunResult[] = [], published: AgentRunResult[] = [];
  const makeAgents = () => new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents,
    budgetUsd: 0, maxConcurrent: 1, retainRuns: true, sessionExport: false, timeoutMs: 60_000,
  }, { runRoot: path.join(residencyRoot, "runs"), meshRoot: mesh.root,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fullCodeMode: false,
    onSettled: result => {
      settlements.push(result);
      // Observe the real manager publication BEFORE the actor consumes/forgets
      // its successful handle. This is not agents.status on a retired run.
      const saved = JSON.parse(fs.readFileSync(path.join(residencyRoot, "runs", result.id, "manager-result.json"), "utf8"));
      published.push(saved);
      writeJsonAtomic(path.join(root, "observed-manager-publication.json"), saved);
    } });
  const options = { actorRoot, persistent: true, meshRetentionSweepPath: false as const, closeGraceMs: 100,
    claimResidency: "durable" as const };
  const makeActors = (manager: AgentManager) => new ActorManager(identity.sessionId, identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, () => {}, options);
  const agents = makeAgents(), actors = makeActors(agents);
  cleanups.push(async () => {
    censusUnknown = false;
    for (const descendant of descendants) workers.get(descendant)!.gone = true;
    child?.emit("message", { type: "fabric-execution-settled" });
    confirmExit(); skew = 0;
    if (stickyCloseDebt) {
      // Test teardown ONLY: a RED baseline still has stale passive debt. Disarm
      // its outward fixture wrapper after captured close + checked tree exit.
      // All regression assertions/observations precede this; teardown is never
      // proof that production can repair its own debt or replace a resident.
      await transport.closed;
      if (await transport.isAlive()) throw new Error("Recorded tree still alive during teardown");
      writeJsonAtomic(path.join(root, "fixture-teardown.json"), { debtBeforeTeardown: transport.lostContact?.(),
        nativeExit: transport.nativeExit?.(), alive: false, fixtureOnlyDebtDisarm: true });
      transport.lostContact = () => undefined;
      transport.stopDebt = () => undefined;
    }
    // Actor success cleanup legitimately forgets the handle. Close managers,
    // which know their remaining owned runs; never stop a stale cached runId.
    await actors.close();
    await agents.close();
  });
  const actor = await actors.create({ name: "resident-reviewer", instructions: "Review once.",
    residency: "durable", transport: "process", extensions: false, responseMode: "text", coalesce: false });
  actors.tell(actor.id, "the activation that completed during residency recovery");
  await eventually(() => expect(actors.status(actor.id).inFlightRun?.id).toBe(runId || "not-launched"));
  const directory = agents.runDirectory(runId)!;
  const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
  writeJsonAtomic(statusFile, { ...record, status: "completed", text: answer, updatedAt: Date.now(), finishedAt: Date.now() });
  const receipt = path.join(directory, "settlement.json");
  if (settlement === "malformed") fs.writeFileSync(receipt, "{ torn settlement");
  else writeJsonAtomic(receipt, { version: 1, runId: settlement === "mismatched" ? "f".repeat(32) : runId,
    outcome: "completed", text: answer });
  if (stickyCloseDebt) {
    // Exercise the actual captured-handle close deadline, shortening only its
    // scheduled timer (not wall time, its callback, or manager recovery logic).
    const timeout = globalThis.setTimeout;
    const timer = vi.spyOn(globalThis, "setTimeout").mockImplementationOnce((callback, _delay, ...args) =>
      timeout(callback, 1, ...args));
    try { await transport.waitForClose!(); } finally { timer.mockRestore(); }
    expect(transport.lostContact?.()).toMatch(/native close within 7000ms/);
  }
  // The durable marker stores the underlying transport reason, NOT the
  // manager's public "execution exit unconfirmed" wrapper. For sticky native
  // debt the transport's real onUnconfirmedExit callback can write it first.
  await eventually(() => {
    const marker = JSON.parse(fs.readFileSync(path.join(directory, "unresolved-worker.json"), "utf8"));
    expect(marker.reason).toBe(stickyCloseDebt ? transport.lostContact?.() : "recorded owned-tree exit unconfirmed");
  });
  expect(JSON.parse(fs.readFileSync(statusFile, "utf8")).status).toBe("completed");
  expect(JSON.parse(fs.readFileSync(path.join(directory, "archive-pending.json"), "utf8")).awaitingResult).toBe(true);
  for (const file of ["result.json", "manager-result.json"]) expect(fs.existsSync(path.join(directory, file))).toBe(false);
  expect(fs.existsSync(path.join(residencyRoot, "results"))).toBe(false);
  expect(agents.isSettled(runId)).toBe(false);
  expect(actors.status(actor.id).inFlightRun?.id).toBe(runId);
  const automaticRecovery = async () => {
    const reconcile = vi.spyOn(agents, "reconcileActorRun");
    skew = 31_100; // Cross the real 30s host-lease and >5s native-exit bounds.
    await mesh.publish({ topic: "missing-result.native-exit-observed", from: identity });
    await eventually(() => {
      // The original monitor may win a safe automatic recovery race. Do not
      // demand an alarm callback after the actor has already consumed success.
      if (!actors.status(actor.id).inFlightRun) return;
      expect(reconcile).toHaveBeenCalledWith(runId, actor.id);
    });
    return await reconcile.mock.results[0]?.value as AgentRunResult | undefined;
  };
  const holdNativeTree = (proof: "descendant" | "execution-pending" | "unreadable-census" | "ambiguous-group") => {
    if (!child) throw new Error("Native counterexample requires the real recorded legacy handle");
    if (proof === "execution-pending") child.emit("message", { type: "fabric-execution-custody" });
    else if (proof === "unreadable-census") censusUnknown = true;
    else {
      const descendant = nextPid++;
      descendants.push(descendant);
      workers.set(descendant, { gone: false, parent: 1, group: proof === "descendant" ? descendant : pid, started: "23456" });
      if (proof === "descendant") child.emit("message", { type: "fabric-execution-started", pid: descendant, started: "23456" });
    }
  };
  const forceNativeTermination = async () => {
    if (!forcedTermination || !child) throw new Error("Forced stop requires its recorded-native counterexample");
    recordedSignals.set(-pid, signal => { if (signal === "SIGKILL") confirmExit(); });
    try { await transport.stop(); }
    finally { recordedSignals.delete(-pid); }
  };
  console.info(JSON.stringify({ fixtureRoot: root, runId, stickyCloseDebt }));
  return { root, agents, actors, actor, runId, directory, residencyRoot, settlements, published,
    transport, primaryExit, confirmExit, automaticRecovery, holdNativeTree, forceNativeTermination, launchCount: () => launches };
}

const expectCompletedOutcome = (result: AgentRunResult | undefined) => {
  expect(result).toMatchObject({ status: "completed", text: answer });
  expect(result?.error).toBeUndefined();
};
const expectPublishedAndArchived = async (f: Awaited<ReturnType<typeof actorFixture>>) => {
  await eventually(() => expect(f.actors.status(f.actor.id).inFlightRun).toBeUndefined());
  expect(f.settlements).toHaveLength(1);
  expect(f.published).toHaveLength(1);
  expect(f.launchCount(), "no activation replay or replacement handle").toBe(1);
  expect(f.settlements[0]?.id).toBe(f.runId);
  expect(f.published[0]?.id).toBe(f.runId);
  expectCompletedOutcome(f.settlements[0]);
  expectCompletedOutcome(f.published[0]);
  const archived = path.join(f.actor.logDir!, f.runId, "status.json");
  expectCompletedOutcome(JSON.parse(fs.readFileSync(archived, "utf8")));
  expect(f.agents.hasRetainedCustody()).toBe(false);
  expect(fs.existsSync(path.join(f.residencyRoot, "results"))).toBe(false); // Normal for actors.
};

describe.skipIf(process.platform !== "linux")("completed actor without a published result (#17990)", () => {
  it.each<Settlement>(["valid", "malformed", "mismatched"])(
    "preserves confirmed completion and publishes before actor consumption (%s settlement)", async settlement => {
      const f = await actorFixture(settlement);
      f.confirmExit(); // Host PID/birth AND captured native/whole-tree close, never status alone.
      const recovered = await f.agents.reconcileActorRun(f.runId, f.actor.id);
      if (recovered) expectCompletedOutcome(recovered); // The original actor may consume first.
      await expectPublishedAndArchived(f);
      const stopped = await f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true });
      expect(stopped).toMatchObject({ id: f.actor.id, status: "stopped" });
      expect(stopped.inFlightRun).toBeUndefined();
      // No wait/status/isSettled/stop(runId) after the actor has consumed it.
    }, 15_000);

  it("stop/dropQueued first joins confirmed exit, archives queued work, and retains the completed outcome", async () => {
    const f = await actorFixture();
    f.actors.tell(f.actor.id, "queued work must be archived, not replayed");
    expect(f.actors.status(f.actor.id).queued).toBe(1);
    f.confirmExit();
    const stopped = await f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true });
    expect(stopped).toMatchObject({ id: f.actor.id, status: "stopped", queued: 0 });
    expect(stopped.inFlightRun).toBeUndefined();
    expect(stopped.stoppedQueue).toBeDefined();
    expect(fs.readFileSync(stopped.stoppedQueue!.archive, "utf8")).toContain("queued work must be archived");
    await expectPublishedAndArchived(f);
  }, 15_000);

  it("the actor's own mesh wake recovers prior transient manager-side drain failure without manual reconcile", async () => {
    const f = await actorFixture();
    f.confirmExit();
    const recovered = await f.automaticRecovery();
    if (recovered) expectCompletedOutcome(recovered);
    await expectPublishedAndArchived(f);
  }, 15_000);

  it("a late native close and fresh checked tree join recover stale legacy close-deadline debt", async () => {
    const f = await actorFixture("valid", true);
    f.confirmExit();
    // Observation only. NO private stop/waitForClose/isAlive/lostContact call
    // may pre-repair the native handle before real ActorManager maintenance.
    await f.transport.closed;
    expect(f.transport.nativeExit?.()).toMatchObject({ pid: Number(f.transport.sessionId), processStartTime: "12345", code: 0 });
    const recovered = await f.automaticRecovery();
    if (recovered) expectCompletedOutcome(recovered);
    try {
      // Must succeed BEFORE public stop can incidentally repair anything.
      await expectPublishedAndArchived(f);
    } finally {
      // No repairing transport getters, even in failure diagnostics. A RED must
      // be the stranded completion, not an unknown/consumed-handle exception.
      writeJsonAtomic(path.join(f.root, "late-close-observation.json"), {
        recovered: recovered ?? null, actor: f.actors.status(f.actor.id),
        settlements: f.settlements, published: f.published, custody: f.agents.hasRetainedCustody(),
        nativeExit: f.transport.nativeExit?.(), launches: f.launchCount(),
      });
    }
    const stopped = await f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true });
    expect(stopped).toMatchObject({ id: f.actor.id, status: "stopped" });
    expect(stopped.inFlightRun).toBeUndefined();
    expect(f.settlements).toHaveLength(1);
    expect(f.launchCount()).toBe(1);
  }, 15_000);

  it("sticky-debt stop/dropQueued recovers directly without automatic recovery or private pre-drain", async () => {
    const f = await actorFixture("valid", true);
    f.actors.tell(f.actor.id, "sticky queued work must be archived, not replayed");
    expect(f.actors.status(f.actor.id).queued).toBe(1);
    expect(f.settlements).toHaveLength(0);
    // Start public stop in the same turn as exit/close. No mesh wake, time skew,
    // reconcile, or private transport operation may drain the handle first.
    f.confirmExit();
    const stopped = await f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true });
    expect(stopped).toMatchObject({ id: f.actor.id, status: "stopped", queued: 0 });
    expect(stopped.inFlightRun).toBeUndefined();
    expect(stopped.stoppedQueue).toBeDefined();
    expect(fs.readFileSync(stopped.stoppedQueue!.archive, "utf8")).toContain("sticky queued work must be archived");
    await expectPublishedAndArchived(f);
  }, 15_000);

  it.each(["descendant", "execution-pending", "unreadable-census", "ambiguous-group"] as const)(
    "completed status plus late close retains sticky custody with %s proof", async proof => {
      const f = await actorFixture("valid", true);
      f.holdNativeTree(proof);
      f.confirmExit();
      await f.transport.closed; // Captured close alone is insufficient.
      expect(await f.agents.reconcileActorRun(f.runId, f.actor.id)).toBeUndefined();
      await expect(f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true }))
        .rejects.toThrow(/exit unconfirmed|custody retained/i);
      expect(f.actors.status(f.actor.id).inFlightRun?.id).toBe(f.runId);
      expect(f.agents.isSettled(f.runId)).toBe(false);
      expect(f.agents.hasRetainedCustody()).toBe(true);
      expect(f.settlements).toEqual([]);
      expect(f.launchCount()).toBe(1);
      expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
    }, 15_000);

  it("completed status plus a late close never erases forced-termination receipt debt", async () => {
    const f = await actorFixture("valid", true, true);
    // Negative control only: the real legacy stop escalates against recorded
    // births/events, never host workers. Positive recovery does not call this.
    await f.forceNativeTermination();
    await f.transport.closed;
    expect(f.transport.lostContact?.()).toMatch(/POSIX worker tree termination is unconfirmed/);
    expect(await f.agents.reconcileActorRun(f.runId, f.actor.id)).toBeUndefined();
    await expect(f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true }))
      .rejects.toThrow(/exit unconfirmed|custody retained/i);
    expect(f.actors.status(f.actor.id).inFlightRun?.id).toBe(f.runId);
    expect(f.agents.isSettled(f.runId)).toBe(false);
    expect(f.agents.hasRetainedCustody()).toBe(true);
    expect(f.settlements).toEqual([]);
    expect(f.launchCount()).toBe(1);
    expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
  }, 15_000);

  it.each([
    { proof: "live", sticky: false }, { proof: "primary-only", sticky: false },
    { proof: "live", sticky: true }, { proof: "primary-only", sticky: true },
  ] as const)("completed status never releases a $proof worker (sticky debt: $sticky)", async ({ proof, sticky }) => {
      const f = await actorFixture("valid", sticky);
      if (proof === "primary-only") f.primaryExit(); // A surviving tree still owns the actor session.
      expect(await f.agents.reconcileActorRun(f.runId, f.actor.id)).toBeUndefined();
      await expect(f.actors.stop(f.actor.id, undefined, true, "actors.stop", { explicit: true, dropQueued: true }))
        .rejects.toThrow(/exit unconfirmed|custody retained/i);
      expect(f.actors.status(f.actor.id).inFlightRun?.id).toBe(f.runId);
      expect(f.agents.isSettled(f.runId)).toBe(false);
      expect(f.agents.hasRetainedCustody()).toBe(true);
      expect(f.settlements).toEqual([]);
      expect(fs.existsSync(path.join(f.directory, "manager-result.json"))).toBe(false);
  }, 15_000);
});
