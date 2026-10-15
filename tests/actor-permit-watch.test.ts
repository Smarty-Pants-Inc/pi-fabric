import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { AgentRunRecord, AgentTransportLaunch } from "../src/agents/types.js";
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
const workers = new Map<number, Worker>();
const claimedGonePid = 899_999_999;
let nextPid = 900_000_000;
let closing = false;
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
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR"]) vi.stubEnv(key, undefined);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    if (signal !== 0) throw new Error("No real or scripted worker signals allowed");
    const worker = workers.get(pid);
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
      record, statusFile: arg("status-file"), nativeClose };
    workers.set(pid, worker);
    fs.writeFileSync(worker.statusFile, JSON.stringify(record));
    if (closing) complete(worker); // Failed-test teardown also drains any queued launches.
    return {
      kind: "process", sessionId: String(pid), closed,
      stopDebt: () => worker.debt, lostContact: () => worker.contactLost ?? worker.debt,
      waitForClose: async () => {
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Native custody still open");
        await closed;
      },
      isAlive: async () => !worker.contactLost && (worker.alive || worker.descendants),
      stop: async () => {
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Owned tree exit unconfirmed");
        await closed;
      },
    };
  });
});

afterEach(async () => {
  closing = true;
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

function fixture(): Fixture {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-permit-watch-"));
  fixtureRoots.push(root); // Constructors can release metadata locks before fixture() returns.
  const admission = path.join(root, "actors", "admission.json");
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents,
    enabled: true, runner: "pi", transport: "process", maxConcurrent: 1, budgetUsd: 0,
    deniedModels: [], timeoutMs: 24 * 60 * 60_000, retainRuns: true, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    meshRoot: path.join(root, "mesh"), admissionSnapshotPath: admission,
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
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
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

  it.each(["linux", "win32"] as const)("settled actor with native debt releases retained RUN only on host proof (%s scan)", async scanPlatform => {
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
