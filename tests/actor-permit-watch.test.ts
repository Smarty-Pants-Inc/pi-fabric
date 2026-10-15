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

// Keep: #9031, a real AgentManager/ActorManager trust and data-safety boundary.
// Only native process identity/custody is scripted; no workers are spawned or killed.
// Product deletion entrypoints are replaced by recorded-call probes, never executed.
const MINUTE = 60_000;
const recordedDeletionCalls: string[] = [];
interface Fixture {
  root: string; admission: string; agents: AgentManager; actors: ActorManager;
  deliveries: Array<Parameters<ConstructorParameters<typeof ActorManager>[5]>[0]>;
  createActors: () => ActorManager;
}
const fixtures: Fixture[] = [];
const eventually = (assertion: () => void) => vi.waitFor(assertion, { timeout: 5_000, interval: 20 });
const ticks = () => new Promise(resolve => setTimeout(resolve, 180));
interface Worker {
  launch: AgentTransportLaunch;
  pid: number;
  alive: boolean;
  identityAlive?: boolean;
  birth: string | undefined;
  probeError?: string;
  descendants: boolean;
  debt?: string;
  stops: number;
  record: AgentRunRecord;
  statusFile: string;
  nativeClose: () => void;
}
const workers = new Map<number, Worker>();
let nextPid = 900_000_000;
let realNow: () => number;
let skew = 0;

beforeEach(() => {
  realNow = Date.now.bind(Date);
  skew = 0;
  workers.clear();
  recordedDeletionCalls.length = 0;
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + skew);
  for (const key of ["PI_FABRIC_DEPTH", "PI_FABRIC_BUDGET", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET_ID",
    "DBUS_SESSION_BUS_ADDRESS", "XDG_RUNTIME_DIR"]) vi.stubEnv(key, undefined);
  const kill = process.kill.bind(process);
  vi.spyOn(process, "kill").mockImplementation((pid, signal) => {
    const worker = workers.get(pid);
    if (!worker) return kill(pid, signal);
    if (signal !== 0) throw new Error("Test must not signal a worker");
    const code = worker.probeError ?? (!(worker.identityAlive ?? worker.alive) ? "ESRCH" : undefined);
    if (code) throw Object.assign(new Error(code), { code });
    return true;
  });
  const birth = processIdentity.processStartTime;
  vi.spyOn(processIdentity, "processStartTime").mockImplementation(pid => workers.has(pid) ? workers.get(pid)!.birth : birth(pid));
  vi.spyOn(fs, "rmSync").mockImplementation(file => {
    recordedDeletionCalls.push(`rm:${String(file)}`);
    // Simulate removal by quarantining the exact fixture path, not deleting it.
    // In particular lock/census cleanup must not leave a fake live lock behind.
    if (fs.existsSync(file)) fs.renameSync(file, `${String(file)}.recorded-removal-${recordedDeletionCalls.length}`);
  });
  vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(() => { recordedDeletionCalls.push("actor-log-prune"); });
  vi.spyOn(ActorChildCompletionStore.prototype, "prune").mockImplementation(() => { recordedDeletionCalls.push("child-completion-prune"); });
  vi.spyOn(AgentManager.prototype, "cleanup").mockImplementation(async id => {
    recordedDeletionCalls.push(`agent-cleanup:${id}`);
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
      stops: 0, record, statusFile: arg("status-file"), nativeClose };
    workers.set(pid, worker);
    fs.writeFileSync(worker.statusFile, JSON.stringify(record));
    return {
      kind: "process", sessionId: String(pid), closed,
      // This checked contract includes the whole tree, not just leader absence.
      stopDebt: () => worker.debt, lostContact: () => worker.debt,
      waitForClose: async () => { if (worker.alive || worker.descendants) throw new Error("Native custody still open"); await closed; },
      isAlive: async () => worker.alive || worker.descendants,
      stop: async () => {
        worker.stops++;
        if (worker.alive || worker.descendants || worker.debt) throw new Error("Owned tree exit unconfirmed");
        await closed;
      },
    };
  });
});

afterEach(async () => {
  // Release only scripted receipts. Leave fixture directories in task TMPDIR;
  // neither fixture cleanup nor product deletion runs in this local proof.
  skew = 0;
  for (const worker of workers.values()) complete(worker);
  for (const f of fixtures.splice(0).reverse()) {
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    await f.actors.close();
    await f.agents.close();
  }
  expect(recordedDeletionCalls.length).toBeGreaterThan(0);
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});

function fixture(snapshot = true): Fixture {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-permit-watch-"));
  const admission = path.join(root, "actors", "admission.json");
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents,
    enabled: true, runner: "pi", transport: "process", maxConcurrent: 1, budgetUsd: 0,
    deniedModels: [], timeoutMs: 24 * 60 * MINUTE, retainRuns: true, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    ...(snapshot ? { admissionSnapshotPath: admission } : {}),
  });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:permit-test", name: "main", kind: "main", sessionId: "permit-test" };
  const deliveries: Array<Parameters<ConstructorParameters<typeof ActorManager>[5]>[0]> = [];
  const createActors = () => new ActorManager("permit-test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, delivery => { deliveries.push(delivery); }, {
      actorRoot: path.join(root, "actors"), persistent: true, retentionSweepMs: 20,
    });
  const actors = createActors();
  const f = { root, admission, agents, actors, deliveries, createActors };
  fixtures.push(f);
  return f;
}
const snapshot = (f: ReturnType<typeof fixture>) => JSON.parse(fs.readFileSync(f.admission, "utf8")) as {
  held: Array<{ runId: string; lastProgressAt: number }>; waiting: unknown[];
};
const launched = async (f: ReturnType<typeof fixture>, actorId: string): Promise<Worker> => {
  let worker!: Worker;
  await eventually(() => {
    const runId = f.actors.status(actorId).inFlightRun?.id;
    worker = [...workers.values()].find(candidate => candidate.launch.id === runId)!;
    expect(worker).toBeDefined();
    expect(f.agents.admissionProgress(runId!)?.lastProgressAt).toBeDefined();
  });
  // Observe the initial running record before skewing time; first observation is progress.
  await ticks();
  return worker;
};
const save = (worker: Worker, patch: Partial<AgentRunRecord>) => {
  worker.record = { ...worker.record, ...patch };
  fs.writeFileSync(worker.statusFile, JSON.stringify(worker.record));
};
function complete(worker: Worker) {
  worker.alive = false;
  worker.descendants = false;
  delete worker.debt;
  delete worker.probeError;
  worker.nativeClose();
  // A lost terminal result must stay lost, including during fixture close.
  const saved = JSON.parse(fs.readFileSync(worker.statusFile, "utf8")) as AgentRunRecord;
  if (saved.errorCode !== "WORKER_GONE") save(worker, { status: "completed", text: "finished", finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
}

// Counterfactual: the pre-fix code retries startup/resumes the lost event or
// strands its permit in #settle's actor/native fence, so the second ask never launches.
describe("resident actor permit watch (#9031)", () => {
  it.each(["absent", "reused", "startup", "resume"] as const)("%s worker settles lost once, releases the real permit/snapshot and runs the next event", async mode => {
    const f = fixture();
    const actor = await f.actors.create({ name: "permit-owner", instructions: "Respond", responseMode: "text", coalesce: false });
    await f.actors.tell(actor.id, "lost event");
    const first = await launched(f, actor.id);
    const next = f.actors.ask(actor.id, "next event");
    void next.catch(() => undefined);
    if (mode === "startup") save(first, { status: "failed", error: "Agent transport exited without a result" });
    if (mode === "resume") save(first, { status: "stopped", error: "Agent transport exited without a result", turns: 2, toolCalls: 1 });
    // A forged live status identity is never an authority for the decision.
    save(first, { sessionId: String(process.pid) });
    if (mode === "reused") first.birth = "67890";
    else first.alive = false;
    // The native contract independently confirms tree/native exit.
    first.alive = false;
    first.identityAlive = mode === "reused";
    first.nativeClose();
    await eventually(() => expect(f.agents.status(first.launch.id)).toMatchObject({ status: "failed", errorCode: "WORKER_GONE", settledLost: true }));
    await eventually(() => expect([...workers.values()]).toHaveLength(2));
    const second = [...workers.values()][1]!;
    await eventually(() => expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]));
    expect(f.deliveries.filter(delivery => delivery.message.data && (delivery.message.data as { reason?: string }).reason === "worker-gone")).toHaveLength(1);
    expect(f.deliveries[0]).toMatchObject({ delivery: "followUp", triggerTurn: true, message: { source: "fabric-host" } });
    complete(second);
    await expect(next).resolves.toMatchObject({ text: "finished" });
    await eventually(() => expect(snapshot(f).held).toEqual([]));
    expect([...workers.values()].filter(worker => worker.record.task.includes("lost event"))).toHaveLength(1);
    await f.actors.close();
    f.actors = f.createActors();
    await ticks();
    expect([...workers.values()]).toHaveLength(2); // no startup/queue restore replay
  });

  it("committed actor success with an already-gone worker remains deliverable success", async () => {
    const f = fixture();
    const actor = await f.actors.create({ name: "successful", instructions: "Respond", responseMode: "text" });
    const answer = f.actors.ask(actor.id, "finish normally");
    const worker = await launched(f, actor.id);
    complete(worker);
    await expect(answer).resolves.toMatchObject({ text: "finished" });
    expect(f.agents.status(worker.launch.id)).toMatchObject({ status: "completed" });
    expect(f.deliveries.every(delivery => delivery.message.source !== "fabric-host")).toBe(true);
    expect([...workers.values()]).toHaveLength(1);
  });

  it("success committed during the native/tree join is not overwritten by the earlier running snapshot", async () => {
    const f = fixture();
    const launch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
    vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
      const handle = await launch(request);
      const worker = [...workers.values()].find(candidate => candidate.launch.id === request.id)!;
      return { ...handle, stop: async () => { worker.stops++; complete(worker); } };
    });
    const actor = await f.actors.create({ name: "close-race", instructions: "Respond", responseMode: "text" });
    const answer = f.actors.ask(actor.id, "finish at close");
    const worker = await launched(f, actor.id);
    worker.alive = false;
    await expect(answer).resolves.toMatchObject({ text: "finished" });
    expect(f.agents.status(worker.launch.id)).toMatchObject({ status: "completed" });
    expect(f.deliveries.every(delivery => delivery.message.source !== "fabric-host")).toBe(true);
  });

  it("a stop/ownership abort after proven death preserves typed loss instead of making a replayable stop", async () => {
    const f = fixture();
    const actor = await f.actors.create({ name: "stop-race", instructions: "Respond", responseMode: "text" });
    const answer = f.actors.ask(actor.id, "lost before abort");
    void answer.catch(() => undefined);
    const worker = await launched(f, actor.id);
    worker.alive = false;
    worker.nativeClose();
    await expect(f.agents.stop(worker.launch.id, { consume: false })).resolves.toMatchObject({ errorCode: "WORKER_GONE", settledLost: true });
    await expect(answer).rejects.toMatchObject({ code: "WORKER_GONE" });
    expect([...workers.values()]).toHaveLength(1);
  });

  it("ordinary task-agent transport failures still retry, and a final-answer receipt remains completed", async () => {
    const f = fixture();
    const handle = await f.agents.spawn({ task: "ordinary task", transport: "process" });
    const first = [...workers.values()][0]!;
    save(first, { status: "failed", error: "Agent transport exited without a result" });
    first.alive = false;
    first.nativeClose();
    await eventually(() => expect([...workers.values()]).toHaveLength(2));
    const replacement = [...workers.values()][1]!;
    const recordedAt = Date.now();
    const receipt = { version: 1, id: "normal-final-answer", runId: handle.id, recordedAt, text: "committed answer" };
    complete(replacement);
    save(replacement, { finalAnswerReceipt: { id: receipt.id, recordedAt } });
    fs.writeFileSync(path.join(path.dirname(replacement.statusFile), "final-answer.json"), JSON.stringify(receipt));
    await expect(f.agents.wait(handle.id)).resolves.toMatchObject({ status: "completed", text: "committed answer" });
    expect(f.agents.status(handle.id)).not.toHaveProperty("errorCode", "WORKER_GONE");
  });

  it("loss delivered after ownership moves is consumed, never parked or replayed on reclaim", async () => {
    const f = fixture();
    const actor = await f.actors.create({ name: "moving-owner", instructions: "Respond", responseMode: "text", coalesce: false });
    const run = f.agents.run.bind(f.agents);
    vi.spyOn(f.agents, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      if (result.errorCode === "WORKER_GONE") await f.actors.cede(actor.id);
      return result;
    });
    await f.actors.tell(actor.id, "lost across ownership");
    const worker = await launched(f, actor.id);
    worker.alive = false;
    worker.nativeClose();
    await eventually(() => expect(f.agents.status(worker.launch.id)).toMatchObject({ errorCode: "WORKER_GONE" }));
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    f.actors.reclaim(actor.id);
    await ticks();
    expect([...workers.values()]).toHaveLength(1);
    expect(f.actors.status(actor.id).queued).toBe(0);
    expect(f.deliveries).toContainEqual(expect.objectContaining({ message: expect.objectContaining({ source: "fabric-host", runId: worker.launch.id }) }));
  });

  it.each([true, false])("alive and silent for >20 min: one unclaimed owner alarm per run, permit retained (snapshot=%s)", async useSnapshot => {
    const f = fixture(useSnapshot);
    const actor = await f.actors.create({ name: "silent-owner", instructions: "Wait", responseMode: "text" });
    await f.actors.tell(actor.id, "silent event");
    const worker = await launched(f, actor.id);
    skew = 21 * MINUTE;
    await eventually(() => expect(f.deliveries).toHaveLength(1));
    expect(f.deliveries[0]).toMatchObject({ delivery: "followUp", triggerTurn: true,
      message: { source: "fabric-host", runId: worker.launch.id, data: { reason: "worker-silent", actorId: actor.id, ageMs: expect.any(Number) } } });
    expect(f.deliveries[0]!.message.text).toContain("silent-owner");
    expect(f.deliveries[0]!.message.text).toContain(worker.launch.id);
    expect(f.deliveries[0]!.message).not.toHaveProperty("principal");
    skew = 7 * 60 * MINUTE;
    await ticks();
    expect(f.deliveries).toHaveLength(1);
    expect(f.agents.admissionProgress(worker.launch.id)).toBeDefined();
    if (useSnapshot) expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(worker.stops).toBe(0);
    complete(worker);
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    await f.actors.tell(actor.id, "second silent run");
    const second = await launched(f, actor.id);
    expect(second.launch.id).not.toBe(worker.launch.id);
    skew += 21 * MINUTE;
    await eventually(() => expect(f.deliveries.filter(delivery =>
      (delivery.message.data as { reason?: string } | undefined)?.reason === "worker-silent")).toHaveLength(2));
  });

  it("a live actor making progress raises no silence alarm", async () => {
    const f = fixture(false);
    const actor = await f.actors.create({ name: "progress", instructions: "Work", responseMode: "text" });
    await f.actors.tell(actor.id, "work");
    const worker = await launched(f, actor.id);
    for (let step = 1; step <= 3; step++) {
      skew = step * 19 * MINUTE;
      save(worker, { turns: step, updatedAt: Date.now() });
      // The existing status/lifecycle path supplies progress, not wall-clock reads.
      await eventually(() => expect(f.agents.admissionProgress(worker.launch.id)!.lastProgressAt).toBeGreaterThanOrEqual(worker.record.updatedAt));
      await ticks();
    }
    expect(f.deliveries).toEqual([]);
    expect(worker.stops).toBe(0);
  });

  it.each(["unreadable", "missing-birth", "empty-birth", "EPERM", "EIO"] as const)("%s identity cannot release admission", async fault => {
    if (fault === "missing-birth" || fault === "empty-birth") {
      vi.mocked(processIdentity.processStartTime).mockImplementation(pid => workers.has(pid) ? (fault === "empty-birth" ? "" : undefined) : "12345");
    }
    const f = fixture();
    const actor = await f.actors.create({ name: "uncertain", instructions: "Wait", responseMode: "text" });
    await f.actors.tell(actor.id, "event");
    const worker = await launched(f, actor.id);
    if (fault === "unreadable") worker.birth = undefined;
    if (fault === "EPERM" || fault === "EIO") { worker.probeError = fault; worker.birth = "different"; }
    skew = 21 * MINUTE;
    await ticks();
    expect(f.agents.status(worker.launch.id).status).toBe("running");
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect(worker.stops).toBe(0);
  });

  it.each(["descendants", "native-debt"] as const)("gone leader with %s keeps the run joined and permit held", async fault => {
    const f = fixture();
    const actor = await f.actors.create({ name: "custody", instructions: "Wait", responseMode: "text", coalesce: false });
    await f.actors.tell(actor.id, "lost event");
    const worker = await launched(f, actor.id);
    await f.actors.tell(actor.id, "next event");
    worker.alive = false;
    worker.nativeClose();
    if (fault === "descendants") worker.descendants = true;
    else worker.debt = "native tree close unconfirmed";
    if (fault === "descendants") await eventually(() => expect(worker.stops).toBeGreaterThan(0));
    await ticks();
    expect([...workers.values()]).toHaveLength(1);
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    // Teardown must not launch a second fake worker while waiting for close.
    const nextLaunch = vi.mocked(ProcessTransport.prototype.launch).getMockImplementation()!;
    vi.mocked(ProcessTransport.prototype.launch).mockImplementation(async request => {
      const handle = await nextLaunch(request);
      const next = [...workers.values()].find(candidate => candidate.launch.id === request.id)!;
      complete(next);
      return handle;
    });
  });
});
