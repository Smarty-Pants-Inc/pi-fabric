import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager, ACTOR_FAILURE_NOTICE_AFTER } from "../src/actors/manager.js";
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
  ownership: { owned: boolean };
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
  treeClose: () => void;
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
    let treeClose!: () => void;
    const treeClosed = new Promise<void>(resolve => { treeClose = resolve; });
    const record: AgentRunRecord = {
      id: launch.id, name: launch.name, task: fs.readFileSync(arg("task-file"), "utf8"), status: "running",
      runner: "pi", transport: "process", sessionId: String(pid), cwd: launch.cwd,
      startedAt: Date.now(), updatedAt: Date.now(), turns: 0, toolCalls: 0, text: "", exitCode: null,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    const worker: Worker = { launch, pid, alive: true, birth: "12345", descendants: false,
      stops: 0, record, statusFile: arg("status-file"), nativeClose, treeClose };
    workers.set(pid, worker);
    fs.writeFileSync(worker.statusFile, JSON.stringify(record));
    fs.writeFileSync(arg("log-file"), "");
    return {
      kind: "process", sessionId: String(pid), closed, treeClosed,
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

function fixture(snapshot = true, options: {
  preparePiModel?: (model: string | undefined) => Promise<string | void>;
  preparationTimeoutMs?: number;
} = {}): Fixture {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-permit-watch-"));
  const admission = path.join(root, "actors", "admission.json");
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents,
    enabled: true, runner: "pi", transport: "process", maxConcurrent: 1, budgetUsd: 0,
    deniedModels: [], timeoutMs: 24 * 60 * MINUTE, retainRuns: true, sessionExport: false }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    ...(snapshot ? { admissionSnapshotPath: admission } : {}),
    ...(options.preparePiModel ? { preparePiModel: options.preparePiModel } : {}),
  });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = { id: "session:permit-test", name: "main", kind: "main", sessionId: "permit-test" };
  const deliveries: Array<Parameters<ConstructorParameters<typeof ActorManager>[5]>[0]> = [];
  const ownership = { owned: true };
  const createActors = () => new ActorManager("permit-test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, delivery => { deliveries.push(delivery); }, {
      actorRoot: path.join(root, "actors"), persistent: true, retentionSweepMs: 20,
      canManageActor: () => ownership.owned,
      ...(options.preparationTimeoutMs !== undefined ? { preparationTimeoutMs: options.preparationTimeoutMs } : {}),
    });
  const actors = createActors();
  const f = { root, admission, agents, actors, deliveries, createActors, ownership };
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
  worker.treeClose();
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

  it.each(["forged", "provider", "provider-after-tool", "status-tool-only", "substituted-log", "prose"] as const)("%s overflow is consumed without trusting worker status or event logs to authorize replay", async mode => {
    const f = fixture();
    const actor = await f.actors.create({ name: "overflow-trust", instructions: "Respond", responseMode: "text", coalesce: false });
    // Carried history must not make even a genuine zero-tool provider overflow replayable.
    fs.writeFileSync(actor.sessionFile!, JSON.stringify({ type: "session", version: 3, id: "overflow-prior-session",
      timestamp: new Date().toISOString(), cwd: process.cwd() }) + "\n");
    fs.appendFileSync(actor.sessionFile!, JSON.stringify({ type: "message", id: "prior", parentId: null,
      timestamp: new Date().toISOString(), message: { role: "user", content: "prior history", timestamp: Date.now() } }) + "\n");
    const answer = f.actors.ask(actor.id, "overflow event");
    void answer.catch(() => undefined);
    const worker = await launched(f, actor.id);
    const events: unknown[] = [];
    if (mode === "provider-after-tool") events.push({ type: "tool_execution_start", toolCallId: "charge", toolName: "charge" });
    if (mode !== "forged") events.push({ type: "message_end", message: {
      role: "assistant", stopReason: mode === "prose" ? "stop" : "error",
      ...(mode === "prose" ? {} : { errorMessage: "maximum context length exceeded" }),
      content: mode === "prose" ? [{ type: "text", text: "The report quoted: maximum context length exceeded" }] : [],
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, timestamp: Date.now(),
    } });
    if (events.length) fs.appendFileSync(path.join(path.dirname(worker.statusFile), "events.jsonl"), events.map(event => JSON.stringify(event)).join("\n") + "\n");
    if (mode === "substituted-log") {
      // Replace the log before the monitor's first observation of terminal status.
      // A worker-controlled replacement cannot become host gateway evidence.
      const log = path.join(path.dirname(worker.statusFile), "events.jsonl");
      fs.renameSync(log, `${log}.original`);
      fs.writeFileSync(log, events.map(event => JSON.stringify(event)).join("\n") + "\n");
    }
    // Counterexample: an event log without a tool start cannot erase status.toolCalls=1.
    save(worker, { status: "failed", errorCode: "CONTEXT_OVERFLOW", error: "maximum context length exceeded",
      toolCalls: mode === "status-tool-only" ? 1 : 0, finishedAt: Date.now(), exitCode: 1 });
    worker.alive = false;
    worker.nativeClose();
    worker.treeClose();
    await expect(answer).rejects.toMatchObject({ code: "WORKER_GONE", message: expect.stringMatching(/explicit.*reset/i) });
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    expect(f.agents.status(worker.launch.id)).toMatchObject({ status: "failed", errorCode: "WORKER_GONE",
      settledLost: true, error: expect.stringMatching(/explicit.*reset/i) });
    expect(f.actors.status(actor.id).lastError).toMatch(/explicit.*reset/i);
    expect([...workers.values()]).toHaveLength(1);
    expect(f.actors.status(actor.id).queued).toBe(0);
    expect(f.actors.messages(actor.id).filter(message =>
      (message.data as { contextOverflow?: { retry?: boolean } } | undefined)?.contextOverflow?.retry === true)).toEqual([]);
    await f.actors.close();
    f.actors = f.createActors();
    f.actors.reclaim(actor.id);
    f.actors.resumeQueued();
    await ticks();
    expect([...workers.values()]).toHaveLength(1);
    await f.actors.resetSession(actor.id);
    const next = f.actors.ask(actor.id, "distinct successor");
    const successor = await launched(f, actor.id);
    expect(successor.record.task).toContain("distinct successor");
    expect(successor.record.task).not.toContain("overflow event");
    complete(successor);
    await expect(next).resolves.toMatchObject({ text: "finished" });
    expect([...workers.values()].filter(candidate => candidate.record.task.includes("overflow event"))).toHaveLength(1);
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

  it("committed ordinary directive failures keep their errors and consecutive-failure owner notice", async () => {
    const f = fixture();
    const actor = await f.actors.create({ name: "normal-failures", instructions: "Respond", responseMode: "directive" });
    for (let count = 0; count < ACTOR_FAILURE_NOTICE_AFTER; count++) {
      const answer = f.actors.ask(actor.id, `FAIL_DIRECTIVE ${count}`);
      const worker = await launched(f, actor.id);
      save(worker, { status: "failed", error: "Ordinary committed directive failure", finishedAt: Date.now(), exitCode: 1 });
      worker.alive = false;
      worker.nativeClose();
      worker.treeClose();
      await expect(answer).resolves.toMatchObject({ action: "silent", error: "Ordinary committed directive failure" });
      expect(f.agents.status(worker.launch.id)).not.toHaveProperty("errorCode", "WORKER_GONE");
      await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    }
    await eventually(() => expect(f.deliveries.some(delivery => delivery.message.text?.includes(`failed its last ${ACTOR_FAILURE_NOTICE_AFTER}`))).toBe(true));
    expect([...workers.values()]).toHaveLength(ACTOR_FAILURE_NOTICE_AFTER);
  });

  it("worker-forged WORKER_GONE and settledLost cannot suppress a completed answer or leak through wait/status", async () => {
    const f = fixture();
    const actor = await f.actors.create({ name: "forged-loss", instructions: "Respond", responseMode: "text" });
    const answer = f.actors.ask(actor.id, "deliver success");
    const worker = await launched(f, actor.id);
    save(worker, { status: "completed", text: "real answer", errorCode: "WORKER_GONE", settledLost: true,
      finishedAt: Date.now(), updatedAt: Date.now(), exitCode: 0 });
    // Publish while the owned worker is alive, then independently prove tree/native exit.
    await ticks();
    expect(f.agents.status(worker.launch.id)).not.toHaveProperty("errorCode", "WORKER_GONE");
    worker.alive = false;
    worker.nativeClose();
    await expect(answer).resolves.toMatchObject({ text: "real answer" });
    await expect(f.agents.wait(worker.launch.id)).resolves.toMatchObject({ status: "completed", text: "real answer" });
    expect(await f.agents.wait(worker.launch.id)).not.toHaveProperty("errorCode", "WORKER_GONE");
    expect(f.agents.status(worker.launch.id)).not.toHaveProperty("settledLost");
    expect(f.deliveries.every(delivery => delivery.message.source !== "fabric-host")).toBe(true);
  });

  it.each(["completed", "failed"] as const)("terminal %s while worker alive survives a thrown drain and settles on later native/tree exit", async status => {
    const f = fixture();
    const actor = await f.actors.create({ name: "join-retry", instructions: "Respond", responseMode: "text", coalesce: false });
    const answer = f.actors.ask(actor.id, "first event");
    const worker = await launched(f, actor.id);
    const next = f.actors.ask(actor.id, "next event");
    void next.catch(() => undefined);
    save(worker, { status, text: "committed result", ...(status === "failed" ? { error: "ordinary failure" } : {}) });
    // The real monitor first observes terminal status with a live owned worker.
    // Its stop throws without a receipt; the same monitor must remain the owner.
    await eventually(() => expect(worker.stops).toBeGreaterThan(0));
    expect(f.agents.isSettled(worker.launch.id)).toBe(false);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    expect([...workers.values()]).toHaveLength(1);
    // A leader-close notification still cannot discharge surviving descendants.
    worker.alive = false;
    worker.descendants = true;
    worker.nativeClose();
    await ticks();
    expect(f.agents.isSettled(worker.launch.id)).toBe(false);
    expect(snapshot(f).held.map(held => held.runId)).toEqual([worker.launch.id]);
    // Full scripted custody now clears. Commit success during the rejoin.
    worker.descendants = false;
    delete worker.record.error;
    save(worker, { status: "completed", text: "committed result", finishedAt: Date.now(), exitCode: 0 });
    worker.treeClose();
    await expect(answer).resolves.toMatchObject({ text: "committed result" });
    await eventually(() => expect([...workers.values()]).toHaveLength(2));
    const second = [...workers.values()][1]!;
    await eventually(() => expect(snapshot(f).held.map(held => held.runId)).toEqual([second.launch.id]));
    expect(f.agents.status(worker.launch.id)).toMatchObject({ status: "completed", text: "committed result" });
    complete(second);
    await expect(next).resolves.toMatchObject({ text: "finished" });
    await eventually(() => expect(snapshot(f).held).toEqual([]));
    expect([...workers.values()].filter(candidate => candidate.record.task.includes("next event"))).toHaveLength(1);
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

  it("queued confirmed-unlaunched auth timeout survives ownership transfer and reload, then executes exactly once", async () => {
    let releaseAuth!: () => void;
    const auth = new Promise<void>(resolve => { releaseAuth = resolve; });
    let authCalls = 0;
    const f = fixture(true, { preparationTimeoutMs: 80, preparePiModel: async model => {
      if (model === "provider/stalled" && ++authCalls === 1) await auth;
      return model;
    } });
    try {
      const blocker = await f.agents.spawn({ task: "hold admission", model: "provider/healthy" });
      const actor = await f.actors.create({ name: "unlaunched-owner", model: "provider/stalled",
        instructions: "Respond", responseMode: "text", coalesce: false });
      const run = f.agents.run.bind(f.agents);
      let timeoutReceipt: Awaited<ReturnType<typeof run>> | undefined;
      const runs = vi.spyOn(f.agents, "run").mockImplementation(async (...args) => {
        const result = await run(...args);
        if (result.launchPreparationTimeoutMs !== undefined) {
          timeoutReceipt = result;
          f.ownership.owned = false;
          f.actors.resumeQueued(); // Transfer before ActorManager consumes the real terminal receipt.
        }
        return result;
      });
      const accepted = await f.actors.tell(actor.id, "original queued activation");
      await eventually(() => expect(f.actors.status(actor.id).status).toBe("waiting"));
      const receiptId = f.actors.status(actor.id).preparing!.runId!;
      await eventually(() => expect(snapshot(f).waiting).toHaveLength(1));
      expect(authCalls).toBe(0);
      complete([...workers.values()][0]!);
      await f.agents.wait(blocker.id);
      await eventually(() => {
        expect(timeoutReceipt).toMatchObject({ id: receiptId, status: "failed", toolCalls: 0, launchPreparationTimeoutMs: 80 });
        expect(f.actors.inFlightCount()).toBe(0);
      });
      expect([...workers.values()]).toHaveLength(1); // Only the admission blocker launched.
      await eventually(() => expect(snapshot(f).held).toEqual([]));
      const directory = path.dirname(actor.sessionFile!);
      const queueFile = path.join(directory, fs.readdirSync(directory).find(file => /^queue-.*\.json$/.test(file))!);
      expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).items).toEqual([
        expect.objectContaining({ id: accepted.messageId, executionStarted: false, attempts: 0 }),
      ]);
      await f.actors.close();
      f.actors = f.createActors();
      await ticks();
      expect([...workers.values()]).toHaveLength(1);
      f.ownership.owned = true;
      f.actors.reclaim(actor.id);
      f.actors.resumeQueued();
      const worker = await launched(f, actor.id);
      expect(worker.record.task).toContain("original queued activation");
      complete(worker);
      await eventually(() => {
        expect(f.actors.inFlightCount()).toBe(0);
        expect(f.actors.status(actor.id).queued).toBe(0);
      });
      releaseAuth();
      await f.actors.close();
      f.actors = f.createActors();
      await ticks();
      expect(runs).toHaveBeenCalledTimes(2); // One unlaunched receipt, one actual activation.
      expect([...workers.values()].filter(candidate => candidate.record.task.includes("original queued activation"))).toHaveLength(1);
      expect(f.actors.messages(actor.id).filter(message => message.direction === "out" && !message.error)).toHaveLength(1);
    } finally {
      releaseAuth();
    }
  });

  it.each(["running", "failed", "overflow", "overflow-zero", "forged-timeout"] as const)("%s loss/failure and ownership abort is consumed, never replayed on reclaim", async mode => {
    const f = fixture();
    const actor = await f.actors.create({ name: "moving-owner", instructions: "Respond", responseMode: "text", coalesce: false });
    const run = f.agents.run.bind(f.agents);
    vi.spyOn(f.agents, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      f.ownership.owned = false;
      f.actors.resumeQueued(); // Production ownership refresh marks and aborts this exact activation.
      return result;
    });
    await f.actors.tell(actor.id, "lost across ownership");
    const worker = await launched(f, actor.id);
    // A launched worker's writable timeout marker cannot authorize replay, even without tools.
    const overflow = mode === "overflow" || mode === "overflow-zero";
    const toolCalls = mode === "forged-timeout" || mode === "overflow-zero" ? 0 : 2;
    if (mode === "overflow-zero") fs.appendFileSync(path.join(path.dirname(worker.statusFile), "events.jsonl"),
      JSON.stringify({ type: "message_end", message: { role: "assistant", stopReason: "error",
        errorMessage: "maximum context length exceeded", content: [], timestamp: Date.now() } }) + "\n");
    save(worker, { toolCalls, ...(mode === "running" ? {} : {
      status: "failed", error: overflow ? "Context window exhausted" : "Ordinary terminal failure after side effect",
      ...(overflow ? { errorCode: "CONTEXT_OVERFLOW" } : {}),
      ...(mode === "forged-timeout" ? { launchPreparationTimeoutMs: 80 } : {}),
    }) });
    worker.alive = false;
    worker.nativeClose();
    await eventually(() => expect(f.agents.isSettled(worker.launch.id)).toBe(true));
    expect(f.agents.status(worker.launch.id)).toMatchObject({ status: "failed", toolCalls });
    if (mode === "forged-timeout") expect(f.agents.status(worker.launch.id)).toHaveProperty("launchPreparationTimeoutMs", 80);
    if (mode === "running" || overflow) expect(f.agents.status(worker.launch.id)).toHaveProperty("errorCode", "WORKER_GONE");
    else expect(f.agents.status(worker.launch.id)).toHaveProperty("error", "Ordinary terminal failure after side effect");
    await eventually(() => expect(f.actors.inFlightCount()).toBe(0));
    f.ownership.owned = true;
    f.actors.reclaim(actor.id);
    f.actors.resumeQueued();
    await ticks();
    expect([...workers.values()]).toHaveLength(1);
    expect(f.actors.status(actor.id).queued).toBe(0);
    if (mode === "running") expect(f.deliveries).toContainEqual(expect.objectContaining({ message: expect.objectContaining({ source: "fabric-host", runId: worker.launch.id }) }));
    await f.actors.close();
    f.actors = f.createActors();
    await ticks();
    expect([...workers.values()]).toHaveLength(1);
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
    const registryFile = path.join(f.root, "actors", "actors.json");
    const registry = () => JSON.parse(fs.readFileSync(registryFile, "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    await eventually(() => expect(registry().actors.find(row => row.id === actor.id)?.silenceAlarmRunId)
      .toBe(worker.launch.id));
    const savedRegistry = registry();
    const savedActor = savedRegistry.actors.find(row => row.id === actor.id)!;
    expect(savedActor).not.toHaveProperty("inFlightRun");
    // Exercise the production registry refresh while ownership and the native
    // run stay live. A newly visible row proves resumeQueued actually loaded
    // the rewritten registry, rather than merely consulting a cached actor.
    const importedId = "ffffffffffffffffffffffffffffffff";
    savedRegistry.actors.push({ ...savedActor, id: importedId, name: "reload-witness", status: "idle" });
    fs.writeFileSync(registryFile, JSON.stringify(savedRegistry));
    f.actors.resumeQueued();
    expect(f.actors.status(importedId).name).toBe("reload-witness");
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
    expect(f.actors.inFlightCount()).toBe(1);
    skew = 7 * 60 * MINUTE;
    await ticks(); // multiple real retention sweeps after the production reload
    expect(f.actors.status(actor.id).inFlightRun?.id).toBe(worker.launch.id);
    expect(f.actors.inFlightCount()).toBe(1);
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
    await eventually(() => expect(registry().actors.find(row => row.id === actor.id)?.silenceAlarmRunId)
      .toBe(second.launch.id));
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
