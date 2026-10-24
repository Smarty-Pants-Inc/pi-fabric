import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import type { FabricActorRequest, FabricActorValidityFacts } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { hasDeliveryOutcome } from "../src/mesh/delivery-outcomes.js";
import { normalizeScope, senderStamp } from "../src/scope.js";
import { ExecutionDeadline } from "../src/runtime/execution-deadline.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

// Keep reason: #4605 ADV-1/data safety. Exercise the real monitor receiver, queue,
// checkpoint and AgentManager.run boundary, not private decision helper stand-ins.
const TOPIC = "github.record-decisions.pulls";
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "session:forwarder", name: "forwarder", kind: "main", sessionId: "forwarder" };
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0)) await close(); vi.restoreAllMocks(); });
const waitFor = async (predicate: () => boolean, timeout = 10_000) => {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("record-decision flow timed out");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const frame = (task: string): { payload: MeshEvent; untrusted?: true; omittedBefore?: { count: number }; coalesced?: Array<{ id: string; sender?: unknown; principal?: unknown; payload?: MeshEvent; dataOmitted?: true; omittedBefore?: { count: number; firstSequence: number; lastSequence: number; countIsLowerBound?: true } }> } => {
  for (const block of task.split("\n\n")) {
    try { const parsed = JSON.parse(block); const value = Array.isArray(parsed) ? parsed[0] : parsed;
      if (value?.source && value.payload) return value; } catch { /* label */ }
  }
  throw new Error("missing run frame");
};
const ids = (task: string) => frame(task).coalesced?.map(record => record.id) ?? [frame(task).payload.id];
const skipChecks = [{ id: "checks", where: [{ path: "data.event", equals: "check_run" }] }];
const skipComments = [{ id: "comments", where: [{ path: "data.event", equals: "issue_comment" }] }];

const world = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-record-decisions-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const runs: AgentRunRequest[] = [];
  const releases: Array<() => void> = [];
  const unstarted = new Set<number>();
  let flowing = false;
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
    const n = runs.push(request);
    const id = `record-decision-${n}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    if (!flowing) await new Promise<void>((resolve, reject) => {
      const abort = () => reject(new Error("record-decision run aborted"));
      releases[n - 1] = () => { signal?.removeEventListener("abort", abort); resolve(); };
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
    });
    return { id, name: "recorded", task: request.task, status: unstarted.has(n - 1) ? "failed" : "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      ...(unstarted.has(n - 1) ? { error: "confirmed unstarted test activation" } : {}),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: !unstarted.has(n - 1), text: `answer-${n}`, value: { action: "message", message: `answer-${n}` },
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } } satisfies AgentRunResult;
  });
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  let actors: ActorManager;
  const open = () => (actors = new ActorManager("test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 100,
      meshRetentionSweepPath: false, canConsumeMesh: () => false,
    }));
  open();
  const flow = () => { flowing = true; for (const release of releases) release?.(); };
  cleanup.push(async () => { flow(); await actors.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  return {
    mesh, agents, runs, get actors() { return actors; }, root, flow,
    create: (extra: Partial<FabricActorRequest> = {}) => actors.create({ name: "reviewer", instructions: "Review.", topics: [TOPIC],
      coalesce: false, coalesceKey: "payload.number", responseMode: "text", delivery: "mailbox", triggerTurn: false, ...extra }),
    publish: (event: string, key = event, number = 7, body = "", producer: MeshIdentity = from) => mesh.publish({ topic: TOPIC, kind: "github.webhook", from: producer,
      data: { event, key, payload: { action: "created", number, body } } }),
    admit: (...events: MeshEvent[]) => { for (const event of events) expect(monitor.callbacks.onEvent(event)).toBe(true); },
    block: async (id: string) => { actors.tell(id, "blocker"); await waitFor(() => runs.length === 1); },
    release: (index: number) => releases[index]?.(),
    unstarted: (index: number) => unstarted.add(index),
    idle: async (id: string, count: number) => { flow(); await waitFor(() => actors.status(id).status === "idle" && runs.length >= count); expect(runs).toHaveLength(count); },
    reopen: async () => { await actors.close(); return open(); },
  };
};

// Keep reason: #4605 P3-3/data safety. Run the real predicate and deadline
// comparison, but advance only its deadline clock, never the actor/mesh clock.
// Async-local state keeps overlapping evaluations separate. The Date.now spy is
// synchronous and restored before returning from the real deadline getter.
type PredicateObservation = {
  facts: FabricActorValidityFacts | undefined;
  terminationReason: string;
  value: unknown;
  error: string | undefined;
};
const withPredicateClock = async (
  run: (observed: PredicateObservation[]) => Promise<void>,
  expireAfterEvaluation: (facts: FabricActorValidityFacts) => boolean = () => false,
) => {
  const clock = new AsyncLocalStorage<{ expire: boolean; elapsedMs: number }>();
  const observed: PredicateObservation[] = [];
  const realExecute = QuickJsRuntime.prototype.execute;
  const realReached = Object.getOwnPropertyDescriptor(ExecutionDeadline.prototype, "reached")!.get!;
  const realSchedule = ExecutionDeadline.prototype.scheduleDeadline;
  const reached = vi.spyOn(ExecutionDeadline.prototype, "reached", "get").mockImplementation(function (this: ExecutionDeadline) {
    const active = clock.getStore();
    if (!active) return realReached.call(this) as boolean;
    const now = vi.spyOn(Date, "now").mockReturnValue(this.startedAt + active.elapsedMs);
    try { return realReached.call(this) as boolean; }
    finally { now.mockRestore(); }
  });
  const schedule = vi.spyOn(ExecutionDeadline.prototype, "scheduleDeadline").mockImplementation(function (this: ExecutionDeadline, ...args) {
    const active = clock.getStore();
    // QuickJS first schedules this timer after evalCode/pumpJobs. Expiring here
    // models descheduling at the result boundary, without delaying or bypassing
    // the guest, changing its verdict, or raising the production 100ms budget.
    if (active?.expire) active.elapsedMs = this.timeoutMs;
    return realSchedule.apply(this, args);
  });
  const execute = vi.spyOn(QuickJsRuntime.prototype, "execute").mockImplementation(function (this: QuickJsRuntime, code, hostCall, options) {
    const facts = options.strings?.facts ? JSON.parse(options.strings.facts) as FabricActorValidityFacts : undefined;
    return clock.run({ expire: facts !== undefined && expireAfterEvaluation(facts), elapsedMs: 0 }, async () => {
      const result = await realExecute.call(this, code, hostCall, options);
      observed.push({ facts, terminationReason: result.terminationReason, value: result.value, error: result.error });
      return result;
    });
  });
  try { await run(observed); }
  finally { execute.mockRestore(); schedule.mockRestore(); reached.mockRestore(); clock.disable(); }
};

describe("coalesced record decisions use each original input (#4605 round 2)", () => {
  it("ADV-1 newest fails / older survives: retroactive filter promotes the comment and receipts name the dropped check", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.actors.setActivationFilter(actor.id, skipChecks);
    expect(w.actors.status(actor.id).queued).toBe(1);
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(comment);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id]);
    const skips = w.actors.messages(actor.id).filter(message => message.reason === "filtered: checks");
    expect(skips).toHaveLength(1);
    expect(skips[0]!.data).toMatchObject({ filteredEventId: check.id, sequence: check.sequence, topic: TOPIC });
    expect(w.actors.status(actor.id).filterSkipped?.count).toBe(1);
  });

  it("counterexample: older fails / newest survives, and all-fail receipts enumerate every dropped identity", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    const all1 = await w.publish("issue_comment", "all-1", 8);
    const all2 = await w.publish("issue_comment", "all-2", 8);
    w.admit(comment, check, all1, all2);
    await w.actors.setActivationFilter(actor.id, skipComments);
    expect(w.actors.status(actor.id).queued).toBe(1);
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(check);
    expect(ids(w.runs[1]!.task)).toEqual([check.id]);
    const dropped = w.actors.messages(actor.id).filter(message => message.reason === "filtered: comments")
      .map(message => (message.data as { filteredEventId: string }).filteredEventId);
    expect(dropped).toEqual([comment.id, all1.id, all2.id]);
    expect(w.actors.status(actor.id).filterSkipped?.count).toBe(3);
  });

  it("arrival filtering still prevents a rejected newer event from replacing eligible queued input", async () => {
    const w = world();
    const actor = await w.create({ activationFilter: skipChecks });
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(comment);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id]);
    expect(w.actors.messages(actor.id).find(message => message.reason === "filtered: checks")?.data)
      .toMatchObject({ filteredEventId: check.id });
  });

  it("validWhile uses the merged actor acceptance sequence with each original record's data", async () => {
    const w = world();
    const actor = await w.create({ validWhile: { version: 1,
      source: '({ activation }) => activation.kind !== "mesh" || (activation.sequence === 3 && activation.data.event === "issue_comment")' } });
    await w.block(actor.id); // actor acceptance 1
    for (let n = 0; n < 6; n++) await w.mesh.publish({ topic: "unrelated", kind: "padding", from });
    const comment = await w.publish("issue_comment"); // actor acceptance 2; mesh sequence deliberately different
    const check = await w.publish("check_run"); // actor acceptance 3
    expect(comment.sequence).not.toBe(2);
    w.admit(comment, check);
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(comment);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id]);
    expect(w.actors.messages(actor.id).some(message => message.stale &&
      (message.data as { invalidatedEventId?: string } | undefined)?.invalidatedEventId === check.id)).toBe(true);
  });

  it("post-run validWhile suppresses the combined directive when one input becomes stale during inference", async () => {
    const w = world();
    vi.spyOn(w.agents, "outputPrincipal").mockReturnValue({ id: "principal:inference", binding: "voice-call" });
    const actor = await w.create({ responseMode: "directive", validWhile: { version: 1, source: '({ activation, current }) => activation.kind !== "mesh" || activation.data.event === "issue_comment" || current.latestActivationSequence <= 3' } });
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    w.release(0);
    await waitFor(() => w.runs.length === 2);
    const captured = w.runs[1]!.task;
    expect(ids(captured)).toEqual([comment.id, check.id]);
    w.actors.tell(actor.id, "advance acceptance sequence while inference is active");
    await w.idle(actor.id, 3);
    expect(w.runs[1]!.task).toBe(captured);
    expect(frame(captured).payload).toEqual(check);
    const output = w.actors.messages(actor.id).filter(message => message.runId === "record-decision-2");
    expect(output.some(message => message.stale && message.action === "silent")).toBe(true);
    expect(output.every(message => message.text === undefined && message.principal === undefined)).toBe(true);
    expect(w.actors.messages(actor.id).some(message => message.text === "answer-2")).toBe(false);
  });

  it("a partial filter decision retains an exact unstarted retry manifest without false skip receipts", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    w.release(0);
    await waitFor(() => w.runs.length === 2);
    const captured = w.runs[1]!.task;
    w.unstarted(1);
    await w.actors.setActivationFilter(actor.id, skipChecks);
    w.release(1);
    await waitFor(() => w.runs.length === 3);
    expect(frame(w.runs[2]!.task).payload).toEqual(frame(captured).payload);
    expect(frame(w.runs[2]!.task).coalesced).toEqual(frame(captured).coalesced);
    expect(ids(w.runs[2]!.task)).toEqual([comment.id, check.id]);
    expect(w.actors.status(actor.id).filterSkipped?.count).toBe(0);
    expect(w.actors.messages(actor.id).find(message => message.reason === "filter match: retained in immutable activation")?.data)
      .toMatchObject({ retainedEventId: check.id });
    await w.idle(actor.id, 3);
  });

  it.each([true, false])("drain dedupe removes only the completed record (completed record newest: %s)", async (completedNewest) => {
    const w = world();
    const actor = await w.create({ residency: "durable", dedupeKey: "data.key" });
    const first = await w.publish("issue_comment", "occurrence-a");
    w.admit(first);
    await waitFor(() => w.runs.length === 1);
    const fresh = await w.publish("issue_comment", "occurrence-b");
    const repeat = await w.publish("check_run", "occurrence-a");
    w.admit(...(completedNewest ? [fresh, repeat] : [repeat, fresh]));
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(fresh);
    expect(ids(w.runs[1]!.task)).toEqual([fresh.id]);
    expect(w.actors.messages(actor.id).some(message => message.reason === "deduplicated: completed key" &&
      (message.data as { skippedEventId?: string } | undefined)?.skippedEventId === repeat.id)).toBe(true);
  });

  it("successful collapsed inference collects every record's occurrence key, not just the primary key", async () => {
    const w = world();
    const actor = await w.create({ residency: "durable", dedupeKey: "data.key" });
    await w.block(actor.id);
    const older = await w.publish("issue_comment", "occurrence-c", 9);
    const newer = await w.publish("check_run", "occurrence-d", 9);
    w.admit(older, newer);
    await w.idle(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([older.id, newer.id]);
    await w.reopen(); // completion fence must survive a fresh ActorManager, not just memory
    const duplicateC = await w.publish("issue_comment", "occurrence-c", 9);
    const duplicateD = await w.publish("check_run", "occurrence-d", 9);
    w.admit(duplicateC, duplicateD);
    w.actors.tell(actor.id, "barrier after both duplicate admissions");
    await waitFor(() => w.actors.status(actor.id).status === "idle" && w.runs.length >= 3);
    expect(w.runs).toHaveLength(3); // blocker, collapsed mesh run, barrier; no duplicate C run
  });

  it("batch collection checks occurrence keys inside a previously coalesced member before absorbing a duplicate", async () => {
    const w = world();
    const actor = await w.create({ residency: "durable", dedupeKey: "data.key" });
    await w.block(actor.id);
    const producer: MeshIdentity = { id: "agent:batch-producer", name: "batch-producer", kind: "agent" };
    const older = await w.publish("issue_comment", "batch-occurrence-a", 7, "", producer);
    const newer = await w.publish("check_run", "batch-occurrence-b", 7, "", producer);
    const duplicate = await w.publish("issue_comment", "batch-occurrence-a", 8, "", producer);
    w.admit(older, newer, duplicate);
    await w.actors.setBatching(actor.id, { batchWindowMs: 10, batchMax: 2 });
    await w.idle(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([older.id, newer.id]);
    const batches = w.runs[1]!.task.split("\n\n").flatMap(block => {
      try { const value = JSON.parse(block); return Array.isArray(value) ? [value] : []; } catch { return []; }
    });
    expect(batches).toHaveLength(1);
    expect(batches[0]).toHaveLength(1); // duplicate A remains pending until this inference completes
    expect(w.runs[1]!.task).not.toContain(duplicate.id);
    expect(w.actors.messages(actor.id).some(message => message.reason === "deduplicated: completed key" &&
      (message.data as { skippedEventId?: string } | undefined)?.skippedEventId === duplicate.id)).toBe(true);
  });

  it("promotion does not deliver the filtered newest addressed send or borrow its principal, including after restart", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const foreign = senderStamp(normalizeScope({ principal: { id: "foreign-owner" }, grants: [{ resource: `mesh:${TOPIC}`, actions: ["write"] }] }));
    const older = await w.mesh.publish({ topic: TOPIC, kind: "github.webhook", from, to: actor.id, sender: foreign,
      principal: { id: `oversized-${"x".repeat(300)}`, binding: "voice-call" },
      data: { event: "issue_comment", payload: { action: "created", number: 7 } } });
    const newest = await w.mesh.publish({ topic: TOPIC, kind: "github.webhook", from, to: actor.id, sender: senderStamp(undefined),
      principal: { id: "newest-trusted-principal", binding: "voice-call" },
      data: { event: "check_run", payload: { action: "created", number: 7 } } });
    w.admit(older, newest);
    await w.actors.setActivationFilter(actor.id, skipChecks);
    await w.reopen();
    await waitFor(() => w.runs.length === 2); // restored direct blocker holds promoted input
    await w.idle(actor.id, 3);
    const run = w.runs[2]!;
    expect(frame(run.task).payload).toEqual(older);
    expect(ids(run.task)).toEqual([older.id]);
    expect(run.provenance?.principal).toBeUndefined();
    expect(frame(run.task).untrusted).toBe(true);
    expect(frame(run.task).coalesced?.[0]?.sender).toBeUndefined();
    expect(frame(run.task).coalesced?.[0]?.principal).toBeUndefined();
    const send = (event: MeshEvent) => ({ eventId: event.id, to: actor.id, from: from.id, mode: "publish" as const });
    expect(hasDeliveryOutcome(w.mesh.root, send(older), "delivered")).toBe(true);
    expect(hasDeliveryOutcome(w.mesh.root, send(newest), "delivered")).toBe(false);
  });

  it("validWhile drops only the older failing input and names both records when all inputs fail", async () => withPredicateClock(async () => {
    const w = world();
    const actor = await w.create({ validWhile: { version: 1,
      source: '({ activation }) => activation.kind !== "mesh" || activation.data.event === "check_run"' } });
    await w.block(actor.id);
    const older = await w.publish("issue_comment");
    const newer = await w.publish("check_run");
    const all1 = await w.publish("issue_comment", "all-1", 8);
    const all2 = await w.publish("pull_request_review", "all-2", 8);
    w.admit(older, newer, all1, all2);
    await w.idle(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([newer.id]);
    const invalidated = w.actors.messages(actor.id).filter(message => message.stale)
      .flatMap(message => (message.data as { invalidatedEventId?: string } | undefined)?.invalidatedEventId ?? []);
    expect(new Set(invalidated)).toEqual(new Set([older.id, all1.id, all2.id]));
  }));

  it("a post-run predicate deadline fails closed and adds the otherwise-valid newer record to stale receipts", async () => {
    const originalNow = Date.now;
    const originalExecute = QuickJsRuntime.prototype.execute;
    const originalReached = Object.getOwnPropertyDescriptor(ExecutionDeadline.prototype, "reached")!.get;
    const originalSchedule = ExecutionDeadline.prototype.scheduleDeadline;
    const w = world();
    let inferenceFinished = false;
    await withPredicateClock(async (observed) => {
      const actor = await w.create({ validWhile: { version: 1,
        source: '({ activation }) => activation.kind !== "mesh" || activation.data.event === "check_run"' } });
      await w.block(actor.id);
      const older = await w.publish("issue_comment");
      const newer = await w.publish("check_run");
      const all1 = await w.publish("issue_comment", "all-1", 8);
      const all2 = await w.publish("pull_request_review", "all-2", 8);
      w.admit(older, newer, all1, all2);
      w.release(0);
      await waitFor(() => w.runs.length === 2);
      expect(ids(w.runs[1]!.task)).toEqual([newer.id]);
      inferenceFinished = true;
      await w.idle(actor.id, 2);
      const stale = w.actors.messages(actor.id).filter(message => message.stale);
      const invalidated = stale.flatMap(message =>
        (message.data as { invalidatedEventId?: string } | undefined)?.invalidatedEventId ?? []);
      expect(new Set(invalidated)).toEqual(new Set([older.id, newer.id, all1.id, all2.id]));
      expect(stale.find(message => (message.data as { invalidatedEventId?: string } | undefined)?.invalidatedEventId === newer.id))
        .toMatchObject({ reason: "validWhile: Execution timed out after 100ms", action: "silent" });
      expect(w.actors.messages(actor.id).some(message => message.text === "answer-2")).toBe(false);
      const newerEvaluations = observed.filter(entry => entry.facts?.activation.kind === "mesh" &&
        (entry.facts.activation.data as { event?: unknown } | undefined)?.event === "check_run");
      expect(newerEvaluations.some(entry => entry.terminationReason === "completed" && entry.value === true)).toBe(true);
      expect(newerEvaluations.at(-1)).toMatchObject({ terminationReason: "timed_out", error: "Execution timed out after 100ms" });
    }, facts => inferenceFinished && facts.activation.kind === "mesh" &&
      (facts.activation.data as { event?: unknown } | undefined)?.event === "check_run");
    expect(Date.now).toBe(originalNow);
    expect(QuickJsRuntime.prototype.execute).toBe(originalExecute);
    expect(Object.getOwnPropertyDescriptor(ExecutionDeadline.prototype, "reached")!.get).toBe(originalReached);
    expect(ExecutionDeadline.prototype.scheduleDeadline).toBe(originalSchedule);
  });

  it("pruning an omittedBefore carrier preserves the exact summary on the surviving primary", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const events: MeshEvent[] = [];
    for (let n = 0; n < 101; n++) events.push(await w.publish(n === 100 ? "issue_comment" : "check_run", `summary-${n}`));
    w.admit(...events);
    await w.actors.setActivationFilter(actor.id, skipChecks);
    await w.idle(actor.id, 2);
    const rendered = frame(w.runs[1]!.task);
    expect(rendered.payload).toEqual(events[100]);
    expect(rendered.coalesced).toHaveLength(1);
    const summary = rendered.coalesced?.[0]?.omittedBefore;
    expect(summary?.count).toBeGreaterThan(0); // either the count or rendered-byte cap may bind first
    expect(summary).toMatchObject({ firstSequence: events[0]!.sequence, lastSequence: events[summary!.count - 1]!.sequence });
    expect(summary?.countIsLowerBound).toBeUndefined();
    expect(w.actors.status(actor.id).filterSkipped?.count).toBe(events.length - summary!.count - 1);
    expect(rendered.coalesced?.[0]?.sender).toBeUndefined();
    expect(w.runs[1]!.task).toContain(`${summary!.count} older events summarized across sequence range ${summary!.firstSequence}..${summary!.lastSequence}`);
    expect(w.runs[1]!.task).not.toContain(`At least ${summary!.count} older`);
    expect(w.runs[1]!.task).toContain("verifying topic, coalesce key, identity and sequence");
    expect(w.runs[1]!.provenance?.principal).toBeUndefined();
  });

  it("a summary remains runnable when every individually retained record is filtered", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const events: MeshEvent[] = [];
    for (let n = 0; n < 101; n++) events.push(await w.publish("check_run", `all-summary-${n}`));
    w.admit(...events);
    await w.actors.setActivationFilter(actor.id, skipChecks);
    expect(w.actors.status(actor.id).queued).toBe(1);
    await w.reopen();
    await waitFor(() => w.runs.length === 2);
    await w.idle(actor.id, 3);
    const rendered = frame(w.runs[2]!.task);
    expect(rendered.payload).toMatchObject({ dataOmitted: true });
    expect(rendered.payload.data).toBeUndefined();
    expect(rendered.omittedBefore?.count).toBeGreaterThan(0);
    expect(rendered.untrusted).toBe(true);
  });

  it("metadata-only history uses verified real mesh read-back, never truncated event/action as a decision", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const events: MeshEvent[] = [];
    for (let n = 0; n < 55; n++) events.push(await w.publish(n === 0 ? "issue_comment" : "check_run", `readback-${n}`, 7, "x".repeat(2048)));
    w.admit(...events);
    const read = vi.spyOn(w.mesh, "read"); // observe the real read, no stand-in payload
    await w.actors.setActivationFilter(actor.id, skipChecks);
    expect(read.mock.calls.some(([input]) => input?.after === events[0]!.sequence - 1 && input.topic === TOPIC && input.limit === 1)).toBe(true);
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(events[0]);
    expect(ids(w.runs[1]!.task)).toEqual([events[0]!.id]);
    expect(w.actors.status(actor.id).filterSkipped?.count).toBe(54);
  });

  it("the full primary is checked when its bounded history copy and mesh read-back are unavailable", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run", "oversized-primary", 7, "x".repeat(40 * 1024));
    w.admit(comment, check);
    const realRead = w.mesh.read.bind(w.mesh);
    vi.spyOn(w.mesh, "read").mockImplementation(input => input?.after === check.sequence - 1 ? [] : realRead(input));
    await w.actors.setActivationFilter(actor.id, skipChecks);
    await w.idle(actor.id, 2);
    expect(frame(w.runs[1]!.task).payload).toEqual(comment);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id]);
  });

  it("unavailable or wrong-ID read-back conservatively retains unknown history when the known primary fails", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    const events: MeshEvent[] = [];
    for (let n = 0; n < 55; n++) events.push(await w.publish("check_run", `unavailable-${n}`, 7, "x".repeat(2048)));
    w.admit(...events);
    const realRead = w.mesh.read.bind(w.mesh);
    vi.spyOn(w.mesh, "read").mockImplementation(input => input?.after === events[0]!.sequence - 1
      ? [events[1]!] : realRead(input)); // a retention gap must not substitute the next event
    await w.actors.setActivationFilter(actor.id, skipChecks);
    expect(w.actors.status(actor.id).queued).toBe(1);
    await w.reopen(); // a labelled summary must not become a fabricated full event on restore
    await waitFor(() => w.runs.length === 2);
    await w.idle(actor.id, 3);
    expect(ids(w.runs[2]!.task)).toEqual([events[0]!.id]);
    expect(frame(w.runs[2]!.task).payload).toMatchObject({ id: events[0]!.id, dataOmitted: true });
    expect(frame(w.runs[2]!.task).payload.data).toBeUndefined();
  });
});
