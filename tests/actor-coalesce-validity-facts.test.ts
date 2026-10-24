import { AsyncLocalStorage } from "node:async_hooks";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, onTestFinished, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import type { CoalescedMeshEvent } from "../src/actors/coalesced-events.js";
import type { FabricActorActivation, FabricActorRequest } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { ExecutionDeadline } from "../src/runtime/execution-deadline.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

// Keep reason: #4605 P2-A/P3-1/P3-2, data safety and the attribution boundary.
// Drive the real monitor receiver, durable queue, sandbox predicate and run boundary.
const TOPIC = "github.validity-facts.pulls";
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const from: MeshIdentity = { id: "agent:forwarder", name: "forwarder", kind: "agent" };
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => { for (const close of cleanup.splice(0)) await close(); vi.restoreAllMocks(); });
const waitFor = async (predicate: () => boolean, timeout = 10_000) => {
  const deadline = Date.now() + timeout;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("validity-facts flow timed out");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
interface Frame { payload: MeshEvent & { dataOmitted?: true }; coalesced?: CoalescedMeshEvent[]; untrusted?: true }
const frames = (task: string): Frame[] => task.split("\n\n").flatMap(block => {
  try {
    const value = JSON.parse(block);
    return (Array.isArray(value) ? value : [value]).filter(frame => frame?.source && frame.payload);
  } catch { return []; }
});
const frame = (task: string) => { const value = frames(task)[0]; if (!value) throw new Error("missing run frame"); return value; };
const ids = (task: string) => frames(task).flatMap(value => value.coalesced?.map(record => record.id) ?? [value.payload.id]);
interface SavedItem { source: string; payload: MeshEvent; activation: FabricActorActivation; coalesced?: CoalescedMeshEvent[] }
const usage = { input: 11, output: 7, cacheRead: 3, cacheWrite: 2, cost: 0.001 };
const latest = '({ activation, current }) => activation.sequence === current.latestActivationSequence';
const skipChecks = [{ id: "checks", where: [{ path: "data.event", equals: "check_run" }] }];

const world = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-validity-facts-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const runs: AgentRunRequest[] = [];
  const releases: Array<() => void> = [];
  let flowing = false;
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned) => {
    const n = runs.push(request);
    const id = `validity-facts-${n}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    if (!flowing) await new Promise<void>((resolve, reject) => {
      const abort = () => reject(new Error("validity-facts run aborted"));
      releases[n - 1] = () => { signal?.removeEventListener("abort", abort); resolve(); };
      signal?.addEventListener("abort", abort, { once: true });
      if (signal?.aborted) abort();
    });
    return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true,
      text: `answer-${n}`, value: { action: "message", message: `answer-${n}` }, usage } satisfies AgentRunResult;
  });
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  let actors!: ActorManager;
  const open = () => (actors = new ActorManager("test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, closeGraceMs: 100,
      meshRetentionSweepPath: false, canConsumeMesh: () => false,
    }));
  open();
  const flow = () => { flowing = true; for (const release of releases) release?.(); };
  cleanup.push(async () => { flow(); await actors.close(); await agents.close(); fs.rmSync(root, { recursive: true, force: true }); });
  const queueFiles = (id: string) => fs.readdirSync(path.join(root, "actors", id))
    .filter(name => /^queue-.*\.json$/.test(name)).map(name => path.join(root, "actors", id, name));
  const saved = (id: string): SavedItem[] => queueFiles(id).flatMap(file => JSON.parse(fs.readFileSync(file, "utf8")).items);
  return {
    mesh, agents, runs, saved, get actors() { return actors; },
    create: (source = latest, extra: Partial<FabricActorRequest> = {}) => actors.create({ name: "reviewer", instructions: "Review.", topics: [TOPIC],
      coalesce: false, coalesceKey: "payload.number", responseMode: "text", delivery: "mailbox", triggerTurn: false,
      validWhile: { version: 1, source }, ...extra }),
    publish: (event: string, number = 7, truncated = false) => mesh.publish({ topic: TOPIC, kind: "github.webhook",
      from: truncated ? { ...from, name: "forwarder-".repeat(40) } : from,
      ...(truncated ? { principal: { id: "original-principal", binding: "voice-call" as const } } : {}),
      data: { event, payload: { action: "created", number, body: `original-${event}` } } }),
    admit: (...events: MeshEvent[]) => { for (const event of events) expect(monitor.callbacks.onEvent(event)).toBe(true); },
    block: async (id: string) => { actors.tell(id, "blocker"); await waitFor(() => runs.length === 1); },
    release: (index: number) => releases[index]?.(),
    finish: async (id: string, count: number) => { flow(); await waitFor(() => actors.status(id).status === "idle" && runs.length >= count); expect(runs).toHaveLength(count); },
    reopen: async (id: string, mutate?: (item: SavedItem) => void) => {
      await actors.close();
      if (mutate) for (const file of queueFiles(id)) {
        const snapshot = JSON.parse(fs.readFileSync(file, "utf8"));
        for (const item of snapshot.items as SavedItem[]) if (item.source.startsWith("mesh:")) mutate(item);
        fs.writeFileSync(file, JSON.stringify(snapshot));
      }
      open();
      await waitFor(() => runs.length >= 2); // restored direct blocker still owns the run slot
    },
  };
};
const staleIds = (actors: ActorManager, id: string) => actors.messages(id).filter(message => message.stale)
  .flatMap(message => (message.data as { invalidatedEventId?: string } | undefined)?.invalidatedEventId ?? []);

// Keep reason: #4605 P3-A/data safety. Use the reviewed record-decisions
// predicate clock: real QuickJS execution and deadline comparison, with only
// the deadline getter's clock frozen in each async-local evaluation.
const withPredicateClock = () => {
  const clock = new AsyncLocalStorage<{ elapsedMs: number }>();
  const realExecute = QuickJsRuntime.prototype.execute;
  const realReached = Object.getOwnPropertyDescriptor(ExecutionDeadline.prototype, "reached")!.get!;
  const reached = vi.spyOn(ExecutionDeadline.prototype, "reached", "get").mockImplementation(function (this: ExecutionDeadline) {
    const active = clock.getStore();
    if (!active) return realReached.call(this) as boolean;
    // Preserve an existing Date.now spy (the merged-createdAt case); nesting
    // spyOn/mockRestore here would restore that spy out from under its owner.
    const now = Date.now;
    Date.now = () => this.startedAt + active.elapsedMs;
    try { return realReached.call(this) as boolean; }
    finally { Date.now = now; }
  });
  const execute = vi.spyOn(QuickJsRuntime.prototype, "execute").mockImplementation(function (this: QuickJsRuntime, code, hostCall, options) {
    return clock.run({ elapsedMs: 0 }, () => realExecute.call(this, code, hostCall, options));
  });
  return () => { execute.mockRestore(); reached.mockRestore(); clock.disable(); };
};

describe("merged validity facts retain each original's own data (#4605 round 3)", () => {
  // Install after Vitest's restoreMocks; finish cleanup also runs on thrown cases.
  beforeEach(() => { const restore = withPredicateClock(); onTestFinished(restore); });

  it("P2-A: comment then check_run on PR7 with activation.sequence === current.latestActivationSequence retains BOTH", async () => {
    const w = world();
    const actor = await w.create();
    await w.block(actor.id);
    for (let n = 0; n < 6; n++) await w.mesh.publish({ topic: "unrelated", kind: "padding", from });
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    expect(comment.sequence).not.toBe(2); // actor acceptance is not a mesh sequence
    w.admit(comment, check);
    await w.finish(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id, check.id]);
    expect(frame(w.runs[1]!.task).payload).toEqual(check);
    expect(frame(w.runs[1]!.task).coalesced?.map(record => record.payload?.data)).toEqual([comment.data, check.data]);
    expect(staleIds(w.actors, actor.id)).toEqual([]);
  });

  it("uses merged createdAt/sequence with each original topic, event and payload, not the latest data", async () => {
    const w = world();
    const acceptedAt = Date.now();
    const actor = await w.create(`({ activation, current }) => activation.kind !== "mesh" ||
      activation.sequence === current.latestActivationSequence && activation.createdAt === ${acceptedAt} &&
      activation.topic === ${JSON.stringify(TOPIC)} && activation.data.payload.number === 7 &&
      activation.data.payload.body === "original-" + activation.data.event`);
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    const clock = vi.spyOn(Date, "now").mockReturnValue(acceptedAt - 500);
    w.admit(comment);
    clock.mockReturnValue(acceptedAt);
    w.admit(check);
    clock.mockRestore();
    await w.finish(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id, check.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([]);
  });

  it.each(["issue_comment", "check_run"])("counterexample: data-based rejection of %s still removes only that original", async rejected => {
    const w = world();
    const actor = await w.create(`({ activation, current }) => activation.kind !== "mesh" ||
      activation.sequence === current.latestActivationSequence && activation.data.event !== ${JSON.stringify(rejected)}`);
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.finish(actor.id, 2);
    const survivor = rejected === "issue_comment" ? check : comment;
    const dropped = rejected === "issue_comment" ? comment : check;
    expect(ids(w.runs[1]!.task)).toEqual([survivor.id]);
    expect(frame(w.runs[1]!.task).payload).toEqual(survivor);
    expect(staleIds(w.actors, actor.id)).toEqual([dropped.id]);
  });

  it("filter promotion and restart preserve merged activation sequence/timing for subsequent validity rechecks", async () => {
    const w = world();
    const actor = await w.create('({ activation, current }) => activation.kind !== "mesh" || activation.sequence === current.latestActivationSequence');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    const merged = structuredClone(w.saved(actor.id).find(item => item.source.startsWith("mesh:"))!.activation);
    expect(merged.sequence).toBe(3);
    await w.actors.setActivationFilter(actor.id, skipChecks);
    expect(w.saved(actor.id).find(item => item.source.startsWith("mesh:"))!.activation).toMatchObject({ sequence: merged.sequence, createdAt: merged.createdAt });
    await w.reopen(actor.id);
    expect(w.saved(actor.id).find(item => item.source.startsWith("mesh:"))!.activation).toMatchObject({ sequence: merged.sequence, createdAt: merged.createdAt });
    await w.finish(actor.id, 3);
    expect(ids(w.runs[2]!.task)).toEqual([comment.id]);
    expect(frame(w.runs[2]!.task).payload).toEqual(comment);
  });

  it("P3-1: a truncated nonprimary is judged on merged facts and verified read-back instead of silently surviving", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || activation.data.event === "check_run"');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment", 7, true);
    const check = await w.publish("check_run");
    w.admit(comment, check);
    const stored = w.saved(actor.id).find(item => item.source.startsWith("mesh:"))!.coalesced!.find(record => record.id === comment.id)!;
    expect(stored).toMatchObject({ metadataTruncated: true, dataOmitted: true, activation: { sequence: 2 } });
    expect(stored.sender).toBeUndefined();
    expect(stored.principal).toBeUndefined();
    const read = vi.spyOn(w.mesh, "read");
    await w.finish(actor.id, 2);
    expect(read.mock.calls.some(([input]) => input?.after === comment.sequence - 1 && input.topic === TOPIC && input.limit === 1)).toBe(true);
    expect(ids(w.runs[1]!.task)).toEqual([check.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id]);
  });

  it("read-back recovers truncated data, never the truncated record's principal or sender authority", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || activation.data.event === "issue_comment"');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment", 7, true);
    expect(comment.principal).toEqual({ id: "original-principal", binding: "voice-call" });
    expect(comment.sender).toBeDefined();
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.finish(actor.id, 2);
    const run = w.runs[1]!;
    expect(frame(run.task).payload).toEqual(comment);
    expect(ids(run.task)).toEqual([comment.id]);
    expect(frame(run.task).untrusted).toBe(true);
    expect(frame(run.task).coalesced?.[0]?.sender).toBeUndefined();
    expect(frame(run.task).coalesced?.[0]?.principal).toBeUndefined();
    expect(run.provenance?.principal).toBeUndefined();
  });

  it.each(["unavailable", "id", "sequence", "topic", "kind"])("counterexample: %s read-back cannot decide an unknown truncated original", async mismatch => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || false');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment", 7, true);
    const check = await w.publish("check_run");
    w.admit(comment, check);
    const read = w.mesh.read.bind(w.mesh);
    vi.spyOn(w.mesh, "read").mockImplementation(input => input?.after !== comment.sequence - 1 ? read(input)
      : mismatch === "unavailable" ? [] : [{ ...comment, [mismatch]: mismatch === "sequence" ? comment.sequence + 1 : "wrong" }]);
    await w.finish(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id]);
    expect(frame(w.runs[1]!.task).payload).toMatchObject({ id: comment.id, dataOmitted: true });
    expect(frame(w.runs[1]!.task).payload.data).toBeUndefined();
    expect(staleIds(w.actors, actor.id)).toEqual([check.id]);
  });

  it("unknown-data promotion preserves merged facts through checkpoint so later verified data is rechecked", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || false');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment", 7, true);
    const check = await w.publish("check_run");
    w.admit(comment, check);
    const merged = structuredClone(w.saved(actor.id).find(item => item.source.startsWith("mesh:"))!.activation);
    const realRead = w.mesh.read.bind(w.mesh);
    const read = vi.spyOn(w.mesh, "read").mockImplementation(input => input?.after === comment.sequence - 1 ? [] : realRead(input));
    await w.actors.setActivationFilter(actor.id, skipChecks);
    const promoted = w.saved(actor.id).find(item => item.source.startsWith("mesh:"))!;
    expect(promoted.payload).toMatchObject({ id: comment.id, dataOmitted: true });
    expect(promoted.activation).toMatchObject({ sequence: merged.sequence, createdAt: merged.createdAt });
    expect("data" in promoted.activation).toBe(false);
    await w.reopen(actor.id);
    read.mockRestore();
    await w.finish(actor.id, 2); // only the original and restored blockers; recovered comment is stale
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id]);
  });

  it("legacy missing record.activation is judged when merged facts and original data are known", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || activation.data.event === "check_run"');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.reopen(actor.id, item => { for (const record of item.coalesced ?? []) delete record.activation; });
    await w.finish(actor.id, 3);
    expect(ids(w.runs[2]!.task)).toEqual([check.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id]);
  });

  it("an original without data never borrows the primary's data when projected onto merged facts", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || activation.data?.event === "check_run"');
    await w.block(actor.id);
    const empty = await w.mesh.publish({ topic: TOPIC, kind: "github.webhook", from });
    const check = await w.publish("check_run");
    w.admit(check);
    await w.reopen(actor.id, item => { item.coalesced = [{ id: empty.id, sequence: empty.sequence, topic: empty.topic,
      kind: empty.kind, from: empty.from, payload: empty }]; });
    await w.finish(actor.id, 3);
    expect(ids(w.runs[2]!.task)).toEqual([check.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([empty.id]);
  });

  it("counterexample: genuinely unknown merged facts stay unknown even if historical record facts are available", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || false');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.reopen(actor.id, item => { item.activation.sequence = -1; item.activation.createdAt = -1; });
    await w.finish(actor.id, 3);
    expect(ids(w.runs[2]!.task)).toEqual([comment.id, check.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([]);
  });

  it("P3-2: drain all-fail emits exactly one stale receipt per original, including latest primary", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || false');
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.finish(actor.id, 1);
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id, check.id]);
  });

  it("P3-2: ordinary batch collection all-fail also receipts each original exactly once", async () => {
    const w = world();
    const actor = await w.create('({ activation }) => activation.kind !== "mesh" || activation.data.payload.number === 7');
    await w.block(actor.id);
    const valid = await w.publish("issue_comment");
    const comment = await w.publish("issue_comment", 8);
    const check = await w.publish("check_run", 8);
    w.admit(valid, comment, check);
    await w.actors.setBatching(actor.id, { batchWindowMs: 10, batchMax: 2 });
    await w.finish(actor.id, 2);
    expect(ids(w.runs[1]!.task)).toEqual([valid.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id, check.id]);
  });

  it("P3-2: restored batch collection all-fail receipts each original once without rewriting surviving inputs", async () => {
    const w = world();
    const actor = await w.create('({ activation, current }) => activation.kind !== "mesh" || activation.data.payload.number === 7 || current.latestActivationSequence <= 4');
    await w.block(actor.id);
    const valid = await w.publish("issue_comment");
    const comment = await w.publish("issue_comment", 8);
    const check = await w.publish("check_run", 8);
    w.admit(valid, comment, check);
    await w.actors.setBatching(actor.id, { batchWindowMs: 10, batchMax: 2 });
    w.release(0);
    await waitFor(() => w.runs.length === 2);
    expect(ids(w.runs[1]!.task)).toEqual([valid.id, comment.id, check.id]);
    w.actors.tell(actor.id, "advance current facts before restoring exact batch");
    await w.reopen(actor.id);
    await w.finish(actor.id, 4);
    expect(ids(w.runs[2]!.task)).toEqual([valid.id]);
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id, check.id]);
  });

  it("post-inference all-fail still emits the final stale runId/usage receipt and never delivers the directive", async () => {
    const w = world();
    const actor = await w.create(latest, { responseMode: "directive" });
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    w.release(0);
    await waitFor(() => w.runs.length === 2);
    expect(ids(w.runs[1]!.task)).toEqual([comment.id, check.id]);
    w.actors.tell(actor.id, "invalidate both while inference runs");
    await w.finish(actor.id, 3);
    const final = w.actors.messages(actor.id).filter(message => message.runId === "validity-facts-2");
    expect(final).toHaveLength(1);
    expect(final[0]).toMatchObject({ stale: true, action: "silent", usage });
    expect(final[0]!.text).toBeUndefined();
    expect(final[0]!.principal).toBeUndefined();
    expect(w.actors.messages(actor.id).some(message => message.text === "answer-2")).toBe(false);
    const originals = w.actors.messages(actor.id).filter(message => message.stale && !message.runId)
      .flatMap(message => (message.data as { invalidatedEventId?: string } | undefined)?.invalidatedEventId ?? []);
    expect(originals).toEqual([comment.id, check.id]);
  });

  it.each(['() => { throw new Error("predicate rejection"); }', '() => Promise.resolve(true)'])("sandbox exceptions and async predicates fail closed for every known original: %s", async predicate => {
    const w = world();
    const actor = await w.create(`({ activation }) => activation.kind !== "mesh" || (${predicate})()`);
    await w.block(actor.id);
    const comment = await w.publish("issue_comment");
    const check = await w.publish("check_run");
    w.admit(comment, check);
    await w.finish(actor.id, 1);
    expect(staleIds(w.actors, actor.id)).toEqual([comment.id, check.id]);
  });
});
