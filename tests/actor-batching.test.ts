import fs from "node:fs";
import { createHash } from "node:crypto";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ACTOR_ACTIVATION_HEADER, ENODEV_UNRESOLVED_REASON, peekEnodevRun } from "../src/agents/enodev-recovery.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { slotAdmissionFor } from "../src/actors/slot-admission.js";
import { FabricActorBatchPolicyError, normalizeActorBatching } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import type { ResidencyClient } from "../src/residency/client.js";
import { residentHostId, RESIDENT_COMMANDS } from "../src/residency/protocol.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import * as deliveryOutcomes from "../src/mesh/delivery-outcomes.js";
import * as wakeText from "../src/actors/wake-text.js";
import * as scratch from "../src/storage/scratch.js";

// #11199: recorded-call proof only. No worker, removal, retention or recursive fixture deletion.
// Fixtures stay inside the runner's private TMPDIR. Hosted CI owns real-delete regressions.
const closers: Array<() => Promise<void>> = [];
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const factory: MeshIdentity = { id: "agent:factory", name: "factory", kind: "agent" };
const W = 300_000;
let eventSequence = 0;
const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 };

beforeEach(() => {
  vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(() => {});
  vi.spyOn(ActorChildCompletionStore.prototype, "prune").mockImplementation(() => {});
  vi.spyOn(ActorMeshMonitor.prototype, "schedule").mockImplementation(() => {});
});
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(closers.splice(0).map(close => close()));
  vi.restoreAllMocks();
});

const fixture = async (root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-batching-")), limit = 32, resume = true,
  options: ConstructorParameters<typeof ActorManager>[6] = {}) => {
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  const runs: AgentRunRequest[] = [];
  let hold: Promise<void> | undefined;
  const runStub = vi.spyOn(agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
    runs.push(request);
    const id = `recorded-${runs.length}`;
    onSpawned?.({ id, name: "recorded", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    await hold;
    return { id, name: "recorded", task: request.task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
      startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: "",
      value: { action: "silent" }, usage } satisfies AgentRunResult;
  });
  vi.spyOn(agents, "cleanup").mockResolvedValue({ cleaned: true });
  vi.spyOn(agents, "runDirectory").mockReturnValue(undefined);
  vi.spyOn(agents, "actorArchiveSources").mockReturnValue(new Map());
  vi.spyOn(agents, "outputPrincipal").mockReturnValue(undefined);
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorQueueLimit: limit }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true, releasePaused: true, reapDeadSessionPresence: false,
    // Disable unrelated retention admission; events still use the real synchronous receiver path.
    canConsumeMesh: () => false, ...options,
  });
  const close = async () => { await actors.close(); await agents.close(); };
  closers.push(close);
  actors.resumeQueued();
  if (resume) actors.resumeAfterRelease();
  await new Promise<void>(resolve => setImmediate(resolve));
  const event = (data: unknown, kind = "work", from = factory): MeshEvent => ({
    id: `event-${++eventSequence}`, sequence: eventSequence, topic: "fleet.work.batch", kind, from, data, verification: "mesh",
    sender: { authority: "host" }, createdAt: Date.now(),
  });
  const sendPublished = async (data: unknown) => {
    const next = await mesh.publish({ topic: "fleet.work.batch", kind: "work", from: factory, data });
    expect(monitor.callbacks.onEvent(next)).toBe(true);
    return next;
  };
  const send = (data: unknown, kind = "work", from = factory) => {
    const next = event(data, kind, from);
    expect(monitor.callbacks.onEvent(next)).toBe(true);
    return next;
  };
  const snapshot = (id: string) => {
    const dir = path.join(root, "actors", id);
    const name = fs.readdirSync(dir).find(file => /^queue-.*\.json$/.test(file));
    return name ? JSON.parse(fs.readFileSync(path.join(dir, name), "utf8")) as {
      items: Array<{ id: string; payload: MeshEvent; executionStarted: boolean; resumed?: boolean; activationId?: string; batchIds?: string[] }>;
      processedKeys?: string[];
    } : { items: [] };
  };
  const snapshotFile = (id: string) => {
    const dir = path.join(root, "actors", id);
    return path.join(dir, fs.readdirSync(dir).find(file => /^queue-.*\.json$/.test(file))!);
  };
  return { root, mesh, agents, actors, runs, runStub, send, sendPublished, event, monitor, snapshot, snapshotFile, close, hold: (gate: Promise<void> | undefined) => { hold = gate; } };
};
const create = (actors: ActorManager, options = {}) => actors.create({
  name: "supervisor", instructions: "Observe all events.", topics: ["fleet.work.batch"], events: ["tool_error"],
  responseMode: "directive", batchWindowMs: W, ...options,
});
const fakeClock = () => vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
const settled = async (actors: ActorManager, id: string) => {
  // Native async I/O is not fake time; yield the event loop, never poll production state.
  for (let turn = 0; turn < 100; turn++) {
    if (actors.status(id).queued === 0 && actors.inFlightCount() === 0) return;
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  throw new Error(`did not settle: ${JSON.stringify(actors.status(id))}`);
};
type Input = { source: string; payload: MeshEvent; id: string; untrusted?: boolean };
const inputs = (run: AgentRunRequest): Input[] => {
  const start = run.task.indexOf("\n\n[");
  const end = run.task.lastIndexOf("]");
  return JSON.parse(run.task.slice(start + 2, end + 1));
};

describe("actor window batching on the durable receiver path", () => {
  it("strict release drain holds an expired window until resume without changing its original IDs", async () => {
    const f = await fixture();
    const actor = await create(f.actors);
    fakeClock();
    f.send({ ref: "before-drain-1" });
    f.send({ ref: "before-drain-2" });
    const ids = f.snapshot(actor.id).items.map(item => item.id);
    f.actors.pauseForRelease({ strict: true });
    await vi.advanceTimersByTimeAsync(W);
    expect(f.runs).toHaveLength(0);
    expect(f.snapshot(actor.id).items.map(item => item.id)).toEqual(ids);
    f.actors.resumeAfterRelease();
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(item => item.id)).toEqual(ids);
  });

  it.each([{ window: W, count: 1 }, { window: W, count: 2 }, { window: 0, count: 2 }])(
    "unreadable slot authority retains all $count originals (window=$window) and resumes their identities", async ({ window, count }) => {
    const f = await fixture();
    let readable = true;
    vi.spyOn(slotAdmissionFor(f.mesh), "requiresEnforcement").mockImplementation(() => {
      if (!readable) throw new Error("slot authority is temporarily unreadable");
      return false;
    });
    const actor = await create(f.actors, { batchWindowMs: window, batchMax: count });
    f.runStub.mockImplementationOnce(async request => {
      f.runs.push(request);
      readable = false;
      throw new Error("recorded unlaunched slot refusal");
    });
    const accepted: string[] = [];
    for (let index = 0; index < count; index++) {
      f.send({ ref: `slot-held-${index}` });
      accepted.push(f.actors.messages(actor.id).filter(message => message.direction === "in").at(-1)!.id);
    }
    for (let turn = 0; turn < 100 && (f.runs.length === 0 || f.actors.inFlightCount() > 0); turn++) {
      await new Promise<void>(resolve => setImmediate(resolve));
    }
    expect(f.runs).toHaveLength(1);
    expect(f.actors.inFlightCount()).toBe(0);
    expect(f.snapshot(actor.id).items.map(item => item.id).sort()).toEqual([...accepted].sort());
    const activationId = f.runs[0]!.activationId;
    readable = true;
    f.actors.resumeAfterRelease();
    await new Promise<void>(resolve => setImmediate(resolve));
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(window > 0 ? 2 : count + 1);
    if (window > 0) {
      expect(f.runs[1]!.activationId).toBe(activationId);
      expect(inputs(f.runs[1]!).map(item => item.id)).toEqual(accepted);
    } else {
      // Ordinary failed singleton preparation retains its existing queue rotation.
      expect(f.runs.slice(1).map(run => JSON.parse(run.task.slice(run.task.indexOf("\n\n") + 2)).id).sort())
        .toEqual([...accepted].sort());
    }
  });

  it("5-minute window: ten original inputs in FIFO order, one activation and no sliding timer", async () => {
    const f = await fixture();
    const actor = await create(f.actors);
    fakeClock();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const events = [await f.sendPublished({ ref: "one", marker: 0 })];
    await vi.advanceTimersByTimeAsync(W / 2);
    for (let n = 1; n < 10; n++) events.push(await f.sendPublished({ ref: `ref-${n}`, marker: n }));
    expect(f.actors.status(actor.id).queued).toBe(10);
    expect(f.snapshot(actor.id).items.map(item => item.payload.id)).toEqual(events.map(event => event.id));
    expect(f.runs).toHaveLength(0);
    const windows = scheduled.mock.calls.filter(([, delay]) => delay === W);
    expect(windows).toHaveLength(1);
    const windowResult = scheduled.mock.results[scheduled.mock.calls.findIndex(([, delay]) => delay === W)]!.value as NodeJS.Timeout;
    expect(windowResult.hasRef()).toBe(false);
    await vi.advanceTimersByTimeAsync(W / 2 - 1);
    expect(f.runs).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(input => input.payload)).toEqual(events);
    expect(new Set(inputs(f.runs[0]!).map(input => input.id)).size).toBe(10);
  });

  it("batchMax flushes across queue/overflow and the remaining event opens its own window", async () => {
    const f = await fixture(undefined, 2);
    const actor = await create(f.actors, { batchMax: 4 });
    fakeClock();
    for (let n = 0; n < 3; n++) f.send({ n });
    expect(f.actors.status(actor.id).queued).toBe(3);
    expect(f.runs).toHaveLength(0);
    f.send({ n: 3 });
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(input => input.payload.data)).toEqual([0, 1, 2, 3].map(n => ({ n })));
    f.send({ n: 4 });
    expect(f.actors.status(actor.id).queued).toBe(1);
    expect(f.runs).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(inputs(f.runs[1]!)).toHaveLength(1);
  });

  it.each(["steer", "ask", "main"])("%s flushes older observations first, then runs urgent input separately", async bypass => {
    const f = await fixture();
    const actor = await create(f.actors);
    fakeClock();
    f.send({ ref: "ordinary-1" });
    f.send({ ref: "ordinary-2" });
    const urgent = f.send({ ref: "urgent" }, bypass === "main" ? "message" : bypass, bypass === "main" ? identity : factory);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(2);
    expect(inputs(f.runs[0]!).map(input => input.payload.data)).toEqual([{ ref: "ordinary-1" }, { ref: "ordinary-2" }]);
    expect(f.runs[1]!.task).toContain(JSON.stringify(urgent.data, null, 2).slice(1, -1).trim());
    expect(f.runs[1]!.task).not.toContain("Pending events in FIFO order");
    await vi.advanceTimersByTimeAsync(W);
    expect(f.runs).toHaveLength(2);
  });

  it("direct ask flushes without batching or losing the caller's binding/promise", async () => {
    const f = await fixture();
    const actor = await create(f.actors);
    fakeClock();
    f.send({ ref: "prior" });
    const answer = f.actors.ask(actor.id, "direct question", undefined, undefined, { binding: { model: "provider/direct" } });
    await settled(f.actors, actor.id);
    expect(await answer).toMatchObject({ action: "silent" });
    expect(f.runs).toHaveLength(2);
    expect(inputs(f.runs[0]!)).toHaveLength(1);
    expect(f.runs[1]!.task).toContain("direct question");
    expect(f.runs[1]!.model).toBe("provider/direct");
  });

  it("filters each arrival and prunes accepted work without arming a window for a skip", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { activationFilter: [{ id: "noise", kind: ["skip"] }] });
    fakeClock();
    f.send({ ref: "skipped" }, "skip");
    expect(f.actors.status(actor.id)).toMatchObject({ queued: 0, filterSkipped: { count: 1 } });
    f.send({ ref: "kept" });
    f.send({ ref: "pruned" });
    await f.actors.setActivationFilter(actor.id, [{ id: "prune", where: [{ path: "data.ref", equals: "pruned" }] }]);
    expect(f.actors.status(actor.id)).toMatchObject({ queued: 1, filterSkipped: { count: 1 } });
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(input => input.payload.data)).toEqual([{ ref: "kept" }]);
    expect(f.runs[0]!.task).not.toContain("skipped");
    expect(f.runs[0]!.task).not.toContain("pruned");
  });

  it("restart mid-window preserves original IDs and remaining deadline, with one delivery each", async () => {
    const first = await fixture();
    const actor = await create(first.actors);
    fakeClock();
    first.send({ ref: "before-restart-1" });
    first.send({ ref: "before-restart-2" });
    const saved = first.snapshot(actor.id).items;
    await vi.advanceTimersByTimeAsync(W / 2);
    await first.close();
    const second = await fixture(first.root);
    expect(second.actors.status(actor.id)).toMatchObject({ batchWindowMs: W, queued: 2 });
    await vi.advanceTimersByTimeAsync(W / 2 - 1);
    expect(second.runs).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(1);
    await settled(second.actors, actor.id);
    expect(second.runs).toHaveLength(1);
    expect(inputs(second.runs[0]!).map(input => input.id)).toEqual(saved.map(item => item.id));
    await vi.advanceTimersByTimeAsync(W);
    expect(second.runs).toHaveLength(1);
  });

  it("active snapshots and completed keys retain per-event identity, never a batch queue unit", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 3, residency: "durable", dedupeKey: "data.key" });
    let release!: () => void;
    f.hold(new Promise<void>(resolve => { release = resolve; }));
    fakeClock();
    for (let n = 0; n < 3; n++) f.send({ key: `key-${n}` });
    for (let turn = 0; turn < 100 && !f.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(f.runs).toHaveLength(1);
    const active = f.snapshot(actor.id).items;
    expect(active).toHaveLength(3);
    expect(active.every(item => item.executionStarted && item.resumed)).toBe(true);
    expect(active.map(item => item.payload.data)).toEqual([0, 1, 2].map(n => ({ key: `key-${n}` })));
    release();
    await settled(f.actors, actor.id);
    expect(f.snapshot(actor.id).processedKeys).toHaveLength(3);
    for (let n = 0; n < 3; n++) f.send({ key: `key-${n}` });
    expect(f.actors.status(actor.id).queued).toBe(0);
    expect(f.runs).toHaveLength(1);
  });

  it("mixed-trust batches mark each input and cannot borrow the first event's output principal", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 2 });
    fakeClock();
    const trusted = { ...f.event({ ref: "trusted" }), principal: { id: "paul", binding: "voice-call" as const } };
    const foreign = { ...f.event({ ref: "foreign" }), sender: { authority: "scope" as const, principalId: "other", digest: "other" } };
    expect(f.monitor.callbacks.onEvent(trusted)).toBe(true);
    expect(f.monitor.callbacks.onEvent(foreign)).toBe(true);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!)[0]).not.toHaveProperty("untrusted");
    expect(inputs(f.runs[0]!)[1]).toMatchObject({ untrusted: true });
    expect(f.runs[0]!.provenance).not.toHaveProperty("principal");
    expect(f.runs[0]!.task).toContain("UNTRUSTED");
  });

  it("a batch settles a separate addressed delivery receipt for every original event", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 3 });
    fakeClock();
    const events = [0, 1, 2].map(n => ({ ...f.event({ ref: `receipt-${n}` }), to: actor.id }));
    for (const event of events) expect(f.monitor.callbacks.onEvent(event)).toBe(true);
    await settled(f.actors, actor.id);
    const dir = path.join(f.mesh.root, "delivery-outcomes");
    const receipts = fs.readdirSync(dir).flatMap(file => fs.readFileSync(path.join(dir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.map(receipt => receipt.eventId)).toEqual(events.map(event => event.id));
    expect(receipts.every(receipt => receipt.outcome === "delivered" && receipt.to === actor.id)).toBe(true);
    expect(f.runs).toHaveLength(1);
  });

  it("queue/overflow and dead letters count original events while a window is pending", async () => {
    const f = await fixture(undefined, 2);
    const actor = await create(f.actors, { batchMax: 100 });
    fakeClock();
    const events = Array.from({ length: 20 }, (_, n) => f.send({ ref: `dead-letter-${n}` }));
    expect(f.actors.status(actor.id).queued).toBe(18); // two queue slots plus sixteen overflow slots.
    expect(f.snapshot(actor.id).items.map(item => item.payload.id)).toEqual(events.slice(0, 18).map(event => event.id));
    const file = path.join(f.root, "actors", actor.id, "dead-letter.jsonl");
    const letters = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(letters.map(letter => letter.event.id)).toEqual(events.slice(18).map(event => event.id));
    expect(letters.every(letter => letter.source === "mesh:fleet.work.batch" && !Array.isArray(letter.event))).toBe(true);
    expect(f.runs).toHaveLength(0); // No local destructive dead-letter drain: hosted CI tests replay cleanup.
  });

  it.each([false, true])("a recovered launched batch cannot absorb new pending input (newer first=%s)", async newerFirst => {
    const first = await fixture();
    const actor = await create(first.actors, { batchMax: 2 });
    let release!: () => void;
    first.hold(new Promise<void>(resolve => { release = resolve; }));
    fakeClock();
    first.send({ ref: "launched-1" });
    first.send({ ref: "launched-2" });
    for (let turn = 0; turn < 100 && !first.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    const file = first.snapshotFile(actor.id);
    const interrupted = fs.readFileSync(file, "utf8");
    const oldIds = first.snapshot(actor.id).items.map(item => item.id);
    expect(first.snapshot(actor.id).items.every(item => JSON.stringify(item.batchIds) === JSON.stringify(oldIds))).toBe(true);
    release();
    await settled(first.actors, actor.id);
    await first.close();
    const snapshot = JSON.parse(interrupted);
    if (newerFirst) {
      // Preparation rotation can place fresh work ahead of a recovered activation.
      const event = first.event({ ref: "new-after-crash" });
      const pending = { ...snapshot.items[0], id: "fresh-ahead-of-recovery", payload: event, executionStarted: false,
        resumed: false, createdAt: Date.now(), activation: { ...snapshot.items[0].activation, id: "fresh-ahead-of-recovery", data: event.data } };
      delete pending.batchIds;
      snapshot.items.unshift(pending);
    }
    fs.writeFileSync(file, JSON.stringify(snapshot)); // Recorded crash snapshot, not a native process kill.
    const second = await fixture(first.root, 32, false);
    if (!newerFirst) second.send({ ref: "new-after-crash" });
    second.actors.resumeAfterRelease();
    for (let turn = 0; turn < 100 && second.actors.inFlightCount() > 0; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    const recovered = second.runs[newerFirst ? 1 : 0]!;
    expect(inputs(recovered).map(input => input.id)).toEqual(oldIds);
    expect(recovered.activationId).toBe(oldIds[0]);
    if (!newerFirst) {
      expect(second.runs).toHaveLength(1);
      expect(second.actors.status(actor.id).queued).toBe(1);
      await vi.advanceTimersByTimeAsync(W);
    }
    await settled(second.actors, actor.id);
    expect(second.runs).toHaveLength(2);
    expect(inputs(second.runs[newerFirst ? 0 : 1]!).map(input => input.payload.data)).toEqual([{ ref: "new-after-crash" }]);
  });

  it("window 0 keeps one activation per event and the existing object input shape", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchWindowMs: 0 });
    f.send({ n: 1 });
    f.send({ n: 2 });
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(2);
    expect(f.runs.every(run => !run.task.includes("Pending events in FIFO order"))).toBe(true);
    expect(f.runs[0]!.task).toContain('"n": 1');
    expect(f.runs[1]!.task).toContain('"n": 2');
  });

  it("window 0 still coalesces repeated host and keyed mesh observations behind a running call", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchWindowMs: 0, coalesce: true, coalesceKey: "ref" });
    let release!: () => void;
    f.hold(new Promise<void>(resolve => { release = resolve; }));
    f.actors.tell(actor.id, "busy");
    for (let turn = 0; turn < 100 && !f.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    f.actors.dispatchHostEvent("tool_error", { marker: "old-host" });
    f.actors.dispatchHostEvent("tool_error", { marker: "latest-host" });
    f.send({ ref: "same", marker: "old-mesh" });
    f.send({ ref: "same", marker: "latest-mesh" });
    expect(f.actors.status(actor.id).queued).toBe(2);
    release();
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(3);
    expect(f.runs[1]!.task).toContain("latest-host");
    expect(f.runs[2]!.task).toContain("latest-mesh");
  });

  it("dead-letter replay recognizes a held batch's non-head original instead of queuing it twice", async () => {
    const f = await fixture(undefined, 2);
    const actor = await create(f.actors, { batchMax: 2 });
    let release!: () => void;
    f.hold(new Promise<void>(resolve => { release = resolve; }));
    const events = [f.send({ ref: "active-first" }), f.send({ ref: "active-second" })];
    for (let turn = 0; turn < 100 && !f.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(f.runs).toHaveLength(1);
    for (let marker = 0; marker < 20; marker++) f.send({ ref: "discard-pending", marker });
    const file = path.join(f.root, "actors", actor.id, "dead-letter.jsonl");
    expect(fs.existsSync(file)).toBe(true);
    // A crash can leave a disk copy of an already-held non-head original.
    fs.writeFileSync(file, `${JSON.stringify({ at: Date.now(), source: "mesh:fleet.work.batch", event: events[1], batchQueued: true })}\n`);
    const remove = fs.rmSync;
    let removals = 0;
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) !== file) return remove(target, options);
      fs.renameSync(file, `${file}.recorded-${++removals}`);
    });
    await f.actors.setActivationFilter(actor.id, [{ id: "discard", where: [{ path: "data.ref", equals: "discard-pending" }] }]);
    try {
      expect(f.actors.status(actor.id).queued).toBe(0);
      expect(f.snapshot(actor.id).items.map(item => item.payload.id)).toEqual(events.map(event => event.id));
      expect(removals).toBe(1);
    } finally { release(); }
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(item => item.payload.id)).toEqual(events.map(event => event.id));
  });

  it("positive-window dead letters retain every addressed original after disabling batching and restarting", async () => {
    const f = await fixture(undefined, 2);
    const actor = await create(f.actors, { batchMax: 100, coalesceKey: "ref" });
    fakeClock();
    const events = Array.from({ length: 20 }, (_, marker) => ({ ...f.event({ ref: "same", marker }), to: actor.id }));
    for (const event of events) expect(f.monitor.callbacks.onEvent(event)).toBe(true);
    const file = path.join(f.root, "actors", actor.id, "dead-letter.jsonl");
    const letters = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(letters.map(letter => letter.event.id)).toEqual(events.slice(18).map(event => event.id));
    expect(letters.every(letter => letter.batchQueued === true)).toBe(true);
    // Recorded removal only: preserve the backlog bytes under a private evidence name.
    const remove = fs.rmSync;
    const removals: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) !== file) return remove(target, options);
      removals.push(String(target));
      fs.renameSync(file, `${file}.recorded-${removals.length}`);
    });
    f.actors.pauseForRelease();
    await f.actors.setBatching(actor.id, { batchWindowMs: 0 });
    await f.close();
    const again = await fixture(f.root, 2);
    await settled(again.actors, actor.id);
    expect(again.runs).toHaveLength(events.length);
    const delivered = again.runs.map(run => JSON.parse(run.task.slice(run.task.indexOf("\n\n") + 2)).payload.id);
    expect(delivered).toEqual(events.map(event => event.id));
    expect(removals).toEqual([file]);
    const dir = path.join(again.mesh.root, "delivery-outcomes");
    const outcomes = fs.readdirSync(dir).flatMap(name => fs.readFileSync(path.join(dir, name), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(outcomes.filter(outcome => events.some(event => event.id === outcome.eventId)).map(outcome => [outcome.eventId, outcome.outcome]))
      .toEqual(events.map(event => [event.id, "delivered"]));
  });

  it("window 0 still writes only the newest keyed dead letter and replays its superseded receipts", async () => {
    const f = await fixture(undefined, 1, false);
    const actor = await create(f.actors, { batchWindowMs: 0, coalesceKey: "ref" });
    for (let marker = 0; marker < 9; marker++) f.send({ ref: `filler-${marker}` });
    const events = [0, 1, 2].map(marker => ({ ...f.event({ ref: "same", marker }), to: actor.id }));
    for (const event of events) expect(f.monitor.callbacks.onEvent(event)).toBe(true);
    const file = path.join(f.root, "actors", actor.id, "dead-letter.jsonl");
    const letters = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(letters).toHaveLength(1);
    expect(letters[0].event.id).toBe(events[2]!.id);
    expect(letters[0].batchQueued).toBeUndefined();
    const remove = fs.rmSync;
    const removals: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) !== file) return remove(target, options);
      removals.push(String(target));
      fs.renameSync(file, `${file}.recorded-${removals.length}`);
    });
    await f.close();
    const again = await fixture(f.root, 1);
    await settled(again.actors, actor.id);
    const subjects = again.runs.filter(run => run.task.includes('"ref": "same"'));
    expect(subjects).toHaveLength(1);
    expect(subjects[0]!.task).toContain('"marker": 2');
    expect(removals).toEqual([file]);
    const dir = path.join(again.mesh.root, "delivery-outcomes");
    const outcomes = fs.readdirSync(dir).flatMap(name => fs.readFileSync(path.join(dir, name), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(outcomes.filter(outcome => events.some(event => event.id === outcome.eventId)).map(outcome => [outcome.eventId, outcome.outcome]))
      .toEqual(events.map((event, index) => [event.id, index === 2 ? "delivered" : "superseded"]));
  });

  it("positive batching retains repeated observations after disabling the window and restarting", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { coalesce: true, coalesceKey: "ref" });
    fakeClock();
    f.actors.dispatchHostEvent("tool_error", { marker: "first" });
    f.actors.dispatchHostEvent("tool_error", { marker: "second" });
    f.send({ ref: "same", marker: "third" });
    f.send({ ref: "same", marker: "fourth" });
    expect(f.actors.status(actor.id).queued).toBe(4);
    f.actors.pauseForRelease();
    await f.actors.setBatching(actor.id, { batchWindowMs: 0 });
    await f.close();
    const again = await fixture(f.root);
    await settled(again.actors, actor.id);
    expect(again.runs).toHaveLength(4);
    expect(again.runs.map(run => run.task.match(/"marker": "([^\"]+)"/)?.[1])).toEqual(["first", "second", "third", "fourth"]);
  });
});

describe("round 2 batch failure and recovery interleavings", () => {
  it("default aggregate cap flushes before a third large event without dropping its neighbor", async () => {
    const f = await fixture();
    const actor = await create(f.actors);
    fakeClock();
    const events = [0, 1, 2].map(n => f.send({ ref: `large-${n}`, padding: "x".repeat(100 * 1024) }));
    for (let turn = 0; turn < 100 && (!f.runs.length || f.actors.inFlightCount()); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(input => input.payload.id)).toEqual(events.slice(0, 2).map(event => event.id));
    expect(Buffer.byteLength(f.runs[0]!.task, "utf8")).toBeLessThan(256 * 1024);
    expect(f.actors.status(actor.id).queued).toBe(1);
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(inputs(f.runs[1]!).map(input => input.payload.id)).toEqual([events[2]!.id]);
  });

  it("configured exact aggregate boundary is inclusive and survives live update/restart", async () => {
    const f = await fixture();
    const actor = await create(f.actors);
    fakeClock();
    const events = [0, 1, 2].map(n => f.send({ ref: `boundary-${n}` }));
    const pending = f.snapshot(actor.id).items;
    const pair = pending.slice(0, 2);
    const cap = Buffer.byteLength([
      ACTOR_ACTIVATION_HEADER + JSON.stringify({ activationId: pair[0]!.id, batchIds: pair.map(item => item.id) }),
      "Fabric actor message from mesh:fleet.work.batch:",
      "Pending events in FIFO order. Process every input once; each retains its own event identity:",
      JSON.stringify(pair.map(item => ({ source: "mesh:fleet.work.batch", payload: item.payload, id: item.id })), null, 2),
    ].join("\n\n"), "utf8");
    f.actors.pauseForRelease();
    await f.actors.setBatching(actor.id, { batchMaxBytes: cap });
    await f.close();
    const second = await fixture(f.root);
    expect(second.actors.status(actor.id)).toMatchObject({ batchMaxBytes: cap });
    for (let turn = 0; turn < 100 && (!second.runs.length || second.actors.inFlightCount()); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(inputs(second.runs[0]!).map(input => input.payload.id)).toEqual(events.slice(0, 2).map(event => event.id));
    expect(Buffer.byteLength(second.runs[0]!.task, "utf8")).toBe(cap);
    expect(second.actors.status(actor.id).queued).toBe(1);
    await vi.advanceTimersByTimeAsync(W);
    await settled(second.actors, actor.id);
    expect(inputs(second.runs[1]!).map(input => input.payload.id)).toEqual([events[2]!.id]);
  });

  it("counts the exact activation header and every wake-text label at a near-cap boundary", async () => {
    const hydrated = (sequence: number): wakeText.ActorWakeText => ({ source: "ingress-receipt", event: "issue_comment", repository: "acme/demo",
      sequence, body: "😀".repeat(200), truncated: false });
    vi.spyOn(wakeText, "hydrateWakeText").mockImplementation((_policy, _source, payload) => hydrated((payload as MeshEvent).sequence));
    const f = await fixture();
    const actor = await create(f.actors, { activationFilter: [{ id: "unused", kind: ["skip"] }] });
    fakeClock();
    const events = [0, 1, 2].map(n => f.send({ ref: `wake-${n}` }));
    const pair = f.snapshot(actor.id).items.slice(0, 2);
    const cap = Buffer.byteLength([
      ACTOR_ACTIVATION_HEADER + JSON.stringify({ activationId: pair[0]!.id, batchIds: pair.map(item => item.id) }),
      "Fabric actor message from mesh:fleet.work.batch:",
      "Pending events in FIFO order. Process every input once; each retains its own event identity:",
      JSON.stringify(pair.map(item => ({ source: "mesh:fleet.work.batch", payload: item.payload, id: item.id })), null, 2),
      ...pair.flatMap(item => [`Event ${item.id}:`, wakeText.renderWakeTextBlock(hydrated(item.payload.sequence))]),
    ].join("\n\n"), "utf8");
    await f.actors.setBatching(actor.id, { batchMaxBytes: cap });
    for (let turn = 0; turn < 100 && (!f.runs.length || f.actors.inFlightCount()); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(input => input.payload.id)).toEqual(events.slice(0, 2).map(event => event.id));
    expect(Buffer.byteLength(f.runs[0]!.task, "utf8")).toBe(cap);
    expect(f.runs[0]!.task).toContain(`Event ${pair[0]!.id}:`);
    expect(f.actors.status(actor.id).queued).toBe(1);
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(inputs(f.runs[1]!).map(input => input.payload.id)).toEqual([events[2]!.id]);
    expect(Buffer.byteLength(f.runs[1]!.task, "utf8")).toBeLessThan(cap);
  });

  it("counts image bytes and runs one oversized event alone, preserving both adjacent events", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMaxBytes: 1024 });
    fakeClock();
    const image = { type: "image" as const, mimeType: "image/png", data: "A".repeat(3000) };
    f.actors.dispatchHostEvent("tool_error", { marker: "before" });
    f.actors.dispatchHostEvent("tool_error", { marker: "oversized" }, [image]);
    f.actors.dispatchHostEvent("tool_error", { marker: "after" });
    for (let turn = 0; turn < 100 && (f.runs.length < 2 || f.actors.inFlightCount()); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(f.runs).toHaveLength(2);
    expect(inputs(f.runs[0]!)).toHaveLength(1);
    expect(f.runs[0]!.task).toContain('"marker": "before"');
    expect(inputs(f.runs[1]!)).toHaveLength(1);
    expect(f.runs[1]!.images).toEqual([image]);
    expect(f.actors.status(actor.id).queued).toBe(1);
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(f.runs[2]!.task).toContain('"marker": "after"');
    expect(f.runs[2]!.images).toBeUndefined();
  });

  it("keeps same-key originals pending when their earlier batch fails; success still suppresses repeats", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 3, residency: "durable", dedupeKey: "data.key" });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    f.runStub.mockImplementationOnce(async request => { f.runs.push(request); await gate; throw new Error("recorded unlaunched failure"); });
    fakeClock();
    const events = ["same", "different", "same"].map((key, n) => ({ ...f.event({ key, n }), to: actor.id }));
    for (const event of events) expect(f.monitor.callbacks.onEvent(event)).toBe(true);
    for (let turn = 0; turn < 100 && !f.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(inputs(f.runs[0]!).map(input => input.payload.id)).toEqual(events.slice(0, 2).map(event => event.id));
    expect(f.snapshot(actor.id).items.map(item => item.payload.id)).toEqual(events.map(event => event.id));
    expect(f.actors.status(actor.id).queued).toBe(1);
    release();
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(2);
    expect(inputs(f.runs[1]!).map(input => input.payload.id)).toEqual([events[2]!.id]);
    const dir = path.join(f.mesh.root, "delivery-outcomes");
    const receipts = fs.readdirSync(dir).flatMap(file => fs.readFileSync(path.join(dir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.map(receipt => receipt.eventId)).toEqual(events.map(event => event.id));
    expect(receipts.map(receipt => receipt.outcome)).toEqual(["failed", "failed", "delivered"]);
    f.send({ key: "same", n: 4 });
    expect(f.actors.status(actor.id).queued).toBe(0);
    expect(f.runs).toHaveLength(2);
  });

  it("W=0 also settles a terminal failed original without losing its following event", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchWindowMs: 0 });
    f.runStub.mockRejectedValueOnce(new Error("recorded unlaunched failure"));
    const events = [0, 1].map(n => ({ ...f.event({ ref: `single-${n}` }), to: actor.id }));
    for (const event of events) expect(f.monitor.callbacks.onEvent(event)).toBe(true);
    await settled(f.actors, actor.id);
    expect(f.runStub).toHaveBeenCalledTimes(2);
    expect(f.runs).toHaveLength(1);
    expect(f.runs[0]!.task).toContain('"ref": "single-1"');
    expect(f.runs[0]!.task).not.toContain("Pending events in FIFO order");
    const dir = path.join(f.mesh.root, "delivery-outcomes");
    const receipts = fs.readdirSync(dir).flatMap(file => fs.readFileSync(path.join(dir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.map(receipt => receipt.eventId)).toEqual(events.map(event => event.id));
    expect(receipts.map(receipt => receipt.outcome)).toEqual(["failed", "delivered"]);
  });

  it.each([1, 2])("crash after durable FAILED and before in-flight cleanup never delivers the same send (members=%s)", async count => {
    const first = await fixture();
    const actor = await create(first.actors, { batchWindowMs: count === 1 ? 0 : W, batchMax: count });
    first.runStub.mockRejectedValueOnce(new Error("recorded terminal refusal"));
    let interrupted = "";
    let queueFile = "";
    const append = deliveryOutcomes.appendDeliveryOutcome;
    const receipt = vi.spyOn(deliveryOutcomes, "appendDeliveryOutcome").mockImplementation((...args) => {
      append(...args); // FAILED is durably appended, but finishInFlight has not cleared the original yet.
      if (args[2] === "failed") {
        queueFile = first.snapshotFile(actor.id);
        interrupted = fs.readFileSync(queueFile, "utf8");
      }
    });
    fakeClock();
    const events = Array.from({ length: count }, (_, n) => ({ ...first.event({ ref: `terminal-${n}` }), to: actor.id }));
    for (const event of events) expect(first.monitor.callbacks.onEvent(event)).toBe(true);
    await settled(first.actors, actor.id);
    expect(interrupted).not.toBe("");
    const saved = JSON.parse(interrupted);
    expect(saved.items).toHaveLength(count);
    expect(saved.items.every((item: { terminalFailed?: boolean }) => item.terminalFailed === true)).toBe(true);
    expect(saved.items.some((item: { outcomeSend?: unknown }) => item.outcomeSend)).toBe(true);
    await first.close();
    receipt.mockRestore();
    fs.writeFileSync(queueFile, interrupted); // Recorded exact crash point; no process or fixture deletion.
    const second = await fixture(first.root);
    expect(second.actors.status(actor.id).queued).toBe(0);
    expect(second.runs).toHaveLength(0);
    for (const event of events) expect(second.monitor.callbacks.onEvent(event)).toBe("ignored"); // Same cursor receipt, not a new original.
    expect(second.actors.status(actor.id).queued).toBe(0);
    await vi.advanceTimersByTimeAsync(W);
    expect(second.runs).toHaveLength(0);
    // A different original remains runnable and flushes the retained failure receipt normally.
    const fresh = { ...second.event({ ref: "new-original" }, "steer"), to: actor.id };
    expect(second.monitor.callbacks.onEvent(fresh)).toBe(true);
    await settled(second.actors, actor.id);
    expect(second.runs).toHaveLength(1);
    const dir = path.join(second.mesh.root, "delivery-outcomes");
    const receipts = fs.readdirSync(dir).flatMap(file => fs.readFileSync(path.join(dir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    for (const event of events) expect(receipts.filter(record => record.eventId === event.id).map(record => record.outcome)).toEqual(["failed"]);
    expect(receipts.filter(record => record.eventId === fresh.id).map(record => record.outcome)).toEqual(["delivered"]);
  });

  it.each([
    { source: "mesh", fence: "own", predecessors: ["unmarked"], retryAppend: false },
    { source: "mesh", fence: "adopted", predecessors: ["unmarked", "terminal"], retryAppend: false },
    { source: "mesh", fence: "adopted", predecessors: ["terminal", "unmarked"], retryAppend: false },
    { source: "mesh", fence: "own", predecessors: ["unmarked"], retryAppend: true },
    { source: "direct", fence: "own", predecessors: ["unmarked"], retryAppend: false },
    { source: "direct", fence: "adopted", predecessors: ["unmarked", "terminal"], retryAppend: false },
    { source: "direct", fence: "adopted", predecessors: ["terminal", "unmarked"], retryAppend: false },
  ])("restores a $fence terminal fence before FAILED append (source=$source, predecessors=$predecessors, retryAppend=$retryAppend)", async ({ source, fence, predecessors, retryAppend }) => {
    // Astra P2, PR #1075 / smarty-dev#11605: no durable receipt exists yet. Index
    // all adopted tombstones before importing an earlier unmarked copy as work.
    const first = await fixture();
    const actor = await create(first.actors, { batchWindowMs: 0 });
    let unmarked = "";
    first.runStub.mockImplementationOnce(async () => {
      unmarked = fs.readFileSync(first.snapshotFile(actor.id), "utf8");
      throw new Error("recorded terminal refusal before inference");
    });
    let interrupted = "";
    let queueFile = "";
    const append = deliveryOutcomes.appendDeliveryOutcome;
    const receipt = vi.spyOn(deliveryOutcomes, "appendDeliveryOutcome").mockImplementation((...args) => {
      if (args[2] === "failed") {
        if (!interrupted) {
          queueFile = first.snapshotFile(actor.id);
          interrupted = fs.readFileSync(queueFile, "utf8");
        }
        throw new Error("recorded crash BEFORE FAILED append");
      }
      append(...args);
    });
    // Routed owner admission carries the send separately: its payload is not a MeshEvent.
    const admit = (event: MeshEvent) => source === "direct"
      ? expect(first.actors.tell(actor.id, "routed original", event.data, { outcomeSend: {
        eventId: event.id, to: actor.id, from: event.from.id, mode: "steer",
      } }).queued).toBe(true)
      : expect(first.monitor.callbacks.onEvent(event)).toBe(true);
    const failed = { ...first.event({ ref: "terminal-predecessor-original" }), to: actor.id };
    admit(failed);
    await settled(first.actors, actor.id);
    expect(interrupted).not.toBe("");
    const crash = JSON.parse(interrupted);
    expect(crash.items).toHaveLength(1);
    expect(crash.items[0]).toMatchObject({ terminalFailed: true, ...(source === "direct"
      ? { source: "direct", payload: { message: "routed original", data: failed.data } }
      : { payload: { id: failed.id } }) });
    const failedSend = deliveryOutcomes.deliverySend(crash.items[0].outcomeSend)!;
    expect(failedSend).toBeDefined();
    expect(crash.pendingDeliveryOutcomes).toMatchObject([{ ...failedSend, outcome: "failed" }]);
    const older = JSON.parse(unmarked);
    expect(older.items).toHaveLength(1);
    expect(older.items[0].terminalFailed).toBeUndefined();
    expect(older.items[0].outcomeSend).toEqual(failedSend);

    // Obtain the counterexample through real admission too: same receiver,
    // sender and mode, but a distinct original/event must remain runnable.
    first.actors.pauseForRelease();
    const unrelated = { ...first.event({ ref: "unrelated-predecessor-original" }), to: actor.id };
    admit(unrelated);
    const unrelatedSnapshot = JSON.parse(fs.readFileSync(first.snapshotFile(actor.id), "utf8"));
    const unrelatedItem = unrelatedSnapshot.items.find((item: { outcomeSend?: deliveryOutcomes.DeliverySend }) => item.outcomeSend?.eventId === unrelated.id);
    expect(unrelatedItem).toBeDefined();
    const unrelatedSend = deliveryOutcomes.deliverySend(unrelatedItem.outcomeSend)!;
    expect(unrelated.id).not.toBe(failed.id);
    expect(unrelatedSend).toEqual({ ...failedSend, eventId: unrelated.id });
    await first.close();
    expect(deliveryOutcomes.hasDeliveryOutcome(first.mesh.root, failedSend, "failed")).toBe(false);
    const receiptDir = path.join(first.mesh.root, "delivery-outcomes");
    const receipts = (): deliveryOutcomes.DeliveryOutcomeRecord[] => fs.existsSync(receiptDir)
      ? fs.readdirSync(receiptDir).flatMap(file => fs.readFileSync(path.join(receiptDir, file), "utf8")
        .trim().split("\n").filter(Boolean).map(line => JSON.parse(line))) : [];
    expect(receipts()).toEqual([]); // The fault never delegates FAILED to the real appender.

    const predecessorFiles = new Map(["unmarked", "terminal"].map(label => {
      const root = `session:retired-before-append-${label}`;
      const key = createHash("sha256").update([root, actor.residency].join("\0")).digest("hex").slice(0, 16);
      return [label, { root, file: path.join(first.root, "actors", actor.id, `queue-${key}.json`) }] as const;
    }));
    // The terminal lineage gets the exact recorded crash bytes, not a snapshot
    // captured after append. The other lineage has no pending failure evidence.
    fs.writeFileSync(queueFile, fence === "own" ? interrupted : JSON.stringify({ ...crash, items: [], pendingDeliveryOutcomes: [] }));
    fs.writeFileSync(predecessorFiles.get("unmarked")!.file, JSON.stringify({
      ...unrelatedSnapshot, items: [...older.items, unrelatedItem], pendingDeliveryOutcomes: [],
    }));
    if (fence === "adopted") fs.writeFileSync(predecessorFiles.get("terminal")!.file, interrupted);
    const registryFile = path.join(first.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8"));
    registry.actors.find((record: { id: string }) => record.id === actor.id).adoptedFrom = predecessors.map(label => predecessorFiles.get(label)!.root);
    fs.writeFileSync(registryFile, JSON.stringify(registry));
    const retained = new Set(predecessors.map(label => predecessorFiles.get(label)!.file));
    const removals: string[] = [];
    const remove = fs.rmSync;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (retained.has(String(file))) {
        removals.push(String(file));
        throw Object.assign(new Error("recorded undeletable predecessor"), { code: "EACCES" });
      }
      return remove(file, options);
    }); // Recorded removal attempts only; no predecessor file is actually deleted.

    if (retryAppend) {
      const blocked = await fixture(first.root, 32, false);
      expect(blocked.runStub).not.toHaveBeenCalled();
      if (source === "mesh") expect(blocked.monitor.callbacks.onEvent(failed)).toBe("ignored");
      await blocked.close();
      expect(receipts()).toEqual([]);
      const pendingOnly = JSON.parse(fs.readFileSync(queueFile, "utf8"));
      expect(pendingOnly.pendingDeliveryOutcomes).toMatchObject([{ ...failedSend, outcome: "failed" }]);
      expect(pendingOnly.items.map((item: { payload: MeshEvent }) => item.payload.id)).toEqual([unrelated.id]);
      expect(pendingOnly.items.some((item: { terminalFailed?: boolean }) => item.terminalFailed)).toBe(false);
    }
    receipt.mockRestore();
    const second = await fixture(first.root, 32, false);
    expect(second.runStub).not.toHaveBeenCalled();
    // Restore alone must settle the obligation, before any fresh event or run
    // can incidentally flush it. Keep worker activation paused for this check.
    // Soft convergence check deliberately reaches the execution assertions on an unfixed manager.
    expect.soft(receipts().filter(record => record.eventId === failed.id)).toEqual([expect.objectContaining({ ...failedSend, outcome: "failed" })]);
    expect(receipts().filter(record => record.eventId === unrelated.id)).toEqual([]);
    if (source === "mesh") expect(second.monitor.callbacks.onEvent(failed)).toBe("ignored");
    second.actors.resumeAfterRelease();
    await settled(second.actors, actor.id);
    expect.soft(second.runStub).toHaveBeenCalledTimes(1);
    expect.soft(second.runs).toHaveLength(1);
    expect.soft(second.runs.some(run => run.task.includes('"ref": "unrelated-predecessor-original"'))).toBe(true);
    expect.soft(second.runs.some(run => run.task.includes('"ref": "terminal-predecessor-original"'))).toBe(false);
    expect.soft(receipts().filter(record => record.eventId === failed.id).map(record => record.outcome)).toEqual(["failed"]);
    expect(receipts().filter(record => record.eventId === unrelated.id)).toEqual([expect.objectContaining({ ...unrelatedSend, outcome: "delivered" })]);
    await second.close();
    for (const file of retained) {
      expect(removals).toContain(file);
      expect(fs.existsSync(file)).toBe(true);
    }

    // Retained, unmarked predecessors remain fenced on another restart. Only
    // mesh has cursor replay: another direct tell would be a new admission.
    // Do not require successful unrelated work to rerun.
    const third = await fixture(first.root);
    await settled(third.actors, actor.id);
    for (const [request] of third.runStub.mock.calls) {
      expect.soft(request.task).toContain('"ref": "unrelated-predecessor-original"');
      expect.soft(request.task).not.toContain('"ref": "terminal-predecessor-original"');
    }
    const calls = third.runStub.mock.calls.length;
    if (source === "mesh") expect(third.monitor.callbacks.onEvent(failed)).toBe("ignored");
    await settled(third.actors, actor.id);
    expect(third.runStub).toHaveBeenCalledTimes(calls);
    expect.soft(receipts().filter(record => record.eventId === failed.id)).toEqual([expect.objectContaining({ ...failedSend, outcome: "failed" })]);
    expect(receipts().filter(record => record.eventId === unrelated.id)).toEqual([expect.objectContaining({ ...unrelatedSend, outcome: "delivered" })]);
    // Retaining a terminal fence for an undeletable predecessor must not hold up
    // a later independent send's receipt after another restart.
    const fresh = { ...third.event({ ref: "fresh-after-terminal-restart" }, "steer"), to: actor.id };
    expect(third.monitor.callbacks.onEvent(fresh)).toBe(true);
    await settled(third.actors, actor.id);
    expect(third.runStub).toHaveBeenCalledTimes(calls + 1);
    expect(receipts().filter(record => record.eventId === fresh.id).map(record => record.outcome)).toEqual(["delivered"]);
  });

  it("an undeletable adopted predecessor cannot resurrect an unmarked FAILED send across restarts", async () => {
    const first = await fixture();
    const actor = await create(first.actors, { batchWindowMs: 0 });
    let older = "";
    first.runStub.mockImplementationOnce(async () => {
      older = fs.readFileSync(first.snapshotFile(actor.id), "utf8");
      throw new Error("recorded terminal refusal");
    });
    const failed = { ...first.event({ ref: "predecessor-original" }), to: actor.id };
    expect(first.monitor.callbacks.onEvent(failed)).toBe(true);
    await settled(first.actors, actor.id);
    const unmarked = JSON.parse(older);
    expect(unmarked.items[0].terminalFailed).toBeUndefined();
    const send = unmarked.items[0].outcomeSend;
    expect(deliveryOutcomes.hasDeliveryOutcome(first.mesh.root, send, "failed")).toBe(true);
    expect(deliveryOutcomes.hasDeliveryOutcome(first.mesh.root, { ...send, from: "another-sender" }, "failed")).toBe(false);
    expect(deliveryOutcomes.hasDeliveryOutcome(first.mesh.root, send, "delivered")).toBe(false);
    await first.close();
    const oldRoot = "session:retired";
    const key = createHash("sha256").update([oldRoot, actor.residency].join("\0")).digest("hex").slice(0, 16);
    const predecessor = path.join(first.root, "actors", actor.id, `queue-${key}.json`);
    fs.writeFileSync(predecessor, older);
    const registryFile = path.join(first.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8"));
    registry.actors.find((record: { id: string }) => record.id === actor.id).adoptedFrom = [oldRoot];
    fs.writeFileSync(registryFile, JSON.stringify(registry));
    const remove = fs.rmSync;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (String(file) === predecessor) throw Object.assign(new Error("recorded undeletable predecessor"), { code: "EACCES" });
      return remove(file, options);
    }); // No real predecessor deletion; normal private lease release is unchanged.
    const second = await fixture(first.root);
    expect(second.actors.status(actor.id).queued).toBe(0);
    expect(second.runs).toHaveLength(0);
    expect(second.monitor.callbacks.onEvent(failed)).toBe("ignored");
    await second.close();
    expect(fs.existsSync(predecessor)).toBe(true);
    const third = await fixture(first.root);
    expect(third.actors.status(actor.id).queued).toBe(0);
    expect(third.runs).toHaveLength(0);
    const fresh = { ...third.event({ ref: "unrelated-new-original" }, "steer"), to: actor.id };
    expect(third.monitor.callbacks.onEvent(fresh)).toBe(true);
    await settled(third.actors, actor.id);
    expect(third.runs).toHaveLength(1);
    const dir = path.join(third.mesh.root, "delivery-outcomes");
    const receipts = fs.readdirSync(dir).flatMap(file => fs.readFileSync(path.join(dir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.filter(record => record.eventId === failed.id).map(record => record.outcome)).toEqual(["failed"]);
    expect(receipts.filter(record => record.eventId === fresh.id).map(record => record.outcome)).toEqual(["delivered"]);
  });

  it("never appends FAILED when its terminal fence cannot checkpoint, then flushes it after durable removal", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchWindowMs: 0 });
    f.runStub.mockRejectedValueOnce(new Error("recorded terminal refusal"));
    const rename = fs.renameSync;
    let refusals = 0;
    const fence = vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (String(destination).includes("queue-") && fs.readFileSync(source, "utf8").includes('"terminalFailed"')) {
        refusals++;
        throw Object.assign(new Error("recorded terminal-fence refusal"), { code: "EIO" });
      }
      return rename(source, destination);
    });
    const failed = { ...f.event({ ref: "fence-refused" }), to: actor.id };
    expect(f.monitor.callbacks.onEvent(failed)).toBe(true);
    await settled(f.actors, actor.id);
    fence.mockRestore();
    expect(refusals).toBeGreaterThan(0);
    const receiptDir = path.join(f.mesh.root, "delivery-outcomes");
    expect(fs.existsSync(receiptDir)).toBe(false);
    const saved = JSON.parse(fs.readFileSync(f.snapshotFile(actor.id), "utf8"));
    expect(saved.items).toHaveLength(0);
    expect(saved.pendingDeliveryOutcomes).toMatchObject([{ eventId: failed.id, outcome: "failed" }]);
    await f.close();
    const second = await fixture(f.root);
    expect(second.runs).toHaveLength(0);
    const fresh = { ...second.event({ ref: "following" }, "steer"), to: actor.id };
    expect(second.monitor.callbacks.onEvent(fresh)).toBe(true);
    await settled(second.actors, actor.id);
    const receipts = fs.readdirSync(receiptDir).flatMap(file => fs.readFileSync(path.join(receiptDir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.filter(record => record.eventId === failed.id).map(record => record.outcome)).toEqual(["failed"]);
    expect(receipts.filter(record => record.eventId === fresh.id).map(record => record.outcome)).toEqual(["delivered"]);
  });

  it("a durable close keeps interrupted originals pending instead of recording terminal FAILED", async () => {
    const first = await fixture(undefined, 32, true, { closeGraceMs: 100 });
    const actor = await create(first.actors, { batchMax: 2 });
    first.runStub.mockImplementationOnce((request, signal) => {
      first.runs.push(request);
      return new Promise<AgentRunResult>((_resolve, reject) => signal!.addEventListener("abort", () => reject(new Error("recorded close before inference")), { once: true }));
    });
    const events = [0, 1].map(n => ({ ...first.event({ ref: `close-pending-${n}` }), to: actor.id }));
    for (const event of events) expect(first.monitor.callbacks.onEvent(event)).toBe(true);
    for (let turn = 0; turn < 100 && !first.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    await first.close();
    const receiptDir = path.join(first.mesh.root, "delivery-outcomes");
    expect(fs.existsSync(receiptDir)).toBe(false);
    const second = await fixture(first.root);
    await settled(second.actors, actor.id);
    expect(second.runs).toHaveLength(1);
    expect(inputs(second.runs[0]!).map(input => input.payload.id)).toEqual(events.map(event => event.id));
    const receipts = fs.readdirSync(receiptDir).flatMap(file => fs.readFileSync(path.join(receiptDir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.map(receipt => receipt.eventId)).toEqual(events.map(event => event.id));
    expect(receipts.every(receipt => receipt.outcome === "delivered")).toBe(true);
  });

  it("same-key pending originals are suppressed only after their earlier batch completes successfully", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 3, residency: "durable", dedupeKey: "data.key" });
    let release!: () => void;
    f.hold(new Promise<void>(resolve => { release = resolve; }));
    fakeClock();
    const events = ["same", "different", "same"].map((key, n) => f.send({ key, n }));
    for (let turn = 0; turn < 100 && !f.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    const active = f.snapshot(actor.id).items.map(item => item.payload.id);
    release(); // Always release the recorded run before assertions can fail.
    expect(active).toEqual(events.map(event => event.id));
    expect(inputs(f.runs[0]!)).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(W);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(f.snapshot(actor.id).processedKeys).toHaveLength(2);
  });

  it.each([W, 0])("preparation failure preserves an older batch before a bypass, but legacy rotation remains (W=%s)", async window => {
    let preparing!: () => void;
    let fail!: (error: Error) => void;
    const entered = new Promise<void>(resolve => { preparing = resolve; });
    const gate = new Promise<never>((_resolve, reject) => { fail = reject; });
    let calls = 0;
    const f = await fixture(undefined, 32, true, { preparationRetryMs: 20, acquireCapabilityView: async () => {
      if (++calls === 1) { preparing(); await gate; }
      return { satisfied: true, missing: [], optionalMissing: [], view: { id: "test", digest: "test", semanticDigest: "test", bindings: {} }, release: async () => {} };
    } });
    const actor = await create(f.actors, { batchWindowMs: window, batchMax: 2, requires: ["demo.echo"] });
    fakeClock();
    f.send({ ref: "older-1" });
    f.send({ ref: "older-2" });
    await entered;
    f.send({ ref: "urgent" }, "steer");
    fail(new Error("recorded preparation failure"));
    for (let turn = 0; turn < 100 && f.actors.inFlightCount(); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(f.runs).toHaveLength(0);
    expect(f.snapshot(actor.id).items.map(item => (item.payload.data as { ref: string }).ref)).toEqual(window > 0
      ? ["older-1", "older-2", "urgent"] : ["older-2", "urgent", "older-1"]);
    await vi.advanceTimersByTimeAsync(20);
    await settled(f.actors, actor.id);
    if (window > 0) {
      expect(inputs(f.runs[0]!).map(input => input.payload.data)).toEqual([{ ref: "older-1" }, { ref: "older-2" }]);
      expect(f.runs[1]!.task).toContain('"ref": "urgent"');
    } else {
      expect(f.runs.map(run => run.task.match(/"ref": "([^"]+)"/)?.[1])).toEqual(["older-2", "urgent", "older-1"]);
    }
  });

  it("a failed durable manifest checkpoint launches nothing and retries the same originals", async () => {
    const f = await fixture(undefined, 32, true, { preparationRetryMs: 20 });
    const actor = await create(f.actors, { batchMax: 2 });
    fakeClock();
    const rename = fs.renameSync;
    let refused = false;
    vi.spyOn(fs, "renameSync").mockImplementation((source, destination) => {
      if (!refused && String(destination).includes("queue-") && fs.readFileSync(source, "utf8").includes('"activationId"')) {
        refused = true;
        throw Object.assign(new Error("recorded manifest write refusal"), { code: "EIO" });
      }
      return rename(source, destination);
    });
    const events = [0, 1].map(n => ({ ...f.event({ ref: `checkpoint-${n}` }), to: actor.id }));
    for (const event of events) expect(f.monitor.callbacks.onEvent(event)).toBe(true);
    for (let turn = 0; turn < 100 && (!refused || f.actors.inFlightCount()); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(refused).toBe(true);
    expect(f.runs).toHaveLength(0);
    expect(f.snapshot(actor.id).items.map(item => item.payload.id)).toEqual(events.map(event => event.id));
    const receiptDir = path.join(f.mesh.root, "delivery-outcomes");
    expect(fs.existsSync(receiptDir)).toBe(false); // Retained preparation work is not terminal FAILED.
    await vi.advanceTimersByTimeAsync(20);
    await settled(f.actors, actor.id);
    expect(f.runs).toHaveLength(1);
    expect(inputs(f.runs[0]!).map(input => input.payload.id)).toEqual(events.map(event => event.id));
    const receipts = fs.readdirSync(receiptDir).flatMap(file => fs.readFileSync(path.join(receiptDir, file), "utf8").trim().split("\n").map(line => JSON.parse(line)));
    expect(receipts.map(receipt => receipt.eventId)).toEqual(events.map(event => event.id));
    expect(receipts.every(receipt => receipt.outcome === "delivered")).toBe(true);
  });

  it("pre-launch membership is durable even when transport never calls onSpawned", async () => {
    const first = await fixture();
    const actor = await create(first.actors, { batchMax: 2 });
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    first.runStub.mockImplementationOnce(async request => { first.runs.push(request); await gate; throw new Error("recorded host crash before launch callback"); });
    fakeClock();
    first.send({ ref: "planned-1" });
    first.send({ ref: "planned-2" });
    for (let turn = 0; turn < 100 && !first.runs.length; turn++) await new Promise<void>(resolve => setImmediate(resolve));
    const file = first.snapshotFile(actor.id);
    const interrupted = fs.readFileSync(file, "utf8");
    const active = first.snapshot(actor.id).items;
    const ids = active.map(item => item.id);
    expect(active.every(item => item.activationId === ids[0] && JSON.stringify(item.batchIds) === JSON.stringify(ids) && !item.executionStarted)).toBe(true);
    expect(first.runs[0]!.task.startsWith(ACTOR_ACTIVATION_HEADER + JSON.stringify({ activationId: ids[0], batchIds: ids }))).toBe(true);
    release();
    await settled(first.actors, actor.id);
    await first.close();
    fs.writeFileSync(file, interrupted); // Recorded pre-callback crash, no destructive execution.
    const second = await fixture(first.root, 32, false);
    second.send({ ref: "new-after-crash" });
    second.actors.resumeAfterRelease();
    for (let turn = 0; turn < 100 && (!second.runs.length || second.actors.inFlightCount()); turn++) await new Promise<void>(resolve => setImmediate(resolve));
    expect(inputs(second.runs[0]!).map(input => input.id)).toEqual(ids);
    expect(second.runs[0]!.activationId).toBe(ids[0]);
    expect(second.actors.status(actor.id).queued).toBe(1);
    await vi.advanceTimersByTimeAsync(W);
    await settled(second.actors, actor.id);
    expect(inputs(second.runs[1]!).map(input => input.payload.data)).toEqual([{ ref: "new-after-crash" }]);
  });

  it.each([1, 3].flatMap(count => ["native", "win32"].map(platform => ({ count, platform }))))(
    "ENODEV saved-answer recovery recognizes an explicit $count-event activation without inference ($platform)", async ({ count, platform }) => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: count });
    fakeClock();
    for (let n = 0; n < count; n++) f.send({ ref: `recover-${n}` });
    await settled(f.actors, actor.id);
    const request = f.runs[0]!;
    const runRoot = path.join(f.root, "saved-runs");
    const id = "a".repeat(32);
    const directory = path.join(runRoot, id);
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "task.txt"), request.task);
    // Recorded worker, not the live test parent: a fake /proc birth mismatch is
    // Linux-only death evidence and must never relax the live/unknown Windows veto.
    const workerPid = 2_147_483_647;
    const alive = scratch.processAlive;
    const workerAlive = vi.spyOn(scratch, "processAlive").mockImplementation(pid => pid === workerPid ? false : alive(pid));
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({ id, actorId: actor.id, status: "running", transport: "process", sessionId: String(workerPid), processStartTime: "0" }));
    fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({ reason: ENODEV_UNRESOLVED_REASON }));
    fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({ version: 1, runId: id, outcome: "completed", text: "saved batch answer" }));
    const manager = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot });
    closers.push(() => manager.close());
    const spawn = vi.spyOn(manager, "spawn").mockRejectedValue(new Error("must not infer again"));
    const nativePlatform = Object.getOwnPropertyDescriptor(process, "platform")!;
    try {
      if (platform === "win32") Object.defineProperty(process, "platform", { ...nativePlatform, value: "win32" });
      if (process.platform === "win32") {
        workerAlive.mockImplementation(pid => pid === workerPid ? true : alive(pid));
        expect(peekEnodevRun(directory, actor.id, request.activationId!)).toBeUndefined(); // Live, unreadable birth keeps the veto.
        workerAlive.mockImplementation(pid => pid === workerPid ? false : alive(pid));
      }
      expect(peekEnodevRun(directory, actor.id, request.activationId!)).toMatchObject({ status: "completed", text: "saved batch answer" });
      expect(peekEnodevRun(directory, actor.id, "another-activation")).toBeUndefined();
      expect(await manager.run(request)).toMatchObject({ id, status: "completed", enodevRecovered: true, text: "saved batch answer" });
      expect(spawn).not.toHaveBeenCalled();
      // The same authorization/abort fences still apply on native and forced win32.
      await expect(manager.run(request, undefined, undefined, () => false)).rejects.toThrow("must not infer again");
      const aborted = new AbortController();
      aborted.abort();
      await expect(manager.run(request, aborted.signal)).rejects.toThrow("must not infer again");
      expect(spawn).toHaveBeenCalledTimes(2);
      expect(fs.existsSync(path.join(directory, "unresolved-worker.json"))).toBe(true); // No local real-delete commit.
      fs.writeFileSync(path.join(directory, "task.txt"), ACTOR_ACTIVATION_HEADER + JSON.stringify({ activationId: "bad", batchIds: [] }) + "\n" + request.task);
      expect(peekEnodevRun(directory, actor.id, request.activationId!)).toBeUndefined();
      // W=0 legacy object tasks still match their own top-level id, not nested payload data.
      fs.writeFileSync(path.join(directory, "task.txt"), "Fabric actor message:\n" + JSON.stringify({ payload: { id: "nested" }, id: "legacy" }, null, 2));
      expect(peekEnodevRun(directory, actor.id, "legacy")).toMatchObject({ text: "saved batch answer" });
      expect(peekEnodevRun(directory, actor.id, "nested")).toBeUndefined();
    } finally { Object.defineProperty(process, "platform", nativePlatform); }
  });
});

describe("batching configuration surface", () => {
  it("validates create/set inputs and persists live/global export and import fields", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 12, batchMaxBytes: 4096 });
    expect(f.actors.definition(actor.id)).toMatchObject({ batchWindowMs: W, batchMax: 12, batchMaxBytes: 4096 });
    expect(await f.actors.setBatching(actor.id, { batchMax: 7 })).toMatchObject({ batchWindowMs: W, batchMax: 7 });
    const global = new GlobalActorRegistry(path.join(f.root, "global.json"), 256 * 1024);
    const template = global.create(f.actors.definition(actor.id));
    expect(global.toRequest(template)).toMatchObject({ batchWindowMs: W, batchMax: 7, batchMaxBytes: 4096 });
    expect(global.update(template.id, { batchWindowMs: 0 })).toMatchObject({ batchWindowMs: 0, batchMax: 7 });
    const loaded = new GlobalActorRegistry(path.join(f.root, "global.json"), 256 * 1024);
    expect(loaded.toRequest(loaded.resolve(template.id)!)).toMatchObject({ batchWindowMs: 0, batchMax: 7, batchMaxBytes: 4096 });
    for (const batchWindowMs of [-1, 1.5, NaN, Infinity, "300000"]) expect(() => normalizeActorBatching({ batchWindowMs })).toThrow("batchWindowMs");
    for (const batchMax of [0, -1, 1.5, NaN, "50", 1001]) expect(() => normalizeActorBatching({ batchMax })).toThrow("batchMax");
    for (const batchMaxBytes of [0, -1, 1.5, NaN, Infinity, "4096"]) expect(() => normalizeActorBatching({ batchMaxBytes })).toThrow(FabricActorBatchPolicyError);
    await expect(f.actors.setBatching(actor.id, {})).rejects.toMatchObject({ code: "FABRIC_ACTOR_BATCH_POLICY_INVALID" });
    await expect(f.actors.create({ name: "bad", instructions: "x", batchMax: 0 })).rejects.toMatchObject({ code: "FABRIC_ACTOR_BATCH_POLICY_INVALID" });
    expect(() => normalizeActorBatching(null)).toThrow(FabricActorBatchPolicyError);
    await expect(f.actors.setBatching(actor.id, { batchMax: 0 })).rejects.toMatchObject({ name: "FabricActorBatchPolicyError", code: "FABRIC_ACTOR_BATCH_POLICY_INVALID" });
    expect(() => global.update(template.id, { batchMax: 0 })).toThrow(FabricActorBatchPolicyError);
    const createDescriptor = AGENTS_ACTION_DESCRIPTORS.find(action => action.name === "create")!;
    expect(createDescriptor.inputSchema.properties).toHaveProperty("batchWindowMs");
    expect(createDescriptor.inputSchema.properties).toHaveProperty("batchMax");
    expect(createDescriptor.inputSchema.properties).toHaveProperty("batchMaxBytes");
    expect(AGENTS_ACTION_DESCRIPTORS.find(action => action.name === "setBatching")).toMatchObject({ risk: "agent" });
  });

  it("resident setters forward the policy only for the actual Main, never a child lineage", async () => {
    const f = await fixture();
    const actor = await create(f.actors);
    const root: FabricParticipantInfo = {
      format: 1, id: identity.id, name: "main", rootId: identity.id, kind: "root", ownerHostId: "host:main",
      ownerIdentityId: identity.id, status: "idle", transport: "host", capabilities: [], controlProtocol: "v1",
      sessionId: "test", startedAt: Date.now(), updatedAt: Date.now(), local: true, stale: false,
    };
    const resident: FabricParticipantInfo = { ...root, id: actor.id, name: actor.name, kind: "actor", ownerHostId: residentHostId(identity.id) };
    const participants: FabricParticipantSource = {
      list: () => [root, resident], get: id => id === actor.id ? resident : id === root.id ? root : undefined,
      self: () => root, peers: () => [], refresh: async () => {}, scheduleRefresh: () => {},
    };
    const main = { id: identity.id, local: true } as FabricMainAgentTarget;
    const setActor = vi.fn().mockResolvedValue({ ...actor, batchWindowMs: W, batchMax: 50 });
    const client = { setActor } as unknown as ResidencyClient;
    const global = new GlobalActorRegistry(path.join(f.root, "resident-global.json"), 256 * 1024);
    const lifecycle = new LifecycleBroker(f.mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(f.agents, f.actors, global, main, participants, undefined, lifecycle, undefined, client);
    vi.spyOn(f.actors, "owns").mockReturnValue(false); // Passive definition; the existing native owner route handles it.
    const context = { cwd: process.cwd(), signal: undefined, parentToolCallId: "test", nestedToolCallId: "test", extensionContext: {}, update: () => {} } as unknown as FabricInvocationContext;
    await provider.invoke("setBatching", { id: actor.id, batchWindowMs: W, batchMax: 50, batchMaxBytes: 4096 }, context);
    expect(setActor).toHaveBeenCalledWith({ operation: "setBatching", id: actor.id, batchWindowMs: W, batchMax: 50, batchMaxBytes: 4096 }, undefined,
      expect.objectContaining({ identity, hostId: root.ownerHostId }));
    expect(RESIDENT_COMMANDS).toContain("setBatching");
    setActor.mockClear();
    await expect(provider.invoke("setBatching", { id: actor.id, batchMaxBytes: 0 }, context)).rejects.toMatchObject({ code: "FABRIC_ACTOR_BATCH_POLICY_INVALID" });
    expect(setActor).not.toHaveBeenCalled();
    const child = new AgentsProvider(f.agents, f.actors, global, { ...main, local: false }, participants, undefined, lifecycle, undefined, client);
    await expect(child.invoke("setBatching", { id: actor.id, batchWindowMs: 0 }, context)).rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
    expect(setActor).not.toHaveBeenCalled();
  });

  it("provider routes create and live/global setter values through the existing actor configuration path", async () => {
    const f = await fixture();
    const global = new GlobalActorRegistry(path.join(f.root, "provider-global.json"), 256 * 1024);
    const main = { id: identity.id, local: true } as FabricMainAgentTarget;
    const participants = { list: () => [], get: () => undefined, refresh: async () => {}, scheduleRefresh: () => {} } as unknown as FabricParticipantSource;
    const lifecycle = new LifecycleBroker(f.mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(f.agents, f.actors, global, main, participants, undefined, lifecycle);
    const context = { cwd: process.cwd(), signal: undefined, parentToolCallId: "test", nestedToolCallId: "test", extensionContext: {}, update: () => {} } as unknown as FabricInvocationContext;
    const actor = await provider.invoke("create", { name: "provider-batch", instructions: "Observe.", batchWindowMs: W, batchMax: 10 }, context) as { id: string };
    expect(f.actors.status(actor.id)).toMatchObject({ batchWindowMs: W, batchMax: 10 });
    expect(await provider.invoke("setBatching", { id: actor.id, batchWindowMs: 0 }, context)).toMatchObject({ batchWindowMs: 0, batchMax: 10 });
    const template = global.create({ name: "global-batch", instructions: "Observe." });
    expect(await provider.invoke("setBatching", { id: template.id, batchWindowMs: W, scope: "global" }, context)).toMatchObject({ batchWindowMs: W });
    await expect(provider.invoke("setBatching", { id: actor.id, batchMax: 0 }, context)).rejects.toMatchObject({ code: "FABRIC_ACTOR_BATCH_POLICY_INVALID" });
    await expect(provider.invoke("setBatching", { id: template.id, batchMax: 0, scope: "global" }, context)).rejects.toMatchObject({ code: "FABRIC_ACTOR_BATCH_POLICY_INVALID" });
  });
});
