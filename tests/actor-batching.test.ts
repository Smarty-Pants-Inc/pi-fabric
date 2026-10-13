import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { normalizeActorBatching } from "../src/actors/types.js";
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

const fixture = async (root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-batching-")), limit = 32, resume = true) => {
  let monitor!: ActorMeshMonitor;
  vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
  const runs: AgentRunRequest[] = [];
  let hold: Promise<void> | undefined;
  vi.spyOn(agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
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
    canConsumeMesh: () => false,
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
      items: Array<{ id: string; payload: MeshEvent; executionStarted: boolean; resumed?: boolean; batchIds?: string[] }>;
      processedKeys?: string[];
    } : { items: [] };
  };
  const snapshotFile = (id: string) => {
    const dir = path.join(root, "actors", id);
    return path.join(dir, fs.readdirSync(dir).find(file => /^queue-.*\.json$/.test(file))!);
  };
  return { root, mesh, agents, actors, runs, send, sendPublished, event, monitor, snapshot, snapshotFile, close, hold: (gate: Promise<void> | undefined) => { hold = gate; } };
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

describe("batching configuration surface", () => {
  it("validates create/set inputs and persists live/global export and import fields", async () => {
    const f = await fixture();
    const actor = await create(f.actors, { batchMax: 12 });
    expect(f.actors.definition(actor.id)).toMatchObject({ batchWindowMs: W, batchMax: 12 });
    expect(await f.actors.setBatching(actor.id, { batchMax: 7 })).toMatchObject({ batchWindowMs: W, batchMax: 7 });
    const global = new GlobalActorRegistry(path.join(f.root, "global.json"), 256 * 1024);
    const template = global.create(f.actors.definition(actor.id));
    expect(global.toRequest(template)).toMatchObject({ batchWindowMs: W, batchMax: 7 });
    expect(global.update(template.id, { batchWindowMs: 0 })).toMatchObject({ batchWindowMs: 0, batchMax: 7 });
    const loaded = new GlobalActorRegistry(path.join(f.root, "global.json"), 256 * 1024);
    expect(loaded.toRequest(loaded.resolve(template.id)!)).toMatchObject({ batchWindowMs: 0, batchMax: 7 });
    for (const batchWindowMs of [-1, 1.5, NaN, Infinity, "300000"]) expect(() => normalizeActorBatching({ batchWindowMs })).toThrow("batchWindowMs");
    for (const batchMax of [0, -1, 1.5, NaN, "50"]) expect(() => normalizeActorBatching({ batchMax })).toThrow("batchMax");
    await expect(f.actors.setBatching(actor.id, {})).rejects.toThrow("required");
    await expect(f.actors.create({ name: "bad", instructions: "x", batchMax: 0 })).rejects.toThrow("batchMax");
    const createDescriptor = AGENTS_ACTION_DESCRIPTORS.find(action => action.name === "create")!;
    expect(createDescriptor.inputSchema.properties).toHaveProperty("batchWindowMs");
    expect(createDescriptor.inputSchema.properties).toHaveProperty("batchMax");
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
    await provider.invoke("setBatching", { id: actor.id, batchWindowMs: W, batchMax: 50 }, context);
    expect(setActor).toHaveBeenCalledWith({ operation: "setBatching", id: actor.id, batchWindowMs: W, batchMax: 50 }, undefined,
      expect.objectContaining({ identity, hostId: root.ownerHostId }));
    expect(RESIDENT_COMMANDS).toContain("setBatching");
    setActor.mockClear();
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
    await expect(provider.invoke("setBatching", { id: actor.id, batchMax: 0 }, context)).rejects.toThrow("batchMax");
  });
});
