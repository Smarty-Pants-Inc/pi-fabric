import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";

const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  await Promise.all(closers.splice(0).map((close) => close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const setup = (root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-settled-interval-")), owns?: () => boolean) => {
  if (!roots.includes(root)) roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true,
    ...(owns ? { canManageActor: owns } : {}),
  });
  const close = async () => { await actors.close(); await agents.close(); };
  closers.push(close);
  return { root, mesh, agents, actors, close };
};
// Genuine FabricRuntimeState host envelope: signal.payload.source describes an input's origin,
// never the identity of the observed agent. The real identity is session.id.
const payload = (sessionId = "source-a", origin = "extension", event = "agent_settled") => ({
  event, session: { id: sessionId, cwd: process.cwd() }, digest: {}, transcript: [],
  signal: { payload: { type: event, source: origin }, idle: true, observedAt: 123 },
});
const incoming = (actors: ActorManager, id: string, source = "host:agent_settled") =>
  actors.messages(id).filter((message) => message.direction === "in" && message.source === source);
const clock = () => {
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  return { set: (value: number) => { now = value; }, advance: (ms: number) => { now += ms; } };
};
const waitFor = async (predicate: () => boolean) => {
  const deadline = performance.now() + 10_000;
  while (!predicate()) {
    if (performance.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

describe("actor agent_settled leading + latest trailing minimum interval", () => {
  const timedActor = async (minIntervalMs = 1_000, owns?: () => boolean, coalesce = false) => {
    const fixture = setup(undefined, owns);
    fixture.actors.pauseForRelease(); // Observe real ingress/queueing without launching a worker.
    const actor = await fixture.actors.create({ name: "timed", instructions: "Observe.", events: ["agent_settled"], coalesce, activation: { minIntervalMs } });
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.setSystemTime(10_000);
    return { ...fixture, actor };
  };

  it("delivers a lone in-window settle once at the original window end without another event", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    expect(actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "trailing" })).toBe(0);
    vi.advanceTimersByTime(899);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "trailing"]);
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("delivers a burst as leading plus one trailing latest payload without sliding the boundary", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "superseded" });
    vi.advanceTimersByTime(800);
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "latest" });
    vi.advanceTimersByTime(99);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "latest"]);
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("cancels the pending trailing timer on close and never delivers after close", async () => {
    const { actors, actor, close } = await timedActor();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const cancelled = vi.spyOn(globalThis, "clearTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload());
    const index = scheduled.mock.calls.findIndex(([, delay]) => delay === 900);
    expect(index).toBeGreaterThanOrEqual(0);
    const timer = scheduled.mock.results[index]!.value;
    await close();
    expect(cancelled).toHaveBeenCalledWith(timer);
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("lets a boundary arrival supersede an older still-pending trailing event", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "old-pending" });
    vi.setSystemTime(11_000); // Event-loop ordering: arrival beats the due timer callback.
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "boundary-latest" });
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "boundary-latest"]);
  });

  it("chunks intervals above Node's timer maximum without early delivery", async () => {
    const maximumDelay = 2_147_483_647;
    const { actors, actor } = await timedActor(maximumDelay + 1_000);
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    actors.dispatchHostEvent("agent_settled", payload());
    expect(scheduled.mock.calls.some(([, delay]) => delay === maximumDelay)).toBe(true);
    vi.advanceTimersByTime(maximumDelay);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(999);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("retains independent per-source trailing payloads and boundaries", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", payload("source-a"));
    vi.advanceTimersByTime(200);
    actors.dispatchHostEvent("agent_settled", payload("source-b"));
    actors.dispatchHostEvent("agent_settled", { ...payload("source-a"), marker: "a-latest" });
    actors.dispatchHostEvent("agent_settled", { ...payload("source-b"), marker: "b-latest" });
    vi.advanceTimersByTime(800);
    expect(incoming(actors, actor.id)).toHaveLength(3);
    vi.advanceTimersByTime(200);
    expect(incoming(actors, actor.id).slice(2).map((message) => (message.data as { marker: string }).marker)).toEqual(["a-latest", "b-latest"]);
  });

  it("cancels on stop and same-name recreation starts with a fresh immutable ID", async () => {
    const { actors, actor } = await timedActor();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const cancelled = vi.spyOn(globalThis, "clearTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "old" });
    const timer = scheduled.mock.results[scheduled.mock.calls.findIndex(([, delay]) => delay === 900)]!.value;
    await actors.stop(actor.id);
    expect(cancelled).toHaveBeenCalledWith(timer);
    const replacement = await actors.create({ ...actors.definition(actor.id), name: actor.name });
    expect(replacement.id).not.toBe(actor.id);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "new" });
    vi.advanceTimersByTime(2_000);
    expect(incoming(actors, replacement.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["new"]);
    expect(() => actors.status(actor.id)).toThrow();
  });

  it("cancels on removal without rearming a removed actor", async () => {
    const { actors, actor } = await timedActor();
    const scheduled = vi.spyOn(globalThis, "setTimeout");
    const cancelled = vi.spyOn(globalThis, "clearTimeout");
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload());
    const timer = scheduled.mock.results[scheduled.mock.calls.findIndex(([, delay]) => delay === 900)]!.value;
    await actors.remove(actor.id);
    expect(cancelled).toHaveBeenCalledWith(timer);
    vi.advanceTimersByTime(2_000);
    expect(actors.list()).toHaveLength(0);
  });

  it("cancels on halt even if user input resumes before the trailing boundary", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "cancelled" });
    actors.haltAll();
    actors.dispatchHostEvent("input", payload("source-a", "user", "input"));
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(1);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "resumed" });
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("delivers trailing on the current registry object after ownership reload", async () => {
    let owned = true;
    const { actors, actor } = await timedActor(1_000, () => owned);
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "latest" });
    owned = false;
    actors.listOwned();
    owned = true; // The timer's fresh ownership check triggers ManagedActor replacement.
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(2);
    expect((incoming(actors, actor.id)[1]!.data as { marker: string }).marker).toBe("latest");
    expect(actors.status(actor.id).queued).toBe(1); // Leading is parked until restore's microtask.
  });

  it("fences trailing delivery when ownership is lost at the boundary", async () => {
    let owned = true;
    const { actors, actor } = await timedActor(1_000, () => owned);
    actors.dispatchHostEvent("agent_settled", payload());
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", payload());
    owned = false;
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("rechecks current event subscriptions at trailing delivery", async () => {
    const { actors, actor } = await timedActor();
    actors.dispatchHostEvent("agent_settled", payload());
    actors.dispatchHostEvent("agent_settled", payload());
    await actors.setEvents(actor.id, []);
    vi.advanceTimersByTime(1_000);
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("skipped arrivals neither replace a held payload nor arm a window, and trailing rechecks filters", async () => {
    const { actors, actor } = await timedActor();
    const filter = [{ id: "skip-marker", source: ["host:agent_settled"], where: [{ path: "marker", equals: "skip" }] }];
    await actors.setActivationFilter(actor.id, filter);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "skip" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "keep" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "skip" });
    vi.advanceTimersByTime(1_000);
    const admitted = () => incoming(actors, actor.id).filter((message) => !message.reason);
    expect(admitted().map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "keep"]);
    expect(actors.status(actor.id).filterSkipped.count).toBe(2);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "now-filtered" });
    await actors.setActivationFilter(actor.id, [{ id: "new-filter", source: ["host:agent_settled"] }]);
    vi.advanceTimersByTime(1_000);
    expect(admitted()).toHaveLength(2);
    expect(actors.status(actor.id).filterSkipped.count).toBe(1);
  });

  it("clones latest payload/images and retains the existing queue coalescing contract", async () => {
    const { actors, actor, root } = await timedActor(1_000, undefined, true);
    const directory = path.join(root, "actors", actor.id);
    const queued = () => JSON.parse(fs.readFileSync(path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!), "utf8")) as {
      items: Array<{ payload: { marker: string }; images?: Array<{ data: string }>; coalesceKey: string }>;
    };
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "superseded" }, [{ type: "image", data: "old", mimeType: "image/png" }]);
    const latest = { ...payload(), marker: "latest" };
    const images = [{ type: "image" as const, data: "latest-image", mimeType: "image/png" }];
    actors.dispatchHostEvent("agent_settled", latest, images);
    latest.marker = "mutated";
    images[0]!.data = "mutated";
    vi.advanceTimersByTime(900);
    expect(queued().items).toHaveLength(1); // Trailing merges with an unlaunched leading item.
    expect(queued().items[0]).toMatchObject({ payload: { marker: "latest" }, images: [{ data: "latest-image" }], coalesceKey: "host:agent_settled" });
    actors.dispatchHostEvent("agent_settled", { ...payload(), marker: "without-image" });
    vi.advanceTimersByTime(1_000);
    expect(queued().items[0]).toMatchObject({ payload: { marker: "without-image" } });
    expect(queued().items[0]!.images).toBeUndefined();
    expect(actors.status(actor.id).queued).toBe(1);
  });

  it("shares latest trailing state with authenticated root-relay ingress, never a stranger", async () => {
    let monitor: ActorMeshMonitor | undefined;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) { monitor = this; });
    const { actors, actor, mesh } = await timedActor();
    const relay = async (from: MeshIdentity, marker: string) => {
      const event = await mesh.publish({
        topic: "fabric.actor.host-event", kind: "agent_settled", from, to: actor.id,
        data: { version: 1, actorId: actor.id, event: "agent_settled", payload: { ...payload(), marker }, mainRevision: 0, taskRevision: 0, idle: true },
      });
      monitor!.callbacks.onEvent(event); // Exact production manager reception callback, without watcher timing.
    };
    await relay({ ...identity, id: "session:stranger" }, "stranger-first");
    actors.dispatchObservedHostEvent("agent_settled", { ...payload(), marker: "leading" });
    vi.advanceTimersByTime(100);
    await relay(identity, "relay-superseded");
    actors.dispatchObservedHostEvent("agent_settled", { ...payload("source-a", "user"), marker: "local-superseded" });
    await relay(identity, "relay-latest");
    await relay({ ...identity, id: "session:stranger" }, "stranger-last");
    vi.advanceTimersByTime(900);
    expect(incoming(actors, actor.id).map((message) => (message.data as { marker: string }).marker)).toEqual(["leading", "relay-latest"]);
  });

  it("admits the first immediately, holds same-source repeats, and never slides the boundary", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "limited", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    const time = clock();
    time.set(10_000);
    expect(actors.dispatchHostEvent("agent_settled", payload())).toBe(1);
    expect(incoming(actors, actor.id)).toHaveLength(1); // synchronous admission; no delay/timer
    const completed = () => actors.messages(actor.id).filter((message) => message.direction === "out" && !message.error);
    await waitFor(() => completed().length === 1 && actors.status(actor.id).status === "idle");
    time.advance(100);
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(1);
    time.set(10_999);
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(1);
    time.set(11_000); // exactly first + interval, not last dropped + interval
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(2);
    await waitFor(() => completed().length === 2 && actors.status(actor.id).status === "idle");
    expect(completed()).toHaveLength(2); // actual worker wakes, not just queued observations
  });

  it("keys by real source session, independently per actor, not input user/extension origin", async () => {
    const { actors } = setup();
    const first = await actors.create({ name: "first", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    const time = clock();
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a", "extension"));
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a", "user"));
    actors.dispatchObservedHostEvent("agent_settled", payload("source-b", "extension"));
    expect(incoming(actors, first.id)).toHaveLength(2);
    const second = await actors.create({ name: "second", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 1_000 } });
    time.advance(1);
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a"));
    expect(incoming(actors, first.id)).toHaveLength(2);
    expect(incoming(actors, second.id)).toHaveLength(1);
  });

  it("leaves other event kinds, direct messages and mesh events unaffected", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({ name: "mixed", instructions: "Observe.", events: ["agent_settled", "turn_end"], topics: ["demo"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    actors.dispatchHostEvent("agent_settled", payload());
    for (let i = 0; i < 2; i++) {
      actors.dispatchHostEvent("turn_end", payload("source-a", "extension", "turn_end"));
      actors.tell(actor.id, "direct");
      await mesh.publish({ topic: "demo", kind: "agent_settled", from: identity, data: payload() });
    }
    await waitFor(() => incoming(actors, actor.id, "mesh:demo").length === 2);
    expect(incoming(actors, actor.id, "host:turn_end")).toHaveLength(2);
    expect(incoming(actors, actor.id, "direct")).toHaveLength(2);
  });

  it.each([undefined, { minIntervalMs: 0 }])("keeps today's every-event behavior when disabled (%j)", async (activation) => {
    const { actors } = setup();
    const actor = await actors.create({ name: "plain", instructions: "Observe.", events: ["agent_settled"], coalesce: false, ...(activation ? { activation } : {}) });
    actors.dispatchHostEvent("agent_settled", payload());
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(2);
  });

  it("uses trusted root identity when an older host envelope has no session.id", async () => {
    const { actors } = setup();
    const actor = await actors.create({ name: "legacy", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    actors.dispatchHostEvent("agent_settled", { signal: { payload: { source: "extension" } } });
    actors.dispatchHostEvent("agent_settled", { signal: { payload: { source: "user" } } });
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("retains config through definition/global export-import/restart, but resets runtime windows", async () => {
    const first = setup();
    const activation = { minIntervalMs: 60_000 };
    const actor = await first.actors.create({ name: "roundtrip", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation });
    expect(actor.activation).toEqual(activation);
    expect(first.actors.status(actor.id).activation).toEqual(activation);
    const definition = first.actors.definition(actor.id);
    expect(definition.activation).toEqual(activation);
    const globals = new GlobalActorRegistry(first.root, 64 * 1024);
    const template = globals.create(definition);
    expect(template.activation).toEqual(activation);
    expect(globals.update(template.id, { instructions: "Updated." }).activation).toEqual(activation);
    const reloadedGlobals = new GlobalActorRegistry(first.root, 64 * 1024);
    expect(reloadedGlobals.resolve(template.id)?.activation).toEqual(activation);
    const request = reloadedGlobals.toRequest(reloadedGlobals.resolve(template.id)!, "imported");
    expect(request.activation).toEqual(activation);
    expect((await first.actors.create(request)).activation).toEqual(activation);
    first.actors.dispatchHostEvent("agent_settled", payload());
    await waitFor(() => first.actors.status(actor.id).status === "idle");
    await first.close();
    closers.splice(closers.indexOf(first.close), 1);
    const second = setup(first.root);
    expect(second.actors.status(actor.id).activation).toEqual(activation);
    second.actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(second.actors, actor.id)).toHaveLength(2); // persisted inbox + new first wake
    const registry = fs.readFileSync(path.join(first.root, "actors", "actors.json"), "utf8");
    expect(registry).not.toMatch(/settledWindows|lastAccepted|nextAllowed/);
  });

  it("survives same-process ownership/registry object reloads", async () => {
    let owned = true;
    const { actors } = setup(undefined, () => owned);
    const actor = await actors.create({ name: "reload", instructions: "Observe.", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    actors.dispatchHostEvent("agent_settled", payload());
    await waitFor(() => actors.status(actor.id).status === "idle");
    owned = false;
    actors.listOwned();
    owned = true;
    actors.listOwned(); // #refreshOwnership rebuilds ManagedActor
    actors.dispatchHostEvent("agent_settled", payload());
    expect(incoming(actors, actor.id)).toHaveLength(1);
  });

  it("shares the local gate with authenticated durable-owner relay reception", async () => {
    const { actors, mesh } = setup();
    const actor = await actors.create({ name: "relay", instructions: "Observe.", residency: "durable", events: ["agent_settled"], coalesce: false, activation: { minIntervalMs: 60_000 } });
    const relay = (from: MeshIdentity, hostPayload: unknown) => mesh.publish({
      topic: "fabric.actor.host-event", kind: "agent_settled", from, to: actor.id,
      data: { version: 1, actorId: actor.id, event: "agent_settled", payload: hostPayload, mainRevision: 0, taskRevision: 0, idle: true },
    });
    const stranger: MeshIdentity = { ...identity, id: "session:stranger" };
    await relay(stranger, payload("source-a")); // must not poison the source window
    await relay(identity, payload("source-a"));
    await waitFor(() => incoming(actors, actor.id).length >= 1);
    actors.dispatchObservedHostEvent("agent_settled", payload("source-a", "user"));
    await relay(identity, payload("source-a"));
    await relay(identity, payload("source-b"));
    await waitFor(() => incoming(actors, actor.id).some((m) => (m.data as ReturnType<typeof payload>).session?.id === "source-b"));
    expect(incoming(actors, actor.id)).toHaveLength(2);
    // local trusted-root fallback and relay trusted-root fallback are the same source
    actors.dispatchObservedHostEvent("agent_settled", {});
    await relay(identity, {});
    await relay(identity, payload("source-c")); // ordered receipt proves fallback relay consumed
    await waitFor(() => incoming(actors, actor.id).some((m) => (m.data as ReturnType<typeof payload>).session?.id === "source-c"));
    expect(incoming(actors, actor.id)).toHaveLength(4);
  });

  it.each([-1, 1.5, Infinity, NaN, "1000", Number.MAX_SAFE_INTEGER + 1])("rejects invalid minIntervalMs %s instead of silently enabling/disabling", async (minIntervalMs) => {
    const { actors, root } = setup();
    const request = { name: "invalid", instructions: "Observe.", activation: { minIntervalMs } };
    await expect(actors.create(request as Parameters<ActorManager["create"]>[0])).rejects.toThrow(/activation|minIntervalMs/);
    expect(() => new GlobalActorRegistry(root, 64 * 1024).create(request as Parameters<ActorManager["create"]>[0])).toThrow(/activation|minIntervalMs/);
  });

  it("publishes a strict create schema and preserves provider create/export/import config", async () => {
    const { actors, agents, mesh, root } = setup();
    const createSchema = AGENTS_ACTION_DESCRIPTORS.find((action) => action.name === "create")!.inputSchema as { properties: Record<string, unknown> };
    expect(createSchema.properties.activation).toMatchObject({ type: "object", additionalProperties: false, properties: { minIntervalMs: { type: "integer", minimum: 0 } } });
    const globalActors = new GlobalActorRegistry(root, 64 * 1024);
    const participants = { scheduleRefresh() {}, list: () => [], get: () => undefined } as unknown as FabricParticipantSource;
    const main = { id: identity.id, local: true } as FabricMainAgentTarget;
    const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, globalActors, main, participants, undefined, lifecycle);
    const context: FabricInvocationContext = {
      cwd: process.cwd(), signal: undefined, parentToolCallId: "test-parent", nestedToolCallId: "test-nested",
      extensionContext: {} as FabricInvocationContext["extensionContext"], activity() {}, update() {},
    };
    const activation = { minIntervalMs: 1_000 };
    const created = await provider.invoke("create", { name: "provider", instructions: "Observe.", activation }, context) as { id: string; activation?: unknown };
    expect(created.activation).toEqual(activation);
    const exported = await provider.invoke("export", { id: created.id, write: true }, context) as { id: string; activation?: unknown };
    expect(exported.activation).toEqual(activation);
    const imported = await provider.invoke("import", { id: exported.id, as: "provider-import" }, context) as { activation?: unknown };
    expect(imported.activation).toEqual(activation);
    const global = await provider.invoke("create", { name: "global-provider", instructions: "Observe.", scope: "global", activation }, context) as { activation?: unknown };
    expect(global.activation).toEqual(activation);
    await expect(provider.invoke("create", { name: "provider-invalid", instructions: "Observe.", activation: { minIntervalMs: -1 } }, context)).rejects.toThrow(/activation|minIntervalMs/);
  });
});
