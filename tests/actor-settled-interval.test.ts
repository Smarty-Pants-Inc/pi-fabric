import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
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

describe("actor agent_settled leading-edge minimum interval", () => {
  it("admits the first immediately, drops same-source repeats, and never slides the boundary", async () => {
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
