import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { AgentManager } from "../src/agents/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { isOwnResidentActor } from "../src/residency/actor-ownership.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentActorMutation, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

// Keep: smarty-dev#18783 caught live subscription setters stopping at Main's
// non-owning projection. Exercise the real Main/resident custody + request boundary
// and mesh delivery; only the inference worker is fake (no provider/registry double).
beforeEach(() => installInProcessResidentFence());
afterEach(() => vi.restoreAllMocks());

const waitFor = async (predicate: () => boolean, timeoutMs = 10_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for resident subscription delivery");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

const fixture = async (scope: "session" | "project" = "project") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "resident-subscriptions-"));
  const identity = { id: "session:subscriptions", name: "Main", kind: "main" as const, sessionId: "subscriptions" };
  const meshRoot = path.join(root, "mesh");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorScope: "session" as const, actorPollMs: 20 };
  const actorRoots = { project: path.join(root, "actors"), session: path.join(root, "actors", identity.sessionId) };
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity });
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 }, {
    runRoot: path.join(root, "main-runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  // Main owns session actors, but observes its resident's actors in both roots.
  const passive = new ActorDirectory([identity.sessionId, identity, mesh, meshConfig, agents, () => {}, {
    persistent: true, rootId: identity.id, claimResidency: "session",
    canManageActor: id => {
      const actor = participants.get(id);
      return actor ? actor.ownerHostId === identity.id : undefined;
    },
    isOwnResidentActor: id => isOwnResidentActor(participants, id, identity.id),
    resolvePiModel: model => model,
  }], actorRoots, "session");
  participants.registerSource(() => [{
    format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id,
    name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host", capabilities: ["fabric"],
    cwd: root, sessionId: identity.sessionId, startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1",
  }, ...passive.listOwned().map(actor => actorParticipantRecord(actor, identity.id, identity.id, identity.id, identity.id))]);
  const unsubscribe = passive.subscribe(() => participants.scheduleRefresh());
  await participants.start();
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20 });
  control.start(() => ({ accepted: false }));
  const lifecycle = new LifecycleBroker(mesh, identity, participants,
    { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
  const config: ResidentHostConfig = {
    format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root, meshRoot,
    actorRoot: actorRoots.project, sessionActorRoot: actorRoots.session,
    residencyRoot: residentRoot(meshRoot, identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, budgetUsd: 0 },
    mesh: meshConfig, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  const host = new ResidentHost(config);
  await host.start();
  const mainAgent = { id: identity.id, local: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
  const client = new ResidencyClient({ config, mesh, participants, commandTimeoutMs: 5_000, mainAgent });
  const provider = new AgentsProvider(
    agents, passive, new GlobalActorRegistry(root, 64 * 1024), mainAgent, participants,
    control, lifecycle, () => false, client, false,
  );
  const actor = await host.actors.create({
    name: "security", instructions: "Keep persona", residency: "durable", scope, model: "fixture/visible", tools: [],
    topics: ["repair.events"], events: ["agent_settled"],
  });
  await participants.refresh();
  const context = {
    cwd: root, signal: undefined,
    extensionContext: { modelRegistry: { getAvailable: () => config.piModels?.available ?? [] } },
    parentToolCallId: "subscriptions", nestedToolCallId: "subscriptions", update() {}, activity() {},
  } as unknown as FabricInvocationContext;
  return {
    root, mesh, config, host, client, participants, passive, provider, actor, context, identity,
    close: async () => {
      unsubscribe();
      await passive.close();
      await agents.close();
      await control.close();
      await lifecycle.close();
      await client.close();
      await host.close();
      await participants.close();
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
};

type Fixture = Awaited<ReturnType<typeof fixture>>;
const publisher = { id: "session:subscription-publisher", name: "publisher", kind: "main" as const, sessionId: "publisher" };

const completedRuns = (f: Fixture, actorId: string): string[] => [...new Set(
  f.host.actors.messages(actorId).flatMap(message => message.direction === "out" && message.runId ? [message.runId] : []),
)];

const messageContains = (f: Fixture, actorId: string, marker: string): boolean =>
  f.host.actors.messages(actorId).some(message => JSON.stringify(message).includes(marker));

it.each(["session", "project"] as const)("routes public setTopics to the %s resident owner and admits only the new topic", async scope => {
  const f = await fixture(scope);
  const oldMarker = "resident-topic-before-cutover";
  const newMarker = "resident-topic-after-cutover";
  const newTopic = "repair.after-cutover";
  try {
    await f.provider.invoke("setTopics", { id: f.actor.id, topics: [newTopic] }, f.context);
    expect(f.host.actors.status(f.actor.id).topics).toEqual([newTopic]);

    await f.mesh.publish({ topic: "repair.events", from: publisher, text: oldMarker });
    await f.mesh.publish({ topic: newTopic, from: publisher, text: newMarker });
    await waitFor(() => messageContains(f, f.actor.id, newMarker) && completedRuns(f, f.actor.id).length > 0);

    expect(messageContains(f, f.actor.id, newMarker)).toBe(true);
    expect(messageContains(f, f.actor.id, oldMarker)).toBe(false);
    expect(completedRuns(f, f.actor.id)).toHaveLength(1);
  } finally {
    await f.close();
  }
}, 30_000);

it.each(["session", "project"] as const)("routes public setEvents to the %s resident owner and relays only the new Main host event", async scope => {
  const f = await fixture(scope);
  const oldMarker = "resident-host-event-before-cutover";
  const newMarker = "resident-host-event-after-cutover";
  try {
    await f.provider.invoke("setEvents", { id: f.actor.id, events: ["tool_error"] }, f.context);
    expect(f.host.actors.status(f.actor.id).events).toEqual(["tool_error"]);
    // Match FabricRuntimeState.dispatchHostEvent: observe both roots before delivery.
    // Do not manually refresh Main's passive projection or dispatch on the resident.
    expect(f.passive.observeHostEvent("agent_settled", true)).toBe(false);
    expect(f.passive.dispatchObservedHostEvent("agent_settled", { marker: oldMarker })).toBe(0);
    expect(f.passive.observeHostEvent("tool_error", false)).toBe(true);
    expect(f.passive.dispatchObservedHostEvent("tool_error", { marker: newMarker })).toBe(1);
    expect(f.passive.owns(f.actor.id)).toBe(false);
    expect(f.host.actors.owns(f.actor.id)).toBe(true);
    await waitFor(() => messageContains(f, f.actor.id, newMarker) && completedRuns(f, f.actor.id).length > 0);

    expect(messageContains(f, f.actor.id, newMarker)).toBe(true);
    expect(messageContains(f, f.actor.id, oldMarker)).toBe(false);
    expect(completedRuns(f, f.actor.id)).toHaveLength(1);
  } finally {
    await f.close();
  }
}, 30_000);

it("rejects malformed subscriptions and foreign setter callers without resident mutation or commit", async () => {
  const f = await fixture();
  const proxy = new ResidentActorClient(f.config.meshRoot, f.config.rootId, 5_000);
  const caller = { identity: f.identity, hostId: f.identity.id };
  const registryPath = path.join(f.config.actorRoot, "actors.json");
  const registry = fs.readFileSync(registryPath, "utf8");
  const before = f.host.actors.status(f.actor.id);
  const assertUnchanged = () => {
    expect(f.host.actors.status(f.actor.id)).toMatchObject({ topics: before.topics, events: before.events });
    expect(fs.readFileSync(registryPath, "utf8")).toBe(registry);
    const decisionsPath = path.join(f.config.residencyRoot, "decisions");
    for (const file of fs.existsSync(decisionsPath) ? fs.readdirSync(decisionsPath) : []) {
      const decision = JSON.parse(fs.readFileSync(path.join(decisionsPath, file), "utf8"));
      expect(decision.state).not.toBe("committed");
    }
  };
  try {
    // Provider admission must not silently coerce malformed topic requests.
    for (const args of [{ topics: "repair.events" }, { topics: [42] }, { topics: [], scope: "session" }]) {
      await expect(f.provider.invoke("setTopics", { id: f.actor.id, ...args }, f.context)).rejects.toThrow();
      assertUnchanged();
    }
    for (const operation of ["setTopics", "setEvents"] as const) {
      // Deliberately bypass TypeScript's request shape, not the real request channel.
      const invalid = { operation, id: f.actor.id, ...(operation === "setTopics" ? { topics: [42] } : { events: ["not_a_host_event"] }) };
      await expect(proxy.setActor(invalid as unknown as ResidentActorMutation, undefined, caller))
        .rejects.toThrow(operation === "setTopics" ? /Invalid Fabric actor topic/ : /Unsupported Fabric actor event/);
      assertUnchanged();
      const nonArray = { operation, id: f.actor.id, ...(operation === "setTopics" ? { topics: "repair.events" } : { events: "" }) };
      await expect(proxy.setActor(nonArray as unknown as ResidentActorMutation, undefined, caller)).rejects.toThrow(/must be an array/);
      assertUnchanged();
      const valid: ResidentActorMutation = operation === "setTopics"
        ? { operation, id: f.actor.id, topics: ["foreign.changed"] }
        : { operation, id: f.actor.id, events: ["tool_error"] };
      await expect(proxy.setActor(valid, undefined, { ...caller, hostId: "host:foreign" }))
        .rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
      assertUnchanged();
    }
  } finally {
    await f.close();
  }
}, 30_000);

it("keeps public topic and event setters working for a local session actor", async () => {
  const f = await fixture();
  try {
    const local = await f.provider.invoke("createActor", {
      name: "local-subscriptions", instructions: "Observe local subscriptions.", residency: "session",
      model: "fixture/visible", tools: [], topics: ["local.before"], events: ["agent_settled"],
    }, f.context) as { id: string };
    await f.participants.refresh();

    await f.provider.invoke("setTopics", { id: local.id, topics: ["local.after"] }, f.context);
    await f.provider.invoke("setEvents", { id: local.id, events: ["tool_error"] }, f.context);

    expect(f.passive.owns(local.id)).toBe(true);
    expect(f.host.actors.owns(local.id)).toBe(false);
    expect(f.participants.get(local.id)?.ownerHostId).toBe(f.identity.id);
    expect(f.passive.status(local.id)).toMatchObject({ scope: "session", topics: ["local.after"], events: ["tool_error"] });
  } finally {
    await f.close();
  }
}, 30_000);
