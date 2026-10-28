import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import type { FabricActorHealth, FabricActorInfo, FabricActorReadInfo } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { MIRROR_COLLISION_TOPIC, ParticipantDirectory } from "../src/topology/participant-directory.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

const UNKNOWN_HEALTH: FabricActorHealth = { state: "unknown", lastProgressAt: null };
const fixtures: Array<{ close(): Promise<void> }> = [];

const makeFixture = async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-health-"));
  const identity: MeshIdentity = { id: "session:actor-health-test", name: "main", kind: "main", sessionId: "actor-health-test" };
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const actorRoot = path.join(root, "actors");
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  let agents: AgentManager | undefined;
  let ownerActors: ActorManager | undefined;
  let passiveActors: ActorManager | undefined;
  let participants: ParticipantDirectory | undefined;
  let lifecycle: LifecycleBroker | undefined;
  let closed = false;

  const close = async (): Promise<void> => {
    if (closed) return;
    closed = true;
    for (const resource of [ownerActors, passiveActors]) {
      if (resource) await resource.close().catch(() => undefined);
    }
    if (lifecycle) await lifecycle.close().catch(() => undefined);
    if (participants) await participants.close().catch(() => undefined);
    if (agents) await agents.close().catch(() => undefined);
    try {
      mesh.closeState();
    } finally {
      // Every fixture write is rooted at this mkdtemp directory; never touch a worktree or Git state.
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  };

  try {
    agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
      claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
      vedaBinary: path.resolve("tests/fixtures/fake-veda.mjs"),
      runRoot: path.join(root, "runs"),
      hostId: identity.id,
      rootSessionCwd: () => root,
    });
    const mainAgent: FabricMainAgentTarget = {
      id: identity.id,
      local: true,
      matches: id => id === "main" || id === identity.id,
      info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
        cwd: root, sessionId: identity.sessionId!, startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
      deliverAgent: () => ({ queued: true, messageId: "health-fixture", routed: "main" }),
      flushHeldAtNextBoundary() {},
    };

    ownerActors = new ActorManager("actor-health-test", identity, mesh, meshConfig, agents, () => {}, {
      actorRoot, persistent: true, mainAgent, rootId: identity.id,
    });
    const created = await ownerActors.create({ name: "health-owner", instructions: "Observe actor health." });
    await ownerActors.cede(created.id);
    await ownerActors.close();

    passiveActors = new ActorManager("actor-health-test", identity, mesh, meshConfig, agents, () => {}, {
      actorRoot, persistent: true, rootId: identity.id, canManageActor: () => false,
    });
    const actor = passiveActors.status(created.id);
    participants = new ParticipantDirectory(mesh, {
      enabled: false, hostId: identity.id, rootId: identity.id, identity,
    });
    const now = Date.now();
    const ownerHealth = {
      state: "healthy" as const,
      lastProgressAt: now - 250,
      privateDiagnostic: "owner-only diagnostic",
    } as FabricActorHealth;
    const owner: FabricParticipantInfo = {
      format: 1,
      id: actor.id,
      kind: "actor",
      rootId: identity.id,
      ownerHostId: "resident:health-owner",
      ownerIdentityId: "resident:health-owner",
      ownerIncarnation: "health-owner-incarnation",
      parentId: identity.id,
      name: actor.name,
      status: "running",
      residency: "session",
      runner: "pi",
      transport: "host",
      capabilities: [],
      startedAt: actor.createdAt,
      updatedAt: now,
      actorHealth: ownerHealth,
      controlProtocol: "v1",
      local: false,
      stale: false,
    };
    const cached: FabricParticipantInfo = {
      ...owner,
      actorHealth: { state: "stale", lastProgressAt: now - 60_000 },
      updatedAt: now - 60_000,
    };
    let live: FabricParticipantInfo | undefined = owner;
    const get = vi.spyOn(participants, "get").mockImplementation((id, _at, options) =>
      id === actor.id ? options?.fresh ? live : cached : undefined,
    );
    const globalActors = new GlobalActorRegistry(root, 64 * 1024);
    let provider!: AgentsProvider;
    lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 },
      async (subscription, event) => provider.deliverLifecycle(subscription, event));
    provider = new AgentsProvider(agents, passiveActors, globalActors, mainAgent, participants, undefined, lifecycle);
    const context: FabricInvocationContext = {
      cwd: root,
      signal: undefined,
      parentToolCallId: "actor-health-test",
      nestedToolCallId: "actor-health-test",
      extensionContext: {} as ExtensionContext,
      update() {},
      activity() {},
    };
    const fixture = {
      actor,
      context,
      get,
      owner,
      passiveActors,
      participants,
      mesh,
      provider,
      setOwner: (value: FabricParticipantInfo | undefined) => { live = value; },
      close,
    };
    fixtures.push(fixture);
    return fixture;
  } catch (error) {
    await close();
    throw error;
  }
};

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(fixtures.splice(0).map(fixture => fixture.close()));
});

const readView = async (state: Awaited<ReturnType<typeof makeFixture>>, action: "actorStatus" | "actors") => {
  const result = action === "actorStatus"
    ? await state.provider.invoke(action, { id: state.actor.id }, state.context) as FabricActorReadInfo
    : (await state.provider.invoke(action, {}, state.context) as FabricActorReadInfo[]).find(actor => actor.id === state.actor.id)!;
  expect(state.get).toHaveBeenCalledWith(state.actor.id, undefined, { fresh: true });
  return result;
};

describe("actor health provider", () => {
  it("returns a compact detached health value from the fresh foreign owner", async () => {
    const state = await makeFixture();
    const expected: FabricActorHealth = { state: "healthy", lastProgressAt: state.owner.actorHealth!.lastProgressAt };

    state.get.mockClear();
    const result = await state.provider.invoke("actorHealth", { id: state.actor.id }, state.context);
    expect(result).toEqual(expected);
    expect(result).not.toBe(state.owner.actorHealth);
    expect(state.get).toHaveBeenCalledWith(state.actor.id, undefined, { fresh: true });

    state.get.mockClear();
    const status = await state.provider.invoke("actorStatus", { id: state.actor.id }, state.context) as FabricActorReadInfo;
    expect(status.health).toEqual(expected);
    expect(status.health).not.toBe(state.owner.actorHealth);
    expect(state.get).toHaveBeenCalledWith(state.actor.id, undefined, { fresh: true });
  });

  it("projects unknown for unavailable, stale, wrong-kind, legacy, or old-version owners without registry leakage", async () => {
    const state = await makeFixture();
    const registryHealth: FabricActorHealth = { state: "healthy", lastProgressAt: Date.now() - 60_000 };
    const registryActor = { ...state.actor, health: registryHealth } as FabricActorInfo;
    const status = vi.spyOn(state.passiveActors, "status").mockReturnValue(registryActor);
    const list = vi.spyOn(state.passiveActors, "list").mockReturnValue([registryActor]);
    const { actorHealth: _signal, ...oldVersion } = state.owner;
    const owners: Array<FabricParticipantInfo | undefined> = [
      undefined,
      { ...state.owner, stale: true },
      { ...state.owner, kind: "agent" },
      { ...state.owner, controlProtocol: "legacy" },
      oldVersion,
    ];

    try {
      for (const owner of owners) {
        state.setOwner(owner);
        expect((await readView(state, "actorStatus")).health).toEqual(UNKNOWN_HEALTH);
        expect((await readView(state, "actors")).health).toEqual(UNKNOWN_HEALTH);
        await expect(state.provider.invoke("actorHealth", { id: state.actor.id }, state.context)).resolves.toEqual(UNKNOWN_HEALTH);
      }
    } finally {
      status.mockRestore();
      list.mockRestore();
    }
  });

  it("classifies malformed directory evidence without writing refusal events or consuming their later notification", async () => {
    const state = await makeFixture();
    // Enable real directory reads after installing malformed remote evidence.
    // No status/ownership/provider methods are mocked for this regression.
    state.get.mockRestore();
    state.participants.options.enabled = true;
    await state.mesh.put({ key: "topology/participants/malformed-health-owner",
      identity: { id: "session:malformed-health-owner", name: "mirror", kind: "main" },
      value: { id: "malformed-health-owner", remoteHost: "example", sessionId: 42 } });
    const before = state.mesh.tail(0, 100).events;
    const publish = vi.spyOn(state.mesh, "publish");
    await expect(state.provider.invoke("actorHealth", { id: "missing-health-actor" }, state.context)).resolves.toEqual(UNKNOWN_HEALTH);
    expect(publish).not.toHaveBeenCalled();
    expect(state.mesh.tail(0, 100).events).toEqual(before);
    // Observation did not mark the refusal as already notified. Its existing
    // ordinary directory path still reports it later, once, as before.
    state.participants.list({ fresh: true });
    await expect.poll(() => state.mesh.tail(0, 100).events.filter(event => event.topic === MIRROR_COLLISION_TOPIC).length,
      { timeout: 5_000, interval: 20 }).toBe(1);
  });

  it("does not use retained health from a stale bridge publication", async () => {
    const state = await makeFixture();
    state.setOwner({ ...state.owner, bridgeStale: true });
    await expect(state.provider.invoke("actorHealth", { id: state.actor.id }, state.context)).resolves.toEqual(UNKNOWN_HEALTH);
  });

  it("returns unknown for a missing actor while actorStatus keeps throwing", async () => {
    const state = await makeFixture();
    await expect(state.provider.invoke("actorHealth", { id: "missing-health-actor" }, state.context))
      .resolves.toEqual(UNKNOWN_HEALTH);
    await expect(state.provider.invoke("actorStatus", { id: "missing-health-actor" }, state.context))
      .rejects.toThrow(/Unknown Fabric (?:actor|participant):/);
  });

  it("propagates cancellation and programming errors, but maps bridge availability to unknown", async () => {
    const state = await makeFixture();
    const controller = new AbortController();
    const cancellation = new Error("actor health read cancelled");
    controller.abort(cancellation);
    await expect(state.provider.invoke("actorHealth", { id: state.actor.id }, { ...state.context, signal: controller.signal }))
      .rejects.toBe(cancellation);

    const programmingError = new TypeError("fixture programming failure");
    state.get.mockImplementationOnce(() => { throw programmingError; });
    await expect(state.provider.invoke("actorHealth", { id: state.actor.id }, state.context)).rejects.toBe(programmingError);

    const bridgeUnavailable = Object.assign(new Error("owner bridge is stale"), { code: "FABRIC_BRIDGE_STALE" });
    state.get.mockImplementationOnce(() => { throw bridgeUnavailable; });
    await expect(state.provider.invoke("actorHealth", { id: state.actor.id }, state.context)).resolves.toEqual(UNKNOWN_HEALTH);
  });
});
