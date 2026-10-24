import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  AckPolicy,
  DeliverPolicy,
  JetStreamApiError,
  ReplayPolicy,
  type ConsumerConfig,
  type ConsumerInfo,
} from "@nats-io/jetstream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, type FabricMeshConfig } from "../src/config.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { startNatsActorEventsConsumer } from "../src/transport/nats/events-consume.js";
import { hashToken } from "../src/transport/nats/subjects.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import type { FabricActorInfo } from "../src/actors/types.js";

vi.mock("../src/transport/nats/connection.js", () => ({
  NatsAuthorizationError: class NatsAuthorizationError extends Error {},
  readNatsCredentials: () => { throw new Error("not used in this test"); },
  openBootstrapNatsConnection: async () => { throw new Error("not used in this test"); },
  openNatsConnection: async () => {
    const connection = (globalThis as unknown as Record<string, unknown>).__PI_FABRIC_EVENTS_STARTAT_CONNECTION__;
    if (!connection) throw new Error("NATS events test connection was not configured");
    return connection;
  },
}));

afterEach(() => {
  delete (globalThis as unknown as Record<string, unknown>).__PI_FABRIC_EVENTS_STARTAT_CONNECTION__;
});

const waitFor = async (predicate: () => boolean): Promise<void> => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for NATS durable creation");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};

const natsFixture = () => {
  const added: Array<Partial<ConsumerConfig>> = [];
  const consumersApi = {
    info: async (): Promise<ConsumerInfo> => {
      throw new JetStreamApiError({ code: 404, description: "consumer not found", err_code: 10014 });
    },
    add: async (_stream: string, config: Partial<ConsumerConfig>): Promise<ConsumerInfo> => {
      added.push({ ...config });
      return { config } as ConsumerInfo;
    },
    update: async (_stream: string, _durable: string, config: Partial<ConsumerConfig>): Promise<ConsumerInfo> =>
      ({ config } as ConsumerInfo),
    delete: async (): Promise<boolean> => true,
  };
  const emptyMessages = {
    async *[Symbol.asyncIterator](): AsyncGenerator<never, void, unknown> {},
    closed: async (): Promise<undefined> => undefined,
    close: async (): Promise<void> => {},
  };
  const connection = {
    manager: { consumers: consumersApi },
    js: { consumers: { get: async () => ({ consume: async () => emptyMessages }) } },
    closed: async (): Promise<undefined> => undefined,
    close: async (): Promise<void> => {},
  } as unknown as NatsConnectionHandle;
  return { added, connection };
};

const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-events-startat-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = {
    id: "session:events-startat-test",
    name: "main",
    kind: "main",
    sessionId: "events-startat-test",
  };
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
    vedaBinary: path.resolve("tests/fixtures/fake-veda.mjs"),
    runRoot: path.join(root, "runs"),
    hostId: identity.id,
    rootSessionCwd: () => process.cwd(),
  });
  const meshConfig: FabricMeshConfig = {
    ...DEFAULT_FABRIC_CONFIG.mesh,
    transport: "nats",
    nats: {
      ...DEFAULT_FABRIC_CONFIG.mesh.nats,
      events: true,
      org: "events-startat-test",
      servers: "tls://nats.test",
      credentials: "unused-test-credential",
      machine: "test-machine",
      custodyStream: "FABRIC_CUSTODY",
      livenessStream: "FABRIC_LIVENESS",
      pathBMachines: [],
    },
  };
  const mainAgent = {
    id: identity.id,
    local: true,
    matches: (id: string) => id === "main" || id === identity.id,
    info: () => ({
      id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: process.cwd(), sessionId: identity.sessionId, startedAt: 1, updatedAt: 1,
      pendingMessages: false, local: true,
    }),
    deliverAgent: () => ({ queued: true, messageId: "main-message", routed: "main" }),
    flushHeldAtNextBoundary() {},
  } as unknown as FabricMainAgentTarget;
  const actors = new ActorManager("events-startat-test", identity, mesh, meshConfig, agents, () => {}, {
    actorRoot: path.join(root, "actors"),
    persistent: true,
    mainAgent,
    rootId: identity.id,
    meshRetentionSweepPath: false,
  });
  const self = {
    format: 1,
    id: identity.id,
    kind: "root",
    rootId: identity.id,
    ownerHostId: identity.id,
    ownerIdentityId: identity.id,
    ownerIncarnation: "fixture:owner",
    name: "main",
    status: "idle",
    runner: "pi",
    transport: "host",
    capabilities: ["steer", "followUp", "fabric"],
    cwd: process.cwd(),
    sessionId: identity.sessionId,
    startedAt: 1,
    updatedAt: 1,
    pendingMessages: false,
    controlProtocol: "v1",
    local: true,
    stale: false,
  } as unknown as FabricParticipantInfo;
  const participants: FabricParticipantSource = {
    list: () => [],
    get: () => undefined,
    self: () => self,
    peers: () => [],
    refresh: async () => {},
    scheduleRefresh: () => {},
  };
  let provider!: AgentsProvider;
  const lifecycle = new LifecycleBroker(mesh, identity, participants,
    { enabled: true, pollMs: 20, maxReadEvents: 100 },
    async (subscription, event) => provider.deliverLifecycle(subscription, event));
  const globalActors = new GlobalActorRegistry(root, 64 * 1024);
  provider = new AgentsProvider(agents, actors, globalActors, mainAgent, participants, undefined, lifecycle);
  const modelRegistry = {
    getAvailable: () => [{ provider: "provider", id: "test-model" }],
    find: (provider: string, id: string) => provider === "provider" && id === "test-model"
      ? { provider, id }
      : undefined,
  };
  const context: FabricInvocationContext = {
    cwd: process.cwd(),
    signal: undefined,
    parentToolCallId: "events-startat-test",
    nestedToolCallId: "events-startat-test",
    extensionContext: { modelRegistry } as never,
    update() {},
    activity() {},
  };
  return { root, mesh, actors, provider, context };
};

describe("public agents.create eventsStartAt", () => {
  it("uses the stored start time for a new durable and keeps replay-all as the omission default", async () => {
    const { added, connection } = natsFixture();
    (globalThis as unknown as Record<string, unknown>).__PI_FABRIC_EVENTS_STARTAT_CONNECTION__ = connection;
    const state = fixture();
    try {
      for (const action of ["create", "createActor"] as const) {
        const descriptor = await state.provider.describe(action, state.context);
        const properties = (descriptor?.inputSchema as { properties: Record<string, { description?: string }> }).properties;
        expect(properties.eventsStartAt?.description).toBe(
          "Epoch milliseconds or an ISO-8601 timestamp for the first NATS durable; omitted replays all retained events.",
        );
      }

      const startAtIso = "2024-04-05T06:07:08.901Z";
      const startAt = Date.parse(startAtIso);
      const startedActor = await state.provider.invoke("create", {
        name: "start-at-actor",
        instructions: "Observe events.",
        eventsStartAt: startAtIso,
      }, state.context) as FabricActorInfo;
      expect(startedActor.eventsStartAt).toBe(startAt);
      expect(state.actors.definition(startedActor.id).eventsStartAt).toBe(startAt);
      await waitFor(() => added.length >= 1);

      const epochStartAt = startAt + 1_000;
      const epochActor = await state.provider.invoke("createActor", {
        name: "epoch-start-at-actor",
        instructions: "Observe events.",
        eventsStartAt: epochStartAt,
      }, state.context) as FabricActorInfo;
      expect(epochActor.eventsStartAt).toBe(epochStartAt);
      expect(state.actors.definition(epochActor.id).eventsStartAt).toBe(epochStartAt);
      await waitFor(() => added.length >= 2);

      const defaultActor = await state.provider.invoke("create", {
        name: "replay-all-actor",
        instructions: "Observe events.",
      }, state.context) as FabricActorInfo;
      expect(defaultActor.eventsStartAt).toBeUndefined();
      await waitFor(() => added.length >= 3);

      const startedConfig = added.find(config => config.durable_name === `ev_${hashToken(startedActor.id)}`);
      expect(startedConfig).toMatchObject({
        deliver_policy: DeliverPolicy.StartTime,
        opt_start_time: new Date(startAt).toISOString(),
      });
      const epochConfig = added.find(config => config.durable_name === `ev_${hashToken(epochActor.id)}`);
      expect(epochConfig).toMatchObject({
        deliver_policy: DeliverPolicy.StartTime,
        opt_start_time: new Date(epochStartAt).toISOString(),
      });
      const defaultConfig = added.find(config => config.durable_name === `ev_${hashToken(defaultActor.id)}`);
      expect(defaultConfig).toMatchObject({ deliver_policy: DeliverPolicy.All });
      expect(defaultConfig).not.toHaveProperty("opt_start_time");
    } finally {
      await state.provider.close();
      state.mesh.closeState();
      fs.rmSync(state.root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  });
});
