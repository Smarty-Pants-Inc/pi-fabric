import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

const mocks = vi.hoisted(() => ({
  onMessage: undefined as unknown,
  connection: { close: async () => {}, closed: () => new Promise<void | Error>(() => {}) },
}));
vi.mock("../src/transport/nats/connection.js", () => ({
  openNatsConnection: async () => mocks.connection,
}));
vi.mock("../src/transport/nats/events-consume.js", () => ({
  startNatsActorEventsConsumer: async (_connection: unknown, options: { onMessage: unknown }) => {
    mocks.onMessage = options.onMessage;
    return { updateTopics: async () => {}, close: async () => {} };
  },
}));

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-nats-event-queue-"));
const meshRoot = path.join(root, "mesh");
const actorRoot = path.join(root, "actors");
const identity: MeshIdentity = { id: "session:nats-events-test", name: "nats-events-test", kind: "main", sessionId: "nats-events-test" };
const waitFor = async (predicate: () => boolean): Promise<void> => {
  const deadline = Date.now() + 10_000;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for NATS actor queue acceptance");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
};
const queueSnapshot = (actorId: string): { items: Array<{ payload: MeshEvent }>; natsEventReceipts?: string[] } => {
  const directory = path.join(actorRoot, actorId);
  const file = fs.readdirSync(directory).find(name => /^queue-.+\.json$/.test(name));
  if (!file) throw new Error("durable actor queue snapshot was not written");
  return JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")) as { items: Array<{ payload: MeshEvent }>; natsEventReceipts?: string[] };
};

const close: Array<() => Promise<void>> = [];
afterEach(async () => {
  await Promise.all(close.splice(0).map(fn => fn()));
  fs.rmSync(root, { recursive: true, force: true });
  mocks.onMessage = undefined;
});

describe("NATS actor event queue admission", () => {
  it("persists the Nats-Msg-Id as data.deliveryId with the queue receipt before ACK", async () => {
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const meshConfig = {
      ...DEFAULT_FABRIC_CONFIG.mesh,
      transport: "nats" as const,
      nats: {
        ...DEFAULT_FABRIC_CONFIG.mesh.nats,
        events: true,
        org: "test-org",
        servers: ["tls://localhost:4222"],
        credentials: "/unused/user.creds",
        machine: "ryzen2",
        custodyStream: "FABRIC_CUSTODY",
        livenessStream: "FABRIC_LIVENESS",
        pathBMachines: ["ryzen2"],
      },
    };
    const actors = new ActorManager("nats-events-test", identity, mesh, meshConfig, agents, () => {}, {
      actorRoot, persistent: true,
    });
    close.push(async () => { await actors.close(); await agents.close(); });
    const actor = await actors.create({ name: "mail-reviewer", instructions: "Review mail events.", topics: ["mail.inbound"],
      coalesce: false, residency: "durable", extensions: false });
    await waitFor(() => typeof mocks.onMessage === "function");

    const event: MeshEvent = {
      id: "mail-event-1", sequence: 7, topic: "mail.inbound", kind: "message",
      from: { id: "mail-service", name: "mail-service", kind: "agent" }, data: { message: "inbound" }, createdAt: Date.now(),
    };
    let resolveAck!: () => void;
    const ackPending = new Promise<void>(resolve => { resolveAck = resolve; });
    const onMessage = mocks.onMessage as (event: MeshEvent, messageId: string, ack: () => Promise<void>) => Promise<void>;
    const accepting = onMessage(event, "nats-msg-mail-1", () => ackPending);
    await waitFor(() => {
      try { return queueSnapshot(actor.id).items.length === 1; }
      catch { return false; }
    });

    let resolveWakeAck!: () => void;
    const wakeAckPending = new Promise<void>(resolve => { resolveWakeAck = resolve; });
    const wakeEvent: MeshEvent = { ...event, id: "wake-event-1", sequence: 8, topic: `wake.${actor.id}`, data: { wake: true } };
    const acceptingWake = onMessage(wakeEvent, "nats-msg-wake-1", () => wakeAckPending);
    await waitFor(() => queueSnapshot(actor.id).items.length === 2);

    const saved = queueSnapshot(actor.id);
    expect(saved.items[0]?.payload.data).toMatchObject({ message: "inbound", deliveryId: "nats-msg-mail-1" });
    expect(saved.items[1]?.payload.data).toMatchObject({ wake: true, deliveryId: "nats-msg-wake-1" });
    expect(saved.natsEventReceipts).toEqual(expect.arrayContaining(["nats-msg-mail-1", "nats-msg-wake-1"]));
    resolveAck();
    resolveWakeAck();
    await Promise.all([accepting, acceptingWake]);

    const previousCallback = mocks.onMessage;
    const idleActor = await actors.create({ name: "mail-idle", instructions: "Wait for a targeted wake.", topics: [],
      coalesce: false, residency: "durable", extensions: false });
    await waitFor(() => typeof mocks.onMessage === "function" && mocks.onMessage !== previousCallback);
    const onIdleMessage = mocks.onMessage as typeof onMessage;
    let resolveIdleAck!: () => void;
    const idleAckPending = new Promise<void>(resolve => { resolveIdleAck = resolve; });
    const idleWake = { ...event, id: "idle-wake-1", sequence: 9, topic: `wake.${idleActor.id}`, data: { wake: true } };
    const acceptingIdleWake = onIdleMessage(idleWake, "nats-msg-idle-wake", () => idleAckPending);
    await waitFor(() => queueSnapshot(idleActor.id).items.length === 1);
    expect(queueSnapshot(idleActor.id).items[0]?.payload.data).toMatchObject({ wake: true, deliveryId: "nats-msg-idle-wake" });
    resolveIdleAck();
    await acceptingIdleWake;
  });
});
