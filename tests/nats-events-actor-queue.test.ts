import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager, NATS_EVENT_RECEIPT_MEMORY, rememberNatsEventReceipt } from "../src/actors/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { diagnostics } from "../src/core/diagnostics.js";
import type { NatsActorEventsConsumer, NatsActorEventsConsumerOptions } from "../src/transport/nats/events-consume.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";

const mocks = vi.hoisted(() => ({
  onMessage: undefined as unknown,
  startConsumer: vi.fn<(connection: unknown, options: NatsActorEventsConsumerOptions) => Promise<NatsActorEventsConsumer>>(),
  connection: { close: async () => {}, closed: () => new Promise<void | Error>(() => {}) },
}));
vi.mock("../src/transport/nats/connection.js", () => ({
  openNatsConnection: async () => mocks.connection,
}));
vi.mock("../src/transport/nats/events-consume.js", () => ({
  startNatsActorEventsConsumer: async (connection: unknown, options: NatsActorEventsConsumerOptions) => {
    mocks.onMessage = options.onMessage;
    return mocks.startConsumer(connection, options) ?? { updateTopics: async () => {}, close: async () => {} };
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
  vi.useRealTimers();
  await Promise.all(close.splice(0).map(fn => fn()));
  fs.rmSync(root, { recursive: true, force: true });
  mocks.onMessage = undefined;
  mocks.startConsumer.mockReset();
  vi.restoreAllMocks();
});

const fakeConsumer = () => ({
  updateTopics: vi.fn(async (_topics: readonly string[], _active?: boolean) => {}),
  close: vi.fn(async (_remove?: boolean) => {}),
});
const setupError = (durable: string, code = "timeout", retryable = true) => Object.assign(
  new Error("server reply containing tls://private.example and credentials must not be logged"),
  { setup: { stream: "FABRIC_EV_test_org", durable, code, retryable } },
);

// Failure-path integration: a fake transport drives the real manager's lifecycle,
// not a second implementation of the setup scheduler. No actors launch inference.
const setupManager = async (count: number, canManageActor: () => boolean = () => true) => {
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const meshConfig = {
    ...DEFAULT_FABRIC_CONFIG.mesh,
    enabled: true,
    transport: "nats" as const,
    nats: {
      ...DEFAULT_FABRIC_CONFIG.mesh.nats,
      events: false,
      org: "test-org", servers: ["tls://localhost:4222"], credentials: "/unused/user.creds", machine: "ryzen2",
      custodyStream: "FABRIC_CUSTODY", livenessStream: "FABRIC_LIVENESS", pathBMachines: ["ryzen2"],
    },
  };
  const actors = new ActorManager("nats-events-test", identity, mesh, meshConfig, agents, () => {}, {
    actorRoot, persistent: true, canManageActor, canConsumeMesh: () => false,
  });
  close.push(async () => { await actors.close(); await agents.close(); });
  const created: Array<Awaited<ReturnType<ActorManager["create"]>>> = [];
  for (let index = 0; index < count; index++) {
    created.push(await actors.create({ name: `mail-${index}`, instructions: "Wait for mail.", topics: ["mail.inbound"],
      residency: "durable", extensions: false }));
  }
  meshConfig.nats.events = true;
  return { actors, created, mesh, start: () => actors.setTopics(created[0]!.id, ["mail.inbound"]) };
};

describe("NATS actor event queue admission", () => {
  it("finishes 35 of 40 setups while five timeouts are pending, then retries only those five independently", async () => {
    const attempts = new Map<string, number>();
    const consumers = new Map<string, ReturnType<typeof fakeConsumer>>();
    const pending = new Map<string, (error: Error) => void>();
    let inFlight = 0;
    let peak = 0;
    mocks.startConsumer.mockImplementation(async (_connection, options) => {
      inFlight++;
      peak = Math.max(peak, inFlight);
      const attempt = (attempts.get(options.actorId) ?? 0) + 1;
      attempts.set(options.actorId, attempt);
      try {
        if (attempt === 1 && pending.size < 5 && attempts.size <= 5) {
          await new Promise<never>((_resolve, reject) => { pending.set(options.actorId, reject); });
        }
        await Promise.resolve();
        const consumer = fakeConsumer();
        consumers.set(options.actorId, consumer);
        return consumer;
      } finally { inFlight--; }
    });
    const { actors, created, start } = await setupManager(40);
    const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    await start();
    await waitFor(() => pending.size === 5 && consumers.size === 35);
    expect(attempts.size).toBe(40);
    expect(inFlight).toBe(5);
    expect(peak).toBe(8);
    expect(warnings).not.toHaveBeenCalled();

    // A healthy consumer is already published, even while the first five setups
    // are unresolved. Topic maintenance must not wait for the slow requests.
    const healthyId = created.find(actor => !pending.has(actor.id))!.id;
    await actors.setTopics(healthyId, ["mail.updated"]);
    expect(consumers.get(healthyId)!.updateTopics).toHaveBeenCalledWith(["mail.updated"], true);
    const consumerOptions = mocks.startConsumer.mock.calls.find(([, options]) => options.actorId === healthyId)?.[1];
    expect(consumerOptions?.directoryReady?.()).toBe(false);
    expect(consumerOptions?.onDirectoryStall).toBeTypeOf("function");

    vi.useFakeTimers();
    const jitter = vi.spyOn(Math, "random");
    for (const value of [0, 0.25, 0.5, 0.75, 1 - Number.EPSILON]) jitter.mockReturnValueOnce(value);
    jitter.mockReturnValue(0);
    const failedIds = [...pending.keys()];
    for (const [index, actorId] of failedIds.entries()) pending.get(actorId)!(setupError(`ev_timeout_${index}`));
    await vi.advanceTimersByTimeAsync(0);
    expect(warnings).toHaveBeenCalledTimes(5);
    for (let index = 0; index < 5; index++) {
      expect(warnings).toHaveBeenNthCalledWith(index + 1, "src/actors/manager.ts", "[pi-fabric] NATS actor event setup failed", {
        stream: "FABRIC_EV_test_org", durable: `ev_timeout_${index}`, code: "timeout", attempt: 1,
      });
    }
    expect(JSON.stringify(warnings.mock.calls)).not.toContain("private.example");
    await vi.advanceTimersByTimeAsync(999);
    expect(consumers.size).toBe(35);
    await vi.advanceTimersByTimeAsync(1);
    expect(consumers.size).toBe(36);
    expect(attempts.get(failedIds[0]!)).toBe(2);
    expect(failedIds.slice(1).map(id => attempts.get(id))).toEqual([1, 1, 1, 1]);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(consumers.size).toBe(40);
    expect(inFlight).toBe(0);
    expect(peak).toBeLessThanOrEqual(8);
    expect(failedIds.map(id => attempts.get(id))).toEqual([2, 2, 2, 2, 2]);
    await vi.advanceTimersByTimeAsync(90_000);
    expect([...attempts].filter(([id]) => !pending.has(id)).map(([, count]) => count)).toEqual(Array(35).fill(1));
    expect(warnings).toHaveBeenCalledTimes(5);
    vi.useRealTimers();
    await actors.stop(healthyId);
    await actors.setTopics(healthyId, []);
    expect(consumers.get(healthyId)!.updateTopics).toHaveBeenLastCalledWith([], false);
    await actors.remove(healthyId);
    expect(consumers.get(healthyId)!.close).toHaveBeenCalledExactlyOnceWith(true);
  }, 30_000);

  it("publishes the directory-stall alarm through the actor activation alarm path", async () => {
    let options: NatsActorEventsConsumerOptions | undefined;
    mocks.startConsumer.mockImplementation(async (_connection, received) => {
      options = received;
      return fakeConsumer();
    });
    const { actors, created, mesh, start } = await setupManager(1);
    const publish = vi.spyOn(mesh, "publish");
    await start();
    await waitFor(() => options !== undefined);
    await options!.onDirectoryStall?.({ stream: "FABRIC_EV_TEST_ORG", durable: "ev_actor", pendingCount: 2, since: 123 });
    expect(publish).toHaveBeenCalledWith(expect.objectContaining({
      topic: "fabric.alarm.actor-activation", kind: "actor-events-directory-not-ready", to: created[0]!.rootId,
      data: expect.objectContaining({ actorId: created[0]!.id, stream: "FABRIC_EV_TEST_ORG", durable: "ev_actor", pendingCount: 2, since: 123 }),
    }));
    await actors.close();
  });

  it("keeps retrying transient failures with per-actor attempts and a 30 second jitter cap until success", async () => {
    let rejectFirst!: (error: Error) => void;
    let attempts = 0;
    const consumer = fakeConsumer();
    mocks.startConsumer.mockImplementation(async () => {
      attempts++;
      if (attempts === 1) await new Promise<never>((_resolve, reject) => { rejectFirst = reject; });
      if (attempts <= 8) throw setupError("ev_repeated");
      return consumer;
    });
    const { actors, created, start } = await setupManager(1);
    const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    await start();
    await waitFor(() => !!rejectFirst);
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(1 - Number.EPSILON);
    rejectFirst(setupError("ev_repeated"));
    await vi.advanceTimersByTimeAsync(0);
    for (const [index, delay] of [2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000, 30_000].entries()) {
      expect(attempts).toBe(index + 1);
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(attempts).toBe(index + 1);
      await vi.advanceTimersByTimeAsync(1);
      expect(attempts).toBe(index + 2);
    }
    expect(warnings).toHaveBeenCalledTimes(8);
    expect(warnings.mock.calls.map(call => (call[2] as { attempt: number }).attempt)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    await vi.advanceTimersByTimeAsync(90_000);
    expect(attempts).toBe(9);
    vi.useRealTimers();
    await actors.setTopics(created[0]!.id, ["mail.recovered"]);
    expect(consumer.updateTopics).toHaveBeenCalledWith(["mail.recovered"], true);
  });

  it.each(["authorization", "configuration_mismatch"])("remembers terminal %s failures without retrying on timers or later syncs", async code => {
    mocks.startConsumer.mockRejectedValue(setupError("ev_terminal", code, false));
    const { actors, created, start } = await setupManager(1);
    const warnings = vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    await start();
    await waitFor(() => warnings.mock.calls.length === 1);
    vi.useFakeTimers();
    await vi.advanceTimersByTimeAsync(300_000);
    vi.useRealTimers();
    await actors.setTopics(created[0]!.id, ["mail.changed"]);
    await actors.setTopics(created[0]!.id, []);
    expect(mocks.startConsumer).toHaveBeenCalledTimes(1);
    expect(warnings).toHaveBeenCalledExactlyOnceWith("src/actors/manager.ts", "[pi-fabric] NATS actor event setup failed", {
      stream: "FABRIC_EV_test_org", durable: "ev_terminal", code, attempt: 1,
    });
  });

  it.each(["topics", "ownership", "stop", "remove"] as const)("rechecks %s changes made while setup is pending", async change => {
    const ownership = { owned: true };
    let finish!: (consumer: NatsActorEventsConsumer) => void;
    mocks.startConsumer.mockImplementation(async () => new Promise<NatsActorEventsConsumer>(resolve => { finish = resolve; }));
    const { actors, created, start } = await setupManager(1, () => ownership.owned);
    await start();
    await waitFor(() => !!finish);
    const actorId = created[0]!.id;
    if (change === "topics") await actors.setTopics(actorId, ["mail.changed-during-setup"]);
    if (change === "ownership") ownership.owned = false;
    if (change === "stop") await actors.stop(actorId);
    if (change === "remove") await actors.remove(actorId);
    const late = fakeConsumer();
    finish(late);
    if (change === "topics") {
      await waitFor(() => late.updateTopics.mock.calls.length > 0);
      expect(late.updateTopics).toHaveBeenCalledWith(["mail.changed-during-setup"], true);
      expect(late.close).not.toHaveBeenCalled();
    } else {
      await waitFor(() => late.close.mock.calls.length > 0);
      expect(late.close).toHaveBeenCalledExactlyOnceWith(change === "remove");
    }
    await actors.close();
    expect(mocks.startConsumer).toHaveBeenCalledTimes(1);
    expect(late.close).toHaveBeenCalledTimes(1);
  });

  it("joins pending setup on session close, closes its late result, and cancels retry timers", async () => {
    let rejectFirst!: (error: Error) => void;
    let finishSecond!: (consumer: NatsActorEventsConsumer) => void;
    mocks.startConsumer.mockImplementation(async () => {
      if (mocks.startConsumer.mock.calls.length === 1) {
        return new Promise<never>((_resolve, reject) => { rejectFirst = reject; });
      }
      return new Promise<NatsActorEventsConsumer>(resolve => { finishSecond = resolve; });
    });
    const { actors, start } = await setupManager(2);
    vi.spyOn(diagnostics, "warn").mockImplementation(() => {});
    await start();
    await waitFor(() => !!rejectFirst && !!finishSecond);
    vi.useFakeTimers();
    rejectFirst(setupError("ev_close_retry"));
    await vi.advanceTimersByTimeAsync(0);
    let closed = false;
    const closing = actors.close().then(() => { closed = true; });
    await vi.advanceTimersByTimeAsync(0);
    expect(closed).toBe(false);
    const late = fakeConsumer();
    finishSecond(late);
    vi.useRealTimers();
    await closing;
    expect(late.close).toHaveBeenCalledExactlyOnceWith(false);
    vi.useFakeTimers();
    await vi.advanceTimersByTimeAsync(90_000);
    expect(mocks.startConsumer).toHaveBeenCalledTimes(2);
  });

  it("bounds the durable receipt history with LRU ordering", () => {
    const receipts = new Set(["oldest", "recent"]);
    for (let index = 0; index < NATS_EVENT_RECEIPT_MEMORY - 2; index++) {
      rememberNatsEventReceipt(receipts, `receipt-${index}`);
    }
    rememberNatsEventReceipt(receipts, "recent");
    const evicted = rememberNatsEventReceipt(receipts, "newest");
    expect(receipts.size).toBe(NATS_EVENT_RECEIPT_MEMORY);
    expect(receipts.has("oldest")).toBe(false);
    expect(receipts.has("recent")).toBe(true);
    expect(evicted).toEqual(["oldest"]);
  });

  it("persists the Nats-Msg-Id as data.deliveryId with the queue receipt before ACK", async () => {
    const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
    });
    const ownership = { owned: true };
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
      actorRoot, persistent: true, canManageActor: () => ownership.owned,
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
    const onMessage = mocks.onMessage as (event: MeshEvent, messageId: string, ack: () => Promise<void>) => Promise<"accepted" | "retry">;
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

    let staleAcked = false;
    const staleOutcome = await onIdleMessage({ ...idleWake, id: "stale-topic", topic: "mail.not-subscribed" }, "nats-stale-topic", async () => {
      staleAcked = true;
    });
    expect(staleOutcome).toBe("retry");
    expect(staleAcked).toBe(false);

    ownership.owned = false;
    let lostOwnerAcked = false;
    const lostOwnerOutcome = await onIdleMessage(idleWake, "nats-lost-owner", async () => {
      lostOwnerAcked = true;
    });
    expect(lostOwnerOutcome).toBe("retry");
    expect(lostOwnerAcked).toBe(false);
  });
});
