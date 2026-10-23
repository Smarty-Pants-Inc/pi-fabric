import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { AckPolicy, DeliverPolicy, ReplayPolicy, type ConsumerConfig, type ConsumerMessages } from "@nats-io/jetstream";
import { normalizeFabricConfig } from "../src/config.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { openNatsTransport, type NatsTransportOptions } from "../src/transport/nats/inbox.js";
import { ensureTransportConsumer, type TransportConsumerPolicy } from "../src/transport/nats/provision.js";
import { transportDurableName, transportInboxFilter, transportInboxStream, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";

// Keep reason: data safety for legacy state migration, and the real consumer-growth incident #16203.
const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const stream = transportInboxStream("machine-a");
const policy = (hostId: string, inactiveThresholdNs?: number): TransportConsumerPolicy => ({
  stream, durable: transportDurableName(hostId), filterSubject: transportInboxFilter("machine-a", hostId),
  description: JSON.stringify({ fabricHostId: hostId, machine: "machine-a" }), maxAckPending: 128,
  ...(inactiveThresholdNs === undefined ? {} : { inactiveThresholdNs }),
});
const legacyConfig = (hostId: string): ConsumerConfig => ({
  durable_name: transportDurableName(hostId),
  description: JSON.stringify({ fabricHostId: hostId, machine: "machine-a" }),
  filter_subject: transportInboxFilter("machine-a", hostId),
  ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
  ack_wait: 30_000_000_000, max_deliver: 5, backoff: Array.from({ length: 5 }, () => 30_000_000_000),
  max_ack_pending: 128, max_waiting: 1, inactive_threshold: 0,
} as ConsumerConfig);

const provision = async (fixture: PathBHarness): Promise<void> => {
  for (const [name, value] of Object.entries(fixture.adminEnv(fixture.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", fixture.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
};

let fixture: PathBHarness;
const runtimes: Array<{ close(): Promise<void> }> = [];
beforeAll(async () => {
  if (!binary) return;
  fixture = await createPathBHarness({ binary, tls: {} });
  await provision(fixture);
}, 30_000);
afterEach(() => vi.unstubAllEnvs());
afterAll(async () => {
  try { await Promise.all(runtimes.map(runtime => runtime.close())); }
  finally { await fixture?.stop(); }
});

describe.skipIf(!binary).sequential("real NATS inbox durable inactivity", () => {
  it.each([
    { route: "direct", settings: {}, expectedMs: 604_800_000 },
    { route: "direct", settings: { inboxConsumerInactiveMs: 120_000 }, expectedMs: 120_000 },
    { route: "direct", settings: { inboxConsumerInactiveMs: 7_776_000_000 }, expectedMs: 7_776_000_000 },
    { route: "factory", settings: {}, expectedMs: 604_800_000 },
    { route: "factory", settings: { inboxConsumerInactiveMs: 60_000 }, expectedMs: 60_000 },
    { route: "direct", settings: { ephemeralHost: true, ephemeralInactiveSeconds: 300 }, expectedMs: 300_000 },
    { route: "factory", settings: { ephemeralHost: true, ephemeralInactiveSeconds: 300, inboxConsumerInactiveMs: 60_000 }, expectedMs: 60_000 },
  ])("applies $route policy $settings as $expectedMs milliseconds", async ({ route, settings, expectedMs }) => {
    const hostId = `expiry-${route}-${expectedMs}-${settings.ephemeralHost === true}`;
    const options = { ...fixture.runtimeOptions(fixture.users.machineA), ...settings };
    let runtime: Awaited<ReturnType<typeof openNatsTransport>>;
    if (route === "factory") {
      vi.stubEnv("PI_FABRIC_MESH_TRANSPORT", "nats");
      vi.stubEnv("PI_FABRIC_MESH_HOST_TRANSPORT", "nats");
      vi.stubEnv("PI_FABRIC_MESH_NATS", JSON.stringify({ ...options, operatorMachine: "fabric-ops" }));
      runtime = await openNatsTransport({ config: normalizeFabricConfig({}).mesh, hostId, mesh: {} as never });
    } else runtime = await openNatsTransport({ ...options, hostId });
    runtimes.push(runtime);
    const nc = await fixture.connect(fixture.users.bootstrap);
    try {
      const { manager } = await openJetStream(nc);
      expect((await manager.consumers.info(stream, transportDurableName(hostId))).config.inactive_threshold).toBe(expectedMs * 1_000_000);
      // First pull re-verifies the same threshold rather than trying to restore the former zero policy.
      const abort = new AbortController();
      const iterator = runtime.inbox(abort.signal)[Symbol.asyncIterator]();
      const reading = iterator.next();
      try {
        await expect.poll(async () => (await manager.consumers.info(stream, transportDurableName(hostId))).num_waiting,
          { timeout: 5_000, interval: 25 }).toBe(1);
      } finally {
        abort.abort();
        await reading;
        await iterator.return?.();
      }
    } finally { await nc.close(); }
  }, 15_000);

  it.each([0, -1, 59_999, 7_776_000_001, 60_000.5, NaN, Infinity, "60000", null, true])("refuses invalid direct inactivity %s before connecting", async value => {
    const options = { servers: "tls://not-contacted.invalid:4222", machine: "machine-a", credentials: "/not-read/runtime.creds",
      hostId: "invalid-inactivity", inboxConsumerInactiveMs: value } as unknown as NatsTransportOptions;
    await expect(openNatsTransport(options)).rejects.toThrow("inboxConsumerInactiveMs");
  });

  it("migrates legacy zero in place through runtime open, retaining pending messages and ACK state", async () => {
    const nc = await fixture.connect(fixture.users.bootstrap);
    try {
      const { manager, js } = await openJetStream(nc);
      const hostId = "expiry-legacy-pending";
      const beforeCreate = await manager.consumers.add(stream, legacyConfig(hostId));
      const subject = transportInboxSubject("machine-a", hostId, "cmd", "migration", "machine-a");
      for (let i = 0; i < 3; i += 1) await js.publish(subject, new TextEncoder().encode(`message-${i}`));
      const consumer = await js.consumers.get(stream, beforeCreate.name);
      const first = await consumer.next({ expires: 1_000 });
      if (!first) throw new Error("missing first legacy message");
      expect(await first.ackAck()).toBe(true);
      const second = await consumer.next({ expires: 1_000 });
      if (!second) throw new Error("missing second legacy message");
      const before = await manager.consumers.info(stream, beforeCreate.name);
      expect(before.num_pending).toBe(1);
      expect(before.num_ack_pending).toBe(1);
      expect(before.ack_floor.consumer_seq).toBe(1);
      expect(before.delivered.consumer_seq).toBe(2);

      const runtime = await openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineA), hostId });
      runtimes.push(runtime);
      const after = await manager.consumers.info(stream, beforeCreate.name);
      expect(after.created).toBe(before.created);
      expect(after.config).toEqual({ ...before.config, inactive_threshold: 604_800_000_000_000 });
      expect(after.num_pending).toBe(before.num_pending);
      expect(after.num_ack_pending).toBe(before.num_ack_pending);
      expect(after.ack_floor).toEqual(before.ack_floor);
      expect(after.delivered).toEqual(before.delivered);
      expect(await runtime.drainState()).toEqual({ pending: 1, ackPending: 1 });
      expect(await second.ackAck()).toBe(true);
      const third = await consumer.next({ expires: 1_000 });
      if (!third) throw new Error("pending message lost during migration");
      expect(third.string()).toBe("message-2");
      expect(await third.ackAck()).toBe(true);
    } finally { await nc.close(); }
  }, 15_000);

  it.each(["filter", "ack", "description"])("refuses wrong %s on a legacy consumer without mutation", async field => {
    const nc = await fixture.connect(fixture.users.bootstrap);
    try {
      const { manager } = await openJetStream(nc);
      const hostId = `expiry-wrong-${field}`;
      const config = legacyConfig(hostId);
      if (field === "filter") config.filter_subject = "fabric.in.machine-a.>";
      if (field === "description") config.description = "another host";
      if (field === "ack") {
        config.ack_policy = AckPolicy.None;
        delete config.max_ack_pending;
        delete config.backoff;
      }
      const before = await manager.consumers.add(stream, config);
      await expect(openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineA), hostId })).rejects.toThrow("consumer configuration mismatch");
      const after = await manager.consumers.info(stream, before.name);
      expect(after.config).toEqual(before.config);
      expect(after.created).toBe(before.created);
      expect(after.config.inactive_threshold ?? 0).toBe(0);
    } finally { await nc.close(); }
  });

  it("refuses a configured nonzero mismatch without changing the existing consumer", async () => {
    const hostId = "expiry-nonzero-mismatch";
    const runtime = await openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineA), hostId, inboxConsumerInactiveMs: 120_000 });
    runtimes.push(runtime);
    const nc = await fixture.connect(fixture.users.bootstrap);
    try {
      const { manager } = await openJetStream(nc);
      const before = await manager.consumers.info(stream, transportDurableName(hostId));
      await expect(openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineA), hostId, inboxConsumerInactiveMs: 60_000 }))
        .rejects.toThrow("consumer configuration mismatch");
      const after = await manager.consumers.info(stream, before.name);
      expect(after.config).toEqual(before.config);
      expect(after.created).toBe(before.created);
    } finally { await nc.close(); }
  });

  it("expires 2000 host incarnations in bounded waves while retaining an active durable, without manual deletion", async () => {
    // Account fixture caps consumers at 256. Ten waves of 200 prove cumulative churn below
    // that cap (and the 2.15 stream cap), with real broker-driven expiry between waves.
    const churn = await createPathBHarness({ binary: binary!, tls: {} });
    let messages: ConsumerMessages | undefined;
    try {
      await provision(churn);
      const nc = await churn.connect(churn.users.bootstrap);
      const { manager, js } = await openJetStream(nc);
      expect((await manager.streams.info(stream)).state.consumer_count).toBe(0);
      // Only this low-level test policy uses a threshold below the runtime's 60-second minimum.
      const thresholdNs = 500_000_000;
      const active = await ensureTransportConsumer(manager.consumers, policy("churn-active", thresholdNs));
      const consumer = await js.consumers.get(stream, active.name);
      messages = await consumer.consume({ max_messages: 1, expires: 1_000, idle_heartbeat: 500, callback: msg => msg.ack() });
      await expect.poll(async () => (await manager.consumers.info(stream, active.name)).num_waiting,
        { timeout: 5_000, interval: 25 }).toBe(1);
      let created = 0;
      for (let wave = 0; wave < 10; wave += 1) {
        // Bounded parallel creation keeps the cohort younger than its inactivity timer.
        for (let batch = 0; batch < 10; batch += 1) {
          await Promise.all(Array.from({ length: 20 }, (_, offset) => {
            const hostId = `churn-host-${wave * 200 + batch * 20 + offset}`;
            return ensureTransportConsumer(manager.consumers, policy(hostId, thresholdNs));
          }));
          created += 20;
        }
        const count = (await manager.streams.info(stream)).state.consumer_count;
        expect(count).toBeGreaterThan(1);
        expect(count).toBeLessThanOrEqual(201);
        // Consumer INFO/stream INFO are read-only; broker inactivity, not a delete call,
        // is the event under test. Polling is a bounded observation of that broker event.
        await expect.poll(async () => (await manager.streams.info(stream)).state.consumer_count,
          { timeout: 10_000, interval: 100 }).toBe(1);
        const retained = await manager.consumers.info(stream, active.name);
        expect(retained.created).toBe(active.created);
        expect(retained.config.inactive_threshold).toBe(thresholdNs);
      }
      expect(created).toBe(2_000);
      expect((await manager.streams.info(stream)).state.consumer_count).toBe(1);
    } finally {
      try { await messages?.close(); }
      finally { await churn.stop(); }
    }
  }, 120_000);
});
