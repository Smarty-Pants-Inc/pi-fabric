import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { AckPolicy, DeliverPolicy, ReplayPolicy, type ConsumerConfig } from "@nats-io/jetstream";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import {
  LEASE_TTL_MS,
  PRESENCE_TTL_MS,
  openNatsKv,
} from "../src/transport/nats/kv.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { consumeDeadLetters, type DeadLetterEnvelope } from "../src/transport/nats/dead-letter.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { createPathBHarness, openJetStream, PATH_B_MACHINES, resolveNatsBinary, type PathBHarness } from "./helpers/nats-cluster.js";
import { NatsWireFaults } from "./helpers/nats-wire-faults.js";
import { ensureTransportConsumer } from "../src/transport/nats/provision.js";
import { normalizeFabricConfig } from "../src/config.js";
import {
  transportAdvisoryStreamSubject,
  transportDurableName,
  transportInboxFilter,
  transportInboxStream,
} from "../src/transport/nats/subjects.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const bounded = async <T>(promise: Promise<T>, label: string, timeoutMs = 5_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([promise, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
    })]);
  } finally { if (timer) clearTimeout(timer); }
};

// Explicit fixture config seeds pre-migration durables; product open/ensure must repair them
// using only the unchanged runtime agent grants (data safety and ownership trust boundary).
const legacyConsumerConfig = (hostId: string, inactiveThreshold = 0): ConsumerConfig => ({
  durable_name: transportDurableName(hostId),
  description: JSON.stringify({ fabricHostId: hostId, machine: "machine-a" }),
  filter_subject: transportInboxFilter("machine-a", hostId),
  ack_policy: AckPolicy.Explicit,
  deliver_policy: DeliverPolicy.All,
  replay_policy: ReplayPolicy.Instant,
  ack_wait: 30_000_000_000,
  max_deliver: 5,
  backoff: Array.from({ length: 5 }, () => 30_000_000_000),
  max_ack_pending: 128,
  max_waiting: 1,
  inactive_threshold: inactiveThreshold,
});

const inboxEnvelope = (hostId: string, id: string) => ({
  id, kind: "command" as const, to: { machine: "machine-a", hostId }, targetId: `target:${id}`, operation: "followUp",
  from: { id: "lifetime-test-sender", name: "lifetime-test", kind: "main" as const }, body: { text: id },
});

// Formerly an anonymous server with verifyMachine:false; now the scoped Path B fixture with real agent
// credentials, exact policy and the test-only loopback opt-in. Fixture-only bootstrap inspects config.
let shared: PathBHarness;
const transports: Array<{ close(): Promise<void> }> = [];

const provisionAs = async (fixture: PathBHarness): Promise<void> => {
  for (const [name, value] of Object.entries(fixture.adminEnv(fixture.users.bootstrap))) vi.stubEnv(name, value);
  try {
    expect(await runTransportAdmin(["provision", "--ca-file", fixture.tls!.caFile, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
  } finally { vi.unstubAllEnvs(); }
};

beforeAll(async () => {
  if (!binary) return;
  shared = await createPathBHarness({ binary, tls: {} });
  await provisionAs(shared);
}, 30_000);

afterAll(async () => {
  await Promise.all(transports.map((transport) => transport.close()));
  await shared?.stop();
});

describe.skipIf(!binary)("real nats-server Path B module", () => {
  it("requires the USER.INFO machine check with no runtime bypass", async () => {
    const { credentials: _unused, ...anonymous } = shared.runtimeOptions(shared.users.machineA);
    await expect(openNatsConnection({ ...anonymous, name: "test-user-info" } as unknown as Parameters<typeof openNatsConnection>[0])).rejects.toThrow();
    await expect(openNatsConnection({ ...shared.runtimeOptions(shared.users.machineA), machine: "machine-b" })).rejects.toThrow();
    await expect(openNatsConnection({ ...shared.runtimeOptions(shared.users.machineB), name: "test-user-info-bypass",
      verifyMachine: false, machine: "machine-a" } as unknown as Parameters<typeof openNatsConnection>[0])).rejects.toThrow();
    const connection = await openNatsConnection({ ...shared.runtimeOptions(shared.users.machineA), name: "test-user-info" });
    expect(connection.userInfo).toBeDefined();
    await connection.close();
  });

  it("checks bounded provisioning order and reads the actual consumer configuration", async () => {
    const transport = await openNatsTransport({
      ...shared.runtimeOptions(shared.users.machineA),
      hostId: "runtime-provision",
      name: "test-provision",
    });
    transports.push(transport);
    const inspector = await shared.connect(shared.users.bootstrap);
    const admin = await openJetStream(inspector);
    try {
      const advisory = await admin.manager.streams.info("FABRIC_ADV");
      expect(advisory.config.subjects).toEqual([transportAdvisoryStreamSubject]);
      expect(advisory.config.retention).toBe("limits");
      expect(advisory.config.storage).toBe("file");
      expect(advisory.config.discard).toBe("new");
      expect(advisory.config.max_age).toBe(30 * 24 * 60 * 60 * 1_000 * 1_000_000);

      const inbox = await admin.manager.streams.info(transportInboxStream("machine-a"));
      expect(inbox.config.max_age).toBe(7 * 24 * 60 * 60_000 * 1_000_000);
      expect(inbox.config.max_bytes).toBe(1_073_741_824);
      expect(inbox.config.max_msg_size).toBe(1_048_576);
      expect(inbox.config.allow_direct).toBe(false);

      const durable = transportDurableName("runtime-provision");
      const consumer = await ensureTransportConsumer(admin.manager.consumers, {
        stream: transportInboxStream("machine-a"),
        durable,
        filterSubject: transportInboxFilter("machine-a", "runtime-provision"),
        description: JSON.stringify({ fabricHostId: "runtime-provision", machine: "machine-a" }),
        maxAckPending: 128,
      });
      expect(consumer.config.ack_policy).toBe("explicit");
      expect(consumer.config.deliver_policy).toBe("all");
      expect(consumer.config.ack_wait).toBe(30_000_000_000);
      expect(consumer.config.max_deliver).toBe(5);
      expect(consumer.config.max_ack_pending).toBe(128);
      expect(consumer.config.inactive_threshold).toBe(7 * 24 * 60 * 60 * 1_000_000_000);
      expect(consumer.config.filter_subject).toBe(transportInboxFilter("machine-a", "runtime-provision"));
      expect(await transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    } finally {
      await inspector.close();
    }
  }, 30_000);

  it.each([
    { hostId: "session:bounded-inbox", seconds: 7 * 24 * 60 * 60 },
    { hostId: "runtime:bounded-inbox", seconds: 7 * 24 * 60 * 60 },
    { hostId: "runtime-without-colon", seconds: 7 * 24 * 60 * 60 },
  ])("creates $hostId with a bounded server-side idle lifetime", async ({ hostId, seconds }) => {
    const transport = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId });
    transports.push(transport);
    const inspector = await shared.connect(shared.users.bootstrap);
    try {
      const admin = await openJetStream(inspector);
      const info = await admin.manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(hostId));
      expect(info.config.inactive_threshold).toBe(seconds * 1_000_000_000);
      await transport.close();
      // Bounded ordinary/runtime hosts keep their cursor on clean close; only explicit
      // ephemeralHost retains the pre-existing best-effort delete-on-close behavior.
      expect((await admin.manager.consumers.info(info.stream_name, info.name)).created).toBe(info.created);
    } finally { await inspector.close(); }
  });

  it.each([
    { hostId: "session:legacy-lifetime", legacyNs: 0, seconds: 7 * 24 * 60 * 60 },
    { hostId: "runtime:legacy-lifetime", legacyNs: 60 * 60 * 1_000_000_000, seconds: 7 * 24 * 60 * 60 },
  ])("heals $hostId with runtime credentials without resetting ACKs, pending records or unrelated config", async ({ hostId, legacyNs, seconds }) => {
    const inspector = await shared.connect(shared.users.bootstrap);
    const admin = await openJetStream(inspector);
    const runtime = await openNatsConnection(shared.runtimeOptions(shared.users.machineA));
    const sender = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId: `${hostId}:sender` });
    transports.push(sender);
    try {
      const stream = transportInboxStream("machine-a"), durable = transportDurableName(hostId);
      await admin.manager.consumers.add(stream, {
        ...legacyConsumerConfig(hostId, legacyNs), sample_freq: "25%", max_batch: 32,
        metadata: { lifetime_test: "preserve-unrelated-config" },
      });
      const sent = [];
      for (const suffix of ["acked", "held", "pending"]) {
        sent.push(await sender.send(inboxEnvelope(hostId, `${hostId}:${suffix}`)));
      }
      // Single-message pulls leave exactly one ACKed record, one in flight and one
      // never delivered. No product inbox prefetch can move the cursor during repair.
      const consumer = await runtime.js.consumers.get(stream, durable);
      const acked = await consumer.next({ expires: 2_000 });
      expect(acked?.info.streamSequence).toBe(sent[0]!.sequence);
      expect(await acked!.ackAck()).toBe(true);
      const held = await consumer.next({ expires: 2_000 });
      expect(held?.info.streamSequence).toBe(sent[1]!.sequence);
      const before = await admin.manager.consumers.info(stream, durable);
      expect(before.ack_floor).toMatchObject({ consumer_seq: 1, stream_seq: sent[0]!.sequence });
      expect(before.delivered).toMatchObject({ consumer_seq: 2, stream_seq: sent[1]!.sequence });
      expect([before.num_ack_pending, before.num_pending]).toEqual([1, 1]);

      // This is the actual scoped runtime opener, not a bootstrap update or fake API.
      const reopened = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId });
      transports.push(reopened);
      const after = await runtime.manager.consumers.info(stream, durable);
      expect(after.config).toEqual({ ...before.config, inactive_threshold: seconds * 1_000_000_000 });
      expect(after.created).toBe(before.created);
      expect(after.ack_floor).toEqual(before.ack_floor);
      expect(after.delivered).toEqual(before.delivered);
      expect([after.num_ack_pending, after.num_pending, after.num_redelivered])
        .toEqual([before.num_ack_pending, before.num_pending, before.num_redelivered]);
      expect(await reopened.drainState()).toEqual({ pending: 1, ackPending: 1 });
      expect(await held!.ackAck()).toBe(true);
    } finally { await runtime.close(); await inspector.close(); }
  }, 15_000);

  it("sets server-side idle expiry for an ephemeral host and deletes its durable on clean close", async () => {
    const runtime = shared.runtimeOptions(shared.users.machineA);
    vi.stubEnv("PI_FABRIC_MESH_TRANSPORT", "nats");
    vi.stubEnv("PI_FABRIC_MESH_HOST_TRANSPORT", "nats");
    vi.stubEnv("PI_FABRIC_MESH_NATS", JSON.stringify({
      ...runtime,
      operatorMachine: "fabric-ops",
      ephemeralHost: true,
      ephemeralInactiveSeconds: 300,
    }));
    const hostId = "runtime:ephemeral-clean-exit";
    const durable = transportDurableName(hostId);
    let transport: Awaited<ReturnType<typeof openNatsTransport>> | undefined;
    let inspector: Awaited<ReturnType<typeof shared.connect>> | undefined;
    try {
      const config = normalizeFabricConfig({}).mesh;
      transport = await openNatsTransport({ config, hostId, mesh: {} as never });
      transports.push(transport);
      inspector = await shared.connect(shared.users.bootstrap);
      const admin = await openJetStream(inspector);
      const info = await admin.manager.consumers.info(transportInboxStream("machine-a"), durable);
      expect(info.config.inactive_threshold).toBe(300 * 1_000_000_000);

      const inboxAbort = new AbortController();
      const inboxIterator = transport.inbox(inboxAbort.signal)[Symbol.asyncIterator]();
      try {
        const received = bounded(inboxIterator.next(), "ephemeral NATS inbox delivery");
        const envelope = {
          id: "ephemeral-inbox-reensure",
          kind: "command" as const,
          to: transport.self,
          targetId: "ephemeral-inbox-target",
          operation: "followUp",
          from: { id: hostId, name: "ephemeral-test", kind: "main" as const },
          body: { text: "re-ensure the existing ephemeral durable" },
        };
        await transport.send(envelope);
        const result = await received;
        expect(result.done).toBe(false);
        if (result.done || !result.value) throw new Error("ephemeral inbox ended before delivery");
        expect(result.value.payload).toMatchObject({ id: envelope.id, to: transport.self });
        await result.value.ack();
      } finally {
        inboxAbort.abort();
        await inboxIterator.return?.();
      }

      await transport.close();
      let deleted = false;
      for (let attempt = 0; attempt < 100; attempt += 1) {
        try { await admin.manager.consumers.info(transportInboxStream("machine-a"), durable); }
        catch { deleted = true; break; }
        await wait(20);
      }
      expect(deleted).toBe(true);
    } finally {
      await transport?.close();
      await inspector?.close();
      vi.unstubAllEnvs();
    }
  }, 30_000);

  it.each([
    { fault: "filter", error: /consumer configuration mismatch/ },
    { fault: "owner", error: /consumer owner description mismatch/ },
  ])("refuses a legacy durable with the wrong $fault without any mutation", async ({ fault, error }) => {
    const inspector = await shared.connect(shared.users.bootstrap);
    const admin = await openJetStream(inspector);
    try {
      const hostId = `runtime:foreign-${fault}`;
      const before = await admin.manager.consumers.add(transportInboxStream("machine-a"), {
        ...legacyConsumerConfig(hostId),
        ...(fault === "filter" ? { filter_subject: "fabric.in.machine-a.>" } : { description: "another owner" }),
      });
      await expect(openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId })).rejects.toThrow(error);
      const after = await admin.manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(hostId));
      expect(after.config).toEqual(before.config);
      expect(after.created).toBe(before.created);
      expect(after.ack_floor).toEqual(before.ack_floor);
      expect(after.delivered).toEqual(before.delivered);
    } finally { await inspector.close(); }
  });

  it("reprovisions a missing durable on reopen and deliberately replays retained ACKed records", async () => {
    const hostId = "runtime:missing-on-reopen", stream = transportInboxStream("machine-a");
    const original = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId });
    transports.push(original);
    const inspector = await shared.connect(shared.users.bootstrap);
    const admin = await openJetStream(inspector);
    const signal = new AbortController();
    const iterator = original.inbox(signal.signal)[Symbol.asyncIterator]();
    try {
      const sent = await original.send(inboxEnvelope(hostId, "retained-acked-before-expiry"));
      const first = await bounded(iterator.next(), "delivery before cursor loss");
      expect(first.value?.sequence).toBe(sent.sequence);
      await first.value!.ack();
      signal.abort();
      await iterator.return?.();
      await original.close();
      // Deterministic stand-in for elapsed server GC: only bootstrap removes the
      // test cursor. The product must create the missing durable with agent grants.
      expect(await admin.manager.consumers.delete(stream, transportDurableName(hostId))).toBe(true);
      const reopened = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId });
      transports.push(reopened);
      const info = await admin.manager.consumers.info(stream, transportDurableName(hostId));
      expect(info.config.inactive_threshold).toBe(7 * 24 * 60 * 60 * 1_000_000_000);
      expect(info.num_pending).toBe(1);
      const replaySignal = new AbortController();
      const replay = reopened.inbox(replaySignal.signal)[Symbol.asyncIterator]();
      try {
        const result = await bounded(replay.next(), "DeliverAll replay after cursor loss");
        expect(result.value).toMatchObject({ sequence: sent.sequence, attempt: 1, payload: { id: sent.id } });
        await result.value!.ack();
      } finally { replaySignal.abort(); await replay.return?.(); }
    } finally { signal.abort(); await iterator.return?.(); await inspector.close(); }
  }, 15_000);

  it("reprovisions a missing durable in the same inbox iterator after a verified native reconnect", async () => {
    const hostId = "runtime:missing-on-reconnect", stream = transportInboxStream("machine-a");
    const relay = await NatsWireFaults.open(shared.serverUrl, shared.tls!);
    const opened: Array<{ close(): Promise<void> }> = [];
    const signal = new AbortController();
    let unsubscribe: (() => void) | undefined;
    let returnInbox: (() => Promise<unknown>) | undefined;
    try {
      const inspector = await shared.connect(shared.users.bootstrap);
      opened.push(inspector);
      const admin = await openJetStream(inspector);
      const receiver = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId, servers: relay.url });
      opened.push(receiver);
      transports.push(receiver);
      const sender = await openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId: "reconnect-lifetime-sender" });
      opened.push(sender);
      transports.push(sender);
      const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
      returnInbox = () => iterator.return!();
      let reconnect!: () => void;
      const reverified = new Promise<void>((resolve) => { reconnect = resolve; });
      unsubscribe = receiver.onVerifiedReconnect!(reconnect);
      const waiting = iterator.next();
      void waiting.catch(() => undefined);
      // The server-side outstanding pull is the readiness event. Poll only as a
      // bounded test safety net; no wall-clock sleep guesses when setup finished.
      await expect.poll(async () => (await admin.manager.consumers.info(stream, transportDurableName(hostId))).num_waiting,
        { timeout: 5_000, interval: 25 }).toBe(1);
      relay.block();
      expect(relay.severed).toBe(1);
      expect(await admin.manager.consumers.delete(stream, transportDurableName(hostId))).toBe(true);
      const sent = await sender.send(inboxEnvelope(hostId, "backlog-during-missing-durable"));
      relay.unblock();
      await bounded(reverified, "verified native reconnect", 10_000);
      const result = await bounded(waiting, "same-iterator delivery after reprovision", 10_000);
      expect(result.value).toMatchObject({ sequence: sent.sequence, attempt: 1, payload: { id: sent.id } });
      await result.value!.ack();
      const info = await admin.manager.consumers.info(stream, transportDurableName(hostId));
      expect(info.config.inactive_threshold).toBe(7 * 24 * 60 * 60 * 1_000_000_000);
      expect(await receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
      expect(relay.accepts).toBeGreaterThanOrEqual(2);
    } finally {
      unsubscribe?.();
      signal.abort();
      try {
        await Promise.all(opened.map((connection) => connection.close()));
        if (returnInbox) await bounded(returnInbox(), "reconnected iterator cleanup");
      } finally { await relay.close(); }
    }
  }, 30_000);

  it("sends, dedupes, redelivers, confirms ACKs and captures max-delivery advisories", async () => {
    // This selected integration path uses actual role credentials and default USER.INFO verification.
    const fixture = await createPathBHarness({ binary, tls: {} });
    const opened: Array<{ close(): Promise<void> }> = [];
    const signal = new AbortController();
    try {
      await provisionAs(fixture);
      const sender = await openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineA), hostId: "sender-messages" });
      opened.push(sender);
      const receiver = await openNatsTransport({ ...fixture.runtimeOptions(fixture.users.machineB), hostId: "receiver-messages" });
      opened.push(receiver);
      const operator = await openNatsConnection(fixture.operatorOptions());
      opened.push(operator);
      expect((await operator.manager.consumers.info("FABRIC_ADV", "transport_dlq_operator")).config.ack_policy).toBe(AckPolicy.None);
      const iterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
      const envelope = {
        id: "message-stable-id", kind: "command" as const, to: receiver.self, targetId: "target-messages", operation: "followUp",
        from: { id: sender.self.hostId, name: "sender", kind: "main" as const }, body: { text: "real broker" },
      };
      const sent = await sender.send(envelope);
      expect((await sender.send(envelope))).toMatchObject({ sequence: sent.sequence, duplicate: true });
      const first = (await iterator.next()).value!;
      expect(first).toMatchObject({ attempt: 1, sourceMachine: "machine-a", sequence: sent.sequence, payload: { id: envelope.id } });
      first.working();
      first.nak();
      const redelivered = (await iterator.next()).value!;
      expect(redelivered.attempt).toBe(2);
      await redelivered.ack();
      expect(await receiver.drainState()).toEqual({ pending: 0, ackPending: 0 });
      const duplicateIterator = receiver.inbox(signal.signal)[Symbol.asyncIterator]();
      await expect(duplicateIterator.next()).rejects.toMatchObject({ code: "FABRIC_TRANSPORT_INVALID" });

      const doomed = await sender.send({ ...envelope, id: "message-max-deliveries" });
      for (let attempt = 1; attempt <= 5; attempt += 1) {
        const delivery = (await iterator.next()).value!;
        expect(delivery.attempt).toBe(attempt);
        delivery.nak();
      }
      const pending = iterator.next(); // Triggers the broker's standard exhaustion advisory.
      const captured: DeadLetterEnvelope[] = [];
      expect(await consumeDeadLetters({ manager: operator.manager, js: operator.js, machines: PATH_B_MACHINES,
        signal: signal.signal, once: true, sink: { async put(record) { captured.push(record); } } })).toBe(1);
      expect(captured).toEqual([{ stream: "FABRIC_IN_machine-b", consumer: transportDurableName(receiver.self.hostId),
        stream_seq: doomed.sequence, deliveries: 5 }]);
      signal.abort();
      expect((await pending).done).toBe(true);
      await iterator.return?.();
    } finally {
      signal.abort();
      vi.unstubAllEnvs();
      await Promise.all(opened.map((connection) => connection.close()));
      await fixture.stop();
    }
  }, 30_000);

  it("uses fixed bucket TTLs, CAS heartbeats, takeover after expiry, and a ready presence watch", async () => {
    const transport = await openNatsTransport({
      ...shared.runtimeOptions(shared.users.machineA),
      hostId: "runtime-kv",
      name: "test-kv",
    });
    transports.push(transport);
    const leases = transport.kv("leases");
    const presence = transport.kv("presence");
    expect(leases.ttlMs).toBe(LEASE_TTL_MS);
    expect(presence.ttlMs).toBe(PRESENCE_TTL_MS);

    const leaseKey = "machine-a.h.lease-test";
    const first = await leases.put(leaseKey, "runtime-kv#one", { pid: 1 }, 0);
    expect((await leases.get<{ pid: number }>(leaseKey))?.value).toEqual({ pid: 1 });
    await expect(leases.put(leaseKey, "runtime-kv#two", { pid: 2 }, first - 1)).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT" });
    const renewed = await leases.put(leaseKey, "runtime-kv#one", { pid: 1 }, first);
    expect(await leases.delete(leaseKey, first)).toBe(false);
    expect(await leases.delete(leaseKey, renewed)).toBe(true);

    const controller = new AbortController();
    const iterator = presence.watch("machine-a.>", controller.signal)[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ done: false, value: { type: "ready" } });
    const presenceKey = "machine-a.p.participant-test";
    const presenceRevision = await presence.put(presenceKey, "runtime-kv#one", { live: true }, 0);
    expect((await iterator.next()).value).toMatchObject({ type: "put", entry: { key: presenceKey, revision: presenceRevision } });
    expect(await presence.delete(presenceKey, presenceRevision)).toBe(true);
    expect((await iterator.next()).value).toMatchObject({ type: "gone", key: presenceKey, reason: "deleted" });
    controller.abort();
    await iterator.return?.();

    // Bucket creation is administration: the fixture-only bootstrap owns this custom-TTL bucket.
    const inspector = await shared.connect(shared.users.bootstrap);
    try {
      const admin = await openJetStream(inspector);
      const bucket = `TEST_LEASE_${Date.now()}`;
      const expiring = (await openNatsKv(admin.js, { bucket, ttlMs: 1_000, markerTtlMs: 1_000 })).withMachine("machine-a");
      const key = "machine-a.h.takeover-test";
      await expiring.put(key, "runtime-kv#one", { pid: 1 }, 0);
      await wait(2_500);
      expect(await expiring.get(key)).toBeUndefined();
      expect(await expiring.put(key, "runtime-kv#two", { pid: 2 }, 0)).toBeGreaterThan(0);
    } finally {
      await inspector.close();
    }
  }, 30_000);
});
