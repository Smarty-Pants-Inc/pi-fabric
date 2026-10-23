import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { AckPolicy, DeliverPolicy, JetStreamApiError, ReplayPolicy, type ConsumerAPI, type ConsumerInfo } from "@nats-io/jetstream";
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

const consumerInfo = (inactiveThreshold?: number): ConsumerInfo => ({
  config: {
    durable_name: "policy-test",
    description: "policy test",
    filter_subject: "fabric.in.machine-a.policy-test",
    ack_policy: AckPolicy.Explicit,
    deliver_policy: DeliverPolicy.All,
    replay_policy: ReplayPolicy.Instant,
    ack_wait: 30_000_000_000,
    max_deliver: 5,
    backoff: Array.from({ length: 5 }, () => 30_000_000_000),
    max_ack_pending: 128,
    max_waiting: 1,
    ...(inactiveThreshold === undefined ? {} : { inactive_threshold: inactiveThreshold }),
  },
} as unknown as ConsumerInfo);

const consumerApi = (info: ConsumerInfo) => ({
  info: vi.fn(async () => info),
  update: vi.fn(async (_stream: string, _durable: string, patch: Record<string, unknown>) => ({
    ...info, config: { ...info.config, ...patch },
  })),
  add: vi.fn(),
  delete: vi.fn(),
});

describe("inbox consumer inactive-threshold policy", () => {
  const basePolicy = {
    stream: "FABRIC_IN_machine-a",
    durable: "policy-test",
    filterSubject: "fabric.in.machine-a.policy-test",
    description: "policy test",
    maxAckPending: 128,
  };

  it.each([604_800_000_000_000, 300_000_000_000])("accepts an exact bounded threshold %s without mutation", async thresholdNs => {
    const api = consumerApi(consumerInfo(thresholdNs));
    await expect(ensureTransportConsumer(api as unknown as ConsumerAPI, {
      ...basePolicy, ...(thresholdNs === 604_800_000_000_000 ? {} : { inactiveThresholdNs: thresholdNs }),
    })).resolves.toMatchObject({ config: { inactive_threshold: thresholdNs } });
    expect(api.update).not.toHaveBeenCalled();
    expect(api.add).not.toHaveBeenCalled();
    expect(api.delete).not.toHaveBeenCalled();
  });

  it.each([undefined, 0])("updates only the legacy threshold %s in place", async thresholdNs => {
    const info = { ...consumerInfo(thresholdNs), num_pending: 7, num_ack_pending: 2,
      ack_floor: { consumer_seq: 3, stream_seq: 4 }, delivered: { consumer_seq: 5, stream_seq: 6 } } as ConsumerInfo;
    const api = consumerApi(info);
    const updated = await ensureTransportConsumer(api as unknown as ConsumerAPI, basePolicy);
    expect(updated).toEqual({ ...info, config: { ...info.config, inactive_threshold: 604_800_000_000_000 } });
    expect(api.update).toHaveBeenCalledExactlyOnceWith(basePolicy.stream, basePolicy.durable, { inactive_threshold: 604_800_000_000_000 });
    expect(api.add).not.toHaveBeenCalled();
    expect(api.delete).not.toHaveBeenCalled();
  });

  it.each([
    { filter_subject: "fabric.in.machine-a.>" }, { filter_subjects: ["fabric.in.machine-a.>"] },
    { ack_policy: AckPolicy.None }, { description: "another owner" }, { durable_name: "another-durable" },
    { deliver_subject: "push" }, { headers_only: true }, { replay_policy: ReplayPolicy.Original },
    { max_waiting: 2 }, { deliver_policy: DeliverPolicy.New }, { ack_wait: 1 }, { max_deliver: 6 },
    { max_ack_pending: 129 }, { backoff: [1] },
  ])("refuses incompatible nonthreshold fields before mutation: %j", async patch => {
    const info = consumerInfo(0);
    Object.assign(info.config, patch);
    const before = structuredClone(info);
    const api = consumerApi(info);
    await expect(ensureTransportConsumer(api as unknown as ConsumerAPI, basePolicy)).rejects.toThrow("consumer configuration mismatch");
    expect(info).toEqual(before);
    expect(api.update).not.toHaveBeenCalled();
    expect(api.add).not.toHaveBeenCalled();
    expect(api.delete).not.toHaveBeenCalled();
  });

  it.each([1, 300_000_000_000, 777_600_000_000_000])("refuses a differing nonzero threshold %s without mutation", async thresholdNs => {
    const api = consumerApi(consumerInfo(thresholdNs));
    await expect(ensureTransportConsumer(api as unknown as ConsumerAPI, basePolicy)).rejects.toThrow("consumer configuration mismatch");
    expect(api.update).not.toHaveBeenCalled();
    expect(api.add).not.toHaveBeenCalled();
    expect(api.delete).not.toHaveBeenCalled();
  });

  it.each([0, -1, NaN, Infinity, 0.5, Number.MAX_SAFE_INTEGER + 1])("refuses invalid low-level threshold %s before API calls", async inactiveThresholdNs => {
    const api = consumerApi(consumerInfo(0));
    await expect(ensureTransportConsumer(api as unknown as ConsumerAPI, { ...basePolicy, inactiveThresholdNs }))
      .rejects.toThrow("inactiveThresholdNs must be a positive safe integer");
    expect(api.info).not.toHaveBeenCalled();
    expect(api.update).not.toHaveBeenCalled();
    expect(api.add).not.toHaveBeenCalled();
  });

  it("creates a bounded durable by default", async () => {
    const api = consumerApi(consumerInfo(604_800_000_000_000));
    api.info.mockRejectedValueOnce(new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" }));
    api.add.mockResolvedValue(consumerInfo(604_800_000_000_000));
    await ensureTransportConsumer(api as unknown as ConsumerAPI, basePolicy);
    expect(api.add).toHaveBeenCalledWith(basePolicy.stream, expect.objectContaining({ inactive_threshold: 604_800_000_000_000 }));
    expect(api.update).not.toHaveBeenCalled();
    expect(api.delete).not.toHaveBeenCalled();
  });

  it("checks a concurrent-create winner before migrating its legacy threshold", async () => {
    const info = consumerInfo(0);
    info.config.filter_subject = "fabric.in.machine-a.>";
    const api = consumerApi(info);
    api.info.mockRejectedValueOnce(new JetStreamApiError({ code: 404, err_code: 10014, description: "consumer not found" }));
    api.add.mockRejectedValueOnce(new Error("concurrent create"));
    await expect(ensureTransportConsumer(api as unknown as ConsumerAPI, basePolicy)).rejects.toThrow("consumer configuration mismatch");
    expect(api.update).not.toHaveBeenCalled();
    expect(api.delete).not.toHaveBeenCalled();
    expect(info.config.inactive_threshold).toBe(0);
  });

  it("revalidates the update result", async () => {
    const api = consumerApi(consumerInfo(0));
    api.update.mockResolvedValueOnce(consumerInfo(0));
    await expect(ensureTransportConsumer(api as unknown as ConsumerAPI, basePolicy)).rejects.toThrow("consumer configuration mismatch");
    expect(api.delete).not.toHaveBeenCalled();
  });
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
      expect(consumer.config.inactive_threshold).toBe(604_800_000_000_000);
      expect(consumer.config.filter_subject).toBe(transportInboxFilter("machine-a", "runtime-provision"));
      expect(await transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    } finally {
      await inspector.close();
    }
  }, 30_000);

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
    const hostId = "runtime-ephemeral-clean-exit";
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

  it("refuses a pre-existing runtime durable with a broader inbox filter", async () => {
    const inspector = await shared.connect(shared.users.bootstrap);
    const admin = await openJetStream(inspector);
    try {
      const hostId = "runtime-drifted-consumer";
      await admin.manager.consumers.add(transportInboxStream("machine-a"), {
        durable_name: transportDurableName(hostId), filter_subject: "fabric.in.machine-a.>", ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        replay_policy: ReplayPolicy.Instant,
        ack_wait: 30_000_000_000, max_deliver: 5, backoff: [30_000_000_000, 30_000_000_000, 30_000_000_000, 30_000_000_000, 30_000_000_000], max_ack_pending: 128, max_waiting: 1,
      });
      await expect(openNatsTransport({ ...shared.runtimeOptions(shared.users.machineA), hostId })).rejects.toThrow("consumer configuration mismatch");
      expect((await admin.manager.consumers.info(transportInboxStream("machine-a"), transportDurableName(hostId))).config.filter_subject).toBe("fabric.in.machine-a.>");
    } finally { await inspector.close(); }
  });

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
