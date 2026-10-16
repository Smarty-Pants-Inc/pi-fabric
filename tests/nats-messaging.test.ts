import { spawn, type ChildProcess } from "node:child_process";
import { createServer } from "node:net";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { AckPolicy, DeliverPolicy, ReplayPolicy } from "@nats-io/jetstream";
import { openNatsConnection } from "../src/transport/nats/connection.js";
import {
  LEASE_TTL_MS,
  PRESENCE_TTL_MS,
  openNatsKv,
} from "../src/transport/nats/kv.js";
import { openNatsTransport } from "../src/transport/nats/inbox.js";
import { consumeDeadLetters, type DeadLetterEnvelope } from "../src/transport/nats/dead-letter.js";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { createPathBHarness, PATH_B_MACHINES, resolveNatsBinary } from "./helpers/nats-cluster.js";
import { ensureTransportConsumer } from "../src/transport/nats/provision.js";
import {
  transportAdvisoryStreamSubject,
  transportDurableName,
  transportInboxFilter,
  transportInboxStream,
} from "../src/transport/nats/subjects.js";

const binary = resolveNatsBinary() ?? "";
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const freePort = async (): Promise<number> => {
  const server = createServer();
  await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("port reservation failed");
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return address.port;
};
const ready = async (url: string, child: ChildProcess): Promise<void> => {
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null || child.signalCode !== null) throw new Error("nats-server exited before readiness");
    try {
      const response = await fetch(`${url}/healthz?js-enabled-only=true`);
      if (response.status === 200) return;
    } catch { /* server is still starting */ }
    await wait(100);
  }
  throw new Error("nats-server readiness timeout");
};

let root = "";
let server: ChildProcess | undefined;
let serverUrl = "";
const transports: Array<{ close(): Promise<void> }> = [];

beforeAll(async () => {
  if (!binary) return;
  root = mkdtempSync(path.join(process.env.TMPDIR || os.tmpdir(), "pi-fabric-nats-transport-"));
  mkdirSync(path.join(root, "store"), { mode: 0o700 });
  const port = await freePort();
  const monitor = await freePort();
  serverUrl = `nats://127.0.0.1:${port}`;
  const config = `port: ${port}\nhttp: ${monitor}\njetstream: {store_dir: ${JSON.stringify(path.join(root, "store"))}, sync_interval: always}\n`;
  const configPath = path.join(root, "nats.conf");
  writeFileSync(configPath, config, { mode: 0o600 });
  server = spawn(binary, ["-c", configPath], { cwd: root, stdio: ["ignore", "pipe", "pipe"] });
  await ready(`http://127.0.0.1:${monitor}`, server);
}, 30_000);

afterAll(async () => {
  await Promise.all(transports.map((transport) => transport.close()));
  if (server && server.exitCode === null && server.signalCode === null) {
    server.kill("SIGTERM");
    await new Promise<void>((resolve) => server!.once("exit", () => resolve()));
  }
  if (root) rmSync(root, { recursive: true, force: true });
});

describe.skipIf(!binary)("real nats-server Path B module", () => {
  it("requires the USER.INFO machine check unless explicitly disabled", async () => {
    await expect(openNatsConnection({ servers: serverUrl, name: "test-user-info", machine: "ryzen1" })).rejects.toThrow();
    const connection = await openNatsConnection({ servers: serverUrl, name: "test-user-info-disabled", machine: "ryzen1", verifyMachine: false });
    await connection.close();
  });

  it("checks bounded provisioning order and reads the actual consumer configuration", async () => {
    const transport = await openNatsTransport({
      servers: serverUrl,
      machine: "ryzen1",
      hostId: "runtime-provision",
      name: "test-provision",
      verifyMachine: false,
    });
    transports.push(transport);
    const admin = await openNatsConnection({ servers: serverUrl, name: "test-inspector", verifyMachine: false });
    try {
      const advisory = await admin.manager.streams.info("FABRIC_ADV");
      expect(advisory.config.subjects).toEqual([transportAdvisoryStreamSubject]);
      expect(advisory.config.retention).toBe("limits");
      expect(advisory.config.storage).toBe("file");
      expect(advisory.config.discard).toBe("new");
      expect(advisory.config.max_age).toBe(30 * 24 * 60 * 60 * 1_000 * 1_000_000);

      const inbox = await admin.manager.streams.info(transportInboxStream("ryzen1"));
      expect(inbox.config.max_age).toBe(0);
      expect(inbox.config.max_bytes).toBe(1_073_741_824);
      expect(inbox.config.max_msg_size).toBe(1_048_576);
      expect(inbox.config.allow_direct).toBe(false);

      const durable = transportDurableName("runtime-provision");
      const consumer = await ensureTransportConsumer(admin.manager.consumers, {
        stream: transportInboxStream("ryzen1"),
        durable,
        filterSubject: transportInboxFilter("ryzen1", "runtime-provision"),
        description: "test consumer",
        maxAckPending: 128,
      });
      expect(consumer.config.ack_policy).toBe("explicit");
      expect(consumer.config.deliver_policy).toBe("all");
      expect(consumer.config.ack_wait).toBe(30_000_000_000);
      expect(consumer.config.max_deliver).toBe(5);
      expect(consumer.config.max_ack_pending).toBe(128);
      expect(consumer.config.inactive_threshold ?? 0).toBe(0);
      expect(consumer.config.filter_subject).toBe(transportInboxFilter("ryzen1", "runtime-provision"));
      expect(await transport.drainState()).toEqual({ pending: 0, ackPending: 0 });
    } finally {
      await admin.close();
    }
  }, 30_000);

  it("refuses a pre-existing runtime durable with a broader inbox filter", async () => {
    const admin = await openNatsConnection({ servers: serverUrl, verifyMachine: false });
    try {
      const hostId = "runtime-drifted-consumer";
      await admin.manager.consumers.add(transportInboxStream("ryzen1"), {
        durable_name: transportDurableName(hostId), filter_subject: "fabric.in.ryzen1.>", ack_policy: AckPolicy.Explicit,
        deliver_policy: DeliverPolicy.All,
        replay_policy: ReplayPolicy.Instant,
        ack_wait: 30_000_000_000, max_deliver: 5, backoff: [30_000_000_000, 30_000_000_000, 30_000_000_000, 30_000_000_000, 30_000_000_000], max_ack_pending: 128, max_waiting: 1,
      });
      await expect(openNatsTransport({ servers: serverUrl, machine: "ryzen1", hostId, verifyMachine: false })).rejects.toThrow("consumer configuration mismatch");
      expect((await admin.manager.consumers.info(transportInboxStream("ryzen1"), transportDurableName(hostId))).config.filter_subject).toBe("fabric.in.ryzen1.>");
    } finally { await admin.close(); }
  });

  it("sends, dedupes, redelivers, confirms ACKs and captures max-delivery advisories", async () => {
    // This selected integration path uses actual role credentials and default USER.INFO verification.
    const fixture = await createPathBHarness({ binary });
    const opened: Array<{ close(): Promise<void> }> = [];
    const signal = new AbortController();
    try {
      vi.stubEnv("PI_FABRIC_MACHINE", fixture.users.bootstrap.machine);
      vi.stubEnv("PI_FABRIC_NATS_SERVERS", fixture.serverUrl);
      vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", fixture.users.bootstrap.credsPath);
      await runTransportAdmin(["provision", "--machines", PATH_B_MACHINES.join(",")]);
      vi.unstubAllEnvs();
      const sender = await openNatsTransport({ servers: fixture.serverUrl, machine: "machine-a", hostId: "sender-messages",
        credentials: fixture.users.machineA.credsPath });
      opened.push(sender);
      const receiver = await openNatsTransport({ servers: fixture.serverUrl, machine: "machine-b", hostId: "receiver-messages",
        credentials: fixture.users.machineB.credsPath });
      opened.push(receiver);
      const operator = await openNatsConnection({ servers: fixture.serverUrl, machine: fixture.users.maintainer.machine,
        credentials: fixture.users.maintainer.credsPath });
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
      servers: serverUrl,
      machine: "ryzen2",
      hostId: "runtime-kv",
      name: "test-kv",
      verifyMachine: false,
    });
    transports.push(transport);
    const leases = transport.kv("leases");
    const presence = transport.kv("presence");
    expect(leases.ttlMs).toBe(LEASE_TTL_MS);
    expect(presence.ttlMs).toBe(PRESENCE_TTL_MS);

    const leaseKey = "ryzen2.h.lease-test";
    const first = await leases.put(leaseKey, "runtime-kv#one", { pid: 1 }, 0);
    expect((await leases.get<{ pid: number }>(leaseKey))?.value).toEqual({ pid: 1 });
    await expect(leases.put(leaseKey, "runtime-kv#two", { pid: 2 }, first - 1)).rejects.toMatchObject({ code: "FABRIC_KV_CONFLICT" });
    const renewed = await leases.put(leaseKey, "runtime-kv#one", { pid: 1 }, first);
    expect(await leases.delete(leaseKey, first)).toBe(false);
    expect(await leases.delete(leaseKey, renewed)).toBe(true);

    const controller = new AbortController();
    const iterator = presence.watch("ryzen2.>", controller.signal)[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ done: false, value: { type: "ready" } });
    const presenceKey = "ryzen2.p.participant-test";
    const presenceRevision = await presence.put(presenceKey, "runtime-kv#one", { live: true }, 0);
    expect((await iterator.next()).value).toMatchObject({ type: "put", entry: { key: presenceKey, revision: presenceRevision } });
    expect(await presence.delete(presenceKey, presenceRevision)).toBe(true);
    expect((await iterator.next()).value).toMatchObject({ type: "gone", key: presenceKey, reason: "deleted" });
    controller.abort();
    await iterator.return?.();

    const admin = await openNatsConnection({ servers: serverUrl, name: "test-kv-expiry", verifyMachine: false });
    try {
      const bucket = `TEST_LEASE_${Date.now()}`;
      const expiring = (await openNatsKv(admin.js, { bucket, ttlMs: 1_000, markerTtlMs: 1_000 })).withMachine("ryzen2");
      const key = "ryzen2.h.takeover-test";
      await expiring.put(key, "runtime-kv#one", { pid: 1 }, 0);
      await wait(2_500);
      expect(await expiring.get(key)).toBeUndefined();
      expect(await expiring.put(key, "runtime-kv#two", { pid: 2 }, 0)).toBeGreaterThan(0);
    } finally {
      await admin.close();
    }
  }, 30_000);
});
