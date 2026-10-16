import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { AckPolicy, DeliverPolicy, ReplayPolicy, type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import { PermissionViolationError, type NatsConnection } from "@nats-io/transport-node";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { consumeDeadLetters, ensureDeadLetterConsumer, type DeadLetterEnvelope, type DeadLetterSink } from "../src/transport/nats/dead-letter.js";
import { openNatsConnection, type NatsConnectionHandle } from "../src/transport/nats/connection.js";
import { ensureTransportStream } from "../src/transport/nats/provision.js";
import { ADVISORY_CAPTURE_STREAM, transportAdvisoryStreamSubject, transportDurableName, transportInboxFilter, transportInboxStream, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, resolveNatsBinary, PATH_B_MACHINES, PATH_B_POLICY_ENV, type PathBHarness, type PathBUser } from "./helpers/nats-cluster.js";

// Admin CLI: explicit test-only plaintext flag (never config/env) plus the expected-policy environment.
const INSECURE = "--allow-insecure-loopback";

// Keep reasons: trust boundary (real report-only credential), data safety (metadata only),
// and AckNone crash semantics across the broker/operator boundary. No broadened runtime role.
const binary = resolveNatsBinary();
if (!binary && process.env.NATS_AUTH_REQUIRED === "1") throw new Error("NATS_AUTH_REQUIRED=1 but no nats-server executable was found");
const API_TIMEOUT_MS = 2_500;
const OPERATOR_DURABLE = "transport_dlq_operator";
let fixture: PathBHarness;
let bootstrap: { manager: JetStreamManager; js: JetStreamClient };
let operator: { manager: JetStreamManager; js: JetStreamClient };
let operatorNc: NatsConnection;
let sender: { manager: JetStreamManager; js: JetStreamClient };
let receiver: { manager: JetStreamManager; js: JetStreamClient };
let connections: NatsConnectionHandle[] = [];
const captured: DeadLetterEnvelope[] = [];
const permissionErrors: string[] = [];

async function verified(user: PathBUser, harness = fixture): Promise<NatsConnectionHandle> {
  // Report-only operator connections pass explicit role:'operator'; every other identity is a strict agent.
  const connection = await openNatsConnection(user.credsPath === harness.users.maintainer.credsPath ? harness.operatorOptions(user) : harness.runtimeOptions(user));
  connections.push(connection);
  connection.onStatus((event) => {
    if (event.type === "error" && event.error instanceof PermissionViolationError) permissionErrors.push(event.error.subject);
  });
  return connection;
}

async function storedMessage(manager: JetStreamManager, stream: string, seq: number) {
  const msg = await manager.streams.getMessage(stream, { seq });
  if (!msg) throw new Error(`missing retained message ${stream}:${seq}`);
  return msg;
}

async function capture(sink: DeadLetterSink = { async put(record) { captured.push(record); } }): Promise<number> {
  const count = await consumeDeadLetters({ ...operator, sink, machines: PATH_B_MACHINES, signal: AbortSignal.timeout(10_000), once: true });
  await connections.find((connection) => connection.js === operator.js)!.nc.flush();
  expect(permissionErrors).toEqual([]); // Includes forbidden ACK/NAK/TERM attempts on malformed records.
  return count;
}

async function exhaust() {
  const hostId = `runtime:${randomUUID()}`;
  const replyTo = `session:${randomUUID()}`;
  const commandId = randomUUID();
  const commandEventId = randomUUID();
  const targetId = randomUUID();
  const kind = "command";
  const sourceMachine = "machine-a";
  const durable = transportDurableName(hostId);
  await receiver.manager.consumers.add(transportInboxStream("machine-b"), {
    durable_name: durable, filter_subject: transportInboxFilter("machine-b", hostId),
    ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
    ack_wait: 30_000_000_000, max_deliver: 5, max_ack_pending: 128,
  });
  const envelope = {
    id: commandEventId, kind,
    to: { machine: "machine-b", hostId }, targetId, operation: "followUp",
    from: { id: replyTo, name: "sender", kind: "main" }, createdAt: Date.now(),
    body: {
      version: 1, commandId, targetId, operation: "followUp", replyTo,
      requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
      message: "private command payload must not leak to the public DLQ",
      sourceMachine: "machine-b",
    },
  };
  const wire = { format: 1, sourceMachine, envelope };
  const subject = transportInboxSubject("machine-b", hostId, "cmd", targetId, sourceMachine);
  const sent = await sender.js.publish(subject, JSON.stringify(wire), { msgID: envelope.id });
  const consumer = await receiver.js.consumers.get(transportInboxStream("machine-b"), durable);
  for (let attempt = 1; attempt <= 5; attempt += 1) {
    const msg = await consumer.next({ expires: API_TIMEOUT_MS });
    expect(msg?.info.deliveryCount).toBe(attempt);
    if (!msg) throw new Error("runtime consumer did not deliver the test message");
    msg.nak();
  }
  // The next pull causes the server to observe exhaustion and emit the real standard advisory.
  expect(await consumer.next({ expires: 1_000 })).toBeNull();
  const adv = await bootstrap.manager.streams.info(ADVISORY_CAPTURE_STREAM);
  expect(adv.state.messages).toBe(1);
  const advisory = await storedMessage(bootstrap.manager, ADVISORY_CAPTURE_STREAM, adv.state.last_seq);
  const letter: DeadLetterEnvelope = {
    stream: sent.stream, consumer: durable, stream_seq: sent.seq, deliveries: 5,
  };
  return { sent, durable, subject, wire, replyTo, commandId, commandEventId, targetId, advisory, letter };
}

async function noPublicStream() {
  await expect(bootstrap.manager.streams.info("FABRIC_EV")).rejects.toThrow(/stream not found/i);
}

function adminCapture(file: string) {
  vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", fixture.users.maintainer.credsPath);
  vi.stubEnv("PI_FABRIC_DLQ_PATH", file);
  return runTransportAdmin(["dlq", "--once", INSECURE, "--machines", PATH_B_MACHINES.join(",")]);
}

beforeEach(async () => {
  if (!binary) throw new Error("real DLQ tests require NATS_BIN or nats-server on PATH");
  fixture = await createPathBHarness({ binary });
  bootstrap = await openJetStream(await fixture.connect(fixture.users.bootstrap));
  // Exercise the real provisioning path under the fixture-only bootstrap credential.
  // Neither runtime nor messaging operator receives any stream administration grant.
  vi.stubEnv("PI_FABRIC_MACHINE", "machine-a");
  vi.stubEnv("PI_FABRIC_NATS_SERVERS", fixture.serverUrl);
  vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", fixture.users.bootstrap.credsPath);
  for (const [name, value] of Object.entries(PATH_B_POLICY_ENV)) vi.stubEnv(name, value);
  await runTransportAdmin(["provision", INSECURE, "--machines", PATH_B_MACHINES.join(",")]);
  captured.length = 0;
  permissionErrors.length = 0;
  connections = [];
  operator = await verified(fixture.users.maintainer);
  // Separate native connection exposes real permission events without consuming the handle's monitor.
  operatorNc = await fixture.connect(fixture.users.maintainer);
  sender = await verified(fixture.users.machineA);
  receiver = await verified(fixture.users.machineB);
}, 60_000);

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(connections.map((connection) => connection.close()));
  if (fixture) await fixture.stop();
});

describe.skipIf(!binary)("standard durable MAX_DELIVERIES operator with actual Path B operator credentials", () => {
  it("refuses a missing bootstrap-owned advisory durable without trying to create or repair it", async () => {
    if (!binary) throw new Error("missing real nats-server binary");
    const missing = await createPathBHarness({ binary });
    let restricted: NatsConnectionHandle | undefined;
    try {
      const seed = await openJetStream(await missing.connect(missing.users.bootstrap));
      await seed.manager.streams.add({ name: ADVISORY_CAPTURE_STREAM, subjects: ["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"] });
      restricted = await openNatsConnection(missing.operatorOptions());
      await expect(consumeDeadLetters({ ...restricted, machines: PATH_B_MACHINES,
        sink: { async put() { throw new Error("no advisory should be delivered"); } },
        once: true, signal: AbortSignal.timeout(5_000),
      })).rejects.toThrow(/consumer not found/i);
      expect(await seed.manager.consumers.list(ADVISORY_CAPTURE_STREAM).next()).toEqual([]);
    } finally { await restricted?.close(); await missing.stop(); }
  });
  it("UPGRADE: bootstrap adds the missing AckNone durable, operator reports, repeat bootstrap is a no-op", async () => {
    if (!binary) throw new Error("missing real nats-server binary");
    const upgrade = await createPathBHarness({ binary });
    try {
      const seed = await openJetStream(await upgrade.connect(upgrade.users.bootstrap));
      await ensureTransportStream(seed.manager, {
        name: ADVISORY_CAPTURE_STREAM, subjects: [transportAdvisoryStreamSubject],
        maxBytes: 1_073_741_824, maxAgeMs: 30 * 24 * 60 * 60_000,
        maxMessageBytes: 64 * 1024, duplicateWindowMs: 10 * 60_000, replicas: 1,
      });
      await expect(seed.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).rejects.toThrow(/consumer not found/i);
      vi.stubEnv("PI_FABRIC_MACHINE", upgrade.users.bootstrap.machine);
      vi.stubEnv("PI_FABRIC_NATS_SERVERS", upgrade.serverUrl);
      vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", upgrade.users.bootstrap.credsPath);
      expect(await runTransportAdmin(["provision", INSECURE, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      const created = await seed.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
      expect(created.config.ack_policy).toBe(AckPolicy.None);
      const record = { stream: transportInboxStream("machine-b"), consumer: transportDurableName("upgrade"), stream_seq: 1, deliveries: 5 };
      await seed.js.publish(`$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.${record.stream}.${record.consumer}`,
        JSON.stringify({ type: "io.nats.jetstream.advisory.v1.max_deliver", ...record }));
      const log = path.join(upgrade.root, "upgrade-report.jsonl");
      vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", upgrade.users.maintainer.credsPath);
      vi.stubEnv("PI_FABRIC_DLQ_PATH", log);
      expect(await runTransportAdmin(["dlq", "--once", INSECURE, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      expect(JSON.parse(readFileSync(log, "utf8"))).toEqual(record);
      const before = await seed.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
      vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", upgrade.users.bootstrap.credsPath);
      expect(await runTransportAdmin(["provision", INSECURE, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
      const after = await seed.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
      expect(after.created).toBe(created.created);
      expect(after.config).toEqual(before.config);
      expect([after.delivered, after.ack_floor, after.num_pending, after.num_ack_pending])
        .toEqual([before.delivered, before.ack_floor, before.num_pending, before.num_ack_pending]);
    } finally { await upgrade.stop(); }
  }, 60_000);

  it.each([
    { ack_policy: AckPolicy.Explicit },
    { filter_subject: "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.FABRIC_IN_machine-a.>" },
    { deliver_policy: DeliverPolicy.New },
    { replay_policy: ReplayPolicy.Original },
    { inactive_threshold: 60_000_000_000 },
    { headers_only: true },
    { deliver_subject: "_INBOX.push-drift" },
  ])("refuses drift without bootstrap or operator repair: %j", async (drift) => {
    if (!binary) throw new Error("missing real nats-server binary");
    const isolated = await createPathBHarness({ binary });
    let restricted: NatsConnectionHandle | undefined;
    try {
      const seed = await openJetStream(await isolated.connect(isolated.users.bootstrap));
      await seed.manager.streams.add({ name: ADVISORY_CAPTURE_STREAM, subjects: [transportAdvisoryStreamSubject] });
      await seed.manager.consumers.add(ADVISORY_CAPTURE_STREAM, {
        durable_name: OPERATOR_DURABLE, filter_subject: transportAdvisoryStreamSubject,
        ack_policy: AckPolicy.None, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant, ...drift,
      });
      const before = await seed.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
      restricted = await openNatsConnection(isolated.operatorOptions());
      await expect(consumeDeadLetters({ ...restricted, machines: PATH_B_MACHINES,
        sink: { async put() { throw new Error("must not pull drifted consumer"); } }, once: true,
        signal: AbortSignal.timeout(5_000),
      })).rejects.toThrow("configuration drift");
      await expect(ensureDeadLetterConsumer(seed.manager)).rejects.toThrow("configuration drift");
      expect((await seed.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).config).toEqual(before.config);
    } finally { await restricted?.close(); await isolated.stop(); }
  }, 60_000);
  it("captures exactly standard MAX5 metadata while preserving the private inbox (E19)", async () => {
    const test = await exhaust();
    const before = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    expect(await capture()).toBe(1);
    expect(captured).toEqual([test.letter]);
    const info = await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
    expect(info.config).toMatchObject({ durable_name: OPERATOR_DURABLE, ack_policy: AckPolicy.None,
      deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant, filter_subject: transportAdvisoryStreamSubject });
    expect(info.config.deliver_subject).toBeUndefined();
    expect(info.config.inactive_threshold ?? 0).toBe(0);
    expect(info.num_ack_pending).toBe(0);
    expect((await storedMessage(bootstrap.manager, test.sent.stream, test.sent.seq)).json()).toEqual(test.wire);
    const after = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    expect([after.num_pending, after.num_ack_pending, after.ack_floor]).toEqual([before.num_pending, before.num_ack_pending, before.ack_floor]);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
    await noPublicStream();
  }, 30_000);

  it("persists only four fields and dedupes JSONL by tuple across CLI invocations", async () => {
    const test = await exhaust();
    const audit = path.join(fixture.root, "dead-letters.jsonl");
    expect(await adminCapture(audit)).toBe(0);
    const extra = { ...test.advisory.json<Record<string, unknown>>(), id: "untrusted-id", body: "private", reason: "invented", deliveries: 6 };
    await bootstrap.js.publish(test.advisory.subject, JSON.stringify(extra));
    expect(await adminCapture(audit)).toBe(0);
    expect(readFileSync(audit, "utf8").trim().split("\n").map((line) => JSON.parse(line))).toEqual([test.letter]);
    // A distinct tuple must not collide with the duplicate's supplied ID or field ordering.
    await bootstrap.js.publish(test.advisory.subject, JSON.stringify({ ...extra, stream_seq: test.sent.seq + 1 }));
    expect(await adminCapture(audit)).toBe(0);
    expect(readFileSync(audit, "utf8").trim().split("\n").map((line) => JSON.parse(line))).toEqual([
      test.letter, { ...test.letter, stream_seq: test.sent.seq + 1, deliveries: 6 },
    ]);
    await noPublicStream();
  }, 30_000);

  it("rejects advisory inventory/subject/shape attacks before a valid retained advisory", async () => {
    const consumer = transportDurableName("malformed");
    const base = { type: "io.nats.jetstream.advisory.v1.max_deliver", stream: transportInboxStream("machine-b"), consumer, stream_seq: 1, deliveries: 5 };
    const subject = `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.${base.stream}.${consumer}`;
    await bootstrap.js.publish(subject, "not json");
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, stream: "PRIVATE_OTHER_TENANT" }));
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, consumer: transportDurableName("somebody-else") }));
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, stream_seq: -1 }));
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, deliveries: 4 }));
    // --once takes just one record even when it is malformed; no speculative next delivery.
    for (let index = 0; index < 5; index += 1) {
      expect(await capture()).toBe(0);
      expect(captured).toEqual([]);
    }
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, body: "private", reason: "not metadata" }));
    expect(await capture()).toBe(1);
    expect(captured).toEqual([{ stream: base.stream, consumer, stream_seq: 1, deliveries: 5 }]);
    expect((await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).num_ack_pending).toBe(0);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("does not prefetch the next AckNone record; untaken advisories survive server restart", async () => {
    const test = await exhaust();
    await bootstrap.js.publish(test.advisory.subject, JSON.stringify({ ...test.advisory.json<Record<string, unknown>>(), stream_seq: test.sent.seq + 1 }));
    expect(await capture()).toBe(1);
    expect((await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).num_pending).toBe(1);
    await Promise.all(connections.map((connection) => connection.close()));
    connections = [];
    await fixture.restart();
    bootstrap = await openJetStream(await fixture.connect(fixture.users.bootstrap));
    operator = await verified(fixture.users.maintainer);
    expect(await capture()).toBe(1);
    expect(captured).toEqual([test.letter, { ...test.letter, stream_seq: test.sent.seq + 1 }]);
    expect((await operator.manager.streams.info(ADVISORY_CAPTURE_STREAM)).state.messages).toBe(2);
  }, 60_000);

  it("does not redeliver a taken record after sink crash/restart, but Limits retains it", async () => {
    const test = await exhaust();
    await expect(capture({ async put() { throw new Error("sink crash"); } })).rejects.toThrow("sink crash");
    await Promise.all(connections.map((connection) => connection.close()));
    connections = [];
    await fixture.restart();
    bootstrap = await openJetStream(await fixture.connect(fixture.users.bootstrap));
    operator = await verified(fixture.users.maintainer);
    expect(await capture()).toBe(0);
    expect(captured).toEqual([]);
    const info = await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
    expect([info.num_pending, info.num_ack_pending]).toEqual([0, 0]);
    // INFO gives retention counts, not authority to reconstruct exact records from those counts.
    expect((await operator.manager.streams.info(ADVISORY_CAPTURE_STREAM)).state.messages).toBe(1);
    expect((await storedMessage(bootstrap.manager, ADVISORY_CAPTURE_STREAM, test.advisory.seq)).json()).toEqual(test.advisory.json());
  }, 60_000);

  it("drain reports real pending, ACK-pending and ACK floor without inventing a rollback receipt (E16c)", async () => {
    const test = await exhaust();
    const info = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    const hostId = test.wire.envelope.to.hostId;
    vi.stubEnv("PI_FABRIC_MACHINE", "machine-b");
    vi.stubEnv("PI_FABRIC_HOST_ID", hostId);
    vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", fixture.users.machineB.credsPath);
    let output = "";
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation((chunk) => {
      output += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
      return true;
    });
    try { expect(await runTransportAdmin(["drain", INSECURE])).toBe(0); }
    finally { stdout.mockRestore(); }
    expect(JSON.parse(output)).toEqual({ machine: "machine-b", hostId, pending: info.num_pending, ackPending: info.num_ack_pending, ackFloor: info.ack_floor });
    expect((await storedMessage(bootstrap.manager, test.sent.stream, test.sent.seq)).json()).toEqual(test.wire);
  });

  it("T-ORPHAN: reports only a named inbox's counters and ACK floor without disposition or mutation (E15)", async () => {
    const hostId = `runtime:${randomUUID()}`;
    const stream = transportInboxStream("machine-b");
    const durable = transportDurableName(hostId);
    await receiver.manager.consumers.add(stream, {
      durable_name: durable, filter_subject: transportInboxFilter("machine-b", hostId),
      ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
      ack_wait: 30_000_000_000, max_deliver: 5, max_ack_pending: 128,
    });
    const subject = transportInboxSubject("machine-b", hostId, "cmd", "orphan-target", "machine-a");
    const sent = await Promise.all([1, 2, 3].map((n) => sender.js.publish(subject, `retained-orphan-message-${n}`)));
    const consumer = await receiver.js.consumers.get(stream, durable);
    const first = await consumer.next({ expires: API_TIMEOUT_MS });
    const second = await consumer.next({ expires: API_TIMEOUT_MS });
    if (!first || !second) throw new Error("orphan fixture did not deliver its two messages");
    // A deliberately out-of-order owner ACK leaves a gap. The operator must not infer an exact
    // pending set from the floor, pull the gap, or dispose of either remaining message.
    expect(await second.ackAck()).toBe(true);
    const before = await receiver.manager.consumers.info(stream, durable);
    expect({ pending: before.num_pending, ackPending: before.num_ack_pending }).toEqual({ pending: 1, ackPending: 1 });
    vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", fixture.users.maintainer.credsPath);
    let output = "";
    const stdout = vi.spyOn(process.stdout, "write").mockImplementation((chunk) => {
      output += typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
      return true;
    });
    try {
      expect(await runTransportAdmin(["orphan", INSECURE, "--machine", "machine-b", "--host-id", hostId, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
    } finally { stdout.mockRestore(); }
    expect(JSON.parse(output)).toEqual({
      ok: true, mode: "report-only", disposition: "none", machine: "machine-b", hostId, stream, consumer: durable,
      pending: before.num_pending, ackPending: before.num_ack_pending, ackFloor: before.ack_floor,
      num_pending: before.num_pending, num_ack_pending: before.num_ack_pending, ack_floor: before.ack_floor,
    });
    const after = await receiver.manager.consumers.info(stream, durable);
    expect({ pending: after.num_pending, ackPending: after.num_ack_pending, ackFloor: after.ack_floor }).toEqual({ pending: before.num_pending, ackPending: before.num_ack_pending, ackFloor: before.ack_floor });
    const policy = await bootstrap.manager.streams.info(stream);
    expect(policy.config.max_age).toBe(0);
    expect(policy.config.discard).toBe("new");
    expect(policy.config.max_bytes).toBe(1_073_741_824);
    expect(policy.state.messages).toBe(3);
    expect(await Promise.all(sent.map(async (ack) => (await storedMessage(bootstrap.manager, stream, ack.seq)).string()))).toEqual(["retained-orphan-message-1", "retained-orphan-message-2", "retained-orphan-message-3"]);
    await noPublicStream();
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("refuses unnamed/out-of-inventory orphan reports rather than enumerating private consumers", async () => {
    vi.stubEnv("PI_FABRIC_HOST_ID", "");
    await expect(runTransportAdmin(["orphan", INSECURE])).rejects.toThrow("--machine");
    await expect(runTransportAdmin(["orphan", INSECURE, "--machine", "ungranted-machine", "--host-id", "runtime:test", "--machines", PATH_B_MACHINES.join(",")])).rejects.toThrow("outside the configured inventory");
    await expect(runTransportAdmin(["orphan", INSECURE, "--machine", "machine-b", "--machines", PATH_B_MACHINES.join(",")])).rejects.toThrow("--host-id");
    await noPublicStream();
  });

  it("T-OPS-NOREAD: actual operator denies private reads, every ACK form, relay/events and lifecycle", async () => {
    const test = await exhaust();
    const before = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    const subjects = [
      ...[...PATH_B_MACHINES.map(transportInboxStream), ADVISORY_CAPTURE_STREAM, "UNLISTED_PRIVATE"].flatMap((stream) => [
        `$JS.API.STREAM.MSG.GET.${stream}`, `$JS.API.DIRECT.GET.${stream}`, `$JS.API.DIRECT.GET.${stream}.private.subject`,
        ...["+ACK", "-NAK", "+TERM"].map((_, index) => `$JS.ACK.${stream}.probe.${index + 1}.1.1.1.0`),
      ]),
      `$JS.ACK.${ADVISORY_CAPTURE_STREAM}.${OPERATOR_DURABLE}.1.1.1.1.0`,
      transportInboxSubject("machine-a", "session:denied-relay", "ack", "target-denied", "fabric-ops"),
      "fabric.ev.fabric-ops.fabric.ops.dlq",
      "fabric.ev.fabric-ops.fabric.ops.owner.alarm",
      "$JS.API.CONSUMER.INFO.FABRIC_IN_unlisted.other",
      "$JS.API.STREAM.INFO.FABRIC_IN_unlisted",
      `$JS.API.CONSUMER.LIST.${test.sent.stream}`,
      `$JS.API.CONSUMER.MSG.NEXT.${test.sent.stream}.${test.durable}`,
      `$JS.ACK.${test.sent.stream}.${test.durable}.1.1.1.1.0`,
      `$JS.API.CONSUMER.DELETE.${test.sent.stream}.${test.durable}`,
      `$JS.API.CONSUMER.CREATE.${ADVISORY_CAPTURE_STREAM}.${OPERATOR_DURABLE}`,
      `$JS.API.CONSUMER.DELETE.${ADVISORY_CAPTURE_STREAM}.${OPERATOR_DURABLE}`,
      `$JS.API.CONSUMER.MSG.NEXT.${ADVISORY_CAPTURE_STREAM}.other_operator`,
      `$JS.API.STREAM.MSG.DELETE.${test.sent.stream}`,
      `$JS.API.STREAM.PURGE.${test.sent.stream}`,
      `$JS.API.STREAM.UPDATE.${test.sent.stream}`,
    ];
    // Listen to the native permission events; no unbounded polling or timeout-as-success oracle.
    const status = operatorNc.status();
    const errors = (async () => {
      const denied: string[] = [];
      for await (const event of status) {
        if (event.type === "error" && event.error instanceof PermissionViolationError) {
          denied.push(event.error.subject);
          if (denied.length === subjects.length) break;
        }
      }
      return denied;
    })();
    try {
      subjects.forEach((subject) => {
        const ackIndex = subject.startsWith("$JS.ACK.") ? Number(subject.split(".").at(-5)) - 1 : -1;
        operatorNc.publish(subject, ["+ACK", "-NAK", "+TERM"][ackIndex] ?? "{}");
      });
      await operatorNc.flush();
      const timeout = AbortSignal.timeout(5_000);
      const denied = await Promise.race([errors, new Promise<never>((_, reject) => timeout.addEventListener("abort", () => reject(new Error("missing operator permission denial")), { once: true }))]);
      expect(denied).toEqual(subjects);
    } finally { await operatorNc.close(); }
    const after = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    expect({ pending: after.num_pending, ackPending: after.num_ack_pending, ackFloor: after.ack_floor }).toEqual({ pending: before.num_pending, ackPending: before.num_ack_pending, ackFloor: before.ack_floor });
    expect((await storedMessage(bootstrap.manager, test.sent.stream, test.sent.seq)).json()).toEqual(test.wire);
    await noPublicStream();
  }, 30_000);
});
