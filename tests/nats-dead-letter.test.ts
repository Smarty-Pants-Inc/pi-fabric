import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import { AckPolicy, DeliverPolicy, ReplayPolicy, type JetStreamClient, type JetStreamManager } from "@nats-io/jetstream";
import { PermissionViolationError, type NatsConnection } from "@nats-io/transport-node";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { runTransportAdmin } from "../src/transport/nats/admin.js";
import { consumeDeadLetters, createDeadLetterPublisher, deadLetterId, type DeadLetterEnvelope } from "../src/transport/nats/dead-letter.js";
import { ADVISORY_CAPTURE_STREAM, transportDurableName, transportInboxFilter, transportInboxStream, transportInboxSubject } from "../src/transport/nats/subjects.js";
import { createPathBHarness, openJetStream, resolveNatsBinary, PATH_B_MACHINES, type PathBHarness } from "./helpers/nats-cluster.js";

// Keep reasons: trust boundary (credential-bound routing), data safety (no original ACK/delete),
// and crash recovery across the broker/operator boundary. No external server or broadened role.
const binary = resolveNatsBinary();
const API_TIMEOUT_MS = 2_500;
const OPERATOR_DURABLE = "transport_dlq_operator";
let fixture: PathBHarness;
let bootstrap: { manager: JetStreamManager; js: JetStreamClient };
let operator: { manager: JetStreamManager; js: JetStreamClient };
let operatorNc: NatsConnection;
let sender: { manager: JetStreamManager; js: JetStreamClient };
let receiver: { manager: JetStreamManager; js: JetStreamClient };

async function storedMessage(manager: JetStreamManager, stream: string, seq: number) {
  const msg = await manager.streams.getMessage(stream, { seq });
  if (!msg) throw new Error(`missing retained message ${stream}:${seq}`);
  return msg;
}

async function capture(sink = createDeadLetterPublisher({ js: operator.js, machine: "fabric-ops", machines: PATH_B_MACHINES })): Promise<number> {
  return consumeDeadLetters({ ...operator, sink, machines: PATH_B_MACHINES, signal: AbortSignal.timeout(10_000), once: true });
}

async function exhaust(options: { kind?: "command" | "ack"; corruptSource?: boolean; sourceMachine?: "machine-a" | "machine-b" } = {}) {
  const hostId = `runtime:${randomUUID()}`;
  const replyTo = `session:${randomUUID()}`;
  const commandId = randomUUID();
  const commandEventId = randomUUID();
  const targetId = randomUUID();
  const kind = options.kind ?? "command";
  const sourceMachine = options.sourceMachine ?? "machine-a";
  const durable = transportDurableName(hostId);
  await receiver.manager.consumers.add(transportInboxStream("machine-b"), {
    durable_name: durable, filter_subject: transportInboxFilter("machine-b", hostId),
    ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, replay_policy: ReplayPolicy.Instant,
    ack_wait: 30_000_000_000, max_deliver: 5, max_ack_pending: 128,
  });
  const envelope = {
    id: kind === "command" ? commandEventId : `ack:${commandEventId}`, kind,
    to: { machine: "machine-b", hostId }, targetId, operation: kind === "command" ? "followUp" : "accepted",
    from: { id: replyTo, name: "sender", kind: "main" }, createdAt: Date.now(),
    body: kind === "command" ? {
      version: 1, commandId, targetId, operation: "followUp", replyTo,
      requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
      message: "private command payload must not leak to the public DLQ",
      // This untrusted field must never select the relay machine.
      sourceMachine: "machine-b",
    } : { version: 1, commandId, targetId, accepted: true },
  };
  const wire = { format: 1, sourceMachine: options.corruptSource ? "machine-b" : sourceMachine, envelope };
  const subject = transportInboxSubject("machine-b", hostId, kind === "command" ? "cmd" : "ack", targetId, sourceMachine);
  const sent = await (sourceMachine === "machine-a" ? sender : receiver).js.publish(subject, JSON.stringify(wire), { msgID: envelope.id });
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
    id: deadLetterId({ stream: sent.stream, consumer: durable, streamSeq: sent.seq }),
    stream: sent.stream, consumer: durable, streamSequence: sent.seq, deliveries: 5,
    original: Buffer.from(JSON.stringify(wire)), originalSubject: subject, originalExpired: false,
    advisory: advisory.json(),
  };
  return { sent, durable, subject, wire, replyTo, commandId, commandEventId, targetId, advisory, letter };
}

async function publicRecords() {
  const info = await bootstrap.manager.streams.info("FABRIC_EV");
  const records: Array<{ subject: string; id: string; topic: string; data: Record<string, unknown>; sourceMachine: string }> = [];
  for (let seq = info.state.first_seq; seq <= info.state.last_seq && info.state.messages > 0; seq += 1) {
    const msg = await storedMessage(bootstrap.manager, "FABRIC_EV", seq);
    records.push({ ...msg.json<{ id: string; topic: string; data: Record<string, unknown>; sourceMachine: string }>(), subject: msg.subject });
  }
  return records;
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
  await runTransportAdmin(["provision", "--machines", PATH_B_MACHINES.join(",")]);
  operatorNc = await fixture.connect(fixture.users.maintainer);
  operator = await openJetStream(operatorNc);
  sender = await openJetStream(await fixture.connect(fixture.users.machineA));
  receiver = await openJetStream(await fixture.connect(fixture.users.machineB));
}, 60_000);

afterEach(async () => {
  vi.unstubAllEnvs();
  if (fixture) await fixture.stop();
});

describe("standard durable MAX_DELIVERIES operator with actual Path B operator credentials", () => {
  it("refuses a missing bootstrap-owned advisory durable without trying to create or repair it", async () => {
    if (!binary) throw new Error("missing real nats-server binary");
    const missing = await createPathBHarness({ binary });
    try {
      const seed = await openJetStream(await missing.connect(missing.users.bootstrap));
      await seed.manager.streams.add({ name: ADVISORY_CAPTURE_STREAM, subjects: ["$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>"] });
      const restricted = await openJetStream(await missing.connect(missing.users.maintainer));
      await expect(consumeDeadLetters({ ...restricted, machines: PATH_B_MACHINES,
        sink: { async put() { throw new Error("no advisory should be delivered"); } },
        once: true, signal: AbortSignal.timeout(5_000),
      })).rejects.toThrow(/consumer not found/i);
      expect(await seed.manager.consumers.list(ADVISORY_CAPTURE_STREAM).next()).toEqual([]);
    } finally { await missing.stop(); }
  });
  it("recovers retained advisories after operator downtime and durably publishes a DLQ, owner alarm and outcome-unknown relay", async () => {
    const test = await exhaust();
    const before = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    expect(await capture()).toBe(1);
    const records = await publicRecords();
    expect(records).toHaveLength(2);
    expect(records[0]).toMatchObject({ id: test.letter.id, topic: "fabric.ops.dlq", sourceMachine: "fabric-ops", subject: "fabric.ev.fabric-ops.fabric.ops.dlq",
      data: { stream: test.sent.stream, consumer: test.durable, streamSequence: test.sent.seq, disposition: "outcome-unknown" } });
    expect(records[1]).toMatchObject({ id: `${test.letter.id}:alarm`, topic: "fabric.ops.owner.alarm" });
    expect(JSON.stringify(records)).not.toContain("private command payload");
    const stored = await storedMessage(bootstrap.manager, transportInboxStream("machine-a"), 1);
    expect(stored.subject).toBe(transportInboxSubject("machine-a", test.replyTo, "ack", test.targetId, "fabric-ops"));
    const relay = stored.json<{ format: number; sourceMachine: string; envelope: { id: string; body: Record<string, unknown> } }>();
    expect(relay).toMatchObject({ format: 1, sourceMachine: "fabric-ops", envelope: { id: `relay:${test.commandEventId}`, kind: "ack", operation: "rejected", to: { machine: "machine-a", hostId: test.replyTo } } });
    expect(test.commandEventId).not.toBe(test.commandId);
    expect(relay.envelope.id).not.toBe(`relay:${test.commandId}`);
    expect(relay.envelope.body).toEqual({
      version: 1, commandId: test.commandId, commandEventId: test.commandEventId, targetId: test.targetId, accepted: false,
      error: "Relay destination unavailable", errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", retryable: true,
    });
    expect(relay.envelope.body).not.toHaveProperty("notRun");
    expect(relay.envelope.body).not.toHaveProperty("notDelivered");
    expect(relay.envelope.body).not.toHaveProperty("ownerIncarnation");
    const operatorInfo = await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE);
    expect(operatorInfo.num_ack_pending).toBe(0);
    expect(operatorInfo.config.max_deliver).toBe(-1);
    expect((await storedMessage(bootstrap.manager, test.sent.stream, test.sent.seq)).json()).toEqual(test.wire);
    const ownerInfo = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    expect(ownerInfo.ack_floor.stream_seq).toBe(0);
    expect({ pending: ownerInfo.num_pending, ackPending: ownerInfo.num_ack_pending, ackFloor: ownerInfo.ack_floor }).toEqual({ pending: before.num_pending, ackPending: before.num_ack_pending, ackFloor: before.ack_floor });

    // A second retained copy of the same advisory gets reconciled by all three stable Nats-Msg-Id values.
    await bootstrap.js.publish(test.advisory.subject, test.advisory.data);
    expect(await capture()).toBe(1);
    expect(await publicRecords()).toEqual(records);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(1);
  }, 30_000);

  it("reserves the whole operator namespace and accepts literal fabric-ops role variants (E16b)", () => {
    expect(() => createDeadLetterPublisher({ js: operator.js, machine: "machine-a", machines: PATH_B_MACHINES })).toThrow("fabric-ops namespace operator");
    expect(() => createDeadLetterPublisher({ js: operator.js, machine: "fabric-ops", machines: [...PATH_B_MACHINES, "fabric-ops"] })).toThrow("excluding the fabric-ops namespace");
    expect(() => createDeadLetterPublisher({ js: operator.js, machine: "fabric-ops", machines: [...PATH_B_MACHINES, "fabric-ops-other1"] })).toThrow("excluding the fabric-ops namespace");
    expect(() => createDeadLetterPublisher({ js: operator.js, machine: "fabric-ops-canary2", machines: PATH_B_MACHINES })).not.toThrow();
    expect(() => createDeadLetterPublisher({ js: operator.js, machines: PATH_B_MACHINES })).not.toThrow();
  });

  it("can publish from an operator machine outside the runtime inventory without widening its grants", async () => {
    const test = await exhaust({ sourceMachine: "machine-b" });
    const publisher = createDeadLetterPublisher({ js: operator.js, machine: "fabric-ops", machines: ["machine-b"] });
    expect(await capture(publisher)).toBe(1);
    expect((await publicRecords())[0]?.data.disposition).toBe("outcome-unknown");
    const relay = await storedMessage(bootstrap.manager, transportInboxStream("machine-b"), 2);
    expect(relay.subject).toBe(transportInboxSubject("machine-b", test.replyTo, "ack", test.targetId, "fabric-ops"));
    expect(relay.json()).toMatchObject({ sourceMachine: "fabric-ops", envelope: { id: `relay:${test.commandEventId}`, to: { machine: "machine-b", hostId: test.replyTo } } });
  });

  it("uses the real admin dlq path, not a local-JSONL-only substitute", async () => {
    const test = await exhaust();
    const audit = path.join(fixture.root, "dead-letters.jsonl");
    vi.stubEnv("PI_FABRIC_MACHINE", "machine-a");
    vi.stubEnv("PI_FABRIC_NATS_SERVERS", fixture.serverUrl);
    vi.stubEnv("PI_FABRIC_NATS_CREDENTIALS", fixture.users.maintainer.credsPath);
    vi.stubEnv("PI_FABRIC_DLQ_PATH", audit);
    expect(await runTransportAdmin(["dlq", "--once", "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
    expect(await publicRecords()).toHaveLength(2);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(1);
    const local = JSON.parse(readFileSync(audit, "utf8")) as Record<string, unknown>;
    expect(local.id).toBe(test.letter.id);
    expect(local.originalSubject).toBe(test.subject);
  }, 30_000);

  it("publishes only a DLQ and alarm for an exhausted ACK, never a relay-of-a-relay", async () => {
    await exhaust({ kind: "ack" });
    expect(await capture()).toBe(1);
    expect((await publicRecords())[0]?.data.disposition).toBe("ack-exhausted");
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("does not trust a sourceMachine field that disagrees with the credential-bound original subject", async () => {
    await exhaust({ corruptSource: true });
    expect(await capture()).toBe(1);
    expect((await publicRecords())[0]?.data.disposition).toBe("invalid-original");
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("rejects advisory inventory/subject/shape attacks before a valid retained advisory", async () => {
    const consumer = transportDurableName("malformed");
    const base = { type: "io.nats.jetstream.advisory.v1.max_deliver", stream: transportInboxStream("machine-b"), consumer, stream_seq: 1, deliveries: 5 };
    const subject = `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.${base.stream}.${consumer}`;
    await bootstrap.js.publish(subject, "not json");
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, stream: "PRIVATE_OTHER_TENANT" }));
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, consumer: transportDurableName("somebody-else") }));
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, stream_seq: -1 }));
    await bootstrap.js.publish(subject, JSON.stringify({ ...base, deliveries: 4 }));
    // A valid advisory with an original which genuinely isn't present: alarm without an invented relay route.
    await bootstrap.js.publish(subject, JSON.stringify(base));
    expect(await capture()).toBe(1);
    const records = await publicRecords();
    expect(records).toHaveLength(2);
    expect(records[0]?.data).toMatchObject({ originalExpired: true, disposition: "original-unavailable" });
    expect((await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).num_ack_pending).toBe(0);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("does not ACK an advisory or invent a relay when the broker refuses a required public alarm publication", async () => {
    await exhaust();
    // Only the fixture bootstrap scope changes this owned stream's capacity. The operator just publishes.
    await bootstrap.manager.streams.update("FABRIC_EV", { max_msgs: 1 });
    await expect(capture()).rejects.toBeInstanceOf(Error);
    expect((await publicRecords()).map((record) => record.topic)).toEqual(["fabric.ops.dlq"]);
    expect((await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).num_ack_pending).toBe(1);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("does not ACK the advisory if a disposition fails after PubAck; a retry reconciles the durable publications", async () => {
    const test = await exhaust();
    const publisher = createDeadLetterPublisher({ js: operator.js, machine: "fabric-ops", machines: PATH_B_MACHINES });
    await expect(capture({ async put(letter) { await publisher.put(letter); throw new Error("crash after publication before advisory ACK"); } })).rejects.toThrow("crash after publication");
    expect((await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).num_ack_pending).toBe(1);
    expect(await publicRecords()).toHaveLength(2);
    // Restart the operator, preserving its durable consumer. AckWait is 30 s, no private pull or ACK workaround.
    expect(await consumeDeadLetters({ ...operator, sink: publisher, machines: PATH_B_MACHINES, signal: AbortSignal.timeout(40_000), once: true })).toBe(1);
    expect(await publicRecords()).toHaveLength(2);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(1);
    expect((await operator.manager.consumers.info(ADVISORY_CAPTURE_STREAM, OPERATOR_DURABLE)).num_ack_pending).toBe(0);
    expect((await storedMessage(bootstrap.manager, test.sent.stream, test.sent.seq)).json()).toEqual(test.wire);
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
    try { expect(await runTransportAdmin(["drain"])).toBe(0); }
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
      expect(await runTransportAdmin(["orphan", "--machine", "machine-b", "--host-id", hostId, "--machines", PATH_B_MACHINES.join(",")])).toBe(0);
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
    expect((await bootstrap.manager.streams.info("FABRIC_EV")).state.messages).toBe(0);
    expect((await bootstrap.manager.streams.info(transportInboxStream("machine-a"))).state.messages).toBe(0);
  });

  it("refuses unnamed/out-of-inventory orphan reports rather than enumerating private consumers", async () => {
    vi.stubEnv("PI_FABRIC_HOST_ID", "");
    await expect(runTransportAdmin(["orphan"])).rejects.toThrow("--machine");
    await expect(runTransportAdmin(["orphan", "--machine", "ungranted-machine", "--host-id", "runtime:test", "--machines", PATH_B_MACHINES.join(",")])).rejects.toThrow("outside the configured inventory");
    await expect(runTransportAdmin(["orphan", "--machine", "machine-b", "--machines", PATH_B_MACHINES.join(",")])).rejects.toThrow("--host-id");
    expect((await bootstrap.manager.streams.info("FABRIC_EV")).state.messages).toBe(0);
  });

  it("the actual operator cannot list/pull/ack/delete private consumers or administer streams", async () => {
    const test = await exhaust();
    const before = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    const subjects = [
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
      subjects.forEach((subject) => operatorNc.publish(subject, "{}"));
      await operatorNc.flush();
      const timeout = AbortSignal.timeout(5_000);
      const denied = await Promise.race([errors, new Promise<never>((_, reject) => timeout.addEventListener("abort", () => reject(new Error("missing operator permission denial")), { once: true }))]);
      expect(denied).toEqual(subjects);
    } finally { await operatorNc.close(); }
    const after = await receiver.manager.consumers.info(test.sent.stream, test.durable);
    expect({ pending: after.num_pending, ackPending: after.num_ack_pending, ackFloor: after.ack_floor }).toEqual({ pending: before.num_pending, ackPending: before.num_ack_pending, ackFloor: before.ack_floor });
    expect((await storedMessage(bootstrap.manager, test.sent.stream, test.sent.seq)).json()).toEqual(test.wire);
    expect((await bootstrap.manager.streams.info("FABRIC_EV")).state.messages).toBe(0);
  }, 30_000);
});
