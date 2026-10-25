// Lane L2 (smarty-dev#15310): the file <-> NATS control carrier, every op, both directions.
import { describe, expect, it } from "vitest";
import type { BridgePublish } from "../src/mesh/bridge.js";
import {
  NatsBridgeCarrier, NatsCarrierRefusal, NatsCarrierUnavailable, boundNatsAck, type NatsCarrierAuthorize, type NatsInbound,
} from "../src/mesh/bridge-nats-carrier.js";
import { bridgeNatsEnvelopeId, bridgeNatsMappedCommandId } from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { STALE_INCARNATION_ERROR } from "../src/topology/control-plane.js";
import {
  BRIDGE_MACHINE, FakePresence, LINK, MemoryLedger, POD_MACHINE, fileLead, natsLead, otherNats,
} from "./helpers/bridge-nats-l2-fakes.js";

const T = Date.now();
const allow: NatsCarrierAuthorize = () => ({ ok: true, principal: "carry" });
const principal: BridgePublish["principal"] = { id: "principal:paul", binding: "herdr-client" };

const setup = (authorize: NatsCarrierAuthorize = allow) => {
  const presence = new FakePresence();
  const ledger = new MemoryLedger();
  const carrier = new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger, authorize, now: () => T });
  return { presence, ledger, carrier };
};

const fileCommand = (data: Record<string, unknown> = {}, extra: Partial<BridgePublish> = {}): BridgePublish => ({
  topic: "fabric.control.command", kind: (data.operation as string | undefined) ?? "followUp", from: fileLead, to: natsLead.id,
  data: {
    version: 1, commandId: "file-cmd-1", targetId: natsLead.id, operation: "followUp", replyTo: fileLead.id,
    ownerIncarnation: "inc-moved-lead", message: "hello v2", requestCreatedAt: T, requestedAt: T, deadlineAt: T + 30_000,
    destinationRemoteHost: LINK, ...data, bridge: { from: "ryzen1", id: (data.eventId as string | undefined) ?? "file-event-1" },
  } as BridgePublish["data"],
  ...extra,
});

const natsCommand = (body: Record<string, unknown> = {}, envelope: Record<string, unknown> = {}) => ({
  id: "nats-env-1", kind: "command", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: fileLead.id,
  operation: (body.operation as string | undefined) ?? "ask", from: natsLead,
  body: { version: 1, commandId: "nats-cmd-1", targetId: fileLead.id, operation: "ask", ownerIncarnation: "inc-file-lead",
    message: "what changed?", replyTo: natsLead.id, requestCreatedAt: T, requestedAt: T, deadlineAt: T + 60_000, ...body },
  ...envelope,
});

const delivery = (envelope: unknown, sourceMachine = POD_MACHINE, sequence = 7) => ({ envelope, sourceMachine, sequence });

const expectRefusal = async (work: Promise<unknown>, reason: RegExp): Promise<void> => {
  const error = await work.then(() => undefined, (caught: unknown) => caught);
  expect(error).toBeInstanceOf(NatsCarrierRefusal);
  // MeshBridge drops (never retries) an error whose message says "not allowed".
  expect((error as Error).message).toMatch(/not allowed/);
  expect((error as NatsCarrierRefusal).reason).toMatch(reason);
};

const sendOf = (plan: Awaited<ReturnType<NatsBridgeCarrier["outbound"]>>) => {
  if (plan.kind !== "send") throw new Error(`expected send, got ${plan.kind}`);
  return plan;
};

describe("file -> NATS commands", () => {
  it("maps a followUp to a fixed NATS envelope: verified sender, mapped id, deterministic id, digest the receiver computes", async () => {
    const { carrier, ledger } = setup();
    const pub = fileCommand({}, { principal, from: { ...fileLead, name: "forged-name" } });
    const plan = sendOf(await carrier.outbound(pub));
    const mapped = bridgeNatsMappedCommandId(LINK, "toNats", fileLead.id, "file-cmd-1");
    expect(plan.envelope).toMatchObject({
      id: bridgeNatsEnvelopeId(LINK, "file-event-1"), kind: "command", operation: "followUp", targetId: natsLead.id,
      to: { machine: POD_MACHINE, hostId: natsLead.id }, from: fileLead, principal,
    });
    expect(plan.envelope.body).toEqual({ version: 1, commandId: mapped, targetId: natsLead.id, operation: "followUp",
      replyTo: fileLead.id, ownerIncarnation: "inc-moved-lead", message: "hello v2", requestCreatedAt: T, requestedAt: T,
      deadlineAt: T + 30_000 });
    // The NATS receiver digests the raw wire body + principal; the ACK must echo exactly that.
    expect(plan.record!.bodyDigest).toBe(controlBodyFingerprint(plan.envelope.body, plan.envelope.principal));
    expect(plan.record).toMatchObject({ direction: "toNats", origin: "control", sourceCommandId: "file-cmd-1",
      destinationCommandId: mapped, ownerMachine: POD_MACHINE, requesterId: fileLead.id, replyTo: fileLead.id });
    // A resend (restart, uncertain PubAck) is the first envelope, byte for byte.
    const again = sendOf(await carrier.outbound(pub));
    expect(JSON.stringify(again.envelope)).toBe(JSON.stringify(plan.envelope));
    expect(ledger.records.size).toBe(1);
  });

  it("a same-key resend follows L3's claim table: resend unconfirmed bytes, await a sent one, replay a recorded answer", async () => {
    const { carrier, ledger } = setup();
    const first = sendOf(await carrier.outbound(fileCommand()));
    const again = () => carrier.outbound(fileCommand({ eventId: "file-event-2", requestedAt: T + 5, deadlineAt: T + 30_005 }));
    // Unconfirmed: the first envelope again, byte for byte.
    expect(JSON.stringify(sendOf(await again()).envelope)).toBe(JSON.stringify(first.envelope));
    // The destination confirmed it: nothing to send, the owner's ACK arrives on its own (never blocks).
    await carrier.sent(first.record!, 3);
    expect(await again()).toMatchObject({ kind: "absorb" });
    // The owner answered: the recorded answer goes back to this event's sender at once, nothing to NATS.
    const recorded = { version: 1, commandId: "file-cmd-1", targetId: natsLead.id, accepted: true, messageId: "m-1", ownerIncarnation: "inc-moved-lead" };
    await carrier.settle({ direction: "toNats", commandId: first.record!.destinationCommandId, accepted: true, fromId: natsLead.id,
      ackEventId: `ack:${first.envelope.id}`, data: recorded }, true);
    expect(await again()).toEqual({ kind: "answer", reason: "answered from the recorded outcome",
      answer: { to: fileLead.id, from: natsLead, stampId: "replay:file-event-2", data: recorded } });
    expect(ledger.records.size).toBe(1);
  });

  it("carries steer, stop, ask and a legacy command without deadlineAt", async () => {
    const { carrier } = setup();
    for (const [index, operation] of ["steer", "stop", "ask"].entries()) {
      const plan = sendOf(await carrier.outbound(fileCommand({ operation, commandId: `c-${operation}`, eventId: `e-${index}`,
        ...(operation === "steer" ? { priority: "interrupt" } : {}) })));
      expect(plan.envelope.operation).toBe(operation);
      expect(plan.envelope.body.operation).toBe(operation);
    }
    const legacy = sendOf(await carrier.outbound(fileCommand({ commandId: "legacy", eventId: "e-legacy", deadlineAt: undefined })));
    expect(legacy.envelope.body.deadlineAt).toBe(T + 60_000);
  });

  it("refuses unverified senders, foreign replyTo, loops, unknown targets, gate refusals and reused ids", async () => {
    const { carrier, presence } = setup();
    await expectRefusal(carrier.outbound(fileCommand({}, { from: { id: "session:nobody", name: "x", kind: "main" } })), /live file participant/);
    await expectRefusal(carrier.outbound(fileCommand({ replyTo: otherNats.id })), /replyTo/);
    // A target this bridge mirrors itself would loop the command back into the file mesh.
    await expectRefusal(carrier.outbound(fileCommand({ targetId: fileLead.id }, { to: fileLead.id })), /file participant this bridge mirrors/);
    await expectRefusal(carrier.outbound(fileCommand({}, { to: otherNats.id })), /NATS owner/);
    // A KV gap is a wait, never a drop: retryable (not "not allowed"), so MeshBridge keeps its cursor.
    const missing = await carrier.outbound(fileCommand({ targetId: "session:gone" }, { to: "session:gone" })).catch((caught: unknown) => caught);
    expect(missing).toBeInstanceOf(NatsCarrierUnavailable);
    expect((missing as Error).message).not.toMatch(/exceeds|not allowed|no bridge stamp|no recipient|no sender|Invalid mesh topic/i);
    presence.natsEntries.set(natsLead.id, { ...presence.natsEntries.get(natsLead.id)!, route: { ...presence.natsEntries.get(natsLead.id)!.route, live: false } });
    await expect(carrier.outbound(fileCommand())).rejects.toBeInstanceOf(NatsCarrierUnavailable);
    const gated = setup(() => ({ ok: false, refuse: "BRIDGE_NATS_OPERATION_NOT_ALLOWED", reason: "seat receives followUp and cancel only" }));
    const error = await gated.carrier.outbound(fileCommand({ operation: "steer" })).catch((caught: unknown) => caught);
    expect(error).toMatchObject({ code: "BRIDGE_NATS_OPERATION_NOT_ALLOWED" });
    const fresh = setup();
    await fresh.carrier.outbound(fileCommand());
    await expectRefusal(fresh.carrier.outbound(fileCommand({ message: "different body", eventId: "file-event-2" })), /reused/);
    await expectRefusal(fresh.carrier.outbound(fileCommand({ version: 2 })), /version/);
    await expectRefusal(fresh.carrier.outbound(fileCommand({ operation: "cancel", commandId: "x" })), /cancelCommandId/);
  });

  it("strips the principal when the gate says strip, and the digest follows the wire", async () => {
    const { carrier } = setup(() => ({ ok: true, principal: "strip" }));
    const plan = sendOf(await carrier.outbound(fileCommand({}, { principal })));
    expect(plan.envelope.principal).toBeUndefined();
    expect(plan.record!.bodyDigest).toBe(controlBodyFingerprint(plan.envelope.body, undefined));
  });

  it("maps cancelCommandId for the sender's own carried command and absorbs any other cancel", async () => {
    const { carrier, presence } = setup();
    await carrier.outbound(fileCommand({ operation: "ask" }));
    const cancel = sendOf(await carrier.outbound(fileCommand({ operation: "cancel", commandId: "cancel-1", cancelCommandId: "file-cmd-1",
      eventId: "file-event-cancel", message: undefined })));
    expect(cancel.envelope.body.cancelCommandId).toBe(bridgeNatsMappedCommandId(LINK, "toNats", fileLead.id, "file-cmd-1"));
    expect(cancel.envelope.body.commandId).toBe(bridgeNatsMappedCommandId(LINK, "toNats", fileLead.id, "cancel-1"));
    expect(await carrier.outbound(fileCommand({ operation: "cancel", commandId: "cancel-2", cancelCommandId: "never-sent",
      eventId: "file-event-cancel-2" }))).toMatchObject({ kind: "absorb" });
    // Another file sender naming the same original id reaches nothing: its mapping differs.
    const intruder = { id: "session:44444444-4444-4444-8444-444444444444", name: "intruder", kind: "main" as const };
    presence.fileEntries.set(intruder.id, { identity: intruder, hostId: intruder.id });
    expect(await carrier.outbound(fileCommand({ operation: "cancel", commandId: "cancel-3", cancelCommandId: "file-cmd-1",
      replyTo: intruder.id, eventId: "file-event-cancel-3" }, { from: intruder }))).toMatchObject({ kind: "absorb" });
  });
});

describe("NATS ACK -> file ACK (the answer to a file sender)", () => {
  const carried = async () => {
    const context = setup();
    const plan = sendOf(await context.carrier.outbound(fileCommand({ operation: "ask" }, { principal })));
    const mapped = plan.envelope.body.commandId as string;
    const ack = (body: Record<string, unknown> = {}, envelope: Record<string, unknown> = {}) => ({
      id: `ack:${plan.envelope.id}`, kind: "ack", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: natsLead.id,
      operation: body.accepted === false ? "rejected" : "accepted", from: natsLead,
      body: { accepted: true, messageId: "answer-1", result: { text: "42" }, version: 1, commandId: mapped, targetId: natsLead.id,
        bodyDigest: plan.record!.bodyDigest, ownerIncarnation: "inc-moved-lead", ...body },
      ...envelope,
    });
    return { ...context, plan, mapped, ack };
  };

  it("maps the owner's answer back to the original id, the sender's host and the owner's presence identity", async () => {
    const { carrier, ack, mapped } = await carried();
    const inbound = await carrier.inbound(delivery(ack({}, { from: { ...natsLead, name: "forged" } })));
    expect(inbound.kind).toBe("event");
    const { event, settle } = inbound as Extract<NatsInbound, { kind: "event" }>;
    expect(event).toMatchObject({ topic: "fabric.control.ack", kind: "accepted", from: natsLead, to: fileLead.id,
      verification: "bridge", sequence: 7 });
    expect(event.data).toEqual({ version: 1, commandId: "file-cmd-1", targetId: natsLead.id, accepted: true,
      messageId: "answer-1", result: { text: "42" }, ownerIncarnation: "inc-moved-lead" });
    expect(settle).toEqual({ direction: "toNats", commandId: mapped, accepted: true, fromId: natsLead.id,
      ackEventId: `ack:${bridgeNatsEnvelopeId(LINK, "file-event-1")}`, data: event.data });
  });

  it("refuses an ACK from the wrong machine, owner, digest or id, and settles one for nothing it sent", async () => {
    const { carrier, presence, ack } = await carried();
    expect(await carrier.inbound(delivery(ack(), "evil-machine"))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(ack({}, { from: otherNats })))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(ack({ bodyDigest: "0".repeat(64) })))).toMatchObject({ kind: "refused" });
    // Not bound to one envelope id: an owner's replay answers a same-key resend's own envelope.
    expect(await carrier.inbound(delivery(ack({}, { id: "ack:another-envelope" })))).toMatchObject({ kind: "event" });
    expect(await carrier.inbound(delivery(ack({}, { to: { machine: BRIDGE_MACHINE, hostId: otherNats.id } })))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(ack({ commandId: "f".repeat(64) })))).toMatchObject({ kind: "settled" });
    expect(await carrier.inbound(delivery(ack({ errorCode: "FABRIC_RELAY_DESTINATION_UNAVAILABLE", accepted: false })))).toMatchObject({ kind: "refused" });
    presence.natsEntries.delete(natsLead.id);
    expect(await carrier.inbound(delivery(ack()))).toMatchObject({ kind: "refused", reason: /source machine/ });
  });

  it("bounds an oversize answer to a refusal (the handler ran: never notRun)", async () => {
    const presence = new FakePresence();
    const carrier = new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger: new MemoryLedger(),
      authorize: allow, now: () => T, maxAckBytes: 2_048 });
    const plan = sendOf(await carrier.outbound(fileCommand({ operation: "ask" })));
    const inbound = await carrier.inbound(delivery({
      id: `ack:${plan.envelope.id}`, kind: "ack", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: natsLead.id,
      operation: "accepted", from: natsLead, body: { accepted: true, result: "x".repeat(10_000), version: 1,
        commandId: plan.envelope.body.commandId, targetId: natsLead.id, bodyDigest: plan.record!.bodyDigest },
    }));
    const { event } = inbound as Extract<NatsInbound, { kind: "event" }>;
    expect(event.kind).toBe("rejected");
    expect(event.data).toMatchObject({ accepted: false, commandId: "file-cmd-1", error: expect.stringMatching(/exceeds/) });
    expect((event.data as Record<string, unknown>).notRun).toBeUndefined();
    expect(boundNatsAck({ accepted: true, warning: "w".repeat(5_000), messageId: "m" }, 1_000)).toEqual({ accepted: true, messageId: "m" });
  });

  it("settles a cancel's ACK without a file event (file senders never await one)", async () => {
    const { carrier } = setup();
    await carrier.outbound(fileCommand({ operation: "ask" }));
    const cancel = sendOf(await carrier.outbound(fileCommand({ operation: "cancel", commandId: "cancel-1", cancelCommandId: "file-cmd-1",
      eventId: "file-event-cancel", message: undefined })));
    const inbound = await carrier.inbound(delivery({
      id: `ack:${cancel.envelope.id}`, kind: "ack", to: { machine: BRIDGE_MACHINE, hostId: fileLead.id }, targetId: natsLead.id,
      operation: "accepted", from: natsLead, body: { accepted: true, version: 1, commandId: cancel.envelope.body.commandId,
        targetId: natsLead.id, bodyDigest: cancel.record!.bodyDigest },
    }));
    expect(inbound).toMatchObject({ kind: "settled", reason: "cancel settled" });
  });
});

describe("NATS -> file commands", () => {
  it("maps a command from the verified KV identity, strips unknown body fields and maps the id", async () => {
    const { carrier } = setup();
    const inbound = await carrier.inbound(delivery(natsCommand({ sender: { forged: true }, inboxTopic: "fleet.work.x", bridge: { from: "x", id: "y" } },
      { from: { ...natsLead, name: "forged", kind: "actor" }, principal })));
    const { event } = inbound as Extract<NatsInbound, { kind: "event" }>;
    expect(event).toMatchObject({ id: "nats-env-1", sequence: 7, topic: "fabric.control.command", kind: "ask", from: natsLead,
      to: fileLead.id, verification: "bridge", principal });
    expect(event.data).toEqual({ version: 1, commandId: bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cmd-1"),
      targetId: fileLead.id, operation: "ask", ownerIncarnation: "inc-file-lead", message: "what changed?", replyTo: natsLead.id,
      requestCreatedAt: T, requestedAt: T, deadlineAt: T + 60_000 });
    // A redelivery presents the same event again; the file store's dedupe receipt makes it one append.
    expect(await carrier.inbound(delivery(natsCommand({}, { principal })))).toMatchObject({ kind: "event", event: { id: "nats-env-1" } });
  });

  it("a second NATS sender reusing another sender's commandId gets its own mapped id, never the first one's", async () => {
    const { carrier } = setup();
    const first = await carrier.inbound(delivery(natsCommand()));
    const second = await carrier.inbound(delivery(natsCommand({ replyTo: otherNats.id }, { id: "nats-env-2", from: otherNats })));
    const ids = [first, second].map(plan => ((plan as Extract<NatsInbound, { kind: "event" }>).event.data as Record<string, unknown>).commandId);
    expect(ids[0]).not.toBe(ids[1]);
    expect(ids[1]).toBe(bridgeNatsMappedCommandId(LINK, "toFile", otherNats.id, "nats-cmd-1"));
  });

  it("refuses unverified senders, foreign replyTo, wrong machines and targets it does not carry", async () => {
    const { carrier } = setup();
    expect(await carrier.inbound(delivery(natsCommand(), "evil-machine"))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(natsCommand(), BRIDGE_MACHINE))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(natsCommand({}, { to: { machine: "elsewhere", hostId: fileLead.id } })))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(natsCommand({ replyTo: otherNats.id })))).toMatchObject({ kind: "refused", reason: /replyTo/ });
    expect(await carrier.inbound(delivery(natsCommand({ targetId: otherNats.id }, { targetId: otherNats.id })))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(natsCommand({}, { to: { machine: BRIDGE_MACHINE, hostId: otherNats.id } })))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(natsCommand({ targetId: natsLead.id })))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery({ ...natsCommand(), body: "not an object" }))).toMatchObject({ kind: "refused" });
    expect(await carrier.inbound(delivery(natsCommand({}, { id: "relay:abc" })))).toMatchObject({ kind: "refused" });
  });

  it("waits for an allow-listed target missing from this link's KV view; refuses any other at once, notRun with a stable code", async () => {
    const presence = new FakePresence();
    presence.natsEntries.delete(fileLead.id);
    const make = (waitForTarget: (id: string) => boolean) => new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE, presence,
      ledger: new MemoryLedger(), authorize: allow, now: () => T, waitForTarget });
    expect(await make(id => id === fileLead.id).inbound(delivery(natsCommand()))).toMatchObject({ kind: "wait" });
    const refused = await make(() => false).inbound(delivery(natsCommand({}, { principal })));
    expect(refused).toMatchObject({ kind: "reply", envelope: { id: "ack:nats-env-1", kind: "ack", operation: "rejected",
      to: { machine: POD_MACHINE, hostId: natsLead.id }, from: { id: fileLead.id }, targetId: fileLead.id,
      body: { accepted: false, notRun: true, errorCode: "BRIDGE_NATS_NOT_ALLOWED", commandId: "nats-cmd-1", targetId: fileLead.id,
        ownerIncarnation: "inc-file-lead", bodyDigest: controlBodyFingerprint(natsCommand().body, principal) } } });
  });

  it("answers a gate refusal and an id conflict for the hop, notRun, in the NATS owner's exact ACK shape", async () => {
    const gated = setup(() => ({ ok: false, refuse: "BRIDGE_NATS_OPERATION_NOT_ALLOWED", reason: "seat receives followUp and cancel only" }));
    const refused = await gated.carrier.inbound(delivery(natsCommand({}, { principal })));
    expect(refused.kind).toBe("reply");
    const reply = (refused as Extract<NatsInbound, { kind: "reply" }>).envelope;
    expect(reply).toMatchObject({ id: "ack:nats-env-1", kind: "ack", operation: "rejected", targetId: fileLead.id, from: fileLead,
      to: { machine: POD_MACHINE, hostId: natsLead.id } });
    expect(reply.body).toMatchObject({ accepted: false, notRun: true, version: 1, commandId: "nats-cmd-1", targetId: fileLead.id,
      ownerIncarnation: "inc-file-lead", bodyDigest: controlBodyFingerprint(natsCommand().body, principal) });
    const { carrier } = setup();
    await carrier.inbound(delivery(natsCommand()));
    const conflict = await carrier.inbound(delivery(natsCommand({ message: "different" }, { id: "nats-env-9" })));
    expect(conflict).toMatchObject({ kind: "reply", envelope: { id: "ack:nats-env-9",
      body: { accepted: false, errorCode: "FABRIC_CONTROL_ID_CONFLICT", notRun: true } } });
  });

  it("a NATS same-key resend under a new envelope gets the owner's recorded answer, or waits for the first ACK", async () => {
    const { carrier } = setup();
    await carrier.inbound(delivery(natsCommand()));
    // No answer recorded yet (claim table: resend): presented again under the same mapped id; the file store's
    // receipt makes it one append, and the first ACK (matched by commandId and digest) settles both attempts.
    const resent = await carrier.inbound(delivery(natsCommand({}, { id: "nats-env-2" })));
    expect(resent).toMatchObject({ kind: "event", event: { data: { commandId: bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cmd-1") } } });
    const mapped = bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cmd-1");
    const answer = sendOf(await carrier.outbound({ topic: "fabric.control.ack", kind: "accepted", from: fileLead, to: natsLead.id,
      data: { version: 1, commandId: mapped, targetId: fileLead.id, ownerIncarnation: "inc-file-lead", accepted: true, messageId: "m-1",
        bridge: { from: "ryzen1", id: "file-ack" } } as BridgePublish["data"] }));
    await carrier.settle(answer.settle!, true);
    const replay = await carrier.inbound(delivery(natsCommand({}, { id: "nats-env-3" })));
    expect(replay).toMatchObject({ kind: "reply", envelope: { id: "ack:nats-env-3", kind: "ack", operation: "accepted", from: fileLead,
      to: { machine: POD_MACHINE, hostId: natsLead.id }, body: { accepted: true, messageId: "m-1", commandId: "nats-cmd-1",
        bodyDigest: answer.envelope.body.bodyDigest, ownerIncarnation: "inc-file-lead", originalOwnerIncarnation: "inc-file-lead" } } });
  });

  it("maps a NATS cancel onto the sender's own carried command and absorbs any other", async () => {
    const { carrier } = setup();
    await carrier.inbound(delivery(natsCommand()));
    const cancel = await carrier.inbound(delivery(natsCommand({ operation: "cancel", commandId: "nats-cancel-1",
      cancelCommandId: "nats-cmd-1", message: undefined }, { id: "nats-env-cancel" })));
    expect((cancel as Extract<NatsInbound, { kind: "event" }>).event.data).toMatchObject({
      operation: "cancel", cancelCommandId: bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cmd-1"),
      commandId: bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cancel-1") });
    const foreign = await carrier.inbound(delivery(natsCommand({ operation: "cancel", commandId: "nats-cancel-2",
      cancelCommandId: "nats-cmd-1", replyTo: otherNats.id, message: undefined }, { id: "nats-env-cancel-2", from: otherNats })));
    expect(foreign).toMatchObject({ kind: "settled" });
  });
});

describe("file ACK -> NATS ACK (the answer to a NATS sender)", () => {
  const answered = async () => {
    const context = setup();
    await context.carrier.inbound(delivery(natsCommand({}, { principal })));
    const mapped = bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, "nats-cmd-1");
    const ack = (data: Record<string, unknown> = {}, extra: Partial<BridgePublish> = {}): BridgePublish => ({
      topic: "fabric.control.ack", kind: data.accepted === false ? "rejected" : "accepted", from: fileLead, to: natsLead.id,
      data: { version: 1, commandId: mapped, targetId: fileLead.id, ownerIncarnation: "inc-file-lead", accepted: true,
        messageId: "file-answer", result: { text: "the answer" }, ...data, bridge: { from: "ryzen1", id: "file-ack-1" } } as BridgePublish["data"],
      ...extra,
    });
    return { ...context, mapped, ack };
  };

  it("passes every check the NATS sender makes (#acceptNatsAcknowledgement), with the ask's answer", async () => {
    const { carrier, ack, mapped } = await answered();
    const plan = sendOf(await carrier.outbound(ack()));
    const envelope = plan.envelope;
    // What the NATS sender captured: owner machine = this bridge, owner = the file lead, its own fingerprint.
    const senderFingerprint = controlBodyFingerprint({ targetId: fileLead.id, operation: "ask", ownerIncarnation: "inc-file-lead",
      message: "what changed?" }, principal);
    expect(envelope.id).toBe("ack:nats-env-1");
    expect(envelope.kind).toBe("ack");
    expect(envelope.to).toEqual({ machine: POD_MACHINE, hostId: natsLead.id });
    expect(envelope.from.id).toBe(fileLead.id);
    expect(envelope.targetId).toBe(envelope.body.targetId);
    expect(envelope.operation).toBe("accepted");
    expect(envelope.body).toEqual({ accepted: true, messageId: "file-answer", result: { text: "the answer" },
      ownerIncarnation: "inc-file-lead", version: 1, commandId: "nats-cmd-1", targetId: fileLead.id, bodyDigest: senderFingerprint });
    expect(plan.settle).toEqual({ direction: "toFile", commandId: mapped, accepted: true, fromId: fileLead.id,
      ackEventId: "file-ack-1", data: envelope.body });
  });

  it("states notRun on a file owner's stale-epoch refusal, which a NATS sender accepts only with it", async () => {
    const { carrier, ack } = await answered();
    const stale = sendOf(await carrier.outbound(ack({ accepted: false, error: STALE_INCARNATION_ERROR,
      errorCode: "FABRIC_CONTROL_STALE_INCARNATION", ownerIncarnation: "inc-file-lead-new", staleIncarnation: "inc-file-lead",
      messageId: undefined, result: undefined }, { kind: "rejected" })));
    expect(stale.envelope.body).toMatchObject({ accepted: false, notRun: true, errorCode: "FABRIC_CONTROL_STALE_INCARNATION",
      ownerIncarnation: "inc-file-lead-new", staleIncarnation: "inc-file-lead", commandId: "nats-cmd-1" });
    // Any other refusal keeps exactly what the owner said.
    const { carrier: other, ack: otherAck } = await answered();
    const plain = sendOf(await other.outbound(otherAck({ accepted: false, error: "busy", messageId: undefined, result: undefined })));
    expect(plain.envelope.body.notRun).toBeUndefined();
  });

  it("refuses an ACK to another sender, from another owner, for an unknown id, or a relay refusal", async () => {
    const { carrier, ack } = await answered();
    await expectRefusal(carrier.outbound(ack({}, { to: otherNats.id })), /does not match/);
    await expectRefusal(carrier.outbound(ack({}, { from: otherNats })), /does not match/);
    await expectRefusal(carrier.outbound(ack({ commandId: "unknown" })), /carried/);
    await expectRefusal(carrier.outbound(ack({ accepted: false, errorCode: "FABRIC_IDEMPOTENCY_KEY_REUSED" })), /outcome/);
    await expectRefusal(carrier.outbound(ack({ accepted: "yes" })), /outcome/);
  });
});
