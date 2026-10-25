// Lane L2 (smarty-dev#15310) N3: a Ryzen 1 work/owner event addressed to a NATS root becomes one keyed followUp.
import { describe, expect, it } from "vitest";
import type { BridgePublish } from "../src/mesh/bridge.js";
import { NatsBridgeCarrier, NatsCarrierRefusal, type NatsCarrierRecord } from "../src/mesh/bridge-nats-carrier.js";
import { NatsTopicTranslator, isNatsTopicEvent, type NatsTopicAuthorize } from "../src/mesh/bridge-nats-topics.js";
import {
  bridgeNatsCommandId, bridgeNatsEnvelopeId, bridgeNatsTopicIdempotencyKey,
} from "../src/mesh/bridge-nats-types.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import {
  BRIDGE_MACHINE, FakePresence, LINK, MemoryLedger, POD_MACHINE, linkIdentity, natsLead, route,
} from "./helpers/bridge-nats-l2-fakes.js";

const T = 1_800_000_000_000;
const allow: NatsTopicAuthorize = () => ({ ok: true, principal: "none" });

const setup = (authorize: NatsTopicAuthorize = allow, now = () => T) => {
  const presence = new FakePresence();
  const ledger = new MemoryLedger();
  const topics = new NatsTopicTranslator({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger, authorize, now });
  return { presence, ledger, topics };
};

const taskDone = (extra: Partial<BridgePublish> = {}, eventId = "file-event-task-1"): BridgePublish => ({
  topic: "fleet.work.tasks", kind: "answer", to: natsLead.id,
  from: { id: "fleet-tool:smarty-task-ryzen2", name: "smarty-task-ryzen2", kind: "main" },
  text: "RYZEN2_TASK_COMPLETE t-17 rc=0\nall green",
  data: { key: "ryzen2-task:t-17", bridge: { from: "ryzen1", id: eventId } } as BridgePublish["data"],
  ...extra,
});

const sent = async (topics: NatsTopicTranslator, pub: BridgePublish) => {
  const plan = await topics.outbound(pub);
  if (plan.kind !== "send") throw new Error("expected a send");
  return plan;
};

describe("N3 topic translator", () => {
  it("turns a task completion addressed to a moved lead into one keyed followUp from the link identity", async () => {
    const { topics } = setup();
    const plan = await sent(topics, taskDone());
    const idempotencyKey = bridgeNatsTopicIdempotencyKey(LINK, "file-event-task-1");
    const commandId = bridgeNatsCommandId({ hostId: linkIdentity.id, ownerHostId: natsLead.id, ownerIdentityId: natsLead.id,
      targetId: natsLead.id, operation: "followUp", idempotencyKey });
    expect(plan.envelope).toMatchObject({ id: bridgeNatsEnvelopeId(LINK, "file-event-task-1"), kind: "command", operation: "followUp",
      targetId: natsLead.id, to: { machine: POD_MACHINE, hostId: natsLead.id }, from: linkIdentity });
    expect(plan.envelope.principal).toBeUndefined();
    expect(plan.envelope.body).toEqual({
      version: 1, commandId, targetId: natsLead.id, operation: "followUp", ownerIncarnation: "inc-moved-lead",
      message: "[mesh fleet.work.tasks/answer from fleet-tool:smarty-task-ryzen2]\nRYZEN2_TASK_COMPLETE t-17 rc=0\nall green",
      data: { key: "ryzen2-task:t-17",
        mesh: { id: "file-event-task-1", topic: "fleet.work.tasks", kind: "answer",
          from: { id: "fleet-tool:smarty-task-ryzen2", name: "smarty-task-ryzen2", kind: "main" }, to: natsLead.id },
        bridge: { from: LINK, id: "file-event-task-1" } },
      triggerTurn: true, replyTo: linkIdentity.id, requestCreatedAt: T, requestedAt: T, deadlineAt: T + 10 * 60_000,
    });
    expect(plan.record).toMatchObject({ direction: "toNats", origin: "topic", operation: "followUp", destinationCommandId: commandId,
      requesterId: linkIdentity.id, replyTo: linkIdentity.id, ownerMachine: POD_MACHINE,
      bodyDigest: controlBodyFingerprint(plan.envelope.body, undefined) });
  });

  it("re-forms the same envelope on a resend, even after the clock moved", async () => {
    let now = T;
    const { topics, ledger } = setup(allow, () => now);
    const first = await sent(topics, taskDone());
    now += 120_000;
    const again = await sent(topics, taskDone());
    expect(JSON.stringify(again.envelope)).toBe(JSON.stringify(first.envelope));
    expect(ledger.records.size).toBe(1);
  });

  it("translates lane reports and the owner kinds MeshBridge carries, not other topics or kinds", async () => {
    expect(isNatsTopicEvent({ topic: "fleet.work.lanes", kind: "ready", to: natsLead.id })).toBe(true);
    expect(isNatsTopicEvent({ topic: "fleet.work.pi-fabric.15310", kind: "done", to: natsLead.id })).toBe(true);
    expect(isNatsTopicEvent({ topic: "ops.owner", kind: "inbox.age.alarm", to: natsLead.id })).toBe(true);
    expect(isNatsTopicEvent({ topic: "ops.owner", kind: "bridge.lease.alarm", to: natsLead.id })).toBe(true);
    expect(isNatsTopicEvent({ topic: "ops.owner", kind: "pr.wake", to: natsLead.id })).toBe(true);
    expect(isNatsTopicEvent({ topic: "ops.owner", kind: "cleanup", to: natsLead.id })).toBe(false);
    expect(isNatsTopicEvent({ topic: "fabric.alarm.bridge-stall", kind: "bridge-stall", to: natsLead.id })).toBe(true);
    expect(isNatsTopicEvent({ topic: "fabric.alarm.bridge-stall", kind: "bridge-stall", to: "factory" })).toBe(false);
    expect(isNatsTopicEvent({ topic: "github.pr", kind: "opened", to: natsLead.id })).toBe(false);
    const { topics } = setup();
    const lanes = await sent(topics, taskDone({ topic: "fleet.work.lanes", kind: "ready", text: "READY head abc" }, "lane-1"));
    expect(lanes.envelope.body.message).toBe("[mesh fleet.work.lanes/ready from fleet-tool:smarty-task-ryzen2]\nREADY head abc");
    await expect(topics.outbound(taskDone({ topic: "ops.owner", kind: "cleanup" }, "cleanup-1"))).rejects.toBeInstanceOf(NatsCarrierRefusal);
  });

  it("refuses a target that is not a live NATS root, and a gate refusal, permanently", async () => {
    const { topics, presence } = setup();
    await expect(topics.outbound(taskDone({ to: "session:unknown" }))).rejects.toThrow(/not allowed/);
    presence.natsEntries.set("actor-1", route({ id: "actor-1", name: "review", kind: "actor" }, POD_MACHINE, "inc", { ownerHostId: natsLead.id,
      ownerIdentityId: natsLead.id }));
    await expect(topics.outbound(taskDone({ to: "actor-1" }))).rejects.toThrow(/live NATS root/);
    // A root this bridge mirrors itself lives in the file mesh: no loop.
    presence.natsEntries.set(natsLead.id, route(natsLead, BRIDGE_MACHINE));
    await expect(topics.outbound(taskDone())).rejects.toThrow(/live NATS root/);
    const gated = setup(() => ({ ok: false, refuse: "BRIDGE_NATS_TOPIC_NOT_ALLOWED", reason: "topic not in receiveTopics" }));
    await expect(gated.topics.outbound(taskDone())).rejects.toMatchObject({ code: "BRIDGE_NATS_TOPIC_NOT_ALLOWED" });
  });

  it("holds (retryable, not a drop) until the link identity is live in NATS presence", async () => {
    const { topics, presence } = setup();
    presence.natsEntries.delete(linkIdentity.id);
    const error = await topics.outbound(taskDone()).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(Error);
    expect(error).not.toBeInstanceOf(NatsCarrierRefusal);
    expect((error as Error).message).not.toMatch(/exceeds|not allowed|no bridge stamp|no recipient|no sender|Invalid mesh topic/i);
  });

  it("bounds a long text with a marker; the whole event stays in the file mesh", async () => {
    const presence = new FakePresence();
    const topics = new NatsTopicTranslator({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger: new MemoryLedger(), authorize: allow,
      now: () => T, maxTextBytes: 64 });
    const plan = await topics.outbound(taskDone({ text: "é".repeat(100) }));
    if (plan.kind !== "send") throw new Error("expected a send");
    expect(plan.envelope.body.message).toMatch(/^\[mesh fleet\.work\.tasks\/answer from fleet-tool:smarty-task-ryzen2\]\n(é){32}\n\[\.\.\. 136 more bytes in the Ryzen 1 mesh event\]$/);
  });

  it("re-sends a stale-epoch refusal to the root's new epoch, bounded; any other answer is final", async () => {
    const { topics, presence } = setup();
    const first = await sent(topics, taskDone());
    const stale = { accepted: false, notRun: true, errorCode: "FABRIC_CONTROL_STALE_INCARNATION", error: "stale" };
    // Same epoch: it would refuse again.
    expect(await topics.retry(first.record!, stale)).toBeUndefined();
    expect(await topics.retry(first.record!, { accepted: false, error: "busy" })).toBeUndefined();
    presence.natsEntries.set(natsLead.id, route(natsLead, POD_MACHINE, "inc-restarted"));
    const second = await topics.retry(first.record!, stale);
    if (second?.kind !== "send") throw new Error("expected a re-send");
    expect(second.envelope.id).toBe(bridgeNatsEnvelopeId(LINK, "file-event-task-1#retry-1"));
    expect(second.envelope.body).toMatchObject({ ownerIncarnation: "inc-restarted", message: first.envelope.body.message,
      commandId: bridgeNatsCommandId({ hostId: linkIdentity.id, ownerHostId: natsLead.id, ownerIdentityId: natsLead.id,
        targetId: natsLead.id, operation: "followUp", idempotencyKey: bridgeNatsTopicIdempotencyKey(LINK, "file-event-task-1"), notRunAttempt: 1 }) });
    presence.natsEntries.set(natsLead.id, route(natsLead, POD_MACHINE, "inc-again"));
    const third = await topics.retry(second.record!, stale);
    expect(third?.kind).toBe("send");
    presence.natsEntries.set(natsLead.id, route(natsLead, POD_MACHINE, "inc-thrice"));
    expect(await topics.retry((third as { record: NatsCarrierRecord }).record, stale)).toBeUndefined();
  });

  it("the followUp's ACK settles through the carrier without a file event", async () => {
    const presence = new FakePresence();
    const ledger = new MemoryLedger();
    const topics = new NatsTopicTranslator({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger, authorize: allow, now: () => T });
    const carrier = new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger, authorize: () => ({ ok: true, principal: "none" }), now: () => T });
    const plan = await sent(topics, taskDone());
    const inbound = await carrier.inbound({ sourceMachine: POD_MACHINE, sequence: 3, envelope: {
      id: `ack:${plan.envelope.id}`, kind: "ack", to: { machine: BRIDGE_MACHINE, hostId: linkIdentity.id }, targetId: natsLead.id,
      operation: "accepted", from: natsLead, body: { accepted: true, messageId: "m", version: 1, commandId: plan.envelope.body.commandId,
        targetId: natsLead.id, bodyDigest: plan.record!.bodyDigest, ownerIncarnation: "inc-moved-lead" },
    } });
    expect(inbound).toMatchObject({ kind: "settled", reason: "topic followUp settled", record: { origin: "topic" },
      settle: { direction: "toNats", accepted: true } });
  });
});
