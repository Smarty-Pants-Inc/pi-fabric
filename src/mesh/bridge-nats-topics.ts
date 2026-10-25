import { BRIDGE_STALL_ALARM_TOPIC, type BridgePublish } from "./bridge.js";
import {
  carrierIdentity, isStaleIncarnationRefusal, NatsCarrierRefusal,
  type NatsCarrierDecision, type NatsCarrierEnvelope, type NatsCarrierLedger, type NatsCarrierPresence, type NatsCarrierRecord,
  type NatsOutbound, type NatsPresenceEntry,
} from "./bridge-nats-carrier.js";
import {
  BRIDGE_NATS_HORIZON_MS, BRIDGE_NATS_MACHINE, bridgeNatsCommandId, bridgeNatsEnvelopeId, bridgeNatsLinkIdentity,
  bridgeNatsTopicIdempotencyKey,
} from "./bridge-nats-types.js";
import { isWorkTopic } from "./bridge-topics.js";
import { controlBodyFingerprint } from "./control-idempotency.js";
import { FABRIC_MESH_OUTAGE_ALARM_TOPIC, isMeshAlarmTarget, type MeshIdentity } from "./store.js";

/**
 * N3 topic translator (smarty-dev#15310, A' lane L2). A NATS Main's RootInbox reads addressed work
 * topics only from its own local log (root-inbox.ts:375), so a Ryzen 1 event addressed to a moved
 * lead (a `smarty-task-ryzen2` completion on fleet.work.tasks, a `smarty-report` lane report on
 * fleet.work.lanes, an inbox-age or bridge alarm on ops.owner) would never reach it. MeshBridge
 * still selects and checks these events toward the NATS side as it does for a file peer; this
 * module turns each one into ONE keyed followUp to the addressed NATS root.
 *
 * - The followUp is sent FROM the link's own identity (bridgeNatsLinkIdentity, kept live in KV by the
 *   NATS side). The original sender is never presented as the sender: its id is framed in the text
 *   and named in data, as a label.
 * - Its key is bridgeNatsTopicIdempotencyKey(link, file event id): a resend or a restart re-forms
 *   the same commandId, and the first envelope is recorded in the ledger and resent byte for byte.
 * - A followUp refused for a stale owner epoch (the root restarted, proven notRun) is re-sent to the
 *   current epoch at most `maxAttempts - 1` times; any other answer is final.
 */

/** ops.owner kinds a root acts on. pr.wake is a registration for the hub factory, not Main work. */
const OWNER_KINDS: ReadonlySet<string> = new Set([
  "inbox.age.alarm", "bridge.lease.alarm", "bridge.lease.recovered", "bridge.delivery-age.alarm", "bridge.delivery-age.recovered",
  "bridge.legacy-baseline.alarm", "bridge.legacy-baseline.recovered",
]);
/** Text over this is cut, with a marker: a followUp is a turn, and the full event stays in the file mesh. */
export const NATS_TOPIC_MAX_TEXT_BYTES = 32 * 1024;
/** The followUp's own delivery window. A late answer is still recorded; the event itself never expires here. */
export const NATS_TOPIC_TIMEOUT_MS = 10 * 60 * 1_000;
const MAX_ATTEMPTS = 3;
const MAX_DATA_FIELD = 256;

/** Whether an event MeshBridge carries toward the NATS side becomes a keyed followUp (N3). */
export const isNatsTopicEvent = (event: Pick<BridgePublish, "topic" | "kind" | "to">): boolean =>
  isWorkTopic(event.topic) ||
  (event.topic === "ops.owner" && OWNER_KINDS.has(event.kind)) ||
  (event.topic === BRIDGE_STALL_ALARM_TOPIC && event.kind === "bridge-stall" && isMeshAlarmTarget(event.to)) ||
  (event.topic === FABRIC_MESH_OUTAGE_ALARM_TOPIC && (event.kind === "mesh-outage" || event.kind === "mesh-recovered") &&
    isMeshAlarmTarget(event.to));

export interface NatsTopicAuthorization {
  /** The NATS root the event is addressed to, verified from KV presence. */
  readonly targetId: string;
  readonly topic: string;
  readonly kind: string;
  /** The file event's sender: a label for the allow-list's topic sender filter, never an identity. */
  readonly fromId: string;
}
export type NatsTopicAuthorize = (request: NatsTopicAuthorization) => NatsCarrierDecision;

export interface NatsTopicTranslatorOptions {
  link: string;
  /** This bridge's credential-bound NATS machine. */
  machine: string;
  presence: NatsCarrierPresence;
  ledger: NatsCarrierLedger;
  authorize: NatsTopicAuthorize;
  now?: () => number;
  timeoutMs?: number;
  maxTextBytes?: number;
  maxAttempts?: number;
}

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const boundedText = (text: string, maxBytes: number): string => {
  const bytes = Buffer.byteLength(text, "utf8");
  if (bytes <= maxBytes) return text;
  let cut = Buffer.from(text, "utf8").subarray(0, maxBytes).toString("utf8");
  if (cut.endsWith("\uFFFD")) cut = cut.slice(0, -1);
  return `${cut}\n[... ${bytes - Buffer.byteLength(cut, "utf8")} more bytes in the Ryzen 1 mesh event]`;
};

const label = (value: unknown): string | undefined =>
  typeof value === "string" && value.trim() ? value.slice(0, MAX_DATA_FIELD) : undefined;

export class NatsTopicTranslator {
  readonly link: string;
  readonly machine: string;
  /** The link's own identity: every topic followUp is sent from it and answered to it. */
  readonly identity: MeshIdentity;
  readonly #presence: NatsCarrierPresence;
  readonly #ledger: NatsCarrierLedger;
  readonly #authorize: NatsTopicAuthorize;
  readonly #now: () => number;
  readonly #timeoutMs: number;
  readonly #maxTextBytes: number;
  readonly #maxAttempts: number;

  constructor(options: NatsTopicTranslatorOptions) {
    if (!BRIDGE_NATS_MACHINE.test(options.machine)) throw new Error("NATS topic translator machine is invalid");
    this.link = options.link;
    this.machine = options.machine;
    this.identity = bridgeNatsLinkIdentity(options.link);
    this.#presence = options.presence;
    this.#ledger = options.ledger;
    this.#authorize = options.authorize;
    this.#now = options.now ?? Date.now;
    this.#timeoutMs = options.timeoutMs ?? NATS_TOPIC_TIMEOUT_MS;
    this.#maxTextBytes = options.maxTextBytes ?? NATS_TOPIC_MAX_TEXT_BYTES;
    this.#maxAttempts = Math.max(1, options.maxAttempts ?? MAX_ATTEMPTS);
  }

  /** The followUp for one file event MeshBridge carries toward a NATS root. */
  async outbound(pub: BridgePublish): Promise<NatsOutbound> {
    if (!isNatsTopicEvent(pub)) throw new NatsCarrierRefusal(`topic ${pub.topic}/${pub.kind} is not translated`);
    const eventId = pub.data.bridge.id;
    if (typeof eventId !== "string" || !eventId) throw new NatsCarrierRefusal("topic event has no source event id");
    const target = this.#target(pub.to);
    const decision = this.#authorize({ targetId: pub.to, topic: pub.topic, kind: pub.kind, fromId: pub.from.id });
    if (!decision.ok) throw new NatsCarrierRefusal(decision.reason, decision.refuse);
    const data: Record<string, unknown> = isObject(pub.data) ? pub.data : {};
    const key = label(data.key), ref = label(data.ref);
    const message = `[mesh ${pub.topic}/${pub.kind} from ${pub.from.id}]` +
      (pub.text?.trim() ? `\n${boundedText(pub.text, this.#maxTextBytes)}` : "");
    const followUpData = {
      ...(key ? { key } : {}), ...(ref ? { ref } : {}),
      mesh: { id: eventId, topic: pub.topic, kind: pub.kind, from: { id: pub.from.id, name: pub.from.name, kind: pub.from.kind }, to: pub.to },
      bridge: { from: this.link, id: eventId },
    };
    return this.#send(eventId, 0, target, message, followUpData);
  }

  /**
   * The ACK of a topic followUp. A proven stale-epoch refusal is re-sent once more to the root's
   * current epoch (bounded); everything else is final and returns undefined.
   */
  async retry(record: NatsCarrierRecord, ack: Readonly<Record<string, unknown>> | undefined): Promise<NatsOutbound | undefined> {
    if (record.origin !== "topic" || !isStaleIncarnationRefusal(ack) || !record.body) return undefined;
    const refused = this.#attemptOf(record);
    if (refused === undefined) return undefined;
    const attempt = refused + 1;
    if (attempt >= this.#maxAttempts) return undefined;
    let target: NatsPresenceEntry;
    try { target = this.#target(record.targetId); } catch { return undefined; }
    // Only a NEW epoch of the same root: the refused one would refuse again.
    if (target.route.ownerIncarnation === record.body.ownerIncarnation) return undefined;
    const body = record.body;
    return this.#send(record.sourceEventId, attempt, target, body.message as string, body.data as Record<string, unknown>);
  }

  /** Which attempt a topic record was: its commandId is derived from the attempt, so it is recomputed, never stored. */
  #attemptOf(record: NatsCarrierRecord): number | undefined {
    const idempotencyKey = bridgeNatsTopicIdempotencyKey(this.link, record.sourceEventId);
    for (let attempt = 0; attempt < this.#maxAttempts; attempt++) {
      if (bridgeNatsCommandId({ hostId: this.identity.id, ownerHostId: record.ownerHostId, ownerIdentityId: record.ownerIdentityId,
        targetId: record.targetId, operation: "followUp", idempotencyKey, notRunAttempt: attempt }) === record.destinationCommandId) return attempt;
    }
    return undefined;
  }

  #target(id: string): NatsPresenceEntry {
    const target = this.#presence.nats(id);
    // A NATS-native root itself (its own host and identity), never an actor or this bridge's own mirror.
    if (!target?.route.live || target.route.machine === this.machine || !BRIDGE_NATS_MACHINE.test(target.route.machine) ||
        target.route.ownerHostId !== id || target.route.ownerIdentityId !== id || target.identity.id !== id) {
      throw new NatsCarrierRefusal("topic target is not a live NATS root");
    }
    return target;
  }

  async #send(
    eventId: string, attempt: number, target: NatsPresenceEntry,
    message: string, data: Record<string, unknown>,
  ): Promise<NatsOutbound> {
    const sender = this.#presence.nats(this.identity.id);
    // The receiver verifies the sender in KV on this machine. Until the NATS side has registered the link's
    // identity, wait (a retryable error holds the cursor) rather than drop work events into a refusal.
    if (!sender?.route.live || sender.route.machine !== this.machine) {
      throw new Error(`Fabric NATS link identity ${this.identity.id} is not live in NATS presence yet`);
    }
    const route = target.route;
    const hostId = this.identity.id;
    const idempotencyKey = bridgeNatsTopicIdempotencyKey(this.link, eventId);
    const commandId = bridgeNatsCommandId({ hostId, ownerHostId: route.ownerHostId, ownerIdentityId: route.ownerIdentityId,
      targetId: route.ownerIdentityId, operation: "followUp", idempotencyKey, notRunAttempt: attempt });
    const existing = await this.#ledger.lookup("toNats", commandId);
    if (existing) return this.#resend(existing);
    const now = this.#now();
    const body = JSON.parse(JSON.stringify({
      version: 1, commandId, targetId: route.ownerIdentityId, operation: "followUp", ownerIncarnation: route.ownerIncarnation,
      message, data, triggerTurn: true, replyTo: hostId, requestCreatedAt: now, requestedAt: now, deadlineAt: now + this.#timeoutMs,
    })) as Record<string, unknown>;
    const envelope: NatsCarrierEnvelope = Object.freeze({
      id: bridgeNatsEnvelopeId(this.link, attempt ? `${eventId}#retry-${attempt}` : eventId), kind: "command" as const,
      to: Object.freeze({ machine: route.machine, hostId: route.ownerHostId }), targetId: route.ownerIdentityId,
      operation: "followUp", from: carrierIdentity(this.identity), body,
    });
    const record: NatsCarrierRecord = {
      v: 1, direction: "toNats", origin: "topic", link: this.link, operation: "followUp",
      sourceCommandId: idempotencyKey, destinationCommandId: commandId, sourceEventId: eventId, destinationEventId: envelope.id,
      targetId: route.ownerIdentityId, ownerHostId: route.ownerHostId, ownerIdentityId: route.ownerIdentityId,
      ownerMachine: route.machine, requesterId: this.identity.id, from: envelope.from, sourceMachine: this.machine, replyTo: hostId,
      bodyDigest: controlBodyFingerprint(body, undefined), deadlineAt: now + this.#timeoutMs,
      expiresAt: now + this.#timeoutMs + BRIDGE_NATS_HORIZON_MS, ownerIncarnation: route.ownerIncarnation, body,
    };
    const { created, record: stored } = await this.#ledger.claim(record);
    return created ? { kind: "send", envelope, record } : this.#resend(stored);
  }

  /** The first envelope again, from the recorded id, machine and body bytes. */
  #resend(record: NatsCarrierRecord): NatsOutbound {
    if (record.origin !== "topic" || record.direction !== "toNats" || !record.body || !record.ownerMachine) {
      throw new NatsCarrierRefusal("topic followUp id collides with another command");
    }
    return { kind: "send", record, envelope: Object.freeze({
      id: record.destinationEventId, kind: "command" as const,
      to: Object.freeze({ machine: record.ownerMachine, hostId: record.ownerHostId }), targetId: record.targetId,
      operation: "followUp", from: record.from, body: record.body,
    }) };
  }
}

