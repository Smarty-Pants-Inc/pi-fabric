import { copyFabricPrincipal, type FabricPrincipal } from "../fabric-provenance.js";
import {
  CONTROL_STALE_INCARNATION, RELAY_DESTINATION_UNAVAILABLE, STALE_INCARNATION_ERROR, type FabricControlOperation,
} from "../topology/control-plane.js";
import type { ParticipantRoute } from "../topology/participant-view.js";
import type { TransportDelivery, TransportEnvelope } from "../transport/types.js";
import type { BridgePublish } from "./bridge.js";
import {
  BRIDGE_NATS_ACK_TOPIC, BRIDGE_NATS_CONTROL_TOPIC, BRIDGE_NATS_HORIZON_MS, BRIDGE_NATS_MACHINE, bridgeNatsAckEnvelopeId,
  bridgeNatsEnvelopeId, bridgeNatsMappedCommandId, type BridgeNatsDirection, type BridgeNatsPortDelivery,
} from "./bridge-nats-types.js";
import { controlBodyFingerprint, FABRIC_IDEMPOTENCY_KEY_REUSED } from "./control-idempotency.js";
import type { MeshEvent, MeshIdentity } from "./store.js";

/**
 * File mesh <-> NATS control carrier (smarty-dev#15310, A' lane L2). Pure mapping between the
 * file mesh's `fabric.control.command` / `fabric.control.ack` events, as MeshBridge hands them to
 * its remote BridgeSide and reads them back, and the TransportEnvelope the NATS control plane
 * sends and consumes. No I/O of its own: the caller supplies verified presence, the allow gate,
 * the durable ledger and the send.
 *
 * Trust (Tier 1):
 * - A sender's identity comes only from verified presence on the side it came from: the file
 *   participant directory outbound, NATS KV presence whose machine equals the delivery's
 *   credential-bound source machine inbound. A body or an envelope label never names a sender.
 * - Command ids are MAPPED per hop (mapNatsCommandId). A file owner replays a stored outcome, ask
 *   results included, to the replyTo of any later command with the same id, so an unmapped id
 *   would let one sender fish another sender's answer. cancelCommandId maps the same way, so a
 *   cancel reaches only its own sender's commands, and every ACK maps back through the ledger.
 * - Every resend of one source event carries the same envelope id and the same bytes; the
 *   destination's dedupe (broker window, then the owner's seen claim) makes it run once.
 * - An ACK crosses only for a command this link carried, from the owner the command went to.
 */

/** Default control deadline for a legacy file command without deadlineAt. */
const LEGACY_DEADLINE_MS = 60_000;
/** One ACK body under the default 256 KiB mesh event and the 1 MiB NATS message, with envelope room. */
export const NATS_CARRIER_MAX_ACK_BYTES = 256 * 1024 - 4 * 1024;

const DIGEST = /^[0-9a-f]{64}$/;
const OPERATIONS: ReadonlySet<string> = new Set(["steer", "followUp", "stop", "ask", "cancel", "setModel", "setThinking"]);
/** The FabricControlCommand wire fields a hop carries; nothing else (bridge stamps, routing, sender stamps) crosses. */
const COMMAND_FIELDS = [
  "version", "commandId", "targetId", "operation", "replyTo", "ownerIncarnation", "message", "data", "wakeCause",
  "triggerTurn", "priority", "binding", "bindingProvenance", "cancelCommandId", "requestCreatedAt", "requestedAt", "deadlineAt",
] as const;
/** Owner outcome fields an ACK carries (control-plane #publishAcknowledgement / #sendNatsAcknowledgement). */
const ACK_FIELDS = [
  "accepted", "messageId", "pendingFollowUps", "oldestAgeS", "stalled", "coalesced", "replacedMessageId", "warning",
  "triggered", "reason", "halted", "result", "error", "errorCode", "notRun", "code", "finalAnswerReceiptId",
  "deliveryStatus", "runState", "ownerIncarnation", "staleIncarnation",
] as const;
/** Infrastructure refusals speak for a relay, never for an owner: they never cross this carrier. */
const RELAY_ERROR_CODES: ReadonlySet<unknown> = new Set([RELAY_DESTINATION_UNAVAILABLE, FABRIC_IDEMPOTENCY_KEY_REUSED]);

export type NatsCarrierDirection = BridgeNatsDirection;

/** A live NATS participant: the identity in its KV presence record and its route (machine included). */
export interface NatsPresenceEntry { readonly identity: MeshIdentity; readonly route: ParticipantRoute }
/** A live native file participant: its directory identity and the host id its runtime replies from. */
export interface FilePresenceEntry { readonly identity: MeshIdentity; readonly hostId: string }

export interface NatsCarrierPresence {
  /** Live KV presence only (L1's verified view); undefined when unknown, lapsed or ambiguous. */
  nats(id: string): NatsPresenceEntry | undefined;
  /** Live native file presence only; never a record the link mirrored from NATS. */
  file(id: string): FilePresenceEntry | undefined;
}

/** The allow-list question for one crossing command (L3's gate answers it). */
export interface NatsCarrierAuthorization {
  readonly direction: NatsCarrierDirection;
  /** Verified on the source side. */
  readonly senderId: string;
  /** Verified on the destination side. */
  readonly targetId: string;
  readonly operation: FabricControlOperation;
  readonly principal?: FabricPrincipal | undefined;
  /** toFile: the delivery's credential-bound source machine the sender was verified on. */
  readonly sourceMachine?: string;
}
export type NatsCarrierDecision =
  | { readonly ok: true; readonly principal: "carry" | "strip" | "none" }
  | { readonly ok: false; readonly refuse: string; readonly reason: string };
export type NatsCarrierAuthorize = (request: NatsCarrierAuthorization) => NatsCarrierDecision;

export type NatsCarrierEnvelope = TransportEnvelope<Record<string, unknown>> & { readonly body: Record<string, unknown> };

/** One carried command and how its ACK maps back. Keyed by (direction, destinationCommandId). */
export interface NatsCarrierRecord {
  readonly v: 1;
  readonly direction: NatsCarrierDirection;
  /** `control`: a carried command; `topic`: a followUp this link made from a work event (N3). */
  readonly origin: "control" | "topic";
  readonly link: string;
  readonly operation: FabricControlOperation;
  /** The id its sender named. */
  readonly sourceCommandId: string;
  /** The id written to the destination (mapNatsCommandId, or the link's own id for a topic followUp). */
  readonly destinationCommandId: string;
  /** toFile: the NATS command envelope id (its ACK is `ack:<id>`); toNats: the file event id. */
  readonly sourceEventId: string;
  /** toNats: this link's deterministic envelope id; toFile: the source envelope id again. */
  readonly destinationEventId: string;
  readonly targetId: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  /** toNats: the destination owner's machine, the only machine whose ACK settles this record. */
  readonly ownerMachine?: string;
  /** The verified sender id. */
  readonly requesterId: string;
  /** The crossing `from` (the verified sender's presence identity, or the link identity for N3). */
  readonly from: MeshIdentity;
  /** The principal as published after carry/strip; absent when none crossed. */
  readonly principal?: FabricPrincipal;
  /** toFile: the credential-bound NATS source machine; toNats: this bridge's machine. */
  readonly sourceMachine: string;
  /** On a cancel: the original id it names and its mapping (same verified sender). */
  readonly sourceCancelCommandId?: string;
  readonly cancelCommandId?: string;
  readonly replyTo: string;
  /** controlBodyFingerprint of the wire body the NATS end digests. */
  readonly bodyDigest: string;
  /** The command's own deadline (NATS body deadlineAt). */
  readonly deadlineAt: number;
  readonly expiresAt: number;
  /** The owner epoch the command was addressed to. */
  readonly ownerIncarnation?: string;
  /**
   * toNats: the exact resolved body sent under destinationEventId to ownerMachine. A resend after any
   * restart rebuilds the envelope from it: same id, same body bytes, the same verified sender id.
   */
  readonly body?: Record<string, unknown>;
  /** The owner's first recorded answer, once settled (data is the ACK as sent back to the source). */
  readonly outcome?: NatsCarrierOutcome;
}

/** What the owner answered, recorded once against a record. */
export interface NatsCarrierOutcome {
  readonly accepted: boolean;
  readonly at: number;
  /** The verified owner identity id the ACK came from, and the ACK's source id. */
  readonly fromId?: string;
  readonly ackEventId?: string;
  /** The translated ACK data republished to the source (bounded). */
  readonly data?: Readonly<Record<string, unknown>>;
}

/**
 * The durable correlation store (lane L3 owns the crash-durable implementation). claim is
 * create-only and returns what is stored: the new record (created), or the earlier one unchanged.
 */
export interface NatsCarrierLedger {
  claim(record: NatsCarrierRecord): Promise<{ readonly created: boolean; readonly record: NatsCarrierRecord }>;
  lookup(direction: NatsCarrierDirection, destinationCommandId: string): Promise<NatsCarrierRecord | undefined>;
  /** Records the owner's outcome. The record stays to its expiry so replays and conflicts still resolve. */
  settle(direction: NatsCarrierDirection, destinationCommandId: string, outcome: NatsCarrierOutcome): Promise<void>;
  /** Optional bookkeeping: the destination confirmed the durable append (PubAck / file append). */
  sent?(direction: NatsCarrierDirection, destinationCommandId: string, receipt: { readonly sequence?: number }): Promise<void>;
  /** Optional bookkeeping: the owner's ACK was durably delivered back to the source side. */
  forwarded?(direction: NatsCarrierDirection, destinationCommandId: string): Promise<void>;
}

export interface NatsCarrierOptions {
  /** The link name: MeshBridge's remoteName and the file mirror's remoteHost. */
  link: string;
  /** This bridge's credential-bound NATS machine. */
  machine: string;
  presence: NatsCarrierPresence;
  ledger: NatsCarrierLedger;
  /** The allow-list gate; required in production (startNatsLink wires L3's). */
  authorize: NatsCarrierAuthorize;
  now?: () => number;
  /** Largest ACK body carried either way (default NATS_CARRIER_MAX_ACK_BYTES). */
  maxAckBytes?: number;
}

/** Retryable: the target's NATS route is missing right now. MeshBridge keeps its cursor before the event. */
export class NatsCarrierUnavailable extends Error {
  override readonly name = "NatsCarrierUnavailable";
  readonly code = "FABRIC_NATS_CARRIER_TARGET_UNAVAILABLE";
  constructor(readonly targetId: string) { super(`NATS route for ${targetId} is unavailable; the event waits for presence`); }
}

/** Thrown for a permanent refusal. "not allowed" makes MeshBridge drop and log the event, never retry it. */
export class NatsCarrierRefusal extends Error {
  override readonly name = "NatsCarrierRefusal";
  constructor(readonly reason: string, readonly code?: string) { super(`Fabric NATS carrier refused: ${reason} (not allowed)`); }
}

export interface NatsCarrierSettle {
  readonly direction: NatsCarrierDirection;
  readonly commandId: string;
  readonly accepted: boolean;
  readonly fromId?: string;
  readonly ackEventId?: string;
  readonly data?: Readonly<Record<string, unknown>>;
}

export type NatsOutbound =
  /** Send this envelope (fixed body); the source event is done only once the destination persisted it. */
  | { readonly kind: "send"; readonly envelope: NatsCarrierEnvelope; readonly record?: NatsCarrierRecord; readonly settle?: NatsCarrierSettle }
  /** Nothing to send; the source event is complete (a cancel naming no command this link carried). */
  | { readonly kind: "absorb"; readonly reason: string };

export type NatsInbound =
  /** Append this event to the file mesh; ACK the delivery only after that append is durable, then settle. */
  | { readonly kind: "event"; readonly event: MeshEvent; readonly settle?: NatsCarrierSettle }
  /** Send this ACK to the NATS sender (the link answers for the hop, notRun), then ACK the delivery. */
  | { readonly kind: "reply"; readonly envelope: NatsCarrierEnvelope; readonly reason: string }
  /** Nothing for the file mesh: settle (when given), then ACK the delivery. */
  | {
    readonly kind: "settled"; readonly reason: string; readonly settle?: NatsCarrierSettle;
    readonly record?: NatsCarrierRecord; readonly ack?: Readonly<Record<string, unknown>>;
  }
  /** Malformed or unauthorized: TERM the delivery; it never reaches the file mesh and gets no answer. */
  | { readonly kind: "refused"; readonly reason: string };

/** One NATS inbox delivery, as a raw TransportDelivery or L1's BridgeNatsPortDelivery supplies it. */
export interface NatsCarrierDelivery {
  readonly envelope: unknown;
  /** Credential-bound (last subject token): never a body field. */
  readonly sourceMachine: string;
  readonly sequence: number;
}

export const carrierDeliveryFromTransport = (delivery: TransportDelivery<TransportEnvelope>): NatsCarrierDelivery => ({
  envelope: delivery.payload, sourceMachine: delivery.sourceMachine, sequence: delivery.sequence,
});

export const carrierDeliveryFromPort = (delivery: BridgeNatsPortDelivery): NatsCarrierDelivery => ({
  envelope: delivery.envelope, sourceMachine: delivery.sourceMachine, sequence: delivery.streamSequence,
});

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const nonEmpty = (value: unknown, max = 512): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= max && !/[\s\u0000-\u001f\u007f]/.test(value);

const wire = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;

/**
 * The id a command carries on the destination side of one hop: deterministic and bound to the
 * link, the direction and the VERIFIED sender, so no sender can name another sender's command.
 */
export const mapNatsCommandId = (link: string, direction: NatsCarrierDirection, requesterId: string, commandId: string): string =>
  bridgeNatsMappedCommandId(link, direction, requesterId, commandId);

/** A verified identity, copied field by field (never a caller object with extra keys). */
export const carrierIdentity = (identity: MeshIdentity): MeshIdentity => ({
  id: identity.id, name: identity.name, kind: identity.kind,
  ...(typeof identity.sessionId === "string" ? { sessionId: identity.sessionId } : {}),
});

const pick = (source: Record<string, unknown>, fields: readonly string[]): Record<string, unknown> => {
  const out: Record<string, unknown> = {};
  for (const field of fields) if (Object.hasOwn(source, field) && source[field] !== undefined) out[field] = source[field];
  return out;
};

/** The FabricControlCommand shape check (control-plane commandFromEvent). Returns a reason when the body is not one. */
export const controlCommandProblem = (data: Record<string, unknown>, operation?: unknown): string | undefined => {
  if (data.version !== 1) return "command version is not 1";
  if (!nonEmpty(data.commandId)) return "command has no commandId";
  if (!nonEmpty(data.targetId)) return "command has no targetId";
  if (typeof data.operation !== "string" || !OPERATIONS.has(data.operation)) return "command operation is unknown";
  if (operation !== undefined && operation !== data.operation) return "command kind and operation differ";
  if (!nonEmpty(data.replyTo)) return "command has no replyTo";
  if (typeof data.requestedAt !== "number" || !Number.isFinite(data.requestedAt)) return "command requestedAt is not finite";
  if (data.requestCreatedAt !== undefined && (typeof data.requestCreatedAt !== "number" || !Number.isFinite(data.requestCreatedAt))) {
    return "command requestCreatedAt is not finite";
  }
  if (data.deadlineAt !== undefined && (typeof data.deadlineAt !== "number" || !Number.isFinite(data.deadlineAt) ||
      data.deadlineAt < data.requestedAt)) return "command deadlineAt is invalid";
  if (data.operation === "cancel" && !nonEmpty(data.cancelCommandId)) return "cancel has no cancelCommandId";
  if (data.operation !== "cancel" && data.cancelCommandId !== undefined) return "cancelCommandId on a non-cancel command";
  if (data.ownerIncarnation !== undefined && (typeof data.ownerIncarnation !== "string" || !data.ownerIncarnation ||
      data.ownerIncarnation.length > 128)) return "command ownerIncarnation is invalid";
  if (data.message !== undefined && typeof data.message !== "string") return "command message is not a string";
  if (data.triggerTurn !== undefined && typeof data.triggerTurn !== "boolean") return "command triggerTurn is not a boolean";
  if (data.priority !== undefined && (data.priority !== "interrupt" || data.operation !== "steer")) return "command priority is invalid";
  if (data.bindingProvenance !== undefined && (!isObject(data.bindingProvenance) || data.bindingProvenance.kind !== "owner-defaults" ||
      typeof data.bindingProvenance.rootId !== "string")) return "command bindingProvenance is invalid";
  if (data.binding !== undefined && (!isObject(data.binding) ||
      (data.binding.model !== undefined && typeof data.binding.model !== "string") ||
      (data.binding.thinking !== undefined && typeof data.binding.thinking !== "string"))) return "command binding is invalid";
  return undefined;
};

/**
 * Keep an ACK body inside the carrier's byte budget, as the owner's #boundedAcceptance does: an
 * accepted delivery first drops only its advisory warning; anything still too large becomes a
 * bounded refusal (never notRun: the handler ran).
 */
export const boundNatsAck = (body: Record<string, unknown>, maxBytes: number): Record<string, unknown> => {
  const size = (value: unknown): number => Buffer.byteLength(JSON.stringify(value), "utf8");
  try {
    if (size(body) <= maxBytes) return body;
    if (body.accepted === true && Object.hasOwn(body, "warning")) {
      const { warning: _warning, ...delivery } = body;
      if (size(delivery) <= maxBytes) return delivery;
    }
  } catch { /* An unserializable result is refused below. */ }
  const identity = pick(body, ["version", "commandId", "targetId", "bodyDigest", "ownerIncarnation"]);
  return { ...identity, accepted: false, error: `Fabric control result exceeds ${maxBytes} bridge bytes` };
};

/** The owner's outcome fields only, shape-checked where a wrong type would change meaning. */
const ackOutcome = (data: Record<string, unknown>): Record<string, unknown> | undefined => {
  if (typeof data.accepted !== "boolean" || RELAY_ERROR_CODES.has(data.errorCode)) return undefined;
  let outcome: Record<string, unknown>;
  try { outcome = wire(pick(data, ACK_FIELDS)); } catch { return undefined; }
  if (outcome.staleIncarnation !== undefined && outcome.staleIncarnation !== null && typeof outcome.staleIncarnation !== "string") {
    delete outcome.staleIncarnation;
  }
  if (outcome.ownerIncarnation !== undefined && typeof outcome.ownerIncarnation !== "string") delete outcome.ownerIncarnation;
  if (data.accepted === true) delete outcome.notRun;
  return outcome;
};

/** A file owner's stale-epoch refusal: exactly the control plane's typed answer, from a valid new epoch. */
const isStaleIncarnationAnswer = (ack: Readonly<Record<string, unknown>>): boolean =>
  ack.accepted === false && ack.errorCode === CONTROL_STALE_INCARNATION && ack.error === STALE_INCARNATION_ERROR &&
  typeof ack.ownerIncarnation === "string" && ack.ownerIncarnation.length > 0 &&
  (ack.staleIncarnation === null || typeof ack.staleIncarnation === "string");

/** True when a NATS refusal proves the owner never ran the command under a stale owner epoch. */
export const isStaleIncarnationRefusal = (ack: Readonly<Record<string, unknown>> | undefined): boolean =>
  ack?.accepted === false && ack.notRun === true && ack.errorCode === CONTROL_STALE_INCARNATION;

export class NatsBridgeCarrier {
  readonly link: string;
  readonly machine: string;
  readonly #presence: NatsCarrierPresence;
  readonly #ledger: NatsCarrierLedger;
  readonly #authorize: NatsCarrierAuthorize;
  readonly #now: () => number;
  readonly #maxAckBytes: number;

  constructor(options: NatsCarrierOptions) {
    if (!nonEmpty(options.link, 128)) throw new Error("NATS carrier link name is invalid");
    if (!BRIDGE_NATS_MACHINE.test(options.machine)) throw new Error("NATS carrier machine is invalid");
    this.link = options.link;
    this.machine = options.machine;
    this.#presence = options.presence;
    this.#ledger = options.ledger;
    this.#authorize = options.authorize;
    this.#now = options.now ?? Date.now;
    this.#maxAckBytes = options.maxAckBytes ?? NATS_CARRIER_MAX_ACK_BYTES;
  }

  /** File -> NATS: a control command or ACK MeshBridge hands the remote side. Topics go to the N3 translator. */
  async outbound(pub: BridgePublish): Promise<NatsOutbound> {
    if (pub.topic === BRIDGE_NATS_CONTROL_TOPIC) return this.#outboundCommand(pub);
    if (pub.topic === BRIDGE_NATS_ACK_TOPIC) return this.#outboundAck(pub);
    throw new NatsCarrierRefusal(`topic ${pub.topic} is not a control topic`);
  }

  /**
   * Record the owner's outcome. With `forwarded`, the ACK was also durably delivered back to the
   * source side (a file append, or a NATS PubAck): call only after that.
   */
  async settle(settle: NatsCarrierSettle, forwarded = false): Promise<void> {
    await this.#ledger.settle(settle.direction, settle.commandId, { accepted: settle.accepted, at: this.#now(),
      ...(settle.fromId !== undefined ? { fromId: settle.fromId } : {}),
      ...(settle.ackEventId !== undefined ? { ackEventId: settle.ackEventId } : {}),
      ...(settle.data !== undefined ? { data: settle.data } : {}) });
    if (forwarded) await this.#ledger.forwarded?.(settle.direction, settle.commandId);
  }

  /** The destination confirmed a command's durable append (bookkeeping; resends never depend on it). */
  async sent(record: NatsCarrierRecord, sequence?: number): Promise<void> {
    await this.#ledger.sent?.(record.direction, record.destinationCommandId, sequence !== undefined ? { sequence } : {});
  }

  async #outboundCommand(pub: BridgePublish): Promise<NatsOutbound> {
    const data = pub.data as Record<string, unknown>;
    const problem = controlCommandProblem(data, pub.kind);
    if (problem) throw new NatsCarrierRefusal(problem);
    const sender = this.#presence.file(pub.from.id);
    // A live native file participant, whose ACK (and an ask's answer) returns to its own runtime only.
    if (!sender || sender.identity.id !== pub.from.id) throw new NatsCarrierRefusal("sender is not a live file participant");
    if (data.replyTo !== sender.hostId) throw new NatsCarrierRefusal("replyTo is not the sender's own host");
    const targetId = data.targetId as string;
    const owner = this.#presence.nats(targetId);
    // Only a NATS-native owner: a route back to this bridge's own machine would loop into the file mesh.
    if (owner && owner.route.machine === this.machine) throw new NatsCarrierRefusal("target is a file participant this bridge mirrors");
    // MeshBridge just held the target's file mirror. A KV gap (reconnect, watch reset) is a wait, never a
    // drop: the cursor stays before the command, and MeshBridge itself drops it if the mirror lapses.
    if (!owner?.route.live || !BRIDGE_NATS_MACHINE.test(owner.route.machine)) throw new NatsCarrierUnavailable(targetId);
    if (owner.route.ownerHostId !== pub.to) throw new NatsCarrierRefusal("command host is not the target's NATS owner");
    const operation = data.operation as FabricControlOperation;
    const requesterId = sender.identity.id;
    const decision = this.#authorize({ direction: "toNats", senderId: requesterId, targetId, operation, principal: pub.principal });
    if (!decision.ok) throw new NatsCarrierRefusal(decision.reason, decision.refuse);
    const map = (id: string): string => mapNatsCommandId(this.link, "toNats", requesterId, id);
    const commandId = map(data.commandId as string);
    let cancelCommandId: string | undefined;
    if (operation === "cancel") {
      cancelCommandId = map(data.cancelCommandId as string);
      const cancelled = await this.#ledger.lookup("toNats", cancelCommandId);
      // A cancel reaches only a command this link carried for the same sender and target.
      if (!cancelled || cancelled.origin !== "control" || cancelled.requesterId !== requesterId || cancelled.targetId !== targetId) {
        return { kind: "absorb", reason: "cancel names no command this link carried for its sender" };
      }
    }
    const principal = decision.principal === "carry" ? copyFabricPrincipal(pub.principal) : undefined;
    const requestedAt = data.requestedAt as number;
    const body: Record<string, unknown> = wire({
      ...pick(data, COMMAND_FIELDS),
      commandId, ...(cancelCommandId !== undefined ? { cancelCommandId } : {}),
      deadlineAt: typeof data.deadlineAt === "number" ? data.deadlineAt : requestedAt + LEGACY_DEADLINE_MS,
    });
    const bodyDigest = controlBodyFingerprint(body, principal);
    const sourceEventId = pub.data.bridge.id;
    if (!nonEmpty(sourceEventId)) throw new NatsCarrierRefusal("command has no source event id");
    const envelope = this.#envelope({
      id: bridgeNatsEnvelopeId(this.link, sourceEventId), kind: "command",
      to: { machine: owner.route.machine, hostId: owner.route.ownerHostId }, targetId, operation,
      from: carrierIdentity(sender.identity), principal, body,
    });
    const record: NatsCarrierRecord = {
      v: 1, direction: "toNats", origin: "control", link: this.link, operation,
      sourceCommandId: data.commandId as string, destinationCommandId: commandId,
      sourceEventId, destinationEventId: envelope.id, targetId,
      ownerHostId: owner.route.ownerHostId, ownerIdentityId: owner.route.ownerIdentityId, ownerMachine: owner.route.machine,
      requesterId, from: envelope.from, ...(principal ? { principal } : {}), sourceMachine: this.machine,
      ...(cancelCommandId !== undefined ? { sourceCancelCommandId: data.cancelCommandId as string, cancelCommandId } : {}),
      replyTo: data.replyTo as string, bodyDigest,
      deadlineAt: body.deadlineAt as number, expiresAt: Math.max(body.deadlineAt as number, this.#now()) + BRIDGE_NATS_HORIZON_MS,
      ...(typeof body.ownerIncarnation === "string" ? { ownerIncarnation: body.ownerIncarnation } : {}), body,
    };
    const { created, record: stored } = await this.#ledger.claim(record);
    if (created) return { kind: "send", envelope, record };
    if (stored.direction !== "toNats" || stored.origin !== "control" || stored.bodyDigest !== bodyDigest ||
        stored.requesterId !== requesterId || stored.targetId !== targetId || !stored.body || !stored.ownerMachine) {
      throw new NatsCarrierRefusal("command id reused with different input");
    }
    // The same file event again (restart, uncertain PubAck): its first envelope, byte for byte.
    if (stored.sourceEventId === sourceEventId) {
      return { kind: "send", record: stored, envelope: this.#envelope({
        id: stored.destinationEventId, kind: "command", to: { machine: stored.ownerMachine, hostId: stored.ownerHostId },
        targetId, operation, from: stored.from, principal: stored.principal, body: stored.body,
      }) };
    }
    // A same-key resend (a new file event, the same commandId and input): a new envelope, so the broker
    // window never swallows it, and the NATS owner replays its stored outcome from its seen claim.
    return { kind: "send", record: stored, envelope };
  }

  async #outboundAck(pub: BridgePublish): Promise<NatsOutbound> {
    const data = pub.data as Record<string, unknown>;
    if (data.version !== 1 || !nonEmpty(data.commandId) || !nonEmpty(data.targetId)) throw new NatsCarrierRefusal("ack is malformed");
    const outcome = ackOutcome(data);
    if (!outcome) throw new NatsCarrierRefusal("ack carries no owner outcome");
    const record = await this.#ledger.lookup("toFile", data.commandId);
    if (!record || record.origin !== "control") throw new NatsCarrierRefusal("ack names no command this link carried");
    if (pub.to !== record.replyTo || data.targetId !== record.targetId || pub.from.id !== record.ownerIdentityId) {
      throw new NatsCarrierRefusal("ack does not match its carried command");
    }
    const owner = this.#presence.file(pub.from.id);
    if (!owner || owner.identity.id !== record.ownerIdentityId || owner.hostId !== record.ownerHostId) {
      throw new NatsCarrierRefusal("ack sender is not the command's live file owner");
    }
    // A file owner refuses a stale epoch before any claim or handler (control-plane #acceptCommand), but
    // says so without notRun (the file sender's own no-resend policy). A NATS sender accepts a stale
    // refusal from a new epoch only with notRun (#acceptNatsAcknowledgement): state the proven fact.
    if (isStaleIncarnationAnswer(outcome)) outcome.notRun = true;
    // The NATS sender settles on the owner's machine (this bridge), its owner identity and the digest of its own wire.
    const body = boundNatsAck({ ...outcome, version: 1, commandId: record.sourceCommandId, targetId: record.targetId,
      bodyDigest: record.bodyDigest }, this.#maxAckBytes);
    const envelope = this.#envelope({
      id: bridgeNatsAckEnvelopeId(record.sourceEventId), kind: "ack", to: { machine: record.sourceMachine, hostId: record.replyTo },
      targetId: record.targetId, operation: body.accepted === true ? "accepted" : "rejected",
      from: carrierIdentity(owner.identity), body,
    });
    return { kind: "send", envelope, record,
      settle: { direction: "toFile", commandId: record.destinationCommandId, accepted: body.accepted === true,
        fromId: owner.identity.id, ackEventId: pub.data.bridge.id, data: body } };
  }

  /** NATS -> file: one delivery from this bridge machine's inbox. */
  async inbound(delivery: NatsCarrierDelivery): Promise<NatsInbound> {
    const envelope = isObject(delivery.envelope) ? delivery.envelope : undefined;
    const from = envelope && isObject(envelope.from) ? envelope.from : undefined;
    const to = envelope && isObject(envelope.to) ? envelope.to : undefined;
    const body = envelope && isObject(envelope.body) ? envelope.body : undefined;
    if (!envelope || !from || !to || !body || !nonEmpty(envelope.id) || !nonEmpty(from.id) || !nonEmpty(to.hostId) ||
        !nonEmpty(envelope.targetId) || typeof envelope.operation !== "string" ||
        !Number.isSafeInteger(delivery.sequence) || delivery.sequence <= 0) return { kind: "refused", reason: "malformed envelope" };
    if (to.machine !== this.machine) return { kind: "refused", reason: "envelope is not addressed to this bridge machine" };
    if (!BRIDGE_NATS_MACHINE.test(delivery.sourceMachine) || delivery.sourceMachine === this.machine) {
      return { kind: "refused", reason: "source machine is invalid or this bridge itself" };
    }
    if (envelope.id.startsWith("relay:") || RELAY_ERROR_CODES.has(body.errorCode)) {
      return { kind: "refused", reason: "relay refusals do not cross this carrier" };
    }
    // Identity only from live KV presence on the credential-bound source machine.
    const sender = this.#presence.nats(from.id);
    if (!sender?.route.live || sender.route.machine !== delivery.sourceMachine || sender.identity.id !== from.id) {
      return { kind: "refused", reason: "sender is not live on its source machine" };
    }
    const input = { envelope, id: envelope.id, hostId: to.hostId, targetId: envelope.targetId, operation: envelope.operation };
    if (envelope.kind === "ack") return this.#inboundAck(delivery, input, sender, body);
    if (envelope.kind === "command") return this.#inboundCommand(delivery, input, sender, body);
    return { kind: "refused", reason: "unknown envelope kind" };
  }

  async #inboundCommand(
    delivery: NatsCarrierDelivery, input: InboundEnvelope, sender: NatsPresenceEntry, body: Record<string, unknown>,
  ): Promise<NatsInbound> {
    const problem = controlCommandProblem(body, input.operation);
    if (problem || body.targetId !== input.targetId || typeof body.deadlineAt !== "number") {
      return { kind: "refused", reason: problem ?? "command envelope and body differ" };
    }
    const sourceMachine = delivery.sourceMachine;
    const requesterId = sender.identity.id;
    // The ACK (and an ask's answer) returns only to the sender's own runtime.
    if (body.replyTo !== sender.route.ownerHostId) return { kind: "refused", reason: "replyTo is not the sender's own host" };
    const targetId = input.targetId;
    const target = this.#presence.nats(targetId);
    // The target must be one this bridge mirrors into KV, under the host the sender addressed.
    if (!target?.route.live || target.route.machine !== this.machine || target.route.ownerHostId !== input.hostId) {
      return { kind: "refused", reason: "target is not a file participant this bridge carries" };
    }
    const principal = copyFabricPrincipal(input.envelope.principal);
    // The NATS sender's ACK check compares the digest of its own wire (body and principal as sent): digest that, unmapped.
    const bodyDigest = controlBodyFingerprint(wire(body), input.envelope.principal === undefined ? undefined : wire(input.envelope.principal));
    const refuse = (acceptance: Record<string, unknown>, reason: string): NatsInbound =>
      ({ kind: "reply", reason, envelope: this.#refusalAck(input.id, sourceMachine, body, bodyDigest, target, acceptance) });
    const operation = body.operation as FabricControlOperation;
    const decision = this.#authorize({ direction: "toFile", senderId: requesterId, targetId, operation, principal, sourceMachine });
    if (!decision.ok) {
      // A verified sender to a target this link speaks for: a definite, notRun answer beats a timeout.
      return refuse({ accepted: false, error: `Fabric NATS bridge refused the command: ${decision.refuse}`, notRun: true }, decision.reason);
    }
    const map = (id: string): string => mapNatsCommandId(this.link, "toFile", requesterId, id);
    const commandId = map(body.commandId as string);
    let cancelCommandId: string | undefined;
    if (operation === "cancel") {
      cancelCommandId = map(body.cancelCommandId as string);
      const cancelled = await this.#ledger.lookup("toFile", cancelCommandId);
      if (!cancelled || cancelled.origin !== "control" || cancelled.requesterId !== requesterId || cancelled.targetId !== targetId) {
        return { kind: "settled", reason: "cancel names no command this link carried for its sender" };
      }
    }
    const carried = decision.principal === "carry" ? principal : undefined;
    const record: NatsCarrierRecord = {
      v: 1, direction: "toFile", origin: "control", link: this.link, operation,
      sourceCommandId: body.commandId as string, destinationCommandId: commandId, sourceEventId: input.id, destinationEventId: input.id,
      targetId, ownerHostId: target.route.ownerHostId, ownerIdentityId: target.route.ownerIdentityId,
      requesterId, from: carrierIdentity(sender.identity), ...(carried ? { principal: carried } : {}), sourceMachine,
      ...(cancelCommandId !== undefined ? { sourceCancelCommandId: body.cancelCommandId as string, cancelCommandId } : {}),
      replyTo: body.replyTo as string, bodyDigest, deadlineAt: body.deadlineAt as number,
      expiresAt: Math.max(body.deadlineAt as number, this.#now()) + BRIDGE_NATS_HORIZON_MS,
      ...(typeof body.ownerIncarnation === "string" ? { ownerIncarnation: body.ownerIncarnation } : {}),
    };
    const { created, record: stored } = await this.#ledger.claim(record);
    if (!created) {
      if (stored.direction !== "toFile" || stored.origin !== "control" || stored.bodyDigest !== bodyDigest ||
          stored.targetId !== targetId || stored.operation !== operation) {
        // The owner never sees a reused id: answer for the hop, as the NATS receiver does (notRun).
        return refuse({ accepted: false, error: "Fabric control id reused with different input",
          errorCode: "FABRIC_CONTROL_ID_CONFLICT", notRun: true }, "command id reused with different input");
      }
      if (stored.replyTo !== record.replyTo || stored.sourceMachine !== sourceMachine) {
        return { kind: "settled", reason: "command already carried for another reply address" };
      }
      if (stored.sourceEventId !== input.id) {
        // A same-key resend under a new envelope. The file store dedupes the command by its mapped id, so the
        // owner never sees it again: replay the owner's recorded answer to this envelope, as a NATS owner
        // replays its seen claim. With no answer yet, the first ACK settles both (a NATS sender matches an
        // ACK by commandId and digest, never by envelope id).
        const answer = stored.outcome?.data;
        if (!answer) return { kind: "settled", reason: "same-key resend; the owner's answer is still pending" };
        return { kind: "reply", reason: "same-key resend answered from the recorded outcome", envelope: this.#envelope({
          id: bridgeNatsAckEnvelopeId(input.id), kind: "ack", to: { machine: sourceMachine, hostId: stored.replyTo },
          targetId, operation: answer.accepted === true ? "accepted" : "rejected", from: carrierIdentity(target.identity),
          // The responder's current epoch authenticates this attempt; the recorded one is history.
          body: { ...answer, ownerIncarnation: target.route.ownerIncarnation,
            originalOwnerIncarnation: typeof answer.ownerIncarnation === "string" ? answer.ownerIncarnation : null },
        }) };
      }
    }
    const data = wire({ ...pick(body, COMMAND_FIELDS), commandId, ...(cancelCommandId !== undefined ? { cancelCommandId } : {}) });
    const event: MeshEvent = {
      id: input.id, sequence: delivery.sequence, createdAt: body.requestedAt as number,
      topic: BRIDGE_NATS_CONTROL_TOPIC, kind: operation, from: carrierIdentity(sender.identity), to: input.hostId,
      ...(carried ? { principal: carried } : {}), verification: "bridge", data,
    };
    return { kind: "event", event };
  }

  async #inboundAck(
    delivery: NatsCarrierDelivery, input: InboundEnvelope, sender: NatsPresenceEntry, body: Record<string, unknown>,
  ): Promise<NatsInbound> {
    if (!input.id.startsWith("ack:") || body.version !== 1 || !nonEmpty(body.commandId) || body.targetId !== input.targetId ||
        typeof body.accepted !== "boolean" || input.operation !== (body.accepted ? "accepted" : "rejected") ||
        typeof body.bodyDigest !== "string" || !DIGEST.test(body.bodyDigest)) return { kind: "refused", reason: "malformed ack" };
    const record = await this.#ledger.lookup("toNats", body.commandId);
    if (!record) return { kind: "settled", reason: "ack names no command this link sent" };
    // Only the owner the command went to, from its own machine, about the exact wire this link sent.
    // Not bound to one envelope id: a same-key resend goes out under its own envelope, and the owner's
    // replay answers that one (control-plane binds NATS ACKs to commandId and digest the same way).
    if (record.ownerMachine !== delivery.sourceMachine || sender.identity.id !== record.ownerIdentityId ||
        body.targetId !== record.targetId || body.bodyDigest !== record.bodyDigest || input.hostId !== record.replyTo) {
      return { kind: "refused", reason: "ack does not match the command this link sent" };
    }
    const outcome = ackOutcome(body);
    if (!outcome) return { kind: "refused", reason: "ack carries no owner outcome" };
    const data = boundNatsAck({ version: 1, commandId: record.sourceCommandId, targetId: record.targetId, ...outcome },
      this.#maxAckBytes);
    const settle: NatsCarrierSettle = { direction: "toNats", commandId: record.destinationCommandId, accepted: body.accepted,
      fromId: sender.identity.id, ackEventId: input.id, data };
    // N3 followUps and cancels have no file sender waiting on an ACK.
    if (record.origin === "topic" || record.operation === "cancel") {
      return { kind: "settled", reason: record.origin === "topic" ? "topic followUp settled" : "cancel settled", settle, record, ack: outcome };
    }
    const event: MeshEvent = {
      id: input.id, sequence: delivery.sequence, createdAt: this.#now(), topic: BRIDGE_NATS_ACK_TOPIC,
      kind: data.accepted === true ? "accepted" : "rejected", from: carrierIdentity(sender.identity), to: record.replyTo,
      verification: "bridge", data,
    };
    return { kind: "event", event, settle };
  }

  /** An ACK the link answers for the hop, shaped exactly as the NATS owner's (#sendNatsAcknowledgement). */
  #refusalAck(
    commandEventId: string, sourceMachine: string, body: Record<string, unknown>, bodyDigest: string,
    target: NatsPresenceEntry, acceptance: Record<string, unknown>,
  ): NatsCarrierEnvelope {
    return this.#envelope({
      id: bridgeNatsAckEnvelopeId(commandEventId), kind: "ack", to: { machine: sourceMachine, hostId: body.replyTo as string },
      targetId: body.targetId as string, operation: acceptance.accepted === true ? "accepted" : "rejected",
      from: carrierIdentity(target.identity),
      body: { ...acceptance, version: 1, commandId: body.commandId, targetId: body.targetId, bodyDigest,
        ownerIncarnation: target.route.ownerIncarnation },
    });
  }

  #envelope(input: {
    id: string; kind: "command" | "ack"; to: { machine: string; hostId: string }; targetId: string; operation: string;
    from: MeshIdentity; principal?: FabricPrincipal | undefined; body: Record<string, unknown>;
  }): NatsCarrierEnvelope {
    return Object.freeze({
      id: input.id, kind: input.kind, to: Object.freeze({ ...input.to }), targetId: input.targetId, operation: input.operation,
      from: input.from, ...(input.principal ? { principal: input.principal } : {}), body: wire(input.body),
    });
  }
}

interface InboundEnvelope {
  readonly envelope: Record<string, unknown>;
  readonly id: string;
  readonly hostId: string;
  readonly targetId: string;
  readonly operation: string;
}
