// src/mesh/bridge-nats-types.ts: the shared contract of A', the NATS <-> file-mesh bridge
// (smarty-dev#15310, D3 gate 3). Owned by lane L3. L1 (raw NATS port), L2 (link, NatsCarrierSide,
// translators), L4 (harness) and L5 (CLI/config) code against these types; change them only
// through L3 (via the integrator, fabric-v2).
//
// Import-light on purpose: type-only edges plus node:crypto for the deterministic ids that every
// lane must compute identically. No runtime state, no I/O.
//
// Boundary (adopted by the integrator, coordination note 1):
//  - The link IS the existing MeshBridge: local side StoreBridgeSide (Ryzen 1 file store), remote
//    side L2's NatsCarrierSide (src/mesh/bridge-nats-file.ts), a BridgeSide over L1's raw port
//    (BridgeNatsPort below). MeshBridge's tested identity, allow, route and cursor checks stay in use.
//  - L2 owns file event <-> TransportEnvelope mapping for every op, deterministic envelope ids,
//    same-bytes resend and bodyDigest. L3 owns identity/allow-list and the durable ledger.
//
// Trust rules (Tier 1, docs/bridge-nats-threat-model.md):
//  1. A sender id in a message body is a lookup key, never an identity. The identity that crosses is
//     built only from the verified presence of the side the message came from
//     (BridgeNatsVerifiedIdentity, produced by bridge-nats-identity.ts).
//  2. NATS presence is verified against the credential-bound source machine of the delivery.
//  3. commandId is MAPPED per hop: mapped = bridgeNatsMappedCommandId(link, direction, verified sender,
//     original id); cancelCommandId the same way; the owner's ACK maps back through the ledger.
//     A receiver replays a stored outcome to whatever replyTo arrives on a commandId collision, so an
//     unmapped id would let one sender read another's result.
//  4. Exactly-once across the hop is keyed by the mapped commandId, durably claimed in the ledger
//     (bridge-nats-ledger.ts) before the cross publish.
//  5. The source is acknowledged only after the destination's durable append (NATS: after MeshBridge
//     reads past the delivery's sequence).
//  6. The allow-list is static, loaded from config, and frozen: there is no runtime widening.
import { createHash } from "node:crypto";
import type { MeshEvent, MeshIdentity } from "./event-log.js";
import type { BridgePresence, BridgeSide } from "./bridge.js";
import type { FabricControlOperation } from "../topology/control-plane.js";
import type { ParticipantRoute } from "../topology/participant-view.js";
import type { FabricPrincipal } from "../fabric-provenance.js";
import type { TransportEnvelope, TransportSent } from "../transport/types.js";

/** Contract version. Bump only with a coordinated change across L1/L2/L4/L5. */
export const BRIDGE_NATS_CONTRACT_VERSION = 2 as const;

// ---------------------------------------------------------------------------------------------
// Sides and directions
// ---------------------------------------------------------------------------------------------

/** Where an identity is NATIVE. "file" = Ryzen 1's file mesh (MeshBridge local); "nats" = the org broker. */
export type BridgeNatsSide = "file" | "nats";

/** toNats = file -> NATS (MeshBridge "toRemote"); toFile = NATS -> file (MeshBridge "toLocal"). */
export type BridgeNatsDirection = "toNats" | "toFile";

export const BRIDGE_NATS_SOURCE_SIDE: Readonly<Record<BridgeNatsDirection, BridgeNatsSide>> =
  Object.freeze({ toNats: "file", toFile: "nats" });
export const BRIDGE_NATS_DESTINATION_SIDE: Readonly<Record<BridgeNatsDirection, BridgeNatsSide>> =
  Object.freeze({ toNats: "nats", toFile: "file" });
/** The NATS side is MeshBridge's `remote` BridgeSide; the file mesh is its `local` side. */
export const BRIDGE_NATS_MESH_DIRECTION: Readonly<Record<BridgeNatsDirection, "toRemote" | "toLocal">> =
  Object.freeze({ toNats: "toRemote", toFile: "toLocal" });

/** The control operations that may cross (control-plane.ts FabricControlOperation). */
export type BridgeNatsOperation = FabricControlOperation;
export const BRIDGE_NATS_OPERATIONS: readonly BridgeNatsOperation[] =
  Object.freeze(["steer", "followUp", "stop", "ask", "cancel", "setModel", "setThinking"] as const);

/** The mesh topics of control traffic (control-plane.ts CONTROL_TOPIC / ACK_TOPIC). */
export const BRIDGE_NATS_CONTROL_TOPIC = "fabric.control.command" as const;
export const BRIDGE_NATS_ACK_TOPIC = "fabric.control.ack" as const;

/** NATS seen horizon (control-plane.ts NATS_HORIZON_MS): the ledger keeps a record this long past its deadline. */
export const BRIDGE_NATS_HORIZON_MS = 24 * 60 * 60 * 1_000;

/** NATS machine token (src/transport/nats/subjects.ts MACHINE). */
export const BRIDGE_NATS_MACHINE = /^[a-z0-9][a-z0-9-]{0,31}$/;

/**
 * Canonical Fabric session id: `session:` + lowercase UUID. This is exactly smarty-dev
 * `setup/factory/org_target.py` canonical() (SESSION regex, `session:` prefix, lower case), which
 * `bin/smarty-org-target` and `bin/smarty-send` use for org.json `orgInstance.fabricSession` pins.
 */
export const BRIDGE_NATS_SESSION_ID = /^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

// ---------------------------------------------------------------------------------------------
// Presence and verified identity
// ---------------------------------------------------------------------------------------------

/**
 * One presence record in side-neutral form. L1's port yields these for KV presence (every record
 * answering to an id, on any machine, live or not); L2 maps the file mesh's host/participant
 * records. Only these records, never message bodies, produce identities.
 */
export interface BridgeNatsPresenceRecord {
  /** Participant id: `session:<uuid>` for a Main root; the actor's participant id for an actor. */
  readonly id: string;
  readonly name: string;
  /** MeshIdentity kind: a root participant is "main". */
  readonly kind: MeshIdentity["kind"];
  /** The participant's root (`session:<uuid>`); a root's own id for a root. */
  readonly rootId: string;
  readonly sessionId?: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation?: string;
  /** NATS only: the machine of the KV key that holds the record (credential-bound writer), never a JSON field. */
  readonly machine?: string;
  /**
   * File only: set when the record is a mirror (its `remoteHost`, the link that mirrored it). On NATS a
   * mirror is a record under the bridge machine (KV presence refuses `remoteHost`), so this stays unset there.
   */
  readonly mirroredBy?: string;
  /** Live by this side's lease rules at observation time (NATS: presence + lease pair, same incarnation). */
  readonly live: boolean;
  /** Bridge clock (ms) when this record was read. */
  readonly observedAt: number;
}

/** A side's presence, read at one observation. L1's port (NATS) and L2 (file) implement it. */
export interface BridgeNatsPresenceView {
  readonly side: BridgeNatsSide;
  /** Every record answering to the id, live or not (NATS: on every machine). Ambiguity is decided from these. */
  records(id: string): readonly BridgeNatsPresenceRecord[];
  /** Optional full listing, used only by the dry run to find actor entries named by root + name. */
  list?(): readonly BridgeNatsPresenceRecord[];
}

/**
 * An identity the bridge may speak for. Produced only by bridge-nats-identity.ts from presence of
 * the side the message came from; never constructed from an event's `from`, body or text. The gate
 * refuses an identity object it did not issue itself, or one older than its freshness window.
 */
export interface BridgeNatsVerifiedIdentity {
  readonly side: BridgeNatsSide;
  readonly id: string;
  readonly name: string;
  readonly kind: MeshIdentity["kind"];
  readonly sessionId?: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation?: string;
  /** NATS side: the credential-bound machine it was verified on. Absent for file identities. */
  readonly machine?: string;
  /** The allow-list entry that admitted it. */
  readonly entry: BridgeNatsAllowEntry;
  readonly verifiedAt: number;
}

/** The MeshIdentity a verified identity becomes on the far side (`from` of the crossing event). */
export const bridgeNatsMeshIdentity = (identity: BridgeNatsVerifiedIdentity): MeshIdentity => ({
  id: identity.id, name: identity.name, kind: identity.kind,
  ...(identity.sessionId !== undefined ? { sessionId: identity.sessionId } : {}),
});

/**
 * A publisher on the file mesh that is not a participant (the factory host, smarty-report relay).
 * Topic routes (N3) only; it never sends a control command across.
 */
export interface BridgeNatsNativePublisher {
  readonly side: "file";
  readonly native: true;
  /** The event's `from.id`, recorded for logs; not an identity claim. */
  readonly fromId: string;
}

// ---------------------------------------------------------------------------------------------
// Allow-list (bridge-nats-identity.ts loads, validates and freezes it)
// ---------------------------------------------------------------------------------------------

/** Fields every allow entry may carry (JSON input). */
interface BridgeNatsAllowCommonInput {
  /** Operations this identity may SEND across (default ["followUp"]; [] = none). */
  readonly send?: readonly BridgeNatsOperation[];
  /** Operations this identity may RECEIVE from across (default ["followUp"]; [] = none). A seat is capped (BRIDGE_NATS_SEAT_RECEIVE). */
  readonly receive?: readonly BridgeNatsOperation[];
  /** N3 topic routes this identity may receive: exact topics or a `prefix.*` (e.g. `fleet.work.*`). Roots only. */
  readonly receiveTopics?: readonly string[];
  /** Fabric principal ids whose authority this sender may carry across; any other principal is stripped. */
  readonly principals?: readonly string[];
  /** The machine this identity must be live on: REQUIRED on every allow.nats entry, refused on allow.file. */
  readonly machine?: string;
  /** Operator note; never authority. */
  readonly note?: string;
}

/**
 * One allow entry as written in the link config's `allow.file` / `allow.nats` lists (the list names
 * the side). Ids are exact: no patterns, labels or names stand in for an id.
 * A principal seat (`principal` set) must equal that principal's org.json
 * `orgInstance.fabricSession` (canonical form) exactly, the pin smarty-dev's
 * org_asks.py / paul_queue.py / smarty-org-target / smarty-send trust; the start check refuses a
 * mismatch, and refuses an entry that uses a principal's bound session without declaring that principal.
 */
export type BridgeNatsAllowEntryInput =
  | (BridgeNatsAllowCommonInput & {
    readonly kind: "root";
    /** Canonical root id, `session:<lowercase uuid>`. */
    readonly id: string;
    /** The org.json principal whose org seat this root is. */
    readonly principal?: string;
  })
  | (BridgeNatsAllowCommonInput & {
    readonly kind: "actor";
    /** The actor's owner root, `session:<lowercase uuid>`; must match the actor's verified presence record. */
    readonly rootId: string;
    /** The actor's name; must match the actor's verified presence record. */
    readonly name: string;
    /** Optional pin of the actor's participant id (32 lowercase hex). */
    readonly id?: string;
  });

/** The `allow` section of the link config (L5's NatsLinkConfig imports this). */
export interface BridgeNatsAllowSection {
  /** File-side identities: mirrored into NATS KV; v2 can reach them, and they may send to v2. */
  readonly file?: readonly BridgeNatsAllowEntryInput[];
  /** NATS-side identities: mirrored into the file mesh; the hub can reach them, and they may send to the hub. */
  readonly nats?: readonly BridgeNatsAllowEntryInput[];
}

/** A normalized, frozen allow entry: defaults applied and its home side attached. */
export type BridgeNatsAllowEntry = BridgeNatsAllowEntryInput & {
  /** The side where this identity is native (its home). It may only SEND from there and RECEIVE there. */
  readonly side: BridgeNatsSide;
  readonly send: readonly BridgeNatsOperation[];
  readonly receive: readonly BridgeNatsOperation[];
};

/** What a principal seat may receive across, at most (AGENTS: a principal's org instance gets followUp only). */
export const BRIDGE_NATS_SEAT_RECEIVE: readonly BridgeNatsOperation[] = Object.freeze(["followUp", "cancel"] as const);

/**
 * Destination class of an allow entry, from the entry alone (kind, principal, side), never from a message:
 * seat = a root with an org.json principal (either side); movedLead = a root on the NATS side;
 * fileParticipant = a root or actor on the file side (Ryzen 1); refused = anything else (a NATS actor).
 */
export type BridgeNatsDestinationClass = "seat" | "movedLead" | "fileParticipant" | "refused";

/**
 * The most a destination class may receive across; enforced at parse and again before every publish.
 * ask includes its answer (the ACK). An ACK is not a destination grant: it crosses only back to the
 * verified requester of a command the ledger claimed (BridgeNatsIdentityGate.verifyAck).
 * stop, setModel and setThinking never cross.
 */
export const BRIDGE_NATS_CLASS_RECEIVE: Readonly<Record<BridgeNatsDestinationClass, readonly BridgeNatsOperation[]>> = Object.freeze({
  seat: BRIDGE_NATS_SEAT_RECEIVE,
  movedLead: Object.freeze(["followUp", "steer", "ask", "cancel"] as const),
  fileParticipant: Object.freeze(["followUp", "steer", "ask", "cancel"] as const),
  refused: Object.freeze([] as const),
});

export const bridgeNatsDestinationClass = (entry: { readonly kind: "root" | "actor"; readonly side: BridgeNatsSide;
  readonly principal?: string }): BridgeNatsDestinationClass =>
  entry.kind === "root" && entry.principal !== undefined ? "seat"
    : entry.side === "file" ? "fileParticipant"
      : entry.kind === "root" ? "movedLead" : "refused";

/** The normalized allow-list the gate uses. Built once at start from the link config; never widened at runtime. */
export interface BridgeNatsAllowListConfig {
  readonly version: 1;
  /** The MeshBridge link name: the `remoteHost` of NATS identities mirrored into the file mesh. */
  readonly link: string;
  /** The bridge's own credential-bound NATS machine (e.g. "ryzen1-bridge"); file identities are mirrored to KV under it. */
  readonly natsMachine: string;
  readonly entries: readonly BridgeNatsAllowEntry[];
}

/** Topic route subject (N3) or a control operation. */
export type BridgeNatsSubject =
  | { readonly type: "control"; readonly operation: BridgeNatsOperation }
  | { readonly type: "topic"; readonly topic: string; readonly kind: string };

export interface BridgeNatsAllowRequest {
  readonly direction: BridgeNatsDirection;
  /** Control: a verified identity from the source side. Topic: may be a native file publisher. */
  readonly sender: BridgeNatsVerifiedIdentity | BridgeNatsNativePublisher;
  /** Verified from the destination side's presence (a mirror the link holds there is not a target). */
  readonly target: BridgeNatsVerifiedIdentity;
  readonly subject: BridgeNatsSubject;
  /** The command's principal as carried in its envelope (never from data/text). */
  readonly principal?: FabricPrincipal | undefined;
}

export type BridgeNatsAllowDecision =
  | {
    readonly ok: true;
    readonly sender: BridgeNatsAllowRequest["sender"];
    readonly target: BridgeNatsVerifiedIdentity;
    /** carry: the principal crosses unchanged; strip: it is removed (a downgrade, never an escalation); none: there was none. */
    readonly principal: "carry" | "strip" | "none";
  }
  | { readonly ok: false; readonly refuse: BridgeNatsRefusalCode; readonly reason: string };

/** Stable refusal codes; L2 maps them to a notRun rejection ACK or a dropped event (and a log line). */
export const BRIDGE_NATS_REFUSAL = Object.freeze({
  /** The id is not on the allow-list. */
  notAllowed: "BRIDGE_NATS_NOT_ALLOWED",
  /** On the list, but this operation is not in its send/receive set. */
  operationNotAllowed: "BRIDGE_NATS_OPERATION_NOT_ALLOWED",
  /** The operation is outside the target's destination class (BRIDGE_NATS_CLASS_RECEIVE), or the class is refused. */
  destinationClassRefused: "BRIDGE_NATS_DESTINATION_CLASS_REFUSED",
  /** On the list, but this topic is not in its receiveTopics. */
  topicNotAllowed: "BRIDGE_NATS_TOPIC_NOT_ALLOWED",
  /** The identity is native to the other side for this direction (a loop or a forged origin). */
  sideMismatch: "BRIDGE_NATS_SIDE_MISMATCH",
  /** No live presence for the id on the source side, or (NATS) the presence machine is not the delivery's. */
  senderUnverified: "BRIDGE_NATS_SENDER_UNVERIFIED",
  /** No live presence for the target on the destination side, or it is not where the list pins it. */
  targetUnavailable: "BRIDGE_NATS_TARGET_UNAVAILABLE",
  /** The id has records on two machines, or on both sides: not routable (fails safe). */
  ambiguous: "BRIDGE_NATS_AMBIGUOUS",
  /** The event already crossed a bridge (`data.bridge`): never forwarded again. */
  bridgedAgain: "BRIDGE_NATS_BRIDGED_AGAIN",
  /** Same mapped commandId, different body digest. */
  idConflict: "BRIDGE_NATS_ID_CONFLICT",
  /** Same mapped commandId and digest from a different source machine or replyTo. */
  claimMismatch: "BRIDGE_NATS_CLAIM_MISMATCH",
  /** An ACK with no ledger claim, or not from the claimed owner / machine / digest. */
  ackUnbound: "BRIDGE_NATS_ACK_UNBOUND",
} as const);
export type BridgeNatsRefusalCode = typeof BRIDGE_NATS_REFUSAL[keyof typeof BRIDGE_NATS_REFUSAL];

export type BridgeNatsVerified =
  | { readonly ok: true; readonly identity: BridgeNatsVerifiedIdentity }
  | { readonly ok: false; readonly refuse: BridgeNatsRefusalCode; readonly reason: string };

export type BridgeNatsAckCheck =
  | { readonly ok: true }
  | { readonly ok: false; readonly refuse: BridgeNatsRefusalCode; readonly reason: string };

/** The origin of an ACK, as the receiving side observed it (L2 passes only native, unbridged file ACKs). */
export interface BridgeNatsAckSource {
  /** The side the ACK came from: the command's destination side. */
  readonly side: BridgeNatsSide;
  /** The ACK's `from.id`, checked against the claim's ownerIdentityId. */
  readonly fromId: string;
  readonly targetId: string;
  /** The (mapped) commandId in the ACK body. */
  readonly commandId: string;
  /** NATS ACKs: credential-bound source machine of the delivery. */
  readonly sourceMachine?: string;
  /** NATS ACKs: `body.bodyDigest`. */
  readonly bodyDigest?: string;
  /** NATS ACKs: the envelope id, which must be `ack:<claim.envelopeId>`. File ACKs: the event id. */
  readonly eventId: string;
}

/** The identity/allow-list gate (bridge-nats-identity.ts implements it). */
export interface BridgeNatsIdentityGate {
  readonly config: BridgeNatsAllowListConfig;
  /**
   * Verify a sender from the presence of the side it came from. `sourceMachine` (the delivery's
   * credential-bound machine) is required for side "nats". `other` is the other side's presence,
   * used to refuse an id that also answers there.
   */
  verifySender(side: BridgeNatsSide, claimedId: string, presence: BridgeNatsPresenceView,
    other: BridgeNatsPresenceView, sourceMachine?: string): BridgeNatsVerified;
  /** Verify a target on the destination side's presence. */
  verifyTarget(side: BridgeNatsSide, targetId: string, presence: BridgeNatsPresenceView,
    other: BridgeNatsPresenceView): BridgeNatsVerified;
  allow(request: BridgeNatsAllowRequest): BridgeNatsAllowDecision;
  /** An ACK crosses back only for a claimed command, from its claimed owner (and machine, digest and id on NATS). */
  verifyAck(claim: BridgeNatsCommandRecord, ack: BridgeNatsAckSource): BridgeNatsAckCheck;
}

// ---------------------------------------------------------------------------------------------
// Deterministic ids and per-hop command id mapping
// ---------------------------------------------------------------------------------------------

/** Unambiguous: a JSON array, so no field content can shift a boundary between fields. */
const digest = (...parts: readonly string[]): string =>
  createHash("sha256").update(JSON.stringify(parts), "utf8").digest("hex");

/**
 * The commandId a command carries on the far side of the hop. Distinct verified senders can never
 * collide, and a resend after a crash maps to the same id. cancelCommandId maps the same way (the
 * cancel comes from the same verified sender as the command it cancels). 64 lowercase hex.
 */
export const bridgeNatsMappedCommandId = (link: string, direction: BridgeNatsDirection, verifiedSenderId: string,
  originalCommandId: string): string => digest("pi-fabric/bridge-nats/command/v1", link, direction, verifiedSenderId, originalCommandId);

/** Prefix of NATS envelope ids the bridge mints for file -> NATS events. */
export const BRIDGE_NATS_ENVELOPE_PREFIX = "bnf1-" as const;

/**
 * NATS envelope id (and Nats-Msg-Id) for a file event crossing to NATS. Deterministic from the link
 * and the file event id, so a resend after a crash or an uncertain PubAck reuses it. Never starts with
 * `ack:` or `relay:` and contains no whitespace/control characters (control-plane isNatsControlEventId).
 */
export const bridgeNatsEnvelopeId = (link: string, fileEventId: string): string =>
  `${BRIDGE_NATS_ENVELOPE_PREFIX}${digest("bridge-nats/envelope/v1", link, fileEventId).slice(0, 48)}`;

/** NATS ACK envelope id answering a NATS command envelope: what a native NATS owner would send. */
export const bridgeNatsAckEnvelopeId = (natsCommandEnvelopeId: string): string => `ack:${natsCommandEnvelopeId}`;

/**
 * The link's own identity, for commands it originates itself (N3 topic -> followUp) and refusals it
 * writes. Same shape as the file bridge's refusal ACK sender (`bridge:<name>`, "mesh-bridge", main).
 * L1 keeps it live in KV presence under the bridge machine so NATS receivers can verify it; it is
 * also the replyTo/hostId of those commands. It is never on the allow-list and never a target.
 */
export const bridgeNatsLinkIdentity = (link: string): MeshIdentity => ({ id: `bridge:${link}`, name: "mesh-bridge", kind: "main" });

/** `data.bridge` stamp on an event the bridge writes to the file mesh (MeshBridge convention, no `via`). */
export interface BridgeNatsStamp { readonly from: string; readonly id: string }

/**
 * control-plane.ts commandId derivation (both carriers):
 * sha256(JSON.stringify([hostId, ownerHostId, ownerIdentityId, targetId, operation, idempotencyKey, notRunAttempt])).
 * Used only for a command the link originates itself (N3), with hostId = bridgeNatsLinkIdentity(link).id.
 */
export const bridgeNatsCommandId = (input: {
  readonly hostId: string; readonly ownerHostId: string; readonly ownerIdentityId: string; readonly targetId: string;
  readonly operation: BridgeNatsOperation; readonly idempotencyKey: string; readonly notRunAttempt?: number;
}): string => createHash("sha256").update(JSON.stringify([input.hostId, input.ownerHostId, input.ownerIdentityId,
  input.targetId, input.operation, input.idempotencyKey, input.notRunAttempt ?? 0])).digest("hex");

/** N3: idempotency key of the followUp that carries one file topic event to a NATS root (link-scoped sha256 of the event id). */
export const bridgeNatsTopicIdempotencyKey = (link: string, fileEventId: string): string =>
  digest("bridge-nats/topic/v1", link, fileEventId);

// ---------------------------------------------------------------------------------------------
// L1 -> L2: the raw NATS port (L1 provides it; L2's NatsCarrierSide consumes it)
// ---------------------------------------------------------------------------------------------

/** Bounded diagnostic labels only; never raw lease/owner/machine values or authority. */
export type BridgeNatsPresenceFailure = "no-route" | "not-ready" | "machine-mismatch" | "identity-mismatch" |
  "incarnation-mismatch" | "binding-mismatch" | "root-recheck" | "terminal" | "expired" | "not-live";

/** One KV presence record resolved for an id: the route plus the identity from the KV record itself. */
export interface BridgeNatsPortResolved {
  /** Classification only: a matching, nonterminal record/lease whose only failed check is expiry. */
  readonly leaseLapsed?: boolean;
  readonly presenceFailure?: BridgeNatsPresenceFailure;
  readonly remainingAuthorityMs?: number;
  /** ParticipantRoute semantics (participant-view.ts): machine from the KV key, presence + lease liveness. */
  readonly route: ParticipantRoute;
  /** From the KV presence record (id, name, kind, sessionId), never from an envelope. */
  readonly identity: MeshIdentity;
}

/** File roots and allow-listed file actors to publish into KV presence (with leases) under the bridge machine. */
export interface BridgeNatsMirrorSet {
  readonly hosts: BridgePresence["hosts"];
  readonly participants: BridgePresence["participants"];
  /** The link's own identity (bridgeNatsLinkIdentity), kept live like a root. */
  readonly linkIdentity: MeshIdentity;
}

/** One inbound delivery from the bridge machine's inbox (`fabric.in.<bridgeMachine>.>`, every hostId). */
export interface BridgeNatsPortDelivery {
  /** The decoded wire envelope; body resolved. */
  readonly envelope: TransportEnvelope & { readonly createdAt: number };
  /** Credential-bound source machine (last subject token), never a body field. */
  readonly sourceMachine: string;
  readonly streamSequence: number;
  /** JetStream num_delivered. */
  readonly attempt: number;
  /** Broker publish timestamp in ms, immutable across redelivery; never envelope.createdAt.
   * Older adapters may omit it; an unknown redelivery then expires only if it is currently lapsed. */
  readonly publishedAt?: number;
  /** Final ack. L2 calls it only after MeshBridge read past this delivery (durable file append) or a ledger replay. */
  ack(): Promise<void>;
  /** Never redeliver (malformed, refused). */
  term(reason: string): Promise<void>;
  nak(delayMs?: number): void;
  /** Extend the ack deadline. */
  working(): void;
}

export interface BridgeNatsPort {
  /** The bridge's credential-bound NATS machine (== allow-list natsMachine). */
  readonly machine: string;
  /** Broker-confirmed stream sequence through which this consumer has settled all deliveries.
   * Optional on older adapters. Failure/absence never authorizes terminal-history reclamation. */
  ackFloor?(): Promise<number>;
  /** Current KV presence: NATS roots and actors, in MeshBridge's shape (records under the bridge machine excluded). */
  presence(): Promise<BridgePresence>;
  /** Presence change hints; failure is fatal for the link. Returns the unsubscribe. */
  watchPresence(changed: () => void, failed: (error: Error) => void): () => void;
  /** ParticipantView semantics: the one record answering to the id, or undefined when absent or on two machines. */
  resolve(id: string): BridgeNatsPortResolved | undefined;
  /** Every KV record for the id on every machine, live or not, for bridge-nats-identity.ts. */
  presenceView(): BridgeNatsPresenceView;
  /** Replace the bridge machine's KV mirror (CAS writes, leases renewed under 15 s by the port). */
  mirror(set: BridgeNatsMirrorSet, observedAt: number): Promise<void>;
  /** Fence future mirror writes; withdraw() also deletes the bridge machine's mirror records. */
  fence(): Promise<void>;
  withdraw(): Promise<void>;
  /** TransportSent / TransportUncertainError semantics (src/transport/types.ts). */
  send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>>;
  /** At-least-once, in stream order. One iterator per process. */
  inbox(signal: AbortSignal): AsyncIterable<BridgeNatsPortDelivery>;
  close(): Promise<void>;
}

/**
 * L2's NATS BridgeSide (MeshBridge `remote`), over a BridgeNatsPort: the ONE L1 <-> L2 boundary
 * (integrator note 3). NatsCarrierSide owns envelope mapping, same-bytes resend, bodyDigest, and
 * acking a port delivery only after MeshBridge read past it (durable file append).
 */
export interface NatsCarrierSideContract extends BridgeSide {
  readonly side: "nats";
  readonly port: BridgeNatsPort;
}

/** Inbound NATS delivery as the ordered MeshEvent NatsCarrierSide gives MeshBridge (L2 builds it). */
export interface BridgeNatsInboundEvent extends MeshEvent {
  readonly nats: {
    readonly sourceMachine: string;
    readonly envelopeId: string;
    readonly streamSequence: number;
    readonly attempt: number;
    readonly kind: "command" | "ack";
    /** controlBodyFingerprint(wire.body, wire.principal) over the raw admitted wire, before hydration. */
    readonly bodyDigest?: string;
  };
}

// ---------------------------------------------------------------------------------------------
// Ledger records (bridge-nats-ledger.ts persists them)
// ---------------------------------------------------------------------------------------------

/**
 * The cross-hop mapping of one command, stored both ways: mapped -> original (to answer the source)
 * and (direction, requester, original) -> mapped (dedupe and cancel). Transport ids differ per side.
 */
export interface BridgeNatsCommandMapping {
  readonly direction: BridgeNatsDirection;
  /** The commandId the source sent. The source-side ACK carries it back. */
  readonly originalCommandId: string;
  /** bridgeNatsMappedCommandId(link, direction, requesterId, originalCommandId): what the destination sees. */
  readonly commandId: string;
  /** Source transport id: the file event id (toNats) or the NATS command envelope id (toFile). */
  readonly sourceEventId: string;
  /**
   * Destination transport id. toNats: bridgeNatsEnvelopeId(link, sourceEventId), required at claim.
   * toFile: absent at claim; the file event id, recorded by markSent once the append is durable.
   */
  readonly envelopeId?: string;
  /** On `cancel`: the original id it cancels, and its mapping (computed with the same sender). */
  readonly originalCancelCommandId?: string;
  readonly cancelCommandId?: string;
}

/**
 * claimed: durably claimed, cross publish not yet confirmed.
 * uncertain: a publish attempt's outcome is unknown; resend with the same id and resolvedBody.
 * sent: the destination confirmed the durable append (PubAck / file append receipt).
 * acked: the destination owner's ACK is recorded (ack set).
 * ackForwarded: that ACK was durably appended back on the source side; the source delivery may be acked.
 */
export type BridgeNatsCommandState = "claimed" | "uncertain" | "sent" | "acked" | "ackForwarded";

export interface BridgeNatsClaimInput extends BridgeNatsCommandMapping {
  /** control: a sender's command carried across; topic: a followUp the link originates for an N3 topic event (followUp only). */
  readonly origin: "control" | "topic";
  /** controlBodyFingerprint of the command body + principal, as the destination will compute it. */
  readonly bodyDigest: string;
  /** NATS machine the command came from (toFile) or the bridge machine (toNats). */
  readonly sourceMachine: string;
  /** Verified sender identity id (the link identity for N3 commands). */
  readonly requesterId: string;
  /** The crossing `from`: bridgeNatsMeshIdentity of the verified sender (or the link identity). A resend reuses it. */
  readonly from: MeshIdentity;
  /** The principal as published (after carry/strip), so a resend is the same bytes; absent when none crosses. */
  readonly principal?: FabricPrincipal;
  /** The source's replyTo, where the mapped-back ACK goes. */
  readonly replyTo: string;
  readonly targetId: string;
  readonly operation: BridgeNatsOperation;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation?: string;
  /** toNats: the target owner's NATS machine ("ownerMachine"; required toward NATS; the owner's ACK must come from it). */
  readonly targetMachine?: string;
  readonly deadlineAt: number;
  /** The exact body to (re)send: required toward NATS (the envelope body); toward the file mesh the published data, when kept. */
  readonly resolvedBody?: Record<string, unknown>;
}

export interface BridgeNatsCommandRecord extends BridgeNatsClaimInput {
  readonly v: 1;
  readonly state: BridgeNatsCommandState;
  readonly claimedAt: number;
  /** max(deadlineAt, claimedAt) + BRIDGE_NATS_HORIZON_MS; compaction drops the record after it. */
  readonly expiresAt: number;
  readonly destinationSequence?: number;
  readonly ack?: BridgeNatsAckRecord;
}

/** The owner's ACK as recorded once; a later different ACK for the same commandId never replaces it. */
export interface BridgeNatsAckRecord {
  readonly v: 1;
  /** The mapped commandId the owner answered. */
  readonly commandId: string;
  readonly accepted: boolean;
  /** Verified owner identity id the ACK came from. */
  readonly fromId: string;
  /** Source ACK id: NATS `ack:<envelopeId>` or the file ACK event id. */
  readonly ackEventId: string;
  /** The translated ACK data the link republishes to the source (original commandId; bounded). */
  readonly data: Record<string, unknown>;
  readonly receivedAt: number;
}

/** `created` is true only for `new`: the one caller that may cross publish. */
export type BridgeNatsClaimResult =
  /** First claim: cross publish it. */
  | { readonly status: "new"; readonly created: true; readonly record: BridgeNatsCommandRecord }
  /** Same mapped id, digest, source machine and replyTo: never publish again; replay the recorded state/ACK. */
  | { readonly status: "duplicate"; readonly created: false; readonly record: BridgeNatsCommandRecord }
  /** Same mapped id, different digest: refuse (BRIDGE_NATS_ID_CONFLICT), never mutate the record. */
  | { readonly status: "conflict"; readonly created: false; readonly record: BridgeNatsCommandRecord }
  /** Same mapped id and digest, different source machine / replyTo / target: refuse (BRIDGE_NATS_CLAIM_MISMATCH). */
  | { readonly status: "mismatch"; readonly created: false; readonly record: BridgeNatsCommandRecord };

/**
 * What the link does with a claim result, at once and without waiting (bridgeNatsClaimAction):
 * publish: first claim, cross publish now. resend: published or not, the outcome is unconfirmed: send the
 * same envelope id and recorded bytes again. awaitAck: the destination confirmed the append; nothing to
 * send (acknowledge the source delivery), the owner's ACK arrives on its own. replayAck: the owner's ACK
 * is recorded: send it back to the source again now (idempotent there). refuse: answer the verified
 * source with a notRun refusal carrying the code; never publish.
 */
export type BridgeNatsClaimAction =
  | { readonly action: "publish" | "resend" | "awaitAck"; readonly record: BridgeNatsCommandRecord }
  | { readonly action: "replayAck"; readonly record: BridgeNatsCommandRecord; readonly ack: BridgeNatsAckRecord }
  | { readonly action: "refuse"; readonly record: BridgeNatsCommandRecord;
    readonly refuse: typeof BRIDGE_NATS_REFUSAL.idConflict | typeof BRIDGE_NATS_REFUSAL.claimMismatch };

export interface BridgeNatsSeenRecord {
  readonly v: 1;
  /** Source event/envelope id. */
  readonly eventId: string;
  readonly direction: BridgeNatsDirection;
  /** The destination's durable id/sequence, when known. */
  readonly destinationId?: string;
  readonly destinationSequence?: number;
  readonly seenAt: number;
  readonly expiresAt: number;
}

/**
 * The durable ACK/dedupe ledger (C6). Every mutating call resolves only after its record is durable
 * (fsync'd). A restart reloads it; exactly-once by mapped commandId holds across the restart.
 */
export interface BridgeNatsLedger {
  claim(input: BridgeNatsClaimInput): Promise<BridgeNatsClaimResult>;
  /** By direction and mapped commandId (what an owner's ACK carries); undefined for the other direction's record. */
  lookup(direction: BridgeNatsDirection, commandId: string): BridgeNatsCommandRecord | undefined;
  /** By the source's view: direction, verified requester and original commandId. */
  lookupOriginal(direction: BridgeNatsDirection, requesterId: string, originalCommandId: string): BridgeNatsCommandRecord | undefined;
  /** An attempt's outcome is unknown (TransportUncertainError, crash before receipt). */
  markUncertain(commandId: string): Promise<BridgeNatsCommandRecord>;
  /** The destination confirmed the durable append. */
  markSent(commandId: string, receipt: { readonly envelopeId?: string; readonly sequence?: number }): Promise<BridgeNatsCommandRecord>;
  /** Record the owner's ACK once. A replayed identical ACK is a no-op; a different one keeps the first. */
  settle(commandId: string, ack: BridgeNatsAckRecord): Promise<BridgeNatsCommandRecord>;
  /** The ACK was durably appended back on the source side. */
  markAckForwarded(commandId: string): Promise<BridgeNatsCommandRecord>;
  /** Claims whose cross publish is not confirmed (claimed or uncertain): resend with the same id and bytes. */
  pendingResends(): readonly BridgeNatsCommandRecord[];
  /** Settled ACKs not yet forwarded back: forward them (same ACK, same id). */
  pendingAcks(): readonly BridgeNatsCommandRecord[];
  /** Event-level dedupe for non-command traffic and inbound deliveries already appended. */
  seen(direction: BridgeNatsDirection, eventId: string): BridgeNatsSeenRecord | undefined;
  markSeen(direction: BridgeNatsDirection, eventId: string,
    receipt?: { readonly destinationId?: string; readonly destinationSequence?: number }): Promise<BridgeNatsSeenRecord>;
  close(): Promise<void>;
}
