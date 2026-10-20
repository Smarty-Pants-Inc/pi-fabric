// src/transport/types.ts — Fabric transport port v1. Upstream-shaped: no NATS imports.
import type { MeshIdentity } from "../mesh/event-log.js";
import type { FabricMessageSender } from "../protocol.js";
import type { FabricPrincipal } from "../fabric-provenance.js";
import type { PlacementTerminalRecord } from "../placement/terminal.js";

/** The Fabric runtime that consumes an inbox: FabricControlPlane options.hostId (stable across reloads). */
export interface TransportOwner {
  /** Credential-bound machine id, /^[a-z0-9][a-z0-9-]{0,31}$/. Never read from JSON (§2). */
  readonly machine: string;
  /** Owner runtime id: Main `session:<uuid>`, task child `runtime:<sessionId>`, resident host id. */
  readonly hostId: string;
}

export type TransportEnvelopeKind = "command" | "ack";

export interface TransportEnvelope<T = unknown> {
  /** Stable logical id; the broker dedupe key (Nats-Msg-Id). §3.2 gives the value per kind. */
  readonly id: string;
  readonly kind: TransportEnvelopeKind;
  readonly to: TransportOwner;
  /** The command's targetId. The lane key (§3.4) and a subject token; never authority. */
  readonly targetId: string;
  /** FabricControlOperation for commands; "accepted" | "rejected" for ACKs. */
  readonly operation: string;
  readonly from: MeshIdentity;
  readonly principal?: FabricPrincipal | undefined;
  readonly sender?: FabricMessageSender;
  /** Body, or a stamp. A stamp runs exactly once, synchronously, with no await between it and the
   *  publish call (NATS) or under the mesh .lock (file). A throw refuses the send; nothing is persisted. */
  readonly body: T | ((committedAt: number) => T);
}

export interface TransportSent<T = unknown> {
  readonly id: string;
  /** JetStream stream seq or mesh event sequence. Orders messages only within one destination inbox. */
  readonly sequence: number;
  /** The carrier already held this id inside its dedupe window. `envelope.body` is THIS attempt's stamp. */
  readonly duplicate: boolean;
  readonly envelope: TransportEnvelope<T> & { readonly body: T; readonly createdAt: number };
}

export interface TransportDelivery<T> {
  readonly payload: T;
  /** Authenticated origin: the credential-bound last subject token (NATS) or the local mesh (file). */
  readonly sourceMachine: string;
  /** "mesh" = same mesh; "bridge" = admitted by a remote carrier (bridge or NATS). */
  readonly verification: "mesh" | "bridge";
  /** 1 on first delivery (JetStream num_delivered). Always 1 on the file adapter. */
  readonly attempt: number;
  readonly sequence: number;
  /** Extend the ack deadline (+WPI). The caller repeats it every ackWait/3 (DV-1b). */
  working(): void;
  /** Final ack, confirmed by the broker (double ack). Deliveries may be acked in any order. */
  ack(): Promise<void>;
  /** Redeliver after delayMs. Counts toward max_deliver. */
  nak(delayMs?: number): void;
  /** Never redeliver (malformed, oversize, unauthorized). The reason is logged. */
  term(reason: string): Promise<void>;
}

export interface TransportKvEntry<V = unknown> {
  /** Full key `<machine>.<token>.<token>`; the first token is the writer's machine. */
  readonly key: string;
  readonly machine: string;
  /** `<hostId>#<incarnation>` of the writer. A fencing input, not authority. */
  readonly owner: string;
  readonly value: V;
  /** Bucket-wide stream sequence: the CAS token (DV-5d). */
  readonly revision: number;
}

export type TransportKvEvent<V = unknown> =
  | { readonly type: "put"; readonly entry: TransportKvEntry<V> }
  | { readonly type: "gone"; readonly key: string; readonly machine: string; readonly revision: number; readonly reason: "deleted" | "expired" }
  | { readonly type: "ready" }
  | { readonly type: "reset" };

export interface TransportKv {
  /** Fixed bucket policy (§4.1): an entry expires ttlMs after its LAST successful write. */
  readonly ttlMs: number;
  get<V>(key: string, signal?: AbortSignal): Promise<TransportKvEntry<V> | undefined>;
  /** CAS write; ifRevision 0 = create only if absent. Resolves the new revision; KvConflictError on mismatch. */
  put<V>(key: string, owner: string, value: V, ifRevision: number, signal?: AbortSignal): Promise<number>;
  /** CAS delete; false when the revision moved. */
  delete(key: string, ifRevision: number, signal?: AbortSignal): Promise<boolean>;
  /** Snapshot, then exactly one `ready`, then changes; after a gap `reset`, a snapshot, `ready` (draft 1 §4.4). */
  watch<V>(prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>>;
}

export type TransportBucket = "presence" | "leases";

export interface TransportDrainState {
  /** Messages stored for self and not yet delivered, plus delivered and not acked. */
  readonly pending: number;
  readonly ackPending: number;
}

export interface FabricTransport {
  readonly kind: "file" | "nats";
  readonly self: TransportOwner;
  /** Persist one envelope into envelope.to's inbox. Resolves on persistence only (§1.1). */
  send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>>;
  /** This runtime's durable inbox. At most one iterator per process. Across processes the host lease (§4)
   *  is the exclusivity rule. At-least-once, delivered in stream order; acks may be out of order. */
  inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>>;
  /** NATS only; the file adapter throws TransportUnsupportedError. */
  kv(bucket: TransportBucket): TransportKv;
  /** Best-effort Placement terminal receipt; only implemented by carriers with their agreed store. */
  writePlacementTerminal?(record: PlacementTerminalRecord): Promise<void>;
  /** For drain and rollback (§8.3). File: computed from the control-plane cursor. */
  drainState(signal?: AbortSignal): Promise<TransportDrainState>;
  /** Ends iterators (in-hand deliveries are neither acked nor nacked), settles publishes, closes. */
  close(): Promise<void>;
}

export interface FabricTransportOptions {
  readonly config: import("../config.js").FabricMeshConfig;
  readonly hostId: string;
  readonly mesh: import("../mesh/store.js").MeshStore;
  readonly signal?: AbortSignal;
}
export type FabricTransportFactory = (options: FabricTransportOptions) => Promise<FabricTransport>;

/** Typed errors; callers branch on `code`. `persisted = false` means definitely not stored. */
export class TransportUnavailableError extends Error { readonly code = "FABRIC_TRANSPORT_UNAVAILABLE"; readonly persisted = false; }
export class TransportUncertainError extends Error {
  readonly code = "FABRIC_TRANSPORT_UNCERTAIN";
  /** May be stored. Retry ONLY with the same id and the same resolved bytes. */
  constructor(readonly id: string, readonly resolvedBody: unknown, options?: ErrorOptions) { super(`transport outcome uncertain for ${id}`, options); }
}
export class TransportCapacityError extends Error { readonly code = "FABRIC_TRANSPORT_CAPACITY"; readonly persisted = false; }
export class TransportDeniedError extends Error { readonly code = "FABRIC_TRANSPORT_DENIED"; readonly persisted = false; }
export class TransportInvalidError extends Error { readonly code = "FABRIC_TRANSPORT_INVALID"; readonly persisted = false; }
export class TransportUnsupportedError extends Error { readonly code = "FABRIC_TRANSPORT_UNSUPPORTED"; }
export class KvConflictError extends Error {
  readonly code = "FABRIC_KV_CONFLICT";
  constructor(readonly key: string, readonly expected: number) { super(`KV revision conflict on ${key}`); }
}
export class DirectoryNotReadyError extends Error { readonly code = "FABRIC_DIRECTORY_NOT_READY"; }
