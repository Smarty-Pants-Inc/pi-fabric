// A' bridge, NATS side, presence both ways (smarty-dev#15310, lane L1).
//
// The link runs on the hub (Ryzen 1) as ONE NATS machine. Its credential writes KV presence and
// lease rows only under its own machine prefix and reads every machine's rows.
//
// - NATS -> file: `NatsPresenceReader` watches every machine's presence and lease rows.
//   `NatsBridgePresence.presence()` turns the live, unique, admitted NATS roots into the
//   `BridgeHost` + root-participant snapshot that `MeshBridge` mirrors into the file mesh.
// - file -> NATS: `NatsKvMirror` writes admitted file roots and actors as KV presence rows plus
//   owner-host lease rows under the bridge machine. A NATS receiver then verifies a bridged
//   sender exactly as it verifies a native one: live presence on the credential-bound source
//   machine (control-plane `#natsSenderVerified`).
//
// Identity rules (Tier 1):
// - The authenticated machine of a NATS row is its KV key prefix, never a JSON field.
// - Every record is re-validated with the receivers' own validators (presenceValue/leaseValue).
// - One id on two machines is ambiguous: never mirrored into the file mesh, and never created
//   in KV by this link while another machine has a row for it.
// - KV authority is renewed only until the file sample's own authority deadline; a stalled hub
//   read stops renewal and the lease lapses (15 s TTL).
// - Only this link's own rows are ever rewritten: an existing row is adopted only when it carries
//   the same owner, the same identity and incarnation, and (for a lease) this process's proof or
//   a provably exited predecessor's.
import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import { natsRenewalFailure, noteNatsRenewalFailures, type NatsRenewalFailure } from "./bridge-nats-diagnostics.js";
import type { BridgeChange } from "./change-notifier.js";
import type { BridgeHost, BridgePresence } from "./bridge.js";
import { ROOT_ID_PREFIX } from "../topology/root-inbox-shared.js";
import { leaseValue, presenceValue } from "../topology/participant-view.js";
import {
  MIRROR_FRESH_MS,
  readLocalHostLeaseProcessProof,
  transportHostLeasePredecessorExited,
  TRANSPORT_HOST_LEASE_RENEW_MS,
  TRANSPORT_HOST_LEASE_SAFETY_MS,
  type TransportHostLeaseProcessProof,
} from "../topology/host-leases.js";
import type { FabricKvHostLease, FabricParticipantCapability, FabricParticipantKind, FabricParticipantRecord } from "../topology/types.js";
import { KvConflictError, type FabricTransport, type TransportKv, type TransportKvEntry } from "../transport/types.js";

/** Same refresh cadence as native KV presence (participant-view KV_PRESENCE_REFRESH_MS). */
export const NATS_MIRROR_PRESENCE_REFRESH_MS = 180_000;
export const NATS_MIRROR_RENEW_MS = TRANSPORT_HOST_LEASE_RENEW_MS;
const KV_PRESENCE_TTL_MS = 600_000;
const KV_LEASE_TTL_MS = 15_000;
const MACHINE_TOKEN = /^[a-z0-9][a-z0-9-]{0,31}$/;
const KEY_TOKEN = /^[a-f0-9]{32}$/;
const CANONICAL_ROOT = new RegExp(`^${ROOT_ID_PREFIX}[A-Za-z0-9-]{8,128}$`);
// Same set as participant-view KV_TERMINAL: a terminal status never routes.
const KV_TERMINAL = new Set(["completed", "failed", "cancelled", "stopped", "timed_out", "done", "aborted"]);
const KV_CAPABILITIES = new Set<FabricParticipantCapability>(["steer", "followUp", "stop", "ask", "actor-bindings", "attach", "fabric"]);
const MAX_SUPERSEDED = 16;
const MAX_REPORTED = 1_000;

export type NatsPresenceDirection = "toFile" | "toNats";
type Bucket = "presence" | "leases";

export const natsKvToken = (id: string): string => createHash("sha256").update(id).digest("hex").slice(0, 32);
export const natsPresenceKey = (machine: string, id: string): string => `${machine}.p.${natsKvToken(id)}`;
export const natsLeaseKey = (machine: string, hostId: string): string => `${machine}.h.${natsKvToken(hostId)}`;
const isCanonicalRoot = (id: string): boolean => CANONICAL_ROOT.test(id);

/** The authenticated machine: the key's first token, never a JSON field. */
const machineOfKey = (key: string, kind: "p" | "h"): string | undefined => {
  const parts = key.split(".");
  return parts.length === 3 && MACHINE_TOKEN.test(parts[0]!) && parts[1] === kind && KEY_TOKEN.test(parts[2]!) ? parts[0] : undefined;
};

const errorCode = (error: unknown): unknown =>
  typeof error === "object" && error !== null ? (error as { code?: unknown }).code : undefined;
const isConflict = (error: unknown): boolean => errorCode(error) === "FABRIC_KV_CONFLICT";
const isUncertain = (error: unknown): boolean => errorCode(error) === "FABRIC_TRANSPORT_UNCERTAIN";
const isUnavailable = (error: unknown): boolean => errorCode(error) === "FABRIC_TRANSPORT_UNAVAILABLE";
/** Refusals a retry cannot fix: the credential lost its grant, or the bucket is not the expected one. */
const isTerminalWatchError = (error: unknown): boolean => {
  for (let depth = 0, cause = error; depth < 6 && typeof cause === "object" && cause !== null; depth++) {
    const code = errorCode(cause);
    const name = (cause as { name?: unknown }).name;
    if (code === "FABRIC_TRANSPORT_DENIED" || code === "FABRIC_TRANSPORT_INVALID" || code === "FABRIC_NATS_AUTHORIZATION" ||
      name === "NatsAuthorizationError" || name === "PermissionViolationError" || name === "AuthorizationError") return true;
    cause = (cause as { cause?: unknown }).cause;
  }
  return false;
};

const canonicalJson = (value: unknown): string => JSON.stringify(value, (_key, child: unknown) => {
  if (typeof child !== "object" || child === null || Array.isArray(child)) return child;
  const record = child as Record<string, unknown>;
  return Object.fromEntries(Object.keys(record).sort().map(key => [key, record[key]]));
}) ?? "";
const freeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) freeze(child);
  }
  return value;
};
const cloneFrozen = <T>(value: T): T => freeze(JSON.parse(JSON.stringify(value)) as T);

/** Stable mirror content: updatedAt alone does not rewrite a row (presence refresh renews it). */
const mirrorContent = (record: FabricParticipantRecord): string => {
  const { updatedAt: _updatedAt, ...stable } = record;
  return canonicalJson(stable);
};

const PROJECTED_STRINGS = ["parentId", "label", "role", "project", "projectRoot", "repository", "cwd", "sessionId", "model", "thinking"] as const;

/**
 * The record this link carries across, in either direction: a fixed field list (no process,
 * pane, placement, usage or service fields), the KV capability vocabulary only, and no mirror
 * provenance (the receiving side adds its own). undefined when it cannot be carried: a
 * non-root/non-actor, a mirrored or terminal-history record, a legacy control protocol, or no
 * owner incarnation (a command to it could not be fenced across the hop).
 */
export const projectNatsMirrorRecord = (record: FabricParticipantRecord): FabricParticipantRecord | undefined => {
  const raw = record as unknown as Record<string, unknown>;
  if (typeof raw !== "object" || raw === null || raw.format !== 1 || (raw.kind !== "root" && raw.kind !== "actor") ||
    raw.remoteHost !== undefined || raw.bridgeVia !== undefined || raw.stopReason !== undefined || raw.controlProtocol !== "v1" ||
    typeof raw.ownerIncarnation !== "string" || raw.ownerIncarnation.length === 0 || raw.ownerIncarnation.length > 128 ||
    !Array.isArray(raw.capabilities)) return undefined;
  const capabilities = (raw.capabilities as unknown[]).filter((capability): capability is FabricParticipantCapability =>
    typeof capability === "string" && KV_CAPABILITIES.has(capability as FabricParticipantCapability));
  const projected: FabricParticipantRecord = {
    format: 1, id: record.id, kind: record.kind, rootId: record.rootId, ownerHostId: record.ownerHostId,
    ownerIdentityId: record.ownerIdentityId, ownerIncarnation: record.ownerIncarnation!, name: record.name,
    status: record.status, ...(record.runner !== undefined ? { runner: record.runner } : {}), transport: record.transport,
    capabilities: [...new Set(capabilities)], startedAt: record.startedAt, updatedAt: record.updatedAt, controlProtocol: "v1",
  };
  for (const key of PROJECTED_STRINGS) if (typeof raw[key] === "string") (projected as unknown as Record<string, unknown>)[key] = raw[key];
  if (typeof raw.interactive === "boolean") projected.interactive = raw.interactive;
  if (raw.residency === "session" || raw.residency === "durable") projected.residency = raw.residency;
  return projected;
};

/** What a NATS receiver resolves for an id (participant-view KvParticipantDirectory#route), plus the lease deadline. */
export interface NatsRoute {
  readonly id: string;
  readonly kind: FabricParticipantKind;
  /** Authenticated KV key machine. */
  readonly machine: string;
  readonly rootId: string;
  readonly ownerHostId: string;
  readonly ownerIdentityId: string;
  readonly ownerIncarnation: string;
  readonly capabilities: readonly string[];
  /** Presence + matching unexpired owner lease, not terminal, and no pending root re-check. */
  readonly live: boolean;
  /**
   * The owner lease matches and is unexpired but carries `rootRecheck: true` (re-created after a KV
   * lapse, host-leases.ts): not live, as native KV routing treats it, yet not gone either. Unknown
   * until the owner rewrites the lease: never withdrawal evidence.
   */
  readonly rootRecheck: boolean;
  /** This process's monotonic clock: the latest the owner lease can still be live. */
  readonly liveUntil: number;
  readonly record: Readonly<FabricParticipantRecord>;
}

interface Seen<V> { readonly entry: TransportKvEntry<V>; readonly receivedAt: number }

export interface NatsPresenceReaderOptions {
  readonly transport: Pick<FabricTransport, "kind" | "self" | "kv">;
  readonly onError?: (error: unknown) => void;
  readonly now?: () => number;
  readonly monotonic?: () => number;
}

export type NatsPresenceListener = (bucket: Bucket, machine: string | undefined) => void;

/**
 * Read-only view of every machine's KV presence and leases. A lease's live window is bounded by
 * BOTH the time this process received its latest write (KV TTL counts from the write, which
 * precedes receipt) and the owner's own expiresAt: a stalled watch therefore loses authority on
 * its own, and a skewed owner clock can only shorten it.
 */
export class NatsPresenceReader {
  readonly machine: string;
  readonly #presence: TransportKv;
  readonly #leases: TransportKv;
  #records = new Map<string, Seen<FabricParticipantRecord>>();
  #hosts = new Map<string, Seen<FabricKvHostLease>>();
  readonly #revisions: Record<Bucket, Map<string, number>> = { presence: new Map(), leases: new Map() };
  /** A watch gap (reconnect) re-snapshots into these; the served view switches only at the new `ready`. */
  readonly #resync: Partial<Record<Bucket, { entries: Map<string, Seen<never>>; revisions: Map<string, number> }>> = {};
  readonly #ready = new Set<Bucket>();
  readonly #listeners = new Set<NatsPresenceListener>();
  readonly #failureListeners = new Set<(error: Error) => void>();
  #failure: Error | undefined;
  readonly #readyWaiters = new Set<() => void>();
  readonly #abort = new AbortController();
  readonly #tasks: Promise<void>[] = [];
  readonly #now: () => number;
  readonly #monotonic: () => number;
  #started = false;
  #closed = false;

  constructor(readonly options: NatsPresenceReaderOptions) {
    const { transport } = options;
    if (transport.kind !== "nats" || !MACHINE_TOKEN.test(transport.self.machine)) {
      throw new TypeError("The NATS bridge presence reader needs a NATS transport with a valid machine");
    }
    this.machine = transport.self.machine;
    this.#presence = transport.kv("presence");
    this.#leases = transport.kv("leases");
    this.#now = options.now ?? Date.now;
    this.#monotonic = options.monotonic ?? (() => performance.now());
  }

  start(): void {
    if (this.#closed) throw new Error("NATS bridge presence reader is closed");
    if (this.#started) return;
    this.#started = true;
    this.#tasks.push(this.#watch("presence"), this.#watch("leases"));
  }

  get ready(): boolean { return this.#started && !this.#closed && this.#ready.size === 2; }

  /** A reconnect gap is open: the served view is the last snapshot, and absence in it proves nothing. */
  get resyncing(): boolean { return this.#resync.presence !== undefined || this.#resync.leases !== undefined; }

  /** Resolves once both snapshots are ready; rejects on close or abort. */
  whenReady(signal?: AbortSignal): Promise<void> {
    if (this.ready) return Promise.resolve();
    if (this.#closed) return Promise.reject(new Error("NATS bridge presence reader is closed"));
    return new Promise<void>((resolve, reject) => {
      const done = (): void => { cleanup(); resolve(); };
      const abort = (): void => { cleanup(); reject(signal?.reason ?? this.#abort.signal.reason ?? new Error("aborted")); };
      const cleanup = (): void => {
        this.#readyWaiters.delete(done);
        signal?.removeEventListener("abort", abort);
        this.#abort.signal.removeEventListener("abort", abort);
      };
      this.#readyWaiters.add(done);
      signal?.addEventListener("abort", abort, { once: true });
      this.#abort.signal.addEventListener("abort", abort, { once: true });
      if (signal?.aborted || this.#abort.signal.aborted) abort();
    });
  }

  /** A terminal watch refusal (denied credential, invalid bucket), reported once per reader. */
  subscribeFailure(listener: (error: Error) => void): () => void {
    if (this.#failure) {
      const failure = this.#failure;
      queueMicrotask(() => { try { listener(failure); } catch (error) { this.#report(error); } });
    }
    this.#failureListeners.add(listener);
    return () => { this.#failureListeners.delete(listener); };
  }

  #fail(error: unknown): void {
    if (this.#failure) return;
    this.#failure = error instanceof Error ? error : new Error("NATS bridge presence watch refused", { cause: error });
    for (const listener of [...this.#failureListeners]) {
      try { listener(this.#failure); } catch (failure) { this.#report(failure); }
    }
  }

  subscribe(listener: NatsPresenceListener): () => void {
    this.#listeners.add(listener);
    return () => { this.#listeners.delete(listener); };
  }

  /** Every machine holding a presence row for this id, live or not (a receiver counts them all). */
  machinesFor(id: string): string[] {
    const machines = new Set<string>();
    for (const seen of this.#records.values()) if (seen.entry.value.id === id) machines.add(machineOfKey(seen.entry.key, "p")!);
    return [...machines].sort();
  }

  /** Every row answering to the id (every row when id is undefined), one per machine, live or not. */
  records(id?: string): NatsRoute[] {
    if (!this.ready) return [];
    return [...this.#records.values()].filter(seen => id === undefined || seen.entry.value.id === id).map(seen => this.#route(seen))
      .sort((a, b) => a.id.localeCompare(b.id) || a.machine.localeCompare(b.machine));
  }

  /** A receiver's route for the id: undefined when absent or ambiguous (rows on two machines). */
  resolve(id: string): NatsRoute | undefined {
    if (!this.ready) return undefined;
    const matches = [...this.#records.values()].filter(seen => seen.entry.value.id === id);
    return matches.length === 1 ? this.#route(matches[0]!) : undefined;
  }

  /** Unique-id routes, live or not; ambiguous ids are reported separately. */
  routes(): { routes: NatsRoute[]; ambiguous: string[] } {
    if (!this.ready) return { routes: [], ambiguous: [] };
    const byId = new Map<string, Seen<FabricParticipantRecord>[]>();
    for (const seen of this.#records.values()) byId.set(seen.entry.value.id, [...(byId.get(seen.entry.value.id) ?? []), seen]);
    const routes: NatsRoute[] = [];
    const ambiguous: string[] = [];
    for (const [id, seen] of byId) {
      if (seen.length === 1) routes.push(this.#route(seen[0]!));
      else ambiguous.push(id);
    }
    return { routes: routes.sort((a, b) => a.id.localeCompare(b.id)), ambiguous: ambiguous.sort() };
  }

  /** The current lease row for a host on a machine, if this view holds a valid one. */
  lease(machine: string, hostId: string): { entry: TransportKvEntry<FabricKvHostLease>; liveUntil: number } | undefined {
    const seen = this.#hosts.get(natsLeaseKey(machine, hostId));
    return seen ? { entry: seen.entry, liveUntil: this.#leaseUntil(seen) } : undefined;
  }

  #leaseUntil(seen: Seen<FabricKvHostLease>): number {
    const lease = seen.entry.value;
    const ttl = Math.min(this.#leases.ttlMs, Math.max(0, lease.expiresAt - lease.updatedAt));
    const byReceipt = seen.receivedAt + ttl;
    const byOwnerClock = this.#monotonic() + (lease.expiresAt - this.#now());
    return Math.min(byReceipt, byOwnerClock) - TRANSPORT_HOST_LEASE_SAFETY_MS;
  }

  #route(seen: Seen<FabricParticipantRecord>): NatsRoute {
    const record = seen.entry.value;
    const machine = machineOfKey(seen.entry.key, "p")!;
    const host = this.#hosts.get(natsLeaseKey(machine, record.ownerHostId));
    const lease = host?.entry.value;
    const liveUntil = host ? this.#leaseUntil(host) : Number.NEGATIVE_INFINITY;
    const leased = !!lease && lease.id === record.ownerHostId && lease.identityId === record.ownerIdentityId &&
      lease.rootId === record.rootId && lease.incarnation === record.ownerIncarnation && host!.entry.owner === seen.entry.owner &&
      !KV_TERMINAL.has(record.status) && liveUntil > this.#monotonic();
    // Same rule as native KV routing (participant-view KvParticipantDirectory#route): a lease pending
    // a root re-check routes nothing, while the record itself is kept.
    const rootRecheck = leased && lease!.rootRecheck === true;
    return {
      id: record.id, kind: record.kind, machine, rootId: record.rootId, ownerHostId: record.ownerHostId,
      ownerIdentityId: record.ownerIdentityId, ownerIncarnation: record.ownerIncarnation!,
      capabilities: [...record.capabilities], live: leased && !rootRecheck, rootRecheck, liveUntil, record,
    };
  }

  #notify(bucket: Bucket, machine?: string): void {
    for (const listener of [...this.#listeners]) {
      try { listener(bucket, machine); } catch (error) { this.#report(error); }
    }
    if (this.ready) for (const waiter of [...this.#readyWaiters]) waiter();
  }

  #report(error: unknown): void { try { this.options.onError?.(error); } catch { /* diagnostic only */ } }

  #clear(bucket: Bucket): void {
    this.#ready.delete(bucket);
    delete this.#resync[bucket];
    this.#revisions[bucket].clear();
    if (bucket === "presence") this.#records.clear(); else this.#hosts.clear();
  }

  async #watch(bucket: Bucket): Promise<void> {
    const kv = bucket === "presence" ? this.#presence : this.#leases;
    const kind = bucket === "presence" ? "p" : "h";
    let attempt = 0;
    while (!this.#closed) {
      try {
        for await (const event of kv.watch<unknown>(">", this.#abort.signal)) {
          if (this.#closed) return;
          if (event.type === "reset") {
            // A reconnect gap: keep serving the last snapshot (each lease still ages out by its own
            // receipt time) while the new one loads, so a short outage never reads as "everyone left".
            if (this.#ready.has(bucket)) this.#resync[bucket] = { entries: new Map(), revisions: new Map() };
            else { this.#clear(bucket); this.#notify(bucket); }
            continue;
          }
          if (event.type === "ready") {
            const resync = this.#resync[bucket];
            if (resync) {
              delete this.#resync[bucket];
              if (bucket === "presence") this.#records = resync.entries as Map<string, Seen<FabricParticipantRecord>>;
              else this.#hosts = resync.entries as Map<string, Seen<FabricKvHostLease>>;
              this.#revisions[bucket] = resync.revisions;
            }
            this.#ready.add(bucket);
            attempt = 0;
            this.#notify(bucket);
            continue;
          }
          const key = event.type === "put" ? event.entry.key : event.key;
          const revision = event.type === "put" ? event.entry.revision : event.revision;
          const machine = machineOfKey(key, kind);
          if (!machine || !Number.isSafeInteger(revision) || revision <= 0) continue;
          const resync = this.#resync[bucket];
          const revisions = resync?.revisions ?? this.#revisions[bucket];
          if (revision <= (revisions.get(key) ?? 0)) continue;
          revisions.set(key, revision);
          const receivedAt = this.#monotonic();
          if (bucket === "presence") {
            const target = (resync?.entries ?? this.#records) as Map<string, Seen<FabricParticipantRecord>>;
            target.delete(key);
            const record = event.type === "put" ? presenceValue(event.entry) : undefined;
            if (record && event.type === "put") target.set(key, { entry: { ...event.entry, value: cloneFrozen(record) }, receivedAt });
          } else {
            const target = (resync?.entries ?? this.#hosts) as Map<string, Seen<FabricKvHostLease>>;
            target.delete(key);
            const lease = event.type === "put" ? leaseValue(event.entry) : undefined;
            if (lease && event.type === "put") target.set(key, { entry: { ...event.entry, value: cloneFrozen(lease) }, receivedAt });
          }
          if (!resync) this.#notify(bucket, machine);
        }
        if (this.#closed) return;
        throw new Error(`NATS bridge ${bucket} watch ended`);
      } catch (error) {
        if (this.#closed) return;
        // No authority without a live watch: the snapshot is gone until a new one is ready.
        this.#clear(bucket);
        this.#notify(bucket);
        if (!isUnavailable(error)) this.#report(error);
        if (isTerminalWatchError(error)) this.#fail(error);
        const delayMs = Math.min(5_000, 100 * 2 ** Math.min(attempt, 6));
        attempt += 1;
        await new Promise<void>(resolve => {
          const finish = (): void => { clearTimeout(timer); this.#abort.signal.removeEventListener("abort", finish); resolve(); };
          const timer = setTimeout(finish, delayMs);
          this.#abort.signal.addEventListener("abort", finish, { once: true });
          if (this.#abort.signal.aborted) finish();
        });
      }
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#abort.abort(new Error("NATS bridge presence reader closed"));
    await Promise.allSettled(this.#tasks);
    this.#ready.clear();
    this.#records.clear();
    this.#hosts.clear();
    this.#listeners.clear();
  }
}

/** A refusal to write one mirrored row; the rest of the pass continues. */
export class NatsMirrorRefusedError extends Error {
  constructor(readonly code: NatsMirrorRefusal, readonly id: string, options?: ErrorOptions) {
    super(`NATS bridge mirror refused ${id}: ${code}`, options);
    this.name = "NatsMirrorRefusedError";
  }
}

export type NatsMirrorRefusal =
  | "FABRIC_BRIDGE_NATS_RECORD_INVALID"
  | "FABRIC_BRIDGE_NATS_DIRECTORY_NOT_READY"
  | "FABRIC_BRIDGE_NATS_ID_ON_ANOTHER_MACHINE"
  | "FABRIC_BRIDGE_NATS_LEASE_CONFLICT"
  | "FABRIC_BRIDGE_NATS_PRESENCE_CONFLICT"
  | "FABRIC_BRIDGE_NATS_STALE_INCARNATION"
  | "FABRIC_BRIDGE_NATS_EXPIRED"
  | "FABRIC_BRIDGE_NATS_DUPLICATE";

export class NatsMirrorFencedError extends Error {
  readonly code = "FABRIC_BRIDGE_NATS_FENCED";
  constructor() { super("NATS bridge mirror is fenced"); this.name = "NatsMirrorFencedError"; }
}

/** One record this link should hold in KV, with the file sample's authority deadline. */
export interface NatsMirrorClaim {
  readonly record: FabricParticipantRecord;
  /** This process's monotonic clock; KV authority is never renewed past it (Infinity: until fenced). */
  readonly until: number;
}

export interface NatsMirrorOutcome {
  readonly held: string[];
  readonly refused: Array<{ id: string; code: string }>;
  readonly withdrawn: string[];
}

interface LeaseSpec { readonly id: string; readonly rootId: string; readonly identityId: string; readonly incarnation: string }
interface OwnedLease { readonly key: string; spec: LeaseSpec; owner: string; revision: number; writtenAt: number }
interface OwnedRow {
  readonly key: string;
  readonly id: string;
  record: FabricParticipantRecord;
  owner: string;
  revision: number;
  writtenAt: number;
  readonly claims: Map<string, number>;
}

export interface NatsKvMirrorOptions {
  readonly transport: Pick<FabricTransport, "kind" | "self" | "kv">;
  readonly reader: NatsPresenceReader;
  /** This bridge process's identity for lease rows (default: read from /proc). */
  readonly processProof?: TransportHostLeaseProcessProof;
  /** Whether a lease row's writer process has exited (default: /proc and boot id). */
  readonly predecessorExited?: (proof: TransportHostLeaseProcessProof) => boolean;
  readonly now?: () => number;
  readonly monotonic?: () => number;
  readonly renewMs?: number;
  readonly operationTimeoutMs?: number;
  readonly onError?: (error: unknown) => void;
  readonly log?: (line: string) => void;
}

const sameSpec = (left: LeaseSpec, right: LeaseSpec): boolean =>
  left.id === right.id && left.rootId === right.rootId && left.identityId === right.identityId && left.incarnation === right.incarnation;
const specOf = (record: FabricParticipantRecord): LeaseSpec =>
  ({ id: record.ownerHostId, rootId: record.rootId, identityId: record.ownerIdentityId, incarnation: record.ownerIncarnation! });
const ownerOf = (spec: LeaseSpec): string => `${spec.id}#${spec.incarnation}`;

/**
 * Writes this link's mirror rows under its own machine: one presence row per mirrored id and one
 * lease row per owner host, shared by every row that host owns. Callers replace a named group
 * (roots, actors) per pass; an incomplete pass never withdraws. A timer renews each lease every
 * `renewMs` only while some row it carries has an unexpired claim, so KV authority ends at most
 * one lease TTL after the file side stops proving it.
 */
export class NatsKvMirror {
  readonly machine: string;
  readonly #presence: TransportKv;
  readonly #leases: TransportKv;
  readonly #reader: NatsPresenceReader;
  readonly #rows = new Map<string, OwnedRow>();
  readonly #owned = new Map<string, OwnedLease>();
  readonly #superseded = new Map<string, string[]>();
  readonly #queues = new Map<string, Promise<unknown>>();
  readonly #reported = new Map<string, string>();
  readonly #now: () => number;
  readonly #monotonic: () => number;
  readonly #renewMs: number;
  readonly #timeoutMs: number;
  #proof: TransportHostLeaseProcessProof | undefined;
  #replacing: Promise<unknown> = Promise.resolve();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #renewing: Promise<void> | undefined;
  #fenced = false;

  constructor(readonly options: NatsKvMirrorOptions) {
    const { transport } = options;
    if (transport.kind !== "nats" || !MACHINE_TOKEN.test(transport.self.machine) || options.reader.machine !== transport.self.machine) {
      throw new TypeError("The NATS bridge mirror needs a NATS transport and a reader on the same machine");
    }
    this.machine = transport.self.machine;
    this.#presence = transport.kv("presence");
    this.#leases = transport.kv("leases");
    if (this.#presence.ttlMs !== KV_PRESENCE_TTL_MS || this.#leases.ttlMs !== KV_LEASE_TTL_MS) {
      throw new TypeError("NATS bridge mirror requires presence TTL 600000 and leases TTL 15000");
    }
    this.#reader = options.reader;
    this.#now = options.now ?? Date.now;
    this.#monotonic = options.monotonic ?? (() => performance.now());
    this.#renewMs = options.renewMs ?? NATS_MIRROR_RENEW_MS;
    this.#timeoutMs = options.operationTimeoutMs ?? 5_000;
    if (!Number.isSafeInteger(this.#renewMs) || this.#renewMs < 1 || this.#renewMs > KV_LEASE_TTL_MS / 2) {
      throw new TypeError("NATS bridge mirror renewMs must be 1..7500");
    }
    if (!Number.isSafeInteger(this.#timeoutMs) || this.#timeoutMs < 1 || this.#timeoutMs > 30_000) {
      throw new TypeError("NATS bridge mirror operationTimeoutMs must be 1..30000");
    }
    if (options.processProof) this.#proof = Object.freeze({ ...options.processProof });
  }

  get fenced(): boolean { return this.#fenced; }

  /**
   * Whether this link holds a routable row for the id now: its lease is ours, matches the row's
   * incarnation, was confirmed within one TTL less the safety margin, and a claim is unexpired.
   */
  holds(id: string): boolean { return this.holdState(id) === "live"; }

  /**
   * live: routable now. renewing: still claimed and its lease is ours, but no lease write was
   * confirmed within the window (a broker outage): retry, do not refuse. absent: not this link's.
   */
  holdState(id: string): "live" | "renewing" | "absent" {
    if (typeof id !== "string" || id.length === 0) return "absent";
    const row = this.#rows.get(natsPresenceKey(this.machine, id));
    const lease = row && this.#owned.get(natsLeaseKey(this.machine, row.record.ownerHostId));
    const now = this.#monotonic();
    if (!row || !lease || !sameSpec(lease.spec, specOf(row.record)) || ![...row.claims.values()].some(until => until > now)) return "absent";
    return now - lease.writtenAt < KV_LEASE_TTL_MS - TRANSPORT_HOST_LEASE_SAFETY_MS ? "live" : "renewing";
  }

  /** The rows this link has written, with whether each is routable now. */
  held(): Array<{ id: string; kind: FabricParticipantKind; groups: string[]; live: boolean }> {
    return [...this.#rows.values()].map(row => ({ id: row.id, kind: row.record.kind, groups: [...row.claims.keys()].sort(), live: this.holds(row.id) }))
      .sort((a, b) => a.id.localeCompare(b.id));
  }

  /**
   * Replace one group's claims. `complete` false (a partial file snapshot) keeps omitted rows,
   * which then lapse when their last claim's deadline passes. Each refused record is reported in
   * the outcome; only a fenced mirror throws.
   */
  replace(group: string, claims: readonly NatsMirrorClaim[], complete: boolean): Promise<NatsMirrorOutcome> {
    const run = this.#replacing.catch(() => undefined).then(() => this.#replace(group, claims, complete));
    this.#replacing = run;
    return run;
  }

  async #replace(group: string, claims: readonly NatsMirrorClaim[], complete: boolean): Promise<NatsMirrorOutcome> {
    if (this.#fenced) throw new NatsMirrorFencedError();
    const outcome: NatsMirrorOutcome = { held: [], refused: [], withdrawn: [] };
    const wanted = new Map<string, NatsMirrorClaim>();
    for (const claim of claims) {
      const id = claim?.record?.id;
      if (typeof id !== "string" || id.length === 0) { outcome.refused.push({ id: String(id), code: "FABRIC_BRIDGE_NATS_RECORD_INVALID" }); continue; }
      const key = natsPresenceKey(this.machine, id);
      if (wanted.has(key)) {
        outcome.refused.push({ id, code: "FABRIC_BRIDGE_NATS_DUPLICATE" });
        wanted.delete(key); // Two claims for one id in one pass: neither is evidence.
        continue;
      }
      wanted.set(key, claim);
    }
    if (complete) {
      for (const row of this.#rows.values()) if (row.claims.has(group) && !wanted.has(row.key)) row.claims.delete(group);
    }
    for (const [key, claim] of wanted) {
      if (this.#fenced) throw new NatsMirrorFencedError();
      try {
        await this.#serial(key, () => this.#hold(group, key, claim));
        outcome.held.push(claim.record.id);
        this.#reported.delete(claim.record.id);
      } catch (error) {
        if (error instanceof NatsMirrorFencedError) throw error;
        // A refused claim must not keep an older claim of this group alive.
        this.#rows.get(key)?.claims.delete(group);
        const code = error instanceof NatsMirrorRefusedError ? error.code : String(errorCode(error) ?? "FABRIC_BRIDGE_NATS_WRITE_FAILED");
        outcome.refused.push({ id: claim.record.id, code });
        this.#reportRefusal(claim.record.id, code, error);
      }
    }
    for (const row of [...this.#rows.values()]) {
      if (row.claims.size > 0 || this.#fenced) continue;
      try {
        if (await this.#serial(row.key, () => this.#dropRow(row))) outcome.withdrawn.push(row.id);
      } catch (error) { this.#report(error); }
    }
    await this.#releaseUnusedLeases();
    this.#arm();
    return outcome;
  }

  #reportRefusal(id: string, code: string, error: unknown): void {
    if (this.#reported.get(id) === code) return;
    if (this.#reported.size >= MAX_REPORTED) this.#reported.delete(this.#reported.keys().next().value!);
    this.#reported.set(id, code);
    this.options.log?.(`nats mirror: not holding ${id} (${code})`);
    if (!(error instanceof NatsMirrorRefusedError)) this.#report(error);
  }

  #report(error: unknown): void { try { this.options.onError?.(error); } catch { /* diagnostic only */ } }

  #signal(timeoutMs = this.#timeoutMs): AbortSignal { return AbortSignal.timeout(timeoutMs); }

  #serial<T>(key: string, work: () => Promise<T>): Promise<T> {
    const previous = this.#queues.get(key) ?? Promise.resolve();
    const result = previous.catch(() => undefined).then(work);
    this.#queues.set(key, result);
    void result.finally(() => { if (this.#queues.get(key) === result) this.#queues.delete(key); }).catch(() => undefined);
    return result;
  }

  #processProof(): TransportHostLeaseProcessProof {
    this.#proof ??= readLocalHostLeaseProcessProof();
    return this.#proof;
  }

  #leaseValue(spec: LeaseSpec): FabricKvHostLease & { incarnationToken: string } {
    const proof = this.#processProof();
    const updatedAt = this.#now();
    return { id: spec.id, rootId: spec.rootId, identityId: spec.identityId, incarnation: spec.incarnation,
      incarnationToken: spec.incarnation, pid: proof.pid, pidStartTime: proof.pidStartTime, bootId: proof.bootId,
      updatedAt, expiresAt: updatedAt + KV_LEASE_TTL_MS };
  }

  /** A lease row this link may adopt: the same owner and identity, written by this process or a dead one. */
  #adoptableLease(entry: TransportKvEntry<unknown>, spec: LeaseSpec): boolean {
    const lease = leaseValue(entry);
    if (!lease || entry.key !== natsLeaseKey(this.machine, spec.id) || entry.owner !== ownerOf(spec) ||
      !sameSpec({ id: lease.id, rootId: lease.rootId, identityId: lease.identityId, incarnation: lease.incarnation }, spec)) return false;
    const proof = this.#processProof();
    if (lease.pid === proof.pid && lease.pidStartTime === proof.pidStartTime && lease.bootId === proof.bootId) return true;
    // Exit evidence is local: only a predecessor on this same boot can be proved gone. Another boot
    // or host (one machine credential used twice) waits for the broker to expire its lease instead.
    if (lease.bootId !== proof.bootId) return false;
    try {
      return (this.options.predecessorExited ?? transportHostLeasePredecessorExited)({ pid: lease.pid, pidStartTime: lease.pidStartTime, bootId: lease.bootId });
    } catch { return false; } // An unreadable proof is not exit evidence.
  }

  #adoptableRow(entry: TransportKvEntry<unknown>, record: FabricParticipantRecord): boolean {
    const current = presenceValue(entry);
    return !!current && entry.key === natsPresenceKey(this.machine, record.id) && entry.owner === ownerOf(specOf(record)) &&
      current.id === record.id && current.kind === record.kind && current.rootId === record.rootId &&
      current.ownerHostId === record.ownerHostId && current.ownerIdentityId === record.ownerIdentityId &&
      current.ownerIncarnation === record.ownerIncarnation;
  }

  /** `recreate`: a lease whose KV row lapsed may be created again (the key is per host, under this machine). */
  async #put(kv: TransportKv, key: string, owner: string, value: unknown, ifRevision: number,
    adoptable: (entry: TransportKvEntry<unknown>) => boolean, recreate = false): Promise<number> {
    // One budget for the write AND all recovery, leaving room for the renewal cadence inside
    // the 13 s freshness window, even when operationTimeoutMs is configured above the default.
    const signal = this.#signal(Math.min(this.#timeoutMs,
      KV_LEASE_TTL_MS - TRANSPORT_HOST_LEASE_SAFETY_MS - this.#renewMs - 1));
    let revision = ifRevision;
    for (;;) {
      signal.throwIfAborted();
      try { return await kv.put(key, owner, value, revision, signal); }
      catch (error) {
        signal.throwIfAborted();
        if (!isUncertain(error) && !isConflict(error)) throw error;
        // Settle each lost revision by reading: only a verified foreign row means lost ownership.
        // Our row may expire again between this read and the retry; keep recovery in this budget.
        const current = await kv.get<unknown>(key, signal);
        signal.throwIfAborted();
        if (!current) {
          // Presence needs #hold's another-machine check; only leases may be re-created here.
          if (!isConflict(error) || !recreate) throw error;
          revision = 0; // create-if-absent also handles the broker's expiry marker.
          continue;
        }
        if (!adoptable(current)) throw new KvConflictError(key, revision);
        if (canonicalJson(current.value) === canonicalJson(value) && current.owner === owner) return current.revision;
        revision = current.revision;
      }
    }
  }

  async #ensureLease(spec: LeaseSpec, id: string): Promise<OwnedLease> {
    const key = natsLeaseKey(this.machine, spec.id);
    if (this.#superseded.get(key)?.includes(spec.incarnation)) throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_STALE_INCARNATION", id);
    const held = this.#owned.get(key);
    if (held && sameSpec(held.spec, spec)) return held;
    return await this.#serial(key, async () => {
      const current = this.#owned.get(key);
      if (current && sameSpec(current.spec, spec)) return current;
      if (this.#fenced) throw new NatsMirrorFencedError();
      const owner = ownerOf(spec);
      const value = this.#leaseValue(spec);
      if (current) {
        // The file host restarted: its newer incarnation replaces ours in one CAS, and the
        // superseded one is never accepted again (a stale claim cannot flip it back).
        const revision = await this.#put(this.#leases, key, owner, value, current.revision, entry => this.#adoptableLease(entry, spec), true);
        const superseded = [...(this.#superseded.get(key) ?? []).filter(token => token !== current.spec.incarnation), current.spec.incarnation];
        this.#superseded.set(key, superseded.slice(-MAX_SUPERSEDED));
        Object.assign(current, { spec, owner, revision, writtenAt: this.#monotonic() });
        return current;
      }
      let revision: number;
      try {
        revision = await this.#put(this.#leases, key, owner, value, 0, entry => this.#adoptableLease(entry, spec));
      } catch (error) {
        if (isConflict(error)) throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_LEASE_CONFLICT", id, { cause: error });
        throw error;
      }
      const lease: OwnedLease = { key, spec, owner, revision, writtenAt: this.#monotonic() };
      this.#owned.set(key, lease);
      return lease;
    });
  }

  async #hold(group: string, key: string, claim: NatsMirrorClaim): Promise<void> {
    if (this.#fenced) throw new NatsMirrorFencedError();
    const record = cloneFrozen(claim.record);
    const owner = ownerOf(specOf(record));
    if (!presenceValue({ key, machine: this.machine, owner, value: record, revision: 1 })) {
      throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_RECORD_INVALID", record.id);
    }
    // Infinity is the link's own identity: live exactly while the mirror is open. NaN never is.
    if (!(claim.until > this.#monotonic())) throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_EXPIRED", record.id);
    const existing = this.#rows.get(key);
    if (!existing) {
      // A receiver treats one id on two machines as ambiguous: never create that state.
      if (!this.#reader.ready) throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_DIRECTORY_NOT_READY", record.id);
      if (this.#reader.machinesFor(record.id).some(machine => machine !== this.machine)) {
        throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_ID_ON_ANOTHER_MACHINE", record.id);
      }
    } else if (existing.claims.size > 0 && [...existing.claims.keys()].some(other => other !== group) &&
      mirrorContent(existing.record) !== mirrorContent(record)) {
      throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_DUPLICATE", record.id);
    }
    // Lease first: a presence row never routes without its owner lease, so this order never
    // advertises a row the link cannot renew.
    await this.#ensureLease(specOf(record), record.id);
    const startedAt = this.#monotonic();
    if (existing) {
      const changed = existing.owner !== owner || mirrorContent(existing.record) !== mirrorContent(record);
      if (changed || startedAt - existing.writtenAt >= NATS_MIRROR_PRESENCE_REFRESH_MS) {
        existing.revision = await this.#putRow(key, owner, record, existing.revision);
        existing.record = record;
        existing.owner = owner;
        existing.writtenAt = this.#monotonic();
      }
      existing.claims.set(group, claim.until);
      return;
    }
    let revision: number;
    try {
      revision = await this.#putRow(key, owner, record, 0);
    } catch (error) {
      if (isConflict(error)) throw new NatsMirrorRefusedError("FABRIC_BRIDGE_NATS_PRESENCE_CONFLICT", record.id, { cause: error });
      throw error;
    }
    this.#rows.set(key, { key, id: record.id, record, owner, revision, writtenAt: this.#monotonic(), claims: new Map([[group, claim.until]]) });
  }

  #putRow(key: string, owner: string, record: FabricParticipantRecord, ifRevision: number): Promise<number> {
    return this.#put(this.#presence, key, owner, record, ifRevision, entry => this.#adoptableRow(entry, record));
  }

  async #dropRow(row: OwnedRow): Promise<boolean> {
    if (this.#rows.get(row.key) !== row || row.claims.size > 0) return false;
    this.#rows.delete(row.key);
    // CAS by our revision only: a newer writer's row is never deleted.
    return await this.#presence.delete(row.key, row.revision, this.#signal());
  }

  async #dropLease(lease: OwnedLease): Promise<boolean> {
    if (this.#owned.get(lease.key) !== lease) return false;
    this.#owned.delete(lease.key);
    return await this.#leases.delete(lease.key, lease.revision, this.#signal());
  }

  /** Each owned lease's latest claim deadline over the rows it carries (their incarnation must match). */
  #leaseUntil(lease: OwnedLease): number {
    let until = Number.NEGATIVE_INFINITY;
    for (const row of this.#rows.values()) {
      if (natsLeaseKey(this.machine, row.record.ownerHostId) !== lease.key || !sameSpec(specOf(row.record), lease.spec)) continue;
      for (const claim of row.claims.values()) until = Math.max(until, claim);
    }
    return until;
  }

  async #releaseUnusedLeases(): Promise<void> {
    const now = this.#monotonic();
    for (const lease of [...this.#owned.values()]) {
      if (this.#leaseUntil(lease) > now) continue;
      try { await this.#serial(lease.key, () => this.#dropLease(lease)); }
      catch (error) { this.#report(error); }
    }
  }

  #arm(scheduledAt = this.#monotonic()): void {
    if (this.#fenced || this.#timer || this.#renewing || (this.#owned.size === 0 && this.#rows.size === 0)) return;
    const next = scheduledAt + this.#renewMs;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.renew().catch(error => this.#report(error)).finally(() => this.#arm(next));
    }, Math.max(0, next - this.#monotonic()));
    this.#timer.unref?.();
  }

  /** One renewal pass: renew live-claimed leases, drop lapsed ones, refresh old presence rows. */
  renew(): Promise<void> {
    if (this.#renewing) return this.#renewing;
    const pass = (async () => {
      if (this.#fenced) return;
      const now = this.#monotonic();
      const failures: unknown[] = [];
      const diagnostics: NatsRenewalFailure[] = [];
      const failed = (error: unknown, operation: NatsRenewalFailure["operation"], key: string, started: number): void => {
        failures.push(error);
        if (diagnostics.length < 5) diagnostics.push(natsRenewalFailure(error, operation, key, this.#monotonic() - started));
      };
      await Promise.all([...this.#owned.values()].map(lease => {
        const started = this.#monotonic();
        let operation: NatsRenewalFailure["operation"] = "lease-renewal";
        return this.#serial(lease.key, async () => {
          if (this.#fenced || this.#owned.get(lease.key) !== lease) return;
          if (this.#leaseUntil(lease) <= this.#monotonic()) { operation = "lease-drop"; await this.#dropLease(lease); return; }
          try {
            lease.revision = await this.#put(this.#leases, lease.key, lease.owner, this.#leaseValue(lease.spec), lease.revision,
              entry => this.#adoptableLease(entry, lease.spec), true);
            lease.writtenAt = this.#monotonic();
          } catch (error) {
            if (isConflict(error)) {
              // Another writer owns the key now: stop renewing; never fight it.
              this.#owned.delete(lease.key);
              this.options.log?.(`nats mirror: lost lease ${lease.spec.id} to another writer`);
            }
            throw error;
          }
        }).catch(error => { failed(error, operation, lease.key, started); });
      }));
      await Promise.all([...this.#rows.values()].map(row => {
        const started = this.#monotonic();
        return this.#serial(row.key, async () => {
          if (this.#fenced || this.#rows.get(row.key) !== row || now - row.writtenAt < NATS_MIRROR_PRESENCE_REFRESH_MS) return;
          if (![...row.claims.values()].some(until => until > this.#monotonic())) return;
          try {
            row.revision = await this.#putRow(row.key, row.owner, row.record, row.revision);
            row.writtenAt = this.#monotonic();
          } catch (error) {
            if (isConflict(error)) this.#rows.delete(row.key);
            throw error;
          }
        }).catch(error => { failed(error, "presence-refresh", row.key, started); });
      }));
      if (failures.length > 0) {
        const error = failures.length === 1 ? failures[0] : new AggregateError(failures, "NATS bridge mirror renewal failed");
        noteNatsRenewalFailures(error, diagnostics);
        throw error;
      }
    })();
    this.#renewing = pass;
    void pass.finally(() => { if (this.#renewing === pass) this.#renewing = undefined; }).catch(() => undefined);
    return pass;
  }

  /** Stop all writes; rows keep their last state and leases lapse within one TTL. */
  async fence(): Promise<void> {
    this.#fenced = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    await this.#replacing.catch(() => undefined);
    await this.#renewing?.catch(() => undefined);
    await Promise.allSettled([...this.#queues.values()]);
  }

  /**
   * Withdraw one group now (CAS-deletes its rows, and each lease no other group still uses), for a
   * clean stop of an identity the link owns. Other groups and their leases are untouched.
   */
  async release(group: string): Promise<void> {
    const run = this.#replacing.catch(() => undefined).then(async () => {
      for (const row of [...this.#rows.values()]) {
        if (!row.claims.delete(group) || row.claims.size > 0) continue;
        const lease = this.#owned.get(natsLeaseKey(this.machine, row.record.ownerHostId));
        const shared = [...this.#rows.values()].some(other => other !== row && other.record.ownerHostId === row.record.ownerHostId);
        try {
          if (lease && !shared) await this.#serial(lease.key, () => this.#dropLease(lease));
          await this.#serial(row.key, () => this.#dropRow(row));
        } catch (error) { this.#report(error); }
      }
    });
    this.#replacing = run;
    await run;
  }

  /** Fence, then CAS-delete every owned row: leases first, so routing stops at once. */
  async withdraw(): Promise<void> {
    await this.fence();
    const failures: unknown[] = [];
    for (const lease of [...this.#owned.values()]) {
      this.#owned.delete(lease.key);
      try { await this.#leases.delete(lease.key, lease.revision, this.#signal()); } catch (error) { failures.push(error); }
    }
    for (const row of [...this.#rows.values()]) {
      this.#rows.delete(row.key);
      try { await this.#presence.delete(row.key, row.revision, this.#signal()); } catch (error) { failures.push(error); }
    }
    for (const failure of failures) this.#report(failure);
  }
}

/** The mirror presence shape `MeshBridge` exchanges (bridge.ts BridgeMirrorPresence). */
export type NatsBridgeMirrorPresence = Pick<BridgePresence, "hosts" | "participants" | "dropped" | "partial" | "droppedCount">;

export interface NatsBridgePresenceOptions {
  readonly transport: Pick<FabricTransport, "kind" | "self" | "kv">;
  /** The link name; the file side records mirrored NATS roots with this `remoteHost`. */
  readonly link: string;
  /** Static allow-list decision (L3 identity module). Nothing crosses without it. */
  readonly admit: (record: FabricParticipantRecord, direction: NatsPresenceDirection, machine?: string) => boolean;
  readonly processProof?: TransportHostLeaseProcessProof;
  readonly predecessorExited?: (proof: TransportHostLeaseProcessProof) => boolean;
  readonly now?: () => number;
  readonly monotonic?: () => number;
  readonly renewMs?: number;
  readonly operationTimeoutMs?: number;
  readonly onError?: (error: unknown) => void;
  readonly log?: (line: string) => void;
}

const LINK_NAME = /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,62}$/;

const isPartial = (presence: NatsBridgeMirrorPresence): boolean => {
  const raw: unknown = presence;
  if (typeof raw !== "object" || raw === null) return true;
  const value = raw as Record<string, unknown>;
  return value.partial === true || (typeof value.droppedCount === "number" && value.droppedCount > 0) ||
    (Array.isArray(value.dropped) && value.dropped.length > 0) || !Array.isArray(value.hosts) || !Array.isArray(value.participants);
};

/**
 * The presence half of the NATS side of a `MeshBridge` link, shaped like `BridgeSide`:
 * `presence()` is the NATS roots the file side may mirror, `mirror()` writes the file roots into
 * KV, and `subscribeChanges()` wakes the bridge on another machine's KV change.
 */
export class NatsBridgePresence {
  readonly reader: NatsPresenceReader;
  readonly writer: NatsKvMirror;
  readonly link: string;
  readonly #now: () => number;
  readonly #monotonic: () => number;
  readonly #ambiguous = new Set<string>();
  readonly #rechecking = new Set<string>();
  readonly #skipped = new Set<string>();

  constructor(readonly options: NatsBridgePresenceOptions) {
    if (!LINK_NAME.test(options.link)) throw new TypeError("Invalid NATS bridge link name");
    if (typeof options.admit !== "function") throw new TypeError("NATS bridge presence needs an admission decision");
    this.link = options.link;
    this.#now = options.now ?? Date.now;
    this.#monotonic = options.monotonic ?? (() => performance.now());
    this.reader = new NatsPresenceReader({ transport: options.transport, ...(options.onError ? { onError: options.onError } : {}),
      now: this.#now, monotonic: this.#monotonic });
    this.writer = new NatsKvMirror({ transport: options.transport, reader: this.reader, now: this.#now, monotonic: this.#monotonic,
      ...(options.processProof ? { processProof: options.processProof } : {}),
      ...(options.predecessorExited ? { predecessorExited: options.predecessorExited } : {}),
      ...(options.renewMs !== undefined ? { renewMs: options.renewMs } : {}),
      ...(options.operationTimeoutMs !== undefined ? { operationTimeoutMs: options.operationTimeoutMs } : {}),
      ...(options.onError ? { onError: options.onError } : {}), ...(options.log ? { log: options.log } : {}) });
  }

  start(): void { this.reader.start(); }

  whenReady(signal?: AbortSignal): Promise<void> { return this.reader.whenReady(signal); }

  /** A receiver's route for an id (destination machine, owner and incarnation), or undefined. */
  resolve(id: string): NatsRoute | undefined { return this.reader.resolve(id); }

  /**
   * Live, unique, admitted canonical NATS roots on other machines, as sampled hosts. Before both
   * KV snapshots are ready the answer is an empty partial snapshot: it can never withdraw a mirror.
   * A root whose owner lease awaits a root re-check is left out and makes the snapshot partial.
   */
  async presence(): Promise<BridgePresence> {
    if (!this.reader.ready) return { hosts: [], participants: [], reserved: [], partial: true };
    const observedAt = this.#now();
    const monotonicNow = this.#monotonic();
    const hosts: BridgeHost[] = [];
    const participants: FabricParticipantRecord[] = [];
    const { routes, ambiguous } = this.reader.routes();
    for (const id of ambiguous) {
      if (this.#ambiguous.has(id) || this.#ambiguous.size >= MAX_REPORTED) continue;
      this.#ambiguous.add(id);
      this.options.log?.(`nats presence: ${id} has rows on several machines; not routable, not mirrored`);
    }
    for (const id of [...this.#ambiguous]) if (!ambiguous.includes(id)) this.#ambiguous.delete(id);
    const rechecking = new Set<string>();
    for (const route of routes) {
      if (route.machine === this.reader.machine || (!route.live && !route.rootRecheck) || route.kind !== "root") continue;
      const record = projectNatsMirrorRecord(route.record);
      if (!record || !isCanonicalRoot(record.id) || record.id !== record.rootId || record.id !== record.ownerHostId ||
        record.id !== record.ownerIdentityId) continue;
      if (!this.options.admit(record, "toFile", route.machine)) continue;
      if (route.rootRecheck) {
        // Its owner is re-checking the root: not routable, so not mirrored live, and not gone, so the
        // snapshot is partial and the file side holds its mirror (to that mirror's own deadline).
        rechecking.add(record.id);
        if (!this.#rechecking.has(record.id) && this.#rechecking.size < MAX_REPORTED) {
          this.#rechecking.add(record.id);
          this.options.log?.(`nats presence: ${record.id} awaits a root re-check; not routable, mirror held`);
        }
        continue;
      }
      const remaining = Math.min(MIRROR_FRESH_MS, route.liveUntil - monotonicNow);
      if (remaining <= 0) continue;
      const expiresAt = observedAt + remaining;
      hosts.push({
        record: {
          format: 1, id: record.id, rootId: record.rootId,
          identity: { id: record.id, name: record.name, kind: "main", ...(record.sessionId ? { sessionId: record.sessionId } : {}) },
          incarnationToken: record.ownerIncarnation!, startedAt: record.startedAt, updatedAt: observedAt, expiresAt,
        },
        expiresAt,
        observedAt,
      });
      participants.push(record);
    }
    for (const id of [...this.#rechecking]) if (!rechecking.has(id)) this.#rechecking.delete(id);
    // During a reconnect gap a root that aged out is unknown, not gone: never withdrawal evidence.
    // So is a root whose lease awaits a root re-check.
    return { hosts, participants, reserved: [], ...(this.reader.resyncing || rechecking.size > 0 ? { partial: true } : {}) };
  }

  /**
   * Hold the file side's canonical native roots in KV. Re-exported (`bridgeVia`) and mirrored
   * (`remoteHost`) roots never cross here. Authority ends at the sample's own deadline, capped
   * at the mirror freshness bound from its observation.
   */
  async mirror(presence: NatsBridgeMirrorPresence, observedAt?: number): Promise<NatsMirrorOutcome> {
    const now = this.#now();
    const monotonicNow = this.#monotonic();
    const complete = !isPartial(presence);
    const hosts = Array.isArray(presence?.hosts) ? presence.hosts : [];
    const participants = Array.isArray(presence?.participants) ? presence.participants : [];
    const roots = new Map<string, BridgeHost>();
    const duplicate = new Set<string>();
    for (const host of hosts) {
      const record = host?.record;
      if (!record || typeof record.id !== "string" || record.remoteHost !== undefined || record.bridgeVia !== undefined ||
        !isCanonicalRoot(record.id) || record.rootId !== record.id || record.identity?.id !== record.id) continue;
      if (roots.has(record.id)) duplicate.add(record.id);
      roots.set(record.id, host);
    }
    const claims: NatsMirrorClaim[] = [];
    let incomplete = !complete;
    for (const participant of participants) {
      const host = participant && roots.get(participant.id);
      if (!host || duplicate.has(participant.id)) continue;
      const record = projectNatsMirrorRecord(participant);
      if (!record || record.kind !== "root" || record.id !== record.rootId || record.id !== record.ownerHostId ||
        record.id !== record.ownerIdentityId) {
        // Usually a legacy root or one without an owner incarnation: a command could not be fenced.
        this.#skip(participant.id, "not carried: needs a v1 canonical root with an owner incarnation");
        continue;
      }
      if (!this.options.admit(record, "toNats")) continue;
      this.#skipped.delete(record.id);
      const sampledAt = typeof host.observedAt === "number" && Number.isFinite(host.observedAt) ? host.observedAt
        : typeof observedAt === "number" && Number.isFinite(observedAt) ? Math.min(observedAt, now) : now;
      if (!Number.isFinite(host.expiresAt)) { incomplete = true; continue; }
      const deadline = Math.min(host.expiresAt, sampledAt + MIRROR_FRESH_MS);
      if (deadline <= now) continue;
      claims.push({ record, until: monotonicNow + (deadline - now) });
    }
    return await this.writer.replace("roots", claims, !incomplete);
  }

  #skip(id: unknown, reason: string): void {
    if (typeof id !== "string" || this.#skipped.has(id) || this.#skipped.size >= MAX_REPORTED) return;
    this.#skipped.add(id);
    this.options.log?.(`nats mirror: ${id} ${reason}`);
  }

  /**
   * The sender check a NATS receiver applies (control-plane #natsSenderVerified): the id resolves
   * to exactly one live row and its KV machine is the delivery's credential-bound source machine.
   */
  verifySender(fromId: string, sourceMachine: string): { ok: true; route: NatsRoute } | { ok: false; refuse: "FABRIC_SENDER_UNVERIFIED" } {
    const route = typeof fromId === "string" ? this.reader.resolve(fromId) : undefined;
    return route?.live === true && route.machine === sourceMachine && route.machine !== this.reader.machine
      ? { ok: true, route } : { ok: false, refuse: "FABRIC_SENDER_UNVERIFIED" };
  }

  /**
   * Hold allow-listed file actors in KV (their own group, so a roots pass never withdraws them).
   * Each actor's authority ends at its owner host's sample deadline when `hosts` carries that host,
   * else at `sampledAt` (when the native listing proved it live) plus the mirror freshness bound.
   */
  async mirrorActors(actors: readonly FabricParticipantRecord[], options: { hosts?: readonly BridgeHost[];
    sampledAt: number; complete: boolean }): Promise<NatsMirrorOutcome> {
    const now = this.#now();
    const monotonicNow = this.#monotonic();
    const owners = new Map<string, BridgeHost>();
    for (const host of options.hosts ?? []) {
      const record = host?.record;
      if (record && typeof record.id === "string" && record.remoteHost === undefined && record.bridgeVia === undefined) owners.set(record.id, host);
    }
    const claims: NatsMirrorClaim[] = [];
    for (const actor of actors) {
      const record = actor && projectNatsMirrorRecord(actor);
      if (!record || record.kind !== "actor") {
        this.#skip(actor?.id, "not carried: needs a v1 native actor with an owner incarnation");
        continue;
      }
      if (!this.options.admit(record, "toNats")) continue;
      this.#skipped.delete(record.id);
      let deadline = Math.min(options.sampledAt, now) + MIRROR_FRESH_MS;
      const owner = owners.get(record.ownerHostId);
      if (owner && Number.isFinite(owner.expiresAt)) {
        const ownerSampledAt = typeof owner.observedAt === "number" && Number.isFinite(owner.observedAt) ? owner.observedAt : now;
        deadline = Math.min(deadline, owner.expiresAt, ownerSampledAt + MIRROR_FRESH_MS);
      }
      if (deadline <= now) continue;
      claims.push({ record, until: monotonicNow + (deadline - now) });
    }
    return await this.writer.replace("actors", claims, options.complete);
  }

  /**
   * Wake the bridge on another machine's presence or lease change; own rows never wake it.
   * `failed` fires once on a terminal watch refusal (credential denied, bucket invalid): the link
   * must stop, not keep routing on a view that can no longer be refreshed.
   */
  subscribeChanges(changed: (kind: BridgeChange) => void, failed?: (error: Error) => void): () => void {
    const stopChanges = this.reader.subscribe((bucket, machine) => {
      if (machine === this.reader.machine) return;
      changed(bucket === "presence" ? "presence" : "lease");
    });
    const stopFailures = failed ? this.reader.subscribeFailure(failed) : undefined;
    return () => { stopChanges(); stopFailures?.(); };
  }

  holds(id: string): boolean { return this.writer.holds(id); }

  fence(): Promise<void> { return this.writer.fence(); }

  async withdraw(): Promise<void> { await this.writer.withdraw(); }

  async close(): Promise<void> {
    await this.writer.fence();
    await this.reader.close();
  }
}
