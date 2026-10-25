// In-memory multi-machine KV for the A' bridge NATS-side unit tests (smarty-dev#15310, lane L1).
// Broker semantics the bridge relies on: one bucket shared by every machine, per-bucket CAS
// revisions, writes bound to the credential's machine prefix, and watches that deliver a snapshot,
// one `ready`, then changes in order. Expiry is explicit (`expire`).
import { createHash } from "node:crypto";
import {
  KvConflictError,
  TransportDeniedError,
  TransportUncertainError,
  type FabricTransport,
  type TransportBucket,
  type TransportKv,
  type TransportKvEntry,
  type TransportKvEvent,
} from "../../src/transport/types.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "../../src/topology/types.js";

export const kvToken = (id: string): string => createHash("sha256").update(id).digest("hex").slice(0, 32);
export const pKey = (machine: string, id: string): string => `${machine}.p.${kvToken(id)}`;
export const hKey = (machine: string, id: string): string => `${machine}.h.${kvToken(id)}`;

type Event = TransportKvEvent<unknown>;
interface Watcher { queue: Event[]; wake?: (() => void) | undefined }

export class MemoryBucket {
  readonly values = new Map<string, TransportKvEntry<unknown>>();
  readonly watchers = new Set<Watcher>();
  revision = 0;
  failNextPut: ((key: string) => "uncertain-stored" | "uncertain-lost" | undefined) | undefined;
  constructor(readonly ttlMs: number, readonly writes: string[] = []) {}
  emit(event: Event): void { for (const watcher of this.watchers) { watcher.queue.push(event); watcher.wake?.(); } }
  put(key: string, owner: string, value: unknown, ifRevision: number): number {
    const current = this.values.get(key);
    if ((ifRevision === 0 && current) || (ifRevision > 0 && current?.revision !== ifRevision)) throw new KvConflictError(key, ifRevision);
    const fault = this.failNextPut?.(key);
    if (fault) this.failNextPut = undefined;
    if (fault === "uncertain-lost") throw new TransportUncertainError(key, { owner, value });
    const entry = { key, machine: key.split(".")[0]!, owner, value: JSON.parse(JSON.stringify(value)) as unknown, revision: ++this.revision };
    this.writes.push(key);
    this.values.set(key, entry);
    this.emit({ type: "put", entry });
    if (fault === "uncertain-stored") throw new TransportUncertainError(key, { owner, value });
    return entry.revision;
  }
  delete(key: string, ifRevision: number): boolean {
    const current = this.values.get(key);
    if (!current || current.revision !== ifRevision) return false;
    this.values.delete(key);
    this.emit({ type: "gone", key, machine: key.split(".")[0]!, revision: ++this.revision, reason: "deleted" });
    return true;
  }
  expire(key: string): void {
    if (!this.values.delete(key)) return;
    this.emit({ type: "gone", key, machine: key.split(".")[0]!, revision: ++this.revision, reason: "expired" });
  }
}

export class MemoryKv implements TransportKv {
  constructor(readonly bucket: MemoryBucket, readonly machine: string) {}
  get ttlMs(): number { return this.bucket.ttlMs; }
  async get<V>(key: string): Promise<TransportKvEntry<V> | undefined> { return this.bucket.values.get(key) as TransportKvEntry<V> | undefined; }
  async put<V>(key: string, owner: string, value: V, ifRevision: number): Promise<number> {
    if (key.split(".")[0] !== this.machine) throw new TransportDeniedError("KV write is outside the credential's machine prefix");
    return this.bucket.put(key, owner, value, ifRevision);
  }
  async delete(key: string, ifRevision: number): Promise<boolean> {
    if (key.split(".")[0] !== this.machine) throw new TransportDeniedError("KV write is outside the credential's machine prefix");
    return this.bucket.delete(key, ifRevision);
  }
  async *watch<V>(_prefix: string, signal: AbortSignal): AsyncIterable<TransportKvEvent<V>> {
    const watcher: Watcher = { queue: [] };
    for (const entry of this.bucket.values.values()) watcher.queue.push({ type: "put", entry });
    watcher.queue.push({ type: "ready" });
    this.bucket.watchers.add(watcher);
    try {
      while (!signal.aborted) {
        const event = watcher.queue.shift() ?? await new Promise<Event | undefined>(resolve => {
          const abort = (): void => { watcher.wake = undefined; resolve(undefined); };
          watcher.wake = () => { watcher.wake = undefined; signal.removeEventListener("abort", abort); resolve(watcher.queue.shift()); };
          signal.addEventListener("abort", abort, { once: true });
        });
        if (!event) return;
        yield event as TransportKvEvent<V>;
      }
    } finally { this.bucket.watchers.delete(watcher); }
  }
}

export interface Clock { wall: number; mono: number }
export const advance = (clock: Clock, ms: number): void => { clock.wall += ms; clock.mono += ms; };

export type KvWorld = ReturnType<typeof kvWorld>;
export function kvWorld() {
  const writes: string[] = [];
  const presence = new MemoryBucket(600_000, writes);
  const leases = new MemoryBucket(15_000, writes);
  const transport = (machine: string, hostId: string, send?: FabricTransport["send"]): FabricTransport => ({
    kind: "nats", self: { machine, hostId },
    send: send ?? (async () => { throw new Error("unused"); }), async *inbox() { return; },
    kv(bucket: TransportBucket) { return new MemoryKv(bucket === "presence" ? presence : leases, machine); },
    async drainState() { return { pending: 0, ackPending: 0 }; }, async close() {},
  });
  return { presence, leases, transport, writes };
}

export function natsLease(id: string, incarnation: string, updatedAt: number, rootId = id, identityId = id): FabricKvHostLease & { incarnationToken: string } {
  return { id, rootId, identityId, incarnation, incarnationToken: incarnation, pid: 7, pidStartTime: "70",
    bootId: "aaaaaaaa-1234-4234-8234-123456789abc", updatedAt, expiresAt: updatedAt + 15_000 };
}

/** A native participant on `machine`: its presence row plus its owner host's lease. */
export function seedNative(w: KvWorld, machine: string, record: FabricParticipantRecord, leaseAt: number): void {
  const owner = `${record.ownerHostId}#${record.ownerIncarnation}`;
  const key = hKey(machine, record.ownerHostId);
  const current = w.leases.values.get(key);
  w.leases.put(key, owner, natsLease(record.ownerHostId, record.ownerIncarnation!, leaseAt, record.rootId, record.ownerIdentityId), current?.revision ?? 0);
  w.presence.put(pKey(machine, record.id), owner, record, 0);
}
