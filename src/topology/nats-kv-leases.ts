import { createHash, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import type { NatsConnection } from "@nats-io/transport-node";
import { jetstream, JetStreamApiCodes, JetStreamApiError, StorageType } from "@nats-io/jetstream";
import { Kvm, type KV, type KvEntry } from "@nats-io/kv";
import type { FabricHostLease } from "./host-leases.js";

/** Prototype only. No runtime selector or ParticipantDirectory imports this module. */
export interface LeaseSnapshot {
  readonly lease: FabricHostLease;
  /** A UUID, not startedAt: two incarnations can start in the same millisecond. */
  readonly incarnation: string;
  /** JetStream stream sequence; consumers must enforce this fencing token. */
  readonly revision: number;
}

/** Async equivalent of the file lease operations; payload remains FabricHostLease. */
export interface HostLeaseStore {
  acquire(lease: FabricHostLease, incarnation: string): Promise<LeaseSnapshot | undefined>;
  renew(expected: LeaseSnapshot, lease: FabricHostLease): Promise<LeaseSnapshot>;
  release(expected: LeaseSnapshot): Promise<boolean>;
  read(id: string): Promise<LeaseSnapshot | undefined>;
  list(): Promise<Map<string, LeaseSnapshot>>;
  acquireWaiting(lease: () => FabricHostLease, incarnation: string,
    options: { waitMs: number; signal?: AbortSignal }): Promise<LeaseSnapshot>;
}

export const newLeaseIncarnation = (): string => randomUUID();
export class LeaseLostError extends Error {
  override readonly name = "LeaseLostError";
}
export class LeaseWaitTimeoutError extends Error {
  override readonly name = "LeaseWaitTimeoutError";
}

interface StoredLease { format: 1; lease: FabricHostLease; incarnation: string }
const keyFor = (id: string): string => createHash("sha256").update(id).digest("hex");
const conflict = (error: unknown): boolean => error instanceof JetStreamApiError &&
  (error.code === JetStreamApiCodes.StreamWrongLastSequence ||
    error.code === JetStreamApiCodes.StreamWrongLastSequenceUnknown);
const missing = (error: unknown): boolean => error instanceof JetStreamApiError &&
  error.code === JetStreamApiCodes.NoMessageFound;
const sameOwner = (a: LeaseSnapshot, b: LeaseSnapshot): boolean => a.incarnation === b.incarnation &&
  a.lease.id === b.lease.id && a.lease.rootId === b.lease.rootId &&
  a.lease.identityId === b.lease.identityId && a.lease.startedAt === b.lease.startedAt;

export class NatsKvLeaseStore implements HostLeaseStore {
  private constructor(private readonly kv: KV, readonly maxLeaseMs: number) {}

  /** Provision/open an R3 file bucket. Deployment MUST set sync_interval: always on all servers. */
  static async open(nc: NatsConnection, options: {
    bucket: string; maxLeaseMs: number; timeoutMs?: number;
  }): Promise<NatsKvLeaseStore> {
    const version = (nc.info?.version ?? "").split(".").map(Number);
    const [major = 0, minor = 0, patch = 0] = version;
    if (!(major > 2 || (major === 2 && (minor > 14 || (minor === 14 && patch >= 7))))) {
      throw new Error("NATS lease prototype requires nats-server 2.14.7+");
    }
    if (!Number.isSafeInteger(options.maxLeaseMs) || options.maxLeaseMs < 1) {
      throw new Error("maxLeaseMs must be a positive integer");
    }
    const kv = await new Kvm(jetstream(nc, { timeout: options.timeoutMs ?? 2_000 })).create(options.bucket, {
      replicas: 3, storage: StorageType.File, history: 1,
      // Leader API reads, never follower/direct reads, including create's tombstone check.
      allow_direct: false, ttl: options.maxLeaseMs, markerTTL: options.maxLeaseMs,
    });
    const status = await kv.status();
    const config = status.streamInfo.config;
    if (status.replicas !== 3 || status.storage !== StorageType.File || status.history !== 1 ||
      status.ttl !== options.maxLeaseMs || config.allow_direct || !config.allow_msg_ttl ||
      status.markerTTL !== options.maxLeaseMs || config.mirror || config.sources?.length) {
      throw new Error("Existing lease bucket violates R3/file/TTL/leader-read requirements");
    }
    return new NatsKvLeaseStore(kv, options.maxLeaseMs);
  }

  #validate(lease: FabricHostLease, incarnation: string): void {
    if (!lease || ![lease.id, lease.rootId, lease.identityId].every(v => typeof v === "string" && v.length > 0) ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(incarnation) ||
      !Number.isSafeInteger(lease.updatedAt) || !Number.isSafeInteger(lease.expiresAt) ||
      lease.expiresAt <= lease.updatedAt || lease.expiresAt - lease.updatedAt > this.maxLeaseMs ||
      (lease.startedAt !== undefined && !Number.isSafeInteger(lease.startedAt))) {
      throw new Error("Invalid lease identity/incarnation or TTL exceeds bucket max_age");
    }
  }
  #decode(entry: KvEntry): LeaseSnapshot | undefined {
    if (entry.operation !== "PUT") return undefined;
    const data = entry.json<StoredLease>();
    if (data.format !== 1 || keyFor(data.lease.id) !== entry.key) throw new Error("Invalid lease record/key");
    this.#validate(data.lease, data.incarnation);
    return { lease: data.lease, incarnation: data.incarnation, revision: entry.revision };
  }
  #encoded(lease: FabricHostLease, incarnation: string): string {
    this.#validate(lease, incarnation);
    // A locally future-dated deadline must not outlive the physical bucket max_age.
    // Cross-host clock skew remains a deployment assumption; see the prototype design.
    if (lease.updatedAt > Date.now()) throw new Error("Cannot publish a future-dated lease");
    return JSON.stringify({ format: 1, lease, incarnation } satisfies StoredLease);
  }
  async #entry(id: string): Promise<KvEntry | undefined> {
    try { return await this.kv.get(keyFor(id)) ?? undefined; }
    catch (error) { if (missing(error)) return undefined; throw error; }
  }
  #live(snapshot: LeaseSnapshot): void {
    if (snapshot.lease.expiresAt <= Date.now()) throw new LeaseLostError("Lease expired; acquire a new incarnation");
    if (!Number.isSafeInteger(snapshot.revision) || snapshot.revision <= 0) throw new LeaseLostError("Invalid fencing token");
  }

  async acquire(lease: FabricHostLease, incarnation: string): Promise<LeaseSnapshot | undefined> {
    const value = this.#encoded(lease, incarnation), key = keyFor(lease.id);
    if (lease.expiresAt <= Date.now()) throw new LeaseLostError("Cannot acquire an expired lease");
    // A physically retained but logically expired value is retired with CAS, NEVER a blind delete.
    const entry = await this.#entry(lease.id);
    const current = entry && this.#decode(entry);
    if (current && current.lease.expiresAt > Date.now()) return undefined;
    try {
      if (current) await this.kv.delete(key, { previousSeq: current.revision });
      const revision = await this.kv.create(key, value);
      const result = { lease: JSON.parse(value).lease as FabricHostLease, incarnation, revision };
      this.#live(result); // Late/ambiguous acknowledgements never grant expired ownership.
      return result;
    } catch (error) {
      if (conflict(error)) return undefined;
      throw error; // A timeout may have committed: no blind retry, no returned ownership.
    }
  }

  async renew(expected: LeaseSnapshot, lease: FabricHostLease): Promise<LeaseSnapshot> {
    this.#live(expected);
    const value = this.#encoded(lease, expected.incarnation);
    if (!sameOwner(expected, { lease, incarnation: expected.incarnation, revision: expected.revision })) {
      throw new LeaseLostError("Renew cannot change owner identity or incarnation");
    }
    if (lease.expiresAt <= Date.now()) throw new LeaseLostError("Cannot renew to an expired lease");
    // Fresh leader check helps diagnostics; only server-side CAS makes the check/write race safe.
    const current = await this.read(lease.id);
    if (!current || !sameOwner(expected, current) || current.revision !== expected.revision) {
      throw new LeaseLostError("Lease replaced before renewal");
    }
    try {
      const revision = await this.kv.update(keyFor(lease.id), value, expected.revision);
      // The predecessor's authority must not lapse while waiting for a publish acknowledgement.
      this.#live(expected);
      const result = { lease: JSON.parse(value).lease as FabricHostLease, incarnation: expected.incarnation, revision };
      this.#live(result);
      return result;
    } catch (error) {
      if (conflict(error)) throw new LeaseLostError("Lease replaced during renewal", { cause: error });
      throw error; // Uncertain commit: caller stops, retaining only its OLD deadline/token.
    }
  }

  async release(expected: LeaseSnapshot): Promise<boolean> {
    if (!Number.isSafeInteger(expected.revision) || expected.revision <= 0) throw new LeaseLostError("Invalid fencing token");
    const entry = await this.#entry(expected.lease.id), current = entry && this.#decode(entry);
    if (!current || !sameOwner(expected, current) || current.revision !== expected.revision) return false;
    try {
      await this.kv.delete(keyFor(expected.lease.id), { previousSeq: expected.revision });
      return true;
    } catch (error) { if (conflict(error)) return false; throw error; }
  }

  /** Authority reads use the stream leader API. Watch entries alone NEVER grant ownership. */
  async read(id: string): Promise<LeaseSnapshot | undefined> {
    const entry = await this.#entry(id), result = entry && this.#decode(entry);
    return result && result.lease.expiresAt > Date.now() ? result : undefined;
  }
  async list(): Promise<Map<string, LeaseSnapshot>> {
    const result = new Map<string, LeaseSnapshot>();
    const keys = await this.kv.keys();
    try {
      for await (const key of keys) {
        let entry: KvEntry | null;
        try { entry = await this.kv.get(key); } catch (error) { if (missing(error)) continue; throw error; }
        const current = entry && this.#decode(entry);
        if (current && current.lease.expiresAt > Date.now()) result.set(current.lease.id, current);
      }
    } finally { keys.stop(); }
    return result; // Per-key leader reads, not an atomic multi-key snapshot.
  }

  /** Watch before attempting acquisition: no lost release wakeup, no read polling/backoff loop. */
  async acquireWaiting(makeLease: () => FabricHostLease, incarnation: string,
    options: { waitMs: number; signal?: AbortSignal }): Promise<LeaseSnapshot> {
    if (!Number.isFinite(options.waitMs) || options.waitMs < 0) throw new Error("Invalid waitMs");
    options.signal?.throwIfAborted();
    const first = makeLease(), id = first.id;
    const deadline = performance.now() + options.waitMs;
    const watch = await this.kv.watch({ key: keyFor(id), ignoreDeletes: false });
    let notify: (() => void) | undefined, pending = false, watchError: unknown;
    let expiry: ReturnType<typeof setTimeout> | undefined;
    const wake = (): void => { pending = true; notify?.(); };
    const pump = (async () => {
      try {
        for await (const entry of watch) {
          if (expiry) clearTimeout(expiry);
          const current = this.#decode(entry);
          if (current) expiry = setTimeout(wake, Math.max(0, current.lease.expiresAt - Date.now()));
          wake(); // Treat entries only as hints: the next acquire rereads the leader.
        }
        watchError = new Error("Lease watcher ended"); wake();
      } catch (error) { watchError = error; wake(); }
    })();
    try {
      for (;;) {
        options.signal?.throwIfAborted();
        if (watchError) throw watchError;
        const lease = makeLease();
        if (lease.id !== id) throw new Error("Waiting lease key changed");
        const acquired = await this.acquire(lease, incarnation);
        if (acquired) return acquired;
        if (performance.now() >= deadline) throw new LeaseWaitTimeoutError(`Lease ${id} remained owned`);
        if (pending) { pending = false; continue; }
        let timer: ReturnType<typeof setTimeout> | undefined;
        let abort: (() => void) | undefined;
        try {
          await new Promise<void>((resolve, reject) => {
            notify = resolve;
            timer = setTimeout(() => reject(new LeaseWaitTimeoutError(`Lease ${id} wait timed out`)),
              Math.max(0, deadline - performance.now()));
            abort = () => reject(options.signal?.reason ?? new Error("Lease wait aborted"));
            options.signal?.addEventListener("abort", abort, { once: true });
            if (options.signal?.aborted) abort();
          });
        } finally {
          if (timer) clearTimeout(timer);
          if (abort) options.signal?.removeEventListener("abort", abort);
          notify = undefined;
        }
        pending = false;
      }
    } finally {
      if (expiry) clearTimeout(expiry);
      watch.stop();
      await pump;
    }
  }
}
