import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import fs from "node:fs";
import type { MeshIdentity, MeshStateEntry, MeshStore } from "../mesh/store.js";
import { hostLeasePath, readHostLeaseCurrent, readHostLeases, removeHostLeaseIf, sameHostLeaseVersion, writeHostLease,
  TRANSPORT_HOST_LEASE_TTL_MS, type FabricHostLease } from "./host-leases.js";
import { HostLeaseLockBusyError, withHostLeaseLock, type HostLeaseLockOptions, type HostLeaseMesh } from "./host-lease-lock.js";
import { readParticipantFiles, removeParticipantFileIf, writeParticipantFileIf } from "./participant-files.js";
import { leaseValue, presenceValue } from "./participant-view.js";
import type { FabricKvHostLease, FabricParticipantRecord } from "./types.js";
import type { FabricTransport, TransportBucket, TransportKvEntry } from "../transport/types.js";

const PARTICIPANT_PREFIX = "topology/participants/";
const MARKER = "natsProjection";
const INITIAL_HOST_LEASE_LOCK_RETRY_MS = 10_000;
const HOST_LEASE_LOCK_RETRY_BASE_MS = 10;
const HOST_LEASE_LOCK_RETRY_MAX_MS = 250;
const markerFor = (entry: TransportKvEntry<unknown>): NatsProjectionMarker => {
  const record = entry.value as Record<string, unknown>;
  const incarnation = typeof record.ownerIncarnation === "string" ? record.ownerIncarnation :
    typeof record.incarnation === "string" ? record.incarnation : "";
  return { format: 1, machine: entry.machine, kvKey: entry.key, owner: entry.owner, incarnation, revision: entry.revision };
};

interface NatsProjectionMarker {
  format: 1;
  machine: string;
  kvKey: string;
  owner: string;
  incarnation: string;
  revision: number;
}

type ProjectedParticipantEntry = MeshStateEntry & { natsProjection: NatsProjectionMarker };
type ProjectedHostLease = FabricHostLease & { natsProjection: NatsProjectionMarker };
type BucketKind = "presence" | "leases";

const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const markerOf = (value: unknown): NatsProjectionMarker | undefined => {
  if (!object(value)) return undefined;
  const marker = value[MARKER];
  if (!object(marker) || marker.format !== 1 || typeof marker.machine !== "string" ||
    typeof marker.kvKey !== "string" || typeof marker.owner !== "string" ||
    typeof marker.incarnation !== "string" || !Number.isSafeInteger(marker.revision) ||
    (marker.revision as number) <= 0) return undefined;
  return marker as unknown as NatsProjectionMarker;
};
const sameMarker = (left: NatsProjectionMarker | undefined, right: NatsProjectionMarker): boolean =>
  left?.format === right.format && left.machine === right.machine && left.kvKey === right.kvKey &&
  left.owner === right.owner && left.incarnation === right.incarnation && left.revision === right.revision;
const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const participantKey = (id: string): string => `${PARTICIPANT_PREFIX}${hash(id)}`;
const participantIdentity = (record: FabricParticipantRecord): MeshIdentity => ({
  id: record.ownerIdentityId,
  name: record.name,
  kind: record.kind === "root" ? "main" : record.kind === "actor" ? "actor" : "agent",
});
const sameParticipant = (current: MeshStateEntry | undefined, record: FabricParticipantRecord): boolean =>
  current !== undefined && current.updatedBy.id === record.ownerIdentityId &&
  JSON.stringify(current.value) === JSON.stringify(record);
const asHostLease = (lease: FabricKvHostLease, marker: NatsProjectionMarker): ProjectedHostLease => ({
  id: lease.id,
  rootId: lease.rootId,
  identityId: lease.identityId,
  incarnationToken: lease.incarnation,
  updatedAt: lease.updatedAt,
  expiresAt: lease.expiresAt,
  [MARKER]: marker,
});

export interface NatsLocalProjectionMetrics {
  machine: string;
  elapsedMs: number;
  participantWrites: number;
  hostLeaseWrites: number;
  participantReconcileReads: number;
  hostLeaseReconcileReads: number;
  kvReconcileReads: number;
  participantWritesPerMinute: number;
  hostLeaseWritesPerMinute: number;
  participantReconcileReadsPerMinute: number;
  hostLeaseReconcileReadsPerMinute: number;
  kvReconcileReadsPerMinute: number;
}

export interface NatsLocalProjectionOptions {
  mesh: MeshStore;
  transport: FabricTransport;
  /** A live transport lease gates writes. Tombstones remain actionable after lease loss. */
  leaseLive: () => boolean;
  onError?: (error: unknown) => void;
  now?: () => number;
  monotonic?: () => number;
}

/**
 * A host-local, one-way projection of that machine's authenticated NATS KV rows.
 * KV revisions and owner/incarnation travel with each file so another adapter or an
 * older watch generation cannot overwrite a newer projection. NATS remains authoritative.
 */
export class NatsLocalProjection {
  readonly #presence;
  readonly #leases;
  readonly #abort = new AbortController();
  readonly #watchTasks: Promise<void>[] = [];
  readonly #ready = new Set<BucketKind>();
  readonly #initialReady: Promise<void>;
  #resolveInitial!: () => void;
  #rejectInitial!: (error: unknown) => void;
  readonly #participantKeysByKv = new Map<string, string>();
  readonly #hostLeaseIdsByKv = new Map<string, string>();
  readonly #leaseCache = new Map<string, { incarnation: string; expiresAt: number; revision: number; ino: number; mtimeMs: number; size: number }>();
  readonly #now: () => number;
  readonly #monotonic: () => number;
  readonly #startedAt: number;
  #participantWrites = 0;
  #hostLeaseWrites = 0;
  #participantReconcileReads = 0;
  #hostLeaseReconcileReads = 0;
  #kvReconcileReads = 0;
  #started = false;
  #closed = false;
  #initialReadyResolved = false;
  #initialLockRetryDeadline: number | undefined;
  #recoveryLockRetryDeadline: number | undefined;
  #resumingAfterLeaseRecovery: Promise<void> | undefined;
  readonly #pendingEntries = new Map<string, { bucket: BucketKind; entry: TransportKvEntry<unknown> }>();

  constructor(readonly options: NatsLocalProjectionOptions) {
    if (options.transport.kind !== "nats") throw new TypeError("NATS local projection requires mesh.transport=nats");
    this.#presence = options.transport.kv("presence");
    this.#leases = options.transport.kv("leases");
    this.#now = options.now ?? Date.now;
    this.#monotonic = options.monotonic ?? (() => performance.now());
    this.#startedAt = this.#monotonic();
    this.#initialReady = new Promise<void>((resolve, reject) => {
      this.#resolveInitial = resolve;
      this.#rejectInitial = reject;
    });
    void this.#initialReady.catch(() => undefined);
  }

  start(): void {
    if (this.#closed) throw new Error("NATS local projection is closed");
    if (this.#started) return;
    this.#started = true;
    this.#watchTasks.push(this.#watch("presence"), this.#watch("leases"));
  }

  whenReady(): Promise<void> {
    if (!this.#started) throw new Error("NATS local projection has not started");
    return this.#initialReady;
  }

  metrics(): NatsLocalProjectionMetrics {
    const elapsedMs = Math.max(1, this.#monotonic() - this.#startedAt);
    const perMinute = (count: number): number => count * 60_000 / elapsedMs;
    return {
      machine: this.options.transport.self.machine,
      elapsedMs,
      participantWrites: this.#participantWrites,
      hostLeaseWrites: this.#hostLeaseWrites,
      participantReconcileReads: this.#participantReconcileReads,
      hostLeaseReconcileReads: this.#hostLeaseReconcileReads,
      kvReconcileReads: this.#kvReconcileReads,
      participantWritesPerMinute: perMinute(this.#participantWrites),
      hostLeaseWritesPerMinute: perMinute(this.#hostLeaseWrites),
      participantReconcileReadsPerMinute: perMinute(this.#participantReconcileReads),
      hostLeaseReconcileReadsPerMinute: perMinute(this.#hostLeaseReconcileReads),
      kvReconcileReadsPerMinute: perMinute(this.#kvReconcileReads),
    };
  }

  async #watch(bucket: BucketKind): Promise<void> {
    const kv = bucket === "presence" ? this.#presence : this.#leases;
    const kind = bucket === "presence" ? "p" : "h";
    const prefix = `${this.options.transport.self.machine}.${kind}.>`;
    let retryCount = 0;
    let lastFailureAt = Number.NEGATIVE_INFINITY;
    while (!this.#closed) {
      let observed = new Set<string>();
      try {
        for await (const event of kv.watch<unknown>(prefix, this.#abort.signal)) {
          if (this.#closed) return;
          if (event.type === "reset") {
            observed = new Set();
            for (const [key, pending] of this.#pendingEntries) if (pending.bucket === bucket) this.#pendingEntries.delete(key);
            this.#ready.delete(bucket);
            continue;
          }
          if (event.type === "ready") {
            await this.#reconcile(bucket, observed);
            this.#ready.add(bucket);
            if (this.#ready.size === 2 && !this.#initialReadyResolved) {
              this.#initialReadyResolved = true;
              this.#resolveInitial();
            }
            continue;
          }
          if (event.type === "put") {
            observed.add(event.entry.key);
            await this.#put(bucket, event.entry);
          } else {
            observed.delete(event.key);
            await this.#gone(bucket, event.key, event.revision);
          }
        }
        if (this.#closed) return;
        throw new Error(`NATS ${bucket} projection watch ended`);
      } catch (error) {
        if (this.#closed) return;
        try { this.options.onError?.(error); } catch { /* diagnostic only */ }
        if (!this.#initialReadyResolved) {
          this.#rejectInitial(error);
          return;
        }
        this.#ready.delete(bucket);
        const failedAt = this.#monotonic();
        if (failedAt - lastFailureAt >= 30_000) retryCount = 0;
        const delayMs = Math.min(5_000, 100 * 2 ** Math.min(retryCount, 6));
        retryCount = Math.min(retryCount + 1, 6);
        lastFailureAt = failedAt;
        await this.#waitBeforeRetry(delayMs);
      }
    }
  }

  #waitBeforeRetry(delayMs: number): Promise<void> {
    return new Promise(resolve => {
      const signal = this.#abort.signal;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (): void => {
        if (timer !== undefined) clearTimeout(timer);
        signal.removeEventListener("abort", finish);
        resolve();
      };
      signal.addEventListener("abort", finish, { once: true });
      timer = setTimeout(finish, delayMs);
      if (signal.aborted) finish();
    });
  }

  async #withHostLeaseLock<T>(file: string, operation: () => T, options: HostLeaseLockOptions = {}): Promise<T> {
    let attempt = 0;
    for (;;) {
      if (!this.#closed) this.#abort.signal.throwIfAborted();
      try {
        return await withHostLeaseLock(this.options.mesh as unknown as HostLeaseMesh, file, operation,
          { ...options, ...(this.#closed ? {} : { signal: this.#abort.signal }) });
      } catch (error) {
        if (this.#closed || this.#initialReadyResolved && this.#recoveryLockRetryDeadline === undefined ||
          !(error instanceof HostLeaseLockBusyError)) throw error;
        const retryDeadline = this.#initialReadyResolved ? this.#recoveryLockRetryDeadline! :
          this.#initialLockRetryDeadline ??= performance.now() + INITIAL_HOST_LEASE_LOCK_RETRY_MS;
        const remaining = retryDeadline - performance.now();
        if (remaining <= 0) throw error;
        const backoff = Math.min(HOST_LEASE_LOCK_RETRY_MAX_MS,
          HOST_LEASE_LOCK_RETRY_BASE_MS * 2 ** Math.min(attempt, 5));
        const delay = Math.min(remaining, Math.floor(backoff * (0.5 + Math.random())));
        attempt += 1;
        await this.#waitBeforeRetry(delay);
      }
    }
  }

  async #put(bucket: BucketKind, entry: TransportKvEntry<unknown>): Promise<void> {
    if (entry.machine !== this.options.transport.self.machine) return;
    const value = bucket === "presence" ? presenceValue(entry) : leaseValue(entry);
    const pendingKey = `${bucket}:${entry.key}`;
    if (!value) {
      this.#pendingEntries.delete(pendingKey);
      await this.#remove(bucket, entry.key, entry.revision);
      return;
    }
    const pending = { bucket, entry };
    this.#pendingEntries.set(pendingKey, pending);
    if (!this.options.leaseLive()) return;
    if (bucket === "presence") await this.#writeParticipant(entry, value as FabricParticipantRecord);
    else await this.#writeHostLease(entry, value as FabricKvHostLease);
    if (this.options.leaseLive() && this.#pendingEntries.get(pendingKey) === pending) this.#pendingEntries.delete(pendingKey);
  }

  async #gone(bucket: BucketKind, key: string, revision: number): Promise<void> {
    if (!key.startsWith(`${this.options.transport.self.machine}.${bucket === "presence" ? "p" : "h"}.`)) return;
    this.#pendingEntries.delete(`${bucket}:${key}`);
    await this.#remove(bucket, key, revision);
  }

  /** Replay snapshot rows deferred while this process's own NATS lease was suspended. */
  resumeAfterLeaseRecovery(): Promise<void> {
    if (this.#closed || !this.options.leaseLive()) return Promise.resolve();
    if (this.#resumingAfterLeaseRecovery) return this.#resumingAfterLeaseRecovery;
    this.#recoveryLockRetryDeadline = performance.now() + INITIAL_HOST_LEASE_LOCK_RETRY_MS;
    const work = (async () => {
      const failures: unknown[] = [];
      for (const [key, pending] of [...this.#pendingEntries]) {
        if (this.#closed || !this.options.leaseLive()) break;
        if (this.#pendingEntries.get(key) !== pending) continue;
        try {
          const kv = pending.bucket === "presence" ? this.#presence : this.#leases;
          const current = await kv.get<unknown>(pending.entry.key, AbortSignal.timeout(5_000));
          if (this.#closed || !this.options.leaseLive()) break;
          if (this.#pendingEntries.get(key) !== pending) continue;
          if (!current) {
            await this.#remove(pending.bucket, pending.entry.key, pending.entry.revision, markerFor(pending.entry));
            if (this.#pendingEntries.get(key) === pending) this.#pendingEntries.delete(key);
            continue;
          }
          await this.#put(pending.bucket, current);
        } catch (error) { failures.push(error); }
      }
      if (failures.length) throw new AggregateError(failures, "NATS local projection lease recovery failed");
    })().finally(() => {
      if (this.#resumingAfterLeaseRecovery === work) this.#resumingAfterLeaseRecovery = undefined;
      this.#recoveryLockRetryDeadline = undefined;
    });
    this.#resumingAfterLeaseRecovery = work;
    return work;
  }

  async #writeParticipant(entry: TransportKvEntry<unknown>, record: FabricParticipantRecord): Promise<void> {
    const key = participantKey(record.id);
    if (record.ownerIncarnation === undefined || entry.owner !== `${record.ownerHostId}#${record.ownerIncarnation}`) return;
    this.#participantKeysByKv.set(entry.key, participantKey(record.id));
    const marker = markerFor(entry);
    let wrote = false;
    await writeParticipantFileIf(this.options.mesh, key, current => {
      this.#participantReconcileReads++;
      if (!this.options.leaseLive()) return undefined;
      const currentMarker = markerOf(current);
      if (current && object(current.value) && current.value.remoteHost !== undefined) {
        // A local KV projection never claims a bridge-owned file.
        return undefined;
      }
      if (currentMarker?.machine === marker.machine && currentMarker.kvKey === marker.kvKey) {
        if (currentMarker.revision > marker.revision) return undefined;
        if (sameParticipant(current, record)) {
          if (currentMarker.revision === marker.revision) return undefined;
          const projected: ProjectedParticipantEntry = {
            key, value: record, version: entry.revision, updatedAt: record.updatedAt,
            updatedBy: participantIdentity(record), [MARKER]: marker,
          };
          wrote = true;
          return projected;
        }
      } else if (current) {
        if (sameParticipant(current, record)) return undefined;
        const currentValue = object(current.value) ? current.value : undefined;
        const currentIncarnation = typeof currentValue?.ownerIncarnation === "string"
          ? currentValue.ownerIncarnation : currentMarker?.incarnation;
        if (current.updatedAt >= record.updatedAt && currentIncarnation !== record.ownerIncarnation) return undefined;
      }
      const projected: ProjectedParticipantEntry = {
        key, value: record, version: entry.revision, updatedAt: record.updatedAt,
        updatedBy: participantIdentity(record), [MARKER]: marker,
      };
      wrote = true;
      return projected;
    });
    if (wrote) this.#participantWrites++;
  }

  #readHostMarker(hostId: string): NatsProjectionMarker | undefined {
    try {
      return markerOf(JSON.parse(fs.readFileSync(hostLeasePath(this.options.mesh.root, hostId), "utf8")));
    } catch { return undefined; }
  }

  async #writeHostLease(entry: TransportKvEntry<unknown>, lease: FabricKvHostLease): Promise<void> {
    if (entry.owner !== `${lease.id}#${lease.incarnation}`) return;
    this.#hostLeaseIdsByKv.set(entry.key, lease.id);
    const marker = markerFor(entry);
    const projected = asHostLease(lease, marker);
    const file = hostLeasePath(this.options.mesh.root, lease.id);
    const cached = this.#leaseCache.get(lease.id);
    if (cached?.incarnation === lease.incarnation && cached.revision >= marker.revision &&
      cached.expiresAt - this.#now() > TRANSPORT_HOST_LEASE_TTL_MS / 2 && lease.expiresAt >= cached.expiresAt) {
      this.#hostLeaseReconcileReads++;
      try {
        const stat = fs.statSync(file);
        if (stat.ino === cached.ino && stat.mtimeMs === cached.mtimeMs && stat.size === cached.size) return;
      } catch { /* Missing or unreadable means the locked writer path must inspect it. */ }
    }
    const rememberLease = (incarnation: string, expiresAt: number, revision: number): void => {
      try {
        const stat = fs.statSync(file);
        this.#leaseCache.set(lease.id, { incarnation, expiresAt, revision, ino: stat.ino, mtimeMs: stat.mtimeMs, size: stat.size });
      } catch { this.#leaseCache.delete(lease.id); }
    };
    let wrote = false;
    await this.#withHostLeaseLock(file, () => {
      this.#hostLeaseReconcileReads++;
      if (!this.options.leaseLive()) return;
      const current = readHostLeaseCurrent(this.options.mesh.root, lease.id);
      const currentMarker = this.#readHostMarker(lease.id);
      if (fs.existsSync(file) && !current) return; // Invalid/unreadable bytes are not an empty slot.
      const sameSource = currentMarker?.machine === marker.machine && currentMarker.kvKey === marker.kvKey;
      const rememberCurrent = (): void => {
        if (!current) return;
        if (sameSource && currentMarker && currentMarker.revision < marker.revision) {
          // Persist the latest source revision even when the lease payload is coalesced.
          // Another projection can then fence an older tombstone without relying on this
          // process's in-memory watch history.
          const currentProjection: ProjectedHostLease = { ...current, [MARKER]: marker };
          writeHostLease(this.options.mesh.root, currentProjection);
          rememberLease(lease.incarnation, current.expiresAt, marker.revision);
          wrote = true;
        } else rememberLease(lease.incarnation, current.expiresAt, currentMarker?.revision ?? 0);
      };
      if (sameSource && currentMarker!.revision > marker.revision) { rememberCurrent(); return; }
      if (current?.incarnationToken === lease.incarnation && current.rootId === lease.rootId &&
        current.identityId === lease.identityId && current.updatedAt >= lease.updatedAt && current.expiresAt >= lease.expiresAt) {
        rememberCurrent();
        return;
      }
      if (!sameSource && current && current.incarnationToken !== lease.incarnation && current.updatedAt >= lease.updatedAt) return;
      // Renew only when the local reader's lease would otherwise lose its live window.
      if (current?.incarnationToken === lease.incarnation && current.expiresAt - this.#now() > TRANSPORT_HOST_LEASE_TTL_MS / 2 &&
        lease.expiresAt > current.expiresAt) {
        rememberCurrent();
        return;
      }
      writeHostLease(this.options.mesh.root, projected);
      rememberLease(lease.incarnation, lease.expiresAt, marker.revision);
      wrote = true;
    }, { lease: { incarnationToken: lease.incarnation, expiresAt: lease.expiresAt } });
    if (wrote) this.#hostLeaseWrites++;
  }

  async #remove(bucket: BucketKind, key: string, revision: number, expected?: NatsProjectionMarker): Promise<void> {
    // A delayed tombstone or stale snapshot may race a delete-and-recreate. KV is
    // authoritative, so confirm the current key before removing its local read model.
    this.#kvReconcileReads++;
    const currentEntry = await (bucket === "presence" ? this.#presence : this.#leases)
      .get<unknown>(key, AbortSignal.timeout(5_000));
    if (currentEntry) {
      const currentValue = bucket === "presence" ? presenceValue(currentEntry) : leaseValue(currentEntry);
      if (currentValue) {
        await this.#put(bucket, currentEntry);
        return;
      }
      if (currentEntry.revision < revision) return;
      if (currentEntry.revision > revision) revision = currentEntry.revision;
    }
    // Tombstoned or absent keys cannot own a cached lease anymore, even if another
    // writer already removed the local file and the locked removal is therefore a no-op.
    if (bucket === "leases") {
      const hostId = this.#hostLeaseIdsByKv.get(key);
      if (hostId) this.#leaseCache.delete(hostId);
    }
    if (bucket === "presence") {
      if (!key.split(".")[2]) return;
      // The marker stores the original KV key, avoiding any reverse hash lookup.
      let localKey = this.#participantKeysByKv.get(key);
      if (!localKey) {
        this.#participantReconcileReads++;
        const candidate = readParticipantFiles(this.options.mesh.root, { maxAgeMs: 0 }).find(entry => {
          const marker = markerOf(entry);
          return marker?.kvKey === key && marker.machine === this.options.transport.self.machine;
        });
        if (!candidate) return;
        localKey = candidate.key;
        this.#participantKeysByKv.set(key, localKey);
      }
      const removed = await removeParticipantFileIf(this.options.mesh, localKey, current => {
        this.#participantReconcileReads++;
        const latest = markerOf(current);
        return latest?.machine === this.options.transport.self.machine && latest.kvKey === key &&
          (expected ? sameMarker(latest, expected) : latest.revision < revision);
      });
      if (removed) this.#participantWrites++;
      this.#participantKeysByKv.delete(key);
      return;
    }
    const keyToken = key.split(".")[2];
    if (!keyToken) return;
    let hostId = this.#hostLeaseIdsByKv.get(key);
    if (!hostId) {
      this.#hostLeaseReconcileReads++;
      const leases = readHostLeases(this.options.mesh.root, []);
      hostId = [...leases.values()].find(value => hostLeasePath(this.options.mesh.root, value.id).endsWith(`${keyToken}.json`))?.id;
      if (!hostId) return;
      this.#hostLeaseIdsByKv.set(key, hostId);
    }
    this.#hostLeaseReconcileReads++;
    const lease = readHostLeaseCurrent(this.options.mesh.root, hostId);
    if (!lease) { this.#hostLeaseIdsByKv.delete(key); return; }
    const file = hostLeasePath(this.options.mesh.root, hostId);
    let removed = false;
    await this.#withHostLeaseLock(file, () => {
      this.#hostLeaseReconcileReads++;
      const current = readHostLeaseCurrent(this.options.mesh.root, lease.id);
      const marker = this.#readHostMarker(lease.id);
      if (!current || !marker || marker.machine !== this.options.transport.self.machine || marker.kvKey !== key ||
        (expected ? !sameMarker(marker, expected) : marker.revision >= revision) ||
        current.incarnationToken !== marker.incarnation || current.id !== lease.id) return;
      removed = removeHostLeaseIf(this.options.mesh.root, lease.id, value => sameHostLeaseVersion(value, current));
      if (removed) this.#leaseCache.delete(lease.id);
    }, { lease: { incarnationToken: lease.incarnationToken ?? "nats-projection", expiresAt: lease.expiresAt } });
    if (removed) this.#hostLeaseWrites++;
    this.#hostLeaseIdsByKv.delete(key);
  }

  async #reconcile(bucket: BucketKind, observed: ReadonlySet<string>): Promise<void> {
    if (bucket === "presence") {
      // Rebuild this lookup from the current local read model after each snapshot;
      // it is an index of extant projections, not a process-lifetime key history.
      this.#participantKeysByKv.clear();
      this.#participantReconcileReads++;
      for (const local of readParticipantFiles(this.options.mesh.root, { maxAgeMs: 0 })) {
        const marker = markerOf(local);
        if (!marker || marker.machine !== this.options.transport.self.machine) continue;
        this.#participantKeysByKv.set(marker.kvKey, local.key);
        if (observed.has(marker.kvKey)) continue;
        this.#participantReconcileReads++;
        this.#kvReconcileReads++;
        let current: TransportKvEntry<unknown> | undefined;
        current = await this.#presence.get<unknown>(marker.kvKey, AbortSignal.timeout(5_000));
        if (current) {
          const value = presenceValue(current);
          if (value) { if (!this.#closed) await this.#put("presence", current); continue; }
          await this.#remove("presence", marker.kvKey, current.revision);
        } else await this.#remove("presence", marker.kvKey, 0, marker);
      }
      return;
    }
    const localLeases = readHostLeases(this.options.mesh.root, []);
    const localHostIds = new Set(localLeases.keys());
    this.#hostLeaseIdsByKv.clear();
    for (const hostId of this.#leaseCache.keys()) if (!localHostIds.has(hostId)) this.#leaseCache.delete(hostId);
    this.#hostLeaseReconcileReads++;
    for (const local of localLeases.values()) {
      const marker = this.#readHostMarker(local.id);
      if (!marker || marker.machine !== this.options.transport.self.machine) continue;
      this.#hostLeaseIdsByKv.set(marker.kvKey, local.id);
      if (observed.has(marker.kvKey)) continue;
      this.#hostLeaseReconcileReads++;
      this.#kvReconcileReads++;
      let current: TransportKvEntry<unknown> | undefined;
      current = await this.#leases.get<unknown>(marker.kvKey, AbortSignal.timeout(5_000));
      if (current) {
        const value = leaseValue(current);
        if (value) { if (!this.#closed) await this.#put("leases", current); continue; }
        await this.#remove("leases", marker.kvKey, current.revision);
      } else await this.#remove("leases", marker.kvKey, 0, marker);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#abort.abort();
    await Promise.allSettled(this.#watchTasks);
    await this.#resumingAfterLeaseRecovery?.catch(() => undefined);
    // Shutdown follows KV withdrawal. Re-read before cleanup because an abort can overtake
    // queued tombstones; a failed get retains the local file rather than guessing absence.
    await Promise.all([this.#reconcile("presence", new Set()), this.#reconcile("leases", new Set())]);
    this.#pendingEntries.clear();
  }
}
