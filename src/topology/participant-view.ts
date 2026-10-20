// src/topology/participant-view.ts (A2 owns; A1 imports only this)
export interface ParticipantRoute { machine: string; ownerHostId: string; ownerIdentityId: string; ownerIncarnation: string;
  capabilities: readonly string[]; live: boolean }
export interface ParticipantView {
  /** Throws DirectoryNotReadyError before the presence watch is ready. undefined = known absent. */
  resolve(participantId: string): ParticipantRoute | undefined;
  /** Waits (bounded by signal) for ready, or for a put/gone on that id. */
  whenChanged(participantId: string, signal: AbortSignal): Promise<void>;
  /** The host lease is live by the local monotonic window (§4.2). */
  leaseLive(): boolean;
}

import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";
import {
  DirectoryNotReadyError,
  TransportInvalidError,
  TransportUnavailableError,
  type FabricTransport,
  type TransportBucket,
  type TransportKv,
  type TransportKvEntry,
} from "../transport/types.js";
import type { FabricKvHostLease, FabricParticipantInfo, FabricParticipantListOptions, FabricParticipantRecord } from "./types.js";
import { isPlacementRunId } from "./placement-run-id.js";

/** The lease task owns acquisition, monotonic expiry, successor evidence and release. */
export interface ParticipantLeaseGate {
  live(): boolean;
  readonly signal: AbortSignal;
}

export interface KvParticipantDirectoryOptions {
  transport: FabricTransport;
  lease: ParticipantLeaseGate;
  identityId: string;
  ownerIncarnation: string;
  rootId: string;
  /** Bounds each KV operation, including reconciliation and best-effort cleanup. */
  operationTimeoutMs?: number;
  onError?: (error: unknown) => void;
}

/** Only this exact admission revision may be rolled back; a later change is never deleted. */
export interface KvPresencePublication {
  readonly key: string;
  readonly revision: number;
  rollback(): Promise<boolean>;
}

export const KV_PRESENCE_TTL_MS = 600_000;
export const KV_LEASE_TTL_MS = 15_000;
export const KV_PRESENCE_REFRESH_MS = 180_000;
const MACHINE_TOKEN = /^[a-z0-9][a-z0-9-]{0,31}$/;
const KEY_TOKEN = /^[a-f0-9]{32}$/;
const RUNNER_TOKEN = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/;
const KV_CAPABILITIES = new Set(["steer", "followUp", "stop", "ask", "actor-bindings", "attach", "fabric"]);
const KV_TRANSPORTS = new Set(["host", "hosted", "auto", "process", "sandbox", "tmux", "screen", "localterm", "herdr"]);
const KV_TERMINAL = new Set(["completed", "failed", "cancelled", "stopped", "timed_out", "done", "aborted"]);
const tokenFor = (id: string): string => createHash("sha256").update(id).digest("hex").slice(0, 32);
const objectValue = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const textValue = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const finiteValue = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const errorCode = (error: unknown): unknown => objectValue(error) ? error.code : undefined;
const uncertain = (error: unknown): boolean => errorCode(error) === "FABRIC_TRANSPORT_UNCERTAIN";
const cloneJson = <T>(value: T): T => JSON.parse(JSON.stringify(value)) as T;
const freezeJson = <T>(value: T): T => {
  if (typeof value === "object" && value !== null) {
    Object.freeze(value);
    for (const child of Object.values(value)) freezeJson(child);
  }
  return value;
};
const canonicalJson = (value: unknown): string => JSON.stringify(value, (_key, child: unknown) => {
  if (!objectValue(child)) return child;
  return Object.fromEntries(Object.keys(child).sort().map(key => [key, child[key]]));
}) ?? "";

/** Key tokens, not entry.machine or JSON fields, establish the authenticated machine. */
function machineFromKey(key: string, kind: "p" | "h"): string | undefined {
  const parts = key.split(".");
  return parts.length === 3 && MACHINE_TOKEN.test(parts[0]!) && parts[1] === kind && KEY_TOKEN.test(parts[2]!)
    ? parts[0] : undefined;
}

export function presenceValue(entry: TransportKvEntry<unknown>): FabricParticipantRecord | undefined {
  const value = entry.value;
  if (!machineFromKey(entry.key, "p") || !Number.isSafeInteger(entry.revision) || entry.revision <= 0 ||
    !objectValue(value) || value.format !== 1 || !textValue(value.id) ||
    entry.key.split(".")[2] !== tokenFor(value.id) ||
    typeof value.kind !== "string" || !["root", "agent", "actor", "provider"].includes(value.kind) ||
    !textValue(value.rootId) || !textValue(value.ownerHostId) || !textValue(value.ownerIdentityId) ||
    !textValue(value.ownerIncarnation) || value.ownerIncarnation.length > 128 ||
    entry.owner !== `${value.ownerHostId}#${value.ownerIncarnation}` ||
    typeof value.name !== "string" || !textValue(value.status) || value.controlProtocol !== "v1" ||
    typeof value.transport !== "string" || !KV_TRANSPORTS.has(value.transport) ||
    (value.kind === "provider" ? !textValue(value.provider) || value.runner !== undefined :
      typeof value.runner !== "string" || value.runner.length > 64 || !RUNNER_TOKEN.test(value.runner)) ||
    !Array.isArray(value.capabilities) || !value.capabilities.every(capability =>
      typeof capability === "string" && KV_CAPABILITIES.has(capability)) ||
    !finiteValue(value.startedAt) || !finiteValue(value.updatedAt) ||
    (value.interactive !== undefined && typeof value.interactive !== "boolean") ||
    !["sessionId", "cwd", "label", "role", "project", "projectRoot", "repository", "model", "thinking", "parentId", "finalAnswerReceiptId", "placementRunId"]
      .every(key => value[key] === undefined || typeof value[key] === "string") ||
    value.placementRunId !== undefined && !isPlacementRunId(value.placementRunId) ||
    value.remoteHost !== undefined || value.bridgeVia !== undefined || value.stopReason !== undefined) return undefined;
  return value as unknown as FabricParticipantRecord;
}

export function leaseValue(entry: TransportKvEntry<unknown>): FabricKvHostLease | undefined {
  const value = entry.value;
  if (!machineFromKey(entry.key, "h") || !Number.isSafeInteger(entry.revision) || entry.revision <= 0 ||
    !objectValue(value) || !textValue(value.id) || entry.key.split(".")[2] !== tokenFor(value.id) ||
    !textValue(value.rootId) || !textValue(value.identityId) || !textValue(value.incarnation) ||
    value.incarnation.length > 128 || entry.owner !== `${value.id}#${value.incarnation}` ||
    !Number.isSafeInteger(value.pid) || (value.pid as number) <= 0 ||
    !textValue(value.pidStartTime) || !textValue(value.bootId) ||
    !finiteValue(value.updatedAt) || !finiteValue(value.expiresAt) ||
    value.mirror !== undefined || value.mirrorLease !== undefined) return undefined;
  return value as unknown as FabricKvHostLease;
}

type OwnedPresence = { record: FabricParticipantRecord; revision: number; writtenAt: number; refreshAt: number };
type PresenceWaiter = { id: string; changed(): void; fail(error: unknown): void };

/**
 * Standalone NATS-only carrier. It does not read a MeshStore, import FILE participants,
 * mirror bridge records, acquire a lease, or launch a worker. Parent composition must
 * select this carrier INSTEAD OF the FILE directory's lifecycle/publication path.
 */
export class KvParticipantDirectory implements ParticipantView {
  readonly #presence: TransportKv;
  readonly #leases: TransportKv;
  readonly #abort = new AbortController();
  readonly #records = new Map<string, TransportKvEntry<FabricParticipantRecord>>();
  readonly #hosts = new Map<string, TransportKvEntry<FabricKvHostLease>>();
  readonly #presenceRevisions = new Map<string, number>();
  readonly #leaseRevisions = new Map<string, number>();
  readonly #owned = new Map<string, OwnedPresence>();
  readonly #pendingCleanup = new Map<string, FabricParticipantRecord>();
  readonly #writes = new Map<string, Promise<unknown>>();
  readonly #waiters = new Set<PresenceWaiter>();
  readonly #timeoutMs: number;
  #presenceReady = false;
  #leasesReady = false;
  #started = false;
  #closed = false;
  #watchFailed = false;
  #watchTasks: Promise<void>[] = [];
  #timer: ReturnType<typeof setTimeout> | undefined;
  #refreshPass: Promise<void> | undefined;
  #closing: Promise<void> | undefined;
  #flushing: Promise<void> | undefined;
  #cleanupAgain = false;
  readonly #leaseLost = (): void => this.#stop(this.options.lease.signal.reason ?? new DirectoryNotReadyError());

  constructor(readonly options: KvParticipantDirectoryOptions) {
    const { transport } = options;
    if (transport.kind !== "nats" || !MACHINE_TOKEN.test(transport.self.machine) ||
      !textValue(transport.self.hostId) || !textValue(options.identityId) ||
      !textValue(options.ownerIncarnation) || options.ownerIncarnation.length > 128 || !textValue(options.rootId)) {
      throw new TransportInvalidError("Invalid KV participant carrier identity");
    }
    this.#presence = transport.kv("presence");
    this.#leases = transport.kv("leases");
    if (this.#presence.ttlMs !== KV_PRESENCE_TTL_MS || this.#leases.ttlMs !== KV_LEASE_TTL_MS) {
      throw new TransportInvalidError("KV presence requires TTL 600000; leases requires TTL 15000");
    }
    const timeout = options.operationTimeoutMs ?? 5_000;
    if (!Number.isFinite(timeout) || timeout < 1 || timeout > 30_000) throw new TransportInvalidError("Invalid KV operation timeout");
    this.#timeoutMs = timeout;
  }

  /** Starts both explicit snapshot watches. Readiness is observed through resolve/whenChanged. */
  start(): void {
    if (this.#closed) throw new DirectoryNotReadyError();
    if (this.#started) return;
    this.options.lease.signal.throwIfAborted();
    this.#started = true;
    this.options.lease.signal.addEventListener("abort", this.#leaseLost, { once: true });
    this.#watchTasks = [this.#watch("presence"), this.#watch("leases")];
  }

  get ready(): boolean {
    return this.#started && !this.#closed && !this.#watchFailed && this.#presenceReady && this.#leasesReady;
  }

  leaseLive(): boolean {
    try { return !this.#closed && !this.options.lease.signal.aborted && this.options.lease.live(); }
    catch { return false; }
  }

  #assertReady(): void { if (!this.ready) throw new DirectoryNotReadyError(); }
  #assertWritable(): void {
    if (!this.#started || !this.leaseLive()) throw new TransportUnavailableError("Participant owner lease is not live");
  }
  #signal(cleanup = false): AbortSignal {
    return cleanup ? AbortSignal.timeout(this.#timeoutMs) :
      AbortSignal.any([this.#abort.signal, this.options.lease.signal, AbortSignal.timeout(this.#timeoutMs)]);
  }
  #report(error: unknown): void { try { this.options.onError?.(error); } catch { /* diagnostic only */ } }

  #stop(reason: unknown): void {
    this.#closed = true;
    this.#presenceReady = this.#leasesReady = false;
    this.#records.clear();
    this.#hosts.clear();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.options.lease.signal.removeEventListener("abort", this.#leaseLost);
    this.#abort.abort(reason);
    for (const waiter of [...this.#waiters]) waiter.fail(reason);
  }

  async #watch(bucket: TransportBucket): Promise<void> {
    const presence = bucket === "presence";
    const kv = presence ? this.#presence : this.#leases;
    try {
      for await (const event of kv.watch<unknown>(">", this.#abort.signal)) {
        if (this.#closed) break;
        if (event.type === "reset") {
          if (presence) { this.#presenceReady = false; this.#records.clear(); this.#presenceRevisions.clear(); }
          else { this.#leasesReady = false; this.#hosts.clear(); this.#leaseRevisions.clear(); }
          continue;
        }
        if (event.type === "ready") {
          const wasReady = this.ready;
          if (presence) this.#presenceReady = true; else this.#leasesReady = true;
          if (!wasReady && this.ready) for (const waiter of [...this.#waiters]) waiter.changed();
          if (presence) this.#requestCleanup();
          continue;
        }
        const key = event.type === "put" ? event.entry.key : event.key;
        const revision = event.type === "put" ? event.entry.revision : event.revision;
        if (!machineFromKey(key, presence ? "p" : "h") || !Number.isSafeInteger(revision) || revision <= 0) continue;
        const revisions = presence ? this.#presenceRevisions : this.#leaseRevisions;
        if (revision <= (revisions.get(key) ?? 0)) continue;
        revisions.set(key, revision);
        if (presence) {
          this.#records.delete(key);
          if (event.type === "put") {
            const record = presenceValue(event.entry);
            if (record) this.#records.set(key, { ...event.entry, value: freezeJson(cloneJson(record)) });
          }
        } else {
          this.#hosts.delete(key);
          if (event.type === "put") {
            const lease = leaseValue(event.entry);
            if (lease) this.#hosts.set(key, { ...event.entry, value: freezeJson(cloneJson(lease)) });
          }
        }
        for (const waiter of [...this.#waiters]) {
          if (presence ? key.endsWith(`.p.${tokenFor(waiter.id)}`) :
            [...this.#records.values()].some(entry => entry.value.id === waiter.id &&
              this.#hostKey(entry) === key)) waiter.changed();
        }
        if (presence && this.#pendingCleanup.has(key)) this.#requestCleanup();
      }
    } catch (error) {
      if (!this.#closed) this.#report(error);
    } finally {
      if (presence) { this.#presenceReady = false; this.#records.clear(); }
      else { this.#leasesReady = false; this.#hosts.clear(); }
      if (!this.#closed) {
        this.#watchFailed = true;
        for (const waiter of [...this.#waiters]) waiter.fail(new DirectoryNotReadyError());
      }
    }
  }

  #hostKey(entry: TransportKvEntry<FabricParticipantRecord>): string {
    return `${machineFromKey(entry.key, "p")!}.h.${tokenFor(entry.value.ownerHostId)}`;
  }

  #entry(id: string): TransportKvEntry<FabricParticipantRecord> | undefined {
    // The same id on multiple authenticated machines is ambiguous, never source authority.
    const matches = [...this.#records.values()].filter(entry => entry.value.id === id);
    return matches.length === 1 ? matches[0] : undefined;
  }

  #route(entry: TransportKvEntry<FabricParticipantRecord>): ParticipantRoute {
    const record = entry.value;
    const host = this.#hosts.get(this.#hostKey(entry));
    const lease = host?.value;
    const machine = machineFromKey(entry.key, "p")!;
    const ownHost = machine === this.options.transport.self.machine && record.ownerHostId === this.options.transport.self.hostId;
    return {
      machine, ownerHostId: record.ownerHostId, ownerIdentityId: record.ownerIdentityId,
      ownerIncarnation: record.ownerIncarnation!, capabilities: [...record.capabilities],
      live: !!lease && machineFromKey(host!.key, "h") === machine &&
        lease.id === record.ownerHostId && lease.identityId === record.ownerIdentityId &&
        lease.rootId === record.rootId && lease.incarnation === record.ownerIncarnation &&
        host!.owner === entry.owner && !KV_TERMINAL.has(record.status) &&
        (!ownHost || record.ownerIncarnation === this.options.ownerIncarnation && this.leaseLive()),
    };
  }

  resolve(participantId: string): ParticipantRoute | undefined {
    this.#assertReady();
    const entry = this.#entry(participantId);
    return entry ? this.#route(entry) : undefined;
  }

  whenChanged(participantId: string, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    if (this.#closed || this.#watchFailed) return Promise.reject(new DirectoryNotReadyError());
    return new Promise<void>((resolve, reject) => {
      const finish = (error?: unknown): void => {
        this.#waiters.delete(waiter);
        signal.removeEventListener("abort", abort);
        if (error !== undefined) reject(error); else resolve();
      };
      const abort = (): void => finish(signal.reason ?? new DOMException("Aborted", "AbortError"));
      const waiter: PresenceWaiter = { id: participantId, changed: () => finish(), fail: error => finish(error) };
      this.#waiters.add(waiter);
      signal.addEventListener("abort", abort, { once: true });
    });
  }

  /** KV-only closed cohort, including stale rows only when explicitly requested. */
  list(options: FabricParticipantListOptions = {}): FabricParticipantInfo[] {
    this.#assertReady();
    const result: FabricParticipantInfo[] = [];
    const unique = new Map<string, TransportKvEntry<FabricParticipantRecord> | undefined>();
    for (const entry of this.#records.values()) unique.set(entry.value.id, unique.has(entry.value.id) ? undefined : entry);
    for (const entry of unique.values()) {
      if (!entry) continue;
      const record = entry.value, route = this.#route(entry);
      const local = route.machine === this.options.transport.self.machine && record.ownerHostId === this.options.transport.self.hostId &&
        record.ownerIdentityId === this.options.identityId && record.ownerIncarnation === this.options.ownerIncarnation;
      if (!route.live && !options.includeStale || options.scope === "local" && !local ||
        options.scope === "lineage" && record.rootId !== this.options.rootId ||
        options.kinds && !options.kinds.includes(record.kind)) continue;
      result.push({ ...cloneJson(record), machine: route.machine, local, stale: !route.live });
    }
    return result.sort((a, b) => a.startedAt - b.startedAt || a.id.localeCompare(b.id));
  }

  get(id: string): FabricParticipantInfo | undefined {
    return this.list({ includeStale: false }).find(record => record.id === id);
  }

  #key(id: string): string { return `${this.options.transport.self.machine}.p.${tokenFor(id)}`; }
  #owner(): string { return `${this.options.transport.self.hostId}#${this.options.ownerIncarnation}`; }
  #ownRecord(record: FabricParticipantRecord): FabricParticipantRecord {
    const copy = freezeJson(cloneJson(record));
    if (!textValue(copy.id) || copy.ownerHostId !== this.options.transport.self.hostId || copy.ownerIdentityId !== this.options.identityId ||
      copy.ownerIncarnation !== this.options.ownerIncarnation || copy.rootId !== this.options.rootId ||
      !presenceValue({ key: this.#key(copy.id), machine: this.options.transport.self.machine,
        owner: this.#owner(), value: copy, revision: 1 })) throw new TransportInvalidError("Invalid owned KV participant record");
    return copy;
  }
  #same(entry: TransportKvEntry<unknown>, record: FabricParticipantRecord): boolean {
    return entry.key === this.#key(record.id) && entry.owner === this.#owner() && !!presenceValue(entry) &&
      canonicalJson(entry.value) === canonicalJson(record);
  }
  #serial<T>(key: string, work: () => Promise<T>): Promise<T> {
    const previous = this.#writes.get(key) ?? Promise.resolve();
    const result = previous.catch(() => undefined).then(work);
    this.#writes.set(key, result);
    void result.finally(() => { if (this.#writes.get(key) === result) this.#writes.delete(key); }).catch(() => undefined);
    return result;
  }
  #remember(key: string, record: FabricParticipantRecord, revision: number): KvPresencePublication {
    const now = performance.now();
    this.#owned.set(key, { record, revision, writtenAt: now, refreshAt: now + KV_PRESENCE_REFRESH_MS });
    this.#armRefresh();
    return { key, revision, rollback: () => this.#serial(key, () => this.#removeRevision(key, revision)) };
  }

  /** Await this receipt BEFORE launching. No worker or callback is invoked by the directory. */
  publishBeforeLaunch(record: FabricParticipantRecord): Promise<KvPresencePublication> {
    const copy = this.#ownRecord(record), key = this.#key(copy.id);
    return this.#serial(key, async () => {
      this.#assertWritable();
      if (this.#owned.has(key) || this.#pendingCleanup.has(key)) throw new TransportInvalidError("Presence admission already exists or needs cleanup");
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const revision = await this.#presence.put(key, this.#owner(), copy, 0, this.#signal());
          return this.#admitted(key, copy, revision);
        } catch (error) {
          if (!uncertain(error)) {
            if (attempt === 1) this.#queueCleanup(key, copy); // A late first attempt must not leave a ghost.
            throw error; // A definite failure never authorizes launch.
          }
          if (attempt === 1) { this.#queueCleanup(key, copy); throw error; }
          let current: TransportKvEntry<unknown> | undefined;
          try { current = await this.#presence.get(key, this.#signal()); }
          catch (readError) { this.#queueCleanup(key, copy); throw readError; }
          if (current) {
            if (this.#same(current, copy)) return this.#admitted(key, copy, current.revision);
            throw new TransportInvalidError("Uncertain presence create belongs to a different record");
          }
          this.#assertWritable(); // Absent: retry ONCE, same key, owner and frozen JSON.
        }
      }
      throw new TransportUnavailableError("Presence admission did not settle");
    });
  }

  #admitted(key: string, record: FabricParticipantRecord, revision: number): KvPresencePublication {
    if (!this.leaseLive()) {
      this.#queueCleanup(key, record);
      throw new TransportUnavailableError("Owner lease lost before launch admission settled");
    }
    return this.#remember(key, record, revision);
  }

  /** On-change publication; only an initial own-root create may succeed a fenced predecessor. */
  publish(record: FabricParticipantRecord): Promise<KvPresencePublication> {
    const copy = this.#ownRecord(record), key = this.#key(copy.id);
    return this.#serial(key, async () => {
      this.#assertWritable();
      if (this.#pendingCleanup.has(key)) throw new TransportUnavailableError("Presence awaits uncertain admission cleanup");
      const owned = this.#owned.get(key);
      if (owned && canonicalJson(owned.record) === canonicalJson(copy) &&
        performance.now() - owned.writtenAt < KV_PRESENCE_REFRESH_MS) {
        return { key, revision: owned.revision, rollback: () => this.#serial(key, () => this.#removeRevision(key, owned.revision)) };
      }
      let revision: number;
      try { revision = await this.#update(key, copy, owned?.revision ?? 0); }
      catch (error) {
        // Only the initial own-root create may replace a predecessor. Children,
        // owned revisions and same-incarnation conflicts never acquire a read revision.
        if (owned || errorCode(error) !== "FABRIC_KV_CONFLICT" || copy.kind !== "root" ||
          copy.id !== this.options.rootId || copy.id !== this.options.identityId) throw error;
        const current = await this.#presence.get<unknown>(key, this.#signal());
        const predecessor = current && presenceValue(current);
        if (!current || current.key !== key || !predecessor || predecessor.kind !== "root" ||
          predecessor.id !== copy.id || predecessor.rootId !== copy.rootId ||
          predecessor.ownerHostId !== copy.ownerHostId || predecessor.ownerIdentityId !== copy.ownerIdentityId ||
          predecessor.sessionId !== copy.sessionId || predecessor.ownerIncarnation === copy.ownerIncarnation) {
          throw new TransportInvalidError("Presence create conflict is not an owned predecessor root");
        }
        // Custody's clean/native-exit proof plus exclusive lease create establishes
        // succession, not UUID ordering or a possibly stale lease watch snapshot.
        const leaseKey = `${this.options.transport.self.machine}.h.${tokenFor(copy.ownerHostId)}`;
        const currentLease = await this.#leases.get<unknown>(leaseKey, this.#signal());
        const lease = currentLease && leaseValue(currentLease);
        if (!currentLease || currentLease.key !== leaseKey || currentLease.owner !== this.#owner() || !lease ||
          lease.id !== copy.ownerHostId || lease.rootId !== copy.rootId ||
          lease.identityId !== copy.ownerIdentityId || lease.incarnation !== copy.ownerIncarnation) {
          throw new TransportUnavailableError("Current owner lease does not authorize root succession");
        }
        this.#assertWritable(); // Reads may outlive the monotonic lease window.
        revision = await this.#update(key, copy, current.revision); // One CAS; never retry/adopt a lost revision.
        try {
          // KV has no cross-bucket transaction. Validate the exact lease revision
          // again after the CAS, before remembering or admitting this publication.
          const confirmed = await this.#leases.get<unknown>(leaseKey, this.#signal());
          const confirmedLease = confirmed && leaseValue(confirmed);
          if (!confirmed || confirmed.key !== currentLease.key || confirmed.revision !== currentLease.revision ||
            confirmed.owner !== currentLease.owner || !confirmedLease || canonicalJson(confirmedLease) !== canonicalJson(lease)) {
            throw new TransportUnavailableError("Owner lease changed during root succession");
          }
          this.#assertWritable();
        } catch (error) {
          // Restore only our successful CAS revision, even if local authority is
          // now aborted. A newer presence writer wins; never retry or delete it.
          try { await this.#presence.put(key, current.owner, predecessor, revision, this.#signal(true)); }
          catch (undoError) { this.#report(undoError); }
          throw new TransportUnavailableError("Owner lease no longer authorizes root succession", { cause: error });
        }
      }
      return this.#admitted(key, copy, revision);
    });
  }

  async #update(key: string, record: FabricParticipantRecord, revision: number): Promise<number> {
    try { return await this.#presence.put(key, this.#owner(), record, revision, this.#signal()); }
    catch (error) {
      if (!uncertain(error)) throw error;
      try {
        const current = await this.#presence.get(key, this.#signal());
        if (current && this.#same(current, record)) return current.revision;
      } catch (readError) { this.#report(readError); }
      // Never allow a later refresh to adopt an unknown CAS revision.
      this.#owned.delete(key);
      this.#queueCleanup(key, record);
      throw error;
    }
  }

  #armRefresh(): void {
    if (this.#closed || this.#timer || this.#refreshPass || this.#owned.size === 0) return;
    const due = Math.min(...[...this.#owned.values()].map(owned => owned.refreshAt));
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.refreshOwned().catch(error => this.#report(error));
    }, Math.max(1, Math.ceil(due - performance.now())));
    this.#timer.unref?.();
  }

  /** One deadline per owned row, three minutes after its last successful change/refresh. */
  refreshOwned(): Promise<void> {
    if (this.#refreshPass) return this.#refreshPass;
    try { this.#assertWritable(); } catch (error) { return Promise.reject(error); }
    const pass = Promise.allSettled([...this.#owned].map(([key]) => this.#serial(key, async () => {
      const owned = this.#owned.get(key);
      if (!owned || performance.now() < owned.refreshAt) return;
      try {
        this.#assertWritable();
        const revision = await this.#update(key, owned.record, owned.revision);
        this.#admitted(key, owned.record, revision);
      } catch (error) {
        // A failed automatic refresh is not a hot-loop retry or a successful-write receipt.
        if (this.#owned.get(key) === owned) owned.refreshAt = performance.now() + KV_PRESENCE_REFRESH_MS;
        throw error;
      }
    }))).then(results => {
      const failed = results.find(result => result.status === "rejected");
      if (failed?.status === "rejected") throw failed.reason;
    });
    this.#refreshPass = pass;
    void pass.finally(() => {
      if (this.#refreshPass === pass) this.#refreshPass = undefined;
      this.#armRefresh();
    }).catch(() => undefined);
    return pass;
  }

  async #removeRevision(key: string, revision: number): Promise<boolean> {
    // Stop renewal even if the deletion is uncertain or a foreign revision already won.
    const owned = this.#owned.get(key);
    if (owned?.revision === revision) this.#owned.delete(key);
    try { return await this.#presence.delete(key, revision, this.#signal(true)); }
    catch (error) {
      // remove() cannot retry after #owned was retired. Preserve the exact-record
      // cleanup intent instead of leaving a terminal row routable until its TTL.
      if (owned?.revision === revision) this.#queueCleanup(key, owned.record);
      throw error;
    }
  }

  /** Terminal cleanup never reads a revision and then takes somebody else's row. */
  remove(id: string): Promise<boolean> {
    const key = this.#key(id);
    return this.#serial(key, async () => {
      const owned = this.#owned.get(key);
      return owned ? this.#removeRevision(key, owned.revision) : false;
    });
  }

  #queueCleanup(key: string, record: FabricParticipantRecord): void {
    this.#pendingCleanup.set(key, record);
    this.#requestCleanup();
  }
  #requestCleanup(): void {
    if (this.#closed || this.#pendingCleanup.size === 0) return;
    if (this.#flushing) { this.#cleanupAgain = true; return; }
    const flushing = this.reconcilePendingCleanup();
    this.#flushing = flushing;
    void flushing.finally(() => {
      if (this.#flushing === flushing) this.#flushing = undefined;
      // Preserve a recovery/put event received during a read. This is event-driven, never polling.
      if (this.#cleanupAgain) { this.#cleanupAgain = false; this.#requestCleanup(); }
    }).catch(error => this.#report(error));
  }

  /** Event-driven reconnect cleanup; the parent may also call once after transport recovery.
   * Unavailable reads retain the intent. Once readable, at most one exact-record CAS delete is attempted. */
  async reconcilePendingCleanup(): Promise<void> {
    await Promise.all([...this.#pendingCleanup].map(([key, record]) => this.#serial(key, async () => {
      if (this.#pendingCleanup.get(key) !== record) return;
      let current: TransportKvEntry<unknown> | undefined;
      try { current = await this.#presence.get(key, this.#signal(true)); }
      catch (error) { this.#report(error); return; }
      if (!current) return; // A delayed uncertain write may still appear on the watch.
      this.#pendingCleanup.delete(key);
      if (!this.#same(current, record)) return;
      try { await this.#presence.delete(key, current.revision, this.#signal(true)); }
      catch (error) { this.#report(error); } // One eventual attempt, never an unfenced retry.
    })));
  }

  /** Stops watches/refreshes, joins in-flight writes, then CAS-withdraws only this helper's receipts. */
  close(): Promise<void> {
    if (this.#closing) return this.#closing;
    this.#stop(new DirectoryNotReadyError());
    this.#closing = (async () => {
      await Promise.allSettled(this.#watchTasks);
      await this.#flushing?.catch(error => this.#report(error));
      await Promise.allSettled([...this.#writes.values()]);
      await Promise.all([...this.#owned].map(([key, owned]) => this.#serial(key, async () => {
        try { await this.#removeRevision(key, owned.revision); } catch (error) { this.#report(error); }
      })));
      await this.reconcilePendingCleanup();
    })();
    return this.#closing;
  }
}
