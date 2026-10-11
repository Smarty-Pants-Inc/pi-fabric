// The NATS JetStream mesh state backend, single host, phase 1 (smarty-dev#7504; design: docs/mesh-backends.md
// option (c)). One JetStream message per committed batch, published with the stream's expected last sequence (a
// compare-and-swap over the whole stream) and a message id (dedupe of a retried publish). A projector applies the
// stream, in order, to a local read model; every read of the StateBackend contract is served synchronously from it.
//
// This module never imports a NATS client: the real port (state-backend-nats-client.ts) is loaded by
// `openNatsStateBackend` at first use, so the startup graph is unchanged. The selector's factory is synchronous, so a
// host prepares the backend first (`openNatsStateBackend`, async: connect, refuse, replay) and the factory then
// returns the prepared backend for that root, or refuses. `nats` never falls back to `file`.
import { randomUUID } from "node:crypto";
import path from "node:path";
import { captureStorageDelete, captureStoragePut, storageRevision } from "../verified/storage.js";
import type { MeshIdentity } from "./event-log.js";
import type {
  MeshCommitEffects, StateBackend, StateBackendBatchInput, StateBackendDeleteInput, StateBackendDiagnostics,
  StateBackendPutInput,
} from "./state-backend.js";
import {
  bracketFileRead, jsonClone, MeshBatchConflictError, MeshStateFileReadChangedError,
  type MeshBatchResult, type MeshBatchView, type MeshReadOptions, type MeshStateEntry,
} from "./state-file.js";
import { validateMeshStateKey } from "./state-sqlite.js";
import { MeshLockTimeoutError } from "../core/atomic-write.js";

// ------------------------------------------------------------------ the JetStream port

/** A JetStream publish refused because the stream's last sequence was not the expected one (NATS err_code 10071). */
export class NatsWrongLastSequenceError extends Error {
  readonly code = "FABRIC_MESH_NATS_WRONG_LAST_SEQUENCE";
  constructor(readonly expected: number, detail = "") {
    super(`JetStream wrong last sequence (expected ${expected})${detail ? `: ${detail}` : ""}`);
    this.name = "NatsWrongLastSequenceError";
  }
}

/** One stored stream message. */
export interface NatsStreamMessage { seq: number; data: Uint8Array }

/**
 * The five JetStream operations this backend uses. The real implementation wraps the official nats.js v3 client
 * (state-backend-nats-client.ts); tests use an in-process fake with the same semantics.
 */
export interface JetStreamPort {
  /** Server facts for the refusals: its version and whether it fsyncs every write (`/varz` sync_always). */
  server(): Promise<{ version: string; syncAlways: boolean | undefined }>;
  /** Creates the stream if absent (file storage, the subject `subject`); returns its last sequence. */
  ensureStream(stream: string, subject: string): Promise<{ lastSeq: number }>;
  /** The stream's last sequence now (leader). */
  lastSequence(stream: string): Promise<number>;
  /** Publishes with `expect.lastSequence` and `msgID`. A duplicate msgID returns the original sequence, `duplicate`. */
  publish(subject: string, data: Uint8Array, options: { expectLastSequence: number; msgID: string }): Promise<{ seq: number; duplicate: boolean }>;
  /** Delivers every message after `afterSeq`, in order, then each new one as it is stored. */
  consume(stream: string, afterSeq: number, onMessage: (message: NatsStreamMessage) => void): Promise<{ stop(): Promise<void> }>;
  close(): Promise<void>;
}

// ------------------------------------------------------------------ the stream records

/** One committed change: the entry after the batch, or a delete (the key's tombstone version). */
type NatsChange =
  | { key: string; put: { value: unknown; version: number; updatedAt: number; updatedBy: MeshIdentity } }
  | { key: string; deleted: { version: number } };

/** A stream message. `batch` is a committed writeBatch; `retire` ends the stream's authority (rollback). */
type NatsRecord =
  | { format: 1; type: "batch"; id: string; base: number; highWater: number; changes: NatsChange[] }
  | { format: 1; type: "retire"; epoch: number }
  | { format: 1; type: "import-begin"; epoch: number }
  | { format: 1; type: "import-commit"; epoch: number };

const encode = (record: NatsRecord): Uint8Array => Buffer.from(JSON.stringify(record), "utf8");
const decode = (data: Uint8Array): NatsRecord => {
  const record = JSON.parse(Buffer.from(data).toString("utf8")) as NatsRecord;
  if (record?.format !== 1 || !["batch", "retire", "import-begin", "import-commit"].includes(record.type)) {
    throw new Error("Unknown Fabric mesh NATS record");
  }
  return record;
};

// ------------------------------------------------------------------ the read model

/** One immutable model snapshot (copy on write per applied message), so a `stateToken` reads exactly that state. */
interface NatsSnapshot {
  readonly seq: number;
  readonly entries: ReadonlyMap<string, MeshStateEntry>;
  /** Every key's latest version, tombstones included (the CAS base of an absent key). */
  readonly versions: ReadonlyMap<string, number>;
  /** Absent keys whose tombstone is retained, oldest first (bounded). */
  readonly tombstones: readonly string[];
  readonly highWater: number;
  readonly status: "ready" | "importing" | "retired";
  /** The ids of the most recent batches and their sequences (bounded): did MY batch commit after an uncertain ack? */
  readonly recent: ReadonlyMap<string, number>;
}

const RECENT_IDS = 4_096;
const EMPTY: NatsSnapshot = { seq: 0, entries: new Map(), versions: new Map(), tombstones: [], highWater: 0, status: "ready", recent: new Map() };

/** Thrown by reads and writes once the stream is retired (rolled back to sqlite or file), or while it is importing. */
export class MeshStateNatsRefusedError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_REFUSED";
  constructor(message: string) {
    super(`FABRIC_MESH_STATE_NATS_REFUSED: ${message}`);
    this.name = "MeshStateNatsRefusedError";
  }
}

/** A `fresh` read while the projector lags the stream, or a write that ran out of its retry budget: retryable. */
export class MeshStateNatsBusyError extends MeshLockTimeoutError {
  readonly busyCode = "FABRIC_MESH_STATE_BUSY";
  constructor(detail: string) {
    super(` (NATS state: ${detail})`, 1, 0);
    this.message = `FABRIC_MESH_STATE_BUSY: ${detail}`;
    this.name = "MeshStateNatsBusyError";
  }
}

/** A publish got no answer within the write budget: the batch may or may not have committed. Not retryable as is. */
export class MeshStateNatsOutcomeUnknownError extends Error {
  readonly code = "FABRIC_MESH_STATE_NATS_OUTCOME_UNKNOWN";
  constructor(readonly batchId: string, detail: string) {
    super(`FABRIC_MESH_STATE_NATS_OUTCOME_UNKNOWN: batch ${batchId} got no acknowledgement (${detail}); re-read before writing again`);
    this.name = "MeshStateNatsOutcomeUnknownError";
  }
}

/**
 * Applies one stream message to a snapshot. Deterministic and idempotent: a message at or below the snapshot's
 * sequence is ignored; a batch whose `base` is not the snapshot's sequence cannot have passed the publish CAS, so
 * it is a protocol violation and refuses (the model never guesses).
 */
export const applyNatsMessage = (snapshot: NatsSnapshot, message: NatsStreamMessage, maxTombstones: number): NatsSnapshot => {
  if (message.seq <= snapshot.seq) return snapshot;
  const record = decode(message.data);
  if (record.type === "retire") return { ...snapshot, seq: message.seq, status: "retired" };
  if (record.type === "import-begin") return { ...snapshot, seq: message.seq, status: "importing" };
  if (record.type === "import-commit") return { ...snapshot, seq: message.seq, status: "ready" };
  if (record.base !== snapshot.seq) {
    throw new MeshStateNatsRefusedError(`stream message ${message.seq} was committed on sequence ${record.base}, not ${snapshot.seq}`);
  }
  const entries = new Map(snapshot.entries);
  const versions = new Map(snapshot.versions);
  let tombstones = snapshot.tombstones.slice();
  for (const change of record.changes) {
    validateMeshStateKey(change.key);
    if ("put" in change) {
      entries.set(change.key, { key: change.key, ...change.put });
      versions.set(change.key, storageRevision(change.put.version));
      tombstones = tombstones.filter(key => key !== change.key);
    } else {
      entries.delete(change.key);
      versions.set(change.key, storageRevision(change.deleted.version));
      tombstones = [...tombstones.filter(key => key !== change.key), change.key];
    }
  }
  // The same bound as the file backend: forget the oldest absent keys' tombstones (their versions) past the limit.
  while (tombstones.length > maxTombstones) {
    const forgotten = tombstones.shift()!;
    if (!entries.has(forgotten)) versions.delete(forgotten);
  }
  const recent = new Map(snapshot.recent);
  if (typeof record.id === "string") recent.set(record.id, message.seq);
  if (recent.size > RECENT_IDS) recent.delete(recent.keys().next().value!);
  return { seq: message.seq, entries, versions, tombstones, highWater: Math.max(snapshot.highWater, storageRevision(record.highWater)),
    status: snapshot.status, recent };
};

const sortedKeys = (keys: Iterable<string>): string[] => [...keys].sort((left, right) => left.localeCompare(right));
const viewOf = (snapshot: NatsSnapshot): MeshBatchView => ({
  get: key => { const entry = snapshot.entries.get(key); return entry ? jsonClone(entry) : undefined; },
  listAll: prefix => sortedKeys([...snapshot.entries.keys()].filter(key => key.startsWith(prefix))).map(key => jsonClone(snapshot.entries.get(key)!)),
  version: key => snapshot.versions.get(key) ?? 0,
});

// ------------------------------------------------------------------ the backend

export interface NatsStateBackendOptions {
  port: JetStreamPort;
  /** The stream (one per mesh root). */
  stream: string;
  /** The subject the stream stores (one subject: the stream's last sequence is the CAS). */
  subject: string;
  maxEventBytes: number;
  maxStateTombstones?: number;
  /** The budget of one write's CAS retries, then `MeshStateNatsBusyError`. Default 5 s. */
  lockTimeoutMs?: number;
}

const DEFAULT_TOMBSTONES = 2_048;

export class NatsStateBackend implements StateBackend {
  readonly kind = "nats" as const;
  readonly readCacheMs = 0;
  readonly backgroundReadCacheMs = 0;
  readonly readCacheRemainingMs = 0;
  readonly #options: NatsStateBackendOptions;
  readonly #streamId = randomUUID();
  #snapshot: NatsSnapshot = EMPTY;
  /** The highest sequence this process knows the stream holds (acks and deliveries). */
  #knownSeq = 0;
  #waiters: { seq: number; resolve: () => void; reject: (error: unknown) => void }[] = [];
  #failure: unknown;
  #consumer: { stop(): Promise<void> } | undefined;
  #closed = false;
  #casRetries = 0;

  private constructor(options: NatsStateBackendOptions) { this.#options = options; }

  /** Connects the projector and replays the stream up to its current last sequence. */
  static async open(options: NatsStateBackendOptions): Promise<NatsStateBackend> {
    const backend = new NatsStateBackend(options);
    const { lastSeq } = await options.port.ensureStream(options.stream, options.subject);
    backend.#knownSeq = lastSeq;
    backend.#consumer = await options.port.consume(options.stream, 0, message => backend.#apply(message));
    await backend.#appliedThrough(lastSeq);
    return backend;
  }

  #apply(message: NatsStreamMessage): void {
    if (this.#failure !== undefined) return;
    try { this.#snapshot = applyNatsMessage(this.#snapshot, message, this.#options.maxStateTombstones ?? DEFAULT_TOMBSTONES); }
    catch (error) {
      this.#failure = error;
      for (const waiter of this.#waiters.splice(0)) waiter.reject(error);
      return;
    }
    this.#knownSeq = Math.max(this.#knownSeq, message.seq);
    const ready = this.#waiters.filter(waiter => waiter.seq <= this.#snapshot.seq);
    this.#waiters = this.#waiters.filter(waiter => waiter.seq > this.#snapshot.seq);
    for (const waiter of ready) waiter.resolve();
  }

  /** Resolves once the model has applied `seq`; a projector that does not get there within the write budget is busy. */
  #appliedThrough(seq: number, budgetMs = this.#options.lockTimeoutMs ?? 5_000): Promise<void> {
    if (this.#failure !== undefined) return Promise.reject(this.#failure);
    if (this.#snapshot.seq >= seq) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#waiters = this.#waiters.filter(waiter => waiter !== entry);
        reject(new MeshStateNatsBusyError(`the projector did not reach sequence ${seq} within ${budgetMs} ms (at ${this.#snapshot.seq})`));
      }, budgetMs);
      timer.unref?.();
      const entry = { seq, resolve: () => { clearTimeout(timer); resolve(); }, reject: (error: unknown) => { clearTimeout(timer); reject(error); } };
      this.#waiters.push(entry);
    });
  }

  #readable(options: MeshReadOptions = {}): NatsSnapshot {
    if (this.#closed) throw new Error("Fabric mesh NATS state backend is closed");
    if (this.#failure !== undefined) throw this.#failure;
    const snapshot = (options.snapshot as NatsSnapshot | undefined) ?? this.#snapshot;
    if (snapshot.status === "retired") throw new MeshStateNatsRefusedError("the stream is retired (rolled back); reopen on the active backend");
    if (options.fresh === true && this.#knownSeq > this.#snapshot.seq) {
      throw new MeshStateNatsBusyError(`the projector is at ${this.#snapshot.seq} of ${this.#knownSeq}`);
    }
    return snapshot;
  }

  get(key: string, options?: MeshReadOptions): MeshStateEntry | undefined {
    validateMeshStateKey(key);
    const entry = this.#readable(options).entries.get(key);
    return entry ? jsonClone(entry) : undefined;
  }
  list(prefix = "", limit?: number, options?: MeshReadOptions): MeshStateEntry[] {
    const entries = this.listAll(prefix, options);
    return limit === undefined ? entries : entries.slice(0, Math.max(0, limit));
  }
  listAll(prefix = "", options?: MeshReadOptions): MeshStateEntry[] {
    return this.listAllShared(prefix, options).map(entry => jsonClone(entry as MeshStateEntry));
  }
  listAllShared(prefix = "", options?: MeshReadOptions): readonly Readonly<MeshStateEntry>[] {
    if (prefix) validateMeshStateKey(prefix);
    const snapshot = this.#readable(options);
    return sortedKeys([...snapshot.entries.keys()].filter(key => key.startsWith(prefix))).map(key => snapshot.entries.get(key)!);
  }
  stateToken(options?: MeshReadOptions): object { return this.#readable(options); }
  stateStamp(): string | undefined { return `nats:${this.#streamId}:${this.#snapshot.seq}`; }
  cachedStateStamp(): string | undefined { return this.stateStamp(); }

  async put(input: StateBackendPutInput): Promise<MeshStateEntry> {
    await this.writeBatch({ identity: input.identity, ops: [{ kind: "put", key: input.key, value: input.value,
      ...(input.ifVersion === undefined ? {} : { ifVersion: input.ifVersion }) }] });
    const entry = this.get(input.key);
    if (!entry) throw new Error(`Fabric mesh NATS put of ${input.key} did not apply`);
    return entry;
  }

  async delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }> {
    const [result] = await this.writeBatch({ identity: { id: "fabric", name: "fabric", kind: "agent" },
      ops: [{ kind: "delete", key: input.key, ...(input.ifVersion === undefined ? {} : { ifVersion: input.ifVersion }) }] });
    return result?.applied ? { deleted: true, version: result.version } : { deleted: false };
  }

  /**
   * The R11 loop of the design: `prepare` runs on the model at sequence S, the batch is published with
   * `expect.lastSequence: S`; another writer's commit fails it (wrong last sequence), the model catches up and the
   * batch runs again, within `lockTimeoutMs`. The promise resolves once this host's model has applied the commit.
   */
  async writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]> {
    const deadline = performance.now() + (this.#options.lockTimeoutMs ?? 5_000);
    for (let attempt = 1; ; attempt++) {
      let fileRead: { value: unknown; stamp: string | undefined } | undefined;
      if (input.fileRead) {
        fileRead = bracketFileRead(input.fileRead);
        if (!fileRead) { if (attempt > 8) throw new MeshStateFileReadChangedError(attempt); continue; }
      }
      const snapshot = this.#readable();
      if (snapshot.status !== "ready") throw new MeshStateNatsRefusedError(`the stream is ${snapshot.status}; writes refuse`);
      if (input.fileRead && input.fileRead.stamp() !== fileRead!.stamp) { if (attempt > 8) throw new MeshStateFileReadChangedError(attempt); continue; }
      const planned = this.#plan(snapshot, input, fileRead?.value);
      if (planned.changes.length === 0) {
        input.beforeCommit?.();
        input.afterCommit?.(viewOf(snapshot));
        this.#outbox(input, planned.results, [], snapshot);
        return planned.results;
      }
      input.beforeCommit?.();
      const id = randomUUID();
      const record: NatsRecord = { format: 1, type: "batch", id, base: snapshot.seq, highWater: planned.highWater, changes: planned.changes };
      const data = encode(record);
      if (data.byteLength > this.#options.maxEventBytes * Math.max(1, planned.changes.length) + 64 * 1024) {
        throw new Error("Fabric mesh NATS batch exceeds its size bound");
      }
      let seq: number;
      try {
        seq = await this.#publish(data, snapshot.seq, id, deadline);
      } catch (error) {
        if (!(error instanceof NatsWrongLastSequenceError)) throw error;
        // Catch up to the stream's head. The server checks the expected sequence before the duplicate window, so a
        // retry of MY stored batch (its ack was lost) also fails this way: it is then in the stream under its id.
        const last = await this.#options.port.lastSequence(this.#options.stream);
        this.#knownSeq = Math.max(this.#knownSeq, last);
        await this.#appliedThrough(last);
        const committed = this.#snapshot.recent.get(id);
        if (committed === undefined) {
          // Another writer won: run the batch again on the new model.
          this.#casRetries++;
          if (performance.now() >= deadline) throw new MeshStateNatsBusyError(`another writer kept committing (${attempt} attempts)`);
          continue;
        }
        seq = committed;
      }
      this.#knownSeq = Math.max(this.#knownSeq, seq);
      await this.#appliedThrough(seq);
      const after = this.#snapshot;
      input.afterCommit?.(viewOf(after));
      this.#outbox(input, planned.results, planned.changes.map(change => change.key), after);
      return planned.results;
    }
  }

  /**
   * One publish of one batch. A lost ack (a timeout, a dropped connection) is retried with the SAME message id and
   * expected sequence. If the first copy was stored, the stream moved past `base` and the retry fails the expected
   * sequence: the caller then finds the batch in the stream by its id (never commits it twice). If it was not stored
   * and nothing else was, the retry stores it (or the duplicate window returns it). Past the deadline with no answer,
   * the outcome is unknown: that is not a retryable busy error, the caller must re-read.
   */
  async #publish(data: Uint8Array, base: number, msgID: string, deadline: number): Promise<number> {
    for (;;) {
      try { return (await this.#options.port.publish(this.#options.subject, data, { expectLastSequence: base, msgID })).seq; }
      catch (error) {
        if (error instanceof NatsWrongLastSequenceError || this.#closed) throw error;
        if (performance.now() >= deadline) throw new MeshStateNatsOutcomeUnknownError(msgID, (error as Error).message);
      }
    }
  }

  #outbox(input: StateBackendBatchInput, results: MeshBatchResult[], changed: string[], snapshot: NatsSnapshot): void {
    if (!input.commitOutbox) return;
    const effects: MeshCommitEffects = { backend: "nats", results: results.map(result => ({ ...result })), changed,
      stamp: `nats:${this.#streamId}:${snapshot.seq}`, view: viewOf(snapshot) } as MeshCommitEffects;
    input.commitOutbox(effects);
  }

  /** The batch's verified transitions on the snapshot, exactly as the file and sqlite backends apply them. */
  #plan(snapshot: NatsSnapshot, input: StateBackendBatchInput, fileValue: unknown): { changes: NatsChange[]; results: MeshBatchResult[]; highWater: number } {
    const entries = new Map(snapshot.entries);
    const versions = new Map(snapshot.versions);
    let highWater = snapshot.highWater;
    const view: MeshBatchView = {
      get: key => { const entry = entries.get(key); return entry ? jsonClone(entry) : undefined; },
      listAll: prefix => sortedKeys([...entries.keys()].filter(key => key.startsWith(prefix))).map(key => jsonClone(entries.get(key)!)),
      version: key => versions.get(key) ?? 0,
    };
    const ops = [...input.ops, ...(input.prepare?.(view, fileValue) ?? [])];
    for (const op of ops) validateMeshStateKey(op.key);
    const results: MeshBatchResult[] = [];
    const changes = new Map<string, NatsChange>();
    const now = Date.now();
    for (const op of ops) {
      const present = entries.has(op.key);
      const version = versions.get(op.key) ?? 0;
      if (op.kind === "delete" && op.condition && !op.condition(view.get)) { results.push({ key: op.key, applied: false, version }); continue; }
      if (op.ifVersion !== undefined && op.ifVersion !== version) {
        const policy = typeof op.onConflict === "function" ? op.onConflict(view.get(op.key)) : op.onConflict ?? "abort";
        if (policy === "abort") throw new MeshBatchConflictError(op.key, op.ifVersion, version);
        results.push({ key: op.key, applied: false, version });
        continue;
      }
      const request = op.kind === "delete"
        ? captureStorageDelete({ key: op.key, ifVersion: op.ifVersion })
        : captureStoragePut({ key: op.key, ifVersion: op.ifVersion,
          value: typeof op.value === "function" ? (op.value as (now: number) => unknown)(now) : op.value,
          identity: op.identity ?? input.identity }, this.#options.maxEventBytes);
      const plan = request.transition(present, version, highWater);
      if (plan.kind === "unchanged") { results.push({ key: op.key, applied: false, version }); continue; }
      if (plan.kind === "delete") {
        entries.delete(plan.key);
        changes.set(plan.key, { key: plan.key, deleted: { version: plan.version } });
      } else {
        const entry = { key: plan.key, value: plan.value, version: plan.version, updatedAt: now, updatedBy: plan.identity };
        entries.set(plan.key, entry);
        changes.set(plan.key, { key: plan.key, put: { value: plan.value, version: plan.version, updatedAt: now, updatedBy: plan.identity } });
      }
      versions.set(plan.key, plan.version);
      highWater = plan.highWater;
      results.push({ key: op.key, applied: true, version: plan.version });
    }
    return { changes: [...changes.values()], results, highWater };
  }

  async confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    const snapshot = this.#readable();
    if (snapshot.status !== "ready") throw new MeshStateNatsRefusedError(`the stream is ${snapshot.status}; writes refuse`);
    this.#knownSeq = Math.max(this.#knownSeq, await this.#options.port.lastSequence(this.#options.stream));
    onAcquired?.(Date.now());
  }

  /**
   * ponytail (phase 1, single host): runs `operation` on the model as applied now. Unlike file and sqlite, a
   * commit by another writer cannot be held off synchronously (the stream is the authority, and Node cannot block
   * on it), so the fence is exactly as strong as the caller's own `.lock` plus the stream CAS of its next publish.
   * Phase 2 (smarty-dev#7504) decides the cross-writer fence (the event log on the same stream).
   */
  withWriteFence<T>(operation: () => T): T {
    this.#readable();
    return operation();
  }

  dropCache(): void { /* the model is the stream's projection: nothing to drop */ }

  diagnostics(): StateBackendDiagnostics {
    return { kind: "nats", nats: { stream: this.#options.stream, appliedSeq: this.#snapshot.seq, knownSeq: this.#knownSeq,
      status: this.#snapshot.status, casRetries: this.#casRetries, ...(this.#failure ? { failure: String(this.#failure) } : {}) } } as StateBackendDiagnostics;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    const consumer = this.#consumer;
    for (const waiter of this.#waiters.splice(0)) waiter.reject(new Error("Fabric mesh NATS state backend is closed"));
    void (async () => { await consumer?.stop(); await this.#options.port.close(); })().catch(() => undefined);
  }
}

// ------------------------------------------------------------------ the prepared-backend registry and the refusals

const prepared = new Map<string, NatsStateBackend>();

/** The prepared backend for a mesh root, or undefined. Read by the selector's synchronous `nats` factory. */
export const preparedNatsStateBackend = (root: string): NatsStateBackend | undefined => prepared.get(path.resolve(root));

/** Minimum server: per-message TTL and delete markers (2.11), used by the KV leases of phase 2. */
const MIN_SERVER = [2, 11, 0];
const versionAtLeast = (version: string, minimum: number[]): boolean => {
  const parts = version.replace(/^v/, "").split(/[.-]/).slice(0, 3).map(part => Number.parseInt(part, 10));
  for (let index = 0; index < 3; index++) {
    const have = parts[index] ?? 0, want = minimum[index] ?? 0;
    if (!Number.isFinite(have)) return false;
    if (have !== want) return have > want;
  }
  return true;
};

export interface OpenNatsStateBackendOptions {
  /** The mesh root this backend serves (its stream is derived from it unless `stream` is given). */
  root: string;
  /** NATS server URLs (the real client); ignored when `port` is given (tests). */
  servers?: string[];
  /** Host-only: the server's monitoring endpoint (`http://host:8222`), read for `/varz` sync_always. */
  monitoringUrl?: string;
  port?: JetStreamPort;
  stream?: string;
  maxEventBytes?: number;
  maxStateTombstones?: number;
  lockTimeoutMs?: number;
  /** Host-only: the operator accepts a server that does not fsync every write (`sync_always: false`). */
  acceptPeriodicSync?: boolean;
}

/** The stream name for a mesh root: stable, NATS-safe. */
export const natsStreamName = (root: string): string =>
  `FABRIC_MESH_${Buffer.from(path.resolve(root)).toString("base64url").replace(/[^A-Za-z0-9_]/g, "_").slice(-48)}`;

/**
 * Opens (and registers for the selector) the NATS state backend of a mesh root. Refuses, never falls back:
 * a server older than 2.11; a server that does not fsync every write unless the operator accepts it; a stream that is
 * retired. Loads the official client only here.
 */
export const openNatsStateBackend = async (options: OpenNatsStateBackendOptions): Promise<NatsStateBackend> => {
  const root = path.resolve(options.root);
  const port = options.port ?? await (await import("./state-backend-nats-client.js")).connectJetStream(options.servers ?? [],
    options.monitoringUrl === undefined ? {} : { monitoringUrl: options.monitoringUrl });
  try {
    const server = await port.server();
    if (!versionAtLeast(server.version, MIN_SERVER)) throw new MeshStateNatsRefusedError(`nats-server ${server.version} is older than ${MIN_SERVER.join(".")}`);
    if (server.syncAlways !== true && options.acceptPeriodicSync !== true) {
      throw new MeshStateNatsRefusedError(`nats-server does not report sync_always (got ${String(server.syncAlways)}): an acked write may not survive a power loss; the operator may accept it explicitly`);
    }
    const stream = options.stream ?? natsStreamName(root);
    const backend = await NatsStateBackend.open({ port, stream, subject: `${stream}.state`, maxEventBytes: options.maxEventBytes ?? 1024 * 1024,
      ...(options.maxStateTombstones === undefined ? {} : { maxStateTombstones: options.maxStateTombstones }),
      ...(options.lockTimeoutMs === undefined ? {} : { lockTimeoutMs: options.lockTimeoutMs }) });
    if (backend.diagnostics().nats?.status === "retired") { backend.close(); throw new MeshStateNatsRefusedError(`the stream ${stream} is retired`); }
    prepared.get(root)?.close();
    prepared.set(root, backend);
    return backend;
  } catch (error) {
    if (!options.port) await port.close().catch(() => undefined);
    throw error;
  }
};

/** Forgets a root's prepared backend (closing it). */
export const closeNatsStateBackend = (root: string): void => {
  const resolved = path.resolve(root);
  prepared.get(resolved)?.close();
  prepared.delete(resolved);
};
