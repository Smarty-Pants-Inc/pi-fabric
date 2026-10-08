/**
 * The mesh state backend seam (smarty-dev#6477, lane L2a).
 *
 * `MeshStore`'s keyed-state operations (reads, the change stamp, put/delete, writeBatch with its
 * callbacks, compare-and-swap) go through ONE `StateBackend`. Three kinds exist:
 *
 * - `"file"` (default): today's `state.json` behind the mesh `.lock` (`state-file.ts`), unchanged.
 * - `"sqlite"`: `<root>/state.db` (`state-sqlite.ts`, lane L1). Every write is one
 *   `BEGIN IMMEDIATE` transaction; the mesh `.lock` is never taken for state. Acquisition never
 *   blocks the event loop: `busy_timeout` <= 5 ms plus an ASYNC retry bounded by the caller's
 *   budget (`lockTimeoutMs`, the active `withTryLock` budget, `writeSignal`). A busy budget that
 *   runs out throws `MeshStateBusyError`, a `MeshLockTimeoutError` (so every existing
 *   `isMeshLockTimeout` retry path keeps working) with the distinct `busyCode`
 *   `FABRIC_MESH_STATE_BUSY` that L8/C1 count separately from `.lock` timeouts.
 * - `"shadow"`: the file backend is the authority for every read and write. After each committed
 *   write, the committed values of its keys are also applied to a separate SQLite database for
 *   divergence checks. A SQLite failure never fails, delays past its own small budget, or changes
 *   the result of the caller; it is counted in `diagnostics()`.
 *
 * Selection: `mesh.stateBackend` in the Fabric config (`"file" | "shadow" | "sqlite"`, default
 * `"file"`); the environment variable `PI_FABRIC_MESH_STATE_BACKEND` overrides it. A root on a
 * non-local filesystem (R16) or a runtime without `node:sqlite` falls back to `"file"`.
 *
 * This file was committed first as the interface milestone; lanes L3 (projector) and L4 (cutover)
 * build against these declarations. Changes after that commit are recorded in the lane's
 * `iface-changes.md`.
 */
import path from "node:path";
import { MeshLockTimeoutError } from "../core/atomic-write.js";
import type { MeshIdentity } from "./event-log.js";
import type { MeshStoreContext } from "./mesh-lock.js";
import { FILE_READ_CHANGED, jsonClone, MeshStateFileReadChangedError, StateFile, type MeshBatchOperation, type MeshBatchResult,
  type MeshBatchView, type MeshReadOptions, type MeshStateEntry, type StateFileOptions } from "./state-file.js";
import { filesystemRefusal, SqliteStateStore, validateMeshStateKey, type SqliteStateStoreOptions } from "./state-sqlite.js";

export type { MeshBatchOperation, MeshBatchResult, MeshBatchView, MeshReadOptions, MeshStateEntry };

/** The configured state backend (`mesh.stateBackend`, env `PI_FABRIC_MESH_STATE_BACKEND`). */
export type MeshStateBackendKind = "file" | "shadow" | "sqlite";

/** The environment override of `mesh.stateBackend`. */
export const MESH_STATE_BACKEND_ENV = "PI_FABRIC_MESH_STATE_BACKEND";

/** `MeshStateBusyError.busyCode`: a SQLite state write that ran out of its busy budget. */
export const MESH_STATE_BUSY_CODE = "FABRIC_MESH_STATE_BUSY";

/**
 * The shape of the SQLite busy-budget error (the class lives with the sqlite adapter). It IS a
 * `MeshLockTimeoutError` (`code` stays `FABRIC_MESH_LOCK_TIMEOUT`) so callers that retry lock
 * timeouts retry it too; `busyCode` tells L8 and commswatch it was the state database, not `.lock`.
 */
export interface MeshStateBusy extends MeshLockTimeoutError {
  readonly busyCode: typeof MESH_STATE_BUSY_CODE;
  /** The SQLite database the write waited for. */
  readonly database: string;
  /** Milliseconds the caller waited before giving up (its whole budget). */
  readonly waitedMs: number;
}

/**
 * R11, file reads a write needs. They run BEFORE the transaction begins (`BEGIN IMMEDIATE`, or the
 * `.lock` acquisition of the file backend), never inside it. `stamp()` is captured right after
 * `read()` and checked AGAIN inside the transaction, before `prepare`: a different stamp means a
 * file changed in between, so the backend rolls back, re-runs `read()` outside the transaction and
 * retries (at most `retries` times, default 3; then it throws `MeshStateFileReadChangedError`).
 * `stamp` must be cheap and synchronous (a `stat`, never a parse). Both run synchronously.
 */
export interface MeshStateFileRead<F = unknown> {
  read(): F;
  stamp(): string | undefined;
  retries?: number;
}

/**
 * What `commitOutbox` receives: one record per committed `writeBatch`. Detached copies only; the
 * hook never sees live transaction state.
 */
export interface MeshCommitEffects {
  readonly backend: MeshStateBackendKind;
  /** One result per operation (the `writeBatch` return value). */
  readonly results: readonly MeshBatchResult[];
  /** Keys this batch changed (applied results), in order. Empty for a write-free batch. */
  readonly changed: readonly string[];
  /** The backend change stamp right after this commit (`stateStamp()`), when known. */
  readonly stamp: string | undefined;
  /** The batch's final view, as committed (a copy, read-only, valid after the transaction). */
  readonly view: MeshBatchView;
}

/**
 * `writeBatch` input. `ops`, `prepare`, `afterCommit` and `lockClass` keep their `MeshStore`
 * meaning; `fileRead` and `commitOutbox` are the R11 additions.
 *
 * Transaction and callback semantics (every backend):
 * 1. `fileRead.read()` (if any) runs before the transaction; see `MeshStateFileRead`.
 * 2. The transaction begins: the `.lock` (file) or `BEGIN IMMEDIATE` (sqlite).
 * 3. `fileRead.stamp()` is re-checked; a mismatch rolls back and goes to 1.
 * 4. `prepare(view, fileReadValue)` runs synchronously on the ONE authoritative snapshot of this
 *    transaction and returns more operations. It must not do I/O, await, or call store writers.
 * 5. Every operation applies with put()/delete() semantics and verified compare-and-swap:
 *    `ifVersion` mismatch with `onConflict: "abort"` (the default) throws `MeshBatchConflictError`
 *    and nothing is written; `"skip"` leaves that key. A thrown callback rolls everything back.
 * 6. COMMIT. A failed commit runs no callback below.
 * 7. `afterCommit(view)` (legacy, synchronous, no store writers): file backend: under the `.lock`
 *    right after the commit (also for a write-free batch). sqlite backend: a write-free batch runs
 *    it inside its own write-free transaction on the exact snapshot; a changing batch runs it
 *    after COMMIT under re-acquired state custody (`state-sqlite.ts`). New code uses
 *    `commitOutbox`, because effects under custody extend the critical section.
 * 8. `commitOutbox(effects)` runs once, after COMMIT, outside every state lock and transaction
 *    (also for a write-free batch, with `changed: []`). It is L2b's durable, idempotent outbox: a
 *    crash between COMMIT and the hook loses the effect, never the commit, so effects must be
 *    replayable from state. A throw rejects the `writeBatch` promise, but the commit stands.
 */
export interface StateBackendBatchInput {
  identity: MeshIdentity;
  ops: MeshBatchOperation[];
  prepare?: (view: MeshBatchView, fileRead?: unknown) => MeshBatchOperation[];
  afterCommit?: (view: MeshBatchView) => void;
  /** Lock-stats class for the bridge's own writes; never inferred from the identity text. */
  lockClass?: "bridge";
  /** R11: file reads before the transaction, re-checked by stamp inside it. */
  fileRead?: MeshStateFileRead;
  /** R11: file effects after COMMIT (L2b's outbox). */
  commitOutbox?: (effects: MeshCommitEffects) => void;
}

export interface StateBackendPutInput {
  key: string;
  value: unknown;
  identity: MeshIdentity;
  /** Compare-and-swap: the write applies only when the key's version (tombstone included) equals this. */
  ifVersion?: number;
}

export interface StateBackendDeleteInput {
  key: string;
  ifVersion?: number;
}

/** Shadow and sqlite counters for `diagnostics()`; the file backend reports only `kind`. */
export interface StateBackendDiagnostics {
  kind: MeshStateBackendKind;
  /** Why a configured sqlite/shadow backend runs as file (non-local filesystem, no node:sqlite). */
  fallback?: string;
  /** sqlite: writes that ran out of their busy budget (`MeshStateBusyError`). */
  busyTimeouts?: number;
  /** shadow: batches applied to the shadow database. */
  shadowApplied?: number;
  /** shadow: shadow writes or opens that failed (never surfaced to the caller). */
  shadowFailures?: number;
  /** shadow: keys whose shadow value differed from the file value after a check. */
  divergences?: number;
  /** shadow: the most recent divergent keys (bounded). */
  divergentKeys?: string[];
  /** shadow/sqlite: the SQLite database file. */
  database?: string;
}

/** One divergence: the key, and what each side holds (undefined: absent). */
export interface StateDivergence {
  key: string;
  file: { value: unknown; updatedBy: MeshIdentity } | undefined;
  sqlite: { value: unknown; updatedBy: MeshIdentity } | undefined;
}

/**
 * Keyed mesh state behind one backend. Method contracts:
 *
 * Reads are synchronous and never take a lock or start a write transaction. `get`, `list`,
 * `listAll`, `listAllShared` keep `MeshStore`'s ordering (`localeCompare` by key), cloning
 * (`listAllShared` returns shared, read-only entries) and `MeshReadOptions` meaning; the sqlite
 * backend is always exact (no cache window) and honours `snapshot` tokens from `stateToken()`.
 *
 * `stateStamp()` changes whenever the committed state may have changed (file: the stat stamp of
 * `state.json`; sqlite: `<store>:<epoch>:<commit>`, review-opus P1-6). Equal stamps mean an
 * unchanged committed state for that backend; stamps of different backends are never compared.
 * `cachedStateStamp()` is the stamp of what reads last returned (file: its cache; sqlite: now).
 *
 * Writes are asynchronous; acquisition honours `lockTimeoutMs`, the active `withTryLock` budget and
 * `writeSignal`, and a timeout is a `MeshLockTimeoutError` (`MeshStateBusy` for sqlite). Each write
 * is atomic: all of it commits or none of it does.
 */
export interface StateBackend {
  readonly kind: MeshStateBackendKind;

  get(key: string, options?: MeshReadOptions): MeshStateEntry | undefined;
  list(prefix?: string, limit?: number, options?: MeshReadOptions): MeshStateEntry[];
  listAll(prefix?: string, options?: MeshReadOptions): MeshStateEntry[];
  listAllShared(prefix?: string, options?: MeshReadOptions): readonly Readonly<MeshStateEntry>[];
  /** An opaque token for the state reads return now; pass it as `snapshot` to read that state again. */
  stateToken(options?: MeshReadOptions): object;
  stateStamp(): string | undefined;
  cachedStateStamp(fresh?: boolean, revalidateGeneration?: boolean): string | undefined;
  readonly readCacheMs: number;
  readonly backgroundReadCacheMs: number;
  readonly readCacheRemainingMs: number;

  put(input: StateBackendPutInput): Promise<MeshStateEntry>;
  delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }>;
  writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]>;
  /** Acquires and releases the state write lock (no write): evidence the state is writable now. */
  confirmWritable(onAcquired?: (at: number) => void): Promise<void>;

  /** Drops parsed caches (called after a failed operation under the mesh lock). */
  dropCache(): void;
  diagnostics(): StateBackendDiagnostics;
  /** Releases database handles; reads and writes after close throw (file: no-op). */
  close(): void;
}

/** Options the factory takes in addition to the `MeshStore` context (root, bounds, lock). */
export interface StateBackendOptions {
  /** Explicit kind; otherwise the env override, else `"file"`. */
  stateBackend?: MeshStateBackendKind;
  maxStateBytes?: number;
  maxStateTombstones?: number;
  lockTimeoutMs?: number;
  writeSignal?: AbortSignal;
  /** sqlite/shadow: SQLite `busy_timeout`, clamped to 0..5 ms. */
  busyTimeoutMs?: number;
  /** shadow: the budget of one shadow write (never the caller's). Default 250 ms. */
  shadowBudgetMs?: number;
}

// ------------------------------------------------------------------ implementation (L2a)

export { MeshStateFileReadChangedError };

const KINDS: ReadonlySet<string> = new Set<MeshStateBackendKind>(["file", "shadow", "sqlite"]);

export const isMeshStateBackendKind = (value: unknown): value is MeshStateBackendKind =>
  typeof value === "string" && KINDS.has(value);

/**
 * The effective kind: an explicit option wins, then a valid `PI_FABRIC_MESH_STATE_BACKEND`, then
 * `"file"`. An unknown environment value is ignored (fail safe to the file backend).
 */
export const resolveMeshStateBackend = (explicit?: MeshStateBackendKind,
  env: string | undefined = process.env[MESH_STATE_BACKEND_ENV]): MeshStateBackendKind => {
  if (explicit !== undefined) {
    if (!isMeshStateBackendKind(explicit)) throw new Error("mesh.stateBackend must be file, shadow or sqlite");
    return explicit;
  }
  const value = env?.trim().toLowerCase();
  return isMeshStateBackendKind(value) ? value : "file";
};

/** A SQLite state write ran out of its busy budget (`MeshStateBusy`). */
export class MeshStateBusyError extends MeshLockTimeoutError implements MeshStateBusy {
  readonly busyCode = MESH_STATE_BUSY_CODE;
  constructor(readonly database: string, readonly waitedMs: number, cause: MeshLockTimeoutError) {
    super(` (SQLite state ${database})`, cause.attempts, cause.maxGapMs);
    this.message = `${MESH_STATE_BUSY_CODE}: ${this.message}, waited ${Math.round(waitedMs)} ms`;
    this.name = "MeshStateBusyError";
    this.cause = cause;
  }
}

export const isMeshStateBusy = (error: unknown): error is MeshStateBusy =>
  error instanceof Error && (error as { busyCode?: unknown }).busyCode === MESH_STATE_BUSY_CODE;

const SQLITE_BUSY = 5;
const SQLITE_LOCKED = 6;
const sqliteBusy = (error: unknown): boolean => {
  const code = (error as { errcode?: unknown } | null)?.errcode;
  return typeof code === "number" && ((code & 0xff) === SQLITE_BUSY || (code & 0xff) === SQLITE_LOCKED);
};

// Only for the first synchronous read of a process: one database open, bounded to ~50 ms.
const sleepSync = (ms: number): void => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); };

const sqliteUnavailable = (): string | undefined => {
  try { return process.getBuiltinModule?.("node:sqlite")?.DatabaseSync ? undefined : "node:sqlite is unavailable"; }
  catch { return "node:sqlite is unavailable"; }
};

export type CreateStateBackendOptions = StateBackendOptions & StateFileOptions & {
  /** shadow: period of the automatic divergence check; 0 disables. Default 60 s. */
  shadowVerifyMs?: number;
};

/**
 * The backend for one MeshStore. `"file"` touches nothing new (no SQLite import side effect, no
 * file). `"sqlite"` and `"shadow"` fall back to `"file"` on a non-local filesystem (R16) or without
 * `node:sqlite` (R19); `diagnostics().fallback` says why.
 */
export const createStateBackend = (context: MeshStoreContext, options: CreateStateBackendOptions = {}): StateBackend => {
  const kind = resolveMeshStateBackend(options.stateBackend);
  if (kind === "file") return new StateFile(context, options);
  const refusal = filesystemRefusal(context.root) ?? sqliteUnavailable();
  if (refusal) {
    const file = new StateFile(context, options);
    file.fallback = `${kind}: ${refusal}`;
    return file;
  }
  if (kind === "sqlite") return new SqliteStateBackend(context, options);
  return new ShadowStateBackend(context, options);
};

const sqliteOptions = (options: CreateStateBackendOptions, lockTimeoutMs = options.lockTimeoutMs): SqliteStateStoreOptions => ({
  ...(options.maxStateBytes !== undefined ? { maxStateBytes: options.maxStateBytes } : {}),
  ...(options.maxStateTombstones !== undefined ? { maxStateTombstones: options.maxStateTombstones } : {}),
  ...(lockTimeoutMs !== undefined ? { lockTimeoutMs } : {}),
  ...(options.busyTimeoutMs !== undefined ? { busyTimeoutMs: options.busyTimeoutMs } : {}),
  ...(options.writeSignal ? { writeSignal: options.writeSignal } : {}),
});

interface SqliteSnapshot {
  stamp: string;
  entries: Map<string, MeshStateEntry>;
  /** Sorted by key (localeCompare), shared and frozen. */
  sorted: readonly Readonly<MeshStateEntry>[];
  versions: Record<string, number>;
}

const freezeEntry = (entry: MeshStateEntry): Readonly<MeshStateEntry> => Object.freeze(entry);

/** A read-only view over a captured snapshot (copies out). */
const snapshotView = (snapshot: () => SqliteSnapshot): MeshBatchView => ({
  get: (key) => { const entry = snapshot().entries.get(key); return entry ? jsonClone(entry) : undefined; },
  listAll: (prefix) => snapshot().sorted.filter((entry) => entry.key.startsWith(prefix)).map((entry) => jsonClone(entry)),
  version: (key) => snapshot().versions[key] ?? 0,
});

/**
 * `"sqlite"`: keyed state in `<root>/state.db` (state-sqlite.ts). No mesh `.lock` for state: each
 * write is one `BEGIN IMMEDIATE` transaction acquired asynchronously within the caller's budget.
 */
export class SqliteStateBackend implements StateBackend {
  readonly kind = "sqlite" as const;
  readonly root: string;
  readonly #context: MeshStoreContext;
  readonly #options: CreateStateBackendOptions;
  readonly #storeOptions: SqliteStateStoreOptions;
  readonly #tokens = new WeakSet<object>();
  #store: SqliteStateStore | undefined;
  #opening: Promise<SqliteStateStore> | undefined;
  #snapshot: SqliteSnapshot | undefined;
  #busyTimeouts = 0;
  #closed = false;

  constructor(context: MeshStoreContext, options: CreateStateBackendOptions = {}, root = context.root,
    storeOptions: SqliteStateStoreOptions = sqliteOptions(options)) {
    this.#context = context;
    this.#options = options;
    this.root = root;
    this.#storeOptions = storeOptions;
  }

  /** The open store, for maintenance callers (projector, census, tests). Opens synchronously. */
  get store(): SqliteStateStore { return this.#open(); }

  get database(): string { return path.join(this.root, "state.db"); }

  // ------------------------------------------------------------ reads

  get readCacheMs(): number {
    return this.#options.readActive?.() ? 0 : Math.max(0, Math.floor(this.#options.readCacheMs ?? 0));
  }

  get backgroundReadCacheMs(): number {
    if (this.#options.backgroundReadCacheMs === undefined) return this.readCacheMs;
    return this.#options.readActive?.() ? 1_000 : Math.max(1_000, Math.floor(this.#options.backgroundReadCacheMs));
  }

  /** SQLite reads are exact: there is no read cache to age. */
  get readCacheRemainingMs(): number { return 0; }

  get(key: string, options: MeshReadOptions = {}): MeshStateEntry | undefined {
    const pinned = this.#pinned(options);
    if (!pinned) return this.#open().get(key);
    validateMeshStateKey(key);
    const entry = pinned.entries.get(key);
    return entry ? jsonClone(entry) : undefined;
  }

  list(prefix = "", limit = 100, options: MeshReadOptions = {}): MeshStateEntry[] {
    const bounded = Math.max(1, Math.min(Math.floor(limit), this.#context.maxReadEvents));
    return this.#select(prefix, options).slice(0, bounded).map((entry) => jsonClone(entry));
  }

  listAll(prefix = "", options: MeshReadOptions = {}): MeshStateEntry[] {
    const pinned = this.#pinned(options);
    if (!pinned) return this.#open().listAll(prefix);
    return this.#select(prefix, options).map((entry) => jsonClone(entry));
  }

  listAllShared(prefix = "", options: MeshReadOptions = {}): readonly Readonly<MeshStateEntry>[] {
    return this.#select(prefix, options);
  }

  /** One consistent snapshot, reused while the commit stamp is unchanged. */
  stateToken(_options: MeshReadOptions = {}): object {
    return this.#current();
  }

  stateStamp(): string | undefined {
    try { return this.#open().stateStamp(); } catch { return undefined; }
  }

  cachedStateStamp(_fresh = false, _revalidateGeneration = false): string | undefined {
    return this.stateStamp();
  }

  // ------------------------------------------------------------ writes

  async put(input: StateBackendPutInput): Promise<MeshStateEntry> {
    return this.#write((store) => store.put(input));
  }

  async delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }> {
    return this.#write((store) => store.delete(input));
  }

  async writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]> {
    for (const op of input.ops) validateMeshStateKey(op.key);
    if (input.ops.length === 0 && !input.prepare && !input.afterCommit && !input.commitOutbox) return [];
    const fileRead = input.fileRead;
    const retries = Math.max(0, Math.floor(fileRead?.retries ?? 3));
    for (let attempt = 0; ; attempt += 1) {
      const fileValue = fileRead?.read();
      const stamp = fileRead?.stamp();
      const userPrepare = input.prepare;
      const prepare = fileRead || userPrepare ? (view: MeshBatchView): MeshBatchOperation[] => {
        if (fileRead && fileRead.stamp() !== stamp) throw FILE_READ_CHANGED;
        return userPrepare?.(view, fileValue) ?? [];
      } : undefined;
      let results: MeshBatchResult[];
      try {
        results = await this.#write((store) => store.writeBatch({
          identity: input.identity, ops: input.ops,
          ...(prepare ? { prepare } : {}),
          ...(input.afterCommit ? { afterCommit: input.afterCommit } : {}),
        }));
      } catch (error) {
        if (error !== FILE_READ_CHANGED) throw error;
        if (attempt >= retries) throw new MeshStateFileReadChangedError(attempt + 1);
        continue;
      }
      if (input.commitOutbox) {
        // Captured at first use, right after COMMIT: this commit or a later one (sqlite has no
        // copy of the transaction's final state without re-acquiring custody).
        let captured: SqliteSnapshot | undefined;
        input.commitOutbox({
          backend: "sqlite",
          results: results.map((result) => ({ ...result })),
          changed: results.filter((result) => result.applied).map((result) => result.key),
          stamp: this.stateStamp(),
          view: snapshotView(() => (captured ??= this.#current())),
        });
      }
      return results;
    }
  }

  async confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    await this.#write((store) => store.confirmWritable(onAcquired));
  }

  dropCache(): void {
    this.#snapshot = undefined;
  }

  diagnostics(): StateBackendDiagnostics {
    return { kind: this.kind, busyTimeouts: this.#busyTimeouts, database: this.database };
  }

  close(): void {
    this.#closed = true;
    this.#snapshot = undefined;
    const store = this.#store;
    this.#store = undefined;
    store?.close();
  }

  // ------------------------------------------------------------ internals

  #pinned(options: MeshReadOptions): SqliteSnapshot | undefined {
    const token = options.snapshot;
    return token !== undefined && options.fresh !== true && this.#tokens.has(token) ? token as SqliteSnapshot : undefined;
  }

  #select(prefix: string, options: MeshReadOptions): readonly Readonly<MeshStateEntry>[] {
    if (prefix) validateMeshStateKey(prefix);
    const snapshot = this.#pinned(options) ?? this.#current();
    return prefix ? snapshot.sorted.filter((entry) => entry.key.startsWith(prefix)) : snapshot.sorted.slice();
  }

  #current(): SqliteSnapshot {
    const store = this.#open();
    const stamp = store.stateStamp();
    if (this.#snapshot?.stamp === stamp) return this.#snapshot;
    // The stamp is read first: a commit in between makes the snapshot newer than its stamp, so
    // the next call only rebuilds it once more.
    const exported = store.exportState();
    const entries = new Map<string, MeshStateEntry>();
    for (const [key, entry] of Object.entries(exported.entries)) entries.set(key, freezeEntry(entry));
    const sorted = Object.freeze([...entries.values()].sort((left, right) => left.key.localeCompare(right.key)));
    const snapshot: SqliteSnapshot = { stamp, entries, sorted, versions: exported.versions };
    this.#tokens.add(snapshot);
    this.#snapshot = snapshot;
    return snapshot;
  }

  #open(): SqliteStateStore {
    if (this.#store) return this.#store;
    if (this.#closed) throw new Error("Fabric mesh SQLite state backend is closed");
    // A synchronous read needs the database now. Opening is busy only while another connection
    // initialises the schema or holds the write lock at our first BEGIN IMMEDIATE: retry briefly.
    for (let attempt = 0; ; attempt += 1) {
      try {
        this.#store = SqliteStateStore.openSync(this.root, this.#context.maxEventBytes, this.#context.maxReadEvents, this.#storeOptions);
        return this.#store;
      } catch (error) {
        if (!sqliteBusy(error) || attempt >= 25) throw error;
        sleepSync(1 + attempt % 2);
      }
    }
  }

  async #openAsync(): Promise<SqliteStateStore> {
    if (this.#store) return this.#store;
    if (this.#closed) throw new Error("Fabric mesh SQLite state backend is closed");
    try {
      this.#store = SqliteStateStore.openSync(this.root, this.#context.maxEventBytes, this.#context.maxReadEvents, this.#storeOptions);
      return this.#store;
    } catch (error) {
      if (!sqliteBusy(error)) throw error;
    }
    this.#opening ??= SqliteStateStore.open(this.root, this.#context.maxEventBytes, this.#context.maxReadEvents, this.#storeOptions)
      .then((store) => {
        if (this.#store || this.#closed) { store.close(); if (this.#store) return this.#store; throw new Error("closed"); }
        this.#store = store;
        return store;
      })
      .finally(() => { this.#opening = undefined; });
    return this.#opening;
  }

  // The caller's budget: the mesh store's withTryLock scope (registry-fenced tries) bounds the
  // SQLite acquisition exactly as it bounds `.lock`; lockTimeoutMs and writeSignal are store options.
  async #write<T>(operation: (store: SqliteStateStore) => Promise<T>): Promise<T> {
    const started = performance.now();
    const scope = this.#context.lock.tryLockScope.getStore();
    let store: SqliteStateStore | undefined;
    try {
      store = await this.#openAsync();
      const ready = store;
      const result = scope?.active ? await ready.withTryLock(() => operation(ready), scope.timeoutMs) : await operation(ready);
      this.#snapshot = undefined;
      return result;
    } catch (error) {
      if (error instanceof MeshLockTimeoutError && !(error instanceof MeshStateBusyError)) {
        this.#busyTimeouts += 1;
        throw new MeshStateBusyError(store?.file ?? this.database, performance.now() - started, error);
      }
      throw error;
    }
  }
}

const SHADOW_IDENTITY: MeshIdentity = { id: "fabric-state-shadow", name: "fabric-state-shadow", kind: "agent" };
const MAX_DIVERGENT_KEYS = 32;
const sameEntry = (left: MeshStateEntry | undefined, right: MeshStateEntry | undefined): boolean =>
  left === undefined || right === undefined ? left === right
    : JSON.stringify(left.value) === JSON.stringify(right.value) && JSON.stringify(left.updatedBy) === JSON.stringify(right.updatedBy);

/**
 * `"shadow"`: state.json stays the authority for every read and write. After each committed write,
 * the committed values of its keys are applied, in the background, to a separate SQLite database
 * `<root>/state-shadow/state.db` (never `state.db`, which stays empty for the L4 import). The
 * shadow keeps values and writers, not revisions: it assigns its own versions, so divergence
 * compares presence, value and `updatedBy` only.
 *
 * Mirroring follows R11: the file values are read before `BEGIN IMMEDIATE` with the state.json stamp
 * taken first, and the stamp must still hold inside the transaction (a newer file commit retries),
 * so a slow mirror never overwrites a newer one. A SQLite failure never reaches the caller: it is
 * counted, and the next full reconcile (first use, `repair()`) heals it.
 *
 * `verify()` compares the whole file state with the shadow. A key counts as divergent only when
 * two consecutive checks see the same difference for the same file revision, so a mirror that
 * another process has not applied yet is not counted.
 */
export class ShadowStateBackend implements StateBackend {
  readonly kind = "shadow" as const;
  readonly #file: StateFile;
  readonly #shadow: SqliteStateBackend;
  readonly #budgetMs: number;
  readonly #pending = new Set<string>();
  #full = true;
  #running: Promise<void> | undefined;
  #timer: ReturnType<typeof setInterval> | undefined;
  #disabled: string | undefined;
  #applied = 0;
  #failures = 0;
  #divergences = 0;
  #divergentKeys: string[] = [];
  #suspects = new Map<string, string>();
  readonly #lockScope: MeshStoreContext["lock"]["tryLockScope"];

  constructor(context: MeshStoreContext, options: CreateStateBackendOptions = {}) {
    this.#file = new StateFile(context, options);
    this.#budgetMs = Math.max(1, Math.floor(options.shadowBudgetMs ?? 250));
    const shadowRoot = path.join(context.root, "state-shadow");
    // The shadow never shares the caller's writeSignal: it has its own small budget.
    const { writeSignal: _callerSignal, ...shadowOptions } = options;
    this.#lockScope = context.lock.tryLockScope;
    this.#shadow = new SqliteStateBackend(context, {}, shadowRoot, {
      ...sqliteOptions(shadowOptions, this.#budgetMs),
      // No maintainer runs for the shadow: keep its WAL small without one.
      emergencyCheckpointBytes: 8 * 1024 * 1024,
    });
    const verifyMs = Math.max(0, Math.floor(options.shadowVerifyMs ?? 60_000));
    if (verifyMs > 0) {
      this.#timer = setInterval(() => { void this.verify().catch(() => { this.#failures += 1; }); }, verifyMs);
      this.#timer.unref?.();
    }
  }

  /** The authority (state.json). */
  get file(): StateFile { return this.#file; }

  /** The shadow database backend (for tests and the L3/L4 tools; never the authority). */
  get shadow(): SqliteStateBackend { return this.#shadow; }

  get readCacheMs(): number { return this.#file.readCacheMs; }
  get backgroundReadCacheMs(): number { return this.#file.backgroundReadCacheMs; }
  get readCacheRemainingMs(): number { return this.#file.readCacheRemainingMs; }

  get(key: string, options?: MeshReadOptions): MeshStateEntry | undefined { return this.#file.get(key, options); }
  list(prefix?: string, limit?: number, options?: MeshReadOptions): MeshStateEntry[] { return this.#file.list(prefix, limit, options); }
  listAll(prefix?: string, options?: MeshReadOptions): MeshStateEntry[] { return this.#file.listAll(prefix, options); }
  listAllShared(prefix?: string, options?: MeshReadOptions): readonly Readonly<MeshStateEntry>[] {
    return this.#file.listAllShared(prefix, options);
  }
  stateToken(options?: MeshReadOptions): object { return this.#file.stateToken(options); }
  stateStamp(): string | undefined { return this.#file.stateStamp(); }
  cachedStateStamp(fresh?: boolean, revalidateGeneration?: boolean): string | undefined {
    return this.#file.cachedStateStamp(fresh, revalidateGeneration);
  }

  async put(input: StateBackendPutInput): Promise<MeshStateEntry> {
    const entry = await this.#file.put(input);
    this.#enqueue([input.key]);
    return entry;
  }

  async delete(input: StateBackendDeleteInput): Promise<{ deleted: boolean; version?: number }> {
    const result = await this.#file.delete(input);
    if (result.deleted) this.#enqueue([input.key]);
    return result;
  }

  async writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]> {
    const outbox = input.commitOutbox;
    const results = await this.#file.writeBatch(outbox ? { ...input, commitOutbox: (effects) => outbox({ ...effects, backend: "shadow" }) } : input);
    this.#enqueue(results.filter((result) => result.applied).map((result) => result.key));
    return results;
  }

  confirmWritable(onAcquired?: (at: number) => void): Promise<void> { return this.#file.confirmWritable(onAcquired); }

  dropCache(): void { this.#file.dropCache(); }

  diagnostics(): StateBackendDiagnostics {
    return {
      kind: this.kind, shadowApplied: this.#applied, shadowFailures: this.#failures, divergences: this.#divergences,
      divergentKeys: [...this.#divergentKeys], database: this.#shadow.database,
      ...(this.#disabled ? { fallback: this.#disabled } : {}),
    };
  }

  close(): void {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    this.#disabled ??= "closed";
    this.#pending.clear();
    this.#shadow.close();
  }

  /** Resolves once every mirror queued so far has been applied or has failed. Never rejects. */
  async flush(): Promise<void> {
    while (this.#running) await this.#running;
  }

  /**
   * Compares the whole file state with the shadow and returns the current differences. A key is
   * COUNTED (diagnostics().divergences) when the previous check saw the same difference for the same
   * file revision. Waits for this process's own queued mirrors first.
   */
  async verify(): Promise<StateDivergence[]> {
    await this.flush();
    if (this.#disabled) return [];
    const files = new Map(this.#file.listAll("", { fresh: true }).map((entry) => [entry.key, entry] as const));
    const shadows = new Map(this.#shadow.listAll("").map((entry) => [entry.key, entry] as const));
    const differences: StateDivergence[] = [];
    const suspects = new Map<string, string>();
    for (const key of new Set([...files.keys(), ...shadows.keys()])) {
      const file = files.get(key);
      const shadow = shadows.get(key);
      if (sameEntry(file, shadow)) continue;
      differences.push({
        key,
        file: file ? { value: file.value, updatedBy: file.updatedBy } : undefined,
        sqlite: shadow ? { value: shadow.value, updatedBy: shadow.updatedBy } : undefined,
      });
      const fingerprint = `${file?.version ?? 0}:${shadow?.version ?? 0}`;
      if (this.#suspects.get(key) === fingerprint) {
        this.#divergences += 1;
        this.#divergentKeys = [...this.#divergentKeys.filter((divergent) => divergent !== key), key].slice(-MAX_DIVERGENT_KEYS);
      } else suspects.set(key, fingerprint);
    }
    this.#suspects = suspects;
    return differences;
  }

  /** Reconciles the whole shadow to the file state (first use does this too). */
  async repair(): Promise<void> {
    this.#full = true;
    this.#kick();
    await this.flush();
  }

  #enqueue(keys: readonly string[]): void {
    if (this.#disabled || keys.length === 0) return;
    for (const key of keys) this.#pending.add(key);
    this.#kick();
  }

  #kick(): void {
    if (this.#running || this.#disabled) return;
    // Outside the caller's withTryLock scope: the mirror has its own budget and outlives the call.
    this.#running = this.#lockScope.exit(async () => {
      while (!this.#disabled && (this.#full || this.#pending.size > 0)) {
        const full = this.#full;
        const keys = [...this.#pending];
        this.#full = false;
        this.#pending.clear();
        try {
          await this.#mirror(full ? undefined : keys);
          this.#applied += 1;
        } catch (error) {
          this.#failures += 1;
          // A refused root or runtime stops the shadow for good; anything else waits for the next write.
          if ((error as { code?: unknown }).code === "FABRIC_MESH_STATE_UNSUPPORTED") {
            this.#disabled = String((error as Error).message);
          } else if (full) this.#full = true;
          break;
        }
      }
    }).finally(() => { this.#running = undefined; });
  }

  async #mirror(keys: readonly string[] | undefined): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      // R11: the stamp first, then the file reads, all before BEGIN IMMEDIATE.
      const stamp = this.#file.stateStamp();
      const wanted = new Map<string, MeshStateEntry | undefined>();
      if (keys) for (const key of keys) wanted.set(key, this.#file.get(key, { fresh: true }));
      else for (const entry of this.#file.listAll("", { fresh: true })) wanted.set(entry.key, entry);
      try {
        await this.#shadow.writeBatch({
          identity: SHADOW_IDENTITY, ops: [],
          prepare: (view) => {
            if (this.#file.stateStamp() !== stamp) throw FILE_READ_CHANGED;
            const all = keys ? [...wanted.keys()] : [...new Set([...wanted.keys(), ...view.listAll("").map((entry) => entry.key)])];
            const ops: MeshBatchOperation[] = [];
            for (const key of all) {
              const file = wanted.get(key);
              const shadow = view.get(key);
              if (sameEntry(file, shadow)) continue;
              ops.push(file ? { kind: "put", key, value: file.value, identity: file.updatedBy } : { kind: "delete", key });
            }
            return ops;
          },
        });
        return;
      } catch (error) {
        if (error !== FILE_READ_CHANGED || attempt >= 3) throw error;
      }
    }
  }
}
