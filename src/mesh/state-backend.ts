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
import type { MeshLockTimeoutError } from "../core/atomic-write.js";
import type { MeshIdentity } from "./event-log.js";
import type { MeshBatchOperation, MeshBatchResult, MeshBatchView, MeshReadOptions, MeshStateEntry } from "./state-file.js";

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
