/**
 * Keyed mesh state in SQLite WAL, one database per mesh root (smarty-dev#6477, lane L1).
 *
 * Not wired into anything yet: lane L2 selects the backend. This module is the adapter only.
 *
 * Contract (matches `MeshStore` in store.ts for get/list/listAll/put/delete/writeBatch):
 * - Every write runs the verified storage transition (`captureStoragePut`/`captureStorageDelete`)
 *   with `present`, `version` and `highWater` read from `kv`, `tombstones` and `meta` INSIDE the
 *   write transaction, so `proofs/storage-spec.bend` stays the authority. Revisions, the persistent
 *   high-water clock, CAS tombstones (capped at `maxStateTombstones`, oldest evicted first) and the
 *   32 MiB aggregate size cap behave as in the file store.
 * - `writeBatch`: `BEGIN IMMEDIATE`, then the synchronous `prepare(view)`, then every operation, all
 *   against ONE snapshot under the state write lock (bridge F1/F2 single-snapshot admission). A
 *   conflict with `onConflict: "abort"` (or any throw) rolls the whole batch back.
 * - `afterCommit` (review-luna P1-2, review-opus P2-2): never runs for a transaction that did not
 *   commit, and never extends the data transaction with file I/O. A no-op batch runs it inside its
 *   own (write-free) transaction, on the exact snapshot. A changing batch COMMITs first and then runs
 *   `afterCommit` under re-acquired state custody (`BEGIN IMMEDIATE`), so no other writer can commit
 *   while the effect runs and its `view` is the latest committed state (this batch's commit unless
 *   another writer committed in between; `stats().afterCommitIntervened` counts that). A crash
 *   between COMMIT and the effect leaves the commit and no effect: effects must be idempotent and
 *   reconciled by the next round (the bridge mirror and the participant heartbeat already are).
 * - Callbacks are synchronous and must not call this store's writers (a re-entrant write throws).
 *
 * Acquisition (review-opus P1-1; the design's FULL measurement): `DatabaseSync` is synchronous, so
 * SQLite's own busy handler would freeze the caller's event loop and is unfair (sleep-and-poll with
 * growing sleeps). `busy_timeout` is clamped to <= 5 ms (default 2 ms); a busy `BEGIN IMMEDIATE` is
 * retried ASYNCHRONOUSLY with short jittered sleeps capped at 4 ms (long waiters poll as often as
 * newcomers), and a waiter older than `fifoAfterMs` joins the existing advisory `MeshLockTicket` FIFO
 * (keyed on `state.db`, separate from the `.lock` queue). The wait honours `writeSignal`, the
 * `withTryLock` budget and `lockTimeoutMs`, and times out with the existing `MeshLockTimeoutError`.
 *
 * Durability (review-luna P1-3, review-opus P3-3): `synchronous=NORMAL`. A commit is atomic and
 * never torn; a process crash loses nothing committed; an OS crash or power loss can drop the most
 * recent commits that no checkpoint has synced yet. That equals today's `state.json`, which is
 * replaced by a rename without fsync, so no existing key family is weakened. No fsync ever runs
 * inside a data write transaction. A caller that needs a power-loss guarantee for a write calls
 * `await sync()` after it: a PASSIVE checkpoint (no writer lock) that confirms the WAL is synced
 * through that commit, or throws when it cannot within its budget.
 *
 * Checkpoints (review-opus P2-1): `wal_autocheckpoint=0` on every connection, so no client commit
 * runs a checkpoint. A store opened with `checkpoint: "maintainer"` (the L3 projector, the
 * maintenance holder, tests) checkpoints on an unref'd timer: PASSIVE first (copies and fsyncs
 * without blocking writers), then, only when the WAL file is above `checkpointBytes` and still
 * growing (constant readers keep it from restarting), TRUNCATE with a short busy budget. SQLite's own busy handler would lose the writer lock to writers retrying every
 * few ms, so the checkpointer raises `state-checkpoint.flag` and writers yield while it is fresh
 * (< 1 s, so a crashed checkpointer stalls nobody for longer). TRUNCATE then holds the writer lock,
 * so the WAL stops growing, and waits only for readers that started before it; new readers read
 * the database file. Reads are single statements (`.get()`/`.all()`, never an iterator or a read
 * transaction held across an await) and the budget doubles after each busy attempt (50 ms to
 * 400 ms), so constant readers cannot starve it. If no maintainer runs, any writer runs the same checkpoint after
 * its COMMIT once the WAL passes `emergencyCheckpointBytes`, so the WAL stays bounded regardless.
 * `journal_size_limit` caps a reset WAL. `stats()` exports the WAL size and checkpoint progress.
 *
 * Rules (design §3B, review-opus P0-1/P3-8): never open `state*.db*` with plain `fs` calls in the
 * same process (closing any descriptor drops that process's POSIX locks; only `stat` is used here);
 * local filesystems only (`filesystemRefusal`); back up with `VACUUM INTO` or the backup API; never
 * rename or delete `state.db*` while any process may have it open: retire it in place with
 * `retire()`, which every later write and read observes after `BEGIN IMMEDIATE`/`data_version` and
 * fails closed with `MeshStateRetiredError`. Lock order: actor registries, then the SQLite state
 * transaction; never acquire `.lock` inside a state transaction (callbacks are synchronous, so they
 * cannot await it). `exclusive(op)` gives the review's group (a) sites exclusion from state commits.
 *
 * Windows: `node:sqlite` ships with Node on Windows and WAL works on local NTFS. Differences that
 * matter here: locks are mandatory `LockFileEx` locks (plain-fs readers or copies of `state.db` can
 * fail), an open database cannot be renamed or deleted (another reason for in-place retirement),
 * and antivirus or indexers surface as transient `SQLITE_IOERR_*`/`SQLITE_CANTOPEN`, not BUSY, so
 * those codes get a bounded retry (exclude the mesh root from Defender scanning). UNC/network paths
 * are refused with `MeshStateUnsupportedError`; WAL needs a coherent shared memory map. A runtime
 * without `node:sqlite` (Node < 22.13, or a Bun build without it) is refused with the same error;
 * pass `open` to plug in another driver (for example `bun:sqlite`) through `SqliteConnection`.
 */
import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { MeshLockTimeoutError } from "../core/atomic-write.js";
import { captureStorageDelete, captureStoragePut, storageRevision, type StorageTransition } from "../verified/storage.js";
import { MeshLockTicket } from "./lock-queue.js";
// From the domain modules, not the store.ts facade: store.ts loads this module through state-backend.ts (L2a).
import { MeshBatchConflictError, type MeshBatchOperation, type MeshBatchResult, type MeshBatchView,
  type MeshReadOptions, type MeshStateEntry } from "./state-file.js";
import type { MeshIdentity } from "./event-log.js";

export type SqliteValue = null | number | bigint | string | Uint8Array;
export type SqliteRow = Record<string, unknown>;

/** The driver surface this module needs: node:sqlite's DatabaseSync, or an adapter (bun:sqlite). */
export interface SqliteStatement {
  run(...params: SqliteValue[]): { changes: number | bigint; lastInsertRowid: number | bigint };
  get(...params: SqliteValue[]): SqliteRow | undefined;
  all(...params: SqliteValue[]): SqliteRow[];
}

export interface SqliteConnection {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
  close(): void;
  readonly isTransaction: boolean;
}

export type SqliteOpener = (file: string) => SqliteConnection;

export class MeshStateUnsupportedError extends Error {
  readonly code = "FABRIC_MESH_STATE_UNSUPPORTED";
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "MeshStateUnsupportedError";
  }
}

/** The database was retired (rolled back to the file backend) or re-epoched: use the file path. */
export class MeshStateRetiredError extends Error {
  readonly code = "FABRIC_MESH_STATE_RETIRED";
  constructor(readonly backend: string, readonly epoch: number, readonly expectedEpoch: number) {
    super(`Fabric mesh state database is ${backend} at epoch ${epoch} (this store opened epoch ${expectedEpoch})`);
    this.name = "MeshStateRetiredError";
  }
}

export interface SqliteStateStoreOptions {
  /** Upper bound on the projected state.json (old readers throw above 32 MiB). Default 32 MiB. */
  maxStateBytes?: number;
  /** Retained CAS tombstones; the oldest are evicted first. Default 1,000 (as the file store). */
  maxStateTombstones?: number;
  /** Whole asynchronous acquisition budget. Default 10 s (as the file store). */
  lockTimeoutMs?: number;
  /** SQLite's synchronous busy handler, clamped to 0..5 ms. Default 2 ms. */
  busyTimeoutMs?: number;
  /** A waiter older than this joins the advisory FIFO ticket queue. Default 25 ms. */
  fifoAfterMs?: number;
  /** Host-owned lifetime of writes: abort stops acquisition, never a started transaction. */
  writeSignal?: AbortSignal;
  /** "maintainer" runs the checkpoint timer (projector or maintenance holder). Default "client". */
  checkpoint?: "maintainer" | "client";
  /** A WAL file above this size that grew since the last checkpoint escalates PASSIVE to TRUNCATE. Default 4 MiB. */
  checkpointBytes?: number;
  /** WAL size above which any writer checkpoints after its COMMIT. Default 64 MiB. */
  emergencyCheckpointBytes?: number;
  /** Maintainer timer period. Default 1,000 ms. */
  checkpointIntervalMs?: number;
  /** First busy budget of a TRUNCATE attempt; doubles after each busy attempt up to 400 ms. Default 50 ms. */
  checkpointBusyMs?: number;
  /** `journal_size_limit`. Default 16 MiB. */
  journalSizeLimitBytes?: number;
  /** Rows kept in the `changes` feed. Default 4,096. */
  changesRetained?: number;
  /** Driver adapter. Default: node:sqlite, loaded at first use. */
  open?: SqliteOpener;
}

export interface SqliteStateStats {
  transactions: number;
  commits: number;
  busyRetries: number;
  transientRetries: number;
  fifoJoins: number;
  maxWaitMs: number;
  maxHoldMs: number;
  totalHoldMs: number;
  maxPrepareMs: number;
  maxAfterCommitMs: number;
  afterCommitReacquired: number;
  afterCommitIntervened: number;
  checkpoints: { passive: number; truncate: number; truncateBusy: number; emergency: number; failed: number; writerYields: number };
  lastCheckpoint?: { busy: number; log: number; checkpointed: number };
  walBytes: number;
  maxWalBytes: number;
}

/** The state in the file store's envelope shape (minus readGeneration), for projection and rollback. */
export interface SqliteStateExport {
  format: 1;
  revisionFormat: 2;
  entries: Record<string, MeshStateEntry>;
  versions: Record<string, number>;
  tombstoneOrder: string[];
  highWater: number;
  backend: string;
  epoch: number;
  commit: number;
}

export interface SqliteStateChanges {
  commit: number;
  /** False when the feed was trimmed past `after`: the reader must rescan. */
  complete: boolean;
  changes: Array<{ commit: number; key: string; version: number; deleted: boolean }>;
}

const SCHEMA_VERSION = 1;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
const DEFAULT_MAX_STATE_TOMBSTONES = 1_000;
const LOCK_TIMEOUT_MS = 10_000;
const MAX_BUSY_TIMEOUT_MS = 5;
const TRANSIENT_RETRIES = 5;
const ENVELOPE_BYTES = 256;
// A TRUNCATE checkpoint must win the writer lock against writers that retry every few ms; SQLite's
// own busy handler would lose that race (the FULL unfairness of design §1.5). Writers therefore
// yield while this flag is fresh. It is a separate inode, never one of the state.db* files.
const CHECKPOINT_FLAG = "state-checkpoint.flag";
const CHECKPOINT_FLAG_STALE_MS = 1_000;
const MAX_TRUNCATE_BUDGET_MS = 400;
const KEY_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,255}$/;
// Every key character is below U+007F, so [prefix, prefix + U+007F) is exactly the prefix range.
const PREFIX_END = "\u007f";
const SQLITE_BUSY = 5;
const SQLITE_LOCKED = 6;
const SQLITE_IOERR = 10;
const SQLITE_CANTOPEN = 14;
// WAL needs a coherent shared memory map between processes on one host (review-opus P2-7).
const NON_LOCAL_FILESYSTEMS = new Set([
  "nfs", "nfs4", "cifs", "smb3", "smbfs", "9p", "virtiofs", "fuse.sshfs", "sshfs", "fuse.rclone", "fuse.gcsfuse",
  "fuse.s3fs", "ceph", "fuse.ceph", "glusterfs", "fuse.glusterfs", "lustre", "afs", "davfs", "fuse.davfs2", "gpfs",
  "ncpfs", "coda", "fuse.vmhgfs-fuse", "vboxsf", "prl_fs",
]);
// statfs magic numbers, used only when /proc/self/mountinfo is unreadable.
const NON_LOCAL_MAGIC = new Map<number, string>([
  [0x6969, "nfs"], [0x517b, "smbfs"], [0xff534d42, "cifs"], [0xfe534d42, "smb2"], [0x01021997, "9p"],
]);

const sqliteCode = (error: unknown): number | undefined => {
  const code = (error as { errcode?: unknown } | null)?.errcode;
  return typeof code === "number" ? code & 0xff : undefined;
};
const isBusy = (error: unknown): boolean => {
  const code = sqliteCode(error);
  return code === SQLITE_BUSY || code === SQLITE_LOCKED;
};
const isTransient = (error: unknown): boolean => {
  const code = sqliteCode(error);
  return code === SQLITE_IOERR || code === SQLITE_CANTOPEN;
};

export const validateMeshStateKey = (key: string): void => {
  const unsafeSegment = key.split(/[/:]/)
    .some(segment => segment === "__proto__" || segment === "prototype" || segment === "constructor");
  if (!KEY_PATTERN.test(key) || unsafeSegment) throw new Error(`Invalid Fabric mesh key: ${key}`);
};

const unescapeMount = (value: string): string => value.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(parseInt(octal, 8)));

const nearestExisting = (target: string): string => {
  let current = path.resolve(target);
  for (;;) {
    try { return fs.realpathSync(current); } catch { /* walk up */ }
    const parent = path.dirname(current);
    if (parent === current) return current;
    current = parent;
  }
};

/**
 * Why a root cannot hold a SQLite WAL database, or undefined when it can. Refuses network and
 * virtual filesystems (NFS, SMB/CIFS, sshfs, 9p, virtiofs, ...) and Windows UNC paths. macOS and
 * mapped Windows drives cannot be classified without native code: they are documented as local-only.
 */
export const filesystemRefusal = (root: string,
  input: { platform?: NodeJS.Platform; mountinfo?: string } = {}): string | undefined => {
  const platform = input.platform ?? process.platform;
  if (platform === "win32") {
    const normalized = root.replace(/\//g, "\\");
    if (/^\\\\\?\\UNC\\/i.test(normalized)) return `network path ${root}`;
    if (/^\\\\[?.]\\/.test(normalized)) return undefined;
    if (normalized.startsWith("\\\\")) return `network path ${root}`;
    return undefined;
  }
  if (platform !== "linux") return undefined;
  // A supplied mountinfo describes a Linux system: resolve with POSIX rules whatever the host is.
  const target = input.mountinfo === undefined ? nearestExisting(root) : path.posix.resolve(root);
  let mountinfo = input.mountinfo;
  if (mountinfo === undefined) {
    try { mountinfo = fs.readFileSync("/proc/self/mountinfo", "utf8"); } catch { mountinfo = undefined; }
  }
  if (mountinfo !== undefined) {
    let best: { mount: string; type: string } | undefined;
    for (const line of mountinfo.split("\n")) {
      const separator = line.indexOf(" - ");
      if (separator < 0) continue;
      const mount = unescapeMount(line.slice(0, separator).split(" ")[4] ?? "");
      const type = line.slice(separator + 3).split(" ")[0] ?? "";
      if (!mount.startsWith("/")) continue;
      const inside = target === mount || mount === "/" || target.startsWith(mount.endsWith("/") ? mount : `${mount}/`);
      if (inside && (!best || mount.length >= best.mount.length)) best = { mount, type };
    }
    if (best) return NON_LOCAL_FILESYSTEMS.has(best.type) ? `${best.type} filesystem at ${best.mount}` : undefined;
  }
  try {
    const type = Number(fs.statfsSync(target).type) >>> 0;
    const name = NON_LOCAL_MAGIC.get(type);
    return name ? `${name} filesystem at ${target}` : undefined;
  } catch { return undefined; }
};

/** The default driver: node:sqlite `DatabaseSync` behind the `SqliteConnection` adapter surface. */
export const openNodeSqlite: SqliteOpener = (file) => {
  // Loaded at first use (AGENTS.md startup budget). Bun 1.4 also provides node:sqlite.
  let sqlite: typeof import("node:sqlite") | undefined;
  try { sqlite = process.getBuiltinModule?.("node:sqlite"); }
  catch (error) { throw new MeshStateUnsupportedError("node:sqlite is unavailable in this runtime (Node >= 22.13 required)", { cause: error }); }
  if (!sqlite?.DatabaseSync) throw new MeshStateUnsupportedError("node:sqlite is unavailable in this runtime (Node >= 22.13 required)");
  const database = new sqlite.DatabaseSync(file, { timeout: 0 });
  return {
    exec: (sql) => { database.exec(sql); },
    prepare: (sql) => {
      const statement = database.prepare(sql);
      return {
        run: (...params) => statement.run(...params),
        get: (...params) => statement.get(...params),
        all: (...params) => statement.all(...params),
      };
    },
    close: () => { database.close(); },
    get isTransaction() { return database.isTransaction; },
  };
};

const delay = (ms: number, signal?: AbortSignal): Promise<void> => new Promise((resolve, reject) => {
  if (signal?.aborted) { reject(signal.reason); return; }
  const onAbort = (): void => { clearTimeout(timer); reject(signal!.reason); };
  const timer = setTimeout(() => { signal?.removeEventListener("abort", onAbort); resolve(); }, Math.max(0, ms));
  signal?.addEventListener("abort", onAbort, { once: true });
});

const bytes = (text: string): number => Buffer.byteLength(text, "utf8");
// Upper-bound share of the projected state.json: the entry, its map key and its versions slot.
const entryBytes = (key: string, value: string, updatedBy: string): number => bytes(value) + bytes(updatedBy) + 3 * bytes(key) + 96;
const tombstoneBytes = (key: string): number => 2 * bytes(key) + 32;

const toEntry = (row: SqliteRow): MeshStateEntry => ({
  key: String(row.key),
  value: JSON.parse(String(row.value)) as unknown,
  version: Number(row.version),
  updatedAt: Number(row.updated_at),
  updatedBy: JSON.parse(String(row.updated_by)) as MeshIdentity,
});

interface Slot { present: boolean; version: number; bytes: number; tombstone: boolean }

interface Tx {
  highWater: number;
  commit: number;
  stateBytes: number;
  tombstoneOrd: number;
  grew: boolean;
  deleted: boolean;
  /** Finish metadata (clock, commit, ordinal) even without a change row: set by importState. */
  finish: boolean;
  changes: Array<{ key: string; version: number; deleted: boolean }>;
}

type Statements = ReturnType<typeof prepareStatements>;

const prepareStatements = (db: SqliteConnection) => ({
  metaAll: db.prepare("SELECT name, value FROM meta"),
  // One statement (one snapshot) of the rows a change stamp and a liveness check need (PK lookups).
  metaStamp: db.prepare("SELECT name, value FROM meta WHERE name IN ('commit_no', 'epoch', 'backend')"),
  metaGet: db.prepare("SELECT value FROM meta WHERE name = ?"),
  metaSet: db.prepare("UPDATE meta SET value = ? WHERE name = ?"),
  kvGet: db.prepare("SELECT key, value, version, updated_at, updated_by, bytes FROM kv WHERE key = ?"),
  kvRange: db.prepare("SELECT key, value, version, updated_at, updated_by FROM kv WHERE key >= ? AND key < ?"),
  kvAll: db.prepare("SELECT key, value, version, updated_at, updated_by FROM kv"),
  kvCount: db.prepare("SELECT count(*) AS n FROM kv"),
  kvUpsert: db.prepare(`INSERT INTO kv(key, value, version, updated_at, updated_by, bytes) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, version = excluded.version, updated_at = excluded.updated_at,
    updated_by = excluded.updated_by, bytes = excluded.bytes`),
  kvDelete: db.prepare("DELETE FROM kv WHERE key = ?"),
  tombGet: db.prepare("SELECT version FROM tombstones WHERE key = ?"),
  tombUpsert: db.prepare(`INSERT INTO tombstones(key, version, ord) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET version = excluded.version, ord = excluded.ord`),
  tombDelete: db.prepare("DELETE FROM tombstones WHERE key = ?"),
  tombCount: db.prepare("SELECT count(*) AS n FROM tombstones"),
  tombCutoff: db.prepare("SELECT ord FROM tombstones ORDER BY ord DESC LIMIT 1 OFFSET ?"),
  tombEvict: db.prepare("DELETE FROM tombstones WHERE ord <= ? RETURNING key"),
  tombAll: db.prepare("SELECT key, version FROM tombstones ORDER BY ord"),
  changeInsert: db.prepare("INSERT INTO changes(commit_no, key, version, deleted) VALUES (?, ?, ?, ?)"),
  // Trims whole commits only: the commit holding row `seq` goes entirely, so a reader never sees half of one.
  changeTrim: db.prepare("DELETE FROM changes WHERE commit_no <= (SELECT commit_no FROM changes WHERE seq <= ? ORDER BY seq DESC LIMIT 1)"),
  changesSince: db.prepare("SELECT commit_no, key, version, deleted FROM changes WHERE commit_no > ? ORDER BY seq"),
  changesOldest: db.prepare("SELECT min(commit_no) AS oldest FROM changes"),
  dataVersion: db.prepare("PRAGMA data_version"),
  checkpointPassive: db.prepare("PRAGMA wal_checkpoint(PASSIVE)"),
  checkpointTruncate: db.prepare("PRAGMA wal_checkpoint(TRUNCATE)"),
});

const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta(name TEXT PRIMARY KEY NOT NULL, value) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL, version INTEGER NOT NULL,
  updated_at INTEGER NOT NULL, updated_by TEXT NOT NULL, bytes INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS tombstones(key TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL, ord INTEGER NOT NULL UNIQUE) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS changes(seq INTEGER PRIMARY KEY, commit_no INTEGER NOT NULL, key TEXT NOT NULL,
  version INTEGER NOT NULL, deleted INTEGER NOT NULL);
`;

export class SqliteStateStore {
  readonly file: string;
  readonly #db: SqliteConnection;
  readonly #sql: Statements;
  readonly #epoch: number;
  readonly #storeId: string;
  readonly #maxStateBytes: number;
  readonly #maxTombstones: number;
  readonly #lockTimeoutMs: number;
  readonly #busyTimeoutMs: number;
  readonly #fifoAfterMs: number;
  readonly #signal: AbortSignal | undefined;
  readonly #checkpointBytes: number;
  readonly #emergencyBytes: number;
  readonly #checkpointBusyMs: number;
  readonly #changesRetained: number;
  readonly #tryLockScope = new AsyncLocalStorage<{ active: boolean; timeoutMs: number }>();
  readonly #stats: SqliteStateStats = {
    transactions: 0, commits: 0, busyRetries: 0, transientRetries: 0, fifoJoins: 0, maxWaitMs: 0, maxHoldMs: 0,
    totalHoldMs: 0, maxPrepareMs: 0, maxAfterCommitMs: 0, afterCommitReacquired: 0, afterCommitIntervened: 0,
    checkpoints: { passive: 0, truncate: 0, truncateBusy: 0, emergency: 0, failed: 0, writerYields: 0 }, walBytes: 0, maxWalBytes: 0,
  };
  #timer: NodeJS.Timeout | undefined;
  #inTransaction = false;
  #closed = false;
  #dataVersion = -1;
  #lastEmergencyCheck = 0;
  #truncateBudgetMs: number;
  #lastWalBytes = 0;

  private constructor(readonly root: string, readonly maxEventBytes: number, readonly maxReadEvents: number,
    db: SqliteConnection, file: string, identity: { epoch: number; storeId: string }, options: SqliteStateStoreOptions) {
    this.file = file;
    this.#db = db;
    this.#sql = prepareStatements(db);
    this.#epoch = identity.epoch;
    this.#storeId = identity.storeId;
    this.#maxStateBytes = Math.max(maxEventBytes * 2, Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES));
    this.#maxTombstones = Math.max(0, Math.floor(options.maxStateTombstones ?? DEFAULT_MAX_STATE_TOMBSTONES));
    this.#lockTimeoutMs = Math.max(0, options.lockTimeoutMs ?? LOCK_TIMEOUT_MS);
    this.#busyTimeoutMs = clampBusy(options.busyTimeoutMs);
    this.#fifoAfterMs = Math.max(0, options.fifoAfterMs ?? 25);
    this.#signal = options.writeSignal;
    this.#checkpointBytes = Math.max(0, options.checkpointBytes ?? 4 * 1024 * 1024);
    this.#emergencyBytes = Math.max(this.#checkpointBytes, options.emergencyCheckpointBytes ?? 64 * 1024 * 1024);
    this.#checkpointBusyMs = Math.max(1, Math.min(MAX_TRUNCATE_BUDGET_MS, options.checkpointBusyMs ?? 50));
    this.#truncateBudgetMs = this.#checkpointBusyMs;
    this.#changesRetained = Math.max(1, Math.floor(options.changesRetained ?? 4_096));
    if (options.checkpoint === "maintainer") {
      const interval = Math.max(10, options.checkpointIntervalMs ?? 1_000);
      this.#timer = setInterval(() => {
        try { if (this.walBytes() > 0) this.checkpoint(); } catch { this.#stats.checkpoints.failed += 1; }
      }, interval);
      this.#timer.unref();
    }
  }

  /**
   * Opens (creating when absent) `<root>/state.db`. Initialisation (WAL setup, schema, seed) retries
   * asynchronously while busy, for `initTimeoutMs` (default `lockTimeoutMs`); then the driver's busy
   * error is thrown. `initTimeoutMs` bounds only this open, never the store's later writes.
   */
  static async open(root: string, maxEventBytes: number, maxReadEvents: number,
    options: SqliteStateStoreOptions = {}, initTimeoutMs?: number): Promise<SqliteStateStore> {
    const refusal = filesystemRefusal(root);
    if (refusal) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state needs a local filesystem: ${refusal}`);
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    const file = path.join(root, "state.db");
    // Create mode 0600 before SQLite opens it (its -wal/-shm inherit the mode). O_EXCL: when this
    // succeeds no connection in this process can have the file open, so closing drops no lock.
    try { fs.closeSync(fs.openSync(file, "wx", 0o600)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    const db = (options.open ?? openNodeSqlite)(file);
    const deadline = Date.now() + Math.max(0, initTimeoutMs ?? options.lockTimeoutMs ?? LOCK_TIMEOUT_MS);
    try {
      let transient = 0;
      for (;;) {
        options.writeSignal?.throwIfAborted();
        try {
          const identity = initialise(db, options);
          return new SqliteStateStore(path.resolve(root), maxEventBytes, maxReadEvents, db, file, identity, options);
        } catch (error) {
          if (db.isTransaction) try { db.exec("ROLLBACK"); } catch { /* already rolled back */ }
          const retry = isBusy(error) || (isTransient(error) && transient++ < TRANSIENT_RETRIES);
          if (!retry || Date.now() >= deadline) throw error;
          await delay(2 + Math.random() * 4, options.writeSignal);
        }
      }
    } catch (error) {
      try { db.close(); } catch { /* best effort */ }
      throw error;
    }
  }

  /**
   * One synchronous open attempt (lane L2a: the first synchronous read of a sqlite-backed MeshStore).
   * Throws the driver's busy error unchanged; the caller decides whether and how to retry.
   */
  static openSync(root: string, maxEventBytes: number, maxReadEvents: number,
    options: SqliteStateStoreOptions = {}): SqliteStateStore {
    const refusal = filesystemRefusal(root);
    if (refusal) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state needs a local filesystem: ${refusal}`);
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    const file = path.join(root, "state.db");
    try { fs.closeSync(fs.openSync(file, "wx", 0o600)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    const db = (options.open ?? openNodeSqlite)(file);
    try {
      return new SqliteStateStore(path.resolve(root), maxEventBytes, maxReadEvents, db, file, initialise(db, options), options);
    } catch (error) {
      if (db.isTransaction) try { db.exec("ROLLBACK"); } catch { /* already rolled back */ }
      try { db.close(); } catch { /* best effort */ }
      throw error;
    }
  }

  // ---------------------------------------------------------------- reads

  get(key: string, _options: MeshReadOptions = {}): MeshStateEntry | undefined {
    validateMeshStateKey(key);
    this.#assertReadable();
    const row = this.#sql.kvGet.get(key);
    return row ? toEntry(row) : undefined;
  }

  list(prefix = "", limit = 100, options: MeshReadOptions = {}): MeshStateEntry[] {
    const bounded = Math.max(1, Math.min(Math.floor(limit), this.maxReadEvents));
    return this.listAll(prefix, options).slice(0, bounded);
  }

  /** One statement, one snapshot; ordered exactly as the file store (localeCompare). */
  listAll(prefix = "", _options: MeshReadOptions = {}): MeshStateEntry[] {
    if (prefix) validateMeshStateKey(prefix);
    this.#assertReadable();
    return this.#scan(prefix);
  }

  /**
   * Changes whenever any connection commits a state change (review-opus P1-6) OR retires or
   * re-epochs the database: `<store>:<epoch>:<commit>` with the CURRENT epoch read from state.db,
   * plus `:<backend>:<opened epoch>` once this store no longer reads a live database. Retirement
   * leaves commit_no unchanged, so a stamp built from the epoch this store opened would let a cache
   * keyed on it outlive the retirement (pi-fabric#626 review round 3). One statement: the
   * backend, epoch and commit come from one snapshot.
   */
  stateStamp(): string {
    this.#assertOpen();
    const meta = this.#stampMeta();
    const live = meta.backend === "sqlite" && meta.epoch === this.#epoch;
    return `${this.#storeId}:${meta.epoch}:${meta.commit}${live ? "" : `:${meta.backend}:${this.#epoch}`}`;
  }

  /**
   * Throws MeshStateRetiredError when state.db was retired or re-epoched since this store opened it
   * (cheap: a full check only after another connection committed). For readers that serve a copy.
   */
  assertLive(): void {
    this.#assertReadable();
  }

  /** Cheap per-connection "did another connection commit?" counter (PRAGMA data_version). */
  dataVersion(): number {
    this.#assertOpen();
    return Number(this.#sql.dataVersion.get()?.data_version ?? 0);
  }

  /** The trimmed change feed after a commit number, for incremental readers and the projector. */
  changesSince(after: number): SqliteStateChanges {
    this.#assertReadable();
    return this.#readTransaction(() => {
      // Retirement re-checked in the feed's own read transaction (pi-fabric#626 review round 3).
      const { commit } = this.#assertLiveMeta(this.#stampMeta());
      const oldest = this.#sql.changesOldest.get()?.oldest;
      const complete = after >= commit || (oldest !== null && oldest !== undefined && Number(oldest) <= after + 1);
      const changes = complete ? this.#sql.changesSince.all(after).map(row => ({
        commit: Number(row.commit_no), key: String(row.key), version: Number(row.version), deleted: Number(row.deleted) === 1,
      })) : [];
      return { commit, complete, changes };
    });
  }

  /**
   * One consistent snapshot in the file store's envelope shape. Maintenance (rollback, projection)
   * exports a retired database too; `live: true` (every read path, e.g. a backend snapshot rebuild)
   * checks retirement and epoch in the SAME read transaction as the rows and throws
   * MeshStateRetiredError instead of exporting retired state (pi-fabric#626 review round 3).
   */
  exportState(options: { live?: boolean } = {}): SqliteStateExport {
    this.#assertOpen();
    return this.#readTransaction(() => {
      const meta = this.#meta();
      if (options.live) this.#assertLiveMeta(meta);
      const entries: Record<string, MeshStateEntry> = {};
      const versions: Record<string, number> = {};
      for (const row of this.#sql.kvAll.all()) {
        const entry = toEntry(row);
        entries[entry.key] = entry;
        versions[entry.key] = entry.version;
      }
      const tombstoneOrder: string[] = [];
      for (const row of this.#sql.tombAll.all()) {
        tombstoneOrder.push(String(row.key));
        versions[String(row.key)] = Number(row.version);
      }
      return { format: 1, revisionFormat: 2, entries, versions, tombstoneOrder, highWater: meta.highWater,
        backend: meta.backend, epoch: meta.epoch, commit: meta.commit };
    });
  }

  // ---------------------------------------------------------------- writes

  async put(input: { key: string; value: unknown; identity: MeshIdentity; ifVersion?: number }): Promise<MeshStateEntry> {
    const { key, value, identity, ifVersion } = input;
    validateMeshStateKey(key);
    const request = captureStoragePut({ key, value, identity, ifVersion }, this.maxEventBytes);
    return this.#write((tx) => {
      const slot = this.#slot(request.key);
      const plan = request.transition(slot.present, slot.version, tx.highWater);
      if (plan.kind !== "put") throw new Error("Invalid verified storage put plan");
      return this.#applyPut(tx, slot, plan, Date.now());
    });
  }

  async delete(input: { key: string; ifVersion?: number }): Promise<{ deleted: boolean; version?: number }> {
    const { key, ifVersion } = input;
    validateMeshStateKey(key);
    const request = captureStorageDelete({ key, ifVersion });
    return this.#write<{ deleted: boolean; version?: number }>((tx) => {
      const slot = this.#slot(request.key);
      const plan = request.transition(slot.present, slot.version, tx.highWater);
      if (plan.kind === "unchanged") return { deleted: false };
      if (plan.kind !== "delete") throw new Error("Invalid verified storage delete plan");
      this.#applyDelete(tx, slot, plan);
      return { deleted: true, version: plan.version };
    });
  }

  /**
   * Several puts and deletes in ONE transaction, with put()/delete() semantics per operation and an
   * optional compare-and-swap (`onConflict`: "skip" leaves the key, "abort" rolls everything back).
   * `prepare` builds more operations from the same snapshot; `afterCommit` runs only after a
   * successful commit, under state custody (see the module comment). One result per operation.
   */
  async writeBatch(input: {
    identity: MeshIdentity;
    ops: MeshBatchOperation[];
    prepare?: (view: MeshBatchView) => MeshBatchOperation[];
    afterCommit?: (view: MeshBatchView) => void;
  }): Promise<MeshBatchResult[]> {
    for (const op of input.ops) validateMeshStateKey(op.key);
    if (input.ops.length === 0 && !input.prepare && !input.afterCommit) return [];
    let committed: number | undefined;
    const results = await this.#write((tx) => {
      const view = this.#view();
      const started = performance.now();
      const prepared = input.prepare?.(view) ?? [];
      if (typeof (prepared as unknown as { then?: unknown }).then === "function") {
        throw new Error("Fabric mesh writeBatch prepare must be synchronous");
      }
      this.#stats.maxPrepareMs = Math.max(this.#stats.maxPrepareMs, performance.now() - started);
      const ops = [...input.ops, ...prepared];
      for (const op of ops) validateMeshStateKey(op.key);
      const results: MeshBatchResult[] = [];
      const now = Date.now();
      for (const op of ops) {
        const slot = this.#slot(op.key);
        if (op.kind === "delete" && op.condition && !op.condition(view.get)) {
          results.push({ key: op.key, applied: false, version: slot.version });
          continue;
        }
        if (op.ifVersion !== undefined && op.ifVersion !== slot.version) {
          const policy = typeof op.onConflict === "function"
            ? op.onConflict(slot.present ? this.#entry(op.key) : undefined) : op.onConflict ?? "abort";
          if (policy === "abort") throw new MeshBatchConflictError(op.key, op.ifVersion, slot.version);
          results.push({ key: op.key, applied: false, version: slot.version });
          continue;
        }
        const request = op.kind === "delete"
          ? captureStorageDelete({ key: op.key, ifVersion: op.ifVersion })
          : captureStoragePut({
            key: op.key, ifVersion: op.ifVersion, identity: op.identity ?? input.identity,
            value: typeof op.value === "function" ? (op.value as (now: number) => unknown)(now) : op.value,
          }, this.maxEventBytes);
        const plan = request.transition(slot.present, slot.version, tx.highWater);
        if (plan.kind === "unchanged") {
          results.push({ key: op.key, applied: false, version: slot.version });
          continue;
        }
        if (plan.kind === "delete") this.#applyDelete(tx, slot, plan);
        else this.#applyPut(tx, slot, plan, now);
        results.push({ key: op.key, applied: true, version: plan.version });
      }
      if (tx.changes.length > 0) committed = tx.commit + 1;
      // A write-free batch cannot fail to commit: run the effect on the exact snapshot.
      else if (input.afterCommit) this.#timedAfterCommit(input.afterCommit, view);
      return results;
    });
    if (committed !== undefined && input.afterCommit) {
      const afterCommit = input.afterCommit;
      this.#stats.afterCommitReacquired += 1;
      await this.#write((tx) => {
        if (tx.commit !== committed) this.#stats.afterCommitIntervened += 1;
        this.#timedAfterCommit(afterCommit, this.#view());
      });
    }
    return results;
  }

  /** Bounds every acquisition in this async step (the registry-fenced try of the file store). */
  async withTryLock<T>(operation: () => Promise<T>, timeoutMs = 0): Promise<T> {
    if (this.#tryLockScope.getStore()?.active) return operation();
    const scope = { active: true, timeoutMs: Math.max(0, timeoutMs) };
    return this.#tryLockScope.run(scope, async () => {
      try { return await operation(); }
      finally { scope.active = false; }
    });
  }

  /** Runs a synchronous operation excluded from every state commit (review-opus P1-2 group (a)). */
  async exclusive<T>(operation: () => T, lockTimeoutMs?: number): Promise<T> {
    return this.#write(() => operation(), lockTimeoutMs);
  }

  /**
   * The R20 write fence (smarty-dev#6477 L2b): ONE synchronous `BEGIN IMMEDIATE` attempt (bounded by
   * `busy_timeout`, <= 5 ms), `operation` on that transaction's snapshot, then ROLLBACK. No other
   * connection commits while it runs, and it writes nothing. It never waits asynchronously, so a
   * caller that holds `.lock` (lock order: `.lock`, then this) never holds `.lock` through a SQLite
   * wait: a busy write lock throws `MeshLockTimeoutError` before `operation` runs, and the caller
   * retries after releasing `.lock`. `operation` must be synchronous and must not call writers.
   */
  fenceSync<T>(operation: () => T): T {
    this.#assertOpen();
    if (this.#inTransaction) throw new Error("Fabric mesh state callbacks must not call store writers");
    try {
      this.#db.exec("BEGIN IMMEDIATE");
    } catch (error) {
      if (!isBusy(error) && !isTransient(error)) throw error;
      this.#stats.busyRetries += 1;
      throw new MeshLockTimeoutError(` (SQLite state ${this.file}, write fence)`, 1, 0);
    }
    this.#inTransaction = true;
    try {
      const meta = this.#meta();
      if (meta.backend !== "sqlite" || meta.epoch !== this.#epoch) throw new MeshStateRetiredError(meta.backend, meta.epoch, this.#epoch);
      const result = operation();
      if (result !== null && typeof result === "object" && typeof (result as { then?: unknown }).then === "function") {
        throw new Error("Fabric mesh state fences must be synchronous");
      }
      return result;
    } finally {
      try { if (this.#db.isTransaction) this.#db.exec("ROLLBACK"); } catch { /* connection ends it */ }
      this.#inTransaction = false;
    }
  }

  /** Proves the state is writable now (and not retired): acquires and releases the write lock. */
  async confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    await this.#write(() => { onAcquired?.(Date.now()); });
  }

  /**
   * Retires the database in place (review-opus P0-1): every later write and every read, through this
   * or another store, fails with MeshStateRetiredError. Never rename or delete the files while open.
   */
  async retire(): Promise<number> {
    return this.#write((tx) => {
      const epoch = this.#epoch + 1;
      // data_version does not move for this connection's own commits: drop the cached value so the
      // next read on this store re-checks the retirement flag (review round 2 P2).
      this.#dataVersion = -1;
      this.#sql.metaSet.run("retired", "backend");
      this.#sql.metaSet.run(epoch, "epoch");
      tx.changes.length = 0;
      return epoch;
    });
  }

  /** One-time import of a file-store snapshot into an empty database (lane L4 drives migration). */
  async importState(state: { entries: Record<string, MeshStateEntry>; versions?: Record<string, number>;
    tombstoneOrder?: string[]; highWater?: number }): Promise<{ entries: number; tombstones: number; highWater: number }> {
    return this.#write((tx) => {
      if (Number(this.#sql.kvCount.get()?.n ?? 0) > 0 || Number(this.#sql.tombCount.get()?.n ?? 0) > 0 || tx.commit !== 0) {
        throw new Error("Fabric mesh SQLite import needs an empty database");
      }
      let highWater = storageRevision(state.highWater ?? 0);
      let entries = 0;
      for (const [key, entry] of Object.entries(state.entries)) {
        validateMeshStateKey(key);
        if (entry.key !== key) throw new Error("Invalid Fabric mesh state entry");
        const version = storageRevision(entry.version);
        if (version === 0) throw new Error("Inconsistent Fabric mesh revision");
        const value = JSON.stringify(entry.value);
        const updatedBy = JSON.stringify(entry.updatedBy);
        if (value === undefined || updatedBy === undefined) throw new Error("Mesh values must be JSON-serializable");
        const size = entryBytes(key, value, updatedBy);
        this.#sql.kvUpsert.run(key, value, version, Number(entry.updatedAt), updatedBy, size);
        tx.stateBytes += size;
        tx.changes.push({ key, version, deleted: false });
        highWater = Math.max(highWater, version);
        entries += 1;
      }
      const order = [...(state.tombstoneOrder ?? [])];
      for (const key of Object.keys(state.versions ?? {})) if (!order.includes(key)) order.push(key);
      let tombstones = 0;
      for (const key of order) {
        if (Object.hasOwn(state.entries, key) || !Object.hasOwn(state.versions ?? {}, key)) continue;
        validateMeshStateKey(key);
        const version = storageRevision(state.versions![key]);
        this.#sql.tombUpsert.run(key, version, ++tx.tombstoneOrd);
        tx.stateBytes += tombstoneBytes(key);
        highWater = Math.max(highWater, version);
        tombstones += 1;
      }
      tx.highWater = highWater;
      tx.deleted = tombstones > 0;
      tx.grew = true;
      // Tombstone-only and metadata-only snapshots push no change row but must still persist the
      // clock, the tombstone ordinal and the commit number (review round 2 P1).
      tx.finish = true;
      return { entries, tombstones, highWater };
    });
  }

  // ---------------------------------------------------------------- maintenance

  /** `<state.db>-wal` size by stat only (never an fd on the database files). */
  walBytes(): number {
    const size = fs.statSync(`${this.file}-wal`, { throwIfNoEntry: false })?.size ?? 0;
    this.#stats.walBytes = size;
    this.#stats.maxWalBytes = Math.max(this.#stats.maxWalBytes, size);
    return size;
  }

  /**
   * PASSIVE (never blocks writers or readers), then TRUNCATE only when the WAL is starving: the
   * file is above the threshold AND grew since the previous checkpoint. A WAL that restarts reuses
   * its file from the start, so it stops growing and writers are never paused for it; one pinned by
   * constant readers only grows. A threshold of 0 forces a TRUNCATE. Never call it in a transaction.
   */
  checkpoint(thresholdBytes = this.#checkpointBytes): { busy: number; log: number; checkpointed: number; truncated: boolean; walBytes: number } {
    this.#assertOpen();
    if (this.#inTransaction) throw new Error("Fabric mesh state checkpoint inside a transaction");
    const passive = this.#sql.checkpointPassive.get() ?? {};
    this.#stats.checkpoints.passive += 1;
    const result = { busy: Number(passive.busy ?? 0), log: Number(passive.log ?? 0), checkpointed: Number(passive.checkpointed ?? 0) };
    let truncated = false;
    const size = this.walBytes();
    const growing = size > this.#lastWalBytes;
    this.#lastWalBytes = size;
    const starving = thresholdBytes <= 0 ? size > 0 : (growing && size > thresholdBytes) || size > this.#emergencyBytes;
    if (starving) {
      // Writers yield to the flag, so the writer lock is free within one short hold; the busy budget
      // then only waits for readers that started before the checkpoint. It escalates after each
      // busy attempt, so readers that outlast it delay the reset but can never starve it.
      const flag = path.join(this.root, CHECKPOINT_FLAG);
      try { fs.writeFileSync(flag, `${process.pid}\n`, { mode: 0o600 }); } catch { /* advisory */ }
      this.#db.exec(`PRAGMA busy_timeout = ${this.#truncateBudgetMs}`);
      try {
        const truncate = this.#sql.checkpointTruncate.get() ?? {};
        truncated = Number(truncate.busy ?? 1) === 0;
        Object.assign(result, { busy: Number(truncate.busy ?? 0), log: Number(truncate.log ?? 0), checkpointed: Number(truncate.checkpointed ?? 0) });
      } catch (error) {
        if (!isBusy(error)) throw error;
      } finally {
        this.#db.exec(`PRAGMA busy_timeout = ${this.#busyTimeoutMs}`);
        try { fs.rmSync(flag, { force: true }); } catch { /* stale after CHECKPOINT_FLAG_STALE_MS */ }
      }
      if (truncated) {
        this.#lastWalBytes = 0;
        this.#stats.checkpoints.truncate += 1;
        this.#truncateBudgetMs = this.#checkpointBusyMs;
      } else {
        this.#stats.checkpoints.truncateBusy += 1;
        this.#truncateBudgetMs = Math.min(MAX_TRUNCATE_BUDGET_MS, this.#truncateBudgetMs * 2);
      }
    }
    this.#stats.lastCheckpoint = { busy: result.busy, log: result.log, checkpointed: result.checkpointed };
    return { ...result, truncated, walBytes: this.walBytes() };
  }

  /**
   * Durability barrier for a caller that needs a power-loss guarantee (review-luna P1-3): resolves
   * once a checkpoint has synced the WAL through every commit made before the call. No writer lock.
   */
  async sync(budgetMs = 1_000): Promise<void> {
    const deadline = Date.now() + Math.max(0, budgetMs);
    for (;;) {
      this.#assertOpen();
      const result = this.#sql.checkpointPassive.get() ?? {};
      this.#stats.checkpoints.passive += 1;
      // checkpointed === log: every frame (ours included) was copied after a WAL sync and the
      // database file was synced; log 0 means a complete checkpoint already reset the WAL.
      if (Number(result.busy ?? 1) === 0 && Number(result.checkpointed ?? -1) === Number(result.log ?? -2)) return;
      if (Date.now() >= deadline) throw new Error("Fabric mesh state durability is not confirmed within the sync budget");
      await delay(2 + Math.random() * 3);
    }
  }

  stats(): SqliteStateStats {
    return { ...this.#stats, checkpoints: { ...this.#stats.checkpoints },
      ...(this.#stats.lastCheckpoint ? { lastCheckpoint: { ...this.#stats.lastCheckpoint } } : {}) };
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer) clearInterval(this.#timer);
    try { if (this.#db.isTransaction) this.#db.exec("ROLLBACK"); } catch { /* closing anyway */ }
    this.#db.close();
  }

  // ---------------------------------------------------------------- internals

  #assertOpen(): void {
    if (this.#closed) throw new Error("Fabric mesh SQLite state store is closed");
  }

  // Re-check the retirement flag only when another connection committed (data_version moved).
  #assertReadable(): void {
    this.#assertOpen();
    if (this.#inTransaction) return;
    const version = Number(this.#sql.dataVersion.get()?.data_version ?? 0);
    if (version === this.#dataVersion) return;
    this.#assertLiveMeta(this.#stampMeta());
    this.#dataVersion = version;
  }

  #assertLiveMeta<M extends { backend: string; epoch: number }>(meta: M): M {
    if (meta.backend !== "sqlite" || meta.epoch !== this.#epoch) throw new MeshStateRetiredError(meta.backend, meta.epoch, this.#epoch);
    return meta;
  }

  #stampMeta(): { backend: string; epoch: number; commit: number } {
    const values = new Map<string, unknown>();
    for (const row of this.#sql.metaStamp.all()) values.set(String(row.name), row.value);
    return {
      backend: String(values.get("backend") ?? "missing"),
      epoch: Number(values.get("epoch") ?? 0),
      commit: Number(values.get("commit_no") ?? 0),
    };
  }

  #meta(): { highWater: number; commit: number; stateBytes: number; tombstoneOrd: number; backend: string; epoch: number } {
    const values = new Map<string, unknown>();
    for (const row of this.#sql.metaAll.all()) values.set(String(row.name), row.value);
    return {
      highWater: storageRevision(Number(values.get("high_water") ?? 0)),
      commit: Number(values.get("commit_no") ?? 0),
      stateBytes: Number(values.get("state_bytes") ?? 0),
      tombstoneOrd: Number(values.get("tombstone_ord") ?? 0),
      backend: String(values.get("backend") ?? "missing"),
      epoch: Number(values.get("epoch") ?? 0),
    };
  }

  #readTransaction<T>(read: () => T): T {
    if (this.#inTransaction) return read();
    this.#db.exec("BEGIN");
    try { return read(); }
    finally { try { this.#db.exec("COMMIT"); } catch { try { this.#db.exec("ROLLBACK"); } catch { /* ended */ } } }
  }

  #scan(prefix: string): MeshStateEntry[] {
    const rows = prefix ? this.#sql.kvRange.all(prefix, prefix + PREFIX_END) : this.#sql.kvAll.all();
    return rows.map(toEntry).filter(entry => !prefix || entry.key.startsWith(prefix))
      .sort((left, right) => left.key.localeCompare(right.key));
  }

  #entry(key: string): MeshStateEntry | undefined {
    const row = this.#sql.kvGet.get(key);
    return row ? toEntry(row) : undefined;
  }

  #slot(key: string): Slot {
    const row = this.#sql.kvGet.get(key);
    if (row) return { present: true, version: storageRevision(Number(row.version)), bytes: Number(row.bytes), tombstone: false };
    const tombstone = this.#sql.tombGet.get(key);
    return tombstone
      ? { present: false, version: storageRevision(Number(tombstone.version)), bytes: 0, tombstone: true }
      : { present: false, version: 0, bytes: 0, tombstone: false };
  }

  #view(): MeshBatchView {
    return {
      get: (key) => this.#entry(key),
      listAll: (prefix) => this.#scan(prefix),
      version: (key) => this.#slot(key).version,
    };
  }

  #applyPut(tx: Tx, slot: Slot, plan: Extract<StorageTransition, { kind: "put" }>, now: number): MeshStateEntry {
    const value = JSON.stringify(plan.value);
    const updatedBy = JSON.stringify(plan.identity);
    const size = entryBytes(plan.key, value, updatedBy);
    this.#sql.kvUpsert.run(plan.key, value, plan.version, now, updatedBy, size);
    if (slot.tombstone) {
      this.#sql.tombDelete.run(plan.key);
      tx.stateBytes -= tombstoneBytes(plan.key);
    }
    tx.stateBytes += size - slot.bytes;
    if (size > slot.bytes) tx.grew = true;
    tx.highWater = plan.highWater;
    tx.changes.push({ key: plan.key, version: plan.version, deleted: false });
    return { key: plan.key, value: JSON.parse(value) as unknown, version: plan.version, updatedAt: now,
      updatedBy: JSON.parse(updatedBy) as MeshIdentity };
  }

  #applyDelete(tx: Tx, slot: Slot, plan: Extract<StorageTransition, { kind: "delete" }>): void {
    this.#sql.kvDelete.run(plan.key);
    this.#sql.tombUpsert.run(plan.key, plan.version, ++tx.tombstoneOrd);
    tx.stateBytes += tombstoneBytes(plan.key) - slot.bytes - (slot.tombstone ? tombstoneBytes(plan.key) : 0);
    tx.highWater = plan.highWater;
    tx.deleted = true;
    tx.changes.push({ key: plan.key, version: plan.version, deleted: true });
  }

  #timedAfterCommit(afterCommit: (view: MeshBatchView) => void, view: MeshBatchView): void {
    const started = performance.now();
    try { afterCommit(view); }
    finally { this.#stats.maxAfterCommitMs = Math.max(this.#stats.maxAfterCommitMs, performance.now() - started); }
  }

  // Asynchronous, fair acquisition of the state write lock (see the module comment). Each attempt
  // runs BEGIN IMMEDIATE, the synchronous body and COMMIT in ONE synchronous segment, so no other
  // caller in this process can interleave a statement on this connection while the lock is held.
  async #write<T>(body: (tx: Tx) => T, lockTimeoutMs = this.#lockTimeoutMs): Promise<T> {
    const scope = this.#tryLockScope.getStore();
    const budget = scope?.active ? Math.min(scope.timeoutMs, lockTimeoutMs) : lockTimeoutMs;
    const started = performance.now();
    const deadline = started + Math.max(0, budget);
    let attempts = 0;
    let maxGap = 0;
    let last = started;
    let transient = 0;
    let ticket: MeshLockTicket | undefined;
    try {
      for (;;) {
        this.#signal?.throwIfAborted();
        this.#assertOpen();
        if (this.#inTransaction) throw new Error("Fabric mesh state callbacks must not call store writers");
        if (this.#checkpointPending()) this.#stats.checkpoints.writerYields += 1;
        else if (!ticket || ticket.mayContend()) {
          const now = performance.now();
          if (attempts > 0) maxGap = Math.max(maxGap, now - last);
          last = now;
          attempts += 1;
          let begun = false;
          try {
            this.#db.exec("BEGIN IMMEDIATE");
            begun = true;
          } catch (error) {
            if (isTransient(error) && transient < TRANSIENT_RETRIES) { transient += 1; this.#stats.transientRetries += 1; }
            else if (isBusy(error)) this.#stats.busyRetries += 1;
            else throw error;
          }
          if (begun) {
            this.#stats.maxWaitMs = Math.max(this.#stats.maxWaitMs, performance.now() - started);
            return this.#run(body);
          }
        }
        const now = performance.now();
        if (now >= deadline) throw new MeshLockTimeoutError(` (SQLite state ${this.file})`, attempts, Math.round(maxGap));
        if (!ticket && now - started >= this.#fifoAfterMs) {
          ticket = this.#ticket(deadline - now);
          if (ticket) this.#stats.fifoJoins += 1;
        }
        const cap = Math.min(4, 2 ** Math.min(attempts - 1, 2));
        await delay(Math.min(deadline - now, cap * (0.5 + Math.random() / 2)), this.#signal);
      }
    } finally {
      ticket?.close();
    }
  }

  // One stat per attempt: a fresh flag means a TRUNCATE checkpoint wants the writer lock.
  #checkpointPending(): boolean {
    const flag = fs.statSync(path.join(this.root, CHECKPOINT_FLAG), { throwIfNoEntry: false });
    return flag !== undefined && Date.now() - flag.mtimeMs < CHECKPOINT_FLAG_STALE_MS;
  }

  #ticket(budgetMs: number): MeshLockTicket | undefined {
    try { return new MeshLockTicket(this.file, randomUUID(), budgetMs); }
    catch { return undefined; } // Advisory only: an unusable queue leaves the plain jittered contest.
  }

  // The synchronous body between BEGIN IMMEDIATE (already taken) and COMMIT.
  #run<T>(body: (tx: Tx) => T): T {
    const began = performance.now();
    this.#inTransaction = true;
    this.#stats.transactions += 1;
    let committed = false;
    let changed = false;
    try {
      const meta = this.#meta();
      // Retirement and epoch are checked after BEGIN IMMEDIATE, so a writer that waited through a
      // rollback can never commit into a retired database (review-opus P0-1).
      if (meta.backend !== "sqlite" || meta.epoch !== this.#epoch) throw new MeshStateRetiredError(meta.backend, meta.epoch, this.#epoch);
      const tx: Tx = { highWater: meta.highWater, commit: meta.commit, stateBytes: meta.stateBytes,
        tombstoneOrd: meta.tombstoneOrd, grew: false, deleted: false, finish: false, changes: [] };
      const result = body(tx);
      if (result !== null && typeof result === "object" && typeof (result as { then?: unknown }).then === "function") {
        throw new Error("Fabric mesh state transactions must be synchronous");
      }
      if (tx.changes.length > 0 || tx.finish) { this.#finish(tx); changed = true; }
      this.#db.exec("COMMIT");
      committed = true;
      if (changed) this.#stats.commits += 1;
      return result;
    } finally {
      if (!committed) try { if (this.#db.isTransaction) this.#db.exec("ROLLBACK"); } catch { /* connection ends it */ }
      this.#inTransaction = false;
      const hold = performance.now() - began;
      this.#stats.totalHoldMs += hold;
      this.#stats.maxHoldMs = Math.max(this.#stats.maxHoldMs, hold);
      if (committed && changed) this.#emergencyCheckpoint();
    }
  }

  #finish(tx: Tx): void {
    if (tx.deleted && this.#maxTombstones >= 0) {
      const count = Number(this.#sql.tombCount.get()?.n ?? 0);
      if (count > this.#maxTombstones) {
        const cutoff = this.#sql.tombCutoff.get(this.#maxTombstones)?.ord;
        if (cutoff !== undefined && cutoff !== null) {
          for (const row of this.#sql.tombEvict.all(Number(cutoff))) tx.stateBytes -= tombstoneBytes(String(row.key));
        }
      }
    }
    if (tx.grew && tx.stateBytes + ENVELOPE_BYTES > this.#maxStateBytes) {
      throw new Error(`Fabric mesh state exceeds ${this.#maxStateBytes} bytes`);
    }
    const commit = tx.commit + 1;
    this.#sql.metaSet.run(tx.highWater, "high_water");
    this.#sql.metaSet.run(commit, "commit_no");
    this.#sql.metaSet.run(tx.stateBytes, "state_bytes");
    this.#sql.metaSet.run(tx.tombstoneOrd, "tombstone_ord");
    let seq = 0;
    for (const change of tx.changes) {
      seq = Number(this.#sql.changeInsert.run(commit, change.key, change.version, change.deleted ? 1 : 0).lastInsertRowid);
    }
    if (seq > this.#changesRetained && seq % 64 < tx.changes.length) this.#sql.changeTrim.run(seq - this.#changesRetained);
  }

  // Fallback when no maintainer runs: after COMMIT, outside any transaction, throttled to 4/s.
  #emergencyCheckpoint(): void {
    const now = Date.now();
    if (now - this.#lastEmergencyCheck < 250) return;
    this.#lastEmergencyCheck = now;
    try {
      if (this.walBytes() <= this.#emergencyBytes) return;
      this.#stats.checkpoints.emergency += 1;
      this.checkpoint(this.#checkpointBytes);
    } catch { this.#stats.checkpoints.failed += 1; }
  }
}

const clampBusy = (value: number | undefined): number => Math.max(0, Math.min(MAX_BUSY_TIMEOUT_MS, Math.floor(value ?? 2)));

// Idempotent per-connection setup plus the one-time schema; throws BUSY for the caller to retry.
const initialise = (db: SqliteConnection, options: SqliteStateStoreOptions): { epoch: number; storeId: string } => {
  db.exec(`PRAGMA busy_timeout = ${clampBusy(options.busyTimeoutMs)}`);
  const mode = db.prepare("PRAGMA journal_mode = WAL").get();
  if (String(mode?.journal_mode ?? "").toLowerCase() !== "wal") {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state could not enter WAL mode (${String(mode?.journal_mode)})`);
  }
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA wal_autocheckpoint = 0");
  db.exec(`PRAGMA journal_size_limit = ${Math.max(0, Math.floor(options.journalSizeLimitBytes ?? 16 * 1024 * 1024))}`);
  db.exec("PRAGMA trusted_schema = OFF");
  db.exec("BEGIN IMMEDIATE");
  db.exec(SCHEMA);
  const seed = db.prepare("INSERT OR IGNORE INTO meta(name, value) VALUES (?, ?)");
  seed.run("schema", SCHEMA_VERSION);
  seed.run("backend", "sqlite");
  seed.run("epoch", 1);
  seed.run("store_id", randomUUID());
  seed.run("high_water", 0);
  seed.run("commit_no", 0);
  seed.run("state_bytes", 0);
  seed.run("tombstone_ord", 0);
  seed.run("created_at", Date.now());
  const read = db.prepare("SELECT value FROM meta WHERE name = ?");
  const schema = Number(read.get("schema")?.value);
  const backend = String(read.get("backend")?.value);
  const epoch = Number(read.get("epoch")?.value);
  const storeId = String(read.get("store_id")?.value);
  db.exec("COMMIT");
  if (schema !== SCHEMA_VERSION) {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state schema ${schema} is not supported (expected ${SCHEMA_VERSION})`);
  }
  if (backend !== "sqlite") throw new MeshStateRetiredError(backend, epoch, epoch);
  return { epoch, storeId };
};
