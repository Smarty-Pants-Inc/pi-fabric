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
 * Checkpoints (smarty-dev#6477, replacing review-opus P2-1's `wal_autocheckpoint=0`): every connection
 * keeps SQLite's built-in autocheckpoint (`WAL_AUTOCHECKPOINT_PAGES`, ~4 MiB). It is PASSIVE and runs on
 * the committing connection right after its COMMIT, so it never blocks another writer; with it off and
 * no maintainer in production a hub's WAL reached 48 MB. A store opened with `checkpoint: "maintainer"` (the L3 projector, the
 * maintenance holder, tests) checkpoints on an unref'd timer: PASSIVE first (copies and fsyncs
 * without blocking writers), then, only when the WAL file is above `checkpointBytes` and still
 * growing (constant readers keep it from restarting), TRUNCATE with a short busy budget. SQLite's own busy handler would lose the writer lock to writers retrying every
 * few ms, so the checkpointer raises its own flag in `state-checkpoint.flags/` and writers yield while any is fresh
 * (< 1 s, so a crashed checkpointer stalls nobody for longer). TRUNCATE then holds the writer lock,
 * so the WAL stops growing, and waits only for readers that started before it; new readers read
 * the database file. Reads are single statements (`.get()`/`.all()`, never an iterator or a read
 * transaction held across an await) and the budget doubles after each busy attempt (50 ms to
 * 400 ms), so constant readers cannot starve it. If no maintainer runs, any writer runs the same checkpoint after
 * its COMMIT once the WAL passes `emergencyCheckpointBytes`, so the WAL stays bounded regardless.
 * `journal_size_limit` caps a reset WAL. `stats()` exports the WAL size and checkpoint progress.
 *
 * Reader starvation (smarty-dev#6477, full-load soak): with many processes reading constantly, some reader
 * always holds a WAL read mark, so PASSIVE autocheckpoints copy frames but the WAL never restarts and grows
 * to the emergency path. After a COMMIT that leaves the WAL above `walResetBytes` (8 MiB), one process at a
 * time (the `state-wal-reset.lock` try-lock) TRUNCATEs it: ONE non-blocking attempt (busy_timeout 0) after
 * the writer's continuation, outside any transaction, so no client stalls on it. A busy attempt is not
 * retried (pi-fabric#694 P1-B, R-no-polling): the next commit that finds the WAL above the threshold
 * tries again, no sooner than 2 s later. Commits drive the reset; nothing sleeps or polls.
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
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { MeshLockTimeoutError } from "../core/atomic-write.js";
import { captureStorageDelete, captureStoragePut, storageRevision, type StorageTransition } from "../verified/storage.js";
import { encodeMeshStateMovedMarker, isMeshStateMovedMarker, readMeshStateMovedMarker } from "./backend-fence.js";
import { holdMeshFence, holdMeshFenceSync } from "./fence-lock.js";
import { MeshLockTicket } from "./lock-queue.js";
import { processStartTime, residentProcessAlive } from "../residency/process-identity.js";
// From the domain modules, not the store.ts facade: store.ts loads this module through state-backend.ts (L2a).
import { MeshBatchConflictError, type MeshBatchOperation, type MeshBatchResult, type MeshBatchView,
  type MeshReadOptions, type MeshStateEntry } from "./state-file.js";
import type { MeshIdentity } from "./event-log.js";
import { formatWalReaders, walReaderPids } from "./wal-readers.js";
import { assertPrivatePath, MeshStateUnsupportedError, openPrivateStateDb } from "./state-gate.js";
export { assertPrivatePath, MeshStateUnsupportedError, openPrivateStateDb, privateGroup, withOwnerOnlyUmask } from "./state-gate.js";

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


/**
 * smarty-dev#6477: a sqlite-mode open never initialises an authoritative EMPTY database over a root
 * whose state still lives in state.json. Without an initialised state.db, a state.json that is not
 * cutover's moved marker and is not empty (entries, tombstones or an allocation clock) is live file
 * state: refuse BEFORE creating state.db, so the root is left byte-identical (no db, no fence, no epoch).
 * The import tool (backend-migration.ts) opens the database itself and never comes through here.
 */
const assertSqliteRootImported = (root: string): void => {
  if (stateDbSize(root) > 0) return; // initialised (or being initialised): its meta decides
  if (classifyStateFile(path.join(root, "state.json")) === "populated") throw importFirst(root, "this root has file-backend state");
};

/** state.db's size, or -1 when it is absent. */
const stateDbSize = (root: string): number => {
  try { return fs.statSync(path.join(root, "state.db")).size; } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ENOTDIR") return -1;
    throw error;
  }
};

/**
 * What a state.json holds: "absent"; "empty" (zero-length, or no entries, no tombstones and highWater 0);
 * "marker" (cutover's moved marker); else "populated" (live file state, or damaged: not provably empty).
 */
const classifyStateFile = (file: string): "absent" | "empty" | "marker" | "populated" => {
  let serialized: string;
  try { serialized = fs.readFileSync(file, "utf8"); } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT" || code === "ENOTDIR") return "absent";
    throw error;
  }
  if (!serialized.trim()) return "empty"; // a zero-length state.json holds nothing
  let parsed: unknown;
  try { parsed = JSON.parse(serialized); } catch { parsed = undefined; } // damaged: not provably empty, refuse
  if (isMeshStateMovedMarker(parsed)) return "marker";
  if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
    const state = parsed as { entries?: unknown; versions?: unknown; tombstoneOrder?: unknown; highWater?: unknown };
    const empty = (value: unknown): boolean => value === undefined || value === null
      || (typeof value === "object" && Object.keys(value).length === 0);
    if (state.entries !== null && typeof state.entries === "object" && !Array.isArray(state.entries)
      && empty(state.entries) && empty(state.versions) && empty(state.tombstoneOrder)
      && (state.highWater === undefined || state.highWater === 0)) return "empty"; // a fresh root's empty state file
  }
  return "populated";
};

const importFirst = (root: string, why: string): MeshStateUnsupportedError =>
  new MeshStateUnsupportedError(`Fabric mesh SQLite state refused for ${root}: ${why}; `
    + `run \`fabric-mesh-backend import --root ${root}\` first (smarty-dev#6477)`);

/**
 * smarty-dev#6477 part 2: only the import path creates `<root>/state.db` (the file-mode fence). Before any
 * side effect, a default open needs a non-empty state.db (its flag decides, see assertImportedDatabase);
 * without one it refuses: loudly when state.json is the moved marker (state.db lost), else "import first".
 * "create" (fixtures, the import's peers) may initialise a fresh root; "detached" (a shadow copy outside
 * the mesh root, never a fence) is unguarded.
 */
/**
 * Test fixtures only: tests/fleet-isolation-setup.ts sets this global symbol to "create" so the suite's
 * existing fixtures that build fresh sqlite roots keep doing so in-process (child processes do not inherit
 * it). Production code never sets it; an explicit `initialize` always wins.
 */
const TEST_FIXTURE_INITIALIZE = Symbol.for("pi-fabric.mesh.sqlite-initialize.test-fixtures");
const initializeOf = (options: SqliteStateStoreOptions): SqliteStateStoreOptions["initialize"] =>
  options.initialize ?? ((globalThis as Record<symbol, unknown>)[TEST_FIXTURE_INITIALIZE] === "create" ? "create" : undefined);

const lostDatabase = (root: string): MeshStateUnsupportedError =>
  new MeshStateUnsupportedError(`Fabric mesh SQLite state refused for ${root}: state.json is the moved marker but state.db is `
    + "missing or empty; restore state.db from a backup or recover state.json from state.json.cutover-<epoch> (smarty-dev#6477)");

/**
 * Before any side effect. Returns the mode the open runs in: "create" only on a GENUINELY fresh root (no
 * state.db, a zero-length one or one with no meta table, and a state.json that is absent or empty: the meta
 * table is checked on the connection, see hasMeta); "create" over an imported root (an
 * initialised state.db plus the marker) is a default open (undefined), so its database is checked like any
 * other. Everything else refuses with the root untouched: an existing database without the marker (never
 * accepted as imported), file state in state.json, or the marker without its database (review/astra P1).
 */
const guardBeforeOpen = (root: string, initialize: SqliteStateStoreOptions["initialize"]): SqliteStateStoreOptions["initialize"] => {
  if (initialize === "detached") return initialize;
  if (initialize === "create") {
    const state = classifyStateFile(path.join(root, "state.json"));
    if (stateDbSize(root) > 0) {
      if (state === "marker") return undefined; // the imported shape: open it as a default open does
      if (state === "populated") throw importFirst(root, "initialize \"create\" found file-backend state in state.json and an existing state.db");
      // No marker: only a database nothing initialised (no meta table, e.g. a bare WAL header) may be
      // created; that needs a SQLite read (never plain fs on state.db), so the open decides on the
      // connection: an initialised one goes through assertImportedDatabase and refuses without the marker.
    }
    if (state === "populated") throw importFirst(root, "this root has file-backend state");
    if (state === "marker") throw lostDatabase(root);
    return "create";
  }
  assertSqliteRootImported(root);
  if (stateDbSize(root) > 0) return initialize;
  if (readMeshStateMovedMarker(root)) throw lostDatabase(root);
  throw importFirst(root, "this root has no imported state.db");
};

/** Whether the connection's database was already initialised (a meta table): "create" never adopts one. */
const hasMeta = (db: SqliteConnection): boolean =>
  db.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get() !== undefined;

/**
 * On the open connection, before initialise seeds anything: an uninitialised state.db is refused, and
 * backend=sqlite needs the moved marker (cutover installs it BEFORE committing sqlite and rollback
 * removes it only AFTER committing file, so sqlite without the marker means no import made this
 * database: an old release's fence over a fresh or file root). Other flags (importing, exporting,
 * file) keep their own handling in initialise and the writers. The marker is read after the flag.
 */
const assertImportedDatabase = (db: SqliteConnection, root: string): void => {
  const table = db.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get();
  const backend = table === undefined ? undefined : db.prepare("SELECT value FROM meta WHERE name = 'backend'").get()?.value;
  if (backend === undefined || backend === null) throw importFirst(root, "state.db is not initialised");
  if (String(backend) === "sqlite" && !readMeshStateMovedMarker(root)) {
    throw importFirst(root, "state.db says backend=sqlite but state.json is not the moved marker, so no import created it "
      + "(inspect state.db and move it aside if it holds nothing)");
  }
};

/**
 * Test-only seam (tests/mesh-state-sqlite-unimported.test.ts): a function under this global symbol runs at
 * "before-marker" (after initialise, before the re-check), "before-claim" (after the re-check, right before the
 * exclusive link) and "before-rename" (an empty state.json about to be claimed aside), so a test can populate
 * state.json in each window. Production never sets it.
 */
const CREATE_MARKER_HOOK = Symbol.for("pi-fabric.mesh.sqlite-create.marker-hook");
const markerHook = (phase: "before-marker" | "before-claim" | "before-rename", root: string): void => {
  const hook = (globalThis as Record<symbol, unknown>)[CREATE_MARKER_HOOK];
  if (typeof hook === "function") (hook as (phase: string, root: string) => void)(phase, root);
};

const raced = (root: string, why: string): MeshStateUnsupportedError => importFirst(root, `initialize "create" raced: ${why}; `
  + "state.json was left as it is (a state.db this open initialised stays and fences file-mode writers: move it aside if it holds nothing)");

/**
 * "create" only, on a fresh root: the moved marker at the database's epoch, as a fresh import leaves it. It NEVER
 * replaces a non-marker state.json (review/astra P1): state.json is re-checked here, then claimed exclusively. An
 * absent state.json is created with link(2) (fails if anything appeared); an empty one is first renamed aside, so
 * the inode that is judged is the inode that is replaced, and the marker is linked in only while the name is
 * still free. A claimed state.json that turns out to be populated is linked back unchanged (same inode, same
 * bytes) and the open refuses. No step overwrites a name another writer may have just written. Runs under the
 * fence (review round 2), so a file-mode writer cannot rename over the marker afterwards; the claim protocol
 * stays for writers that ignore `.lock`.
 */
const ensureMovedMarker = (root: string, epoch: number): void => {
  markerHook("before-marker", root);
  const file = path.join(root, "state.json");
  const before = classifyStateFile(file);
  if (before === "marker") return;
  if (before === "populated") throw raced(root, "state.json was populated after the fresh-root check");
  const temp = `${file}.sqlite-create-${process.pid}-${randomUUID()}.tmp`;
  const descriptor = fs.openSync(temp, "wx", 0o600);
  try {
    fs.writeSync(descriptor, encodeMeshStateMovedMarker(epoch));
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  try {
    markerHook("before-claim", root);
    for (let attempt = 0; attempt < 3; attempt += 1) {
      try { fs.linkSync(temp, file); return; } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      const present = classifyStateFile(file);
      if (present === "marker") return; // a concurrent creator or import installed it
      if (present === "populated") throw raced(root, "state.json was populated before the marker write");
      if (present === "absent") continue;
      // An empty state.json: claim that exact inode, judge it, and only then put the marker in place.
      const aside = `${file}.sqlite-create-${process.pid}-${randomUUID()}.aside`;
      markerHook("before-rename", root);
      try { fs.renameSync(file, aside); } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
        throw error;
      }
      const claimed = classifyStateFile(aside);
      if (claimed === "empty") {
        try { fs.linkSync(temp, file); } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
            try { fs.linkSync(aside, file); fs.rmSync(aside, { force: true }); } catch { /* the aside keeps it */ }
            throw error;
          }
          fs.rmSync(aside, { force: true }); // it held nothing; the name is a newer writer's now
          if (classifyStateFile(file) === "marker") return;
          throw raced(root, "state.json was written during the marker write");
        }
        fs.rmSync(aside, { force: true });
        return;
      }
      // Populated (or a marker) arrived between the re-check and the claim: put the same inode back.
      try { fs.linkSync(aside, file); } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") {
          throw raced(root, `state.json changed twice during the marker write; the claimed state is kept at ${aside}`);
        }
        throw error;
      }
      fs.rmSync(aside, { force: true });
      if (claimed === "marker") return;
      throw raced(root, "state.json was populated before the marker write");
    }
    throw raced(root, "state.json kept changing during the marker write");
  } finally { fs.rmSync(temp, { force: true }); }
};

/** The database was retired (rolled back to the file backend) or re-epoched: use the file path. */
export class MeshStateRetiredError extends Error {
  readonly code = "FABRIC_MESH_STATE_RETIRED";
  constructor(readonly backend: string, readonly epoch: number, readonly expectedEpoch: number) {
    super(`Fabric mesh state database is ${backend} at epoch ${epoch} (this store opened epoch ${expectedEpoch})`);
    this.name = "MeshStateRetiredError";
  }
}

/** A write refused while the WAL is above `walHardCapBytes` (a reader is pinning it). Retryable later. */
export class MeshStateWalCapError extends Error {
  readonly code = "FABRIC_MESH_STATE_WAL_CAP";
  constructor(readonly walBytes: number, readonly capBytes: number, readonly readers = "") {
    super(`Fabric mesh state write refused: the SQLite WAL is ${walBytes} bytes, above the ${capBytes}-byte cap; `
      + `a reader is pinning the WAL; restart it or roll back${readers ? ` (WAL readers: ${readers})` : ""}`);
    this.name = "MeshStateWalCapError";
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
  /** WAL size above which a writer resets the WAL with a client-side TRUNCATE after its COMMIT; 0 disables. Default 8 MiB. */
  walResetBytes?: number;
  /**
   * WAL hard cap (smarty-dev#6477 security pass P1). A reader that pins one snapshot defeats every
   * checkpoint, so the WAL only grows. Before EVERY write transaction (pi-fabric#694 P1 1) the store stats
   * the shared `state.db-wal`; above this size it makes ONE non-blocking TRUNCATE attempt and, if the WAL is
   * still above it, refuses before BEGIN with `MeshStateWalCapError` (reads keep working). The check reads
   * the shared file, so every store and process sharing the root refuses, a fresh or restarted one included,
   * and writes recover by themselves once the reader lets go. Default 96 MiB. Must be a finite number
   * greater than 0: 0 (formerly "disabled"), NaN, Infinity and negative values throw a TypeError at open.
   */
  walHardCapBytes?: number;
  /** `journal_size_limit`. Default 16 MiB. */
  journalSizeLimitBytes?: number;
  /** Rows kept in the `changes` feed. Default 4,096. */
  changesRetained?: number;
  /** Driver adapter. Default: node:sqlite, loaded at first use. */
  open?: SqliteOpener;
  /**
   * Who may initialise state.db (smarty-dev#6477). Default: nobody; only an imported mesh root opens (an
   * initialised state.db plus the moved marker), anything else refuses with no side effect. "create":
   * test fixtures and harnesses only: a FRESH root (no state.json or an empty one) is initialised as an
   * import would leave it (state.db at epoch 1 plus the moved marker), under the import's fence
   * (custody.lock, then .lock; fence-lock.ts) from the re-checked guard through the marker. "detached": a database outside the
   * mesh root that is never a fence (the shadow backend's and the shadow projector's copies).
   */
  initialize?: "create" | "detached";
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
  checkpoints: { passive: number; truncate: number; truncateBusy: number; emergency: number; failed: number; writerYields: number;
    walResets: number; walResetBusy: number };
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

/** A key the change feed named since a reader's commit: its current row, or its tombstone version (0: gone). */
export interface SqliteDeltaKey {
  key: string;
  entry: MeshStateEntry | undefined;
  /** The live version, else the retained tombstone's, else 0 (exportState's `versions`). */
  version: number;
}

/** `SqliteStateStore.readDelta`: what changed after a reader's commit, from one read transaction. */
export interface SqliteStateDelta {
  /** The live `stateStamp()` of the snapshot this delta brings a reader to. */
  stamp: string;
  commit: number;
  /** False when the feed no longer covers every commit after `after`: read `exportLive()` instead. */
  complete: boolean;
  /** Each changed key once, in first-change order. */
  changed: SqliteDeltaKey[];
  /** Every retained tombstone `[key, version]` in eviction order, only when the predicted count was wrong. */
  tombstones?: Array<[string, number]>;
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
// One flag file per active checkpoint request, so requests never share or clear each other's flag
// (pi-fabric#691 review). Writers yield while any fresh one exists.
const CHECKPOINT_FLAGS = "state-checkpoint.flags";
const CHECKPOINT_FLAG_STALE_MS = 1_000;
const MAX_TRUNCATE_BUDGET_MS = 400;
// The client-side WAL reset (reader starvation, smarty-dev#6477): one process at a time, one attempt.
const WAL_RESET_LOCK = "state-wal-reset.lock";
const WAL_RESET_LOCK_STALE_MS = 2_000;
// A busy reset: this process's next attempt waits for a commit at least this much later.
const WAL_RESET_BACKOFF_MS = 2_000;
const DEFAULT_WAL_RESET_BYTES = 8 * 1024 * 1024;
// ponytail: 96 MiB, not 256: with a pinned reader every commit above 64 MiB stalls ~400 ms, so the cap must act
// within minutes (~6-7 min at 2.5 commits/s); normal peaks are ~6-11 MiB (smarty-dev#6477).
const DEFAULT_WAL_HARD_CAP_BYTES = 96 * 1024 * 1024;
/** `walHardCapBytes`, validated: a finite number greater than 0 (pi-fabric#694: 0 no longer disables the cap). */
const walHardCapOf = (options: SqliteStateStoreOptions): number => {
  const cap = options.walHardCapBytes ?? DEFAULT_WAL_HARD_CAP_BYTES;
  if (typeof cap !== "number" || !Number.isFinite(cap) || cap <= 0) {
    throw new TypeError(`Fabric mesh SQLite walHardCapBytes must be a finite number greater than 0 (got ${String(cap)})`);
  }
  return Math.max(1, Math.floor(cap));
};
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
    checkpoints: { passive: 0, truncate: 0, truncateBusy: 0, emergency: 0, failed: 0, writerYields: 0, walResets: 0, walResetBusy: 0 },
    walBytes: 0, maxWalBytes: 0,
  };
  #timer: NodeJS.Timeout | undefined;
  #inTransaction = false;
  #closed = false;
  #dataVersion = -1;
  #lastEmergencyCheck = 0;
  #truncateBudgetMs: number;
  #lastWalBytes = 0;
  readonly #walResetBytes: number;
  readonly #walHardCapBytes: number;
  #walResetting = false;
  #walResetBusyAt = Number.NEGATIVE_INFINITY;
  #lastWalResetCheck = 0;

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
    this.#walResetBytes = Math.max(0, Math.floor(options.walResetBytes ?? DEFAULT_WAL_RESET_BYTES));
    this.#walHardCapBytes = walHardCapOf(options);
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
    walHardCapOf(options); // a bad cap refuses before any side effect
    const refusal = filesystemRefusal(root);
    if (refusal) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state needs a local filesystem: ${refusal}`);
    const requested = initializeOf(options);
    const initialize = guardBeforeOpen(root, requested);
    if (initialize !== "create") return SqliteStateStore.#openAsync(root, maxEventBytes, maxReadEvents, options, initialize, initTimeoutMs);
    // smarty-dev#6477 review round 2: "create" on a fresh root runs under the import's fence (custody.lock, then
    // .lock) from the re-checked guard through the db init to the marker, so a file-mode writer either commits
    // first (the guard below sees its state.json and refuses) or its fence check sees this state.db and refuses.
    return holdMeshFence(root, { lockTimeoutMs: options.lockTimeoutMs ?? LOCK_TIMEOUT_MS }, () =>
      SqliteStateStore.#openAsync(root, maxEventBytes, maxReadEvents, options, guardBeforeOpen(root, requested), initTimeoutMs));
  }

  static async #openAsync(root: string, maxEventBytes: number, maxReadEvents: number, options: SqliteStateStoreOptions,
    initialize: SqliteStateStoreOptions["initialize"], initTimeoutMs?: number): Promise<SqliteStateStore> {
    const file = path.join(root, "state.db");
    // The one root and file gate (openPrivateStateDb): 0700 root on a pinned fd, 0600 state.db, -wal/-shm inherit it.
    const db = openPrivateStateDb(root, true, options.open ?? openNodeSqlite);
    const deadline = Date.now() + Math.max(0, initTimeoutMs ?? options.lockTimeoutMs ?? LOCK_TIMEOUT_MS);
    try {
      let transient = 0;
      for (;;) {
        options.writeSignal?.throwIfAborted();
        try {
          const fresh = initialize === "create" && !hasMeta(db);
          if (initialize !== "detached" && !fresh) assertImportedDatabase(db, root);
          const identity = initialise(db, options);
          if (fresh) ensureMovedMarker(root, identity.epoch);
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
    walHardCapOf(options); // a bad cap refuses before any side effect
    const refusal = filesystemRefusal(root);
    if (refusal) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state needs a local filesystem: ${refusal}`);
    const requested = initializeOf(options);
    const initialize = guardBeforeOpen(root, requested);
    if (initialize !== "create") return SqliteStateStore.#openSync(root, maxEventBytes, maxReadEvents, options, initialize);
    // The same fence as open(), one synchronous attempt at the same two locks: a busy fence throws an
    // SQLITE_BUSY-coded MeshFenceBusyError, which callers retry like a busy database (see the contract above).
    return holdMeshFenceSync(root, 0, () =>
      SqliteStateStore.#openSync(root, maxEventBytes, maxReadEvents, options, guardBeforeOpen(root, requested)));
  }

  static #openSync(root: string, maxEventBytes: number, maxReadEvents: number, options: SqliteStateStoreOptions,
    initialize: SqliteStateStoreOptions["initialize"]): SqliteStateStore {
    const file = path.join(root, "state.db");
    const db = openPrivateStateDb(root, true, options.open ?? openNodeSqlite);
    try {
      const fresh = initialize === "create" && !hasMeta(db);
      if (initialize !== "detached" && !fresh) assertImportedDatabase(db, root);
      const identity = initialise(db, options);
      if (fresh) ensureMovedMarker(root, identity.epoch);
      return new SqliteStateStore(path.resolve(root), maxEventBytes, maxReadEvents, db, file, identity, options);
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
   * The incremental reader's step (smarty-dev#6477): ONE read transaction gives the live stamp and, when
   * the change feed still covers every commit after `after`, the current row (or tombstone version)
   * of each key changed since. Tombstone evictions and an import's tombstones write no change row:
   * `tombstones(changed)` predicts the reader's tombstone count after applying `changed`, and a
   * different count returns every tombstone too (at most `maxStateTombstones` rows). A retired or
   * re-epoched database throws MeshStateRetiredError, as `exportState({ live: true })` does.
   */
  readDelta(after: number, tombstones: (changed: readonly SqliteDeltaKey[]) => number): SqliteStateDelta {
    this.#assertOpen();
    return this.#readTransaction(() => {
      const { epoch, commit } = this.#assertLiveMeta(this.#stampMeta());
      const stamp = `${this.#storeId}:${epoch}:${commit}`;
      if (after > commit) return { stamp, commit, complete: false, changed: [] };
      const oldest = after === commit ? undefined : this.#sql.changesOldest.get()?.oldest;
      if (after < commit && (oldest === null || oldest === undefined || Number(oldest) > after + 1)) {
        return { stamp, commit, complete: false, changed: [] };
      }
      const keys = new Set<string>();
      if (after < commit) for (const row of this.#sql.changesSince.all(after)) keys.add(String(row.key));
      const changed: SqliteDeltaKey[] = [];
      for (const key of keys) {
        const row = this.#sql.kvGet.get(key);
        if (row) {
          const entry = toEntry(row);
          changed.push({ key, entry, version: entry.version });
          continue;
        }
        const tombstone = this.#sql.tombGet.get(key);
        changed.push({ key, entry: undefined, version: tombstone ? Number(tombstone.version) : 0 });
      }
      const count = Number(this.#sql.tombCount.get()?.n ?? 0);
      if (count === tombstones(changed)) return { stamp, commit, complete: true, changed };
      const all: Array<[string, number]> = this.#sql.tombAll.all().map((row) => [String(row.key), Number(row.version)]);
      return { stamp, commit, complete: true, changed, tombstones: all };
    });
  }

  /** `exportState({ live: true })` with the live `stateStamp()` of that same read transaction. */
  exportLive(): { stamp: string; state: SqliteStateExport } {
    const state = this.exportState({ live: true });
    return { stamp: `${this.#storeId}:${state.epoch}:${state.commit}`, state };
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
    // Exempt from the WAL cap: retiring is the roll-back the cap's refusal advises (a few meta rows).
    return this.#write((tx) => {
      const epoch = this.#epoch + 1;
      // data_version does not move for this connection's own commits: drop the cached value so the
      // next read on this store re-checks the retirement flag (review round 2 P2).
      this.#dataVersion = -1;
      this.#sql.metaSet.run("retired", "backend");
      this.#sql.metaSet.run(epoch, "epoch");
      tx.changes.length = 0;
      return epoch;
    }, this.#lockTimeoutMs, true);
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
      const flag = raiseCheckpointFlag(this.root, ownerToken());
      this.#db.exec(`PRAGMA busy_timeout = ${this.#truncateBudgetMs}`);
      try {
        const truncate = this.#sql.checkpointTruncate.get() ?? {};
        truncated = Number(truncate.busy ?? 1) === 0;
        Object.assign(result, { busy: Number(truncate.busy ?? 0), log: Number(truncate.log ?? 0), checkpointed: Number(truncate.checkpointed ?? 0) });
      } catch (error) {
        if (!isBusy(error)) throw error;
      } finally {
        this.#db.exec(`PRAGMA busy_timeout = ${this.#busyTimeoutMs}`);
        lowerCheckpointFlag(flag);
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
  async #write<T>(body: (tx: Tx) => T, lockTimeoutMs = this.#lockTimeoutMs, walCapExempt = false): Promise<T> {
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
        if (!walCapExempt) this.#refuseAboveWalCap();
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
    return checkpointFlagRaised(this.root);
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
      if (committed && changed) { this.#emergencyCheckpoint(); this.#maybeResetWal(); }
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

  // Admission before EVERY write transaction (pi-fabric#694 P1 1): one stat of the SHARED state.db-wal, so a
  // fresh or restarted store, and every other store or process on the root, sees the same over-cap WAL (no
  // per-store latch to arm). Above the cap: ONE non-blocking TRUNCATE (busy_timeout 0), then re-stat; still
  // above it: refuse. No loop, no timer; the next write re-checks. A write already admitted may land at most
  // one commit past the cap per store.
  #refuseAboveWalCap(): void {
    let size = this.walBytes();
    if (size > this.#walHardCapBytes && !this.#db.isTransaction) {
      try {
        if (this.#tryTruncate()) { this.#stats.checkpoints.walResets += 1; this.#lastWalBytes = 0; }
        else this.#stats.checkpoints.walResetBusy += 1;
      } catch { this.#stats.checkpoints.failed += 1; }
      size = this.walBytes();
    }
    if (size > this.#walHardCapBytes) throw new MeshStateWalCapError(size, this.#walHardCapBytes, formatWalReaders(walReaderPids(this.file)));
  }

  // Reader starvation (see the module comment): after a COMMIT, throttled to 4/s, a WAL above
  // `walResetBytes` starts one asynchronous reset, if this process wins the try-lock.
  #maybeResetWal(): void {
    if (this.#walResetBytes <= 0 || this.#walResetting) return;
    const now = Date.now();
    if (now - this.#lastWalResetCheck < 250 || now - this.#walResetBusyAt < WAL_RESET_BACKOFF_MS) return;
    this.#lastWalResetCheck = now;
    try { if (this.walBytes() <= this.#walResetBytes) return; } catch { return; }
    const lock = path.join(this.root, WAL_RESET_LOCK);
    const token = ownerToken();
    if (!tryLockFile(lock, WAL_RESET_LOCK_STALE_MS, token)) return;
    this.#walResetting = true;
    void new Promise<void>((resolve) => setImmediate(resolve)) // after the writer's own continuation
      .then(() => this.#resetWal())
      .catch(() => { this.#stats.checkpoints.failed += 1; return false; })
      .then((reset) => {
        this.#walResetting = false;
        if (!reset) this.#walResetBusyAt = Date.now(); // deferred to a later commit, see WAL_RESET_BACKOFF_MS
        removeOwnedFile(lock, token); // only while this process still owns it
      });
  }

  // ONE non-blocking TRUNCATE (pi-fabric#694 P1-B): busy defers to a later commit; nothing retries here.
  #resetWal(): boolean {
    try {
      if (this.#closed) return true;
      if (this.#inTransaction || this.#db.isTransaction || !this.#tryTruncate()) { this.#stats.checkpoints.walResetBusy += 1; return false; }
      this.#stats.checkpoints.walResets += 1;
      this.#lastWalBytes = 0;
      return true;
    } finally {
      if (!this.#closed) this.walBytes();
    }
  }

  // One TRUNCATE with no busy handler (busy_timeout 0): true when the WAL was reset.
  #tryTruncate(): boolean {
    this.#db.exec("PRAGMA busy_timeout = 0");
    try {
      const result = this.#sql.checkpointTruncate.get() ?? {};
      return Number(result.busy ?? 1) === 0;
    } catch (error) {
      if (!isBusy(error)) throw error;
      return false;
    } finally {
      this.#db.exec(`PRAGMA busy_timeout = ${this.#busyTimeoutMs}`);
    }
  }
}

// The holder an owner token names (`ownerToken`): its pid and, when recorded, its start ticks. Undefined
// for an unreadable record. A pre-#694 token (`pid.uuid`) names the pid only.
const lockHolder = (record: string): { pid: number; started?: string | undefined } | undefined => {
  const parts = (record.split("\n")[0] ?? "").split(".");
  if (parts.length < 2 || !/^\d+$/.test(parts[0] ?? "")) return undefined;
  const pid = Number(parts[0]);
  if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
  return { pid, started: parts.length >= 3 && /^\d+$/.test(parts[1] ?? "") ? parts[1] : undefined };
};

// An advisory try-lock file (O_EXCL). It is reclaimed only from a holder proven dead on this host (its pid
// gone, or alive with other start ticks: the host-lease rule, residentProcessAlive), or, with no readable
// owner, when it is older than `staleMs` (pi-fabric#694 P2-C: never on age alone from a live owner).
// A reclaim never moves the shared name aside (pi-fabric#694 P2, smarty-dev#6477). It first takes an exclusive
// CLAIM named by the dead record (`<file>.reclaim.<hash>`, itself a tryLockFile, so a dead claimer's claim is
// reclaimed the same way one level down), then re-reads the lock: same inode, mtime and record. From then until
// its unlink only two parties could vacate the name: the dead holder (it cannot) and the claim's holder (us).
// The live owner's own release (removeOwnedFile) never runs for a dead owner. So the unlink removes exactly the
// dead record and no successor can be detached; a lock that changed since it was read is left alone.
// `hooks` is a test seam for the interleavings.
export interface LockFileHooks { afterVerdict?: () => void; afterClaim?: () => void }
export const reclaimClaimPath = (file: string, key: string): string =>
  `${file}.reclaim.${createHash("sha256").update(key).digest("hex").slice(0, 16)}`;
export const tryLockFile = (file: string, staleMs: number, token: string, hooks: LockFileHooks = {}): boolean => {
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const fd = fs.openSync(file, "wx", 0o600);
      try { fs.writeSync(fd, `${token}\n`); } finally { fs.closeSync(fd); }
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") return false;
      const stat = fs.lstatSync(file, { throwIfNoEntry: false });
      if (!stat) continue;
      let record: string;
      try { record = fs.readFileSync(file, "utf8"); } catch { continue; }
      const holder = lockHolder(record);
      const dead = holder ? !residentProcessAlive(holder.pid, holder.started) : Date.now() - stat.mtimeMs > staleMs;
      if (!dead) return false;
      hooks.afterVerdict?.();
      // A record names its holder uniquely; an ownerless file is named by its inode and mtime.
      const claim = reclaimClaimPath(file, holder ? record : `${stat.dev}:${stat.ino}:${stat.mtimeMs}:${record}`);
      if (!tryLockFile(claim, staleMs, token)) return false; // another live reclaimer has this dead record
      try {
        hooks.afterClaim?.();
        const current = fs.lstatSync(file, { throwIfNoEntry: false });
        let currentRecord: string | undefined;
        try { currentRecord = fs.readFileSync(file, "utf8"); } catch { /* gone: already reclaimed */ }
        if (current === undefined || current.ino !== stat.ino || current.dev !== stat.dev || current.mtimeMs !== stat.mtimeMs
          || currentRecord !== record) return false; // already reclaimed, and maybe held by a successor: never touch it
        try { fs.unlinkSync(file); } catch { return false; }
      } finally { removeOwnedFile(claim, token); }
    }
  }
  return false;
};

// The checkpoint flag and the reset lock carry their writer's token, and only that writer removes them:
// a process never clears a flag or lock that another process raised or reclaimed (pi-fabric#691 review).
// A flag raised by two processes belongs to the last writer; the first one's next attempt raises it again.
// `pid.startTicks.uuid` ("-" without /proc): a reclaimer proves the holder dead by pid and start ticks.
export const ownerToken = (): string => `${process.pid}.${processStartTime(process.pid) ?? "-"}.${randomUUID()}`;

export const ownsFile = (file: string, token: string): boolean => {
  try { return fs.readFileSync(file, "utf8") === `${token}\n`; } catch { return false; }
};

// A checkpoint request's own flag: raising it again refreshes its mtime; lowering it removes only it.
// An empty name means the flag could not be raised (advisory). One readdir and a stat per flag (normally
// zero or one) per pending check; a crashed request's flag goes stale and is ignored.
// An existing flag directory is used only when it is private (assertPrivatePath, pi-fabric#694 P2-D);
// anything else is refused with a clear error and never written through.
export const raiseCheckpointFlag = (root: string, token: string): string => {
  const dir = path.join(root, CHECKPOINT_FLAGS);
  const flag = path.join(dir, token);
  try { fs.mkdirSync(dir, { mode: 0o700 }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") return ""; }
  assertPrivatePath(dir, "directory");
  try {
    fs.writeFileSync(flag, "", { mode: 0o600 });
    return flag;
  } catch { return ""; }
};

export const lowerCheckpointFlag = (flag: string): void => {
  if (flag) try { fs.rmSync(flag, { force: true }); } catch { /* stale after CHECKPOINT_FLAG_STALE_MS */ }
};

export const checkpointFlagRaised = (root: string): boolean => {
  const dir = path.join(root, CHECKPOINT_FLAGS);
  let names: string[];
  try { assertPrivatePath(dir, "directory"); names = fs.readdirSync(dir); } catch { return false; } // a foreign directory raises nothing
  const now = Date.now();
  for (const name of names) {
    const stat = fs.statSync(path.join(dir, name), { throwIfNoEntry: false });
    if (stat && now - stat.mtimeMs < CHECKPOINT_FLAG_STALE_MS) return true;
  }
  return false;
};


// The owner's release: check the token, then unlink (pi-fabric#694 P2, smarty-dev#6477). Nothing is moved aside.
// While this owner lives nobody else vacates its lock: a reclaimer needs a dead holder (tryLockFile), so the
// file checked here is still ours at the unlink.
export const removeOwnedFile = (file: string, token: string): void => {
  if (!ownsFile(file, token)) return; // plainly not ours: never displace it
  try { fs.unlinkSync(file); } catch { /* already gone */ }
};

const clampBusy = (value: number | undefined): number => Math.max(0, Math.min(MAX_BUSY_TIMEOUT_MS, Math.floor(value ?? 2)));

// SQLite's default: a PASSIVE checkpoint after the COMMIT that takes the WAL past ~4 MiB (4 KiB pages).
const WAL_AUTOCHECKPOINT_PAGES = 1000;

const SEEDED_META = ["schema", "backend", "epoch", "store_id", "high_water", "commit_no", "state_bytes", "tombstone_ord"];

// The identity of an initialised database from plain reads (no write lock), or undefined when any seeded
// row is missing. smarty-dev#6477: a restart on a busy hub must not contend for BEGIN IMMEDIATE.
const readIdentity = (db: SqliteConnection): { schema: number; backend: string; epoch: number; storeId: string } | undefined => {
  if (db.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'changes'").get() === undefined) return undefined;
  const values = new Map<string, unknown>();
  for (const row of db.prepare(`SELECT name, value FROM meta WHERE name IN (${SEEDED_META.map(() => "?").join(", ")})`).all(...SEEDED_META)) {
    values.set(String(row.name), row.value);
  }
  if (SEEDED_META.some((name) => values.get(name) === undefined || values.get(name) === null)) return undefined;
  return { schema: Number(values.get("schema")), backend: String(values.get("backend")), epoch: Number(values.get("epoch")),
    storeId: String(values.get("store_id")) };
};

const checkIdentity = (identity: { schema: number; backend: string; epoch: number; storeId: string }): { epoch: number; storeId: string } => {
  if (identity.schema !== SCHEMA_VERSION) {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state schema ${identity.schema} is not supported (expected ${SCHEMA_VERSION})`);
  }
  if (identity.backend !== "sqlite") throw new MeshStateRetiredError(identity.backend, identity.epoch, identity.epoch);
  return { epoch: identity.epoch, storeId: identity.storeId };
};

// Idempotent per-connection setup plus the one-time schema; throws BUSY for the caller to retry.
const initialise = (db: SqliteConnection, options: SqliteStateStoreOptions): { epoch: number; storeId: string } => {
  db.exec(`PRAGMA busy_timeout = ${clampBusy(options.busyTimeoutMs)}`);
  const mode = db.prepare("PRAGMA journal_mode = WAL").get();
  if (String(mode?.journal_mode ?? "").toLowerCase() !== "wal") {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state could not enter WAL mode (${String(mode?.journal_mode)})`);
  }
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec(`PRAGMA wal_autocheckpoint = ${WAL_AUTOCHECKPOINT_PAGES}`);
  db.exec(`PRAGMA journal_size_limit = ${Math.max(0, Math.floor(options.journalSizeLimitBytes ?? 16 * 1024 * 1024))}`);
  db.exec("PRAGMA trusted_schema = OFF");
  // smarty-dev#6477: an initialised database opens with plain reads; only a missing schema takes the write lock.
  const existing = readIdentity(db);
  if (existing) return checkIdentity(existing);
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
