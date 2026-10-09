/**
 * Mesh state backend migration: import, cutover and rollback with the R1 fence (smarty-dev#6477, lane L4b).
 *
 * Plan section 5 (docs/mesh-lock-plan.md) is the contract. `meta.backend` in `<mesh>/state.db` is
 * `sqlite`, `importing`, `exporting` or `file`; `meta.epoch` only grows; `state.json` records the epoch of
 * its export in `backendEpoch` (and the export's snapshot digest in `backendDigest`), right after
 * `readGeneration`, so older readers and writers keep and ignore both fields.
 *
 * Invariants (pi-fabric#627 review rounds 4 and 5; legacy writers fail closed on the moved marker, readers follow it to SQLite):
 *   roll-forward (import, cutover; marker BEFORE flag): fill SQLite at backend=importing E+1 (state.json stays the authority), verify, install the marker, THEN commit backend=sqlite.
 *   roll-back (flag BEFORE marker removal): exporting E+1, export and verify, commit backend=file, THEN replace the marker; abort restores the marker before exporting -> sqlite.
 *   Hence, at every crash point, backend=sqlite implies the marker, and a missing marker implies state.json is the authority (each sequence runs under ONE `.lock` hold).
 *
 * The fence is `.lock` plus `custody.lock` (smarty-dev#6477, org decision 10-08): every mutating
 * operation (import, cutover, rollback, abort-rollback) holds both, custody first, for its whole
 * section. The writer census (writer-census.ts) is ADVISORY (smarty-dev#6982): when a provider is
 * given it runs once inside the fence and its report ("advisory: N writers, M unknown") is returned
 * and passed to `onAdvisory`; it never blocks, permits or changes an operation.
 *
 * Rollback commit order, all under ONE fence hold (each step is a crash point; a rerun converges
 * from the stored flag and the marker):
 *   1. The operator stops every v3 writer and the projector (the advisory census informs, it proves nothing).
 *   2. Flag: one BEGIN IMMEDIATE transaction at synchronous=FULL checks backend=sqlite at epoch E,
 *      sets backend=exporting and epoch=E+1. state-sqlite.ts writers check the flag after BEGIN
 *      IMMEDIATE and fail closed (MeshStateRetiredError), so nothing commits after this point.
 *   3. Export: the committed snapshot through the normal state.json encoder (fresh readGeneration,
 *      revisionFormat 2, backendEpoch E+1, backendDigest) into `state.json.rollback-<E+1>.tmp`;
 *      fsync it and the directory, read it back through the normal decoder and verify epoch,
 *      generation and digest against the database snapshot. state.json is still the marker.
 *   4. Switch: BEGIN IMMEDIATE at FULL checks backend=exporting at E+1 and sets backend=file.
 *   5. Replace: rename the verified temp over the marker, fsync the directory; the store's own read
 *      path re-checks it. Older binaries may start only after this step.
 * A rerun with `exporting` repeats 3 to 5 (the fence blocked every write, so the export is
 * identical); with `file` and the marker it repeats 5 (re-exporting with the recorded generation
 * first when the temp is missing or unverifiable); with `file` and a real state.json it verifies.
 * `abortMeshRollback` takes `.lock`, ensures the marker, then sets exporting back to sqlite at E+1.
 * The file epoch never exceeds the database epoch; with backend=file they are equal only after a
 * verified export.
 *
 * Reader rule (`resolveMeshStateSource`): use state.json only when backend=file and it is a real
 * state file whose epoch equals meta.epoch; with sqlite or exporting, or while state.json is the
 * moved marker, read SQLite; any other combination (a file epoch above the
 * database epoch, backend=file at another epoch, an unknown flag, a missing state.db under an
 * epoch-stamped state.json) fails closed with `MeshBackendFenceError` and an alarm.
 *
 * Import (file -> sqlite, also the roll forward) runs the cutover section below (`.lock` and
 * `custody.lock` held throughout, the marker last). Its core: under the mesh `.lock` (no file-mode writer can
 * commit), state.json is read through the normal decoder and cross-checked against the store's own
 * read path (which follows the read journal), then written into state.db in ONE transaction that
 * replaces kv, tombstones and the change feed, sets backend=importing and epoch=E+1 (not authoritative:
 * readers and file-mode writers keep using state.json, sqlite-mode writers fail closed) and verifies the
 * digest both ways (database read-back, and the database snapshot through the encoder and decoder)
 * before COMMIT, and once more after it. backend=sqlite is committed only after the marker landed.
 * Rerun: importing without the marker redoes the import from state.json (a legacy writer that took the
 * dead tool's `.lock` committed to the authority); importing with the marker only commits sqlite. Right before COMMIT it re-reads state.json: a changed
 * readGeneration or digest (a writer that ignored `.lock`) rolls the transaction back, so the flag
 * never moves on a state the import did not see. A fresh state.db is created by this tool at
 * backend=file, epoch 0 (the legacy file authority), never at an authoritative empty sqlite.
 *
 * Cutover holds the mesh `.lock` for the WHOLE critical section (file-mode writers serialize on it, so
 * none commits inside; `custody.lock` is held too): advisory census -> read state.json (generation
 * and digest G0) -> import at backend=importing E+1 (BEGIN IMMEDIATE, FULL), digest verification ->
 * state.json re-read against G0 -> copy, marker -> backend=sqlite. Only then are the locks released. A state.json that
 * moved (a writer that ignored `.lock`) fails the cutover: the rollback
 * fence runs under the same `.lock` back to backend=file at E+2 and the error is thrown
 * (MeshCutoverFailedError); a cutover never succeeds "with an alarm". On success, before `.lock` is
 * released, cutover copies state.json to `state.json.cutover-<E+1>` (fsync), atomically replaces
 * state.json with the moved marker (backend-fence.ts; temp, fsync, rename, directory fsync) and only
 * then commits backend=sqlite at E+1 (review round 5). Every deployed write path reads
 * state.json strictly and throws "invalid state format" on it, so a LEGACY file-mode writer that
 * waited on `.lock` (or starts later) fails closed and writes nothing; rollback step 5 replaces the
 * marker with the export. New writers are also fenced by `assertFileStateWritable` (backend-fence.ts,
 * re-exported here): StateFile calls it on every commit under `.lock`, right before the state.json
 * rename, so a refused write leaves state.json untouched.
 * ponytail: an OLD tolerant reader (recoverDamage=true) sees an empty mesh after cutover. Accepted:
 * the operator stops old processes before a cutover, and the marker exists only while backend=sqlite.
 *
 * Lock order: custody, then `.lock` (as withMeshCustody). This tool is the one place where `.lock`
 * is held around a state transaction (R20). Its own connection uses a synchronous busy handler
 * (default 5 s): it is a dedicated process, so it never freezes a session's event loop.
 * Schema knowledge (tables, meta keys, the byte accounting) mirrors state-sqlite.ts; the tests
 * check both against a store-created database.
 */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { renameAtomic } from "../core/atomic-write.js";
import { storageRevision } from "../verified/storage.js";
import { assertFileStateWritable, encodeMeshStateMovedMarker, MeshBackendFenceError, meshFenceAlarm, meshStateSourceOf, readMeshStateMovedMarker,
  readStateFileEpoch as readEpochHeader, type MeshBackendAlarm, type MeshStateMovedMarker, type MeshStateSource } from "./backend-fence.js";
import type { MeshIdentity } from "./event-log.js";
import { holdMeshFence } from "./fence-lock.js";
import type { MeshLock } from "./mesh-lock.js";
import { decodeMeshStateFile, encodeMeshStateFile, StateFile, type MeshStateEntry, type MeshStateFile } from "./state-file.js";
import { filesystemRefusal, MeshStateUnsupportedError, openNodeSqlite, validateMeshStateKey, type SqliteConnection,
  type SqliteOpener, type SqliteRow } from "./state-sqlite.js";

// The writer fence lives in a leaf module (state-file.ts calls it; this module imports state-file.ts).
export { assertFileStateWritable, MeshBackendFenceError, readMeshStateMovedMarker, type MeshBackendAlarm, type MeshStateMovedMarker, type MeshStateSource };

export type MeshBackendFlag = "sqlite" | "importing" | "exporting" | "file";

/** Crash points, in commit order. `onStep` runs synchronously right after each one. */
export type MeshBackendStep =
  | "import-read" | "import-commit" | "import-verify"
  | "rollback-flag" | "rollback-export-temp" | "rollback-verify" | "rollback-switch" | "rollback-replace"
  | "abort-rollback" | "cutover-reconcile" | "cutover-copy" | "cutover-marker" | "cutover-flag";

export interface MeshCensusWriter { pid: number; release: string; mode: string }
/** Provided by the census lane (writer-census.ts): the writers it attributed and the evidence it could not. */
export type MeshWriterCensus = () => Promise<{ writers: MeshCensusWriter[]; unknown?: MeshCensusWriter[] }>;

/** What one run saw from the census: ADVISORY only (smarty-dev#6982), never a verdict or a gate. */
export interface MeshCensusAdvisory {
  writers: MeshCensusWriter[];
  unknown: MeshCensusWriter[];
  /** The census itself failed; the operation went ahead regardless. */
  error?: string;
}

/** "advisory: N writers, M unknown": never "safe" or "clean" (an empty report proves nothing). */
export const describeCensusAdvisory = (advisory: MeshCensusAdvisory): string => advisory.error !== undefined
  ? `advisory: census failed (${advisory.error})`
  : `advisory: ${advisory.writers.length} writer${advisory.writers.length === 1 ? "" : "s"}, ${advisory.unknown.length} unknown`;

export interface MeshBackendOptions {
  /** The writer census: ADVISORY. Run once per operation, reported, never blocks or permits it. */
  census?: MeshWriterCensus;
  /** Receives the advisory census report of each operation (logs, the CLI). Never throws into it. */
  onAdvisory?: (advisory: MeshCensusAdvisory) => void;
  /** The mesh `.lock` protocol of the fleet. Default 1 (as mesh-bridge). */
  lockProtocol?: 1 | 2;
  /** Budget for each fence lock (`custody.lock`, then `.lock`). Default 60 s. */
  lockTimeoutMs?: number;
  /** Synchronous SQLite busy handler of the tool's own connection. Default 5,000 ms. */
  busyTimeoutMs?: number;
  /** state.json and state.db size bound. Default 32 MiB (as both stores). */
  maxStateBytes?: number;
  /** Driver adapter (bun:sqlite, tests). Default node:sqlite. */
  open?: SqliteOpener;
  /** Synchronous crash-point hook (tests kill the tool here). */
  onStep?: (step: MeshBackendStep) => void;
  /** Fence violations and late writers. The error is thrown as well. */
  onAlarm?: (alarm: MeshBackendAlarm) => void;
}

/**
 * A cutover found a moved state.json (a writer that ignored `.lock`) inside its critical
 * section: it did not succeed. `rollback` is the fence run back to backend=file (absent when the
 * flag never moved or the rollback itself failed; rerun `rollback` then).
 */
export class MeshCutoverFailedError extends MeshBackendFenceError {
  constructor(message: string, readonly rollback?: MeshRollbackResult, readonly conflictFile?: string) {
    super(message);
    this.name = "MeshCutoverFailedError";
  }
}

/** A precondition (flag, root) refuses the operation; nothing was changed. */
export class MeshBackendRefusedError extends Error {
  readonly code = "FABRIC_MESH_BACKEND_REFUSED";
  constructor(message: string) {
    super(message);
    this.name = "MeshBackendRefusedError";
  }
}

export interface MeshStateSnapshot {
  entries: Record<string, MeshStateEntry>;
  versions?: Record<string, number>;
  tombstoneOrder?: string[];
  highWater?: number;
  format?: number;
  revisionFormat?: number;
}

export interface MeshImportResult {
  root: string;
  backend: "sqlite";
  epoch: number;
  previousEpoch: number;
  entries: number;
  tombstones: number;
  highWater: number;
  digest: string;
  /** readGeneration of the imported state.json (G0). */
  generation: string;
  /** True when a rerun found the import already committed and verified it only. */
  converged: boolean;
}

export interface MeshCutoverResult extends MeshImportResult {
  /** The advisory census writers (empty without a provider); informational only. */
  writers: MeshCensusWriter[];
  /** The advisory census report, when a provider ran. */
  census?: MeshCensusAdvisory;
  /** The retired state.json, kept at `state.json.cutover-<epoch>`; state.json is now the moved marker. */
  stateCopy: string;
}

export interface MeshRollbackResult {
  root: string;
  backend: "file";
  epoch: number;
  digest: string;
  generation: string;
  /** Steps this run executed (a rerun starts from the stored flag). */
  steps: number[];
  converged: boolean;
  /** The advisory census report, when a provider ran. */
  census?: MeshCensusAdvisory;
}

export interface MeshAbortRollbackResult {
  root: string;
  backend: "sqlite";
  epoch: number;
  converged: boolean;
  /** The advisory census report, when a provider ran. */
  census?: MeshCensusAdvisory;
}

export interface MeshBackendStatus {
  root: string;
  /** `none` when state.db is absent or uninitialised (a legacy file root). */
  backend: string;
  epoch: number;
  commit?: number;
  fileEpoch?: number;
  fileGeneration?: string | undefined;
  fileError?: string;
  /** state.json is cutover's moved marker (backend=sqlite at fileEpoch), not state. */
  fileMoved?: boolean;
  dbDigest?: string;
  fileDigest?: string;
  importDigest?: string | undefined;
  exportDigest?: string | undefined;
  exportGeneration?: string | undefined;
  reader: { source: "sqlite" | "file" } | { error: string };
  /** File epoch <= database epoch; with backend=file equal. */
  fenceHolds: boolean;
  /** Advisory census (never part of the fence verdict). */
  writers?: MeshCensusWriter[];
  unknownWriters?: MeshCensusWriter[];
  censusError?: string;
}

// ------------------------------------------------------------------ constants

const STATE_DB = "state.db";
const STATE_JSON = "state.json";
const TEMP_PREFIX = "state.json.mesh-backend-";
const SCHEMA_VERSION = 1;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
const DEFAULT_BUSY_MS = 5_000;
const DEFAULT_LOCK_MS = 60_000;
const ENVELOPE_BYTES = 256;
// Mirrors state-sqlite.ts (schema 1). The tests compare it with a store-created database.
const SCHEMA = `
CREATE TABLE IF NOT EXISTS meta(name TEXT PRIMARY KEY NOT NULL, value) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL, version INTEGER NOT NULL,
  updated_at INTEGER NOT NULL, updated_by TEXT NOT NULL, bytes INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS tombstones(key TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL, ord INTEGER NOT NULL UNIQUE) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS changes(seq INTEGER PRIMARY KEY, commit_no INTEGER NOT NULL, key TEXT NOT NULL,
  version INTEGER NOT NULL, deleted INTEGER NOT NULL);
`;
const bytes = (text: string): number => Buffer.byteLength(text, "utf8");
// state-sqlite.ts's upper-bound share of the projected state.json per entry and per tombstone.
const entryBytes = (key: string, value: string, updatedBy: string): number => bytes(value) + bytes(updatedBy) + 3 * bytes(key) + 96;
const tombstoneBytes = (key: string): number => 2 * bytes(key) + 32;

type FencedFile = MeshStateFile & { backendEpoch?: number; backendDigest?: string };

// ------------------------------------------------------------------ digest

interface NormalizedState {
  entries: MeshStateEntry[];
  tombstones: Array<{ key: string; version: number }>;
  highWater: number;
}

const codeUnitOrder = (left: string, right: string): number => (left < right ? -1 : left > right ? 1 : 0);
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/**
 * The file store's own validation (state-file.ts stateSlot) and tombstone order
 * (compactStateTombstones: tombstoneOrder first occurrence, then the remaining versions keys).
 */
const normalizeSnapshot = (state: MeshStateSnapshot): NormalizedState => {
  if (!isRecord(state) || !isRecord(state.entries)) throw new Error("Invalid Fabric mesh state envelope");
  if (state.versions !== undefined && !isRecord(state.versions)) throw new Error("Invalid Fabric mesh revision table");
  if (state.tombstoneOrder !== undefined && !Array.isArray(state.tombstoneOrder)) throw new Error("Invalid Fabric mesh tombstone order");
  if (Object.hasOwn(state, "revisionFormat") && state.revisionFormat !== 2) throw new Error("Unsupported Fabric mesh revision protocol");
  if ((state.format === 2 || state.revisionFormat === 2) && !Object.hasOwn(state, "highWater")) {
    throw new Error("Missing Fabric mesh high-water revision");
  }
  const versions = state.versions ?? {};
  let highWater = state.highWater === undefined ? 0 : storageRevision(state.highWater);
  const entries = Object.keys(state.entries).sort(codeUnitOrder).map((key) => {
    const entry = state.entries[key];
    if (!isRecord(entry) || entry.key !== key) throw new Error("Invalid Fabric mesh state entry");
    const version = storageRevision(entry.version);
    if (version === 0 || (Object.hasOwn(versions, key) && versions[key] !== version)) throw new Error("Inconsistent Fabric mesh revision");
    highWater = Math.max(highWater, version);
    return entry;
  });
  const seen = new Set<string>();
  const tombstones: Array<{ key: string; version: number }> = [];
  const take = (key: unknown): void => {
    if (typeof key !== "string" || Object.hasOwn(state.entries, key) || !Object.hasOwn(versions, key) || seen.has(key)) return;
    seen.add(key);
    const version = storageRevision(versions[key]);
    highWater = Math.max(highWater, version);
    tombstones.push({ key, version });
  };
  for (const key of state.tombstoneOrder ?? []) take(key);
  for (const key of Object.keys(versions)) take(key);
  return { entries, tombstones, highWater };
};

const digestNormalized = (state: NormalizedState, withMeta = true): string => {
  const hash = createHash("sha256");
  hash.update("fabric-mesh-state-digest/1\n");
  for (const entry of state.entries) {
    hash.update(`${JSON.stringify(["e", entry.key, entry.value ?? null, entry.version, entry.updatedAt, entry.updatedBy])}\n`);
  }
  if (withMeta) {
    for (const tombstone of state.tombstones) hash.update(`${JSON.stringify(["t", tombstone.key, tombstone.version])}\n`);
    hash.update(`${JSON.stringify(["h", state.highWater])}\n`);
  }
  return `sha256:${hash.digest("hex")}`;
};

/**
 * Digest of a keyed state in either representation: entries in code-unit key order (key, value,
 * version, updatedAt, updatedBy), tombstones in eviction order, and the effective high-water clock.
 */
export const meshSnapshotDigest = (state: MeshStateSnapshot): string => digestNormalized(normalizeSnapshot(state));

const entriesDigest = (entries: Iterable<MeshStateEntry>): string =>
  digestNormalized({ entries: [...entries].sort((left, right) => codeUnitOrder(left.key, right.key)), tombstones: [], highWater: 0 }, false);

// ------------------------------------------------------------------ the tool's database connection

interface DbMeta {
  backend: string;
  epoch: number;
  commit: number;
  values: Map<string, unknown>;
}

const metaOf = (rows: SqliteRow[]): DbMeta => {
  const values = new Map<string, unknown>();
  for (const row of rows) values.set(String(row.name), row.value);
  const schema = Number(values.get("schema"));
  if (schema !== SCHEMA_VERSION) {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state schema ${schema} is not supported (expected ${SCHEMA_VERSION})`);
  }
  return {
    backend: String(values.get("backend") ?? "missing"),
    epoch: Number(values.get("epoch") ?? Number.NaN),
    commit: Number(values.get("commit_no") ?? 0),
    values,
  };
};

class FenceDb {
  private constructor(readonly file: string, readonly db: SqliteConnection) {}

  /**
   * Opens `<root>/state.db` on the tool's own connection. `create` creates and seeds a missing or
   * uninitialised database at backend=file, epoch 0; otherwise such a database reads as absent.
   */
  static open(root: string, options: MeshBackendOptions, intent: "read" | "write", create = false): FenceDb | undefined {
    const file = path.join(root, STATE_DB);
    const exists = fs.statSync(file, { throwIfNoEntry: false }) !== undefined;
    if (!exists && !create) return undefined;
    if (intent === "write") {
      const refusal = filesystemRefusal(root);
      if (refusal) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state needs a local filesystem: ${refusal}`);
    }
    if (!exists) {
      fs.mkdirSync(root, { recursive: true, mode: 0o700 });
      // O_EXCL: when this succeeds no connection in this process has the file open.
      try { fs.closeSync(fs.openSync(file, "wx", 0o600)); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    }
    const db = (options.open ?? openNodeSqlite)(file);
    try {
      db.exec(`PRAGMA busy_timeout = ${Math.max(0, Math.floor(options.busyTimeoutMs ?? DEFAULT_BUSY_MS))}`);
      db.exec("PRAGMA trusted_schema = OFF");
      if (intent === "write") {
        const mode = db.prepare("PRAGMA journal_mode = WAL").get();
        if (String(mode?.journal_mode ?? "").toLowerCase() !== "wal") {
          throw new MeshStateUnsupportedError(`Fabric mesh SQLite state could not enter WAL mode (${String(mode?.journal_mode)})`);
        }
        // The fence transactions run at FULL (plan section 5, steps 2 and 5).
        db.exec("PRAGMA synchronous = FULL");
        db.exec("PRAGMA wal_autocheckpoint = 0");
      }
      const fence = new FenceDb(file, db);
      if (!fence.#initialised()) {
        if (!create) { db.close(); return undefined; }
        fence.#seed();
      }
      return fence;
    } catch (error) {
      try { db.close(); } catch { /* closing anyway */ }
      throw error;
    }
  }

  #initialised(): boolean {
    const table = this.db.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get();
    return table !== undefined && this.db.prepare("SELECT value FROM meta WHERE name = 'schema'").get() !== undefined;
  }

  #seed(): void {
    this.transaction(() => {
      this.db.exec(SCHEMA);
      const seed = this.db.prepare("INSERT OR IGNORE INTO meta(name, value) VALUES (?, ?)");
      seed.run("schema", SCHEMA_VERSION);
      // Not authoritative until an import: the legacy file root, epoch 0 (state.json carries none).
      seed.run("backend", "file");
      seed.run("epoch", 0);
      seed.run("store_id", randomUUID());
      seed.run("high_water", 0);
      seed.run("commit_no", 0);
      seed.run("state_bytes", 0);
      seed.run("tombstone_ord", 0);
      seed.run("created_at", Date.now());
    });
  }

  meta(): DbMeta {
    return metaOf(this.db.prepare("SELECT name, value FROM meta").all());
  }

  setMeta(name: string, value: string | number): void {
    this.db.prepare("INSERT INTO meta(name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value").run(name, value);
  }

  /** BEGIN IMMEDIATE ... COMMIT on this connection (synchronous=FULL for write intent). */
  transaction<T>(body: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const result = body();
      this.db.exec("COMMIT");
      return result;
    } catch (error) {
      try { if (this.db.isTransaction) this.db.exec("ROLLBACK"); } catch { /* the connection ends it */ }
      throw error;
    }
  }

  #read<T>(body: () => T): T {
    if (this.db.isTransaction) return body();
    this.db.exec("BEGIN");
    try { return body(); }
    finally { try { this.db.exec("COMMIT"); } catch { try { this.db.exec("ROLLBACK"); } catch { /* ended */ } } }
  }

  /** One consistent snapshot in the file store's envelope shape (works in any backend state). */
  snapshot(): { meta: DbMeta; state: MeshStateSnapshot & { highWater: number; tombstoneOrder: string[]; versions: Record<string, number> } } {
    return this.#read(() => {
      const meta = this.meta();
      const entries: Record<string, MeshStateEntry> = {};
      const versions: Record<string, number> = {};
      for (const row of this.db.prepare("SELECT key, value, version, updated_at, updated_by FROM kv ORDER BY key").all()) {
        const entry: MeshStateEntry = {
          key: String(row.key),
          value: JSON.parse(String(row.value)) as unknown,
          version: Number(row.version),
          updatedAt: Number(row.updated_at),
          updatedBy: JSON.parse(String(row.updated_by)) as MeshIdentity,
        };
        entries[entry.key] = entry;
        versions[entry.key] = entry.version;
      }
      const tombstoneOrder: string[] = [];
      for (const row of this.db.prepare("SELECT key, version FROM tombstones ORDER BY ord").all()) {
        tombstoneOrder.push(String(row.key));
        versions[String(row.key)] = Number(row.version);
      }
      const highWater = storageRevision(Number(meta.values.get("high_water") ?? 0));
      return { meta, state: { format: 1, revisionFormat: 2, entries, versions, tombstoneOrder, highWater } };
    });
  }

  counts(): { kv: number; tombstones: number } {
    return {
      kv: Number(this.db.prepare("SELECT count(*) AS n FROM kv").get()?.n ?? 0),
      tombstones: Number(this.db.prepare("SELECT count(*) AS n FROM tombstones").get()?.n ?? 0),
    };
  }

  /** Inside a transaction: replace the whole keyed state with a normalized file snapshot. */
  replaceState(state: NormalizedState, commit: number, maxStateBytes: number): { stateBytes: number } {
    this.db.exec("DELETE FROM kv; DELETE FROM tombstones; DELETE FROM changes;");
    const insertKv = this.db.prepare("INSERT INTO kv(key, value, version, updated_at, updated_by, bytes) VALUES (?, ?, ?, ?, ?, ?)");
    const insertTomb = this.db.prepare("INSERT INTO tombstones(key, version, ord) VALUES (?, ?, ?)");
    const insertChange = this.db.prepare("INSERT INTO changes(commit_no, key, version, deleted) VALUES (?, ?, ?, ?)");
    let stateBytes = 0;
    for (const entry of state.entries) {
      validateMeshStateKey(entry.key);
      const value = JSON.stringify(entry.value);
      const updatedBy = JSON.stringify(entry.updatedBy);
      if (value === undefined || updatedBy === undefined) throw new Error("Mesh values must be JSON-serializable");
      const updatedAt = Number(entry.updatedAt);
      if (!Number.isFinite(updatedAt)) throw new Error(`Invalid Fabric mesh state entry ${entry.key}: updatedAt`);
      const size = entryBytes(entry.key, value, updatedBy);
      insertKv.run(entry.key, value, entry.version, updatedAt, updatedBy, size);
      insertChange.run(commit, entry.key, entry.version, 0);
      stateBytes += size;
    }
    let ord = 0;
    for (const tombstone of state.tombstones) {
      validateMeshStateKey(tombstone.key);
      insertTomb.run(tombstone.key, tombstone.version, ++ord);
      stateBytes += tombstoneBytes(tombstone.key);
    }
    if (stateBytes + ENVELOPE_BYTES > maxStateBytes) throw new Error(`Fabric mesh state exceeds ${maxStateBytes} bytes`);
    this.setMeta("high_water", state.highWater);
    this.setMeta("commit_no", commit);
    this.setMeta("state_bytes", stateBytes);
    this.setMeta("tombstone_ord", ord);
    return { stateBytes };
  }

  close(): void {
    try { if (this.db.isTransaction) this.db.exec("ROLLBACK"); } catch { /* closing anyway */ }
    this.db.close();
  }
}

// ------------------------------------------------------------------ helpers

const maxBytesOf = (options: MeshBackendOptions): number => Math.max(1, Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES));

/**
 * The migration fence (smarty-dev#6477, org decision 10-08): `custody.lock` then `.lock` (the
 * order of withMeshCustody), both held across awaits for the whole section; this tool is a
 * dedicated process. Nothing else gates a mutating operation: the census is advisory. The helper is
 * shared with a fixture's `initialize: "create"` (fence-lock.ts), the only other marker writer.
 */
const holdFence = <T>(root: string, options: MeshBackendOptions, operation: (lock: MeshLock) => Promise<T> | T): Promise<T> =>
  holdMeshFence(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs ?? DEFAULT_LOCK_MS }, operation);

const readFile = (root: string, options: MeshBackendOptions): FencedFile =>
  decodeMeshStateFile(path.join(root, STATE_JSON), maxBytesOf(options), false) as FencedFile;

/**
 * smarty-dev#6477: a zero-length (or whitespace-only) state.json holds nothing, but the strict decoder reads it
 * as damage. Under `.lock` the import of such a fresh root drops it and proceeds as for a root without one.
 */
const dropBlankStateFile = (root: string): void => {
  const file = path.join(root, STATE_JSON);
  const stat = fs.statSync(file, { throwIfNoEntry: false });
  if (!stat?.isFile() || stat.size > 4096 || fs.readFileSync(file, "utf8").trim() !== "") return;
  fs.rmSync(file, { force: true });
};

const fileEpochOf = (state: FencedFile): number => (state.backendEpoch === undefined ? 0 : storageRevision(state.backendEpoch));

/** The store's own read path (state-file.ts, through the read journal when it applies): its entries' digest. */
const storeReadDigest = (root: string, lock: MeshLock, options: MeshBackendOptions): string => {
  const reader = new StateFile({ root, maxEventBytes: 1024 * 1024, maxReadEvents: 1_000, lock },
    { maxStateBytes: maxBytesOf(options), writeReadJournal: false });
  return entriesDigest(reader.listAll("", { fresh: true }));
};

/** The epoch recorded in state.json: a bounded header read, else the full normal decoder. */
export const readStateFileEpoch = (root: string, options: Pick<MeshBackendOptions, "maxStateBytes"> = {}): number =>
  readEpochHeader(root, { decodeFileEpoch: (file) => fileEpochOf(decodeMeshStateFile(file, maxBytesOf(options), false) as FencedFile) });

const alarm = (options: MeshBackendOptions, root: string, message: string, detail: Partial<MeshBackendAlarm> = {}): MeshBackendFenceError =>
  meshFenceAlarm(options, root, message, detail);

/**
 * The advisory census (smarty-dev#6982): run the provider once, report what it saw. It never throws
 * and its result never decides anything; a failed census is reported as such and the operation
 * goes ahead on the fence alone. Undefined without a provider.
 */
const adviseCensus = async (options: MeshBackendOptions): Promise<MeshCensusAdvisory | undefined> => {
  if (!options.census) return undefined;
  const others = (list: unknown): MeshCensusWriter[] =>
    Array.isArray(list) ? (list as MeshCensusWriter[]).filter(writer => writer?.pid !== process.pid) : [];
  let advisory: MeshCensusAdvisory;
  try {
    const result = await options.census();
    advisory = { writers: others(result?.writers), unknown: others(result?.unknown) };
  } catch (error) {
    advisory = { writers: [], unknown: [], error: error instanceof Error ? error.message : String(error) };
  }
  try { options.onAdvisory?.(advisory); } catch { /* a log sink never changes the operation */ }
  return advisory;
};

const fsyncDirectory = (directory: string): void => {
  if (process.platform === "win32") return; // NTFS directory entries need no (and allow no) fsync.
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
};

const removeStaleTemps = (root: string): void => {
  for (const name of fs.readdirSync(root)) {
    if (name.startsWith(TEMP_PREFIX) && name.endsWith(".tmp")) fs.rmSync(path.join(root, name), { force: true });
  }
};

// ------------------------------------------------------------------ the reader rule

/**
 * Which representation a reader may use (plan section 5; the rule itself is `meshStateSourceOf` in
 * backend-fence.ts). Throws MeshBackendFenceError (and raises the alarm) on any mismatch. A root
 * without state.db is a legacy file root.
 */
export const resolveMeshStateSource = (root: string, options: MeshBackendOptions = {}): MeshStateSource => {
  root = path.resolve(root);
  let backend = "none";
  let epoch = 0;
  const fence = FenceDb.open(root, options, "read");
  if (fence) {
    try {
      const meta = fence.meta();
      backend = meta.backend;
      epoch = meta.epoch;
    } finally { fence.close(); }
  }
  return meshStateSourceOf(root, backend, epoch, {
    ...(options.onAlarm ? { onAlarm: options.onAlarm } : {}),
    decodeFileEpoch: (file) => fileEpochOf(decodeMeshStateFile(file, maxBytesOf(options), false) as FencedFile),
  });
};

// ------------------------------------------------------------------ import and cutover

const verifyImported = (fence: FenceDb, expected: { epoch: number; digest: string }, root: string, options: MeshBackendOptions,
  flag: "sqlite" | "importing" = "sqlite"): void => {
  const { meta, state } = fence.snapshot();
  if (meta.backend !== flag || meta.epoch !== expected.epoch) {
    throw alarm(options, root, `Fabric mesh import verification found backend=${meta.backend} epoch ${meta.epoch} (expected ${flag} at ${expected.epoch})`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  const readBack = meshSnapshotDigest(state);
  const roundTrip = meshSnapshotDigest(JSON.parse(encodeMeshStateFile(state as MeshStateFile).serialized.toString("utf8")) as MeshStateSnapshot);
  if (readBack !== expected.digest || roundTrip !== expected.digest) {
    throw alarm(options, root, `Fabric mesh import digest mismatch: state.json ${expected.digest}, state.db ${readBack}, re-encoded ${roundTrip}`,
      { backend: meta.backend, epoch: meta.epoch });
  }
};

interface FileMark { generation: string; digest: string }

const generationOf = (file: FencedFile): string => (typeof file.readGeneration === "string" ? file.readGeneration : "");

/** Reconcile: state.json must still be the one imported (G0); anything else means a writer committed. */
const assertFileUnchanged = (root: string, options: MeshBackendOptions, g0: FileMark, when: string, consequence: string): void => {
  let now: FileMark;
  try {
    const file = readFile(root, options);
    now = { generation: generationOf(file), digest: meshSnapshotDigest(file) };
  } catch (error) {
    throw alarm(options, root, `Fabric mesh state.json became unreadable ${when}: ${(error as Error).message}; ${consequence}`);
  }
  if (now.generation !== g0.generation || now.digest !== g0.digest) {
    throw alarm(options, root, `Fabric mesh state.json changed ${when} (generation ${g0.generation || "none"} -> ${now.generation || "none"}, `
      + `digest ${g0.digest} -> ${now.digest}): a file-mode writer committed; ${consequence}`);
  }
};

/**
 * A rerun after the marker landed: converge on the database, which the import verified. Writers were
 * fenced since the marker (sqlite-mode ones by `importing`), so SQLite is current; with backend=importing
 * the caller redoes only the flag commit (`commitRollForward`).
 */
const importConvergedOnMarker = (root: string, options: MeshBackendOptions, fence: FenceDb, marker: MeshStateMovedMarker): MeshImportResult => {
  const meta = fence.meta();
  const digest = String(meta.values.get("import_digest") ?? "");
  if ((meta.backend !== "sqlite" && meta.backend !== "importing") || meta.epoch !== marker.epoch || !digest) {
    throw alarm(options, root, `Fabric mesh state.json is the moved marker (epoch ${marker.epoch}) but state.db has backend=${meta.backend} `
      + `at epoch ${meta.epoch}: import refused`, { backend: meta.backend, epoch: meta.epoch, fileEpoch: marker.epoch });
  }
  const normalized = normalizeSnapshot(fence.snapshot().state);
  if (digestNormalized(normalized) !== digest) {
    throw new MeshBackendRefusedError(`Fabric mesh state.db is authoritative (backend=sqlite epoch ${meta.epoch} commit ${meta.commit}) since the cutover; nothing to import`);
  }
  verifyImported(fence, { epoch: meta.epoch, digest }, root, options, meta.backend as "sqlite" | "importing");
  options.onStep?.("import-verify");
  return { root, backend: "sqlite", epoch: meta.epoch, previousEpoch: Number(meta.values.get("import_previous_epoch") ?? meta.epoch - 1),
    entries: normalized.entries.length, tombstones: normalized.tombstones.length, highWater: normalized.highWater, digest,
    generation: String(meta.values.get("import_generation") ?? ""), converged: true };
};

const importUnderLock = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock): MeshImportResult => {
  const marker = readMeshStateMovedMarker(root);
  if (marker) return importConvergedOnMarker(root, options, fence, marker);
  dropBlankStateFile(root);
  // Under .lock no file-mode writer can commit state.json while it is read and imported.
  const file = readFile(root, options);
  const fileEpoch = fileEpochOf(file);
  const normalized = normalizeSnapshot(file);
  const digest = digestNormalized(normalized);
  const generation = generationOf(file);
  const viaStore = storeReadDigest(root, lock, options);
  if (viaStore !== entriesDigest(normalized.entries)) {
    throw alarm(options, root, "Fabric mesh state.json decodes differently through the store read path (journal); import refused");
  }
  options.onStep?.("import-read");
  const result = (epoch: number, previousEpoch: number, converged: boolean): MeshImportResult => ({
    root, backend: "sqlite", epoch, previousEpoch, entries: normalized.entries.length, tombstones: normalized.tombstones.length,
    highWater: normalized.highWater, digest, generation, converged,
  });
  const before = fence.meta();
  if (before.backend === "sqlite" && (before.commit > 0 || before.values.has("import_digest"))) {
    // A rerun after the commit converges only on the identical state; anything else is live sqlite authority.
    if (before.values.get("import_digest") === digest && meshSnapshotDigest(fence.snapshot().state) === digest) {
      verifyImported(fence, { epoch: before.epoch, digest }, root, options);
      options.onStep?.("import-verify");
      return result(before.epoch, Number(before.values.get("import_previous_epoch") ?? before.epoch - 1), true);
    }
    throw new MeshBackendRefusedError(`Fabric mesh state.db is authoritative (backend=sqlite epoch ${before.epoch} commit ${before.commit}) and differs from state.json: roll back first, then import`);
  }
  const { epoch, previousEpoch } = fence.transaction(() => {
    const meta = fence.meta();
    const counts = fence.counts();
    const fresh = meta.backend === "sqlite" && meta.commit === 0 && counts.kv === 0 && counts.tombstones === 0;
    const rollForward = meta.backend === "file" && fileEpoch === meta.epoch;
    // Killed before the marker (review round 5): state.json is still the authority, redo from the import.
    const redo = meta.backend === "importing" && fileEpoch < meta.epoch;
    if (!Number.isSafeInteger(meta.epoch) || meta.epoch < 0 || fileEpoch > meta.epoch || (!fresh && !rollForward && !redo)) {
      throw alarm(options, root, `Fabric mesh import refused: state.db backend=${meta.backend} epoch ${meta.epoch} commit ${meta.commit}, state.json epoch ${fileEpoch}`,
        { backend: meta.backend, epoch: meta.epoch, fileEpoch });
    }
    const next = redo ? meta.epoch : meta.epoch + 1;
    const previous = redo ? Number(meta.values.get("import_previous_epoch") ?? fileEpoch) : meta.epoch;
    fence.replaceState(normalized, meta.commit + 1, maxBytesOf(options));
    // Not authoritative yet: backend=sqlite is committed only after the moved marker (commitRollForward).
    fence.setMeta("backend", "importing");
    fence.setMeta("epoch", next);
    fence.setMeta("import_digest", digest);
    fence.setMeta("import_generation", generation);
    fence.setMeta("import_previous_epoch", previous);
    fence.setMeta("imported_at", Date.now());
    // Verify inside the transaction: a mismatch rolls the whole import back.
    verifyImported(fence, { epoch: next, digest }, root, options, "importing");
    // Reconcile right before COMMIT: a state.json that moved since G0 keeps the flag where it was.
    assertFileUnchanged(root, options, { generation, digest }, "before the flag commit", "import aborted, the backend flag is unchanged");
    return { epoch: next, previousEpoch: previous };
  });
  options.onStep?.("import-commit");
  verifyImported(fence, { epoch, digest }, root, options, "importing");
  options.onStep?.("import-verify");
  return result(epoch, previousEpoch, false);
};

/**
 * Import (file -> sqlite, also the roll forward) and cutover are ONE fenced section (pi-fabric#627
 * review round 4): an import commits backend=sqlite, so it holds the same fence as cutover
 * (`.lock` and `custody.lock` throughout; the census only advises), and installs the moved marker
 * BEFORE it commits backend=sqlite (review round 5), so a legacy file-mode writer can never commit
 * to a state.json that readers ignore, even after a crash.
 */
export const importMeshState = (root: string, options: MeshBackendOptions = {}): Promise<MeshCutoverResult> =>
  fencedCutover(root, options);

/**
 * Cutover: one critical section fenced on `custody.lock` and the mesh `.lock` (file-mode writers
 * serialize on it): advisory census -> G0 -> import at backend=importing E+1 -> verify -> state.json
 * against G0 -> the moved marker -> backend=sqlite. A moved state.json found after the import fails the
 * cutover and runs the rollback fence back to backend=file at E+2 before the locks are released.
 * The census never gates it (smarty-dev#6982): its report is returned and passed to `onAdvisory`.
 */
export const cutoverMeshState = (root: string, options: MeshBackendOptions = {}): Promise<MeshCutoverResult> =>
  fencedCutover(root, options);

const fencedCutover = async (root: string, options: MeshBackendOptions): Promise<MeshCutoverResult> => {
  root = path.resolve(root);
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory()) throw new MeshBackendRefusedError(`No mesh root at ${root}`);
  return holdFence(root, options, async (lock) => {
    const census = await adviseCensus(options);
    const fence = FenceDb.open(root, options, "write", true)!;
    try {
      // G0, import, flag at E+1 and verification; a state.json that moved before COMMIT aborts it.
      const imported = importUnderLock(root, options, fence, lock);
      const failures: string[] = [];
      let conflictFile: string | undefined;
      // A rerun after the marker: the reconcile ran before it, and the marker is not state.
      const moved = readMeshStateMovedMarker(root)?.epoch === imported.epoch;
      try {
        if (!moved) assertFileUnchanged(root, options, { generation: imported.generation, digest: imported.digest }, "after the flag commit", "cutover fails");
      } catch (error) {
        failures.push((error as Error).message);
        // Keep what a writer that ignored `.lock` committed: the rollback export replaces state.json.
        if (fs.existsSync(path.join(root, STATE_JSON))) {
          conflictFile = path.join(root, `${STATE_JSON}.cutover-conflict-${Date.now()}-${process.pid}`);
          fs.copyFileSync(path.join(root, STATE_JSON), conflictFile);
        }
      }
      if (failures.length === 0) {
        options.onStep?.("cutover-reconcile");
        const stateCopy = path.join(root, `${STATE_JSON}.cutover-${imported.epoch}`);
        if (!moved) retireStateFile(root, options, imported.epoch, stateCopy);
        commitRollForward(root, options, fence, imported);
        return { ...imported, writers: census?.writers ?? [], ...(census ? { census } : {}), stateCopy };
      }
      const reason = failures.join("; ");
      try {
        options.onAlarm?.({ code: "FABRIC_MESH_BACKEND_LATE_WRITER", root, backend: "sqlite", epoch: imported.epoch,
          message: `Fabric mesh cutover failed: ${reason}; rolling back to backend=file` });
      } catch { /* an alarm sink never masks the failure */ }
      let rollback: MeshRollbackResult;
      try { rollback = rollbackUnderLock(root, options, fence, lock); }
      catch (error) {
        throw new MeshCutoverFailedError(`Fabric mesh cutover failed: ${reason}; the rollback to backend=file did not complete `
          + `(${(error as Error).message}): rerun rollback`, undefined, conflictFile);
      }
      throw new MeshCutoverFailedError(`Fabric mesh cutover failed: ${reason}; rolled back to backend=file at epoch ${rollback.epoch}`
        + (conflictFile ? `; the conflicting state.json is kept at ${conflictFile}` : ""), rollback, conflictFile);
    } finally { fence.close(); }
  });
};

/**
 * Roll-forward step 3, still under the `.lock` hold, after the import at backend=importing and both
 * checks: keep state.json at `stateCopy` (fsync), then replace it with the moved marker (temp, fsync,
 * rename, directory fsync). A legacy writer that gets `.lock` afterwards reads it strictly and
 * fails with "invalid state format" before it commits (pi-fabric#627 review round 3).
 */
const retireStateFile = (root: string, options: MeshBackendOptions, epoch: number, stateCopy: string): void => {
  const statePath = path.join(root, STATE_JSON);
  if (fs.existsSync(statePath)) {
    fs.copyFileSync(statePath, stateCopy);
    const copied = fs.openSync(stateCopy, "r+");
    try { fs.fsyncSync(copied); } finally { fs.closeSync(copied); }
    fsyncDirectory(root);
  }
  options.onStep?.("cutover-copy");
  writeMovedMarker(root, epoch);
  options.onStep?.("cutover-marker");
};

/**
 * Roll-forward step 4, the LAST (pi-fabric#627 review round 5): backend importing -> sqlite at E+1, only
 * with the moved marker in place, so a crash anywhere before leaves state.json the authority (no marker)
 * or the marker fencing every legacy writer. A rerun at backend=sqlite verifies only.
 */
const commitRollForward = (root: string, options: MeshBackendOptions, fence: FenceDb, imported: { epoch: number; digest: string }): void => {
  const marker = readMeshStateMovedMarker(root);
  if (marker?.epoch !== imported.epoch) {
    throw alarm(options, root, `Fabric mesh roll-forward refused: backend=sqlite at epoch ${imported.epoch} needs the moved marker first `
      + `(state.json ${marker ? `is the marker at epoch ${marker.epoch}` : "is not the marker"})`, { epoch: imported.epoch });
  }
  const committed = fence.transaction(() => {
    const current = fence.meta();
    if (current.backend === "sqlite" && current.epoch === imported.epoch) return false;
    verifyImported(fence, imported, root, options, "importing");
    fence.setMeta("backend", "sqlite");
    return true;
  });
  if (committed) options.onStep?.("cutover-flag");
  verifyImported(fence, imported, root, options);
};

const writeMovedMarker = (root: string, epoch: number): void => {
  const temporary = path.join(root, `${TEMP_PREFIX}${process.pid}-${randomUUID()}.tmp`);
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeSync(descriptor, encodeMeshStateMovedMarker(epoch));
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  renameAtomic(temporary, path.join(root, STATE_JSON));
  fsyncDirectory(root);
};

// ------------------------------------------------------------------ rollback

/**
 * Rollback (pi-fabric#627 review round 4) keeps ONE invariant: while the moved marker is state.json,
 * legacy writers fail closed (their strict read throws "invalid state format") and new ones are
 * refused (the reader rule reads SQLite); the marker is removed only by the very last step, after
 * backend=file is committed. Under one fence hold (`custody.lock`, then `.lock`):
 *   (1) the operator stopped every writer (the advisory census only reports); the marker is put in place if state.json is not it;
 *   (2) flag sqlite -> exporting at E+1 (BEGIN IMMEDIATE, FULL): sqlite-mode writers fail closed;
 *   (3) export to `state.json.rollback-<E+1>.tmp`, fsync it and the directory, and verify it by
 *       reading it back (epoch, generation, recorded digest, file digest = database digest);
 *   (4) commit backend=file at E+1 with the export's digest and generation;
 *   (5) rename the temp over the marker, fsync the directory (then the store read path re-checks it).
 * Rerun after a crash: exporting -> redo from (3); file + marker + verified temp -> redo (5);
 * file + marker + no (or an unverifiable) temp -> redo (3) with the recorded generation, then (5);
 * file + a real state.json -> verify only.
 */
const rollbackTempName = (epoch: number): string => `${STATE_JSON}.rollback-${epoch}.tmp`;
const ROLLBACK_TEMP = /^state\.json\.rollback-\d+\.tmp$/;

const removeRollbackTemps = (root: string, keep?: string): void => {
  for (const name of fs.readdirSync(root)) {
    if (ROLLBACK_TEMP.test(name) && name !== keep) fs.rmSync(path.join(root, name), { force: true });
  }
};

interface ExportMark { epoch: number; digest: string; generation: string }

/** Why `file` is not the verified export `expected` of the database snapshot `dbDigest`, or undefined. */
const exportMismatch = (file: string, options: MeshBackendOptions, expected: ExportMark, dbDigest: string): string | undefined => {
  let back: FencedFile;
  try { back = decodeMeshStateFile(file, maxBytesOf(options), false) as FencedFile; }
  catch (error) { return `unreadable (${(error as Error).message})`; }
  const fileEpoch = fileEpochOf(back);
  if (fileEpoch !== expected.epoch) return `epoch ${fileEpoch}, expected ${expected.epoch}`;
  if (back.readGeneration !== expected.generation) return "replaced after the export (another readGeneration)";
  const fileDigest = meshSnapshotDigest(back);
  if (back.backendDigest !== expected.digest || fileDigest !== expected.digest || dbDigest !== expected.digest) {
    return `digest mismatch: recorded ${String(back.backendDigest)}, file ${fileDigest}, state.db ${dbDigest}, expected ${expected.digest}`;
  }
  return undefined;
};

/**
 * Step 3: the committed snapshot through the normal encoder (readGeneration first; a rerun at
 * backend=file reuses the recorded generation) into `state.json.rollback-<epoch>.tmp`, fsync, directory
 * fsync, then read back through the normal decoder and verified. state.json (the marker) is untouched.
 */
const writeExportTemp = (root: string, options: MeshBackendOptions, fence: FenceDb, epoch: number, recorded?: { digest: string; generation: string }):
  ExportMark & { temporary: string } => {
  removeStaleTemps(root);
  removeRollbackTemps(root);
  const { meta, state } = fence.snapshot();
  const expectedFlag = recorded ? "file" : "exporting";
  if (meta.backend !== expectedFlag || meta.epoch !== epoch) {
    throw alarm(options, root, `Fabric mesh export expected backend=${expectedFlag} at epoch ${epoch}, found ${meta.backend} at ${meta.epoch}`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  const digest = meshSnapshotDigest(state);
  if (recorded && recorded.digest !== digest) {
    throw alarm(options, root, `Fabric mesh backend=file at epoch ${epoch} recorded export ${recorded.digest} but state.db is ${digest}`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  const generation = recorded?.generation || randomUUID();
  const exported: FencedFile = { readGeneration: generation, backendEpoch: epoch, backendDigest: digest, format: 1, revisionFormat: 2,
    entries: state.entries, versions: state.versions, tombstoneOrder: state.tombstoneOrder, highWater: state.highWater };
  const serialized = encodeMeshStateFile(exported).serialized;
  if (serialized.byteLength > maxBytesOf(options)) throw new Error(`Fabric mesh state exceeds ${maxBytesOf(options)} bytes`);
  const temporary = path.join(root, rollbackTempName(epoch));
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    let offset = 0;
    while (offset < serialized.byteLength) offset += fs.writeSync(descriptor, serialized, offset, serialized.byteLength - offset);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fsyncDirectory(root);
  options.onStep?.("rollback-export-temp");
  const mismatch = exportMismatch(temporary, options, { epoch, digest, generation }, meshSnapshotDigest(fence.snapshot().state));
  if (mismatch) throw alarm(options, root, `Fabric mesh export verification of ${temporary}: ${mismatch}`, { epoch, fileEpoch: epoch });
  options.onStep?.("rollback-verify");
  return { epoch, digest, generation, temporary };
};

/** After step 5: state.json is the export, and the store's own read path (read journal) agrees. */
const verifyExport = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock, expected: ExportMark): void => {
  const mismatch = exportMismatch(path.join(root, STATE_JSON), options, expected, meshSnapshotDigest(fence.snapshot().state));
  if (mismatch) throw alarm(options, root, `Fabric mesh export verification: state.json ${mismatch}`, { epoch: expected.epoch });
  if (storeReadDigest(root, lock, options) !== entriesDigest(normalizeSnapshot(readFile(root, options)).entries)) {
    throw alarm(options, root, "Fabric mesh export verification: the store read path disagrees with state.json", { epoch: expected.epoch });
  }
};

/** A rerun on a switched root with a real state.json: a later file-mode commit (new readGeneration) only needs the epoch. */
const reverifySwitched = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock, meta: DbMeta): { digest: string; generation: string } => {
  const back = readFile(root, options);
  const fileEpoch = fileEpochOf(back);
  if (fileEpoch !== meta.epoch) {
    throw alarm(options, root, `Fabric mesh backend=file at epoch ${meta.epoch} but state.json carries epoch ${fileEpoch}`,
      { backend: meta.backend, epoch: meta.epoch, fileEpoch });
  }
  const digest = String(meta.values.get("export_digest") ?? "");
  const generation = String(meta.values.get("export_generation") ?? "");
  if (back.readGeneration === generation) verifyExport(root, options, fence, lock, { epoch: meta.epoch, digest, generation });
  return { digest: meshSnapshotDigest(back), generation: String(back.readGeneration ?? "") };
};

/** The marker is state.json (written at `epoch` when it is not): from here on legacy writers fail closed. */
const ensureMovedMarker = (root: string, epoch: number): void => {
  if (!readMeshStateMovedMarker(root)) writeMovedMarker(root, epoch);
};

/** Step 2: BEGIN IMMEDIATE at FULL checks backend=sqlite (importing: a failed cutover) at E, sets exporting at E+1. */
const rollbackFlag = (root: string, options: MeshBackendOptions, fence: FenceDb, from: number, flag = "sqlite"): number => {
  fence.transaction(() => {
    const current = fence.meta();
    if (current.backend !== flag || current.epoch !== from) {
      throw new MeshBackendRefusedError(`Fabric mesh rollback raced: backend=${current.backend} epoch ${current.epoch}`);
    }
    fence.setMeta("backend", "exporting");
    fence.setMeta("epoch", from + 1);
    fence.setMeta("rollback_from_epoch", from);
  });
  options.onStep?.("rollback-flag");
  return from + 1;
};

/** Step 4: BEGIN IMMEDIATE at FULL checks backend=exporting at E+1, sets file (the marker still stands). */
const rollbackSwitch = (root: string, options: MeshBackendOptions, fence: FenceDb, exported: ExportMark): void => {
  fence.transaction(() => {
    const current = fence.meta();
    if (current.backend !== "exporting" || current.epoch !== exported.epoch) {
      throw alarm(options, root, `Fabric mesh rollback switch found backend=${current.backend} epoch ${current.epoch}`,
        { backend: current.backend, epoch: current.epoch });
    }
    fence.setMeta("backend", "file");
    fence.setMeta("export_digest", exported.digest);
    fence.setMeta("export_generation", exported.generation);
    fence.setMeta("exported_at", Date.now());
  });
  options.onStep?.("rollback-switch");
};

/** Step 5, the very last: the verified temp replaces the marker (atomic rename, directory fsync). */
const rollbackReplace = (root: string, options: MeshBackendOptions, temporary: string): void => {
  renameAtomic(temporary, path.join(root, STATE_JSON));
  fsyncDirectory(root);
  options.onStep?.("rollback-replace");
};

/**
 * Steps 2 to 5 (and the reruns) under the fence the caller holds. `fromCutover`: a failed cutover
 * rolls back its own backend=importing; a standalone rollback refuses an interrupted import instead.
 */
const rollbackLocked = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock, steps: number[], fromCutover: boolean):
  MeshRollbackResult => {
  let meta = fence.meta();
  const done = (exported: ExportMark, converged: boolean): MeshRollbackResult =>
    ({ root, backend: "file", epoch: exported.epoch, digest: exported.digest, generation: exported.generation, steps, converged });
  if (meta.backend === "file") {
    if (!readMeshStateMovedMarker(root)) {
      removeRollbackTemps(root);
      return { ...done({ epoch: meta.epoch, ...reverifySwitched(root, options, fence, lock, meta) }, true), steps: [] };
    }
    // Crashed between the switch (4) and the replace (5): the marker still fences legacy writers.
    const recorded = { epoch: meta.epoch, digest: String(meta.values.get("export_digest") ?? ""), generation: String(meta.values.get("export_generation") ?? "") };
    let temporary = path.join(root, rollbackTempName(meta.epoch));
    const verified = fs.existsSync(temporary) && recorded.generation !== ""
      && exportMismatch(temporary, options, recorded, meshSnapshotDigest(fence.snapshot().state)) === undefined;
    if (!verified) {
      ({ temporary } = writeExportTemp(root, options, fence, meta.epoch, recorded));
      steps.push(3);
    }
    rollbackReplace(root, options, temporary);
    steps.push(5);
    verifyExport(root, options, fence, lock, recorded);
    return done(recorded, false);
  }
  if (meta.backend === "importing" && !fromCutover) {
    throw new MeshBackendRefusedError(`Fabric mesh backend=importing at epoch ${meta.epoch}: an import or cutover was interrupted; rerun it (or roll it back by rerunning after it completes)`);
  }
  if (meta.backend === "sqlite" || meta.backend === "importing") {
    // A state.json that is not the marker (an older tool, a failed cutover) is not authority under sqlite;
    // a failed cutover at importing exports the state it imported (a conflicting state.json is kept aside).
    ensureMovedMarker(root, meta.epoch);
    rollbackFlag(root, options, fence, meta.epoch, meta.backend);
    steps.push(2);
    meta = fence.meta();
  }
  if (meta.backend !== "exporting") {
    throw alarm(options, root, `Fabric mesh rollback: backend flag ${JSON.stringify(meta.backend)} is not sqlite, exporting or file`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  ensureMovedMarker(root, meta.epoch);
  const exported = writeExportTemp(root, options, fence, meta.epoch);
  steps.push(3);
  rollbackSwitch(root, options, fence, exported);
  steps.push(4);
  rollbackReplace(root, options, exported.temporary);
  steps.push(5);
  verifyExport(root, options, fence, lock, exported);
  return done(exported, false);
};

/** A failed cutover's way back to file, under its held fence. */
const rollbackUnderLock = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock): MeshRollbackResult => {
  const meta = fence.meta();
  if (meta.backend !== "sqlite" && meta.backend !== "importing") {
    throw alarm(options, root, `Fabric mesh cutover rollback expected backend=importing or sqlite, found ${meta.backend} at epoch ${meta.epoch}`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  return rollbackLocked(root, options, fence, lock, [], true);
};

/** Rollback (sqlite -> file) with the R1 fence; reruns converge from the stored flag and the marker. */
export const rollbackMeshState = async (root: string, options: MeshBackendOptions = {}): Promise<MeshRollbackResult> => {
  root = path.resolve(root);
  const fence = FenceDb.open(root, options, "write");
  if (!fence) throw new MeshBackendRefusedError(`No state.db at ${root}: the root is on the file backend`);
  try {
    return await holdFence(root, options, async lock => {
      const census = await adviseCensus(options);
      // Step 1 is the operator's (every v3 writer and the projector stopped); the census only reports.
      const steps: number[] = fence.meta().backend === "sqlite" ? [1] : [];
      return { ...rollbackLocked(root, options, fence, lock, steps, false), ...(census ? { census } : {}) };
    });
  } finally { fence.close(); }
};

/**
 * `exporting` back to `sqlite`, keeping epoch E+1 (stores opened at E reopen). The fence
 * (`custody.lock`, `.lock`) is taken FIRST; the marker is ensured (restored if anything replaced it) before the flag moves, so no
 * legacy writer queued on `.lock` ever sees a real state.json while SQLite is the authority.
 * Like the other mutating commands, it runs the advisory census inside the fence and returns its
 * report (also passed to `onAdvisory`); the census never gates it.
 */
export const abortMeshRollback = async (root: string, options: MeshBackendOptions = {}): Promise<MeshAbortRollbackResult> => {
  root = path.resolve(root);
  const fence = FenceDb.open(root, options, "write");
  if (!fence) throw new MeshBackendRefusedError(`No state.db at ${root}`);
  try {
    return await holdFence(root, options, async () => {
      const census = await adviseCensus(options);
      const advisory = census ? { census } : {};
      const meta = fence.meta();
      const converged = meta.backend === "sqlite" && Number(meta.values.get("aborted_rollback_epoch")) === meta.epoch;
      if (!converged && meta.backend !== "exporting") {
        throw new MeshBackendRefusedError(`abort-rollback needs backend=exporting (found ${meta.backend} at epoch ${meta.epoch})`);
      }
      ensureMovedMarker(root, meta.epoch);
      removeRollbackTemps(root);
      removeStaleTemps(root);
      if (converged) return { root, backend: "sqlite" as const, epoch: meta.epoch, converged: true, ...advisory };
      fence.transaction(() => {
        const current = fence.meta();
        if (current.backend !== "exporting" || current.epoch !== meta.epoch) {
          throw new MeshBackendRefusedError(`abort-rollback raced: backend=${current.backend} epoch ${current.epoch}`);
        }
        fence.setMeta("backend", "sqlite");
        fence.setMeta("aborted_rollback_epoch", meta.epoch);
      });
      options.onStep?.("abort-rollback");
      return { root, backend: "sqlite" as const, epoch: meta.epoch, converged: false, ...advisory };
    });
  } finally { fence.close(); }
};

// ------------------------------------------------------------------ status

export const meshBackendStatus = async (root: string, options: MeshBackendOptions = {}): Promise<MeshBackendStatus> => {
  root = path.resolve(root);
  const status: MeshBackendStatus = { root, backend: "none", epoch: 0, reader: { source: "file" }, fenceHolds: true };
  const fence = FenceDb.open(root, options, "read");
  if (fence) {
    try {
      const { meta, state } = fence.snapshot();
      status.backend = meta.backend;
      status.epoch = meta.epoch;
      status.commit = meta.commit;
      status.dbDigest = meshSnapshotDigest(state);
      const text = (name: string): string | undefined => (meta.values.has(name) ? String(meta.values.get(name)) : undefined);
      status.importDigest = text("import_digest");
      status.exportDigest = text("export_digest");
      status.exportGeneration = text("export_generation");
    } finally { fence.close(); }
  }
  const marker = readMeshStateMovedMarker(root);
  if (marker) {
    status.fileEpoch = marker.epoch;
    status.fileMoved = true;
  } else try {
    const file = readFile(root, options);
    status.fileEpoch = fileEpochOf(file);
    status.fileGeneration = typeof file.readGeneration === "string" ? file.readGeneration : undefined;
    status.fileDigest = meshSnapshotDigest(file);
  } catch (error) { status.fileError = (error as Error).message; }
  status.fenceHolds = status.fileEpoch === undefined ? status.backend !== "file"
    : status.fileEpoch <= status.epoch && (status.fileMoved ? ["sqlite", "importing", "exporting", "file"].includes(status.backend)
      : status.backend !== "file" || status.fileEpoch === status.epoch);
  const { onAlarm: _quiet, ...quiet } = options;
  try { status.reader = { source: resolveMeshStateSource(root, quiet).source }; }
  catch (error) { status.reader = { error: (error as Error).message }; }
  // Advisory only: the census never enters fenceHolds or the reader decision.
  const census = await adviseCensus(options);
  if (census?.error !== undefined) status.censusError = census.error;
  else if (census) { status.writers = census.writers; status.unknownWriters = census.unknown; }
  return status;
};
