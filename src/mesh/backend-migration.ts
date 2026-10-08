/**
 * Mesh state backend migration: import, cutover and rollback with the R1 fence (smarty-dev#6477, lane L4b).
 *
 * Plan section 5 (docs/mesh-lock-plan.md) is the contract. `meta.backend` in `<mesh>/state.db` is
 * `sqlite`, `exporting` or `file`; `meta.epoch` only grows; `state.json` records the epoch of
 * its export in `backendEpoch` (and the export's snapshot digest in `backendDigest`), right after
 * `readGeneration`, so older readers and writers keep and ignore both fields.
 *
 * Rollback commit order (each step is a crash point; rerun converges from the stored flag):
 *   1. The census shows no writer left (or the operator attests it with `assumeNoWriters`).
 *   2. Flag: one BEGIN IMMEDIATE transaction at synchronous=FULL checks backend=sqlite at epoch E,
 *      sets backend=exporting and epoch=E+1. state-sqlite.ts writers check the flag after BEGIN
 *      IMMEDIATE and fail closed (MeshStateRetiredError), so nothing commits after this point.
 *   3. Export, under the mesh `.lock`: the committed snapshot through the normal state.json encoder
 *      (fresh readGeneration, revisionFormat 2, backendEpoch E+1, backendDigest) into a temporary
 *      file; fsync it, rename it over state.json, fsync the directory. state.db* stay in place.
 *   4. Verify, still under `.lock`: the normal decoder reads state.json back; backendEpoch must be
 *      E+1 and its digest must equal the database snapshot's (and the store's own read path agrees).
 *   5. Switch: BEGIN IMMEDIATE at FULL checks backend=exporting at E+1 and sets backend=file. Older
 *      binaries may start only after this step.
 * A rerun with `exporting` repeats 3 to 5 (the fence blocked every write, so the export is
 * identical); with `file` it repeats 4. `abortRollback` sets exporting back to sqlite and keeps E+1.
 * The file epoch never exceeds the database epoch; with backend=file they are equal only after a
 * verified export. With backend=sqlite a file epoch equal to the database epoch (an aborted rollback)
 * is not authority: the reader rule below reads SQLite.
 *
 * Reader rule (`resolveMeshStateSource`): use state.json only when backend=file and its epoch equals
 * meta.epoch; with sqlite or exporting read SQLite; any other combination (a file epoch above the
 * database epoch, backend=file at another epoch, an unknown flag, a missing state.db under an
 * epoch-stamped state.json) fails closed with `MeshBackendFenceError` and an alarm.
 *
 * Import (file -> sqlite, also the roll forward): under the mesh `.lock` (no file-mode writer can
 * commit), state.json is read through the normal decoder and cross-checked against the store's own
 * read path (which follows the read journal), then written into state.db in ONE transaction that
 * replaces kv, tombstones and the change feed, sets backend=sqlite and epoch=E+1 and verifies the
 * digest both ways (database read-back, and the database snapshot through the encoder and decoder)
 * before COMMIT, and once more after it. Right before COMMIT it re-reads state.json: a changed
 * readGeneration or digest (a writer that ignored `.lock`) rolls the transaction back, so the flag
 * never moves on a state the import did not see. A fresh state.db is created by this tool at
 * backend=file, epoch 0 (the legacy file authority), never at an authoritative empty sqlite.
 *
 * Cutover holds the mesh `.lock` for the WHOLE critical section (file-mode writers serialize on it, so
 * none commits inside): census (no writer outside the cutover modes) -> read state.json (generation
 * and digest G0) -> import, flag sqlite at E+1 (BEGIN IMMEDIATE, FULL), digest verification -> second
 * census -> state.json re-read against G0. Only then is `.lock` released. A file-mode writer seen by
 * the second census (or a failed census, or a state.json that moved) fails the cutover: the rollback
 * fence runs under the same `.lock` back to backend=file at E+2 and the error is thrown
 * (MeshCutoverFailedError); a cutover never succeeds "with an alarm". Cutover writes nothing to
 * state.json except through that rollback export. Writers that start after the flag are fenced by
 * `assertFileStateWritable` (backend-fence.ts, re-exported here): StateFile calls it on every commit
 * under `.lock`, right before the state.json rename, so a refused write leaves state.json untouched.
 *
 * Lock order: this tool is the one place where `.lock` is held around a state transaction
 * (R20). Its own connection uses a synchronous busy handler (default 5 s): it is a dedicated
 * process and the census stopped every writer, so it never freezes a session's event loop.
 * Schema knowledge (tables, meta keys, the byte accounting) mirrors state-sqlite.ts; the tests
 * check both against a store-created database.
 */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { renameAtomic } from "../core/atomic-write.js";
import { storageRevision } from "../verified/storage.js";
import { assertFileStateWritable, MeshBackendFenceError, meshFenceAlarm, meshStateSourceOf, readStateFileEpoch as readEpochHeader,
  type MeshBackendAlarm, type MeshStateSource } from "./backend-fence.js";
import type { MeshIdentity } from "./event-log.js";
import { MeshLock } from "./mesh-lock.js";
import { decodeMeshStateFile, encodeMeshStateFile, StateFile, type MeshStateEntry, type MeshStateFile } from "./state-file.js";
import { filesystemRefusal, MeshStateUnsupportedError, openNodeSqlite, validateMeshStateKey, type SqliteConnection,
  type SqliteOpener, type SqliteRow } from "./state-sqlite.js";

// The writer fence lives in a leaf module (state-file.ts calls it; this module imports state-file.ts).
export { assertFileStateWritable, MeshBackendFenceError, type MeshBackendAlarm, type MeshStateSource };

export type MeshBackendFlag = "sqlite" | "exporting" | "file";

/** Crash points, in commit order. `onStep` runs synchronously right after each one. */
export type MeshBackendStep =
  | "import-read" | "import-commit" | "import-verify"
  | "rollback-flag" | "rollback-export-temp" | "rollback-export" | "rollback-verify" | "rollback-switch"
  | "abort-rollback" | "cutover-reconcile";

export interface MeshCensusWriter { pid: number; release: string; mode: string }
/** Provided by the census lane (writer-census.ts): every live process that may write the root. */
export type MeshWriterCensus = () => Promise<{ writers: MeshCensusWriter[] }>;

export interface MeshBackendOptions {
  /** The writer census. Cutover and rollback refuse without it unless `assumeNoWriters`. */
  census?: MeshWriterCensus;
  /** Operator attestation that no process writes the root (no census lane available). */
  assumeNoWriters?: boolean;
  /** Census modes a cutover accepts; any other mode (`file` above all) refuses. Default sqlite, auto. */
  cutoverModes?: readonly string[];
  /** The mesh `.lock` protocol of the fleet. Default 1 (as mesh-bridge). */
  lockProtocol?: 1 | 2;
  /** Budget for the mesh `.lock`. Default 60 s. */
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
 * A cutover found a file-mode writer (or a moved state.json, or no census) inside its critical
 * section: it did not succeed. `rollback` is the fence run back to backend=file (absent when the
 * flag never moved or the rollback itself failed; rerun `rollback` then).
 */
export class MeshCutoverFailedError extends MeshBackendFenceError {
  constructor(message: string, readonly lateWriters: MeshCensusWriter[], readonly rollback?: MeshRollbackResult,
    readonly conflictFile?: string) {
    super(message);
    this.name = "MeshCutoverFailedError";
  }
}

/** A precondition (census, flag) refuses the operation; nothing was changed. */
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
  writers: MeshCensusWriter[];
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
}

export interface MeshAbortRollbackResult { root: string; backend: "sqlite"; epoch: number; converged: boolean }

export interface MeshBackendStatus {
  root: string;
  /** `none` when state.db is absent or uninitialised (a legacy file root). */
  backend: string;
  epoch: number;
  commit?: number;
  fileEpoch?: number;
  fileGeneration?: string | undefined;
  fileError?: string;
  dbDigest?: string;
  fileDigest?: string;
  importDigest?: string | undefined;
  exportDigest?: string | undefined;
  exportGeneration?: string | undefined;
  reader: { source: "sqlite" | "file" } | { error: string };
  /** File epoch <= database epoch; with backend=file equal. */
  fenceHolds: boolean;
  writers?: MeshCensusWriter[];
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
const DEFAULT_CUTOVER_MODES = ["sqlite", "auto"];
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

const meshLock = (root: string, options: MeshBackendOptions): MeshLock =>
  new MeshLock(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs ?? DEFAULT_LOCK_MS }, () => undefined);

const withMeshLock = <T>(root: string, options: MeshBackendOptions, operation: (lock: MeshLock) => T): Promise<T> => {
  const lock = meshLock(root, options);
  return lock.withLock(() => operation(lock), options.lockTimeoutMs ?? DEFAULT_LOCK_MS, "other");
};

/** `.lock` held across awaits (the cutover's census): this tool is a dedicated process. */
const holdMeshLock = <T>(root: string, options: MeshBackendOptions, operation: (lock: MeshLock) => Promise<T>): Promise<T> => {
  const lock = meshLock(root, options);
  return lock.withLockAcrossAwait(() => operation(lock), options.lockTimeoutMs ?? DEFAULT_LOCK_MS, "other");
};

const readFile = (root: string, options: MeshBackendOptions): FencedFile =>
  decodeMeshStateFile(path.join(root, STATE_JSON), maxBytesOf(options), false) as FencedFile;

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

const censusWriters = async (options: MeshBackendOptions): Promise<MeshCensusWriter[] | undefined> => {
  if (!options.census) return undefined;
  const result = await options.census();
  if (!result || !Array.isArray(result.writers)) throw new MeshBackendRefusedError("The writer census returned no writer list");
  return result.writers.filter(writer => writer.pid !== process.pid);
};

const describeWriters = (writers: MeshCensusWriter[]): string =>
  writers.map(writer => `pid ${writer.pid} (${writer.mode}, ${writer.release})`).join(", ");

const censusMissing = (operation: string, need: string): MeshBackendRefusedError =>
  new MeshBackendRefusedError(`${operation} needs the writer census (${need}); without one pass --assume-no-writers after stopping every writer`);

const requireCensus = async (options: MeshBackendOptions, operation: string,
  refuses: (writer: MeshCensusWriter) => boolean, need: string): Promise<MeshCensusWriter[]> => {
  const writers = await censusWriters(options);
  if (writers === undefined) {
    if (options.assumeNoWriters) return [];
    throw censusMissing(operation, need);
  }
  const offenders = writers.filter(refuses);
  if (offenders.length > 0) throw new MeshBackendRefusedError(`${operation} refused: the census shows ${describeWriters(offenders)} (${need})`);
  return writers;
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

const verifyImported = (fence: FenceDb, expected: { epoch: number; digest: string }, root: string, options: MeshBackendOptions): void => {
  const { meta, state } = fence.snapshot();
  if (meta.backend !== "sqlite" || meta.epoch !== expected.epoch) {
    throw alarm(options, root, `Fabric mesh import verification found backend=${meta.backend} epoch ${meta.epoch} (expected sqlite at ${expected.epoch})`,
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

const importUnderLock = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock): MeshImportResult => {
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
    if (!Number.isSafeInteger(meta.epoch) || meta.epoch < 0 || fileEpoch > meta.epoch || (!fresh && !rollForward)) {
      throw alarm(options, root, `Fabric mesh import refused: state.db backend=${meta.backend} epoch ${meta.epoch} commit ${meta.commit}, state.json epoch ${fileEpoch}`,
        { backend: meta.backend, epoch: meta.epoch, fileEpoch });
    }
    const next = meta.epoch + 1;
    fence.replaceState(normalized, meta.commit + 1, maxBytesOf(options));
    fence.setMeta("backend", "sqlite");
    fence.setMeta("epoch", next);
    fence.setMeta("import_digest", digest);
    fence.setMeta("import_generation", generation);
    fence.setMeta("import_previous_epoch", meta.epoch);
    fence.setMeta("imported_at", Date.now());
    // Verify inside the transaction: a mismatch rolls the whole import back.
    verifyImported(fence, { epoch: next, digest }, root, options);
    // Reconcile right before COMMIT: a state.json that moved since G0 keeps the flag where it was.
    assertFileUnchanged(root, options, { generation, digest }, "before the flag commit", "import aborted, the backend flag is unchanged");
    return { epoch: next, previousEpoch: meta.epoch };
  });
  options.onStep?.("import-commit");
  verifyImported(fence, { epoch, digest }, root, options);
  options.onStep?.("import-verify");
  return result(epoch, previousEpoch, false);
};

const runImport = async (root: string, options: MeshBackendOptions): Promise<MeshImportResult> => {
  root = path.resolve(root);
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory()) throw new MeshBackendRefusedError(`No mesh root at ${root}`);
  const fence = FenceDb.open(root, options, "write", true)!;
  try { return await withMeshLock(root, options, lock => importUnderLock(root, options, fence, lock)); }
  finally { fence.close(); }
};

/**
 * Import (file -> sqlite) and the roll forward. With a census it refuses on any writer whose mode
 * a cutover refuses; without one it is the offline import (the operator stopped the root).
 */
export const importMeshState = async (root: string, options: MeshBackendOptions = {}): Promise<MeshImportResult> => {
  if (options.census) {
    const modes = new Set(options.cutoverModes ?? DEFAULT_CUTOVER_MODES);
    await requireCensus(options, "import", writer => !modes.has(writer.mode), `no writer outside ${[...modes].join("/")} mode`);
  }
  return runImport(root, options);
};

/**
 * Cutover: one critical section under the mesh `.lock` (file-mode writers serialize on it):
 * census -> G0 -> import and flag at E+1 -> verify -> second census -> state.json against G0.
 * A file-mode writer (or a moved state.json) found after the flag fails the cutover and runs the
 * rollback fence back to backend=file at E+2 before `.lock` is released.
 */
export const cutoverMeshState = async (root: string, options: MeshBackendOptions = {}): Promise<MeshCutoverResult> => {
  root = path.resolve(root);
  const modes = new Set(options.cutoverModes ?? DEFAULT_CUTOVER_MODES);
  const refuses = (writer: MeshCensusWriter): boolean => !modes.has(writer.mode);
  const need = `no writer outside ${[...modes].join("/")} mode`;
  if (!options.census && !options.assumeNoWriters) throw censusMissing("cutover", need);
  if (!fs.statSync(root, { throwIfNoEntry: false })?.isDirectory()) throw new MeshBackendRefusedError(`No mesh root at ${root}`);
  return holdMeshLock(root, options, async (lock) => {
    const writers = await requireCensus(options, "cutover", refuses, need);
    const fence = FenceDb.open(root, options, "write", true)!;
    try {
      // G0, import, flag at E+1 and verification; a state.json that moved before COMMIT aborts it.
      const imported = importUnderLock(root, options, fence, lock);
      const failures: string[] = [];
      let lateWriters: MeshCensusWriter[] = [];
      try {
        lateWriters = ((await censusWriters(options)) ?? []).filter(refuses);
        if (lateWriters.length > 0) failures.push(`the second census shows ${describeWriters(lateWriters)}`);
      } catch (error) { failures.push(`the second census failed (${(error as Error).message})`); }
      let conflictFile: string | undefined;
      try {
        assertFileUnchanged(root, options, { generation: imported.generation, digest: imported.digest }, "after the flag commit", "cutover fails");
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
        return { ...imported, writers };
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
          + `(${(error as Error).message}): rerun rollback`, lateWriters, undefined, conflictFile);
      }
      throw new MeshCutoverFailedError(`Fabric mesh cutover failed: ${reason}; rolled back to backend=file at epoch ${rollback.epoch}`
        + (conflictFile ? `; the conflicting state.json is kept at ${conflictFile}` : ""), lateWriters, rollback, conflictFile);
    } finally { fence.close(); }
  });
};

// ------------------------------------------------------------------ rollback

const exportAndVerify = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock, epoch: number):
  { digest: string; generation: string } => {
  removeStaleTemps(root);
  const { meta, state } = fence.snapshot();
  if (meta.backend !== "exporting" || meta.epoch !== epoch) {
    throw alarm(options, root, `Fabric mesh export expected backend=exporting at epoch ${epoch}, found ${meta.backend} at ${meta.epoch}`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  const digest = meshSnapshotDigest(state);
  // Step 3: the normal encoder, readGeneration first (readers take it from the header).
  const generation = randomUUID();
  const exported: FencedFile = { readGeneration: generation, backendEpoch: epoch, backendDigest: digest, format: 1, revisionFormat: 2,
    entries: state.entries, versions: state.versions, tombstoneOrder: state.tombstoneOrder, highWater: state.highWater };
  const serialized = encodeMeshStateFile(exported).serialized;
  if (serialized.byteLength > maxBytesOf(options)) throw new Error(`Fabric mesh state exceeds ${maxBytesOf(options)} bytes`);
  const temporary = path.join(root, `${TEMP_PREFIX}${process.pid}-${randomUUID()}.tmp`);
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    let offset = 0;
    while (offset < serialized.byteLength) offset += fs.writeSync(descriptor, serialized, offset, serialized.byteLength - offset);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  options.onStep?.("rollback-export-temp");
  renameAtomic(temporary, path.join(root, STATE_JSON));
  fsyncDirectory(root);
  options.onStep?.("rollback-export");
  // Step 4: the old reader's decoder reads it back.
  verifyExport(root, options, fence, lock, { epoch, digest, generation });
  options.onStep?.("rollback-verify");
  return { digest, generation };
};

const verifyExport = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock,
  expected: { epoch: number; digest: string; generation: string }): void => {
  const back = readFile(root, options);
  const fileEpoch = fileEpochOf(back);
  if (fileEpoch !== expected.epoch) {
    throw alarm(options, root, `Fabric mesh export verification: state.json epoch ${fileEpoch}, expected ${expected.epoch}`, { epoch: expected.epoch, fileEpoch });
  }
  if (back.readGeneration !== expected.generation) {
    throw alarm(options, root, "Fabric mesh export verification: state.json was replaced after the export", { epoch: expected.epoch, fileEpoch });
  }
  const fileDigest = meshSnapshotDigest(back);
  const dbDigest = meshSnapshotDigest(fence.snapshot().state);
  if (back.backendDigest !== expected.digest || fileDigest !== expected.digest || dbDigest !== expected.digest) {
    throw alarm(options, root, `Fabric mesh export digest mismatch: recorded ${String(back.backendDigest)}, state.json ${fileDigest}, state.db ${dbDigest}`,
      { epoch: expected.epoch, fileEpoch });
  }
  if (storeReadDigest(root, lock, options) !== entriesDigest(normalizeSnapshot(back).entries)) {
    throw alarm(options, root, "Fabric mesh export verification: the store read path disagrees with state.json", { epoch: expected.epoch, fileEpoch });
  }
};

/** Rerun of step 4 on a switched root: a later file-mode commit (new readGeneration) only needs the epoch. */
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

/** Rollback step 2: BEGIN IMMEDIATE at FULL checks backend=sqlite at E, sets exporting at E+1. */
const rollbackFlag = (root: string, options: MeshBackendOptions, fence: FenceDb, from: number): number => {
  fence.transaction(() => {
    const current = fence.meta();
    if (current.backend !== "sqlite" || current.epoch !== from) {
      throw new MeshBackendRefusedError(`Fabric mesh rollback raced: backend=${current.backend} epoch ${current.epoch}`);
    }
    fence.setMeta("backend", "exporting");
    fence.setMeta("epoch", from + 1);
    fence.setMeta("rollback_from_epoch", from);
  });
  options.onStep?.("rollback-flag");
  return from + 1;
};

/** Rollback step 5: BEGIN IMMEDIATE at FULL checks backend=exporting at E+1, sets file. */
const rollbackSwitch = (root: string, options: MeshBackendOptions, fence: FenceDb, epoch: number,
  exported: { digest: string; generation: string }): void => {
  fence.transaction(() => {
    const current = fence.meta();
    if (current.backend !== "exporting" || current.epoch !== epoch) {
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

/**
 * Steps 2 to 5 under a `.lock` the caller holds: a failed cutover's way back to file. No census
 * (step 1): sqlite-mode writers are fenced by the flag, file-mode writers wait on the held `.lock`.
 */
const rollbackUnderLock = (root: string, options: MeshBackendOptions, fence: FenceDb, lock: MeshLock): MeshRollbackResult => {
  const meta = fence.meta();
  if (meta.backend !== "sqlite") {
    throw alarm(options, root, `Fabric mesh cutover rollback expected backend=sqlite, found ${meta.backend} at epoch ${meta.epoch}`,
      { backend: meta.backend, epoch: meta.epoch });
  }
  const epoch = rollbackFlag(root, options, fence, meta.epoch);
  const exported = exportAndVerify(root, options, fence, lock, epoch);
  rollbackSwitch(root, options, fence, epoch, exported);
  return { root, backend: "file", epoch, ...exported, steps: [2, 3, 4, 5], converged: false };
};

/** Rollback (sqlite -> file) with the R1 fence; reruns converge from the stored flag. */
export const rollbackMeshState = async (root: string, options: MeshBackendOptions = {}): Promise<MeshRollbackResult> => {
  root = path.resolve(root);
  const fence = FenceDb.open(root, options, "write");
  if (!fence) throw new MeshBackendRefusedError(`No state.db at ${root}: the root is on the file backend`);
  try {
    const steps: number[] = [];
    let meta = fence.meta();
    if (meta.backend === "file") {
      const verified = await withMeshLock(root, options, lock => reverifySwitched(root, options, fence, lock, meta));
      return { root, backend: "file", epoch: meta.epoch, ...verified, steps: [4], converged: true };
    }
    if (meta.backend === "sqlite") {
      // Step 1: no writer, no projector.
      await requireCensus(options, "rollback", () => true, "every v3 writer and the projector stopped");
      steps.push(1);
      // Step 2: the flag, at FULL.
      rollbackFlag(root, options, fence, meta.epoch);
      steps.push(2);
      meta = fence.meta();
    }
    if (meta.backend !== "exporting") {
      throw alarm(options, root, `Fabric mesh rollback: backend flag ${JSON.stringify(meta.backend)} is not sqlite, exporting or file`,
        { backend: meta.backend, epoch: meta.epoch });
    }
    const epoch = meta.epoch;
    // Steps 3 and 4 under the mesh .lock (the file store's write discipline).
    const exported = await withMeshLock(root, options, lock => exportAndVerify(root, options, fence, lock, epoch));
    steps.push(3, 4);
    // Step 5: the reader switch, at FULL.
    rollbackSwitch(root, options, fence, epoch, exported);
    steps.push(5);
    return { root, backend: "file", epoch, ...exported, steps, converged: false };
  } finally { fence.close(); }
};

/** `exporting` back to `sqlite`, keeping epoch E+1 (stores opened at E reopen). */
export const abortMeshRollback = async (root: string, options: MeshBackendOptions = {}): Promise<MeshAbortRollbackResult> => {
  root = path.resolve(root);
  const fence = FenceDb.open(root, options, "write");
  if (!fence) throw new MeshBackendRefusedError(`No state.db at ${root}`);
  try {
    const result = fence.transaction(() => {
      const meta = fence.meta();
      if (meta.backend === "sqlite" && Number(meta.values.get("aborted_rollback_epoch")) === meta.epoch) {
        return { epoch: meta.epoch, converged: true };
      }
      if (meta.backend !== "exporting") {
        throw new MeshBackendRefusedError(`abort-rollback needs backend=exporting (found ${meta.backend} at epoch ${meta.epoch})`);
      }
      fence.setMeta("backend", "sqlite");
      fence.setMeta("aborted_rollback_epoch", meta.epoch);
      return { epoch: meta.epoch, converged: false };
    });
    if (!result.converged) options.onStep?.("abort-rollback");
    return { root, backend: "sqlite", ...result };
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
  try {
    const file = readFile(root, options);
    status.fileEpoch = fileEpochOf(file);
    status.fileGeneration = typeof file.readGeneration === "string" ? file.readGeneration : undefined;
    status.fileDigest = meshSnapshotDigest(file);
  } catch (error) { status.fileError = (error as Error).message; }
  status.fenceHolds = status.fileEpoch === undefined ? status.backend !== "file"
    : status.fileEpoch <= status.epoch && (status.backend !== "file" || status.fileEpoch === status.epoch);
  const { onAlarm: _quiet, ...quiet } = options;
  try { status.reader = { source: resolveMeshStateSource(root, quiet).source }; }
  catch (error) { status.reader = { error: (error as Error).message }; }
  try {
    const writers = await censusWriters(options);
    if (writers) status.writers = writers;
  }
  catch (error) { status.censusError = (error as Error).message; }
  return status;
};
