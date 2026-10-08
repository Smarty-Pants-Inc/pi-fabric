/**
 * The file-mode writer fence (smarty-dev#6477 L4b, plan section 5, R1 late writers).
 *
 * A leaf module: state-file.ts calls `assertFileStateWritable` on its real commit path, under the
 * mesh `.lock`, right before the state.json rename, and backend-migration.ts (which imports
 * state-file.ts) re-exports it with `MeshBackendFenceError`. It therefore imports nothing from
 * state-file.ts, store.ts or state-sqlite.ts at runtime (only types), and opens SQLite at first use.
 *
 * Cost: one `fs.statSync` of `<root>/state.db` per write. A root without state.db (every legacy
 * file root) writes as before without opening SQLite; the reader rule (`meshStateSourceOf`) still
 * alarms on an epoch-stamped state.json without state.db. With state.db present the guard reads
 * meta (schema, backend, epoch) on a short-lived connection and state.json's epoch (a bounded
 * header read) and applies the reader rule: the write passes only where readers read state.json.
 * Nothing is cached across lock holds.
 */
import fs from "node:fs";
import path from "node:path";
import { storageRevision } from "../verified/storage.js";
import type { SqliteConnection, SqliteOpener } from "./state-sqlite.js";

export interface MeshBackendAlarm {
  code: "FABRIC_MESH_BACKEND_FENCE" | "FABRIC_MESH_BACKEND_LATE_WRITER";
  root: string;
  message: string;
  backend?: string;
  epoch?: number;
  fileEpoch?: number;
}

export interface MeshStateSource {
  source: "sqlite" | "file";
  backend: string;
  epoch: number;
  fileEpoch: number;
}

export interface MeshBackendFenceOptions {
  /** state.json size bound for the fallback decoder. Default 32 MiB. */
  maxStateBytes?: number;
  /** Driver adapter (tests). Default node:sqlite, loaded at first use. */
  open?: SqliteOpener;
  /** Synchronous SQLite busy handler of the guard's connection. Default 5,000 ms. */
  busyTimeoutMs?: number;
  /** Fence violations. The error is thrown as well. */
  onAlarm?: (alarm: MeshBackendAlarm) => void;
  /**
   * The epoch of a state.json whose bounded header does not carry it: the store's own decoder
   * (state-file.ts, backend-migration.ts). Default: a bounded JSON parse of `backendEpoch`.
   */
  decodeFileEpoch?: (file: string) => number;
}

/** The fence does not hold: readers and the tool fail closed, file-mode writers are refused. */
export class MeshBackendFenceError extends Error {
  readonly code = "FABRIC_MESH_BACKEND_FENCE";
  constructor(message: string) {
    super(message);
    this.name = "MeshBackendFenceError";
  }
}

const STATE_DB = "state.db";
const STATE_JSON = "state.json";
const SCHEMA_VERSION = 1;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
const DEFAULT_BUSY_MS = 5_000;
const HEADER_BYTES = 512;
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const EPOCH_HEADER = new RegExp(String.raw`^\{"readGeneration":"${UUID}"(?:,"readJournalHash":"[0-9a-f]{64}")?,"backendEpoch":(\d+)[,}]`);

/** Raises the alarm (a sink never masks the fence) and returns the error to throw. */
export const meshFenceAlarm = (options: Pick<MeshBackendFenceOptions, "onAlarm">, root: string, message: string,
  detail: Partial<MeshBackendAlarm> = {}): MeshBackendFenceError => {
  try { options.onAlarm?.({ code: "FABRIC_MESH_BACKEND_FENCE", root, message, ...detail }); } catch { /* an alarm sink never masks the fence */ }
  return new MeshBackendFenceError(message);
};

const decodeEpochDefault = (file: string, maxBytes: number): number => {
  const size = fs.statSync(file).size;
  if (size > maxBytes) throw new Error(`Fabric mesh state exceeds ${maxBytes} bytes`);
  const parsed = JSON.parse(fs.readFileSync(file, "utf8")) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("Invalid Fabric mesh state envelope");
  const epoch = (parsed as { backendEpoch?: unknown }).backendEpoch;
  return epoch === undefined ? 0 : storageRevision(epoch);
};

/**
 * The moved marker (pi-fabric#627 review round 3). Cutover's LAST step under its `.lock` hold replaces
 * state.json with this small document (after keeping a copy at `state.json.cutover-<E+1>`). Its
 * `format` is not 1 or 2, so no release decodes it as a state envelope: every deployed write path
 * (put, delete, writeBatch, tombstone compaction) reads state.json with recoverDamage=false and
 * throws "invalid state format" before it commits. A LEGACY file-mode writer (no backend guard) that
 * waited on `.lock` through the cutover therefore fails closed and writes nothing. New code treats
 * it as backend=sqlite (never as state, damage or an empty mesh); without state.db it is an alarm.
 * Rollback replaces it with the exported state.json only as its very last step, after backend=file
 * committed (review round 4); until then every reader reads SQLite and every writer fails closed.
 */
export interface MeshStateMovedMarker {
  format: "sqlite";
  movedTo: "state.db";
  backend: "sqlite";
  epoch: number;
  at: string;
}

export const isMeshStateMovedMarker = (value: unknown): value is MeshStateMovedMarker => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const marker = value as Record<string, unknown>;
  return marker.format === "sqlite" && marker.movedTo === STATE_DB && marker.backend === "sqlite"
    && typeof marker.epoch === "number" && Number.isSafeInteger(marker.epoch) && marker.epoch >= 1;
};

/** The marker's exact bytes (field order fixed; no release reads `format: "sqlite"` as state). */
export const encodeMeshStateMovedMarker = (epoch: number, at = new Date().toISOString()): string =>
  JSON.stringify({ format: "sqlite", movedTo: STATE_DB, backend: "sqlite", epoch: storageRevision(epoch), at } satisfies MeshStateMovedMarker);

/** A state.json that is the moved marker: fail closed, never an empty or damaged state. */
export const meshStateMovedError = (file: string, marker: MeshStateMovedMarker): MeshBackendFenceError =>
  new MeshBackendFenceError(`Fabric mesh state.json was moved to state.db (backend=sqlite at epoch ${marker.epoch}, ${marker.at}): `
    + `${file} is not state; this release must run in sqlite mode`);

const parseMovedMarker = (text: string): MeshStateMovedMarker | undefined => {
  try {
    const parsed: unknown = JSON.parse(text);
    return isMeshStateMovedMarker(parsed) ? parsed : undefined;
  } catch { return undefined; }
};

/** state.json's bounded head (HEADER_BYTES) and whether that is the whole file; undefined when absent. */
const readStateHead = (file: string): { head: string; complete: boolean } | undefined => {
  let descriptor: number;
  try { descriptor = fs.openSync(file, "r"); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  try {
    const buffer = Buffer.alloc(HEADER_BYTES);
    const read = fs.readSync(descriptor, buffer, 0, HEADER_BYTES, 0);
    return { head: buffer.subarray(0, read).toString("utf8"), complete: read < HEADER_BYTES };
  } finally { fs.closeSync(descriptor); }
};

/** The moved marker at `<root>/state.json` (a bounded read), or undefined (absent, state, damage). */
export const readMeshStateMovedMarker = (root: string): MeshStateMovedMarker | undefined => {
  const head = readStateHead(path.join(path.resolve(root), STATE_JSON));
  return head?.complete ? parseMovedMarker(head.head) : undefined;
};

/** The epoch recorded in state.json (the marker's epoch for a moved marker) and whether it is that marker. */
const readStateFileMark = (root: string, options: Pick<MeshBackendFenceOptions, "maxStateBytes" | "decodeFileEpoch">):
  { epoch: number; moved: boolean } => {
  const file = path.join(path.resolve(root), STATE_JSON);
  const read = readStateHead(file);
  if (read === undefined) return { epoch: 0, moved: false };
  const match = EPOCH_HEADER.exec(read.head);
  if (match) return { epoch: storageRevision(Number(match[1])), moved: false };
  const marker = read.complete ? parseMovedMarker(read.head) : undefined;
  if (marker) return { epoch: marker.epoch, moved: true };
  return { epoch: options.decodeFileEpoch
    ? options.decodeFileEpoch(file)
    : decodeEpochDefault(file, Math.max(1, Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES))), moved: false };
};

/** The epoch recorded in state.json: a bounded header read (or the moved marker's), else the (injected) full decoder. */
export const readStateFileEpoch = (root: string, options: Pick<MeshBackendFenceOptions, "maxStateBytes" | "decodeFileEpoch"> = {}): number =>
  readStateFileMark(root, options).epoch;

/**
 * The reader rule (plan section 5) for a known database flag (`none`: state.db absent or
 * uninitialised): use state.json only when backend=file and it is a real state file whose epoch
 * equals meta.epoch, or backend=importing (an import not yet authoritative, pi-fabric#627 review
 * round 5) and a real state file below meta.epoch; with sqlite or exporting read SQLite; anything else fails closed with
 * MeshBackendFenceError and an alarm. While state.json is the moved marker SQLite is read whatever
 * the (valid) flag (pi-fabric#627 review round 4): it is the last committed state, because the flag
 * fences sqlite-mode writers once it is exporting or file, and rollback removes the marker only as
 * its very last step, after backend=file committed.
 */
export const meshStateSourceOf = (root: string, backend: string, epoch: number, options: MeshBackendFenceOptions = {}): MeshStateSource => {
  let fileEpoch: number;
  let moved = false;
  try { ({ epoch: fileEpoch, moved } = readStateFileMark(root, options)); }
  catch (error) {
    // A damaged state.json is not authority under sqlite/exporting; the legacy store tolerates it.
    if (backend === "sqlite" || backend === "exporting") return { source: "sqlite", backend, epoch, fileEpoch: Number.NaN };
    if (backend === "none") return { source: "file", backend, epoch, fileEpoch: Number.NaN };
    throw meshFenceAlarm(options, root, `Fabric mesh state.json is unreadable under backend=${backend} epoch ${epoch}: ${(error as Error).message}`, { backend, epoch });
  }
  const detail = { backend, epoch, fileEpoch };
  // The moved marker means "read state.db": never state, damage or empty, and an alarm without state.db.
  if (moved && backend === "none") {
    throw meshFenceAlarm(options, root, `Fabric mesh state.json is the moved marker (state.db at epoch ${fileEpoch}) but state.db is missing or uninitialised`, detail);
  }
  if (!Number.isSafeInteger(epoch) || epoch < 0) throw meshFenceAlarm(options, root, `Fabric mesh state.db has an invalid epoch (${String(epoch)})`, detail);
  if (fileEpoch > epoch) {
    throw meshFenceAlarm(options, root, backend === "none"
      ? `Fabric mesh state.json carries epoch ${fileEpoch} but state.db is missing`
      : `Fabric mesh state.json epoch ${fileEpoch} exceeds the database epoch ${epoch} (backend=${backend})`, detail);
  }
  if (backend === "none") return { source: "file", ...detail };
  if (backend === "sqlite" || backend === "exporting" || (moved && (backend === "file" || backend === "importing"))) return { source: "sqlite", ...detail };
  // Importing without the marker: SQLite is being filled and is not authoritative; state.json still is.
  if (backend === "importing") {
    if (fileEpoch < epoch) return { source: "file", ...detail };
    throw meshFenceAlarm(options, root, `Fabric mesh backend=importing at epoch ${epoch} but state.json carries epoch ${fileEpoch}`, detail);
  }
  if (backend === "file") {
    if (fileEpoch === epoch) return { source: "file", ...detail };
    throw meshFenceAlarm(options, root, `Fabric mesh backend=file at epoch ${epoch} but state.json carries epoch ${fileEpoch}`, detail);
  }
  throw meshFenceAlarm(options, root, `Fabric mesh state.db backend flag ${JSON.stringify(backend)} is not sqlite, importing, exporting or file`, detail);
};

/** node:sqlite at first use (AGENTS.md startup budget); the subset the guard needs. */
const openDefault = (file: string): Pick<SqliteConnection, "exec" | "prepare" | "close"> => {
  const sqlite = process.getBuiltinModule?.("node:sqlite") as typeof import("node:sqlite") | undefined;
  if (!sqlite?.DatabaseSync) throw new Error("node:sqlite is unavailable in this runtime (Node >= 22.13 required)");
  const database = new sqlite.DatabaseSync(file, { timeout: 0 });
  return {
    exec: (sql) => { database.exec(sql); },
    prepare: (sql) => {
      const statement = database.prepare(sql);
      return { run: (...params) => statement.run(...params), get: (...params) => statement.get(...params), all: (...params) => statement.all(...params) };
    },
    close: () => { database.close(); },
  };
};

/** meta.backend and meta.epoch of an existing state.db (undefined when uninitialised), on a short-lived connection. */
const readFenceMeta = (root: string, options: MeshBackendFenceOptions): { backend: string; epoch: number } | undefined => {
  const db = (options.open ?? openDefault)(path.join(root, STATE_DB));
  try {
    db.exec(`PRAGMA busy_timeout = ${Math.max(0, Math.floor(options.busyTimeoutMs ?? DEFAULT_BUSY_MS))}`);
    db.exec("PRAGMA trusted_schema = OFF");
    if (db.prepare("SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = 'meta'").get() === undefined) return undefined;
    const values = new Map<string, unknown>();
    for (const row of db.prepare("SELECT name, value FROM meta WHERE name IN ('schema', 'backend', 'epoch')").all()) values.set(String(row.name), row.value);
    if (!values.has("schema")) return undefined;
    const schema = Number(values.get("schema"));
    if (schema !== SCHEMA_VERSION) throw new Error(`Fabric mesh SQLite state schema ${schema} is not supported (expected ${SCHEMA_VERSION})`);
    return { backend: String(values.get("backend") ?? "missing"), epoch: Number(values.get("epoch") ?? Number.NaN) };
  } finally { db.close(); }
};

/**
 * The file-mode writer fence (R1, late writers): every process that writes state.json in file mode
 * calls this under the mesh `.lock`, right before its write (StateFile does so on every commit). It
 * passes on a root without state.db (one stat, no SQLite) or where the reader rule reads state.json
 * (backend=file at the file's epoch), and otherwise fails closed with MeshBackendFenceError: after a
 * cutover committed backend=sqlite, a writer blocked on `.lock` (or started later) must not commit
 * to a retired state.json.
 */
export const assertFileStateWritable = (root: string, options: MeshBackendFenceOptions = {}): void => {
  root = path.resolve(root);
  let source: MeshStateSource;
  try {
    if (fs.statSync(path.join(root, STATE_DB), { throwIfNoEntry: false }) === undefined) return;
    const meta = readFenceMeta(root, options);
    source = meshStateSourceOf(root, meta?.backend ?? "none", meta?.epoch ?? 0, options);
  } catch (error) {
    if (error instanceof MeshBackendFenceError) throw error;
    throw meshFenceAlarm(options, root, `Fabric mesh file-mode write refused: the backend flag is unreadable (${(error as Error).message})`);
  }
  if (source.source !== "file") {
    throw meshFenceAlarm(options, root, `Fabric mesh file-mode write refused: backend=${source.backend} at epoch ${source.epoch}; `
      + "state.json is not the authority (this release must run in sqlite mode)",
    { backend: source.backend, epoch: source.epoch, fileEpoch: source.fileEpoch });
  }
};
