/**
 * The mesh state projector (smarty-dev#6477, lane L3).
 *
 * In "shadow" mode the file store (state.json behind the mesh .lock) stays the authority. ONE
 * projector per mesh root keeps <databaseRoot>/state.db in step with it, revision for revision, so
 * readers and the divergence check (gate G4: 0 divergences in 24 h) can use SQLite before cutover.
 * After cutover ("sqlite" mode) the projector only maintains the database: it is the elected WAL
 * checkpointer (R10) and reports the WAL size. It keeps NO state.json projection after cutover:
 * section 5 of docs/mesh-lock-plan.md exports state.json once, inside the rollback fence (L4), and
 * a reader may use state.json only when meta.backend=file and the file epoch equals meta.epoch
 * (R1). A continuously refreshed state.json would serve old binaries that the census (R5) already
 * excludes, and would invite their writes into a file that is no longer authoritative.
 *
 * Election: a lease row (meta "projector.lease": owner, expiry) written in BEGIN IMMEDIATE; the
 * holder renews it at half its life, a standby takes it only after it expires or is released, and
 * every apply transaction re-checks it (fencing): a projector that lost the lease cannot commit.
 *
 * Following the file store never takes the mesh .lock (state.json is replaced by rename, so one
 * descriptor reads one consistent snapshot):
 * 1. A stat of state.json. The physical identity of the last applied generation: caught up.
 * 2. Otherwise the canonical header (readGeneration, readJournalHash) and the read journal
 *    (state.read-journal.jsonl) from the stored cursor. Records that link to the projected
 *    generation (previous generation, previous chain hash, previous physical identity) are followed
 *    up to the header's generation; the terminal record's chain hash must equal the hash committed
 *    in state.json's header, its identity must equal the file's, and its payload hash is verified
 *    (verifyStateJournalEndpoint). Each generation is then applied in its OWN transaction, together
 *    with the projection progress (generation, chain hash, identity, journal cursor, commit), so a
 *    crash at any point resumes at the last committed generation.
 * 3. Anything else (no journal, a rotated journal, a legacy writer that publishes none, a rewrite
 *    that kept the generation, a broken chain) is a gap: a full resync from one state.json snapshot,
 *    diffed against SQLite in one transaction.
 *
 * Idempotent by revision: a row is written only when its (version, value, updatedAt, updatedBy) or
 * its tombstone version differs; a re-applied generation commits nothing and leaves commit_no.
 * Each changing generation is ONE SQLite commit: commit_no + 1, its keys in the changes feed, the
 * high-water clock (never lowered), state_bytes and the tombstone order as state-sqlite.ts keeps
 * them, so SqliteStateStore readers (get/list/stateStamp/changesSince) see ordinary commits.
 *
 * Fences (R5): the projector stops with an alarm on a foreign write (commit_no moved without it:
 * another writer committed, for example a cut-over sqlite backend), on a database it did not
 * create (rows but no projection progress) and on a retired or exporting database. A divergence
 * (SQLite edited behind its back, no commit) is found by verify(), recorded as StateDivergence
 * rows for L8/commswatch and, by default, repaired with a full resync.
 *
 * Lag (revisions, generations, ms) and divergences are in status() and, from the active projector,
 * in <root>/state-projector.status.json (atomic rewrite, at most once per statusMs).
 */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import type { MeshIdentity } from "./event-log.js";
import { journalCursorOf, verifyStateJournalEndpoint, type JournalCursor } from "./read-journal.js";
import type { StateDivergence } from "./state-backend.js";
import type { MeshStateEntry } from "./state-file.js";
import { openNodeSqlite, SqliteStateStore, type SqliteConnection, type SqliteOpener, type SqliteRow } from "./state-sqlite.js";

/** Written by the active projector in the mesh root (never one of the state*.db* files). */
export const STATE_PROJECTOR_STATUS_FILE = "state-projector.status.json";
const LEASE_META = "projector.lease";
const PROGRESS_META = "projector.progress";
const STATE_FILE = "state.json";
const JOURNAL_FILE = "state.read-journal.jsonl";
const ABSENT = "absent";
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const STATE_HEADER = new RegExp(`^\\{"readGeneration":"(${UUID})"(?:,"readJournalHash":"([0-9a-f]{64})")?`);
const GENERATION = new RegExp(`^${UUID}$`);
const HEADER_BYTES = 160;
// The writer rotates the journal at 2 MiB; a larger file is not one it wrote.
const MAX_JOURNAL_BYTES = 4 * 1024 * 1024;
const MAX_RECENT = 32;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;

export type StateProjectorMode = "shadow" | "sqlite";
export type StateProjectorRole = "starting" | "active" | "standby" | "stopped";
export type StateProjectorHaltReason = "foreign-write" | "foreign-database" | "retired";
/** initial: first projection; gap: no verified journal chain; rewrite: same generation, new file. */
export type StateProjectorResyncReason = "initial" | "gap" | "rewrite" | "divergence" | "manual";

/** A divergence with the field that differs and both revisions (undefined: absent on that side). */
export interface ProjectorDivergence extends StateDivergence {
  field: "presence" | "version" | "value" | "updatedAt" | "updatedBy" | "tombstone";
  fileVersion: number | undefined;
  sqliteVersion: number | undefined;
}

export interface StateProjectorLag {
  /** File high-water revision minus the projected high-water revision, when last observed. */
  revisions: number;
  /** Committed file generations not yet applied, when last observed. */
  generations: number;
  /** Age of the oldest unapplied file commit, when last observed; 0 when caught up. */
  ms: number;
}

export interface StateProjectorStatus {
  root: string;
  database: string;
  mode: StateProjectorMode;
  owner: string;
  role: StateProjectorRole;
  haltReason: StateProjectorHaltReason | undefined;
  leaseHolder: string | undefined;
  generation: string | undefined;
  commit: number;
  appliedGenerations: number;
  fullResyncs: number;
  lastResync: { reason: StateProjectorResyncReason; at: number; entries: number; tombstones: number } | undefined;
  lag: StateProjectorLag;
  maxLagMs: number;
  lastApplyLagMs: number | undefined;
  divergenceChecks: number;
  divergences: number;
  divergentKeys: string[];
  lastDivergences: ProjectorDivergence[];
  foreignWrites: number;
  errors: number;
  lastError: string | undefined;
  checkpoints: number;
  walBytes: number;
  walAlarm: boolean;
  updatedAt: number;
}

export type StateProjectorEvent =
  | { type: "role"; at: number; role: StateProjectorRole; leaseHolder: string | undefined }
  | { type: "resync"; at: number; reason: StateProjectorResyncReason; entries: number; tombstones: number }
  | { type: "divergence"; at: number; divergences: ProjectorDivergence[] }
  | { type: "alarm"; at: number; alarm: StateProjectorHaltReason | "wal-size"; detail: string }
  | { type: "error"; at: number; message: string };

export interface StateProjectorOptions {
  /** The mesh root (state.json, the read journal, the status file). */
  root: string;
  /** Where state.db lives. Default: the mesh root (<root>/state.db). */
  databaseRoot?: string;
  /** "shadow" (default): project state.json. "sqlite": after cutover, maintain only. */
  mode?: StateProjectorMode;
  /** Lease owner id. Default host:pid:random. */
  owner?: string;
  /** Loop period of run(). Default 250 ms. */
  pollMs?: number;
  /** Lease lifetime; renewed at half. Default 10 s. */
  leaseMs?: number;
  /** Period of the automatic divergence check while caught up; 0 disables. Default 60 s. */
  verifyMs?: number;
  /** Repair a found divergence with a full resync. Default true. */
  repairDivergence?: boolean;
  /** Async budget for one BEGIN IMMEDIATE (busy_timeout stays at 2 ms). Default 5 s. */
  busyBudgetMs?: number;
  /** state.json read cap. Default 32 MiB. */
  maxStateBytes?: number;
  /** Rows kept in the changes feed. Default 4,096 (as state-sqlite.ts). */
  changesRetained?: number;
  /** Checkpoint period of the active projector. Default 1 s. */
  checkpointMs?: number;
  /** WAL size above which a growing WAL is TRUNCATEd (R10). Default 64 MiB. */
  checkpointBytes?: number;
  /** WAL size alarm. Default 64 MiB. */
  walAlarmBytes?: number;
  /** Status file period; 0 disables the file. Default 1 s. */
  statusMs?: number;
  onEvent?: (event: StateProjectorEvent) => void;
  /** Driver adapter (tests, bun:sqlite). Default node:sqlite. */
  open?: SqliteOpener;
  /** Test seam: runs inside each apply transaction, right before COMMIT. */
  beforeCommit?: (info: { kind: "record" | "snapshot"; generation: string | undefined; index: number }) => void;
}

interface Progress {
  generation: string | null;
  hash: string | null;
  identity: string | null;
  cursor: JournalCursor | null;
  commit: number;
  appliedAt: number;
}

interface Lease { owner: string; expiresAt: number; pid?: number; host?: string }

interface JournalRecord {
  generation: string;
  previous: unknown;
  previousHash: unknown;
  previousIdentity: unknown;
  hash: string;
  identity: string | null;
  payloadHash: unknown;
  entries: Record<string, unknown>;
  versions: Record<string, unknown>;
  tombstoneOrder: unknown;
  tombstonePatch: unknown;
  highWater: number;
  /** Byte offset right after this record's line. */
  end: number;
}

interface FileHead { identity: string; generation: string | null; hash: string | null; mtimeMs: number }

interface FileSnapshot extends FileHead {
  cursor: JournalCursor | null;
  entries: Map<string, MeshStateEntry>;
  tombstones: Array<[string, number]>;
  highWater: number;
}

interface Row { value: string; version: number; updatedAt: number; updatedBy: string; bytes: number }

/** A fence the projector must not cross. "lease-lost" demotes to standby; the rest halt it. */
class ProjectorFence extends Error {
  constructor(readonly reason: StateProjectorHaltReason | "lease-lost", detail: string) {
    super(detail);
    this.name = "ProjectorFence";
  }
}

/** The journal cannot carry the projection forward: fall back to a full resync. */
class ProjectorGap extends Error {
  constructor(detail: string) {
    super(detail);
    this.name = "ProjectorGap";
  }
}

const digest = (text: string): string => createHash("sha256").update(text).digest("hex");
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isBusy = (error: unknown): boolean => {
  const code = (error as { errcode?: unknown } | null)?.errcode;
  return typeof code === "number" && ((code & 0xff) === 5 || (code & 0xff) === 6);
};
const delay = (ms: number): Promise<void> => new Promise(resolve => setTimeout(resolve, Math.max(0, ms)));
const revision = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
const physicalIdentity = (stat: fs.BigIntStats): string =>
  `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}:${stat.ctimeNs}:${stat.birthtimeNs}`;
const utf8 = (text: string): number => Buffer.byteLength(text, "utf8");
// The accounting of state-sqlite.ts (entryBytes), so its 32 MiB cap stays exact after cutover.
const entryBytes = (key: string, value: string, updatedBy: string): number => utf8(value) + utf8(updatedBy) + 3 * utf8(key) + 96;
const errorText = (error: unknown): string => error instanceof Error ? error.message : String(error);

const toEntry = (key: string, value: unknown): MeshStateEntry | undefined => {
  if (!isRecord(value) || value.key !== key || revision(value.version) === undefined || value.version === 0 ||
    typeof value.updatedAt !== "number" || !isRecord(value.updatedBy)) return undefined;
  return value as unknown as MeshStateEntry;
};

const encode = (entry: MeshStateEntry): Row => {
  const value = JSON.stringify(entry.value) ?? "null";
  const updatedBy = JSON.stringify(entry.updatedBy);
  return { value, version: entry.version, updatedAt: entry.updatedAt, updatedBy, bytes: entryBytes(entry.key, value, updatedBy) };
};

const sameRow = (row: SqliteRow | undefined, next: Row): boolean => row !== undefined &&
  String(row.value) === next.value && Number(row.version) === next.version &&
  Number(row.updated_at) === next.updatedAt && String(row.updated_by) === next.updatedBy;

/** The tombstone delta of read-journal.ts: keep the base order minus remove, then append. */
const applyTombstonePatch = (base: readonly string[], patch: unknown): string[] | undefined => {
  if (!isRecord(patch) || !Array.isArray(patch.remove) || !Array.isArray(patch.append) ||
    [...patch.remove, ...patch.append].some(key => typeof key !== "string")) return undefined;
  const remove = new Set(patch.remove as string[]);
  if (remove.size !== patch.remove.length || (patch.remove as string[]).some(key => !base.includes(key))) return undefined;
  const next = [...base.filter(key => !remove.has(key)), ...(patch.append as string[])];
  return new Set(next).size === next.length ? next : undefined;
};

const parseRecord = (line: string, end: number): JournalRecord | undefined => {
  try {
    const row: unknown = JSON.parse(line);
    if (!isRecord(row) || !isRecord(row.delta) || row.checksum !== digest(JSON.stringify(row.delta))) return undefined;
    const delta = row.delta;
    if (typeof delta.generation !== "string" || !GENERATION.test(delta.generation) || !isRecord(delta.entries) ||
      !isRecord(delta.versions) || !isRecord(delta.envelope)) return undefined;
    // The chain hash covers the record without its own endpoint (identity, stamp), as replayStateJournal.
    const { identity, stamp: _stamp, ...body } = delta;
    return {
      generation: delta.generation, previous: delta.previous, previousHash: delta.previousHash,
      previousIdentity: delta.previousIdentity, hash: digest(JSON.stringify(body)),
      identity: typeof identity === "string" ? identity : null, payloadHash: delta.canonicalPayloadHash,
      entries: delta.entries, versions: delta.versions,
      tombstoneOrder: Object.hasOwn(delta, "tombstoneOrder") ? delta.tombstoneOrder : undefined,
      tombstonePatch: Object.hasOwn(delta, "tombstonePatch") ? delta.tombstonePatch : undefined,
      highWater: revision(delta.envelope.highWater) ?? 0, end,
    };
  } catch { return undefined; }
};

/** Complete journal lines from the cursor (from 0 after a rotation); undefined when unreadable. */
const readJournal = (root: string, cursor: JournalCursor | null): JournalRecord[] | undefined => {
  let fd: number | undefined;
  try {
    try { fd = fs.openSync(path.join(root, JOURNAL_FILE), "r"); }
    catch (error) { return (error as NodeJS.ErrnoException).code === "ENOENT" ? [] : undefined; }
    const stat = fs.fstatSync(fd);
    if (stat.size > MAX_JOURNAL_BYTES) return undefined;
    const inode = `${stat.dev}:${stat.ino}`;
    const start = cursor && cursor.inode === inode && cursor.offset <= stat.size ? cursor.offset : 0;
    const buffer = Buffer.allocUnsafe(stat.size - start);
    let read = 0;
    while (read < buffer.length) {
      const count = fs.readSync(fd, buffer, read, buffer.length - read, start + read);
      if (count === 0) break;
      read += count;
    }
    const records: JournalRecord[] = [];
    for (let from = 0; from < read;) {
      const newline = buffer.indexOf(0x0a, from);
      if (newline < 0 || newline >= read) break; // a record still being appended
      const record = parseRecord(buffer.toString("utf8", from, newline), start + newline + 1);
      if (record) records.push(record);
      from = newline + 1;
    }
    return records;
  } catch { return undefined; }
  finally { if (fd !== undefined) try { fs.closeSync(fd); } catch { /* best effort */ } }
};

/** The records that carry the projection from base to target, or undefined (a gap). */
const chainOf = (records: readonly JournalRecord[], base: Progress, target: FileHead): JournalRecord[] | undefined => {
  const chain: JournalRecord[] = [];
  let generation: unknown = base.generation, hash: unknown = base.hash, identity: unknown = base.identity;
  for (const record of records) {
    if (record.previous !== generation || record.previousHash !== (hash ?? null) ||
      (identity !== null && record.previousIdentity !== identity)) continue;
    chain.push(record);
    ({ generation, hash, identity } = record);
    if (generation === target.generation) break;
  }
  return chain.length > 0 && generation === target.generation && hash === target.hash ? chain : undefined;
};

/** The commit time of a generation: its newest written entry, else the file's mtime. */
const commitTime = (record: JournalRecord, fallback: number): number => {
  let at = 0;
  for (const entry of Object.values(record.entries)) {
    if (isRecord(entry) && typeof entry.updatedAt === "number") at = Math.max(at, entry.updatedAt);
  }
  return at > 0 ? at : fallback;
};

const readHead = (root: string): FileHead => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(path.join(root, STATE_FILE), "r");
    const stat = fs.fstatSync(fd, { bigint: true });
    const buffer = Buffer.alloc(HEADER_BYTES);
    const read = fs.readSync(fd, buffer, 0, HEADER_BYTES, 0);
    const match = STATE_HEADER.exec(buffer.toString("latin1", 0, read));
    return { identity: physicalIdentity(stat), generation: match?.[1] ?? null, hash: match?.[2] ?? null, mtimeMs: Number(stat.mtimeMs) };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return { identity: ABSENT, generation: null, hash: null, mtimeMs: Date.now() };
    throw error;
  } finally { if (fd !== undefined) try { fs.closeSync(fd); } catch { /* best effort */ } }
};

const stateIdentity = (root: string): string => {
  try { return physicalIdentity(fs.statSync(path.join(root, STATE_FILE), { bigint: true })); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return ABSENT;
    throw error;
  }
};

/** One consistent state.json snapshot (one descriptor), normalised as the file store compacts it. */
const readSnapshot = (root: string, maxBytes: number): FileSnapshot => {
  // Captured BEFORE the read: every record of a later generation lies at or past it.
  const cursor = journalCursorOf(root) ?? null;
  const empty = (identity: string, mtimeMs: number): FileSnapshot =>
    ({ identity, generation: null, hash: null, mtimeMs, cursor, entries: new Map(), tombstones: [], highWater: 0 });
  let fd: number | undefined;
  let text: string;
  let head: { identity: string; mtimeMs: number };
  try {
    try { fd = fs.openSync(path.join(root, STATE_FILE), "r"); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return empty(ABSENT, Date.now());
      throw error;
    }
    const stat = fs.fstatSync(fd, { bigint: true });
    if (stat.size > BigInt(maxBytes)) throw new Error(`Fabric mesh state.json exceeds ${maxBytes} bytes`);
    const buffer = Buffer.allocUnsafe(Number(stat.size));
    let read = 0;
    while (read < buffer.length) {
      const count = fs.readSync(fd, buffer, read, buffer.length - read, read);
      if (count === 0) break;
      read += count;
    }
    if (read !== buffer.length) throw new Error("Fabric mesh state.json changed size during the read");
    text = buffer.toString("utf8");
    head = { identity: physicalIdentity(stat), mtimeMs: Number(stat.mtimeMs) };
  } finally { if (fd !== undefined) try { fs.closeSync(fd); } catch { /* best effort */ } }
  if (!text.trim()) return empty(head.identity, head.mtimeMs);
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed) || !isRecord(parsed.entries)) throw new Error("Fabric mesh state.json is not a state envelope");
  const entries = new Map<string, MeshStateEntry>();
  let highWater = revision(parsed.highWater) ?? 0;
  for (const [key, value] of Object.entries(parsed.entries)) {
    const entry = toEntry(key, value);
    if (!entry) throw new Error(`Fabric mesh state.json has an invalid entry ${JSON.stringify(key)}`);
    entries.set(key, entry);
    highWater = Math.max(highWater, entry.version);
  }
  const versions = isRecord(parsed.versions) ? parsed.versions : {};
  const tombstones: Array<[string, number]> = [];
  const seen = new Set<string>();
  const order: unknown[] = Array.isArray(parsed.tombstoneOrder) ? parsed.tombstoneOrder : [];
  for (const key of [...order, ...Object.keys(versions)]) {
    if (typeof key !== "string" || seen.has(key) || entries.has(key) || !Object.hasOwn(versions, key)) continue;
    const version = revision(versions[key]);
    if (version === undefined) throw new Error(`Fabric mesh state.json has an invalid revision for ${JSON.stringify(key)}`);
    seen.add(key);
    tombstones.push([key, version]);
    highWater = Math.max(highWater, version);
  }
  for (const value of Object.values(versions)) highWater = Math.max(highWater, revision(value) ?? 0);
  const generation = typeof parsed.readGeneration === "string" && GENERATION.test(parsed.readGeneration) ? parsed.readGeneration : null;
  const hash = typeof parsed.readJournalHash === "string" && /^[0-9a-f]{64}$/.test(parsed.readJournalHash) ? parsed.readJournalHash : null;
  return { ...head, generation, hash, cursor, entries, tombstones, highWater };
};

const statements = (db: SqliteConnection) => ({
  metaGet: db.prepare("SELECT value FROM meta WHERE name = ?"),
  metaSome: db.prepare("SELECT name, value FROM meta WHERE name IN ('backend', 'commit_no', 'projector.progress', 'projector.lease')"),
  metaSet: db.prepare("UPDATE meta SET value = ? WHERE name = ?"),
  metaPut: db.prepare("INSERT INTO meta(name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = excluded.value"),
  metaDelete: db.prepare("DELETE FROM meta WHERE name = ?"),
  kvGet: db.prepare("SELECT value, version, updated_at, updated_by, bytes FROM kv WHERE key = ?"),
  kvAll: db.prepare("SELECT key, value, version, updated_at, updated_by, bytes FROM kv"),
  kvUpsert: db.prepare(`INSERT INTO kv(key, value, version, updated_at, updated_by, bytes) VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, version = excluded.version, updated_at = excluded.updated_at,
    updated_by = excluded.updated_by, bytes = excluded.bytes`),
  kvDelete: db.prepare("DELETE FROM kv WHERE key = ?"),
  kvCount: db.prepare("SELECT count(*) AS n FROM kv"),
  tombAll: db.prepare("SELECT key, version FROM tombstones ORDER BY ord"),
  tombGet: db.prepare("SELECT version FROM tombstones WHERE key = ?"),
  tombCount: db.prepare("SELECT count(*) AS n FROM tombstones"),
  tombInsert: db.prepare("INSERT INTO tombstones(key, version, ord) VALUES (?, ?, ?)"),
  tombVersion: db.prepare("UPDATE tombstones SET version = ? WHERE key = ?"),
  tombDelete: db.prepare("DELETE FROM tombstones WHERE key = ?"),
  tombClear: db.prepare("DELETE FROM tombstones"),
  changeInsert: db.prepare("INSERT INTO changes(commit_no, key, version, deleted) VALUES (?, ?, ?, ?)"),
  // Whole commits only, as state-sqlite.ts: a reader never sees half of one.
  changeTrim: db.prepare("DELETE FROM changes WHERE commit_no <= (SELECT commit_no FROM changes WHERE seq <= ? ORDER BY seq DESC LIMIT 1)"),
  maxVersion: db.prepare("SELECT max(m) AS m FROM (SELECT max(version) AS m FROM kv UNION ALL SELECT max(version) AS m FROM tombstones)"),
  kvBytes: db.prepare("SELECT coalesce(sum(bytes), 0) AS n FROM kv"),
  tombBytes: db.prepare("SELECT coalesce(sum(2 * length(CAST(key AS BLOB)) + 32), 0) AS n FROM tombstones"),
});

const parseJson = <T>(value: unknown): T | undefined => {
  if (typeof value !== "string") return undefined;
  try { return JSON.parse(value) as T; } catch { return undefined; }
};

const EMPTY_PROGRESS: Progress = { generation: null, hash: null, identity: null, cursor: null, commit: 0, appliedAt: 0 };

/**
 * One projector for one mesh root. open() connects; run() starts the loop; tick() is one pass;
 * verify() is the divergence check; stop() ends it cleanly (lease released, connections closed).
 */
export class StateProjector {
  readonly root: string;
  readonly database: string;
  readonly mode: StateProjectorMode;
  readonly owner: string;
  readonly #options: StateProjectorOptions;
  readonly #store: SqliteStateStore;
  readonly #db: SqliteConnection;
  readonly #sql: ReturnType<typeof statements>;
  readonly #pollMs: number;
  readonly #leaseMs: number;
  readonly #verifyMs: number;
  readonly #busyBudgetMs: number;
  readonly #maxStateBytes: number;
  readonly #changesRetained: number;
  readonly #checkpointMs: number;
  readonly #walAlarmBytes: number;
  readonly #statusMs: number;
  #role: StateProjectorRole = "starting";
  #haltReason: StateProjectorHaltReason | undefined;
  #leaseHolder: string | undefined;
  #progress: Progress | undefined;
  #queue: Promise<unknown> = Promise.resolve();
  #timer: ReturnType<typeof setTimeout> | undefined;
  #running = false;
  #lastVerifyAt = Date.now();
  #lastCheckpointAt = 0;
  #lastStatusAt = 0;
  #lag: StateProjectorLag = { revisions: 0, generations: 0, ms: 0 };
  #stats = {
    appliedGenerations: 0, fullResyncs: 0, divergenceChecks: 0, divergences: 0, foreignWrites: 0, errors: 0,
    checkpoints: 0, walBytes: 0, maxLagMs: 0,
  };
  #lastResync: StateProjectorStatus["lastResync"];
  #lastApplyLagMs: number | undefined;
  #lastError: string | undefined;
  #divergentKeys: string[] = [];
  #lastDivergences: ProjectorDivergence[] = [];
  #walAlarm = false;

  private constructor(options: StateProjectorOptions, store: SqliteStateStore, db: SqliteConnection) {
    this.#options = options;
    this.root = path.resolve(options.root);
    this.database = store.file;
    this.mode = options.mode ?? "shadow";
    this.owner = options.owner ?? `${os.hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.#store = store;
    this.#db = db;
    this.#sql = statements(db);
    this.#pollMs = Math.max(10, options.pollMs ?? 250);
    this.#leaseMs = Math.max(100, options.leaseMs ?? 10_000);
    this.#verifyMs = Math.max(0, options.verifyMs ?? 60_000);
    this.#busyBudgetMs = Math.max(0, options.busyBudgetMs ?? 5_000);
    this.#maxStateBytes = Math.max(1, options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES);
    this.#changesRetained = Math.max(1, Math.floor(options.changesRetained ?? 4_096));
    this.#checkpointMs = Math.max(10, options.checkpointMs ?? 1_000);
    this.#walAlarmBytes = Math.max(0, options.walAlarmBytes ?? 64 * 1024 * 1024);
    this.#statusMs = Math.max(0, options.statusMs ?? 1_000);
  }

  /** Opens (and, when absent, creates) the database and a projector connection. Does not start the loop. */
  static async open(options: StateProjectorOptions): Promise<StateProjector> {
    const databaseRoot = path.resolve(options.databaseRoot ?? options.root);
    // The L1 store creates the schema and WAL mode and refuses a non-local filesystem (R16) or a
    // retired database; the projector checkpoints through it (R10) and never writes through it.
    const store = await SqliteStateStore.open(databaseRoot, 64 * 1024, 1_000, {
      checkpoint: "client", checkpointBytes: options.checkpointBytes ?? 64 * 1024 * 1024,
      lockTimeoutMs: options.busyBudgetMs ?? 5_000, ...(options.open ? { open: options.open } : {}),
    });
    let db: SqliteConnection | undefined;
    try {
      db = (options.open ?? openNodeSqlite)(store.file);
      db.exec("PRAGMA busy_timeout = 2");
      db.exec("PRAGMA synchronous = NORMAL");
      db.exec("PRAGMA wal_autocheckpoint = 0");
      db.exec("PRAGMA trusted_schema = OFF");
      return new StateProjector(options, store, db);
    } catch (error) {
      try { db?.close(); } catch { /* best effort */ }
      store.close();
      throw error;
    }
  }

  /** Starts the loop (one tick per pollMs; unref'd timers: the host owns the process lifetime). */
  run(): this {
    if (this.#role === "stopped" || this.#running) return this;
    this.#running = true;
    this.#schedule(0);
    return this;
  }

  /** One pass: election, then projection (shadow) or maintenance only (sqlite). Serialised. */
  tick(): Promise<StateProjectorStatus> {
    return this.#serial(() => this.#tick());
  }

  /** Compares the whole file state with SQLite at the projected generation (shadow, active only). */
  verify(): Promise<ProjectorDivergence[]> {
    return this.#serial(async () => {
      let found: ProjectorDivergence[] = [];
      await this.#guard(async () => { found = await this.#verify(); });
      return found;
    });
  }

  /** Forces a full resync from state.json (shadow, active only). */
  resync(): Promise<StateProjectorStatus> {
    return this.#serial(async () => {
      if (this.#role === "active" && this.mode === "shadow") await this.#guard(() => this.#resync("manual"));
      return this.status();
    });
  }

  /** Stops cleanly: waits for the running pass, releases the lease, closes both connections. */
  async stop(): Promise<void> {
    this.#running = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    await this.#serial(() => this.#shutdown(undefined));
  }

  status(): StateProjectorStatus {
    return {
      root: this.root, database: this.database, mode: this.mode, owner: this.owner, role: this.#role,
      haltReason: this.#haltReason, leaseHolder: this.#leaseHolder, generation: this.#progress?.generation ?? undefined,
      commit: this.#progress?.commit ?? 0, appliedGenerations: this.#stats.appliedGenerations,
      fullResyncs: this.#stats.fullResyncs, lastResync: this.#lastResync ? { ...this.#lastResync } : undefined,
      lag: { ...this.#lag }, maxLagMs: this.#stats.maxLagMs, lastApplyLagMs: this.#lastApplyLagMs,
      divergenceChecks: this.#stats.divergenceChecks, divergences: this.#stats.divergences,
      divergentKeys: [...this.#divergentKeys], lastDivergences: this.#lastDivergences.map(divergence => ({ ...divergence })),
      foreignWrites: this.#stats.foreignWrites, errors: this.#stats.errors, lastError: this.#lastError,
      checkpoints: this.#stats.checkpoints, walBytes: this.#stats.walBytes, walAlarm: this.#walAlarm, updatedAt: Date.now(),
    };
  }

  // ---------------------------------------------------------------- loop

  #serial<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.#queue.then(operation, operation);
    this.#queue = run.catch(() => undefined);
    return run;
  }

  #schedule(ms: number): void {
    if (!this.#running || this.#role === "stopped") return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.tick().finally(() => this.#schedule(this.#pollMs));
    }, ms);
    this.#timer.unref?.();
  }

  async #tick(): Promise<StateProjectorStatus> {
    if (this.#role === "stopped") return this.status();
    await this.#guard(async () => {
      if (!(await this.#elect())) return;
      if (this.mode === "shadow") await this.#project();
      this.#maintain();
    });
    this.#writeStatus(false);
    return this.status();
  }

  // Fences demote or halt; anything else is counted and retried on the next pass.
  async #guard(operation: () => Promise<void>): Promise<void> {
    try { await operation(); }
    catch (error) {
      if (error instanceof ProjectorFence) {
        if (error.reason === "lease-lost") this.#setRole("standby", this.#readMeta().lease?.owner);
        else await this.#halt(error.reason, error.message);
        return;
      }
      this.#stats.errors += 1;
      this.#lastError = errorText(error);
      this.#emit({ type: "error", at: Date.now(), message: this.#lastError });
    }
  }

  #emit(event: StateProjectorEvent): void {
    try { this.#options.onEvent?.(event); } catch { /* an observer never stops the projector */ }
  }

  #setRole(role: StateProjectorRole, leaseHolder: string | undefined): void {
    const changed = role !== this.#role || leaseHolder !== this.#leaseHolder;
    this.#role = role;
    this.#leaseHolder = leaseHolder;
    if (changed) {
      this.#emit({ type: "role", at: Date.now(), role, leaseHolder });
      this.#writeStatus(true);
    }
  }

  async #halt(reason: StateProjectorHaltReason, detail: string): Promise<void> {
    if (reason === "foreign-write") this.#stats.foreignWrites += 1;
    this.#lastError = detail;
    this.#emit({ type: "alarm", at: Date.now(), alarm: reason, detail });
    await this.#shutdown(reason);
  }

  async #shutdown(reason: StateProjectorHaltReason | undefined): Promise<void> {
    if (this.#role === "stopped") return;
    this.#running = false;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    const wasActive = this.#role === "active";
    try {
      // Release the lease so a standby takes over at once (a crash releases it by expiry).
      await this.#transaction(() => {
        if (this.#readMeta().lease?.owner === this.owner) this.#sql.metaDelete.run(LEASE_META);
      }, 250);
    } catch { /* expiry releases it */ }
    this.#haltReason = reason;
    this.#role = "stopped";
    this.#leaseHolder = undefined;
    this.#emit({ type: "role", at: Date.now(), role: "stopped", leaseHolder: undefined });
    if (wasActive) this.#writeStatus(true);
    try { this.#db.close(); } catch { /* closing anyway */ }
    try { this.#store.close(); } catch { /* closing anyway */ }
  }

  // ---------------------------------------------------------------- database

  // BEGIN IMMEDIATE with an ASYNC retry (busy_timeout stays at 2 ms), the synchronous body, COMMIT.
  async #transaction<T>(body: () => T, budgetMs = this.#busyBudgetMs): Promise<T> {
    const deadline = Date.now() + budgetMs;
    for (;;) {
      try { this.#db.exec("BEGIN IMMEDIATE"); }
      catch (error) {
        if (!isBusy(error) || Date.now() >= deadline) throw error;
        await delay(2 + Math.random() * 4);
        continue;
      }
      let committed = false;
      try {
        const result = body();
        this.#db.exec("COMMIT");
        committed = true;
        return result;
      } finally {
        if (!committed && this.#db.isTransaction) try { this.#db.exec("ROLLBACK"); } catch { /* ended */ }
      }
    }
  }

  #readMeta(): { backend: string; commit: number; progress: Progress | undefined; lease: Lease | undefined } {
    const values = new Map<string, unknown>();
    for (const row of this.#sql.metaSome.all()) values.set(String(row.name), row.value);
    const lease = parseJson<Lease>(values.get(LEASE_META));
    return {
      backend: String(values.get("backend") ?? "missing"),
      commit: Number(values.get("commit_no") ?? 0),
      progress: parseJson<Progress>(values.get(PROGRESS_META)),
      lease: lease && typeof lease.owner === "string" && typeof lease.expiresAt === "number" ? lease : undefined,
    };
  }

  // The R5 fence. In a transaction it also requires the lease; returns the progress to extend.
  #fence(inTransaction: boolean): Progress {
    const meta = this.#readMeta();
    if (meta.backend !== "sqlite") throw new ProjectorFence("retired", `state database backend is ${meta.backend}`);
    if (inTransaction && meta.lease?.owner !== this.owner) throw new ProjectorFence("lease-lost", "projector lease lost");
    if (!meta.progress) {
      const empty = meta.commit === 0 && Number(this.#sql.kvCount.get()?.n ?? 0) === 0 && Number(this.#sql.tombCount.get()?.n ?? 0) === 0;
      if (!empty) throw new ProjectorFence("foreign-database", `${this.database} holds state the projector did not write`);
      return EMPTY_PROGRESS;
    }
    if (meta.progress.commit !== meta.commit) {
      throw new ProjectorFence("foreign-write", `commit ${meta.commit} in ${this.database} was not written by the projector (last ${meta.progress.commit})`);
    }
    this.#progress = meta.progress;
    return meta.progress;
  }

  async #elect(): Promise<boolean> {
    const now = Date.now();
    const current = this.#readMeta().lease;
    if (current && current.owner !== this.owner && current.expiresAt > now) {
      this.#setRole("standby", current.owner);
      return false;
    }
    if (!current || current.owner !== this.owner || current.expiresAt - now <= this.#leaseMs / 2) {
      const holder = await this.#transaction(() => {
        const lease = this.#readMeta().lease;
        const at = Date.now();
        if (lease && lease.owner !== this.owner && lease.expiresAt > at) return lease.owner;
        this.#sql.metaPut.run(LEASE_META, JSON.stringify({ owner: this.owner, expiresAt: at + this.#leaseMs, pid: process.pid, host: os.hostname() }));
        return this.owner;
      });
      if (holder !== this.owner) {
        this.#setRole("standby", holder);
        return false;
      }
    }
    this.#setRole("active", this.owner);
    return true;
  }

  #maintain(): void {
    const now = Date.now();
    if (now - this.#lastCheckpointAt >= this.#checkpointMs) {
      this.#lastCheckpointAt = now;
      try {
        // R10: the elected projector is the checkpointer: PASSIVE, TRUNCATE when large and growing.
        this.#store.checkpoint();
        this.#stats.checkpoints += 1;
      } catch (error) {
        this.#stats.errors += 1;
        this.#lastError = errorText(error);
      }
    }
    this.#stats.walBytes = this.#store.walBytes();
    const alarm = this.#walAlarmBytes > 0 && this.#stats.walBytes > this.#walAlarmBytes;
    if (alarm && !this.#walAlarm) {
      this.#emit({ type: "alarm", at: now, alarm: "wal-size", detail: `WAL ${this.#stats.walBytes} bytes > ${this.#walAlarmBytes}` });
    }
    this.#walAlarm = alarm;
  }

  #writeStatus(force: boolean): void {
    if (this.#statusMs === 0 || (this.#role !== "active" && this.#role !== "stopped")) return;
    const now = Date.now();
    if (!force && now - this.#lastStatusAt < this.#statusMs) return;
    this.#lastStatusAt = now;
    try { writeFileAtomic(path.join(this.root, STATE_PROJECTOR_STATUS_FILE), `${JSON.stringify(this.status())}\n`); }
    catch { /* observability only */ }
  }

  // ---------------------------------------------------------------- projection

  async #project(): Promise<void> {
    const progress = this.#fence(false);
    if (stateIdentity(this.root) === progress.identity) {
      this.#lag = { revisions: 0, generations: 0, ms: 0 };
      if (this.#verifyMs > 0 && Date.now() - this.#lastVerifyAt >= this.#verifyMs) await this.#verify();
      return;
    }
    const head = readHead(this.root);
    if (head.identity === progress.identity) return;
    if (!progress.generation || !head.generation || !head.hash) {
      await this.#resync(progress.identity === null ? "initial" : "gap");
      return;
    }
    if (head.generation === progress.generation) {
      await this.#resync("rewrite");
      return;
    }
    // Follow the journal to the generation this head names. A writer renames state.json BEFORE it
    // appends that generation's record, so a missing record is retried briefly (same head) before
    // a gap costs a full resync. Later generations are left for the next pass.
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const result = await this.#follow(progress, head);
      if (result === "applied") return;
      if (result === "broken") break;
      await delay(10 * (attempt + 1));
    }
    await this.#resync("gap");
  }

  // Applies the verified chain from the projected generation to the head's, one transaction each.
  // "missing": no chain reaches the head yet; "broken": the chain is not bound to the file, or it
  // broke mid-way; the caller resyncs.
  async #follow(progress: Progress, head: FileHead): Promise<"applied" | "missing" | "broken"> {
    const records = readJournal(this.root, progress.cursor);
    const chain = records && chainOf(records, progress, head);
    const terminal = chain?.at(-1);
    if (!chain || !terminal) return "missing";
    if (terminal.identity !== head.identity) return "broken";
    // The chain hash was read from the header of the very file whose identity the terminal record
    // names, so it binds every record body. The payload check additionally binds the bytes
    // (copied-marker replacement). Under load the file has often moved on by now; then the bytes
    // are gone and the header binding stands (a copied marker would surface in verify()).
    const bound = verifyStateJournalEndpoint(this.root,
      { generation: terminal.generation, chainHash: terminal.hash, identity: terminal.identity, payloadHash: terminal.payloadHash }) ||
      stateIdentity(this.root) !== head.identity;
    if (!bound) return "broken";
    const highWater = Number(this.#sql.metaGet.get("high_water")?.value ?? 0);
    this.#observeLag({ revisions: Math.max(0, terminal.highWater - highWater), generations: chain.length,
      ms: Date.now() - commitTime(chain[0]!, head.mtimeMs) });
    const inode = journalCursorOf(this.root)?.inode ?? "";
    for (const [index, record] of chain.entries()) {
      try {
        await this.#transaction(() => {
          const base = this.#fence(true);
          if (base.generation !== record.previous) throw new ProjectorGap("the projected generation moved");
          this.#applyRecord(record, { generation: record.generation, hash: record.hash, identity: record.identity,
            cursor: { inode, offset: record.end }, commit: base.commit, appliedAt: Date.now() });
          this.#options.beforeCommit?.({ kind: "record", generation: record.generation, index });
        });
      } catch (error) {
        if (error instanceof ProjectorGap) return "broken";
        throw error;
      }
      this.#stats.appliedGenerations += 1;
      this.#lastApplyLagMs = Math.max(0, Date.now() - commitTime(record, head.mtimeMs));
      this.#stats.maxLagMs = Math.max(this.#stats.maxLagMs, this.#lastApplyLagMs);
      const next = chain[index + 1];
      this.#lag = next
        ? { revisions: Math.max(0, terminal.highWater - record.highWater), generations: chain.length - index - 1,
          ms: Math.max(0, Date.now() - commitTime(next, head.mtimeMs)) }
        : { revisions: 0, generations: 0, ms: 0 };
    }
    return "applied";
  }

  #observeLag(lag: StateProjectorLag): void {
    this.#lag = { ...lag, ms: Math.max(0, lag.ms) };
    this.#stats.maxLagMs = Math.max(this.#stats.maxLagMs, this.#lag.ms);
  }

  async #resync(reason: StateProjectorResyncReason, given?: FileSnapshot): Promise<void> {
    const snapshot = given ?? readSnapshot(this.root, this.#maxStateBytes);
    this.#observeLag({ revisions: Math.max(0, snapshot.highWater - Number(this.#sql.metaGet.get("high_water")?.value ?? 0)),
      generations: 1, ms: Date.now() - snapshot.mtimeMs });
    await this.#transaction(() => {
      const base = this.#fence(true);
      this.#applySnapshot(snapshot, { generation: snapshot.generation, hash: snapshot.hash, identity: snapshot.identity,
        cursor: snapshot.cursor, commit: base.commit, appliedAt: Date.now() });
      this.#options.beforeCommit?.({ kind: "snapshot", generation: snapshot.generation ?? undefined, index: 0 });
    });
    this.#stats.fullResyncs += 1;
    this.#lastApplyLagMs = Math.max(0, Date.now() - snapshot.mtimeMs);
    this.#lag = { revisions: 0, generations: 0, ms: 0 };
    this.#lastResync = { reason, at: Date.now(), entries: snapshot.entries.size, tombstones: snapshot.tombstones.length };
    this.#emit({ type: "resync", ...this.#lastResync });
  }

  // "live:<v>", "tomb:<v>" or "none": a key's slot, for the changes feed.
  #slot(key: string): string {
    const live = this.#sql.kvGet.get(key);
    if (live) return `live:${Number(live.version)}`;
    const tomb = this.#sql.tombGet.get(key);
    return tomb ? `tomb:${Number(tomb.version)}` : "none";
  }

  #applyRecord(record: JournalRecord, next: Progress): void {
    const current = this.#sql.tombAll.all().map(row => [String(row.key), Number(row.version)] as [string, number]);
    const currentKeys = current.map(([key]) => key);
    const currentVersions = new Map(current);
    let order: string[];
    if (record.tombstonePatch !== undefined) {
      const patched = applyTombstonePatch(currentKeys, record.tombstonePatch);
      if (!patched) throw new ProjectorGap("the tombstone patch does not apply");
      order = patched;
    } else if (record.tombstoneOrder !== undefined && record.tombstoneOrder !== null) {
      if (!Array.isArray(record.tombstoneOrder) || record.tombstoneOrder.some(key => typeof key !== "string")) {
        throw new ProjectorGap("invalid tombstone order");
      }
      order = record.tombstoneOrder as string[];
    } else order = currentKeys;
    const candidates = new Set([...Object.keys(record.entries), ...Object.keys(record.versions), ...currentKeys, ...order]);
    const before = new Map([...candidates].map(key => [key, this.#slot(key)] as const));
    for (const [key, value] of Object.entries(record.entries)) {
      if (value === null) { this.#sql.kvDelete.run(key); continue; }
      const entry = toEntry(key, value);
      if (!entry) throw new ProjectorGap(`invalid journal entry ${JSON.stringify(key)}`);
      const row = encode(entry);
      if (!sameRow(this.#sql.kvGet.get(key), row)) this.#sql.kvUpsert.run(key, row.value, row.version, row.updatedAt, row.updatedBy, row.bytes);
    }
    const desired = order.map((key): [string, number] => {
      const stated = record.versions[key];
      const version = stated === undefined ? currentVersions.get(key) : revision(stated);
      if (version === undefined || this.#sql.kvGet.get(key)) throw new ProjectorGap(`inconsistent tombstone ${JSON.stringify(key)}`);
      return [key, version];
    });
    this.#syncTombstones(desired, current);
    this.#finish(candidates, before, record.highWater, next);
  }

  #applySnapshot(snapshot: FileSnapshot, next: Progress): void {
    const current = this.#sql.tombAll.all().map(row => [String(row.key), Number(row.version)] as [string, number]);
    const existing = new Map(this.#sql.kvAll.all().map(row => [String(row.key), row] as const));
    const candidates = new Set([...existing.keys(), ...snapshot.entries.keys(), ...current.map(([key]) => key),
      ...snapshot.tombstones.map(([key]) => key)]);
    const before = new Map([...candidates].map(key => [key, this.#slot(key)] as const));
    for (const [key, entry] of snapshot.entries) {
      const row = encode(entry);
      if (!sameRow(existing.get(key), row)) this.#sql.kvUpsert.run(key, row.value, row.version, row.updatedAt, row.updatedBy, row.bytes);
    }
    for (const key of existing.keys()) if (!snapshot.entries.has(key)) this.#sql.kvDelete.run(key);
    this.#syncTombstones(snapshot.tombstones, current);
    this.#finish(candidates, before, snapshot.highWater, next);
  }

  // Makes the tombstone table equal `desired`, in order: in place when the change is "drop some,
  // append some" (the file store's only shapes), else a rewrite with fresh ordinals.
  #syncTombstones(desired: ReadonlyArray<[string, number]>, current: ReadonlyArray<[string, number]>): void {
    const desiredKeys = new Set(desired.map(([key]) => key));
    const currentKeys = new Set(current.map(([key]) => key));
    const kept = current.filter(([key]) => desiredKeys.has(key));
    let ordinal = Number(this.#sql.metaGet.get("tombstone_ord")?.value ?? 0);
    const inPlace = kept.every(([key], index) => desired[index]?.[0] === key) &&
      desired.slice(kept.length).every(([key]) => !currentKeys.has(key));
    if (inPlace) {
      for (const [key] of current) if (!desiredKeys.has(key)) this.#sql.tombDelete.run(key);
      for (const [index, [key, version]] of kept.entries()) {
        const target = desired[index]![1];
        if (target !== version) this.#sql.tombVersion.run(target, key);
      }
      for (const [key, version] of desired.slice(kept.length)) this.#sql.tombInsert.run(key, version, ++ordinal);
    } else {
      this.#sql.tombClear.run();
      for (const [key, version] of desired) this.#sql.tombInsert.run(key, version, ++ordinal);
    }
    this.#sql.metaSet.run(ordinal, "tombstone_ord");
  }

  // One SQLite commit per changing generation: changes rows, commit_no, clock, bytes, progress.
  #finish(candidates: Iterable<string>, before: ReadonlyMap<string, string>, highWater: number, next: Progress): void {
    const changes: Array<{ key: string; version: number; deleted: boolean }> = [];
    for (const key of candidates) {
      const after = this.#slot(key);
      if (after === before.get(key)) continue;
      const [kind, version] = after.split(":");
      changes.push({ key, version: Number(version ?? 0), deleted: kind !== "live" });
    }
    let commit = Number(this.#sql.metaGet.get("commit_no")?.value ?? 0);
    if (changes.length > 0) {
      commit += 1;
      let seq = 0;
      for (const change of changes) {
        seq = Number(this.#sql.changeInsert.run(commit, change.key, change.version, change.deleted ? 1 : 0).lastInsertRowid);
      }
      if (seq > this.#changesRetained) this.#sql.changeTrim.run(seq - this.#changesRetained);
    }
    // The clock never goes back: SQLite issues the next revisions after cutover.
    const clock = Math.max(Number(this.#sql.metaGet.get("high_water")?.value ?? 0), highWater,
      Number(this.#sql.maxVersion.get()?.m ?? 0));
    this.#sql.metaSet.run(clock, "high_water");
    this.#sql.metaSet.run(commit, "commit_no");
    this.#sql.metaSet.run(Number(this.#sql.kvBytes.get()?.n ?? 0) + Number(this.#sql.tombBytes.get()?.n ?? 0), "state_bytes");
    const progress: Progress = { ...next, commit };
    this.#sql.metaPut.run(PROGRESS_META, JSON.stringify(progress));
    this.#progress = progress;
  }

  // ---------------------------------------------------------------- divergence

  async #verify(): Promise<ProjectorDivergence[]> {
    if (this.#role !== "active" || this.mode !== "shadow") return [];
    this.#lastVerifyAt = Date.now();
    const snapshot = readSnapshot(this.root, this.#maxStateBytes);
    // One read transaction: the rows and the fence come from one SQLite snapshot.
    let rows: SqliteRow[];
    let tombs: SqliteRow[];
    let progress: Progress;
    this.#db.exec("BEGIN");
    try {
      progress = this.#fence(false);
      rows = this.#sql.kvAll.all();
      tombs = this.#sql.tombAll.all();
    } finally { try { this.#db.exec("COMMIT"); } catch { try { this.#db.exec("ROLLBACK"); } catch { /* ended */ } } }
    // Only at the projected generation: anything newer is lag, not divergence.
    if (snapshot.identity !== progress.identity) return [];
    this.#stats.divergenceChecks += 1;
    const sqlite = new Map(rows.map(row => [String(row.key), row] as const));
    const differences: ProjectorDivergence[] = [];
    const side = (entry: { value: unknown; updatedBy: MeshIdentity } | undefined) =>
      entry ? { value: entry.value, updatedBy: entry.updatedBy } : undefined;
    for (const key of new Set([...snapshot.entries.keys(), ...sqlite.keys()])) {
      const file = snapshot.entries.get(key);
      const row = sqlite.get(key);
      const stored = row ? {
        value: parseJson<unknown>(String(row.value)), updatedBy: parseJson<MeshIdentity>(String(row.updated_by)) as MeshIdentity,
        version: Number(row.version), updatedAt: Number(row.updated_at),
      } : undefined;
      const field: ProjectorDivergence["field"] | undefined = !file || !stored ? (file || stored ? "presence" : undefined)
        : file.version !== stored.version ? "version"
          : JSON.stringify(file.value) !== JSON.stringify(stored.value) ? "value"
            : JSON.stringify(file.updatedBy) !== JSON.stringify(stored.updatedBy) ? "updatedBy"
              : file.updatedAt !== stored.updatedAt ? "updatedAt" : undefined;
      if (field) differences.push({ key, field, file: side(file), sqlite: side(stored), fileVersion: file?.version, sqliteVersion: stored?.version });
    }
    const fileTombs = new Map(snapshot.tombstones);
    const sqliteTombs = new Map(tombs.map(row => [String(row.key), Number(row.version)] as const));
    for (const key of new Set([...fileTombs.keys(), ...sqliteTombs.keys()])) {
      if (fileTombs.get(key) === sqliteTombs.get(key) || differences.some(divergence => divergence.key === key)) continue;
      differences.push({ key, field: "tombstone", file: undefined, sqlite: undefined, fileVersion: fileTombs.get(key), sqliteVersion: sqliteTombs.get(key) });
    }
    if (differences.length > 0) {
      this.#stats.divergences += differences.length;
      for (const divergence of differences) {
        this.#divergentKeys = [...this.#divergentKeys.filter(key => key !== divergence.key), divergence.key].slice(-MAX_RECENT);
      }
      this.#lastDivergences = differences.slice(0, MAX_RECENT);
      this.#emit({ type: "divergence", at: Date.now(), divergences: this.#lastDivergences.map(divergence => ({ ...divergence })) });
      this.#writeStatus(true);
      if (this.#options.repairDivergence !== false) await this.#resync("divergence", snapshot);
    }
    return differences;
  }
}
