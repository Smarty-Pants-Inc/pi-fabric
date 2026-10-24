import fs from "node:fs";
import path from "node:path";
import type { SqliteConnection, SqliteStatement, SqliteValue } from "./state-sqlite.js";

export const DEFAULT_SQLITE_HOLD_MS = 2_000;
export const SQLITE_HOLDS_MAX_BYTES = 1024 * 1024;
const MAX_PHASES = 128;
const MAX_LINE_BYTES = 32 * 1024;

/** Fixed caller classes only: never identities, paths, SQL text or bound values. */
export type SqliteBatchHoldTag = "outbox:fence" | "outbox:recover" | "outbox:retire" | "shadow:mirror";
export interface SqliteHoldPhase { tag: string; count: number; totalMs: number; changes: number; errors: number }
export interface SqliteHoldLine {
  v: 1;
  at: string;
  tag: string;
  pid: number;
  release: string;
  bridge: boolean;
  waitMs: number;
  holdMs: number;
  commitMs: number;
  rollbackMs: number;
  outcome: "commit" | "rollback" | "close";
  phases: SqliteHoldPhase[];
}

export const sqliteHoldsPath = (root: string): string => path.join(root, "metrics", "sqlite-holds.jsonl");

/** As with lock events: one bounded current file and one previous generation, best effort only.
 * Serialize append AND rotation with a non-waiting metrics-only lock; a contending sampler drops
 * its line rather than growing an already full file. Reclaim a crashed sampler's exact old inode.
 * No database or mesh locks are acquired here. All this runs AFTER writer custody has ended.
 */
export function appendSqliteHold(root: string, line: SqliteHoldLine): void {
  let lockFd: number | undefined;
  let own: fs.BigIntStats | undefined;
  const file = sqliteHoldsPath(root);
  const lock = file + ".lock";
  // Metrics may already be group-writable (other appenders use the bridge's umask).
  // Remove only the lock inode we held or judged stale, not a successor at the same name.
  // BigInt preserves the full file id on Windows as well as Unix.
  const removeLock = (seen: fs.BigIntStats): boolean => {
    const current = fs.lstatSync(lock, { bigint: true, throwIfNoEntry: false });
    if (!current || current.dev !== seen.dev || current.ino !== seen.ino) return false;
    fs.unlinkSync(lock);
    return true;
  };
  try {
    const data = Buffer.from(JSON.stringify(line) + "\n");
    if (data.length > MAX_LINE_BYTES) return;
    // Do not resurrect a removed mesh root.
    try { fs.mkdirSync(path.dirname(file), { mode: 0o700 }); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") return; }
    const directory = fs.lstatSync(path.dirname(file));
    if (!directory.isDirectory() || directory.isSymbolicLink()) return;
    for (let attempt = 0; attempt < 2; attempt++) {
      try { lockFd = fs.openSync(lock, "wx", 0o600); break; }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST" || attempt !== 0) return;
        const seen = fs.lstatSync(lock, { bigint: true });
        if (!seen.isFile() || seen.isSymbolicLink() || Date.now() - Number(seen.mtimeMs) <= 60_000) return;
        if (!removeLock(seen)) return;
      }
    }
    if (lockFd === undefined) return;
    own = fs.fstatSync(lockFd, { bigint: true });
    const seen = fs.lstatSync(file, { throwIfNoEntry: false });
    if (seen && (!seen.isFile() || seen.isSymbolicLink())) return;
    if (seen && seen.size + data.length > SQLITE_HOLDS_MAX_BYTES) {
      // Rotation, not deletion: replace exactly one previous generation under the append lock.
      fs.renameSync(file, file + ".1");
    }
    const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT
      | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK, 0o600);
    try {
      const current = fs.fstatSync(fd);
      if (!current.isFile() || current.size + data.length > SQLITE_HOLDS_MAX_BYTES) return;
      fs.writeSync(fd, data); // One O_APPEND write per JSON line.
    } finally { fs.closeSync(fd); }
  } catch { /* Diagnostics must never affect a writer. */ }
  finally {
    if (lockFd !== undefined) {
      try { fs.closeSync(lockFd); } catch { /* best effort */ }
      try { if (own) removeLock(own); } catch { /* best effort */ }
    }
  }
}

const thresholdOf = (): number => {
  const raw = process.env.PI_FABRIC_SQLITE_HOLD_MS;
  if (raw === undefined || raw.trim() === "") return DEFAULT_SQLITE_HOLD_MS;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : DEFAULT_SQLITE_HOLD_MS;
};
let bridgeProcess = false;
/** Called by a bridge side, not inferred from arbitrary identity or command-line text. */
export const markSqliteBridgeProcess = (): void => { bridgeProcess = true; };

// Read provenance only for a slow line, never at import/connection-open or on a fast hold.
let release: string | undefined;
const ownRelease = (): string => {
  if (release !== undefined) return release;
  const sha = process.env.PI_FABRIC_RELEASE_SHA ?? process.env.PI_FABRIC_BUILD_SHA ?? process.env.GITHUB_SHA;
  if (sha && /^[a-f0-9]{7,64}$/i.test(sha)) return release = sha;
  // Source/shared chunks and standalone bundles have different depths. Never emit a path.
  for (const relative of ["../../package.json", "../package.json"]) {
    try {
      const manifest = JSON.parse(fs.readFileSync(new URL(relative, import.meta.url), "utf8")) as { name?: unknown; version?: unknown };
      if (manifest.name === "pi-fabric" && typeof manifest.version === "string" && /^[0-9][0-9a-z.+-]{0,63}$/i.test(manifest.version)) {
        return release = manifest.version;
      }
    } catch { /* unavailable */ }
  }
  return release = "unknown";
};

/** Stable short SQL fingerprint: SQL/parameters themselves never leave the process. */
const statementTag = (sql: string): string => {
  let hash = 2166136261;
  for (let i = 0; i < sql.length; i++) hash = Math.imul(hash ^ sql.charCodeAt(i), 16777619);
  const verb = /^\s*(SELECT|INSERT|UPDATE|DELETE|PRAGMA|CREATE)\b/i.exec(sql)?.[1]?.toLowerCase() ?? "sql";
  const table = /\b(?:FROM|INTO|UPDATE|TABLE(?: IF NOT EXISTS)?)\s+(meta|kv|tombstones|changes)\b/i.exec(sql)?.[1]?.toLowerCase();
  return verb + (table ? ":" + table : "") + ":" + (hash >>> 0).toString(16).padStart(8, "0");
};

interface HoldOptions {
  thresholdMs?: number;
  /** Deterministic timing/output seams; production uses monotonic performance.now and bounded JSONL. */
  now?: () => number;
  append?: (root: string, line: SqliteHoldLine) => void;
}
const recorders = new WeakMap<SqliteConnection, HoldRecorder>();

/** Set immediately before EACH BEGIN attempt. Retrying callers pass their original monotonic
 * wait start, so async busy backoff counts without sharing mutable context across awaits.
 */
export const sqliteHoldContext = (db: SqliteConnection, tag: string, waitStarted: number): void => {
  const recorder = recorders.get(db);
  if (recorder) { recorder.tag = tag; recorder.waitStarted = waitStarted; }
};

class HoldRecorder {
  active = false;
  tag: string;
  waitStarted = Number.NaN;
  acquired = 0;
  waitMs = 0;
  commitMs = 0;
  rollbackMs = 0;
  private epoch = 0;
  private readonly seen = new Float64Array(MAX_PHASES);
  private readonly counts = new Float64Array(MAX_PHASES);
  private readonly totals = new Float64Array(MAX_PHASES);
  private readonly changes = new Float64Array(MAX_PHASES);
  private readonly errors = new Float64Array(MAX_PHASES);
  private readonly tags = ["other"];
  private readonly ids = new Map<string, number>();
  private readonly execIds = new Map<string, number>();
  constructor(readonly root: string, readonly defaultTag: string, readonly threshold: number,
    readonly now: () => number, readonly append: (root: string, line: SqliteHoldLine) => void) { this.tag = defaultTag; }

  phase(tag: string): number {
    let id = this.ids.get(tag);
    if (id !== undefined) return id;
    if (this.tags.length >= MAX_PHASES) return 0;
    id = this.tags.length;
    this.tags.push(tag);
    this.ids.set(tag, id);
    return id;
  }
  execPhase(sql: string): number {
    const cached = this.execIds.get(sql);
    if (cached !== undefined) return cached;
    const id = this.phase(statementTag(sql) + ":exec");
    if (this.execIds.size < MAX_PHASES) this.execIds.set(sql, id);
    return id;
  }
  begin(at: number, started: number): void {
    this.epoch++;
    this.acquired = at;
    this.waitMs = Math.max(0, at - started);
    this.commitMs = this.rollbackMs = 0;
    this.active = true;
  }
  note(id: number, elapsed: number, changes = 0, error = false): void {
    if (this.seen[id] !== this.epoch) {
      this.seen[id] = this.epoch;
      this.counts[id] = 1;
      this.totals[id] = elapsed;
      this.changes[id] = changes;
      this.errors[id] = error ? 1 : 0;
      return;
    }
    this.counts[id]!++;
    this.totals[id]! += elapsed;
    this.changes[id]! += changes;
    if (error) this.errors[id]!++;
  }
  end(outcome: SqliteHoldLine["outcome"], at: number): void {
    const holdMs = at - this.acquired;
    this.active = false;
    const tag = this.tag;
    this.tag = this.defaultTag;
    this.waitStarted = Number.NaN;
    if (holdMs <= this.threshold) return;
    // The only per-hold object/array construction and I/O is below this threshold guard.
    try {
      const phases: SqliteHoldPhase[] = [];
      for (let id = 0; id < this.tags.length; id++) {
        if (this.seen[id] === this.epoch) phases.push({ tag: this.tags[id]!, count: this.counts[id]!,
          totalMs: this.totals[id]!, changes: this.changes[id]!, errors: this.errors[id]! });
      }
      const line: SqliteHoldLine = { v: 1, at: new Date().toISOString(), tag, pid: process.pid,
        release: "unknown", bridge: bridgeProcess, waitMs: this.waitMs, holdMs,
        commitMs: this.commitMs, rollbackMs: this.rollbackMs, outcome, phases };
      // Let the caller finish its own COMMIT/hold accounting and synchronous release cleanup
      // before filesystem work. This is event-driven, only on slow holds, not a timer/poller.
      queueMicrotask(() => {
        try { line.release = ownRelease(); this.append(this.root, line); } catch { /* best effort */ }
      });
    } catch { /* No metric failure can replace a transaction's result/error. */ }
  }
}

/** Decorate an admitted connection once, BEFORE preparing statements or acquiring custody.
 * No Proxy, per-transaction maps/arrays, extra SQL, timers, retries, or changes to driver receivers.
 * When disabled return the exact original connection. Prepared wrappers and numeric counters are
 * connection-scoped; get/all/run record both successful and throwing calls in finally/catch.
 */
export function recordSqliteHolds(db: SqliteConnection, root: string, defaultTag: string, options: HoldOptions = {}): SqliteConnection {
  const threshold = options.thresholdMs ?? thresholdOf();
  if (threshold === 0 || recorders.has(db)) return db;
  const r = new HoldRecorder(root, defaultTag, threshold, options.now ?? (() => performance.now()), options.append ?? appendSqliteHold);
  const endedOnError = () => {
    // SQLite can automatically roll back on an I/O or FULL error. Do not wait for a ROLLBACK
    // the caller deliberately skips when isTransaction is false, or emit twice for that error.
    try { if (r.active && !db.isTransaction) r.end("rollback", r.now()); } catch { /* diagnostics only */ }
  };
  const wrapped: SqliteConnection = {
    ...(db.attestDatabaseFile ? { attestDatabaseFile: () => db.attestDatabaseFile!() } : {}),
    get isTransaction() { return db.isTransaction; },
    close() {
      db.close();
      if (r.active) r.end("close", r.now());
    },
    exec(sql) {
      if (sql === "BEGIN IMMEDIATE") {
        const started = Number.isNaN(r.waitStarted) ? r.now() : r.waitStarted;
        db.exec(sql); // A busy BEGIN never constitutes custody and never emits a hold.
        r.begin(r.now(), started);
        return;
      }
      if (!r.active) { db.exec(sql); return; }
      const commit = sql === "COMMIT";
      const rollback = sql === "ROLLBACK";
      const id = commit || rollback ? 0 : r.execPhase(sql);
      const started = r.now();
      let failed = true;
      try { db.exec(sql); failed = false; }
      finally {
        const at = r.now();
        if (commit) r.commitMs += at - started;
        else if (rollback) r.rollbackMs += at - started;
        else r.note(id, at - started, 0, failed);
        if (!failed && (commit || rollback)) r.end(commit ? "commit" : "rollback", at);
        else if (failed) endedOnError();
      }
    },
    prepare(sql) {
      const tag = statementTag(sql);
      const prepareId = r.phase(tag + ":prepare");
      const started = r.active ? r.now() : 0;
      let statement: SqliteStatement;
      try { statement = db.prepare(sql); }
      catch (error) {
        if (r.active) { r.note(prepareId, r.now() - started, 0, true); endedOnError(); }
        throw error;
      }
      if (r.active) r.note(prepareId, r.now() - started);
      const runId = r.phase(tag + ":run"), getId = r.phase(tag + ":get"), allId = r.phase(tag + ":all");
      return {
        run(...params: SqliteValue[]) {
          if (!r.active) return statement.run(...params);
          const started = r.now();
          let result: ReturnType<SqliteStatement["run"]>;
          try { result = statement.run(...params); }
          catch (error) {
            try { r.note(runId, r.now() - started, 0, true); } catch { /* diagnostics only */ }
            endedOnError();
            throw error;
          }
          // Bookkeeping cannot classify a successful driver call as a statement error or replace its result.
          try { r.note(runId, r.now() - started, Number(result.changes)); } catch { /* diagnostics only */ }
          return result;
        },
        get(...params: SqliteValue[]) {
          if (!r.active) return statement.get(...params);
          const started = r.now();
          let result: ReturnType<SqliteStatement["get"]>;
          try { result = statement.get(...params); }
          catch (error) {
            try { r.note(getId, r.now() - started, 0, true); } catch { /* diagnostics only */ }
            endedOnError();
            throw error;
          }
          // Bookkeeping cannot classify a successful driver call as a statement error or replace its result.
          try { r.note(getId, r.now() - started, 0); } catch { /* diagnostics only */ }
          return result;
        },
        all(...params: SqliteValue[]) {
          if (!r.active) return statement.all(...params);
          const started = r.now();
          let result: ReturnType<SqliteStatement["all"]>;
          try { result = statement.all(...params); }
          catch (error) {
            try { r.note(allId, r.now() - started, 0, true); } catch { /* diagnostics only */ }
            endedOnError();
            throw error;
          }
          // Bookkeeping cannot classify a successful driver call as a statement error or replace its result.
          try { r.note(allId, r.now() - started, 0); } catch { /* diagnostics only */ }
          return result;
        },
      };
    },
  };
  recorders.set(wrapped, r);
  return wrapped;
}
