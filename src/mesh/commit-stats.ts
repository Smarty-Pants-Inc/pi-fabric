import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

interface CommitStats {
  record(bytes: number, keys: readonly string[]): void;
}
interface ProcessCommitStats {
  /** Registry generation, not the JSONL schema version. The counter API stays stable. */
  version: number;
  file: string | undefined;
  counter: CommitStats | undefined;
  dispose(): void;
}
const processKey = Symbol.for("pi-fabric.mesh.commit-stats");
const processGlobals = globalThis as typeof globalThis & { [processKey]?: ProcessCommitStats };

/**
 * Capture the opt-in once, including disabled, across module/release reloads.
 * Older and newer generations reuse the stable record API, original sink and minute
 * boundary rather than replacing the owner. Keep this registry contract compatible:
 * a future implementation must hand over explicitly before changing that contract.
 */
export const createCommitStats = (file = process.env.PI_FABRIC_COMMIT_STATS): CommitStats | undefined => {
  const existing = processGlobals[processKey];
  if (existing) return existing.counter;
  if (!file) {
    processGlobals[processKey] = { version: 1, file, counter: undefined, dispose() {} };
    return undefined;
  }
  let since = Date.now();
  let commits = 0;
  let bytesWritten = 0;
  let byReason: Record<string, { commits: number; bytesWritten: number }> = Object.create(null);
  const reasonOf = (key: string): string => key.startsWith("sessions/") ? "legacy-session"
    : key.startsWith("topology/hosts/") ? "host-lease"
    : key.startsWith("topology/participants/") ? "participant"
    : key.startsWith("topology/") ? "topology" : key.split("/")[0] || "state";
  const timer = setInterval(() => {
    const at = Date.now();
    try {
      fs.appendFileSync(file, JSON.stringify({ version: 1, pid: process.pid, since, at,
        commits, bytesWritten, byReason }) + "\n", { mode: 0o600 });
      since = at;
      commits = 0;
      bytesWritten = 0;
      byReason = Object.create(null);
    } catch { /* Diagnostics must not fail or change a committed operation; retry next minute. */ }
  }, 60_000);
  timer.unref();
  const dispose = () => {
    clearInterval(timer);
    process.removeListener("exit", dispose);
  };
  const counter: CommitStats = { record(bytes, keys) {
    const reason = [...new Set(keys.map(reasonOf))].sort().join("+") || "state";
    const bucket = byReason[reason] ??= { commits: 0, bytesWritten: 0 };
    bucket.commits++;
    bucket.bytesWritten += bytes;
    commits++;
    bytesWritten += bytes;
  } };
  processGlobals[processKey] = { version: 1, file, counter, dispose };
  process.once("exit", dispose);
  return counter;
};

// ---------------------------------------------------------------------------------------------
// Mesh-lock wait and hold time by caller class (smarty-dev#6477 L8).
//
// Every acquisition of a mesh root's `.lock` records who took it (the store entry point), how
// long it waited and how long it held the lock. Wall-clock minutes are aggregated in memory and
// written just after each minute (and at exit) to `<root>/lock-stats/<host>-<pid>.json`: one
// small file per process, rewritten by an atomic rename, holding at most the last 60 complete
// minutes plus the current one (the longest query window, see LOCK_STATS_RETAIN_MINUTES). No extra
// lock, no fsync, and nothing at all before the first acquisition. `fabric-mesh-lock-stats`
// sums every process's file into fleet busy %, timeouts and the classes and pids that hold it.
// On by default; PI_FABRIC_LOCK_STATS=0 (or off/false/no) disables it for the process.

/** Who took the mesh lock: the store entry point, never a stack walk. */
export type MeshLockClass = "publish" | "put/delete" | "writeBatch" | "heartbeat/confirm" | "custody" | "bridge" | "other";
const LOCK_CLASSES: ReadonlySet<string> = new Set<MeshLockClass>(["publish", "put/delete", "writeBatch", "heartbeat/confirm", "custody", "bridge", "other"]);

export interface LockStats {
  /** One acquisition: wait runs from the request to custody, hold from custody to release (ms). */
  acquired(root: string, lockClass: MeshLockClass, waitMs: number, holdMs: number): void;
  /** One acquisition that ended in the typed lock timeout. `tried` marks a bounded try (a
   * registry-fenced or zero-wait caller), which fails by design while the lock is busy. */
  failed(root: string, lockClass: MeshLockClass, waitMs: number, tried: boolean): void;
}

/** Histogram upper bounds in ms; one more bucket counts everything above the last bound. */
export const LOCK_STATS_BOUNDS_MS: readonly number[] = [1, 2, 5, 10, 20, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];
export const LOCK_STATS_DIR = "lock-stats";
/** Complete minutes retained besides the current one; also the longest query window (--minutes). */
export const LOCK_STATS_RETAIN_MINUTES = 60;
const LOCK_STATS_MAX_ROOTS = 32;
const LOCK_STATS_STALE_FILE_MS = 24 * 60 * 60_000;
const LOCK_STATS_PRUNE_EVERY_MS = 60 * 60_000;
/** Reader caps: a stats file is a few KiB; anything near these is not ours. */
export const LOCK_STATS_MAX_FILE_BYTES = 1024 * 1024;
export const LOCK_STATS_MAX_FILES = 4096;

export interface LockStatsBucket {
  /** Successful acquisitions. */
  n: number;
  waitMs: number;
  waitMaxMs: number;
  holdMs: number;
  holdMaxMs: number;
  /** Full-budget acquisitions that timed out. */
  timeouts: number;
  /** Bounded tries that found the lock busy. */
  tries: number;
  /** Time spent waiting by the failed acquisitions. */
  failedWaitMs: number;
  waitHist: number[];
  holdHist: number[];
}
export interface LockStatsMinute { minute: number; classes: Partial<Record<MeshLockClass, LockStatsBucket>> }
export interface LockStatsFile {
  version: 1;
  host: string;
  pid: number;
  root: string;
  startedAt: number;
  updatedAt: number;
  minutes: LockStatsMinute[];
}

interface ProcessLockStats {
  /** Registry generation. Keep the record API stable; hand over explicitly before changing it. */
  version: number;
  stats: LockStats | undefined;
  flush(): void;
  dispose(): void;
}
interface RootLockStats {
  root: string;
  minutes: Map<number, Partial<Record<MeshLockClass, LockStatsBucket>>>;
  dirty: boolean;
  prunedAt: number;
  /** The lock-stats directory is unsafe to write: nothing more is recorded for this root. */
  disabled: boolean;
}
const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const lockGlobals = globalThis as typeof globalThis & { [lockKey]?: ProcessLockStats };

const histogramIndex = (ms: number): number => {
  let index = 0;
  while (index < LOCK_STATS_BOUNDS_MS.length && ms > LOCK_STATS_BOUNDS_MS[index]!) index++;
  return index;
};
const emptyBucket = (): LockStatsBucket => ({
  n: 0, waitMs: 0, waitMaxMs: 0, holdMs: 0, holdMaxMs: 0, timeouts: 0, tries: 0, failedWaitMs: 0,
  waitHist: new Array<number>(LOCK_STATS_BOUNDS_MS.length + 1).fill(0),
  holdHist: new Array<number>(LOCK_STATS_BOUNDS_MS.length + 1).fill(0),
});
const errorCodeOf = (error: unknown): unknown => (error as { code?: unknown } | undefined)?.code;
const roundMs = (_key: string, value: unknown): unknown =>
  typeof value === "number" && !Number.isInteger(value) ? Math.round(value * 1000) / 1000 : value;
const trimMinutes = (minutes: Map<number, unknown>, current: number): void => {
  // Keep current-60..current: the 60 complete minutes a query during `current` covers, plus current.
  for (const minute of minutes.keys()) if (minute < current - LOCK_STATS_RETAIN_MINUTES) minutes.delete(minute);
};
const lockStatsDisabled = (setting: string | undefined): boolean =>
  setting !== undefined && /^(?:0|off|false|no)$/i.test(setting.trim());

const UNSAFE_DIRECTORY = "FABRIC_LOCK_STATS_UNSAFE_DIRECTORY";
/** Refuse a lock-stats directory another local user could redirect or plant files in. */
const checkStatsDirectory = (directory: string): void => {
  const stat = fs.lstatSync(directory);
  const uid = process.platform === "win32" ? undefined : process.getuid?.();
  const reason = stat.isSymbolicLink() ? "is a symlink"
    : !stat.isDirectory() ? "is not a directory"
    : uid !== undefined && stat.uid !== uid ? `is owned by uid ${stat.uid}, not ${uid}`
    : uid !== undefined && (stat.mode & 0o022) !== 0 ? `is group- or other-writable (mode ${(stat.mode & 0o777).toString(8)})`
    : undefined;
  if (reason) throw Object.assign(new Error(`${directory} ${reason}`), { code: UNSAFE_DIRECTORY });
};
// No private log facility exists for the store: one line per refused root to the profile's
// private file (never stderr, which belongs to the TUI). Best effort; a missing profile skips it.
// The profile is resolved inline (as core/agent-dir.ts does): this module stays self-contained
// because every release generation loads its own copy.
const warnPrivately = (message: string): void => {
  try {
    const configured = process.env.PI_CODING_AGENT_DIR;
    const agentDir = configured ? configured.replace(/^~(?=$|[\\/])/, os.homedir()) : path.join(os.homedir(), ".pi", "agent");
    fs.appendFileSync(path.join(agentDir, "fabric-lock-stats.log"),
      `${new Date().toISOString()} pid ${process.pid}: ${message}\n`, { mode: 0o600 });
  } catch { /* Diagnostics only. */ }
};
const WRITE_FLAGS = process.platform === "win32" ? "wx"
  : fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0);

/** Sanitized host label used in file names. */
export const lockStatsHost = (): string => (os.hostname() || "host").replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 64);

/**
 * The process's mesh-lock recorder, captured once (including disabled) across module and
 * release reloads, as for commit stats: later generations reuse the first owner's recorder.
 */
export const createLockStats = (setting = process.env.PI_FABRIC_LOCK_STATS,
  options: { warn?: (message: string) => void } = {}): LockStats | undefined => {
  const existing = lockGlobals[lockKey];
  if (existing) return existing.stats;
  if (lockStatsDisabled(setting)) {
    lockGlobals[lockKey] = { version: 1, stats: undefined, flush() {}, dispose() {} };
    return undefined;
  }
  const host = lockStatsHost();
  const startedAt = Date.now();
  const roots = new Map<string, RootLockStats>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  let started = false;
  const warn = options.warn ?? warnPrivately;

  // By age only, the own file included: a fresh own file is never stale, and a stale one (no
  // acquisition here for a day) is safe to delete because the next write recreates it by rename.
  // ponytail: no sooner prune by pid liveness (smarty-dev#7826 review): pid reuse would race it, and
  // the reader skips files outside its window before the cap, so stale files no longer hide live ones.
  const prune = (directory: string, now: number): void => {
    checkStatsDirectory(directory);
    for (const name of fs.readdirSync(directory)) {
      if (!(name.endsWith(".json") || name.endsWith(".tmp"))) continue;
      const file = path.join(directory, name);
      try {
        const seen = fs.lstatSync(file);
        if (now - seen.mtimeMs <= LOCK_STATS_STALE_FILE_MS) continue;
        // ponytail: re-lstat right before the unlink and skip a file its owner refreshed
        // (rename = new inode, or a new mtime) since the age check. Best effort: a refresh
        // between this lstat and the unlink still loses that file, until its owner's next
        // write after an acquisition recreates it with the whole retained window.
        const again = fs.lstatSync(file);
        if (again.ino !== seen.ino || again.mtimeMs !== seen.mtimeMs) continue;
        fs.unlinkSync(file);
      } catch { /* Raced with its owner or another pruner. */ }
    }
  };
  const write = (entry: RootLockStats, now: number): void => {
    const directory = path.join(entry.root, LOCK_STATS_DIR);
    // Never recreate a removed mesh root: a missing parent fails with ENOENT.
    try { fs.mkdirSync(directory, { mode: 0o700 }); }
    catch (error) { if (errorCodeOf(error) !== "EEXIST") throw error; }
    // An existing name may be anyone's: write only into our own private directory.
    checkStatsDirectory(directory);
    trimMinutes(entry.minutes, Math.floor(now / 60_000));
    const name = `${host}-${process.pid}.json`;
    const file = path.join(directory, name);
    const body: LockStatsFile = {
      version: 1, host, pid: process.pid, root: entry.root, startedAt, updatedAt: now,
      minutes: [...entry.minutes].sort(([a], [b]) => a - b).map(([minute, classes]) => ({ minute, classes })),
    };
    // Atomic for readers; deliberately not durable (diagnostics, no fsync). A unique temporary,
    // created exclusively and never through a symlink, then renamed over the name (rename
    // replaces a planted symlink rather than following it).
    const temporary = `${file}.${randomBytes(6).toString("hex")}.tmp`;
    const descriptor = fs.openSync(temporary, WRITE_FLAGS, 0o600);
    try {
      try { fs.writeSync(descriptor, JSON.stringify(body, roundMs)); }
      finally { fs.closeSync(descriptor); }
      fs.renameSync(temporary, file);
    } catch (error) {
      try { fs.unlinkSync(temporary); } catch { /* Already renamed or gone. */ }
      throw error;
    }
  };
  const flush = (): void => {
    const now = Date.now();
    for (const [key, entry] of roots) {
      if (entry.disabled) continue;
      try {
        if (entry.dirty) {
          write(entry, now);
          entry.dirty = false;
        }
        // Hourly and read-only, also for a quiet root: never creates the directory.
        if (now - entry.prunedAt >= LOCK_STATS_PRUNE_EVERY_MS) {
          entry.prunedAt = now;
          prune(path.join(entry.root, LOCK_STATS_DIR), now);
        }
      } catch (error) {
        // A removed root is forgotten; an unsafe directory disables the root, once and quietly;
        // any other failure retries at the next flush.
        if (errorCodeOf(error) === UNSAFE_DIRECTORY) {
          entry.disabled = true;
          entry.minutes.clear();
          entry.dirty = false;
          warn(`mesh lock stats disabled for ${entry.root}: ${(error as Error).message}`);
        } else if (errorCodeOf(error) === "ENOENT" || errorCodeOf(error) === "ENOTDIR") roots.delete(key);
      }
    }
  };
  // Just after each wall-clock minute (spread by pid), so a reader sees every process's last
  // complete minute within a few seconds.
  const schedule = (): void => {
    timer = setTimeout(() => {
      flush();
      schedule();
    }, 60_000 - Date.now() % 60_000 + 500 + process.pid % 2_000);
    timer.unref?.();
  };
  const onExit = (): void => flush();
  // One bucket per real root, whatever the spelling (relative, trailing slash, symlink, case on
  // Windows): two buckets would write and overwrite the same <host>-<pid>.json and lose counts.
  // Computed once per spelling; the cache is bounded like the roots.
  const canonical = new Map<string, { key: string; root: string }>();
  const canonicalOf = (spelling: string): { key: string; root: string } => {
    let known = canonical.get(spelling);
    if (!known) {
      const resolved = path.resolve(spelling);
      let real = resolved;
      try { real = fs.realpathSync.native?.(resolved) ?? fs.realpathSync(resolved); } catch { /* Fall back to resolve. */ }
      known = { key: process.platform === "win32" ? real.toLowerCase() : real, root: real };
      if (canonical.size < LOCK_STATS_MAX_ROOTS * 8) canonical.set(spelling, known);
    }
    return known;
  };
  const bucketOf = (spelling: string, lockClass: MeshLockClass): LockStatsBucket | undefined => {
    const { key, root } = canonicalOf(spelling);
    let entry = roots.get(key);
    if (!entry) {
      if (roots.size >= LOCK_STATS_MAX_ROOTS) return undefined;
      entry = { root, minutes: new Map(), dirty: false, prunedAt: 0, disabled: false };
      roots.set(key, entry);
      if (!started) {
        started = true;
        schedule();
        process.once("exit", onExit);
      }
    }
    if (entry.disabled) return undefined;
    const minute = Math.floor(Date.now() / 60_000);
    let classes = entry.minutes.get(minute);
    if (!classes) {
      classes = Object.create(null) as Partial<Record<MeshLockClass, LockStatsBucket>>;
      entry.minutes.set(minute, classes);
      trimMinutes(entry.minutes, minute);
    }
    entry.dirty = true;
    return classes[lockClass] ??= emptyBucket();
  };
  const stats: LockStats = {
    acquired(root, lockClass, waitMs, holdMs) {
      const bucket = bucketOf(root, lockClass);
      if (!bucket) return;
      bucket.n++;
      bucket.waitMs += waitMs;
      bucket.holdMs += holdMs;
      if (waitMs > bucket.waitMaxMs) bucket.waitMaxMs = waitMs;
      if (holdMs > bucket.holdMaxMs) bucket.holdMaxMs = holdMs;
      bucket.waitHist[histogramIndex(waitMs)]!++;
      bucket.holdHist[histogramIndex(holdMs)]!++;
    },
    failed(root, lockClass, waitMs, tried) {
      const bucket = bucketOf(root, lockClass);
      if (!bucket) return;
      if (tried) bucket.tries++;
      else bucket.timeouts++;
      bucket.failedWaitMs += waitMs;
    },
  };
  const dispose = (): void => {
    if (timer) clearTimeout(timer);
    timer = undefined;
    started = false;
    process.removeListener("exit", onExit);
  };
  lockGlobals[lockKey] = { version: 1, stats, flush, dispose };
  return stats;
};

// ---------------------------------------------------------------------------------------------
// Reading and summarizing (fabric-mesh-lock-stats).

const isCount = (value: unknown): boolean => Number.isSafeInteger(value) && (value as number) >= 0;
const isDuration = (value: unknown): boolean => typeof value === "number" && Number.isFinite(value) && value >= 0;
const isHistogram = (value: unknown): boolean =>
  Array.isArray(value) && value.length === LOCK_STATS_BOUNDS_MS.length + 1 && value.every(isCount);
const isBucket = (value: unknown): value is LockStatsBucket => {
  const bucket = value as Record<string, unknown> | null;
  return !!bucket && typeof bucket === "object" && !Array.isArray(bucket) &&
    [bucket.n, bucket.timeouts, bucket.tries].every(isCount) &&
    [bucket.waitMs, bucket.waitMaxMs, bucket.holdMs, bucket.holdMaxMs, bucket.failedWaitMs].every(isDuration) &&
    isHistogram(bucket.waitHist) && isHistogram(bucket.holdHist);
};
/** Why a parsed stats file is invalid, or undefined. A file is used whole or not at all. */
const invalidLockStats = (value: unknown): string | undefined => {
  const file = value as Partial<LockStatsFile> | null;
  if (!file || typeof file !== "object" || file.version !== 1) return "not a version-1 stats file";
  if (typeof file.host !== "string" || !file.host || file.host.length > 256) return "bad host";
  if (!Number.isSafeInteger(file.pid) || file.pid! < 0) return "bad pid";
  if (!Array.isArray(file.minutes) || file.minutes.length > LOCK_STATS_RETAIN_MINUTES + 1) return "bad minutes";
  for (const minute of file.minutes as unknown[]) {
    const { minute: at, classes } = (minute ?? {}) as Partial<LockStatsMinute>;
    if (!isCount(at) || !classes || typeof classes !== "object" || Array.isArray(classes)) return "bad minute";
    for (const [lockClass, bucket] of Object.entries(classes)) {
      if (!LOCK_CLASSES.has(lockClass)) return "unknown lock class";
      if (!isBucket(bucket)) return `invalid ${lockClass} counters`;
    }
  }
  return undefined;
};
const READ_FLAGS = fs.constants.O_RDONLY | (process.platform === "win32" ? 0 : fs.constants.O_NOFOLLOW ?? 0);
const readRegularFile = (file: string): string => {
  // Regular files only, never through a symlink, and never more than the cap.
  if (!fs.lstatSync(file).isFile()) throw new Error("not a regular file");
  const descriptor = fs.openSync(file, READ_FLAGS);
  try {
    const stat = fs.fstatSync(descriptor);
    if (!stat.isFile()) throw new Error("not a regular file");
    if (stat.size > LOCK_STATS_MAX_FILE_BYTES) throw new Error(`larger than ${LOCK_STATS_MAX_FILE_BYTES} bytes`);
    const buffer = Buffer.alloc(stat.size);
    let read = 0;
    while (read < buffer.length) {
      const count = fs.readSync(descriptor, buffer, read, buffer.length - read, read);
      if (!count) break;
      read += count;
    }
    return buffer.toString("utf8", 0, read);
  } finally { fs.closeSync(descriptor); }
};

/** Start of a window of `span` complete minutes before `now`, the current minute included. */
const lockStatsWindowStartMs = (now: number, span: number): number => (Math.floor(now / 60_000) - span) * 60_000;

/**
 * Every process's file under `<root>/lock-stats` that can hold a minute of the window. Unreadable, oversized,
 * foreign or invalid files are skipped and named in `problems` (a gate must not pass on what it could not read).
 */
export const readLockStats = (root: string, problems: string[] = [],
  options: { minutes?: number; now?: number | undefined } = {}): LockStatsFile[] => {
  const directory = path.join(root, LOCK_STATS_DIR);
  let names: string[];
  try {
    if (!fs.lstatSync(directory).isDirectory()) {
      problems.push(`${directory}: not a directory`);
      return [];
    }
    names = fs.readdirSync(directory);
  } catch (error) { if (errorCodeOf(error) === "ENOENT") return []; throw error; }
  // A file is written after each minute it holds, so one modified before the window starts
  // holds no minute in it: skip it before the cap, and cap the newest (smarty-dev#7826).
  const span = Math.max(1, Math.min(LOCK_STATS_RETAIN_MINUTES, Math.floor(options.minutes ?? LOCK_STATS_RETAIN_MINUTES)));
  const since = lockStatsWindowStartMs(options.now ?? Date.now(), span + 1);
  const candidates: Array<{ name: string; mtimeMs: number }> = [];
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    try {
      const { mtimeMs } = fs.lstatSync(path.join(directory, name));
      if (mtimeMs >= since) candidates.push({ name, mtimeMs });
    } catch (error) {
      if (errorCodeOf(error) !== "ENOENT") problems.push(`${name}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  if (candidates.length > LOCK_STATS_MAX_FILES) {
    problems.push(`${directory}: ${candidates.length} stats files in the window, read only the newest ${LOCK_STATS_MAX_FILES}`);
    candidates.length = LOCK_STATS_MAX_FILES;
  }
  const files: LockStatsFile[] = [];
  for (const { name } of candidates) {
    try {
      const file = JSON.parse(readRegularFile(path.join(directory, name))) as unknown;
      const invalid = invalidLockStats(file);
      if (invalid) problems.push(`${name}: ${invalid}`);
      else files.push(file as LockStatsFile);
    } catch (error) {
      // Removed by a pruner since the listing: not a problem.
      if (errorCodeOf(error) !== "ENOENT") problems.push(`${name}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  return files;
};

export interface LockStatsTotals {
  n: number;
  holdMs: number;
  /** Share of the window's wall time, in percent. */
  busyPct: number;
  holdMeanMs: number;
  holdMaxMs: number;
  /** Upper bound of the histogram bucket holding the 99th percentile (Infinity above 10 s). */
  holdP99Ms: number;
  waitMeanMs: number;
  waitMaxMs: number;
  waitP99Ms: number;
  timeouts: number;
  tries: number;
}
export interface LockStatsSummary extends LockStatsTotals {
  root: string;
  /** Inclusive epoch minutes of the window (complete minutes only). */
  fromMinute: number;
  toMinute: number;
  minutes: number;
  processes: number;
  peakMinuteBusyPct: number;
  classes: Array<LockStatsTotals & { lockClass: MeshLockClass; holdSharePct: number }>;
  pids: Array<LockStatsTotals & { host: string; pid: number; topClass: MeshLockClass | undefined }>;
}

const mergeBucket = (into: LockStatsBucket, from: LockStatsBucket): void => {
  into.n += from.n;
  into.waitMs += from.waitMs;
  into.holdMs += from.holdMs;
  into.waitMaxMs = Math.max(into.waitMaxMs, from.waitMaxMs);
  into.holdMaxMs = Math.max(into.holdMaxMs, from.holdMaxMs);
  into.timeouts += from.timeouts;
  into.tries += from.tries;
  into.failedWaitMs += from.failedWaitMs;
  for (let index = 0; index < into.waitHist.length; index++) {
    into.waitHist[index]! += from.waitHist[index] ?? 0;
    into.holdHist[index]! += from.holdHist[index] ?? 0;
  }
};
const percentileBound = (histogram: readonly number[], fraction: number): number => {
  const total = histogram.reduce((sum, count) => sum + count, 0);
  if (!total) return 0;
  let seen = 0;
  for (let index = 0; index < histogram.length; index++) {
    seen += histogram[index]!;
    if (seen >= Math.ceil(total * fraction)) return LOCK_STATS_BOUNDS_MS[index] ?? Number.POSITIVE_INFINITY;
  }
  return Number.POSITIVE_INFINITY;
};
const totalsOf = (bucket: LockStatsBucket, windowMs: number): LockStatsTotals => ({
  n: bucket.n,
  holdMs: bucket.holdMs,
  busyPct: windowMs > 0 ? bucket.holdMs / windowMs * 100 : 0,
  holdMeanMs: bucket.n ? bucket.holdMs / bucket.n : 0,
  holdMaxMs: bucket.holdMaxMs,
  holdP99Ms: percentileBound(bucket.holdHist, 0.99),
  waitMeanMs: bucket.n ? bucket.waitMs / bucket.n : 0,
  waitMaxMs: bucket.waitMaxMs,
  waitP99Ms: percentileBound(bucket.waitHist, 0.99),
  timeouts: bucket.timeouts,
  tries: bucket.tries,
});

/** Fleet view of the last `minutes` complete wall-clock minutes before `now`. */
export const summarizeLockStats = (root: string, files: readonly LockStatsFile[],
  options: { minutes?: number; now?: number | undefined; top?: number } = {}): LockStatsSummary => {
  const span = Math.max(1, Math.min(LOCK_STATS_RETAIN_MINUTES, Math.floor(options.minutes ?? 10)));
  const toMinute = Math.floor((options.now ?? Date.now()) / 60_000) - 1;
  const fromMinute = toMinute - span + 1;
  const windowMs = span * 60_000;
  const top = Math.max(1, Math.floor(options.top ?? 5));
  const all = emptyBucket();
  const byClass = new Map<MeshLockClass, LockStatsBucket>();
  const perMinuteHold = new Map<number, number>();
  const byPid: Array<{ host: string; pid: number; bucket: LockStatsBucket; classes: Map<MeshLockClass, number> }> = [];
  for (const file of files) {
    const own = { host: file.host, pid: file.pid, bucket: emptyBucket(), classes: new Map<MeshLockClass, number>() };
    for (const { minute, classes } of file.minutes) {
      if (minute < fromMinute || minute > toMinute) continue;
      for (const [lockClass, bucket] of Object.entries(classes) as Array<[MeshLockClass, LockStatsBucket]>) {
        mergeBucket(all, bucket);
        mergeBucket(own.bucket, bucket);
        let classBucket = byClass.get(lockClass);
        if (!classBucket) byClass.set(lockClass, classBucket = emptyBucket());
        mergeBucket(classBucket, bucket);
        own.classes.set(lockClass, (own.classes.get(lockClass) ?? 0) + bucket.holdMs);
        perMinuteHold.set(minute, (perMinuteHold.get(minute) ?? 0) + bucket.holdMs);
      }
    }
    if (own.bucket.n || own.bucket.timeouts || own.bucket.tries) byPid.push(own);
  }
  const classes = [...byClass].map(([lockClass, bucket]) => ({
    lockClass, ...totalsOf(bucket, windowMs), holdSharePct: all.holdMs > 0 ? bucket.holdMs / all.holdMs * 100 : 0,
  })).sort((a, b) => b.holdMs - a.holdMs || b.n - a.n || a.lockClass.localeCompare(b.lockClass));
  const pids = byPid.map(({ host, pid, bucket, classes: held }) => ({
    host, pid, ...totalsOf(bucket, windowMs),
    topClass: [...held].sort((a, b) => b[1] - a[1])[0]?.[0],
  })).sort((a, b) => b.holdMs - a.holdMs || b.n - a.n || a.pid - b.pid).slice(0, top);
  return {
    root, fromMinute, toMinute, minutes: span, processes: byPid.length, ...totalsOf(all, windowMs),
    peakMinuteBusyPct: Math.max(0, ...perMinuteHold.values()) / 60_000 * 100,
    classes, pids,
  };
};
