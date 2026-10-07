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
// small file per process, rewritten by an atomic rename, holding at most the last hour. No extra
// lock, no fsync, and nothing at all before the first acquisition. `fabric-mesh-lock-stats`
// sums every process's file into fleet busy %, timeouts and the classes and pids that hold it.
// On by default; PI_FABRIC_LOCK_STATS=0 (or off/false/no) disables it for the process.

/** Who took the mesh lock: the store entry point, never a stack walk. */
export type MeshLockClass = "publish" | "put/delete" | "writeBatch" | "heartbeat/confirm" | "custody" | "bridge" | "other";

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
export const LOCK_STATS_RETAIN_MINUTES = 60;
const LOCK_STATS_MAX_ROOTS = 32;
const LOCK_STATS_STALE_FILE_MS = 24 * 60 * 60_000;
const LOCK_STATS_PRUNE_EVERY_MS = 60 * 60_000;

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
interface RootLockStats { root: string; minutes: Map<number, Partial<Record<MeshLockClass, LockStatsBucket>>>; dirty: boolean; prunedAt: number }
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
  for (const minute of minutes.keys()) if (minute <= current - LOCK_STATS_RETAIN_MINUTES) minutes.delete(minute);
};
const lockStatsDisabled = (setting: string | undefined): boolean =>
  setting !== undefined && /^(?:0|off|false|no)$/i.test(setting.trim());

/** Sanitized host label used in file names. */
export const lockStatsHost = (): string => (os.hostname() || "host").replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 64);

/**
 * The process's mesh-lock recorder, captured once (including disabled) across module and
 * release reloads, as for commit stats: later generations reuse the first owner's recorder.
 */
export const createLockStats = (setting = process.env.PI_FABRIC_LOCK_STATS): LockStats | undefined => {
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

  const prune = (directory: string, own: string, now: number): void => {
    for (const name of fs.readdirSync(directory)) {
      if (name === own || !(name.endsWith(".json") || name.endsWith(".tmp"))) continue;
      const file = path.join(directory, name);
      try {
        if (now - fs.statSync(file).mtimeMs > LOCK_STATS_STALE_FILE_MS) fs.rmSync(file, { force: true });
      } catch { /* Raced with its owner or another pruner. */ }
    }
  };
  const write = (entry: RootLockStats, now: number): void => {
    const directory = path.join(entry.root, LOCK_STATS_DIR);
    // Never recreate a removed mesh root: a missing parent fails with ENOENT.
    try { fs.mkdirSync(directory, { mode: 0o700 }); }
    catch (error) { if (errorCodeOf(error) !== "EEXIST") throw error; }
    trimMinutes(entry.minutes, Math.floor(now / 60_000));
    const name = `${host}-${process.pid}.json`;
    const file = path.join(directory, name);
    const body: LockStatsFile = {
      version: 1, host, pid: process.pid, root: entry.root, startedAt, updatedAt: now,
      minutes: [...entry.minutes].sort(([a], [b]) => a - b).map(([minute, classes]) => ({ minute, classes })),
    };
    // Atomic for readers; deliberately not durable (diagnostics, no fsync).
    fs.writeFileSync(`${file}.tmp`, JSON.stringify(body, roundMs), { mode: 0o600 });
    fs.renameSync(`${file}.tmp`, file);
    if (now - entry.prunedAt >= LOCK_STATS_PRUNE_EVERY_MS) {
      entry.prunedAt = now;
      prune(directory, name, now);
    }
  };
  const flush = (): void => {
    const now = Date.now();
    for (const [key, entry] of roots) {
      if (!entry.dirty) continue;
      try {
        write(entry, now);
        entry.dirty = false;
      } catch (error) {
        // A removed root is forgotten; any other failure retries at the next flush.
        if (errorCodeOf(error) === "ENOENT" || errorCodeOf(error) === "ENOTDIR") roots.delete(key);
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
  const bucketOf = (root: string, lockClass: MeshLockClass): LockStatsBucket | undefined => {
    let entry = roots.get(root);
    if (!entry) {
      if (roots.size >= LOCK_STATS_MAX_ROOTS) return undefined;
      entry = { root: path.resolve(root), minutes: new Map(), dirty: false, prunedAt: 0 };
      roots.set(root, entry);
      if (!started) {
        started = true;
        schedule();
        process.once("exit", onExit);
      }
    }
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

const isBucket = (value: unknown): value is LockStatsBucket => {
  const bucket = value as LockStatsBucket | undefined;
  return !!bucket && typeof bucket === "object" && [bucket.n, bucket.waitMs, bucket.waitMaxMs, bucket.holdMs, bucket.holdMaxMs,
    bucket.timeouts, bucket.tries].every(Number.isFinite) && Array.isArray(bucket.waitHist) && Array.isArray(bucket.holdHist);
};

/** Every process's file under `<root>/lock-stats`; unreadable or foreign files are skipped. */
export const readLockStats = (root: string): LockStatsFile[] => {
  const directory = path.join(root, LOCK_STATS_DIR);
  let names: string[];
  try { names = fs.readdirSync(directory); }
  catch (error) { if (errorCodeOf(error) === "ENOENT") return []; throw error; }
  const files: LockStatsFile[] = [];
  for (const name of names.filter(name => name.endsWith(".json")).sort()) {
    try {
      const file = JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as LockStatsFile;
      if (file?.version !== 1 || typeof file.host !== "string" || !Number.isSafeInteger(file.pid) ||
        !Array.isArray(file.minutes)) continue;
      file.minutes = file.minutes.filter(minute => Number.isSafeInteger(minute?.minute) && minute.classes &&
        typeof minute.classes === "object");
      for (const minute of file.minutes) {
        for (const [lockClass, bucket] of Object.entries(minute.classes)) {
          if (!isBucket(bucket)) delete minute.classes[lockClass as MeshLockClass];
        }
      }
      files.push(file);
    } catch { /* Torn by an older writer or unrelated: skip. */ }
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
  into.failedWaitMs += from.failedWaitMs ?? 0;
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
