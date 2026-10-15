import { randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { verifiedRemove } from "../storage/verified-remove.js";

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
  // Named maintenance exception: opt-in PI_FABRIC_COMMIT_STATS diagnostic sampler.
  // Its unref'd minute boundary (including zero samples) is the public sink contract.
  // Disabled by default, so normal file stores/bridges allocate no sampler timer.
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
// written on the next acquisition after a minute boundary (and at exit/explicit flush) to
// `<root>/lock-stats/<host>-<pid>.json`: one
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
/**
 * What kind of process wrote a stats file (smarty-dev#8305), captured once at the recorder's first
 * acquisition and never re-derived: an exited writer stays attributable from its file alone. Each
 * field is a separate source fact, present only when its source was set; nothing is inferred.
 */
export interface LockStatsWriter {
  /** Basename of process.argv[1] (the script the runtime was started with). */
  argv1?: string;
  /** PI_FABRIC_AGENT_NAME. */
  agentName?: string;
  /** PI_FABRIC_ACTOR_ID. */
  actorId?: string;
  /** PI_FABRIC_ACTOR_NAME. */
  actorName?: string;
  /** PI_FABRIC_ROLE, verbatim. */
  role?: string;
  /** SMARTY_ROLE, verbatim (may carry an "@revision" suffix). */
  smartyRole?: string;
  /** process.ppid at capture. */
  ppid?: number;
}
const WRITER_STRING_FIELDS = ["argv1", "agentName", "actorId", "actorName", "role", "smartyRole"] as const;
/** Writer strings are truncated to this many UTF-16 units when captured; a reader rejects longer. */
export const LOCK_STATS_WRITER_MAX_CHARS = 128;
export interface LockStatsFile {
  version: 1;
  host: string;
  pid: number;
  root: string;
  startedAt: number;
  updatedAt: number;
  /** Optional in version 1: files written before smarty-dev#8305 have none. */
  writer?: LockStatsWriter;
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

/** The writer facts of this process, read once from its argv, environment and ppid. */
export const captureLockStatsWriter = (env: NodeJS.ProcessEnv = process.env,
  argv1: string | undefined = process.argv[1], ppid: number = process.ppid): LockStatsWriter => {
  const bounded = (value: string | undefined): string | undefined => {
    const text = value?.trim();
    return text ? text.slice(0, LOCK_STATS_WRITER_MAX_CHARS) : undefined;
  };
  const facts: Record<string, string | number | undefined> = {
    argv1: bounded(argv1 ? path.basename(argv1) : undefined),
    agentName: bounded(env.PI_FABRIC_AGENT_NAME),
    actorId: bounded(env.PI_FABRIC_ACTOR_ID),
    actorName: bounded(env.PI_FABRIC_ACTOR_NAME),
    role: bounded(env.PI_FABRIC_ROLE),
    smartyRole: bounded(env.SMARTY_ROLE),
    ppid: Number.isSafeInteger(ppid) && ppid >= 0 ? ppid : undefined,
  };
  return Object.freeze(Object.fromEntries(Object.entries(facts).filter(([, value]) => value !== undefined)) as LockStatsWriter);
};

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
  let flushedMinute = Math.floor(startedAt / 60_000);
  let started = false;
  // Captured once, at the first acquisition (with the exit hook): later env changes never alter it.
  let writer: LockStatsWriter | undefined;
  const warn = options.warn ?? warnPrivately;

  // By age only, the own file included: a fresh own file is never stale, and a stale one (no
  // acquisition here for a day) is safe to delete because the next write recreates it by rename.
  // ponytail: no sooner prune by pid liveness (smarty-dev#7826 review): pid reuse would race it, and
  // the reader skips files outside its window before the cap, so stale files no longer hide live ones.
  const prune = (directory: string, now: number): void => {
    checkStatsDirectory(directory);
    for (const name of fs.readdirSync(directory)) {
      // A `.prune-<pid>` name is a pruner's graveyard; one left by a crash or failed relink is pruned by age too.
      if (!(name.endsWith(".json") || name.endsWith(".tmp") || /\.prune-\d+$/.test(name))) continue;
      const file = path.join(directory, name);
      try {
        const seen = fs.lstatSync(file);
        // Files only, as main's unlink: a directory (a verifiedRemove graveyard or anything else) is never removed
        // recursively here; graveyards (`.retention-graveyard-<pid>`) do not match the names above either.
        if (!seen.isFile() || now - seen.mtimeMs <= LOCK_STATS_STALE_FILE_MS) continue;
        // Temporaries and graveyards have unique names that no writer refreshes. smarty-dev#8907: only the inode
        // judged aged above is removed; off Linux main's unlink.
        if (!name.endsWith(".json")) {
          verifiedRemove(file, seen, { deleter: "lockStatsPrune", fallback: () => { fs.unlinkSync(file); return true; } });
          continue;
        }
        // Never unlink a stats file by name (smarty-dev#7879): its writer may refresh it by rename
        // at any time. Move it aside atomically (readers skip non-.json names), then delete it only
        // if it is still the aged file. Otherwise the writer's fresh file was moved: put it back with
        // an atomic no-replace link (EEXIST when an even newer file holds the name: keep that one;
        // works on NTFS too), then drop the graveyard name only after restoration or EEXIST.
        const grave = `${file}.prune-${process.pid}`;
        fs.renameSync(file, grave);
        const moved = fs.lstatSync(grave);
        if (moved.dev !== seen.dev || moved.ino !== seen.ino || moved.mtimeMs !== seen.mtimeMs) {
          try { fs.linkSync(grave, file); } catch (error) {
            // Keep the fresh graveyard for age pruning unless a newer file holds the name.
            if (errorCodeOf(error) !== "EEXIST") continue;
          }
          // ponytail: not the aged inode: our per-pid graveyard name (no writer writes it) only drops the extra link of
          // the refreshed file put back above, or one a newer file at the name superseded (main's behaviour).
          fs.unlinkSync(grave);
        } else {
          // smarty-dev#8907: the judged (aged) stat, never the refreshed lstat: a refusal leaves the graveyard name,
          // pruned by age later. Off Linux main's unlink.
          verifiedRemove(grave, seen, { deleter: "lockStatsPrune", fallback: () => { fs.unlinkSync(grave); return true; } });
        }
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
      ...writer ? { writer } : {},
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
      // ponytail: our own unpublished temporary (random name, O_EXCL|O_NOFOLLOW above), removed only after its write or
      // rename failed; nothing judged it.
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
        // At most hourly, during actual diagnostic work: never creates the directory.
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
  // A minute boundary alone is not work: even one startup sample must not wake an
  // otherwise empty bridge 60 s later. Flush on the next real record in a newer minute
  // (and at exit/explicit flush). Quiet roots are pruned only during that actual work.
  const flushIfDue = (): void => {
    const minute = Math.floor(Date.now() / 60_000);
    if (minute <= flushedMinute) return;
    flushedMinute = minute;
    flush();
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
        writer ??= captureLockStatsWriter();
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
      flushIfDue();
    },
    failed(root, lockClass, waitMs, tried) {
      const bucket = bucketOf(root, lockClass);
      if (!bucket) return;
      if (tried) bucket.tries++;
      else bucket.timeouts++;
      bucket.failedWaitMs += waitMs;
      flushIfDue();
    },
  };
  const dispose = (): void => {
    started = false;
    process.removeListener("exit", onExit);
  };
  lockGlobals[lockKey] = { version: 1, stats, flush, dispose };
  return stats;
};

// ---------------------------------------------------------------------------------------------
// Mesh-lock events at the source (smarty-dev#9290).
//
// The minute files above say how busy the lock was; they cannot say which hold made a waiter give
// up, so commswatch sampled `.lock` every 50 ms to catch it. Instead the two parties report it
// themselves, to `<root>/lock-stats/events.jsonl`: the holder when it releases after holding
// longer than a threshold (`held`), the waiter when it gives up with the typed lock timeout
// (`wait-timeout`, FABRIC_MESH_STATE_BUSY at the bridge), naming the holder it could identify.
// A FIFO refusal before canonical acquisition is a separate `pre-lock` event (FABRIC_MESH_LOCK_QUEUE_EXPIRED), never a
// real-contention timeout in these events or the minute counters. It has no identified lock holder.
// It is a local append, never a mesh publish: publishing would need the very lock that is
// contended. One O_APPEND write() per line, each line at most PIPE_BUF (4096) bytes, so concurrent
// appenders never interleave. At 1 MiB the file is renamed to `events.jsonl.1` (one old file kept),
// under a short O_EXCL rotation lock and only if the name still holds the oversized inode.
// Windows: rotation is disabled (scope cut, smarty-dev#10191). Concurrent appenders across a forced
// rotation lost ~46% of lines on Windows CI with no counted error, cause not found; there the file
// only grows, and one warning per process says so once it passes the rotation size.
// An I/O error never blocks or throws into the lock path: it is counted, and warned about at most
// once a minute. Off with the minute files (PI_FABRIC_LOCK_STATS=0); `held` is also off with a
// threshold of 0 (`lockHeldEventMs`). Bounded tries (registry-fenced or zero-wait callers and the
// R20 write fence) fail by design while the lock is busy: they stay `tries` in the minute files.

export type LockEventStore = "sqlite" | "mesh-lock";
/** Who held the lock. A wait-timeout names the holder only as far as the lock's record shows it. */
export interface LockEventHolder {
  pid: number | null;
  /** The process's short name (/proc/PID/comm), never its arguments, which may hold secrets. */
  process: string | null;
  /** The holder's caller class: known for `held` (the holder itself), null on `wait-timeout`. */
  op: MeshLockClass | null;
  /** Sanitized host label (both locks live on a local filesystem). */
  host: string;
}
export interface LockEventWaiter { pid: number; process: string; op: MeshLockClass }
export interface LockEvent {
  v: 1;
  /** Epoch ms of the release (`held`), contention timeout, or pre-lock refusal. */
  at: number;
  kind: "held" | "wait-timeout" | "pre-lock";
  store: LockEventStore;
  class: MeshLockClass;
  /** `held` only. */
  heldMs?: number;
  /** `wait-timeout` or `pre-lock`. */
  waitedMs?: number;
  /** `pre-lock` only: a refusal before canonical lock acquisition, never contention. */
  preLockCode?: "FABRIC_MESH_LOCK_QUEUE_EXPIRED";
  /** `pre-lock` only: observed FIFO receipts ahead, if available. */
  waitersAhead?: number;
  holder: LockEventHolder;
  /** `wait-timeout` or `pre-lock`. */
  waiter?: LockEventWaiter;
}
export interface LockEvents {
  /** A release after a hold longer than the caller's threshold (the caller compares). */
  held(root: string, store: LockEventStore, lockClass: MeshLockClass, heldMs: number): void;
  /** A full-budget acquisition that gave up; `holderPid` from the lock's owner record, if readable. */
  waitTimeout(root: string, store: LockEventStore, lockClass: MeshLockClass, waitedMs: number,
    holderPid: number | undefined): void;
  /** FIFO refusal before admission; separate from the real-contention event and minute counters. */
  queueExpired(root: string, store: LockEventStore, lockClass: MeshLockClass, waitedMs: number,
    waitersAhead: number | undefined): void;
  /** Appends that failed (never thrown into the lock path). */
  readonly errors: number;
}

export const LOCK_EVENTS_FILE = "events.jsonl";
/** At this size the file is renamed to `events.jsonl.1`, replacing the older one. */
export const LOCK_EVENTS_ROTATE_BYTES = 1024 * 1024;
/** The follow-up issue that re-enables rotation on Windows, where it is disabled until then. */
export const LOCK_EVENTS_WIN32_ROTATION_ISSUE = "smarty-dev#10191";
/** Exclusive (O_EXCL) rotation lock beside the file: one rotator at a time across processes. */
export const LOCK_EVENTS_ROTATE_LOCK = `${LOCK_EVENTS_FILE}.rotating`;
/** A rotation lock older than this was left by a crashed rotator (a rotation takes microseconds). */
export const LOCK_EVENTS_ROTATE_LOCK_STALE_MS = 10_000;
/** PIPE_BUF: an O_APPEND write() of at most this many bytes never interleaves with another. */
export const LOCK_EVENT_MAX_LINE_BYTES = 4096;
/** Default `lockHeldEventMs`: a hold longer than this emits a `held` event; 0 disables them. */
export const LOCK_HELD_EVENT_MS = 1_000;
/** The effective `lockHeldEventMs`: undefined or not finite means the default; negative means 0 (off). */
export const lockHeldEventThreshold = (value: number | undefined): number =>
  value === undefined || !Number.isFinite(value) ? LOCK_HELD_EVENT_MS : Math.max(0, value);
const LOCK_EVENTS_WARN_EVERY_MS = 60_000;
const PROCESS_NAME_MAX_CHARS = LOCK_STATS_WRITER_MAX_CHARS;
const APPEND_FLAGS = process.platform === "win32" ? "a"
  : fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT | (fs.constants.O_NOFOLLOW ?? 0);
const ROTATE_LOCK_FLAGS = process.platform === "win32" ? "wx"
  : fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0);
/**
 * Windows: a rename, open or lstat that races another process's handle on events.jsonl (or a name
 * left delete-pending by a rename) fails transiently with one of these. Never a lost line: a refused
 * rename skips the rotation, a refused append retries.
 */
const WIN32_TRANSIENT_CODES: ReadonlySet<unknown> = new Set(["EPERM", "EBUSY", "EACCES"]);
/** Windows: tries per line against the current events.jsonl before the append counts as failed. */
export const LOCK_EVENTS_APPEND_TRIES_WIN32 = 5;
/** Windows: the backoff before retry N is N times this (10, 20, 30, 40 ms: at most 100 ms per line). */
export const LOCK_EVENTS_APPEND_RETRY_MS = 10;
/** A synchronous bounded sleep (the appender is synchronous inside the lock path). */
const sleepSync = (ms: number): void => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); };

interface ProcessLockEvents {
  /** Registry generation. Keep existing methods/line shapes stable; explicitly hand over additive APIs. */
  version: number;
  events: LockEvents | undefined;
  dispose(): void;
}
const eventsKey = Symbol.for("pi-fabric.mesh.lock-events");
const eventsGlobals = globalThis as typeof globalThis & { [eventsKey]?: ProcessLockEvents };

/** A process's short name: Linux /proc/PID/comm (never cmdline), else undefined. */
export const lockEventProcessName = (pid: number): string | undefined => {
  if (process.platform !== "linux" || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
  try {
    const name = fs.readFileSync(`/proc/${pid}/comm`, "utf8").trim();
    return name ? name.slice(0, PROCESS_NAME_MAX_CHARS) : undefined;
  } catch { return undefined; }
};

/** One JSONL line of at most LOCK_EVENT_MAX_LINE_BYTES: process names shrink until it fits. */
export const lockEventLine = (event: LockEvent): string => {
  let line = `${JSON.stringify(event)}\n`;
  let room = PROCESS_NAME_MAX_CHARS;
  while (Buffer.byteLength(line) > LOCK_EVENT_MAX_LINE_BYTES && room > 0) {
    room = Math.floor(room / 2);
    const holder = { ...event.holder, process: event.holder.process === null ? null : event.holder.process.slice(0, room) };
    const waiter = event.waiter ? { waiter: { ...event.waiter, process: event.waiter.process.slice(0, room) } } : {};
    line = `${JSON.stringify({ ...event, holder, ...waiter })}\n`;
  }
  return line;
};

/**
 * The process's lock-event appender, captured once (including disabled) across module and release
 * reloads like the recorder: on unless PI_FABRIC_LOCK_STATS=0. No timer, file or descriptor is held.
 */
export const createLockEvents = (setting = process.env.PI_FABRIC_LOCK_STATS,
  options: {
    warn?: (message: string) => void;
    rotateBytes?: number;
    /** Tests: the platform whose rename/append semantics apply (default process.platform). */
    platform?: NodeJS.Platform;
    /** Tests: the synchronous backoff sleep (default Atomics.wait). */
    sleep?: (ms: number) => void;
  } = {}): LockEvents | undefined => {
  const existing = eventsGlobals[eventsKey];
  if (existing && (!existing.events || (existing.version >= 3 && typeof existing.events.queueExpired === "function"))) return existing.events;
  if (!existing && lockStatsDisabled(setting)) {
    eventsGlobals[eventsKey] = { version: 1, events: undefined, dispose() {} };
    return undefined;
  }
  const host = lockStatsHost();
  const rotateBytes = Math.max(1, options.rotateBytes ?? LOCK_EVENTS_ROTATE_BYTES);
  const warn = options.warn ?? warnPrivately;
  const windows = (options.platform ?? process.platform) === "win32";
  const sleep = options.sleep ?? sleepSync;
  const transient = (error: unknown): boolean => windows && WIN32_TRANSIENT_CODES.has(errorCodeOf(error));
  let errors = 0;
  let unwarned = 0;
  let warnedAt = Number.NEGATIVE_INFINITY;
  let warnedUnrotated = false;
  let ownName: string | undefined;
  const self = (): string => ownName ??= lockEventProcessName(process.pid)
    ?? (process.title.trim().slice(0, PROCESS_NAME_MAX_CHARS) || "unknown");
  const failed = (error: unknown): void => {
    errors++;
    unwarned++;
    const now = Date.now();
    if (now - warnedAt < LOCK_EVENTS_WARN_EVERY_MS) return;
    warnedAt = now;
    const count = unwarned;
    unwarned = 0;
    try { warn(`mesh lock events: ${count} append error(s) since the last warning; last: ${error instanceof Error ? error.message : String(error)}`); }
    catch { /* Diagnostics only. */ }
  };
  // BigInt stats: a Windows (NTFS) file id is 64 bits and above 2^53 once its sequence number
  // passes 31, so as a Number two different files can compare equal (nodejs/node#12115).
  const open = (file: string): { descriptor: number; stat: fs.BigIntStats } => {
    const descriptor = fs.openSync(file, APPEND_FLAGS, 0o600);
    try {
      const stat = fs.fstatSync(descriptor, { bigint: true });
      if (!stat.isFile()) throw new Error(`${file} is not a regular file`);
      return { descriptor, stat };
    } catch (error) { fs.closeSync(descriptor); throw error; }
  };
  const append = (root: string, event: LockEvent): void => {
    try {
      const directory = path.join(root, LOCK_STATS_DIR);
      // Never recreate a removed mesh root: a missing parent fails with ENOENT (counted).
      try { fs.mkdirSync(directory, { mode: 0o700 }); }
      catch (error) { if (errorCodeOf(error) !== "EEXIST") throw error; }
      checkStatsDirectory(directory);
      const file = path.join(directory, LOCK_EVENTS_FILE);
      const data = Buffer.from(lockEventLine(event));
      // A concurrent rotation moved the name between our open and our rename check (ENOENT): append
      // once more against the current events.jsonl (O_APPEND|O_CREAT). Windows: also EPERM/EBUSY/
      // EACCES while another process holds or renames the file, up to LOCK_EVENTS_APPEND_TRIES_WIN32
      // tries with a short growing backoff. The last failure is counted.
      const tries = windows ? LOCK_EVENTS_APPEND_TRIES_WIN32 : 2;
      for (let attempt = 1; attempt <= tries; attempt++) {
        try { appendOnce(file, data); return; }
        catch (error) {
          if (attempt === tries || (errorCodeOf(error) !== "ENOENT" && !transient(error))) throw error;
          if (windows) sleep(LOCK_EVENTS_APPEND_RETRY_MS * attempt);
        }
      }
    } catch (error) { failed(error); }
  };
  /** Takes the rotation lock, breaking one a crashed rotator left; undefined when another holds it. */
  /** Off Linux main's unlink; ENOENT (another breaker removed it) counts as gone, as main ignored it. */
  const unlinkGone = (lock: string) => (): boolean => {
    try { fs.unlinkSync(lock); } catch (error) { if (errorCodeOf(error) !== "ENOENT") throw error; }
    return true;
  };
  const takeRotateLock = (lock: string): number | undefined => {
    for (let attempt = 0; attempt < 2; attempt++) {
      try { return fs.openSync(lock, ROTATE_LOCK_FLAGS, 0o600); }
      catch (error) {
        if (errorCodeOf(error) !== "EEXIST" || attempt > 0) return undefined;
        try {
          // smarty-dev#8907: break only the lock judged stale: a fresh lock swapped in since then stays (refused: skip).
          const seen = fs.lstatSync(lock);
          // A non-file is never broken (main's unlink refused it; no recursive removal).
          if (!seen.isFile() || Date.now() - seen.mtimeMs <= LOCK_EVENTS_ROTATE_LOCK_STALE_MS) return undefined;
          if (!verifiedRemove(lock, seen, { deleter: "lockEventsRotateLock", fallback: unlinkGone(lock) }).removed) return undefined;
        } catch (stale) { if (errorCodeOf(stale) !== "ENOENT") return undefined; }
      }
    }
    return undefined;
  };
  /**
   * Renames the oversized file `seen` to `.1` under the rotation lock, only if the name still holds
   * that inode: an appender holding a stale lstat can never rename a newer file over `.1`. Busy lock:
   * skip (the holder rotates; this line lands in whichever file the name holds next). The inode is
   * compared exactly (BigInt), and the name must still hold at least the bytes seen: a reused or
   * colliding file id on a smaller, newer file never renames it over `.1`. Windows: a rename (or
   * lstat) refused because another process holds the file skips this rotation; the file stays
   * oversized until a later rotation succeeds.
   */
  const rotate = (file: string, seen: fs.BigIntStats): void => {
    const lock = path.join(path.dirname(file), LOCK_EVENTS_ROTATE_LOCK);
    const held = takeRotateLock(lock);
    if (held === undefined) return;
    let own: fs.Stats | undefined;
    try {
      own = fs.fstatSync(held);
      let current: fs.BigIntStats;
      try { current = fs.lstatSync(file, { bigint: true }); }
      catch (error) { if (errorCodeOf(error) === "ENOENT" || transient(error)) return; throw error; }
      if (current.dev !== seen.dev || current.ino !== seen.ino || current.size < seen.size) return;
      // ponytail: rotation, not a judged delete: the one previous generation `.1` is replaced by design, under the
      // rotation lock, and only by the exact (BigInt) inode checked just above.
      try { fs.renameSync(file, `${file}.1`); }
      catch (error) { if (!transient(error)) throw error; }
    } finally {
      try { fs.closeSync(held); } catch { /* Closed either way. */ }
      // smarty-dev#8907: our own lock inode only (never written, so its mtime is the created one): a stale-lock breaker
      // may have removed it and another rotator taken the name, whose lock stays.
      verifiedRemove(lock, own, { deleter: "lockEventsRotateLock", fallback: unlinkGone(lock) });
    }
  };
  /** Windows: rotation is disabled; the first time the file passes the rotation size, warn once. */
  const unrotated = (file: string): void => {
    if (warnedUnrotated) return;
    warnedUnrotated = true;
    try {
      warn(`mesh lock events: ${file} passed the ${rotateBytes}-byte rotation size; rotation is disabled on Windows `
        + `until ${LOCK_EVENTS_WIN32_ROTATION_ISSUE}, so the file keeps growing`);
    } catch { /* Diagnostics only. */ }
  };
  const appendOnce = (file: string, data: Buffer): void => {
    let { descriptor, stat } = open(file);
    try {
      const oversized = stat.size > 0n && stat.size + BigInt(data.length) > BigInt(rotateBytes);
      // Windows: append only (no rename, no rotation lock); see LOCK_EVENTS_WIN32_ROTATION_ISSUE.
      if (oversized && windows) unrotated(file);
      else if (oversized) {
        // Closed first, so the rename also works on Windows.
        fs.closeSync(descriptor);
        descriptor = -1;
        rotate(file, stat);
        ({ descriptor, stat } = open(file));
      }
      // ONE write per line, appended atomically by O_APPEND.
      const written = fs.writeSync(descriptor, data);
      if (written !== data.length) throw new Error(`short append to ${file} (${written} of ${data.length} bytes)`);
    } finally { if (descriptor >= 0) fs.closeSync(descriptor); }
  };
  const events: LockEvents = {
    held(root, store, lockClass, heldMs) {
      append(root, { v: 1, at: Date.now(), kind: "held", store, class: lockClass, heldMs: Math.round(heldMs),
        holder: { pid: process.pid, process: self(), op: lockClass, host } });
    },
    waitTimeout(root, store, lockClass, waitedMs, holderPid) {
      const pid = holderPid !== undefined && Number.isSafeInteger(holderPid) && holderPid > 0 ? holderPid : null;
      const name = pid === null ? null : pid === process.pid ? self() : lockEventProcessName(pid) ?? null;
      append(root, { v: 1, at: Date.now(), kind: "wait-timeout", store, class: lockClass, waitedMs: Math.round(waitedMs),
        holder: { pid, process: name, op: null, host }, waiter: { pid: process.pid, process: self(), op: lockClass } });
    },
    queueExpired(root, store, lockClass, waitedMs, waitersAhead) {
      append(root, { v: 1, at: Date.now(), kind: "pre-lock", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED", store, class: lockClass,
        waitedMs: Math.round(waitedMs), ...(waitersAhead === undefined ? {} : { waitersAhead }),
        holder: { pid: null, process: null, op: null, host }, waiter: { pid: process.pid, process: self(), op: lockClass } });
    },
    get errors() { return errors; },
  };
  // Additive handover from captured v1/v2 appenders: retain existing methods/sink/error count. The queue event
  // uses this generation's full code, so reload cannot retain the older abbreviated field or a missing method.
  const legacy = existing?.events;
  const shared: LockEvents = legacy ? { held: legacy.held, waitTimeout: legacy.waitTimeout,
    queueExpired: events.queueExpired, get errors() { return legacy.errors + events.errors; } } : events;
  eventsGlobals[eventsKey] = { version: 3, events: shared, dispose: existing?.dispose ?? (() => {}) };
  return shared;
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
/** A present writer must be an object of bounded strings and a pid; unknown keys are ignored. */
const invalidWriter = (value: unknown): boolean => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return true;
  const writer = value as Record<string, unknown>;
  if (writer.ppid !== undefined && !isCount(writer.ppid)) return true;
  return WRITER_STRING_FIELDS.some(field => writer[field] !== undefined &&
    (typeof writer[field] !== "string" || (writer[field] as string).length > LOCK_STATS_WRITER_MAX_CHARS));
};
/** Only the known writer facts, so a summary never relays unknown keys from a file. */
const knownWriter = (writer: LockStatsWriter | undefined): LockStatsWriter | undefined => {
  if (!writer) return undefined;
  const known: LockStatsWriter = {};
  for (const field of WRITER_STRING_FIELDS) if (writer[field] !== undefined) known[field] = writer[field];
  if (writer.ppid !== undefined) known.ppid = writer.ppid;
  return known;
};
/** Why a parsed stats file is invalid, or undefined. A file is used whole or not at all. */
const invalidLockStats = (value: unknown): string | undefined => {
  const file = value as Partial<LockStatsFile> | null;
  if (!file || typeof file !== "object" || file.version !== 1) return "not a version-1 stats file";
  if (typeof file.host !== "string" || !file.host || file.host.length > 256) return "bad host";
  if (!Number.isSafeInteger(file.pid) || file.pid! < 0) return "bad pid";
  if (file.writer !== undefined && invalidWriter(file.writer)) return "bad writer";
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

/** Bytes of a stats file's tail that hold its newest minutes (two full minutes are under 4 KiB). */
const LOCK_STATS_PEEK_BYTES = 8192;
/**
 * The newest minute before `current` in a stats file, from a bounded tail read: -1 when the file
 * holds none, undefined when the tail cannot tell. `minutes` is the last key and sorted, and
 * JSON escapes every quote inside a string, so these patterns match only the array itself.
 */
const peekNewestCompleteMinute = (file: string, current: number): number | undefined => {
  try {
    const descriptor = fs.openSync(file, READ_FLAGS);
    try {
      const { size } = fs.fstatSync(descriptor);
      const buffer = Buffer.alloc(Math.min(size, LOCK_STATS_PEEK_BYTES));
      const tail = buffer.toString("utf8", 0, fs.readSync(descriptor, buffer, 0, buffer.length, size - buffer.length));
      let newest = -1;
      for (const [, minute] of tail.matchAll(/\{"minute":(\d+),/g)) if (+minute! < current) newest = Math.max(newest, +minute!);
      return newest >= 0 || tail.includes("\"minutes\":[") ? newest : undefined;
    } finally { fs.closeSync(descriptor); }
  } catch { return undefined; }
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
  // holds no minute in it: skip it before the cap, and cap the newest (smarty-dev#7826). The
  // window is summarizeLockStats's exactly: the `span` complete minutes before the current one.
  const span = Math.max(1, Math.min(LOCK_STATS_RETAIN_MINUTES, Math.floor(options.minutes ?? LOCK_STATS_RETAIN_MINUTES)));
  const since = lockStatsWindowStartMs(options.now ?? Date.now(), span);
  let candidates: Array<{ name: string; mtimeMs: number; regular: boolean }> = [];
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    try {
      const stat = fs.lstatSync(path.join(directory, name));
      if (stat.mtimeMs >= since) candidates.push({ name, mtimeMs: stat.mtimeMs, regular: stat.isFile() });
    } catch (error) {
      if (errorCodeOf(error) !== "ENOENT") problems.push(`${name}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
  candidates.sort((a, b) => b.mtimeMs - a.mtimeMs || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  // Over the cap, a file that provably holds no complete minute of the window (only the current,
  // incomplete one, say) takes no slot (smarty-dev#7894). Only then: the usual case reads no more.
  if (candidates.length > LOCK_STATS_MAX_FILES) {
    const current = Math.floor(since / 60_000) + span;
    candidates = candidates.filter(({ name, regular }) =>
      !regular || (peekNewestCompleteMinute(path.join(directory, name), current) ?? current) >= current - span);
  }
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
  pids: Array<LockStatsTotals & { host: string; pid: number; topClass: MeshLockClass | undefined; writer?: LockStatsWriter }>;
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
  const byPid: Array<{ host: string; pid: number; writer: LockStatsWriter | undefined; bucket: LockStatsBucket; classes: Map<MeshLockClass, number> }> = [];
  for (const file of files) {
    const own = { host: file.host, pid: file.pid, writer: knownWriter(file.writer), bucket: emptyBucket(), classes: new Map<MeshLockClass, number>() };
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
  const pids = byPid.map(({ host, pid, writer, bucket, classes: held }) => ({
    host, pid, ...totalsOf(bucket, windowMs),
    topClass: [...held].sort((a, b) => b[1] - a[1])[0]?.[0],
    ...writer ? { writer } : {},
  })).sort((a, b) => b.holdMs - a.holdMs || b.n - a.n || a.pid - b.pid).slice(0, top);
  return {
    root, fromMinute, toMinute, minutes: span, processes: byPid.length, ...totalsOf(all, windowMs),
    peakMinuteBusyPct: Math.max(0, ...perMinuteHold.values()) / 60_000 * 100,
    classes, pids,
  };
};
