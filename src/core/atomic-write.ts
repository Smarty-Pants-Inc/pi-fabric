/** Reserved Landlock controls are host-owned and must not propagate to unrelated children. */
export const childProcessEnvironment = (environment: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv =>
  Object.fromEntries(Object.entries(environment).filter(([key]) => !key.startsWith("PI_FABRIC_LANDLOCK_")));

/** Bash's policy wrapper consumes escape requests before supplying these native-helper controls. */
export const bashProcessEnvironment = (environment: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv => ({
  ...childProcessEnvironment(environment),
  ...(environment.PI_FABRIC_LANDLOCK_SHELL !== undefined
    ? { PI_FABRIC_LANDLOCK_SHELL: environment.PI_FABRIC_LANDLOCK_SHELL } : {}),
  ...(environment.PI_FABRIC_LANDLOCK_WRITES !== undefined
    ? { PI_FABRIC_LANDLOCK_WRITES: environment.PI_FABRIC_LANDLOCK_WRITES } : {}),
});
import { retryDelayMs } from "./retry-backoff.js";
import { createHash, randomUUID } from "node:crypto";
import childProcess from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

export interface PhysicalHostIdentity { machineId: string; bootId: string }
export const validBootId = (value: unknown): value is string =>
  typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);

/** Hostnames and mesh hostIds are labels, never evidence that a PID is local.
 * Keep machine evidence independent of boot evidence: unreadable procfs must not erase locality. */
export const readPhysicalMachineId = (): string | undefined => {
  try {
    const machineId = fs.readFileSync("/etc/machine-id", "utf8").trim().toLowerCase();
    return /^[0-9a-f]{32}$/.test(machineId) ? machineId : undefined;
  } catch { return undefined; }
};

/** Missing/unreadable boot identity is unknown, never evidence of a previous boot. */
export const readPhysicalHostIdentity = (): PhysicalHostIdentity | undefined => {
  const machineId = readPhysicalMachineId();
  if (!machineId) return undefined;
  try {
    const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim().toLowerCase();
    return validBootId(bootId) ? { machineId, bootId } : undefined;
  } catch { return undefined; }
};


const DARWIN_START = /^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) {1,2}\d{1,2} \d{2}:\d{2}:\d{2} \d{4}$/;

/** Only identities from this platform's native reader are comparable; unknown stays live. */
export const validProcessIncarnation = (value: string | undefined, platform = process.platform): boolean => {
  if (!value) return false;
  if (platform === "linux") return /^\d+$/.test(value);
  if (platform === "win32") return /^win32:\d+$/.test(value);
  if (platform === "darwin") return value.startsWith("darwin:") && DARWIN_START.test(value.slice(7));
  return false;
};

export type IncarnationCommandRunner = (
  executable: string,
  args: string[],
  options: { timeout: number; maxBuffer: number; windowsHide: boolean; signal: AbortSignal; env?: NodeJS.ProcessEnv },
) => Promise<string>;

const runIncarnationCommand: IncarnationCommandRunner = (executable, args, options) => new Promise((resolve, reject) => {
  childProcess.execFile(executable, args, { ...options, env: childProcessEnvironment(options.env), encoding: "utf8" }, (error, stdout) => {
    if (error) reject(error);
    else resolve(stdout);
  });
});

/** Injectable native reader. No commands run until read/own is actually used. */
export const createProcessIncarnationReader = (options: {
  platform: NodeJS.Platform;
  systemRoot?: string | undefined;
  run?: IncarnationCommandRunner;
  timeoutMs?: number;
}) => {
  const { platform, systemRoot, run = runIncarnationCommand, timeoutMs = 2_000 } = options;
  let own: Promise<string | undefined> | undefined;
  const read = async (pid: number, remainingMs = timeoutMs): Promise<string | undefined> => {
    const budgetMs = Math.max(0, Math.min(timeoutMs, remainingMs));
    if (!Number.isSafeInteger(pid) || pid <= 0 || budgetMs <= 0) return undefined;
    try {
      if (platform === "linux") {
        const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
        const start = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
        return validProcessIncarnation(start, platform) ? start : undefined;
      }
      let executable: string;
      let args: string[];
      if (platform === "darwin") {
        executable = "/bin/ps";
        args = ["-p", String(pid), "-o", "lstart="];
      } else if (platform === "win32" && systemRoot) {
        executable = path.win32.join(systemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
        args = ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
          `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime.ToUniversalTime().Ticks.ToString([System.Globalization.CultureInfo]::InvariantCulture)`];
      } else return undefined;
      const abort = new AbortController();
      let timer: ReturnType<typeof setTimeout> | undefined;
      // Independent deadline also bounds injected/broken runners. Abort kills the native
      // child; late output never becomes evidence. No synchronous spawn on this path.
      const timeout = new Promise<undefined>((resolve) => {
        timer = setTimeout(() => { abort.abort(); resolve(undefined); }, budgetMs);
      });
      try {
        const start = await Promise.race([run(executable, args, {
          timeout: budgetMs, maxBuffer: 4_096, windowsHide: true, signal: abort.signal,
          ...(platform === "darwin" ? { env: { ...process.env, LC_ALL: "C", TZ: "UTC" } } : {}),
        }), timeout]);
        if (start === undefined) return undefined;
        const identity = `${platform}:${start.trim()}`;
        return validProcessIncarnation(identity, platform) ? identity : undefined;
      } finally { clearTimeout(timer); }
    } catch { return undefined; }
  };
  return {
    read,
    // This PID's creation identity cannot change during our lifetime. UNKNOWN is safe
    // for publication too: it can never justify recovering a live holder.
    own: (): Promise<string | undefined> => own ??= read(process.pid),
  };
};

const readers = new Map<string, ReturnType<typeof createProcessIncarnationReader>>();
const nativeReader = () => {
  const key = `${process.platform}:${process.env.SystemRoot ?? ""}`;
  let reader = readers.get(key);
  if (!reader) {
    reader = createProcessIncarnationReader({ platform: process.platform, systemRoot: process.env.SystemRoot });
    readers.set(key, reader);
  }
  return reader;
};

/** Fresh holder evidence, optionally bounded by remaining acquisition time.
 * macOS second-resolution reuse stays conservative; timeout aborts the native child. */
export const processIncarnation = (pid: number, timeoutMs?: number): Promise<string | undefined> => nativeReader().read(pid, timeoutMs);
/** Memoized lazily, not at import/registration/session start. */
export const ownProcessIncarnation = (): Promise<string | undefined> => nativeReader().own();

export interface AtomicWriteOptions {
  // File mode for the committed file (default 0o600) and for mkdir -p of its
  // parent directory (default 0o700).
  mode?: number;
  dirMode?: number;
  // Opt in to stable-storage ordering: sync the file before rename, then its directory
  // and all containing directories through the filesystem root (except on Windows).
  durable?: boolean;
  // Windows transiently rejects rename() with EPERM/EACCES/EEXIST/EBUSY while
  // an antivirus scan, indexer, or sibling reader probes the destination —
  // milliseconds of contention, not a policy failure. Retry a bounded number
  // of times with linear backoff before surfacing the error.
  renameRetries?: number;
  renameRetryDelayMs?: number;
  /** External ownership CAS after staging, immediately before EACH rename attempt. A throw leaves the target untouched. */
  beforeRename?: () => void;
}

const RETRYABLE_RENAME_CODES = new Set(["EPERM", "EACCES", "EEXIST", "EBUSY"]);

const errorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error
    ? String((error as { code?: unknown }).code)
    : undefined;

// Portable synchronous sleep for the retry window (Atomics.wait is legal on
// the Node main thread). If unavailable, retries proceed immediately — still
// correct, just less cooperative under contention.
const syncSleep = (() => {
  try {
    const buffer = new Int32Array(new SharedArrayBuffer(4));
    return (ms: number): void => {
      Atomics.wait(buffer, 0, 0, ms);
    };
  } catch {
    return (): void => undefined;
  }
})();

// Windows fails an open with EPERM/EACCES/EBUSY while the file is being replaced by a rename
// (the old file is pending deletion) or probed by a scanner: milliseconds, not a real answer.
const RETRYABLE_READ_CODES = new Set(["EPERM", "EACCES", "EBUSY"]);

/** readFileSync that retries the transient Windows open failures a few times, briefly. */
export const readFileRetrying = (file: string, attempts = 5, delayMs = 5): string => {
  for (let attempt = 1; ; attempt++) {
    try {
      return fs.readFileSync(file, "utf8");
    } catch (error) {
      const code = errorCode(error);
      if (attempt >= attempts || code === undefined || !RETRYABLE_READ_CODES.has(code)) throw error;
      syncSleep(delayMs * attempt);
    }
  }
};

// Rename-only: callers needing stable storage must provide their own barriers.
export const renameAtomic = (
  source: string,
  target: string,
  options?: Omit<AtomicWriteOptions, "durable">,
): void => {
  const attempts = Math.max(1, options?.renameRetries ?? 8);
  const delay = options?.renameRetryDelayMs ?? 25;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    options?.beforeRename?.(); // Validation errors are not transient rename failures.
    try {
      fs.renameSync(source, target);
      return;
    } catch (error) {
      const code = errorCode(error);
      if (attempt === attempts || code === undefined || !RETRYABLE_RENAME_CODES.has(code)) {
        throw error;
      }
      syncSleep(delay * attempt);
    }
  }
};

type Inode = Pick<fs.Stats, "dev" | "ino"> | Pick<fs.BigIntStats, "dev" | "ino">;
const sameInode = (left: Inode, right: Inode): boolean => left.dev === right.dev && left.ino === right.ino;

/** Confirm the complete reopenable namespace, not just realpath's collapsed endpoint.
 * Every symlink's parent and the endpoint's directory owe barriers through the root.
 * A second walk detects replacements, including links retargeted to the SAME inode.
 * Windows skips unsupported directory fsync, but still binds the opened receipt.
 */
const namespaceSnapshot = (target: string, receipt?: Inode) => {
    const absolute = path.isAbsolute(target) ? target : `${process.cwd()}${path.sep}${target}`;
    const split = (value: string) => value.split(path.sep === "\\" ? /[\\/]+/ : /\/+/);
    let current = path.parse(absolute).root;
    let pending = split(absolute.slice(current.length)).filter(Boolean);
    const entries: string[] = [];
    const directories = new Map<string, fs.Stats>();
    const parents: string[] = [];
    const seen = new Set<string>();
    let hops = 0;
    const record = (file: string, stat: fs.Stats, link?: string) => {
      entries.push(JSON.stringify([file, stat.dev, stat.ino, stat.mode & fs.constants.S_IFMT,
        // A removed/recreated link can reuse its inode number; include its change time.
        stat.isSymbolicLink() ? stat.ctimeMs : undefined, link]));
      if (stat.isDirectory()) {
        const previous = directories.get(file);
        if (previous && !sameInode(previous, stat)) throw new Error("Namespace directory changed during resolution");
        directories.set(file, stat);
      }
    };
    record(current, fs.lstatSync(current));
    while (pending.length) {
      const component = pending.shift()!;
      if (component === ".") continue;
      if (component === "..") { current = path.dirname(current); continue; }
      const file = path.join(current, component);
      const stat = fs.lstatSync(file);
      if (stat.isSymbolicLink()) {
        const state = JSON.stringify([file, pending]);
        if (++hops > 40 || seen.has(state)) throw new Error("Namespace symlink loop or hop limit exceeded");
        seen.add(state);
        const link = fs.readlinkSync(file);
        const checked = fs.lstatSync(file);
        if (!checked.isSymbolicLink() || !sameInode(stat, checked) || stat.ctimeMs !== checked.ctimeMs) {
          throw new Error("Namespace link changed during resolution");
        }
        record(file, stat, link);
        parents.push(current);
        // Do not normalize '..' before following symlinks in the target itself.
        if (path.isAbsolute(link)) {
          current = path.parse(link).root;
          record(current, fs.lstatSync(current));
          pending = [...split(link.slice(current.length)).filter(Boolean), ...pending];
        } else pending = [...split(link).filter(Boolean), ...pending];
      } else {
        record(file, stat);
        if (pending.length && !stat.isDirectory()) throw new Error("Namespace component is not a directory");
        current = file;
      }
    }
    const endpoint = fs.lstatSync(current);
    record(current, endpoint);
    // Archive receipts on Windows carry exact 64-bit file IDs. Do not round them
    // back to Number when binding the reopened endpoint; numeric callers are unchanged.
    const identity = typeof receipt?.ino === "bigint" ? fs.lstatSync(current, { bigint: true }) : endpoint;
    if (receipt && !sameInode(receipt, identity)) throw new Error("Session receipt inode changed during namespace confirmation");
    parents.push(endpoint.isDirectory() ? current : path.dirname(current));
    return { entries, directories, parents };
};

export const syncPathNamespace = (target: string, receipt?: Inode): void => {
  const before = namespaceSnapshot(target, receipt);
  if (process.platform !== "win32") {
    const synced = new Set<string>();
    for (const parent of before.parents.reverse()) {
      for (let current = parent; ; current = path.dirname(current)) {
        if (!synced.has(current)) {
          const fd = fs.openSync(current, fs.constants.O_RDONLY);
          try {
            const opened = fs.fstatSync(fd);
            const expected = before.directories.get(current);
            if (!opened.isDirectory() || !expected || !sameInode(opened, expected)) throw new Error("Namespace directory changed before barrier");
            fs.fsyncSync(fd);
          } finally { fs.closeSync(fd); }
          synced.add(current);
        }
        if (path.dirname(current) === current) break;
      }
    }
  }
  if (JSON.stringify(namespaceSnapshot(target, receipt).entries) !== JSON.stringify(before.entries)) {
    throw new Error("Namespace changed during durability barriers");
  }
};

/** Same namespace fence as the synchronous writer, with barriers off the event loop.
 * Always confirm all parent entries: an unchanged ancestor inode does not prove
 * that a newly created or replaced child directory is durably linked. */
export const syncPathNamespaceAsync = async (target: string, receipt: Inode): Promise<void> => {
  const before = namespaceSnapshot(target, receipt);
  if (process.platform !== "win32") {
    for (const [directory, expected] of [...before.directories].reverse()) {
      const handle = await fs.promises.open(directory, fs.constants.O_RDONLY);
      try {
        const opened = await handle.stat();
        if (!opened.isDirectory() || !sameInode(opened, expected)) throw new Error("Namespace directory changed before barrier");
        await handle.sync();
      } finally { await handle.close(); }
    }
  }
  if (JSON.stringify(namespaceSnapshot(target, receipt).entries) !== JSON.stringify(before.entries)) {
    throw new Error("Namespace changed during durability barriers");
  }
};
/** Existence is not a receipt; retry every required directory barrier without a cache. */
export const syncDirectoryChain = (directory: string): void => {
  if (process.platform !== "win32") syncPathNamespace(directory);
};

// Temps this process staged and has not yet renamed or removed (smarty-dev#6622).
// An exit (a Main /quit, a worker/resident exit or a handled SIGTERM) can end the
// process while a writer awaits the mesh lock with its staged state temp on disk;
// a try/finally never runs then. The bounded exit hook below removes them. Every
// name carries this pid, so the hook never touches a temp another live pid owns.
const ownedTemporaries = new Set<string>();
/** At most this many temps are removed by the synchronous exit hook. */
export const EXIT_TEMP_CLEANUP_LIMIT = 256;
let temporaryExitHook: (() => void) | undefined;

/** Removes this process's still-staged temps; synchronous, bounded, best-effort. */
export const removeOwnedTemporaries = (): number => {
  let removed = 0;
  for (const file of [...ownedTemporaries].slice(0, EXIT_TEMP_CLEANUP_LIMIT)) {
    ownedTemporaries.delete(file);
    try { fs.rmSync(file, { force: true }); removed += 1; } catch { /* Best-effort exit cleanup. */ }
  }
  return removed;
};

/** Registers a temp this process owns (its name must carry process.pid) until the
 * returned release runs: on process exit it is removed by a bounded hook. */
export const trackTemporary = (file: string): (() => void) => {
  ownedTemporaries.add(file);
  if (!temporaryExitHook) {
    temporaryExitHook = () => { removeOwnedTemporaries(); };
    process.once("exit", temporaryExitHook);
  }
  return () => { ownedTemporaries.delete(file); };
};

/** Test/diagnostic view of the temps currently registered for exit cleanup. */
export const trackedTemporaries = (): readonly string[] => [...ownedTemporaries];

export const writeFileAtomic = (
  filePath: string,
  contents: string | Uint8Array,
  options?: AtomicWriteOptions,
): void => {
  const directory = path.dirname(filePath);
  const missing: string[] = [];
  let existingParent = path.resolve(directory);
  if (options?.durable && process.platform !== "win32") {
    // Record the missing chain before mkdir: syncing only the leaf cannot persist
    // its ancestors' entries, including the topmost new directory's link.
    for (;;) {
      try { fs.statSync(existingParent); break; } catch (error) {
        if (errorCode(error) !== "ENOENT") throw error;
        missing.unshift(existingParent);
        existingParent = path.dirname(existingParent);
      }
    }
  }
  fs.mkdirSync(directory, {
    recursive: true,
    mode: options?.dirMode ?? 0o700,
  });
  if (missing.length > 0) {
    // Fail closed before the file barriers. Sync new components top down, then
    // their pre-existing containing directory so the entire chain is linked.
    for (const created of [...missing, existingParent]) {
      const fd = fs.openSync(created, fs.constants.O_RDONLY);
      try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    }
  }
  const temporary = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
  const release = trackTemporary(temporary);
  try {
    if (options?.durable) {
      const fd = fs.openSync(temporary, "w", options.mode ?? 0o600);
      try {
        fs.writeFileSync(fd, contents, { encoding: "utf8" });
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    } else {
      fs.writeFileSync(temporary, contents, {
        encoding: "utf8",
        mode: options?.mode ?? 0o600,
      });
    }
    renameAtomic(temporary, filePath, options);
    if (options?.durable) syncDirectoryChain(directory);
  } finally {
    // No-op right after a successful rename; removes the temp on failure.
    try { fs.rmSync(temporary, { force: true }); } finally { release(); }
  }
};

/** A single-owner writer (or used under its protocol lock). Equal bytes may skip
 * only a soft-state replacement; durable writes always establish fresh barriers.
 */
export class AtomicFileWriter {
  constructor(readonly file: string) {}

  #stamp(): string {
    const stat = fs.statSync(this.file);
    return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
  }

  write(contents: string, options?: AtomicWriteOptions): boolean {
    let unchanged = false;
    try {
      const before = this.#stamp();
      const current = readFileRetrying(this.file);
      const after = this.#stamp();
      unchanged = before === after && current === contents;
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    }
    // The retained equal-bytes cache is a soft-state optimization only. Durable
    // writes are receipts: always replace, fsync, and re-confirm the namespace.
    if (unchanged && !options?.durable) return false;
    writeFileAtomic(this.file, contents, options);
    return true;
  }
}

/** Contention is retryable; permission, I/O, and operation errors are not (upstream file-lock). */
export class FileLockTimeoutError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FileLockTimeoutError";
  }
}

export interface ExclusiveLockOptions {
  directory: string;
  lockName: string;
  /** Error message when acquisition times out. */
  timeoutMessage: string;
  staleMs?: number;
  attempts?: number;
  delayMs?: number;
}

/** Synchronous claim locks use no process probe until actual acquisition. */
const exclusiveLockProcessAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch (error) {
    // EPERM (or an unknown probe failure) is not evidence of a dead owner (upstream file-lock).
    return (error as NodeJS.ErrnoException | undefined)?.code !== "ESRCH";
  }
};

// Upstream file-lock reaping, ported onto our sync lock: damaged metadata must not strand the
// lock forever (fall back to the directory mtime), and an owner in another PID namespace is
// judged by its identity/heartbeat, where "unknown" is never death.
const exclusiveLockOwnerCanBeReaped = (owner: string, mtimeMs: number, staleMs: number): boolean => {
  const [, pid, created, identity] = owner.split("\n");
  const timestamp = Number(created);
  const since = created?.trim() && Number.isSafeInteger(timestamp) && timestamp >= 0 && timestamp <= Date.now()
    ? timestamp
    : mtimeMs;
  return Date.now() - since > staleMs && lockOwnerLiveness(Number(pid), since, identity, {
    legacyAlive: exclusiveLockProcessAlive,
    maxHoldMs: Math.max(SHORT_LOCK_MAX_HOLD_MS, staleMs),
  }) === "dead";
};

// Stale-lock recovery must be an exclusive claim. Stat-then-delete is
// TOCTOU: two reapers (or a reaper and a fresh writer that recreated the
// lock in between) can both pass their checks, and the slower rm then
// deletes a lock the faster one already replaced. rename() is the claim —
// only one process can move the directory, and removal targets the claimed
// path, never the live lock path. A claim that turns out to hold a live
// lock is renamed back before any destructive step; a live lock is never
// deleted, even if the rename-back races a fresh writer.
const reapStaleLock = (lock: string, verify: (claimed: string) => boolean): boolean => {
  const claim = `${lock}.reap-${process.pid}-${randomUUID()}`;
  try {
    fs.renameSync(lock, claim);
  } catch {
    return false;
  }
  if (!verify(claim)) {
    try {
      fs.renameSync(claim, lock);
    } catch {
      // `lock` was recreated after the claim. Re-verify before any
      // destructive step so a claimed live lock is only ever abandoned as
      // garbage, never deleted.
      if (verify(claim)) fs.rmSync(claim, { recursive: true, force: true });
    }
    return false;
  }
  fs.rmSync(claim, { recursive: true, force: true });
  return true;
};

export const withExclusiveFileLock = <T>(
  options: ExclusiveLockOptions,
  operation: () => T,
): T => {
  const attempts = options.attempts ?? 50;
  const delayMs = options.delayMs ?? 5;
  const staleMs = options.staleMs ?? 30_000;
  fs.mkdirSync(options.directory, { recursive: true, mode: 0o700 });
  const lock = path.join(options.directory, options.lockName);
  const ownerPath = path.join(lock, "owner");
  const token = randomUUID();
  let acquired = false;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      fs.mkdirSync(lock, { mode: 0o700 });
      try {
        // The identity line is additive: older readers only consume the first three.
        fs.writeFileSync(ownerPath, `${token}\n${process.pid}\n${Date.now()}\n${encodeOwnerIdentityLine()}\n`, {
          encoding: "utf-8",
          mode: 0o600,
        });
      } catch (error) {
        fs.rmSync(lock, { recursive: true, force: true });
        throw error;
      }
      acquired = true;
      break;
    } catch (error) {
      if (errorCode(error) !== "EEXIST") throw error;
      try {
        const firstOwner = fs.readFileSync(ownerPath, "utf8");
        if (exclusiveLockOwnerCanBeReaped(firstOwner, fs.statSync(lock).mtimeMs, staleMs)) {
          const secondOwner = fs.readFileSync(ownerPath, "utf8");
          if (
            secondOwner === firstOwner &&
            reapStaleLock(lock, (claimed) => {
              try {
                const owner = fs.readFileSync(path.join(claimed, "owner"), "utf8");
                return owner === firstOwner && exclusiveLockOwnerCanBeReaped(owner, fs.statSync(claimed).mtimeMs, staleMs);
              } catch {
                return false;
              }
            })
          ) {
            continue;
          }
        }
      } catch {
        try {
          // Ownerless lock (crash between mkdir and the owner write): age is
          // the only signal, and the claim re-verifies it after the rename.
          const first = fs.statSync(lock);
          if (
            Date.now() - first.mtimeMs > staleMs &&
            reapStaleLock(lock, (claimed) => {
              try {
                return Date.now() - fs.statSync(claimed).mtimeMs > staleMs;
              } catch {
                return false;
              }
            })
          ) {
            continue;
          }
        } catch {
          // Lock creation or stale recovery raced; retry the bounded acquisition.
        }
      }
      if (attempt === attempts - 1) break;
      syncSleep(delayMs);
    }
  }
  if (!acquired) throw new FileLockTimeoutError(options.timeoutMessage);
  try {
    return operation();
  } finally {
    try {
      const owner = fs.readFileSync(ownerPath, "utf8");
      if (owner.startsWith(`${token}\n`)) {
        fs.rmSync(lock, { recursive: true, force: true });
      }
    } catch {
      // A recovering process already removed this lock.
    }
  }
};

export interface AtomicJsonOptions extends AtomicWriteOptions {
  // Pretty-print indent for JSON.stringify (default: compact).
  space?: number;
  // Some on-disk formats expect a trailing newline (schema state files).
  newline?: boolean;
}

export const writeJsonAtomic = (
  filePath: string,
  value: unknown,
  options?: AtomicJsonOptions,
): void => {
  const space = options?.space;
  const serialized =
    JSON.stringify(value, null, space) + (options?.newline === true ? "\n" : "");
  writeFileAtomic(filePath, serialized, options);
};

const asyncSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const renameAtomicAsync = async (
  source: string,
  target: string,
  options?: Omit<AtomicWriteOptions, 'durable'>,
): Promise<void> => {
  const attempts = Math.max(1, options?.renameRetries ?? 8);
  const delay = options?.renameRetryDelayMs ?? 25;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await fs.promises.rename(source, target);
      return;
    } catch (error) {
      const code = errorCode(error);
      if (attempt === attempts || code === undefined || !RETRYABLE_RENAME_CODES.has(code)) {
        throw error;
      }
      await asyncSleep(delay * attempt);
    }
  }
};

const writeFileAtomicAsync = async (
  filePath: string,
  contents: string,
  options?: Omit<AtomicWriteOptions, 'durable'>,
): Promise<void> => {
  await fs.promises.mkdir(path.dirname(filePath), {
    recursive: true,
    mode: options?.dirMode ?? 0o700,
  });
  const temporary = `${filePath}.${process.pid}.${randomUUID()}.tmp`;
  const release = trackTemporary(temporary);
  try {
    await fs.promises.writeFile(temporary, contents, {
      encoding: "utf8",
      mode: options?.mode ?? 0o600,
    });
    await renameAtomicAsync(temporary, filePath, options);
  } finally {
    try { await fs.promises.rm(temporary, { force: true }); } finally { release(); }
  }
};

export const writeJsonAtomicAsync = async (
  filePath: string,
  value: unknown,
  options?: Omit<AtomicJsonOptions, 'durable'>,
): Promise<void> => {
  const space = options?.space;
  const serialized =
    JSON.stringify(value, null, space) + (options?.newline === true ? "\n" : "");
  await writeFileAtomicAsync(filePath, serialized, options);
};

export const MESH_LOCK_TIMEOUT_CODE = "FABRIC_MESH_LOCK_TIMEOUT";

/** A failed acquisition wrote nothing. Foreground callers must see this error, not a retry. */
export class MeshLockTimeoutError extends Error {
  readonly code = MESH_LOCK_TIMEOUT_CODE;
  constructor(readonly holder: string, readonly attempts: number, readonly maxGapMs: number) {
    super(`${MESH_LOCK_TIMEOUT_CODE}: Timed out waiting for the Fabric mesh lock${holder} after ${attempts} attempts, largest gap between attempts ${maxGapMs} ms`);
    this.name = "MeshLockTimeoutError";
  }
}

// Compatible with older stores and the bridge's deliberately narrow error wire.
export const isMeshLockTimeout = (error: unknown): error is Error & { code: typeof MESH_LOCK_TIMEOUT_CODE } =>
  error instanceof Error && "code" in error && error.code === MESH_LOCK_TIMEOUT_CODE;

/** A write that wrote nothing and may succeed later: a lock timeout, or a SQLite WAL-cap refusal
 * (`FABRIC_MESH_STATE_WAL_CAP`, state-backend.ts isMeshStateWalCap; smarty-dev#7451 item 8). */
export const isMeshWriteDeferred = (error: unknown): error is Error =>
  isMeshLockTimeout(error) || (error instanceof Error && (error as { code?: unknown }).code === "FABRIC_MESH_STATE_WAL_CAP");

/** Best-effort protocol writes may swallow conflicts, but not a retryable lock wait. */
export const rethrowMeshLockTimeout = (error: unknown): undefined => {
  if (isMeshLockTimeout(error)) throw error;
  return undefined;
};

export const DEFAULT_MESH_RETRY_WARNING_WINDOW_MS = 60_000;

/** `PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS`: positive milliseconds; blank or invalid uses 60 s. */
export const meshRetryWarningWindowMs = (): number => {
  const raw = process.env.PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS;
  const value = Number(raw);
  return raw?.trim() && Number.isFinite(value) && value >= 1 ? Math.floor(value) : DEFAULT_MESH_RETRY_WARNING_WINDOW_MS;
};

/** Per-background-path outage state; no process-global handlers or foreground retry policy. */
export class MeshBackgroundRetry {
  #delay = 0;
  #retryAt = 0;
  #lastReportedAt: number | undefined;
  #suppressed = 0;
  #running = false;
  constructor(readonly label: string, readonly minMs = 100, readonly maxMs = 5_000,
    readonly warningWindowMs = meshRetryWarningWindowMs()) {}

  get waitMs(): number { return Math.max(0, this.#retryAt - Date.now()); }
  #resetBackoff(): void { this.#delay = 0; this.#retryAt = 0; }
  /** A confirmed acquisition: reset backoff, never the warning window (smarty-dev#8305). */
  success(): void {
    this.#resetBackoff();
    if (!this.#suppressed || this.#lastReportedAt === undefined) return;
    const now = Date.now();
    const elapsedMs = now - this.#lastReportedAt;
    if (elapsedMs < this.warningWindowMs) return;
    // Final summary: retries suppressed after the last line are reported once the window
    // has passed, so an outage that ends inside a window is not silently dropped. It takes
    // that window's single line; a later outage inside it is counted, not printed.
    console.warn(`[pi-fabric] ${this.label}: mesh lock recovered (${this.#suppressed} similar retries suppressed in the last ${Math.floor(elapsedMs / 1_000)}s)`);
    this.#lastReportedAt = now;
    this.#suppressed = 0;
  }
  failure(error: unknown): boolean {
    const transient = isMeshWriteDeferred(error);
    if (!transient) {
      // Contain the owned callback, but surface unrelated bugs and preserve its existing
      // retry cadence. Only a typed acquisition failure earns lock backoff/deduplication.
      // Not proof that the lock is free: no recovery summary, suppression is kept.
      this.#resetBackoff();
      console.warn(`[pi-fabric] ${this.label}: background operation failed: ${error instanceof Error ? error.message : String(error)}`);
      return false;
    }
    this.#delay = Math.min(this.maxMs, Math.max(this.minMs, this.#delay * 2));
    // Keep the exponential ceiling separate from the randomized draw. Timers
    // have a 1ms scheduling floor so a zero draw cannot form a microtask spin.
    const delayMs = Math.max(1, retryDelayMs(0, this.#delay, this.maxMs));
    const now = Date.now();
    this.#retryAt = now + delayMs;
    const elapsedMs = this.#lastReportedAt === undefined ? Infinity : now - this.#lastReportedAt;
    // At most one lock-retry line per instance per window (default 60 s), even across
    // successful acquisitions: a flapping lock must not print one warning per short outage.
    // The first warning is immediate and carries the holder diagnostics.
    if (elapsedMs >= this.warningWindowMs) {
      const summary = this.#suppressed > 0
        ? ` (${this.#suppressed} similar retries suppressed in the last ${Math.floor(elapsedMs / 1_000)}s)` : "";
      console.warn(`[pi-fabric] ${this.label}: ${isMeshLockTimeout(error) ? "mesh lock timeout; retrying" : "mesh WAL cap; retrying"} in ${delayMs} ms: ${error instanceof Error ? error.message : String(error)}${summary}`);
      this.#lastReportedAt = now;
      this.#suppressed = 0;
    } else {
      this.#suppressed++;
    }
    return transient;
  }

  /** Polls retry unchanged state on a later tick after backoff; contains sync handlers too.
   * Paths that may no-op can reset explicitly after a confirmed acquisition instead.
   */
  async run(operation: () => unknown | Promise<unknown>, resetOnSuccess = true): Promise<"done" | "retry" | "failed" | "skipped"> {
    if (this.#running || this.waitMs > 0) return "skipped";
    this.#running = true;
    try {
      await operation();
      if (resetOnSuccess) this.success();
      return "done";
    } catch (error) {
      return this.failure(error) ? "retry" : "failed";
    } finally { this.#running = false; }
  }
}

/** One-shot notifications have no natural next tick. An owned serial queue retains them;
 * only a typed acquisition timeout retries (no write occurred). Idle costs no timer.
 * Awaiting enqueue waits for the first attempt, never an indefinitely locked mesh.
 */
export class MeshBackgroundQueue {
  readonly #retry: MeshBackgroundRetry;
  readonly #pending: Array<{ operation: () => unknown | Promise<unknown>; attempted: () => void }> = [];
  #timer: NodeJS.Timeout | undefined;
  #draining: Promise<void> | undefined;
  #closed = false;
  #failed = false;
  constructor(label: string, minMs = 100, maxMs = 5_000) { this.#retry = new MeshBackgroundRetry(label, minMs, maxMs); }

  enqueue(operation: () => unknown | Promise<unknown>): Promise<void> {
    if (this.#closed) return Promise.resolve();
    // Bounded best-effort notifications. Durable protocol cursors are not stored here.
    if (this.#pending.length >= 1_000) {
      this.#failed = true;
      console.warn("[pi-fabric] background mesh notification queue full; dropping newest notification");
      return Promise.resolve();
    }
    const admission = new Promise<void>(resolve => this.#pending.push({ operation, attempted: resolve }));
    // A hook behind an already queued/outage write must not wait for mesh recovery.
    // The queue still owns its retry; close also settles every outstanding admission.
    if (this.#pending.length > 1 || this.#retry.waitMs > 0) this.#pending[this.#pending.length - 1]!.attempted();
    this.#schedule();
    return admission;
  }

  retry(operation: () => unknown | Promise<unknown>, error: unknown): Promise<void> {
    this.#retry.failure(error);
    return this.enqueue(operation);
  }

  #schedule(): void {
    if (this.#closed || this.#draining || this.#timer || !this.#pending.length) return;
    if (this.#retry.waitMs > 0) {
      this.#timer = setTimeout(() => { this.#timer = undefined; this.#schedule(); }, this.#retry.waitMs);
      this.#timer.unref();
      return;
    }
    // Assign before the microtask starts, so a burst has exactly one drain.
    this.#draining = Promise.resolve(); // fence reentrant admission before first operation
    const work = this.#drain();
    this.#draining = work;
    void work.finally(() => { this.#draining = undefined; this.#schedule(); }).catch(error => {
      this.#retry.failure(error); // an owned boundary, not a global bug-hiding handler
    });
  }

  async #drain(): Promise<void> {
    while (!this.#closed && this.#pending.length) {
      const item = this.#pending[0]!;
      const result = await this.#retry.run(item.operation);
      item.attempted();
      if (result === "retry" || result === "skipped") return;
      if (result === "failed") this.#failed = true;
      this.#pending.shift(); // permanent failures are visible but do not poison later notices
    }
  }

  /** Admission only waits for a first attempt; release needs a confirmed empty queue. */
  async checkpointForRelease(): Promise<void> {
    await this.#draining;
    if (this.#pending.length || this.#failed) throw new Error("Background mesh release has unconfirmed publication obligations");
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    await this.#draining;
    for (const item of this.#pending.splice(0)) item.attempted();
  }
}

// ---------------------------------------------------------------------------
// Namespace-safe owner liveness. It shares this module with the atomic
// writers whose files it guards: a separate module would become its own
// shared chunk in the eager startup graph, which is at its file budget.
//
// `process.kill(pid, 0)` only answers for the
// caller's own PID namespace and boot: inside containers the same number can
// name an unrelated process (or nothing), and after PID reuse it names a
// stranger. Owners therefore record who they are (host, PID namespace, boot,
// process start time) and long-lived owners refresh a heartbeat. A probe only
// trusts the signal answer when the record shares the caller's namespace and
// boot; otherwise the heartbeat decides. Records without identity fields keep
// the caller's pre-identity behaviour exactly.

/** Long-lived owners refresh `heartbeatAt` this often. */
export const OWNER_HEARTBEAT_INTERVAL_MS = 10_000;
/** A heartbeat older than this is stale: the owner is unknown, never proven dead. */
export const OWNER_HEARTBEAT_TTL_MS = 45_000;
/**
 * Short-lived locks carry no heartbeat. Across namespaces their creation time
 * stands in for one; past this hold ceiling the holder is unknown, not dead,
 * so only its own namespace (signal probe or PID-reuse check) can reap it.
 */
export const SHORT_LOCK_MAX_HOLD_MS = 10 * 60_000;
// Linux reports process start in clock ticks since boot; the wall-clock
// conversion of each side may disagree by up to a btime rounding second.
const PID_START_TOLERANCE_MS = 2_000;
// Linux USER_HZ is 100 on every mainstream architecture. Both sides of a
// comparison use the same conversion, so only consistency matters.
const CLOCK_TICKS_PER_SECOND = 100;
const MAX_IDENTITY_JSON_CHARS = 1_024;

export type OwnerLiveness = "alive" | "dead" | "unknown";

export interface OwnerIdentity {
  pid: number;
  hostname: string;
  pidNamespace?: string;
  bootId?: string;
  startedAt: number;
}

/** A persisted owner. Identity fields are optional so old records still parse. */
export interface OwnerRecord {
  pid: number;
  hostname?: string;
  pidNamespace?: string;
  bootId?: string;
  startedAt?: number;
}

export interface LivenessProbes {
  self?: () => OwnerIdentity;
  /** `kill(pid, 0)`: ESRCH dead, EPERM alive, anything else unknown. */
  signal?: (pid: number) => OwnerLiveness;
  /** Wall-clock start of `pid` when the platform reports it (Linux procfs). */
  processStartedAt?: (pid: number) => number | undefined;
  /** Whether `pid` is visible in this process's procfs (Linux `/proc/<pid>/stat`). */
  processVisible?: (pid: number) => boolean;
  now?: () => number;
}

export interface OwnerLivenessOptions {
  heartbeatAt?: number;
  heartbeatTtlMs?: number;
  /** The caller's exact pre-identity probe, used only for legacy records. */
  legacyAlive?: (pid: number) => boolean;
  probes?: LivenessProbes;
}

const readTrimmed = (file: string): string | undefined => {
  try {
    const value = fs.readFileSync(file, "utf8").trim();
    return value === "" ? undefined : value;
  } catch {
    return undefined;
  }
};

let bootTimeSeconds: number | undefined | null = null;
const readBootTimeSeconds = (): number | undefined => {
  if (bootTimeSeconds !== null) return bootTimeSeconds;
  const line = readTrimmed("/proc/stat")?.split("\n").find((entry) => entry.startsWith("btime "));
  const value = Number(line?.slice("btime ".length).trim());
  bootTimeSeconds = Number.isSafeInteger(value) && value > 0 ? value : undefined;
  return bootTimeSeconds;
};

/** Parses `/proc/<pid>/stat` (field 22, starttime) into wall-clock ms. */
export const parseProcStartedAt = (stat: string, bootSeconds: number): number | undefined => {
  // The command name is parenthesised and may contain spaces or parentheses.
  const fields = stat.slice(stat.lastIndexOf(")") + 2).split(" ");
  const ticks = Number(fields[19]);
  if (!Number.isSafeInteger(ticks) || ticks < 0) return undefined;
  return bootSeconds * 1_000 + Math.round((ticks * 1_000) / CLOCK_TICKS_PER_SECOND);
};

const procStartedAt = (pid: number | "self"): number | undefined => {
  if (process.platform !== "linux") return undefined;
  const boot = readBootTimeSeconds();
  const stat = readTrimmed(`/proc/${pid}/stat`);
  return boot === undefined || stat === undefined ? undefined : parseProcStartedAt(stat, boot);
};

/**
 * Linux: field 22 (starttime, clock ticks since boot) of `/proc/<pid>/stat` in THIS process's
 * procfs. undefined: the PID is not visible here (or no procfs); "": visible, start unreadable.
 */
export const procfsStartTicks = (pid: number): string | undefined => {
  if (process.platform !== "linux" || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
  let stat: string;
  try {
    stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
  } catch (error) {
    const code = errorCode(error);
    // Gone or never here. Anything else (hidepid, EACCES) still shows the PID exists.
    return code === "ENOENT" || code === "ESRCH" ? undefined : "";
  }
  const start = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
  return start && /^\d+$/.test(start) ? start : "";
};

/**
 * Positive evidence of life for an owner whose PID namespace is unknown (PR #864 P1): our
 * procfs shows its PID and, when the record carries a start time (field 22 ticks), the same
 * start. A visible PID with an unreadable start cannot be excluded either. Only ever evidence
 * of life, never of death: callers fall back to heartbeat age when this is false.
 */
export const procfsShowsOwner = (
  pid: number,
  recordedStart: string | undefined,
  read: (pid: number) => string | undefined = procfsStartTicks,
): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  const actual = read(pid);
  if (actual === undefined) return false;
  const recorded = recordedStart !== undefined && /^\d+$/.test(recordedStart) ? recordedStart : undefined;
  return recorded === undefined || actual === "" || actual === recorded;
};

const procfsVisible = (pid: number): boolean => procfsStartTicks(pid) !== undefined;

/** Same evidence for a JSON owner record: only a start recorded with a boot id is procfs-derived. */
const unknownNamespaceOwnerVisible = (record: OwnerRecord, probes: LivenessProbes): boolean => {
  if (!Number.isSafeInteger(record.pid) || record.pid <= 0) return false;
  if (!(probes.processVisible ?? procfsVisible)(record.pid)) return false;
  if (record.bootId === undefined || record.startedAt === undefined) return true;
  const actual = (probes.processStartedAt ?? procStartedAt)(record.pid);
  return actual === undefined || Math.abs(actual - record.startedAt) <= PID_START_TOLERANCE_MS;
};

const signalProbe = (pid: number): OwnerLiveness => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return "dead";
  try {
    process.kill(pid, 0);
    return "alive";
  } catch (error) {
    const code = errorCode(error);
    if (code === "ESRCH") return "dead";
    return code === "EPERM" ? "alive" : "unknown";
  }
};

let cachedIdentity: OwnerIdentity | undefined;
/** This process's identity, computed once on first use (never at import). */
export const currentOwnerIdentity = (): OwnerIdentity => {
  if (cachedIdentity) return { ...cachedIdentity };
  let pidNamespace: string | undefined;
  let bootId: string | undefined;
  let startedAt: number | undefined;
  if (process.platform === "linux") {
    try {
      pidNamespace = fs.readlinkSync("/proc/self/ns/pid");
    } catch {
      pidNamespace = undefined;
    }
    bootId = readTrimmed("/proc/sys/kernel/random/boot_id");
    // PID-reuse detection compares procfs start times, so record ours the
    // same way. Without a boot id, readers skip that comparison.
    if (bootId !== undefined) startedAt = procStartedAt("self");
    if (startedAt === undefined) bootId = undefined;
  } else if (process.platform === "darwin" || process.platform === "win32") {
    // No PID namespaces here: one host-wide PID space, named explicitly because an absent
    // namespace is always unknown, never local (ownerLiveness, lockTokenLocality).
    pidNamespace = `pid:${process.platform}`;
  }
  cachedIdentity = {
    pid: process.pid,
    hostname: os.hostname(),
    ...(pidNamespace === undefined ? {} : { pidNamespace }),
    ...(bootId === undefined ? {} : { bootId }),
    startedAt: startedAt ?? Math.round(performance.timeOrigin),
  };
  return { ...cachedIdentity };
};

// Lock-token locality. Line-oriented shared-storage locks whose wire cannot grow (the B68
// three-line mesh lock above all) carry their writer's place in the token instead:
// `<uuid>-<host><pid-namespace><boot>`, three 6-hex digests (`------` when unknown). The
// token keeps the [a-f0-9-] alphabet that mesh lock-queue ticket names require, and old
// readers only compare tokens for equality, so the wire is unchanged. A reader trusts its
// local `kill(pid, 0)` answer only for a token from its own host, known PID namespace and boot;
// any other place, including an unknown (`------`) namespace on either side, is unknown, which
// is never death. As in ownerLiveness, an owner on the same host
// from an earlier boot is dead: no process outlives its kernel boot.
const LOCALITY_UNKNOWN = "------";
const LOCALITY_UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const LOCALITY_TOKEN = new RegExp(`^${LOCALITY_UUID}-([0-9a-f]{6})([0-9a-f]{6}|-{6})([0-9a-f]{6}|-{6})$`);
const LOCALITY_TAG = /^([0-9a-f]{6})([0-9a-f]{6}|-{6})([0-9a-f]{6}|-{6})$/;
const localityDigest = (value: string | undefined): string =>
  value === undefined ? LOCALITY_UNKNOWN : createHash("sha256").update(value).digest("hex").slice(0, 6);

/** The locality tag of an owner identity (this process by default). */
export const ownerLocalityTag = (
  identity: Pick<OwnerIdentity, "hostname" | "pidNamespace" | "bootId"> = currentOwnerIdentity(),
): string => `${localityDigest(identity.hostname)}${localityDigest(identity.pidNamespace)}${localityDigest(identity.bootId)}`;

/** A fresh lock-owner token naming this process's host, PID namespace and boot. */
export const localityLockToken = (): string => `${randomUUID()}-${ownerLocalityTag()}`;

/**
 * Where a lock token's writer ran, relative to this process:
 * - `local`: same host, known PID namespace and boot; the signal probe is trustworthy.
 * - `previous-boot`: same host, another (known) boot; the owner is dead.
 * - `foreign`: another host, another or unknown PID namespace (or an unknown boot); unknown, never dead.
 * - `untagged`: an older writer; callers keep their pre-locality probe.
 */
export type LockTokenLocality = "local" | "previous-boot" | "foreign" | "untagged";

export const lockTokenLocality = (token: string | undefined, self = ownerLocalityTag()): LockTokenLocality => {
  const owner = token === undefined ? null : LOCALITY_TOKEN.exec(token);
  const own = LOCALITY_TAG.exec(self);
  if (!owner || !own) return "untagged";
  const [, host, namespace, boot] = owner;
  // An unknown namespace on either side is never the same namespace (PR #864 P1).
  const sameNamespace = namespace !== LOCALITY_UNKNOWN && own[2] !== LOCALITY_UNKNOWN && namespace === own[2];
  if (host === own[1] && sameNamespace && boot === own[3]) return "local";
  if (host === own[1] && boot !== own[3] && boot !== LOCALITY_UNKNOWN && own[3] !== LOCALITY_UNKNOWN) {
    return "previous-boot";
  }
  return "foreign";
};

/**
 * Liveness of a short line-oriented lock (no heartbeat) whose token comes from this host with
 * an unknown PID namespace on either side. Its locality stays `foreign`, so the local signal
 * probe (ESRCH) proves nothing and is never consulted. As in lockOwnerLiveness, the creation
 * time is the heartbeat, with the same hold ceiling (SHORT_LOCK_MAX_HOLD_MS, or the caller's
 * longer stale bound as in exclusive locks): within it the owner is alive, past it dead and
 * reapable by the caller's dead-owner path (PR #864 P1). Positive evidence outranks age: with
 * `pid` given, an owner our procfs still shows (with its recorded `start`, when the record
 * carries one) stays alive however old (procfsShowsOwner). Any other token, including an
 * earlier boot of this host, another host, or a known namespace, returns undefined: the caller
 * keeps its locality path.
 */
export const unknownNamespaceLockLiveness = (
  token: string | undefined,
  createdAt: number,
  options: {
    staleMs?: number;
    now?: number;
    pid?: number;
    start?: string | undefined;
    procfsStart?: (pid: number) => string | undefined;
  } = {},
  self = ownerLocalityTag(),
): "alive" | "dead" | undefined => {
  const owner = token === undefined ? null : LOCALITY_TOKEN.exec(token);
  const own = LOCALITY_TAG.exec(self);
  if (!owner || !own || owner[1] !== own[1]) return undefined;
  if (owner[2] !== LOCALITY_UNKNOWN && own[2] !== LOCALITY_UNKNOWN) return undefined;
  if (lockTokenLocality(token, self) !== "foreign" || !Number.isFinite(createdAt)) return undefined;
  const maxHoldMs = Math.max(SHORT_LOCK_MAX_HOLD_MS, options.staleMs ?? 0);
  if ((options.now ?? Date.now()) - createdAt <= maxHoldMs) return "alive";
  if (options.pid !== undefined && procfsShowsOwner(options.pid, options.start, options.procfsStart)) return "alive";
  return "dead";
};

const boundedString = (value: unknown, max: number): value is string =>
  typeof value === "string" && value.length > 0 && value.length <= max;

/** Identity fields of a persisted record, or undefined for legacy/malformed ones. */
export const ownerIdentityFields = (
  value: unknown,
): Omit<OwnerIdentity, "pid"> | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (!boundedString(record.hostname, 255)) return undefined;
  if (record.pidNamespace !== undefined && !boundedString(record.pidNamespace, 128)) return undefined;
  if (record.bootId !== undefined && !boundedString(record.bootId, 128)) return undefined;
  if (typeof record.startedAt !== "number" || !Number.isFinite(record.startedAt)) return undefined;
  return {
    hostname: record.hostname,
    ...(record.pidNamespace === undefined ? {} : { pidNamespace: record.pidNamespace as string }),
    ...(record.bootId === undefined ? {} : { bootId: record.bootId as string }),
    startedAt: record.startedAt,
  };
};

/** One JSON line appended to line-oriented lock owner files. */
export const encodeOwnerIdentityLine = (): string => {
  const { pid: _pid, ...identity } = currentOwnerIdentity();
  return JSON.stringify(identity);
};

/** The identity line of a lock owner file; absent or malformed is legacy. */
export const decodeOwnerIdentityLine = (
  line: string | undefined,
): Omit<OwnerIdentity, "pid"> | undefined => {
  if (!line || line.length > MAX_IDENTITY_JSON_CHARS || !line.startsWith("{")) return undefined;
  try {
    return ownerIdentityFields(JSON.parse(line));
  } catch {
    return undefined;
  }
};

/** Optional heartbeat field of a persisted record. */
const heartbeatField = (value: unknown): number | undefined =>
  typeof value === "object" && value !== null &&
  typeof (value as { heartbeatAt?: unknown }).heartbeatAt === "number" &&
  Number.isFinite((value as { heartbeatAt: number }).heartbeatAt)
    ? (value as { heartbeatAt: number }).heartbeatAt
    : undefined;

/** Additive fields a long-lived JSON owner record carries. */
export interface OwnerHeartbeatFields {
  identity?: Omit<OwnerIdentity, "pid">;
  heartbeatAt?: number;
}

/** Identity and first heartbeat for a JSON owner record being acquired. */
export const ownerHeartbeatFields = (now = Date.now()): Required<OwnerHeartbeatFields> => {
  const { pid: _pid, ...identity } = currentOwnerIdentity();
  return { identity, heartbeatAt: now };
};

/**
 * Liveness of a persisted JSON owner record `{ pid, identity?, heartbeatAt? }`.
 * Malformed identity is treated as legacy; a non-numeric pid is dead.
 */
export const recordOwnerLiveness = (
  record: unknown,
  options: Omit<OwnerLivenessOptions, "heartbeatAt"> = {},
): OwnerLiveness => {
  if (typeof record !== "object" || record === null) return "dead";
  const { pid, identity: rawIdentity } = record as { pid?: unknown; identity?: unknown };
  if (typeof pid !== "number") return "dead";
  const identity = ownerIdentityFields(rawIdentity);
  const heartbeatAt = heartbeatField(record);
  return ownerLiveness(identity ? { ...identity, pid } : { pid }, {
    ...options,
    ...(heartbeatAt === undefined ? {} : { heartbeatAt }),
  });
};

export const ownerLiveness = (
  record: OwnerRecord,
  options: OwnerLivenessOptions = {},
): OwnerLiveness => {
  const probes = options.probes ?? {};
  const signal = probes.signal ?? signalProbe;
  if (typeof record.hostname !== "string") {
    return options.legacyAlive
      ? (options.legacyAlive(record.pid) ? "alive" : "dead")
      : signal(record.pid);
  }
  const self = (probes.self ?? currentOwnerIdentity)();
  const sameHost = record.hostname === self.hostname;
  // An absent PID namespace on either side is unknown, never the same place: two unreadable
  // namespaces prove nothing, and ESRCH from another namespace is not death (PR #864 P1).
  const sameNamespace = record.pidNamespace !== undefined && self.pidNamespace !== undefined &&
    record.pidNamespace === self.pidNamespace;
  if (
    sameHost &&
    sameNamespace &&
    record.bootId === self.bootId
  ) {
    const answer = signal(record.pid);
    if (answer !== "alive") return answer;
    // Same boot: a different start time under the same PID is reuse.
    if (record.bootId !== undefined && record.startedAt !== undefined) {
      const actual = (probes.processStartedAt ?? procStartedAt)(record.pid);
      if (actual !== undefined && Math.abs(actual - record.startedAt) > PID_START_TOLERANCE_MS) {
        return "dead";
      }
    }
    return "alive";
  }
  // Another or unknown namespace, another host, or another boot: the signal answer means
  // nothing here, so a fresh heartbeat is the only evidence of life.
  if (options.heartbeatAt !== undefined) {
    const now = (probes.now ?? Date.now)();
    if (now - options.heartbeatAt <= (options.heartbeatTtlMs ?? OWNER_HEARTBEAT_TTL_MS)) return "alive";
    // On this host with an unknown PID namespace (either side) the heartbeat is the authority:
    // stale beyond the same TTL is dead and reapable, never judged by ESRCH. Otherwise a single
    // crash would leave the owner unknown and wedge its lock forever (PR #864 P1).
    if (sameHost && (record.pidNamespace === undefined || self.pidNamespace === undefined)) {
      // No process outlives its kernel boot: a known other boot stays dead.
      if (record.bootId !== undefined && self.bootId !== undefined && record.bootId !== self.bootId) return "dead";
      // Positive evidence outranks heartbeat age: our procfs still shows the PID, with the
      // recorded start when the record carries one. Such an owner is alive, never reaped.
      return unknownNamespaceOwnerVisible(record, probes) ? "alive" : "dead";
    }
    // Another host or known namespace: a missed heartbeat is only absence of evidence, never
    // confirmed death: a paused owner holds no fencing token and may resume and write again.
    return "unknown";
  }
  // No process outlives its kernel boot (containers share the host's boot id).
  if (
    sameHost &&
    record.bootId !== undefined &&
    self.bootId !== undefined &&
    record.bootId !== self.bootId
  ) return "dead";
  return "unknown";
};

/**
 * Liveness of a line-oriented lock owner (`[token\n]pid\ncreated\n{identity}`).
 * Short locks never heartbeat, so their creation time is the heartbeat with a
 * generous hold ceiling.
 */
export const lockOwnerLiveness = (
  pid: number,
  createdAt: number,
  identityLine: string | undefined,
  options: Pick<OwnerLivenessOptions, "legacyAlive" | "probes"> & { maxHoldMs?: number } = {},
): OwnerLiveness => {
  const identity = decodeOwnerIdentityLine(identityLine);
  return ownerLiveness(identity ? { ...identity, pid } : { pid }, {
    ...options,
    ...(Number.isFinite(createdAt) ? { heartbeatAt: createdAt } : {}),
    heartbeatTtlMs: options.maxHoldMs ?? SHORT_LOCK_MAX_HOLD_MS,
  });
};

/**
 * Runs `refresh(now)` on an unref'd interval until the returned stop function
 * is called. Refresh failures are swallowed: an expired remote heartbeat
 * makes the owner unknown, not dead.
 */
export const startOwnerHeartbeat = (
  refresh: (heartbeatAt: number) => void,
  intervalMs = OWNER_HEARTBEAT_INTERVAL_MS,
): (() => void) => {
  const timer = setInterval(() => {
    try {
      refresh(Date.now());
    } catch {
      // The next beat retries; a stale remote heartbeat cannot prove death.
    }
  }, intervalMs);
  timer.unref?.();
  return () => clearInterval(timer);
};
