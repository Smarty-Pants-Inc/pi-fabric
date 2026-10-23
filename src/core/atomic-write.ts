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
import { diagnostics } from "./diagnostics.js";
import { randomUUID } from "node:crypto";
import childProcess from "node:child_process";
import fs from "node:fs";
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

export const runIncarnationCommand: IncarnationCommandRunner = (executable, args, options) => new Promise((resolve, reject) => {
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

/** Why the last stale check left a holder alone (smarty-dev#9569): its namespace is foreign or unprovable. */
export interface StaleLockNote { verdict?: "foreign" | "unproven"; namespace?: string; token?: string }

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
  /** Durable only: the post-rename directory barrier; default (and every other caller) syncDirectoryChain. */
  syncDirectory?: (directory: string) => void;
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
    return { entries, directories, parents, hasSymlinks: hops !== 0 };
};

/**
 * Whether syncPathNamespace can confirm a directory entry on this platform. Windows has no directory
 * fsync, so every namespace barrier below is skipped there (NTFS journals names, but nothing here
 * confirms it). The single platform fact behind syncPathNamespace, its async form, syncDirectoryChain
 * and the fail-closed durability variant.
 */
export const namespaceBarrierAvailable = (): boolean => process.platform !== "win32";

/** Builds the caller's typed not-durable error (for mesh.sync: a MeshSyncNotDurableError code). */
export type NamespaceNotDurable = (platform: NodeJS.Platform) => Error;

/**
 * Refuses, before any I/O, a durability barrier that would need a namespace (directory) step on a
 * platform without one (pi-fabric#894): a barrier that reports success to a caller, which then
 * advances an external cursor or delivered mark, must never skip the directory fsync silently.
 */
export const requireNamespaceBarrier = (notDurable: NamespaceNotDurable): void => {
  if (!namespaceBarrierAvailable()) throw notDurable(process.platform);
};

/**
 * syncPathNamespace for a durability barrier (mesh.sync, smarty-dev#9313): fails closed with
 * notDurable(platform) where syncPathNamespace would skip the directory fsync (win32), instead of
 * returning as if the namespace were confirmed. Ordinary atomic writes keep syncPathNamespace.
 */
export const syncPathNamespaceDurable = (target: string, receipt: Inode | undefined, notDurable: NamespaceNotDurable): void => {
  requireNamespaceBarrier(notDurable);
  syncPathNamespace(target, receipt);
};

/** The checked complete-namespace identity syncPathNamespace compares (not a durability receipt). */
export const namespaceKey = (target: string, receipt?: Inode): string => JSON.stringify(namespaceSnapshot(target, receipt).entries);

/** Read-only identity inspection using the SAME resolver as the full barrier. Not a durability receipt. */
export const namespaceIdentitySnapshot = (target: string, receipt?: Inode) => {
  const snapshot = namespaceSnapshot(target, receipt);
  return Object.freeze({ key: JSON.stringify(snapshot.entries), hasSymlinks: snapshot.hasSymlinks,
    directories: Object.freeze([...snapshot.directories].map(([directory, stat]) => Object.freeze({
      directory, dev: stat.dev, ino: stat.ino, mode: stat.mode & fs.constants.S_IFMT,
    }))),
  });
};

export const syncPathNamespace = (target: string, receipt?: Inode): void => {
  const before = namespaceSnapshot(target, receipt);
  if (namespaceBarrierAvailable()) {
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
  if (namespaceKey(target, receipt) !== JSON.stringify(before.entries)) {
    throw new Error("Namespace changed during durability barriers");
  }
};

/** Same namespace fence as the synchronous writer, with barriers off the event loop.
 * Always confirm all parent entries: an unchanged ancestor inode does not prove
 * that a newly created or replaced child directory is durably linked. */
export const syncPathNamespaceAsync = async (target: string, receipt: Inode): Promise<void> => {
  const before = namespaceSnapshot(target, receipt);
  if (namespaceBarrierAvailable()) {
    for (const [directory, expected] of [...before.directories].reverse()) {
      const handle = await fs.promises.open(directory, fs.constants.O_RDONLY);
      try {
        const opened = await handle.stat();
        if (!opened.isDirectory() || !sameInode(opened, expected)) throw new Error("Namespace directory changed before barrier");
        await handle.sync();
      } finally { await handle.close(); }
    }
  }
  if (namespaceKey(target, receipt) !== JSON.stringify(before.entries)) {
    throw new Error("Namespace changed during durability barriers");
  }
};
/**
 * A namespace barrier refused because a path it confirms was replaced meanwhile (a concurrent
 * rename: compaction, a state commit). The fsyncs themselves succeeded, so a new barrier may run.
 * Never true for an I/O error: a failed fsync is not retried (its dirty pages may be dropped).
 */
export const isNamespaceRace = (error: unknown): boolean => error instanceof Error &&
  /^(Namespace (changed|directory changed|link changed)|Session receipt inode changed)/.test(error.message);

/** Existence is not a receipt; retry every required directory barrier without a cache. */
export const syncDirectoryChain = (directory: string): void => {
  if (namespaceBarrierAvailable()) syncPathNamespace(directory);
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
    if (options?.durable) (options.syncDirectory ?? syncDirectoryChain)(directory);
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
  try { process.kill(pid, 0); return true; } catch { return false; }
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
        fs.writeFileSync(ownerPath, `${token}\n${process.pid}\n${Date.now()}\n`, {
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
        const [, pidText, createdText] = firstOwner.trim().split("\n");
        const stale = Date.now() - Number(createdText) > staleMs;
        if (stale && !exclusiveLockProcessAlive(Number(pidText))) {
          const secondOwner = fs.readFileSync(ownerPath, "utf8");
          if (
            secondOwner === firstOwner &&
            reapStaleLock(lock, (claimed) => {
              try {
                const owner = fs.readFileSync(path.join(claimed, "owner"), "utf8");
                const [, pid, created] = owner.trim().split("\n");
                return Date.now() - Number(created) > staleMs && !exclusiveLockProcessAlive(Number(pid));
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
export const MESH_INCARNATION_PENDING_CODE = "FABRIC_MESH_INCARNATION_PENDING";
export const MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE = "FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED";

/** Constructor-owned process identity is not ready: no mesh lock acquisition was attempted. */
export class MeshIncarnationPendingError extends Error {
  readonly code = MESH_INCARNATION_PENDING_CODE;
  constructor() {
    super(`${MESH_INCARNATION_PENDING_CODE}: Fabric mesh process incarnation preparation is still pending`);
    this.name = "MeshIncarnationPendingError";
  }
}

/** Also recognizes the bridge's deliberately narrow error wire, without classifying pending as a timeout. */
export const isMeshIncarnationPending = (error: unknown): error is Error & { code: typeof MESH_INCARNATION_PENDING_CODE } =>
  error instanceof Error && "code" in error && error.code === MESH_INCARNATION_PENDING_CODE;

/** Optimistic state snapshots kept changing before admission: no write was committed. */
export class MeshSnapshotRetriesExhaustedError extends Error {
  readonly code = MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE;
  constructor(readonly attempts: number) {
    super(`${MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE}: Fabric mesh state snapshot retries exhausted after ${attempts} validation attempts`);
    this.name = "MeshSnapshotRetriesExhaustedError";
  }
}

/** The narrow bridge wire retains the refusal code/message, not arbitrary error properties. */
export const isMeshSnapshotRetriesExhausted = (error: unknown): error is Error & { code: typeof MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE } =>
  error instanceof Error && "code" in error && error.code === MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE;

/** A failed acquisition wrote nothing. Foreground callers must see this error, not a retry. */
/** smarty-dev#10072: a gap between attempts this long means the waiter itself was not running (event-loop or paging stall). */
export const MESH_LOCK_STALLED_GAP_MS = 1_000;

export class MeshLockTimeoutError extends Error {
  readonly code = MESH_LOCK_TIMEOUT_CODE;
  /** smarty-dev#10072: the waiter stalled (largest gap >= 1 s), so this timeout may not be contention; meters count it apart. */
  readonly stalled: boolean;
  constructor(readonly holder: string, readonly attempts: number, readonly maxGapMs: number) {
    const stalled = maxGapMs >= MESH_LOCK_STALLED_GAP_MS;
    super(`${MESH_LOCK_TIMEOUT_CODE}: Timed out waiting for the Fabric mesh lock${holder} after ${attempts} attempts, largest gap between attempts ${maxGapMs} ms${stalled ? `, waiter stalled ${maxGapMs} ms` : ""}`);
    this.name = "MeshLockTimeoutError";
    this.stalled = stalled;
  }
}

/**
 * A lock whose holder this process may not reclaim on pid evidence (smarty-dev#9569): another PID namespace, or one
 * it cannot prove. Still a MeshLockTimeoutError (callers keep their retry handling), with its own code.
 */
export class MeshLockForeignHolderError extends MeshLockTimeoutError {
  readonly foreignCode: "FABRIC_MESH_LOCK_FOREIGN_HOLDER" | "FABRIC_MESH_LOCK_UNPROVEN_HOLDER";
  /** Operator recovery command for valid owner metadata; malformed tokens remain non-operational placeholders. */
  readonly recovery: string;
  /** The same command as an argument vector: the safe form for a program to run. */
  readonly recoveryArgv: string[];
  readonly holderNamespace: string;
  readonly holderToken: string;
  constructor(holder: string, attempts: number, maxGapMs: number, readonly verdict: "foreign" | "unproven",
    holderNamespace: unknown, readonly lockPath = "", holderToken: unknown = "") {
    // Project disk metadata before retaining it or formatting any diagnostics. Keep this local: the namespace
    // reader is optional/lazy, and these checks must not alter owner bytes or reclaim decisions.
    const uuid = "[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}";
    const token = typeof holderToken === "string" && (holderToken.length === 32 || holderToken.length === 36) &&
      new RegExp(`^(?:[0-9A-Fa-f]{32}|${uuid})$`).test(holderToken) ? holderToken : "invalid-token";
    const namespace = holderNamespace === "" ? "" : typeof holderNamespace === "string" && holderNamespace.length <= 128 &&
      new RegExp(`^(?:linux:(?:[0-9a-f]{32}|-):${uuid}:\\d+|darwin:(?:${uuid}|-):${uuid}:host|win32:${uuid}:\\d+:host)(?![\\s\\S])`).test(holderNamespace)
        ? holderNamespace : "invalid-namespace";
    super(holder, attempts, maxGapMs);
    this.holderNamespace = namespace;
    this.holderToken = token;
    this.foreignCode = verdict === "foreign" ? "FABRIC_MESH_LOCK_FOREIGN_HOLDER" : "FABRIC_MESH_LOCK_UNPROVEN_HOLDER";
    const kind = path.basename(lockPath) === ".lock" ? "mesh" : "custody";
    this.recoveryArgv = ["fabric-mesh-backend", "lock-recover", "--root", path.dirname(lockPath), "--lock", kind, "--holder", token];
    this.recovery = renderRecoveryCommand(this.recoveryArgv);
    const where = verdict === "foreign" ? `another PID namespace (${namespace})`
      : namespace ? `a PID namespace this process cannot prove (${namespace})` : "no recorded PID namespace (a record from before protocol 3)";
    this.message = `${this.foreignCode}: ${this.message}; the holder of ${lockPath} is in ${where}, so it is never reclaimed on pid evidence. ` +
      `Recover it from inside the holder's namespace with: ${this.recovery} ; or, once the holder is STOPPED, add --unsafe-holder-stopped`;
    this.name = "MeshLockForeignHolderError";
  }
}

/**
 * The recovery command for this platform's shell (pi-fabric#923 round 4). The root and the token come from disk, so
 * each argument is single-quoted: POSIX (`'` becomes `'\''`) or PowerShell on Windows (`'` becomes `''`). A copied
 * command then runs nothing else.
 */
export const renderRecoveryCommand = (argv: string[], platform: NodeJS.Platform = process.platform): string =>
  argv.map(value => /^[A-Za-z0-9._/=+-]+$/.test(value) ? value // plain words stay bare (the command name too)
    : platform === "win32" ? `'${value.replace(/'/g, "''")}'` : `'${value.replace(/'/g, "'\\''")}'`).join(" ");

const alarmedHolders = new Set<string>();
/**
 * The lock-timeout alarm line for a holder this process may not reclaim, once per holder per process (the same
 * `[pi-fabric] ALARM` path as custody-unrecoverable). A mesh publish cannot carry it: publishing takes the stuck lock.
 */
export const alarmForeignHolder = (error: MeshLockForeignHolderError): void => {
  const key = `${error.lockPath}\n${error.holderToken}`;
  if (alarmedHolders.has(key)) return;
  alarmedHolders.add(key);
  try { diagnostics.warn("src/core/atomic-write.ts", `[pi-fabric] ALARM ${error.foreignCode}: ${error.message}`); } catch { /* an alarm never masks the refusal */ }
};

// Compatible with older stores and the bridge's deliberately narrow error wire.
export const isMeshLockTimeout = (error: unknown): error is Error & { code: typeof MESH_LOCK_TIMEOUT_CODE } =>
  error instanceof Error && "code" in error && error.code === MESH_LOCK_TIMEOUT_CODE;

/** Shared with state-backend.ts so the core deferred-write path does not import the storage backend. */
export const MESH_STATE_WAL_CAP_CODE = "FABRIC_MESH_STATE_WAL_CAP";
export const isMeshStateWalCap = (error: unknown): boolean =>
  error instanceof Error && (error as { code?: unknown }).code === MESH_STATE_WAL_CAP_CODE;

/** A write that wrote nothing and may succeed later: pending identity, snapshot validation exhaustion,
 * a lock timeout, or a SQLite WAL-cap refusal (`FABRIC_MESH_STATE_WAL_CAP`; smarty-dev#7451 item 8). */
export const isMeshWriteDeferred = (error: unknown): error is Error =>
  isMeshIncarnationPending(error) || isMeshSnapshotRetriesExhausted(error) || isMeshLockTimeout(error) ||
  isMeshStateWalCap(error);

/** Best-effort protocol writes may swallow conflicts, but not retryable admission refusals.
 * Retain the historical name and the same refusals that previously carried the timeout code. */
export const rethrowMeshLockTimeout = (error: unknown): undefined => {
  if (isMeshIncarnationPending(error) || isMeshSnapshotRetriesExhausted(error) || isMeshLockTimeout(error)) throw error;
  return undefined;
};

export const DEFAULT_MESH_RETRY_WARNING_WINDOW_MS = 60_000;

/** `PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS`: positive milliseconds; blank or invalid uses 60 s. */
export const meshRetryWarningWindowMs = (): number => {
  const raw = process.env.PI_FABRIC_MESH_RETRY_WARNING_WINDOW_MS;
  const value = Number(raw);
  return raw?.trim() && Number.isFinite(value) && value >= 1 ? Math.floor(value) : DEFAULT_MESH_RETRY_WARNING_WINDOW_MS;
};

/** A deferred-write outage (admission refusal, lock timeout or WAL-cap refusal) this long on one background path raises one alarm
 * (smarty-dev#8825). */
export const MESH_OUTAGE_ALARM_AFTER_MS = 5 * 60 * 1_000;
/** An outage ends only after this many consecutive confirmed writes, so one lucky write does not restart the window. */
export const MESH_OUTAGE_RECOVERY_SUCCESSES = 3;
/** The structured stderr tag of the outage alarm: the journal keeps it even when the mesh never accepts the event. */
export const MESH_OUTAGE_ALARM_TAG = "fabric.alarm.mesh-outage";
/** Bounds of a path's undelivered alarm events (pi-fabric#893 round 5): the queue keeps the newest 32; an event is
 * tried at most 8 times or for 30 min, whichever comes first. A drop writes one journal alarm line (reason
 * mesh-alarm-undeliverable) with the counts. */
export const MESH_ALARM_QUEUE_MAX = 32;
export const MESH_ALARM_MAX_ATTEMPTS = 8;
export const MESH_ALARM_MAX_AGE_MS = 30 * 60 * 1_000;
/** Every stderr alarm line is `fabric.alarm.mesh-outage {json}`, the one shape smarty-dev#9729's factory reader pages
 * on: label (string), state ("outage" | "recovered"), sinceMs (epoch ms, int), optional durationMs (int) and reason
 * (string); extra fields are allowed. */
/** One sink attempt's deadline (smarty-dev#9848): a publish that never settles counts as a failed attempt, so the
 * attempt and age limits still run; its late settle is ignored. */
export const MESH_ALARM_ATTEMPT_MS = 10_000;
class MeshAlarmAttemptTimeout extends Error {}
const withAttemptDeadline = (attempt: Promise<unknown> | void): Promise<unknown> => {
  if (!attempt) return Promise.resolve();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new MeshAlarmAttemptTimeout("mesh alarm publish did not settle")), MESH_ALARM_ATTEMPT_MS);
    timer.unref?.();
  });
  Promise.resolve(attempt).catch(() => undefined); // a late rejection after the deadline is ignored, never unhandled
  return Promise.race([attempt, deadline]).finally(() => clearTimeout(timer));
};
export const MESH_JOURNAL_ALARM_TAG = MESH_OUTAGE_ALARM_TAG;
export const meshJournalAlarmLine = (alarm: { label: string; state: "outage" | "recovered"; sinceMs: number; durationMs?: number;
  reason?: string; [extra: string]: unknown }): string =>
  `${MESH_JOURNAL_ALARM_TAG} ${JSON.stringify(alarm)}`;
export interface MeshOutageAlarm { label: string; state: "outage" | "recovered"; sinceMs: number; durationMs: number; reason?: string }

let journalToolUnavailable = false;
let journalMirrorFailures = 0;
/** Diagnostic count of failed native journal attempts (each child counts at most once). */
export const meshJournalMirrorFailures = (): number => journalMirrorFailures;

/** Pane stderr is not necessarily journal-backed (smarty-dev#9657). Only probe at alarm time. */
const mirrorMeshJournalLine = (line: string): void => {
  if (process.platform !== "linux" || journalToolUnavailable) return;
  const stream = process.env.JOURNAL_STREAM;
  if (stream && /^\d+:\d+$/.test(stream)) {
    try {
      const stat = fs.fstatSync(2, { bigint: true });
      if (stream === `${stat.dev}:${stat.ino}`) return;
    } catch { /* Unknown stderr identity needs the independent journal sink. */ }
  }
  let failed = false;
  const fail = (error: unknown): void => {
    if (errorCode(error) === "ENOENT") journalToolUnavailable = true;
    if (!failed) { failed = true; journalMirrorFailures++; }
  };
  try {
    // No shell/tool lookup, no await: a broken journal never blocks mesh delivery. execFile
    // owns child errors and its kill deadline; stdin needs its own local error boundary.
    // Fixed trusted system path: a pane's PATH may start with a writable project directory.
    const child = childProcess.execFile("/usr/bin/systemd-cat", ["--identifier", MESH_OUTAGE_ALARM_TAG], {
      // systemd-cat also resolves its default cat child: both executable paths must be trusted.
      timeout: 2_000, maxBuffer: 4_096, env: { PATH: "/usr/bin:/bin" },
    }, (error) => { if (error) fail(error); });
    child.unref();
    if (!child.stdin) { fail(undefined); return; }
    child.stdin.on("error", fail);
    child.stdin.end(`${line}\n`);
  } catch (error) { fail(error); }
};
export const emitMeshJournalAlarm = (alarm: Parameters<typeof meshJournalAlarmLine>[0]): void => {
  const line = meshJournalAlarmLine(alarm);
  diagnostics.warn("src/core/atomic-write.ts", line);
  mirrorMeshJournalLine(line);
};

/** A log-safe description of a deferred write (pi-fabric#893 round 3). Only a typed WAL-cap refusal selects the
 * numeric/count-only summary; ordinary errors and messages retain their own diagnostics. */
export const meshDeferredSummary = (error: unknown): string => {
  const details = error as { code?: unknown; message?: unknown } | null | undefined;
  const code = details?.code;
  if (isMeshStateWalCap(error)) {
    const capped = error as { walBytes?: unknown; capBytes?: unknown; readerCount?: unknown };
    const readers = typeof capped.readerCount === "number" && Number.isSafeInteger(capped.readerCount) && capped.readerCount >= 0
      ? capped.readerCount : "unknown";
    if (typeof capped.walBytes !== "number" || !Number.isFinite(capped.walBytes) || capped.walBytes < 0
        || typeof capped.capBytes !== "number" || !Number.isFinite(capped.capBytes) || capped.capBytes <= 0) {
      return `FABRIC_MESH_STATE_WAL_CAP (sizes not available); ${readers} WAL reader(s)`;
    }
    return `FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is ${String(capped.walBytes)} bytes, above the ${String(capped.capBytes)}-byte cap; `
      + `${readers} WAL reader(s)`;
  }
  if (isMeshIncarnationPending(error) || isMeshSnapshotRetriesExhausted(error) || isMeshLockTimeout(error)) return error.message;
  const message = error instanceof Error ? error.message
    : typeof details?.message === "string" ? details.message : String(error);
  const prefix = (typeof code === "string" && code.length > 0) || typeof code === "number" ? `${String(code)}: ` : "";
  return `${prefix}${message}`;
};

type PendingAlarm = { alarm: MeshOutageAlarm; attempts: number; firstAt: number };

/** Per-background-path outage state; no process-global handlers or foreground retry policy. */
export class MeshBackgroundRetry {
  // smarty-dev#8825 outage alarm. Per instance by design: a restarted process starts a new 5-min window and
  // observes a still-running outage again. The window runs on the monotonic clock (performance.now).
  #outageSince: number | undefined;
  #outageSinceWall = 0;
  #outageRaised = false;
  #outageSuccesses = 0;
  // Events not yet accepted by the sink, in order (the alarm, then its recovery). The publish can hit the same
  // outage: a rejected or throwing publish keeps its event, and the next failure() or success() retries it.
  readonly #pendingAlarms: PendingAlarm[] = [];
  #flushing = false;
  // pi-fabric#979 Astra P2: the one retry a timed-out attempt schedules (a hung sink may be the last trigger).
  #retryFlush: ReturnType<typeof setTimeout> | undefined;
  #closed = false;
  #droppedOverflow = 0;
  #droppedExhausted = 0;
  #undeliverableReported = false;
  #alarm: ((alarm: MeshOutageAlarm) => Promise<unknown> | void) | undefined;
  #alarmAfterMs = MESH_OUTAGE_ALARM_AFTER_MS;
  /** Where this path's outage alarm goes (the owner's mesh publish). The stderr line is written regardless.
   * `afterMs`: the outage bound (tests shorten it). */
  alarmsTo(sink: (alarm: MeshOutageAlarm) => Promise<unknown> | void, afterMs = MESH_OUTAGE_ALARM_AFTER_MS): this {
    this.#alarm = sink;
    this.#alarmAfterMs = afterMs;
    return this;
  }
  /** No outage being tracked and every alarm event accepted: an owner may drop this retry (pi-fabric#893 round 3). */
  get idle(): boolean { return this.#outageSince === undefined && this.#pendingAlarms.length === 0 && !this.#flushing; }
  /** Retry the pending alarm events now (an owner whose path has no further writes). */
  flushAlarms(): Promise<void> { return this.#flushAlarms(); }
  /** Clears a scheduled retry flush; no new one is scheduled after close. */
  close(): void {
    this.#closed = true;
    if (this.#retryFlush) clearTimeout(this.#retryFlush);
    this.#retryFlush = undefined;
  }
  #raise(alarm: MeshOutageAlarm): void {
    // The independent sink: one structured line, written at alarm time whatever the mesh does.
    emitMeshJournalAlarm({ ...alarm });
    if (this.#alarm) {
      this.#pendingAlarms.push({ alarm, attempts: 0, firstAt: performance.now() });
      while (this.#pendingAlarms.length > MESH_ALARM_QUEUE_MAX) { this.#pendingAlarms.shift(); this.#droppedOverflow++; this.#reportUndeliverable(); }
    }
    void this.#flushAlarms();
  }
  // One owner-visible journal line per run of drops (until an event is delivered again), with the running counts.
  #reportUndeliverable(): void {
    if (this.#undeliverableReported) return;
    this.#undeliverableReported = true;
    emitMeshJournalAlarm({ label: this.label, state: "outage", sinceMs: Date.now(), reason: "mesh-alarm-undeliverable",
      droppedOverflow: this.#droppedOverflow, droppedAfterRetries: this.#droppedExhausted, queued: this.#pendingAlarms.length });
  }
  /** Remove `head` by identity (pi-fabric#893 Astra P1): while its publish was awaited, an overflow in #raise may
   * already have dropped (and counted) it, and the queue's first entry is then the NEXT, unattempted event. */
  #removeHead(head: PendingAlarm): boolean {
    const at = this.#pendingAlarms.indexOf(head);
    if (at >= 0) this.#pendingAlarms.splice(at, 1);
    return at >= 0;
  }
  async #flushAlarms(): Promise<void> {
    if (this.#flushing) return;
    this.#flushing = true;
    try {
      while (this.#pendingAlarms.length && this.#alarm) {
        const head = this.#pendingAlarms[0]!;
        head.attempts++;
        try { await withAttemptDeadline(this.#alarm(head.alarm)); }
        catch (error) {
          // Kept for the next call, until its attempts or age run out: then dropped, and reported once.
          if (head.attempts >= MESH_ALARM_MAX_ATTEMPTS || performance.now() - head.firstAt >= MESH_ALARM_MAX_AGE_MS) {
            if (this.#removeHead(head)) { this.#droppedExhausted++; this.#reportUndeliverable(); } // else counted as overflow
            continue;
          }
          // A timed-out attempt may have swallowed the last trigger (calls made while it was in flight returned at
          // the #flushing guard): it schedules one retry itself, so the attempt and age limits still run. A sink that
          // rejects keeps the old contract (the next failure() or success() retries). One unref'd timer, no loop.
          if (error instanceof MeshAlarmAttemptTimeout && !this.#closed && !this.#retryFlush) {
            this.#retryFlush = setTimeout(() => { this.#retryFlush = undefined; void this.#flushAlarms(); }, 0);
            this.#retryFlush.unref?.();
          }
          return;
        }
        this.#removeHead(head);
        this.#undeliverableReported = false;
      }
    } finally { this.#flushing = false; }
  }
  #delay = 0;
  #retryAt = 0;
  #lastReportedAt: number | undefined;
  #recoveryMessage = "mesh lock recovered";
  #suppressed = 0;
  #running = false;
  // Opt-in (backoffFailures) state for non-transient failures; the default boundary never sets it.
  #failureBackoff: { minMs: number; maxMs: number } | undefined;
  #failureDelay = 0;
  #failureMessage: string | undefined;
  #failureReportedAt: number | undefined;
  #failureSuppressed = 0;
  constructor(readonly label: string, readonly minMs = 100, readonly maxMs = 5_000,
    readonly warningWindowMs = meshRetryWarningWindowMs()) {}

  /** Opt-in for a self-retriggering path (a periodic refresh, not a queue): a non-transient failure stays
   * `failed` but waits out a deterministic exponential backoff (minMs doubling to maxMs) before run() tries
   * again, and the same repeated message is printed at most once per warning window. A confirmed success()
   * clears the backoff. Default instances keep the immediate-retry semantics. */
  backoffFailures(minMs: number, maxMs: number): this {
    const min = Math.max(1, Math.floor(minMs));
    this.#failureBackoff = { minMs: min, maxMs: Math.max(min, Math.floor(maxMs)) };
    return this;
  }

  get waitMs(): number { return Math.max(0, this.#retryAt - Date.now()); }
  #resetBackoff(): void { this.#delay = 0; this.#retryAt = 0; this.#failureDelay = 0; }
  /** A confirmed acquisition: reset backoff, never the warning window (smarty-dev#8305). `final`: the path's work is
   * done (a one-shot write such as one actor's presence), so its outage ends now, with no further writes to wait for. */
  success(final = false): void {
    this.#resetBackoff();
    if (this.#outageSince !== undefined && (++this.#outageSuccesses >= MESH_OUTAGE_RECOVERY_SUCCESSES || final)) {
      if (this.#outageRaised) this.#raise({ label: this.label, state: "recovered", sinceMs: this.#outageSinceWall,
        durationMs: Math.round(performance.now() - this.#outageSince) });
      this.#outageSince = undefined;
      this.#outageRaised = false;
      this.#outageSuccesses = 0;
    }
    void this.#flushAlarms(); // a pending alarm goes out before its recovery (queue order)
    if (!this.#suppressed || this.#lastReportedAt === undefined) return;
    const now = Date.now();
    const elapsedMs = now - this.#lastReportedAt;
    if (elapsedMs < this.warningWindowMs) return;
    // Final summary: retries suppressed after the last line are reported once the window
    // has passed, so an outage that ends inside a window is not silently dropped. It takes
    // that window's single line; a later outage inside it is counted, not printed.
    diagnostics.warn("src/core/atomic-write.ts", `[pi-fabric] ${this.label}: ${this.#recoveryMessage} (${this.#suppressed} similar retries suppressed in the last ${Math.floor(elapsedMs / 1_000)}s)`);
    this.#lastReportedAt = now;
    this.#suppressed = 0;
  }
  failure(error: unknown): boolean {
    const transient = isMeshWriteDeferred(error);
    this.#outageSuccesses = 0; // any failure breaks the recovery streak (pi-fabric#893 CODE P2)
    if (!transient) {
      // Contain the owned callback, but surface unrelated bugs and preserve its existing
      // retry cadence. Only a typed acquisition failure earns lock backoff/deduplication.
      // Not proof that the lock is free: no recovery summary, suppression is kept.
      const message = error instanceof Error ? error.message : String(error);
      if (this.#failureBackoff) {
        this.#failBackoff(message);
        return false;
      }
      this.#resetBackoff();
      diagnostics.warn("src/core/atomic-write.ts", `[pi-fabric] ${this.label}: background operation failed: ${message}`);
      return false;
    }
    this.#delay = Math.min(this.maxMs, Math.max(this.minMs, this.#delay * 2));
    // Keep the exponential ceiling separate from the randomized draw. Timers
    // have a 1ms scheduling floor so a zero draw cannot form a microtask spin.
    const delayMs = Math.max(1, retryDelayMs(0, this.#delay, this.maxMs));
    const now = Date.now();
    this.#retryAt = now + delayMs;
    const clock = performance.now();
    if (this.#outageSince === undefined) { this.#outageSince = clock; this.#outageSinceWall = now; }
    this.#outageSuccesses = 0;
    const refusal = isMeshIncarnationPending(error) ? "mesh incarnation pending"
      : isMeshSnapshotRetriesExhausted(error) ? "mesh snapshot retries exhausted" : undefined;
    this.#recoveryMessage = isMeshIncarnationPending(error) ? "mesh incarnation preparation ready"
      : isMeshSnapshotRetriesExhausted(error) ? "mesh snapshot validation recovered" : "mesh lock recovered";
    if (!this.#outageRaised && clock - this.#outageSince >= this.#alarmAfterMs) {
      this.#outageRaised = true;
      this.#raise({ label: this.label, state: "outage", sinceMs: this.#outageSinceWall, durationMs: Math.round(clock - this.#outageSince),
        reason: refusal ?? (isMeshLockTimeout(error) ? "mesh lock timeout" : "SQLite WAL cap") });
    } else void this.#flushAlarms();
    const elapsedMs = this.#lastReportedAt === undefined ? Infinity : now - this.#lastReportedAt;
    // At most one lock-retry line per instance per window (default 60 s), even across
    // successful acquisitions: a flapping lock must not print one warning per short outage.
    // The first warning is immediate and carries the holder diagnostics.
    if (elapsedMs >= this.warningWindowMs) {
      const summary = this.#suppressed > 0
        ? ` (${this.#suppressed} similar retries suppressed in the last ${Math.floor(elapsedMs / 1_000)}s)` : "";
      diagnostics.warn("src/core/atomic-write.ts", `[pi-fabric] ${this.label}: ${refusal ?? (isMeshLockTimeout(error) ? "mesh lock timeout" : "mesh WAL cap")}; retrying in ${delayMs} ms: ${meshDeferredSummary(error)}${summary}`);
      this.#lastReportedAt = now;
      this.#suppressed = 0;
    } else {
      this.#suppressed++;
    }
    return transient;
  }

  #failBackoff(message: string): void {
    const { minMs, maxMs } = this.#failureBackoff!;
    this.#failureDelay = Math.min(maxMs, Math.max(minMs, this.#failureDelay * 2));
    const now = Date.now();
    this.#retryAt = now + this.#failureDelay;
    const elapsedMs = this.#failureReportedAt === undefined ? Infinity : now - this.#failureReportedAt;
    if (message === this.#failureMessage && elapsedMs < this.warningWindowMs) {
      this.#failureSuppressed++;
      return;
    }
    const summary = this.#failureSuppressed > 0
      ? ` (${this.#failureSuppressed} similar failures suppressed in the last ${Math.floor(elapsedMs / 1_000)}s)` : "";
    diagnostics.warn("src/core/atomic-write.ts", `[pi-fabric] ${this.label}: background operation failed: ${message}; retrying in ${this.#failureDelay} ms${summary}`);
    this.#failureMessage = message;
    this.#failureReportedAt = now;
    this.#failureSuppressed = 0;
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
      diagnostics.warn("src/core/atomic-write.ts", "[pi-fabric] background mesh notification queue full; dropping newest notification");
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
    this.#retry.close();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    await this.#draining;
    for (const item of this.#pending.splice(0)) item.attempted();
  }
}
