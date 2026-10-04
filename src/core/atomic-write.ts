import { retryDelayMs } from "./retry-backoff.js";
import { randomUUID } from "node:crypto";
import childProcess from "node:child_process";
import fs from "node:fs";
import path from "node:path";

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
  childProcess.execFile(executable, args, { ...options, encoding: "utf8" }, (error, stdout) => {
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
  const read = async (pid: number): Promise<string | undefined> => {
    if (!Number.isSafeInteger(pid) || pid <= 0) return undefined;
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
        timer = setTimeout(() => { abort.abort(); resolve(undefined); }, timeoutMs);
      });
      try {
        const start = await Promise.race([run(executable, args, {
          timeout: timeoutMs, maxBuffer: 4_096, windowsHide: true, signal: abort.signal,
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

/** Fresh holder evidence; macOS second-resolution reuse stays conservative. */
export const processIncarnation = (pid: number): Promise<string | undefined> => nativeReader().read(pid);
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

type Inode = Pick<fs.Stats, "dev" | "ino">;
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
    if (receipt && !sameInode(receipt, endpoint)) throw new Error("Session receipt inode changed during namespace confirmation");
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
    fs.rmSync(temporary, { force: true });
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
  if (!acquired) throw new Error(options.timeoutMessage);
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
  try {
    await fs.promises.writeFile(temporary, contents, {
      encoding: "utf8",
      mode: options?.mode ?? 0o600,
    });
    await renameAtomicAsync(temporary, filePath, options);
  } finally {
    await fs.promises.rm(temporary, { force: true });
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

/** Best-effort protocol writes may swallow conflicts, but not a retryable lock wait. */
export const rethrowMeshLockTimeout = (error: unknown): undefined => {
  if (isMeshLockTimeout(error)) throw error;
  return undefined;
};

/** Per-background-path outage state; no process-global handlers or foreground retry policy. */
export class MeshBackgroundRetry {
  #delay = 0;
  #retryAt = 0;
  #reported = false;
  #running = false;
  constructor(readonly label: string, readonly minMs = 100, readonly maxMs = 5_000) {}

  get waitMs(): number { return Math.max(0, this.#retryAt - Date.now()); }
  success(): void { this.#delay = 0; this.#retryAt = 0; this.#reported = false; }
  failure(error: unknown): boolean {
    const transient = isMeshLockTimeout(error);
    if (!transient) {
      // Contain the owned callback, but surface unrelated bugs and preserve its existing
      // retry cadence. Only a typed acquisition failure earns lock backoff/deduplication.
      this.success();
      console.warn(`[pi-fabric] ${this.label}: background operation failed: ${error instanceof Error ? error.message : String(error)}`);
      return false;
    }
    this.#delay = Math.min(this.maxMs, Math.max(this.minMs, this.#delay * 2));
    // Keep the exponential ceiling separate from the randomized draw. Timers
    // have a 1ms scheduling floor so a zero draw cannot form a microtask spin.
    const delayMs = Math.max(1, retryDelayMs(0, this.#delay, this.maxMs));
    this.#retryAt = Date.now() + delayMs;
    if (!this.#reported) {
      // Includes the holder and scheduler-stall diagnostics. Once per continuous outage,
      // not once per poll, which would flood a throttled host's stderr.
      console.warn(`[pi-fabric] ${this.label}: ${transient ? "mesh lock timeout; retrying" : "background operation failed"} in ${delayMs} ms: ${error instanceof Error ? error.message : String(error)}`);
      this.#reported = true;
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
