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
export const syncPathNamespace = (target: string, receipt?: Inode): void => {
  const walk = () => {
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
  const before = walk();
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
  if (JSON.stringify(walk().entries) !== JSON.stringify(before.entries)) {
    throw new Error("Namespace changed during durability barriers");
  }
};

/** Existence is not a receipt; retry every required directory barrier without a cache. */
export const syncDirectoryChain = (directory: string): void => {
  if (process.platform !== "win32") syncPathNamespace(directory);
};

export const writeFileAtomic = (
  filePath: string,
  contents: string,
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
