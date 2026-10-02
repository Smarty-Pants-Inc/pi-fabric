import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export interface AtomicWriteOptions {
  // File mode for the committed file (default 0o600) and for mkdir -p of its
  // parent directory (default 0o700).
  mode?: number;
  dirMode?: number;
  // Opt in to stable-storage ordering: sync the file before rename, then its directory
  // and all containing directories through the filesystem root (except on Windows).
  durable?: boolean;
  // An explicitly prepared, identity-bound namespace receipt lets a hot writer
  // sync only its changed parent entry. Ordinary durable callers still sync the full chain.
  directoryReceipt?: DurableDirectory;
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
const walkPathNamespace = (target: string, receipt?: Inode) => {
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
    return { entries, directories, parents, endpoint: current, stat: endpoint };
};

const syncNamespace = (target: string, receipt?: Inode) => {
  const before = walkPathNamespace(target, receipt);
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
  if (JSON.stringify(walkPathNamespace(target, receipt).entries) !== JSON.stringify(before.entries)) {
    throw new Error("Namespace changed during durability barriers");
  }
  return before;
};

export const syncPathNamespace = (target: string, receipt?: Inode): void => {
  syncNamespace(target, receipt);
};

const directoryReceiptOf = (walk: ReturnType<typeof walkPathNamespace>): string => JSON.stringify([
  walk.entries,
  // The leaf's own entries are the hot writes. Its ancestors are not: their
  // change times catch a renamed-away-and-back inode even if identity is reused.
  [...walk.directories].filter(([directory]) => directory !== walk.endpoint)
    .map(([directory, stat]) => [directory, stat.ctimeMs]),
]);

/** A successful full-namespace barrier, not an existence cache. Lazily prepare
 * outside a hot writer's lock; every commit rechecks all directory/link identities.
 * Unchanged ancestors owe no new barrier when only the leaf's entries change.
 * Ancestor change times also invalidate a same-inode detach/reattach. The leaf
 * change time is excluded because state/lock entry writes legitimately change it.
 */
export class DurableDirectory {
  #prepared: string | undefined;
  #identities: string | undefined;

  constructor(readonly directory: string) {}

  prepare(): void {
    try {
      const current = walkPathNamespace(this.directory);
      if (!current.stat.isDirectory()) throw new Error("Durability receipt is not a directory");
      const entries = directoryReceiptOf(current);
      if (this.#prepared === entries) return;
      // Never keep a receipt after a failed/replaced namespace barrier. A later
      // attempt (including a fresh process) must retry the entire owed chain.
      this.#prepared = undefined;
      const synced = syncNamespace(this.directory);
      this.#prepared = directoryReceiptOf(synced);
      this.#identities = JSON.stringify(synced.entries);
    } catch (error) {
      this.#prepared = undefined;
      throw error;
    }
  }

  sync(directory: string): void {
    if (path.resolve(directory) !== path.resolve(this.directory)) throw new Error("Durability receipt directory mismatch");
    try {
      // Walk the actual writer path too: lexical normalization alone is unsafe
      // when a symlink is followed by dot-dot.
      // Identity alone misses same-inode detach/reattach during an awaited file
      // barrier, or even during the namespace barrier itself. Reconfirm changed
      // evidence before ack; bounded retries tolerate benign sibling churn.
      let confirmed = this.#prepared;
      // Concurrent sibling writers can change ancestor ctimes during each
      // sweep. Allow a bounded burst without relaxing any inode/ctime check or
      // acknowledging before the last changed ancestor has been reconfirmed.
      for (let attempt = 0; attempt < 16; attempt++) {
        const before = walkPathNamespace(directory);
        if (confirmed === undefined || JSON.stringify(before.entries) !== this.#identities) {
          throw new Error("Durability receipt namespace changed or unprepared");
        }
        const previousTimes = new Map<string, number>((JSON.parse(confirmed) as [string[], Array<[string, number]>])[1]);
        // Unchanged ancestors already have receipts. Only changed containing
        // directories owe reconfirmation, including a same-inode ABA's parent.
        // Do not charge another full-chain sweep for unrelated /tmp sibling churn.
        const owed = [before.endpoint, ...[...before.directories]
          .filter(([parent, stat]) => parent !== before.endpoint && previousTimes.get(parent) !== stat.ctimeMs)
          .map(([parent]) => parent)];
        if (process.platform !== "win32") {
          for (const parent of owed) {
            const fd = fs.openSync(parent, fs.constants.O_RDONLY);
            try {
              const opened = fs.fstatSync(fd), expected = before.directories.get(parent)!;
              if (!opened.isDirectory() || !sameInode(opened, expected)) throw new Error("Namespace directory changed before barrier");
              fs.fsyncSync(fd);
            } finally { fs.closeSync(fd); }
          }
        }
        confirmed = directoryReceiptOf(before);
        const after = walkPathNamespace(directory);
        if (JSON.stringify(after.entries) !== this.#identities) throw new Error("Namespace changed during durability barriers");
        if (directoryReceiptOf(after) === confirmed) { this.#prepared = confirmed; return; }
      }
      throw new Error("Namespace changed during durability barriers");
    } catch (error) {
      this.#prepared = undefined;
      throw error;
    }
  }
}

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
    if (options?.durable) {
      if (options.directoryReceipt) options.directoryReceipt.sync(directory);
      else syncDirectoryChain(directory);
    }
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
    this.#retryAt = Date.now() + this.#delay;
    if (!this.#reported) {
      // Includes the holder and scheduler-stall diagnostics. Once per continuous outage,
      // not once per poll, which would flood a throttled host's stderr.
      console.warn(`[pi-fabric] ${this.label}: ${transient ? "mesh lock timeout; retrying" : "background operation failed"} in ${this.#delay} ms: ${error instanceof Error ? error.message : String(error)}`);
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
