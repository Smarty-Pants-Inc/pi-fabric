import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

export interface AtomicWriteOptions {
  // File mode for the committed file (default 0o600) and for mkdir -p of its
  // parent directory (default 0o700).
  mode?: number;
  dirMode?: number;
  // Opt in to stable-storage ordering: sync the file before rename and its parent after.
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

export const renameAtomic = (
  source: string,
  target: string,
  options?: AtomicWriteOptions,
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
    // ponytail: Windows cannot open directories for fsync; only this barrier is skipped.
    if (options?.durable && process.platform !== "win32") {
      const fd = fs.openSync(path.dirname(filePath), fs.constants.O_RDONLY);
      try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
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

  /** Polls retry unchanged state on a later tick after backoff; contains sync handlers too. */
  async run(operation: () => unknown | Promise<unknown>): Promise<"done" | "retry" | "failed" | "skipped"> {
    if (this.#running || this.waitMs > 0) return "skipped";
    this.#running = true;
    try {
      await operation();
      this.success();
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
  constructor(label: string, minMs = 100, maxMs = 5_000) { this.#retry = new MeshBackgroundRetry(label, minMs, maxMs); }

  enqueue(operation: () => unknown | Promise<unknown>): Promise<void> {
    if (this.#closed) return Promise.resolve();
    // Bounded best-effort notifications. Durable protocol cursors are not stored here.
    if (this.#pending.length >= 1_000) {
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
      this.#pending.shift(); // permanent failures are visible but do not poison later notices
    }
  }

  async close(): Promise<void> {
    this.#closed = true;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    await this.#draining;
    for (const item of this.#pending.splice(0)) item.attempted();
  }
}
