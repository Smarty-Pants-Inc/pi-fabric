// Cross-process exclusive lock for Fabric's durable stores (the repair
// table, the compiled entropy surface). Acquisition is a bounded retry over
// mkdir; stale-lock recovery is an exclusive rename claim, so racing
// reapers can never delete a lock a fresh writer owns.

import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { encodeOwnerIdentityLine, lockOwnerLiveness, SHORT_LOCK_MAX_HOLD_MS } from "./atomic-write.js";

const DEFAULT_LOCK_ATTEMPTS = 50;
const DEFAULT_LOCK_DELAY_MS = 5;
const DEFAULT_STALE_LOCK_MS = 30_000;

/** Contention is retryable; permission, I/O, and operation errors are not. */
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
  /** No ownerless or age/heartbeat-only recovery: require confirmed process/incarnation death. */
  requireDeadOwner?: boolean;
}

const errorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error
    ? String((error as { code?: unknown }).code)
    : undefined;

const sleepSync = (() => {
  try {
    const buffer = new Int32Array(new SharedArrayBuffer(4));
    return (ms: number): void => {
      Atomics.wait(buffer, 0, 0, ms);
    };
  } catch {
    return (): void => undefined;
  }
})();

const processAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM (or an unknown probe failure) is not evidence of a dead owner.
    return errorCode(error) !== "ESRCH";
  }
};

// The identity line is additive: older readers only consume the first three.
const ownerText = (token: string): string =>
  `${token}\n${process.pid}\n${Date.now()}\n${encodeOwnerIdentityLine()}\n`;

const ownerCanBeReaped = (owner: string, mtimeMs: number, staleMs: number, requireDeadOwner = false): boolean => {
  const [, pid, created, identity] = owner.split("\n");
  if (requireDeadOwner && (!Number.isSafeInteger(Number(pid)) || Number(pid) <= 0)) return false;
  const timestamp = Number(created);
  // Damaged metadata must not strand the lock forever. Still protect a live
  // PID, and use filesystem age when the creation timestamp is unusable.
  const since = created?.trim() && Number.isSafeInteger(timestamp) && timestamp >= 0 && timestamp <= Date.now()
    ? timestamp
    : mtimeMs;
  return Date.now() - since > staleMs && lockOwnerLiveness(Number(pid), requireDeadOwner ? NaN : since, identity, {
    legacyAlive: processAlive,
    maxHoldMs: Math.max(SHORT_LOCK_MAX_HOLD_MS, staleMs),
  }) === "dead";
};

const sleepAsync = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const reapStaleLockAsync = async (
  lock: string,
  verify: (claimed: string) => Promise<boolean>,
  fenceReapers = false,
): Promise<boolean> => {
  if (fenceReapers) {
    // Serialize recovery inside this lock incarnation. A delayed reaper must
    // revalidate AFTER owning this marker; otherwise rename can move a live
    // successor even when post-rename verification refuses to delete it.
    const marker = path.join(lock, "reaping");
    const token = randomUUID();
    try { await fs.promises.writeFile(marker, token, { flag: "wx", mode: 0o600 }); }
    catch { return false; }
    try {
      if (!(await verify(lock))) return false;
      return await reapStaleLockAsync(lock, verify);
    } finally {
      // Successful recovery removed the marker with its claimed directory.
      // Never remove a successor's marker (or an unconfirmed crashed reaper).
      if (await fs.promises.readFile(marker, "utf8").catch(() => undefined) === token) {
        await fs.promises.rm(marker, { force: true });
      }
    }
  }
  const claim = `${lock}.reap-${process.pid}-${randomUUID()}`;
  try {
    await fs.promises.rename(lock, claim);
  } catch {
    return false;
  }
  if (!(await verify(claim))) {
    try {
      await fs.promises.rename(claim, lock);
    } catch {
      if (await verify(claim)) {
        await fs.promises.rm(claim, { recursive: true, force: true });
      }
    }
    return false;
  }
  await fs.promises.rm(claim, { recursive: true, force: true });
  return true;
};

export const withExclusiveFileLockAsync = async <T>(
  options: ExclusiveLockOptions,
  operation: () => T | Promise<T>,
): Promise<T> => {
  const attempts = options.attempts ?? DEFAULT_LOCK_ATTEMPTS;
  const delayMs = options.delayMs ?? DEFAULT_LOCK_DELAY_MS;
  const staleMs = options.staleMs ?? DEFAULT_STALE_LOCK_MS;
  await fs.promises.mkdir(options.directory, { recursive: true, mode: 0o700 });
  const lock = path.join(options.directory, options.lockName);
  const ownerPath = path.join(lock, "owner");
  const token = randomUUID();
  let acquired = false;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      await fs.promises.mkdir(lock, { mode: 0o700 });
      try {
        await fs.promises.writeFile(ownerPath, ownerText(token), {
          encoding: "utf-8",
          mode: 0o600,
        });
      } catch (error) {
        await fs.promises.rm(lock, { recursive: true, force: true });
        throw error;
      }
      acquired = true;
      break;
    } catch (error) {
      if (errorCode(error) !== "EEXIST") throw error;
      try {
        const firstOwner = await fs.promises.readFile(ownerPath, "utf8");
        if (ownerCanBeReaped(firstOwner, (await fs.promises.stat(lock)).mtimeMs, staleMs, options.requireDeadOwner)) {
          const secondOwner = await fs.promises.readFile(ownerPath, "utf8");
          if (
            secondOwner === firstOwner &&
            await reapStaleLockAsync(lock, async (claimed) => {
              try {
                const owner = await fs.promises.readFile(path.join(claimed, "owner"), "utf8");
                return owner === firstOwner &&
                  ownerCanBeReaped(owner, (await fs.promises.stat(claimed)).mtimeMs, staleMs, options.requireDeadOwner);
              } catch {
                return false;
              }
            }, options.requireDeadOwner)
          ) {
            continue;
          }
        }
      } catch (error) {
        if (errorCode(error) !== "ENOENT") throw error;
        try {
          const first = await fs.promises.stat(lock);
          if (
            !options.requireDeadOwner && Date.now() - first.mtimeMs > staleMs &&
            await reapStaleLockAsync(lock, async (claimed) => {
              try {
                try {
                  await fs.promises.readFile(path.join(claimed, "owner"), "utf8");
                  return false;
                } catch (error) {
                  if (errorCode(error) !== "ENOENT") return false;
                }
                return Date.now() - (await fs.promises.stat(claimed)).mtimeMs > staleMs;
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
      await sleepAsync(delayMs);
    }
  }
  if (!acquired) throw new FileLockTimeoutError(options.timeoutMessage);
  try {
    return await operation();
  } finally {
    try {
      const owner = await fs.promises.readFile(ownerPath, "utf8");
      if (owner.startsWith(`${token}\n`)) {
        await fs.promises.rm(lock, { recursive: true, force: true });
      }
    } catch {
      // A recovering process already removed this lock.
    }
  }
};

// Stale-lock recovery must be an exclusive claim. Stat-then-delete is
// TOCTOU: two reapers (or a reaper and a fresh writer that recreated the
// lock in between) can both pass their checks, and the slower rm then
// deletes a lock the faster one already replaced. rename() is the claim —
// only one process can move the directory, and removal targets the claimed
// path, never the live lock path. A claim that turns out to hold a live
// lock is renamed back before any destructive step; a live lock is never
// deleted, even if the rename-back races a fresh writer.
const reapStaleLock = (lock: string, verify: (claimed: string) => boolean, fenceReapers = false): boolean => {
  if (fenceReapers) {
    const marker = path.join(lock, "reaping");
    const token = randomUUID();
    try { fs.writeFileSync(marker, token, { flag: "wx", mode: 0o600 }); }
    catch { return false; }
    try {
      if (!verify(lock)) return false;
      return reapStaleLock(lock, verify);
    } finally {
      try { if (fs.readFileSync(marker, "utf8") === token) fs.rmSync(marker, { force: true }); }
      catch { /* Recovery removed the claimed directory, not a successor. */ }
    }
  }
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
  const attempts = options.attempts ?? DEFAULT_LOCK_ATTEMPTS;
  const delayMs = options.delayMs ?? DEFAULT_LOCK_DELAY_MS;
  const staleMs = options.staleMs ?? DEFAULT_STALE_LOCK_MS;
  fs.mkdirSync(options.directory, { recursive: true, mode: 0o700 });
  const lock = path.join(options.directory, options.lockName);
  const ownerPath = path.join(lock, "owner");
  const token = randomUUID();
  let acquired = false;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      fs.mkdirSync(lock, { mode: 0o700 });
      try {
        fs.writeFileSync(ownerPath, ownerText(token), {
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
        if (ownerCanBeReaped(firstOwner, fs.statSync(lock).mtimeMs, staleMs, options.requireDeadOwner)) {
          const secondOwner = fs.readFileSync(ownerPath, "utf8");
          if (
            secondOwner === firstOwner &&
            reapStaleLock(lock, (claimed) => {
              try {
                const owner = fs.readFileSync(path.join(claimed, "owner"), "utf8");
                return owner === firstOwner &&
                  ownerCanBeReaped(owner, fs.statSync(claimed).mtimeMs, staleMs, options.requireDeadOwner);
              } catch {
                return false;
              }
            }, options.requireDeadOwner)
          ) {
            continue;
          }
        }
      } catch (error) {
        if (errorCode(error) !== "ENOENT") throw error;
        try {
          // Ownerless lock (crash between mkdir and the owner write): age is
          // the only signal, and the claim re-verifies it after the rename.
          const first = fs.statSync(lock);
          if (
            !options.requireDeadOwner && Date.now() - first.mtimeMs > staleMs &&
            reapStaleLock(lock, (claimed) => {
              try {
                try {
                  fs.readFileSync(path.join(claimed, "owner"), "utf8");
                  return false;
                } catch (error) {
                  if (errorCode(error) !== "ENOENT") return false;
                }
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
      sleepSync(delayMs);
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
