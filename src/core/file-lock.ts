// Cross-process exclusive lock for Fabric's durable stores. Recovery markers
// fence provisional acquisitions before any user operation can begin.
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { lockRecoveryBlocked, reapStaleLock, releaseLockToken as withdrawToken } from "./atomic-write.js";

const DEFAULT_LOCK_ATTEMPTS = 50;
const DEFAULT_LOCK_DELAY_MS = 5;
const DEFAULT_STALE_LOCK_MS = 30_000;

export interface ExclusiveLockOptions {
  directory: string;
  lockName: string;
  /** Error message when acquisition times out. */
  timeoutMessage: string;
  staleMs?: number;
  attempts?: number;
  delayMs?: number;
}

const errorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error
    ? String((error as { code?: unknown }).code)
    : undefined;

const sleepSync = (() => {
  try {
    const buffer = new Int32Array(new SharedArrayBuffer(4));
    return (ms: number): void => { Atomics.wait(buffer, 0, 0, ms); };
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
    // EPERM/unknown liveness cannot authorize taking over a live operation.
    return errorCode(error) !== "ESRCH";
  }
};

const sleepAsync = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const releaseToken = (lock: string, token: string): void => {
  withdrawToken(lock, token);
};

const clearStaleLock = (lock: string, staleMs: number): boolean => {
  const ownerPath = path.join(lock, "owner");
  try {
    const firstOwner = fs.readFileSync(ownerPath, "utf8");
    const [, pid, created] = firstOwner.trim().split("\n");
    if (!(Date.now() - Number(created) > staleMs) || processAlive(Number(pid))) return false;
    return reapStaleLock(lock, claimed => {
      try {
        const owner = fs.readFileSync(path.join(claimed, "owner"), "utf8");
        const [, currentPid, currentCreated] = owner.trim().split("\n");
        return owner === firstOwner && Date.now() - Number(currentCreated) > staleMs &&
          !processAlive(Number(currentPid));
      } catch { return false; }
    });
  } catch (error) {
    if (errorCode(error) !== "ENOENT") return false;
    try {
      const first = fs.statSync(lock);
      if (!(Date.now() - first.mtimeMs > staleMs)) return false;
      return reapStaleLock(lock, claimed => {
        try {
          fs.readFileSync(path.join(claimed, "owner"), "utf8");
          return false;
        } catch (error) {
          if (errorCode(error) !== "ENOENT") return false;
        }
        try {
          const current = fs.statSync(claimed);
          return current.dev === first.dev && current.ino === first.ino &&
            current.mtimeMs === first.mtimeMs && Date.now() - current.mtimeMs > staleMs;
        } catch { return false; }
      });
    } catch { return false; }
  }
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
    if (!lockRecoveryBlocked(lock)) {
      let created = false;
      try {
        await fs.promises.mkdir(lock, { mode: 0o700 });
        created = true;
        // A paused mkdir winner may resume after ownerless recovery. wx never
        // overwrites the token of a replacement/restored live owner.
        await fs.promises.writeFile(ownerPath, `${token}\n${process.pid}\n${Date.now()}\n`, {
          encoding: "utf-8", mode: 0o600, flag: "wx",
        });
        if (!lockRecoveryBlocked(lock) &&
            fs.readFileSync(ownerPath, "utf8").startsWith(`${token}\n`)) {
          acquired = true;
          break;
        }
        withdrawToken(lock, token);
      } catch (error) {
        if (created) withdrawToken(lock, token);
        const code = errorCode(error);
        if (code !== "EEXIST" && code !== "ENOENT") throw error;
        if (clearStaleLock(lock, staleMs)) continue;
      }
    }
    if (attempt < attempts - 1) await sleepAsync(delayMs);
  }
  if (!acquired) throw new Error(options.timeoutMessage);
  try {
    return await operation();
  } finally {
    releaseToken(lock, token);
  }
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
    if (!lockRecoveryBlocked(lock)) {
      let created = false;
      try {
        fs.mkdirSync(lock, { mode: 0o700 });
        created = true;
        fs.writeFileSync(ownerPath, `${token}\n${process.pid}\n${Date.now()}\n`, {
          encoding: "utf-8", mode: 0o600, flag: "wx",
        });
        if (!lockRecoveryBlocked(lock) &&
            fs.readFileSync(ownerPath, "utf8").startsWith(`${token}\n`)) {
          acquired = true;
          break;
        }
        withdrawToken(lock, token);
      } catch (error) {
        if (created) withdrawToken(lock, token);
        const code = errorCode(error);
        if (code !== "EEXIST" && code !== "ENOENT") throw error;
        if (clearStaleLock(lock, staleMs)) continue;
      }
    }
    if (attempt < attempts - 1) sleepSync(delayMs);
  }
  if (!acquired) throw new Error(options.timeoutMessage);
  try {
    return operation();
  } finally {
    releaseToken(lock, token);
  }
};
