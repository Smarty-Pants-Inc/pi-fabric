// Cross-process exclusive lock for Fabric's durable stores (the repair
// table, the compiled entropy surface). Acquisition is a bounded retry over
// mkdir; stale-lock recovery is an exclusive rename claim, so racing
// reapers can never delete a lock a fresh writer owns.

import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const DEFAULT_LOCK_ATTEMPTS = 50;
const DEFAULT_LOCK_DELAY_MS = 5;
const DEFAULT_STALE_LOCK_MS = 30_000;

import type { ExclusiveLockOptions } from "./atomic-write.js";
export type { ExclusiveLockOptions } from "./atomic-write.js";
export { withExclusiveFileLock } from "./atomic-write.js";

const errorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error
    ? String((error as { code?: unknown }).code)
    : undefined;



const processAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

const sleepAsync = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const reapStaleLockAsync = async (
  lock: string,
  verify: (claimed: string) => Promise<boolean>,
): Promise<boolean> => {
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
        await fs.promises.writeFile(ownerPath, `${token}\n${process.pid}\n${Date.now()}\n`, {
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
        const [, pidText, createdText] = firstOwner.trim().split("\n");
        const stale = Date.now() - Number(createdText) > staleMs;
        if (stale && !processAlive(Number(pidText))) {
          const secondOwner = await fs.promises.readFile(ownerPath, "utf8");
          if (
            secondOwner === firstOwner &&
            await reapStaleLockAsync(lock, async (claimed) => {
              try {
                const owner = await fs.promises.readFile(path.join(claimed, "owner"), "utf8");
                const [, pid, created] = owner.trim().split("\n");
                return Date.now() - Number(created) > staleMs && !processAlive(Number(pid));
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
          const first = await fs.promises.stat(lock);
          if (
            Date.now() - first.mtimeMs > staleMs &&
            await reapStaleLockAsync(lock, async (claimed) => {
              try {
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
  if (!acquired) throw new Error(options.timeoutMessage);
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
