import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";
import { isMeshLockTimeout, MeshLockTimeoutError, renameAtomic } from "../core/atomic-write.js";

export interface HostLeaseMesh {
  readonly root: string;
  custody<T>(operation: () => T, timeoutMs?: number): Promise<T>;
}
export interface HostLeaseLockOptions {
  signal?: AbortSignal;
  /** Publication/registry custody must never wait or recover another lock. */
  timeoutMs?: number;
}
export class HostLeaseLockBusyError extends MeshLockTimeoutError {
  constructor(lock: string) { super(` host lease lock ${lock}`, 1, 0); }
}
const ownerOf = (lock: string): string | undefined => {
  try { return fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { return undefined; }
};
const dead = (owner: string | undefined): boolean => {
  if (!owner?.endsWith("\n")) return false;
  const pid = Number(owner.split("\n")[1]);
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
};

/** Complete receipts, never age-steal a live/unknown owner. Recoveries share mesh custody;
 * normal claim/renew/remove share only this per-host lock, on BOTH state backends. */
export const withHostLeaseLock = async <T>(mesh: HostLeaseMesh, file: string,
  operation: () => T, options: HostLeaseLockOptions = {}): Promise<T> => {
  const directory = path.join(mesh.root, "host-lease-locks");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const lock = path.join(directory, path.basename(file, ".json"));
  const owner = `${randomUUID()}\n${process.pid}\n`;
  const deadline = performance.now() + Math.max(0, options.timeoutMs ?? 0);
  for (;;) {
    options.signal?.throwIfAborted();
    const staging = fs.mkdtempSync(`${lock}.pending-`);
    let acquired = false;
    try {
      fs.writeFileSync(path.join(staging, "owner"), owner, { flag: "wx", mode: 0o600 });
      try { fs.renameSync(staging, lock); acquired = true; }
      catch (error) {
        if (!["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
      }
    } finally {
      if (!acquired) fs.rmSync(staging, { recursive: true, force: true });
    }
    if (acquired) {
      try { options.signal?.throwIfAborted(); return operation(); }
      finally {
        // Nobody recovers a live PID; detach only our exact receipt, never delete canonical.
        if (ownerOf(lock) === owner) {
          const aside = `${lock}.released-${randomUUID()}`;
          renameAtomic(lock, aside);
          fs.rmSync(aside, { recursive: true, force: true });
        }
      }
    }
    if (performance.now() >= deadline) throw new HostLeaseLockBusyError(lock);
    const seen = ownerOf(lock);
    if (dead(seen)) {
      try { await mesh.custody(() => {
        options.signal?.throwIfAborted();
        if (ownerOf(lock) !== seen || !dead(seen)) return;
        const aside = `${lock}.dead-${randomUUID()}`;
        renameAtomic(lock, aside);
        fs.rmSync(aside, { recursive: true, force: true });
      }, 0); } catch (error) { if (!isMeshLockTimeout(error)) throw error; }
    }
    await sleep(Math.min(5, Math.max(0, deadline - performance.now())), undefined, { signal: options.signal });
  }
};
