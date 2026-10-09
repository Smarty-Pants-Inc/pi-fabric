import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";
import { isMeshLockTimeout, MeshLockTimeoutError, renameAtomic } from "../core/atomic-write.js";

export interface HostLeaseMesh {
  readonly root: string;
  leaseCustody<T>(file: string, operation: () => T, timeoutMs?: number): Promise<T>;
}
export interface HostLeaseLockOptions {
  signal?: AbortSignal;
  /** Publication/registry custody must never wait or recover another lock. */
  timeoutMs?: number;
  /** Remote recovery is permitted only after this incarnation's lease deadline. */
  lease?: { incarnationToken: string; expiresAt: number };
}
export class HostLeaseLockBusyError extends MeshLockTimeoutError {
  constructor(lock: string) { super(` host lease lock ${lock}`, 1, 0); }
}
export class HostLeaseLockLostError extends Error {
  readonly code = "FABRIC_HOST_LEASE_LOCK_LOST";
  constructor(lock: string) { super(`Host lease lock receipt no longer owns ${lock}`); }
}
interface LockOwner {
  token: string;
  host: string;
  pid: number;
  incarnationToken?: string;
  expiresAt?: number;
}
const ownerOf = (lock: string): string | undefined => {
  try { return fs.readFileSync(path.join(lock, "owner"), "utf8"); } catch { return undefined; }
};
const receipt = (text: string | undefined): LockOwner | undefined => {
  try {
    const owner = JSON.parse(text ?? "") as LockOwner;
    if (!owner || typeof owner.token !== "string" || !owner.token || typeof owner.host !== "string" || !owner.host ||
      !Number.isSafeInteger(owner.pid) || owner.pid <= 0 ||
      (owner.incarnationToken !== undefined && (typeof owner.incarnationToken !== "string" || !owner.incarnationToken)) ||
      (owner.expiresAt !== undefined && !Number.isFinite(owner.expiresAt))) return undefined;
    return owner;
  } catch { return undefined; }
};
const recoverable = (text: string | undefined, host: string): boolean => {
  const owner = receipt(text);
  if (!owner) return false; // Historical/unknown receipts have no trustworthy host identity.
  if (owner.host !== host) return owner.incarnationToken !== undefined && owner.expiresAt !== undefined &&
    owner.expiresAt <= Date.now(); // A remote PID is never evidence about a local process.
  try { process.kill(owner.pid, 0); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
};

/** Subscribe before the reread, so a release between acquisition and watch is not lost.
 * The one timer bounds this active acquisition (or wakes at a remote lease deadline). */
const waitForRemoval = (lock: string, seen: string | undefined, waitMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined, timer: NodeJS.Timeout | undefined;
    const finish = (error?: unknown) => {
      watcher?.close(); if (timer) clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (error) reject(error); else resolve();
    };
    const abort = () => finish(signal?.reason ?? new DOMException("Aborted", "AbortError"));
    try {
      signal?.throwIfAborted();
      watcher = fs.watch(path.dirname(lock), (_event, name) => {
        if (name === null || name.toString() === path.basename(lock)) finish();
      });
      watcher.on("error", finish);
      signal?.addEventListener("abort", abort, { once: true });
      timer = setTimeout(() => finish(), Math.max(0, waitMs));
      if (ownerOf(lock) !== seen) finish();
    } catch (error) { finish(error); }
  });

/** Complete host-qualified receipts. Commit and recovery share custody: a process paused
 * after reading its lease cannot write across a recovered receipt. Per-host custody guards files only;
 * this does not rewrite shared state. Both state backends use exactly the same JSON CAS. */
export const withHostLeaseLock = async <T>(mesh: HostLeaseMesh, file: string,
  operation: () => T, options: HostLeaseLockOptions = {}): Promise<T> => {
  const directory = path.join(mesh.root, "host-lease-locks");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const lock = path.join(directory, path.basename(file, ".json"));
  const host = os.hostname();
  const owner = JSON.stringify({ token: randomUUID(), host, pid: process.pid, ...options.lease } satisfies LockOwner);
  const budget = options.timeoutMs ?? 0;
  const deadline = performance.now() + (Number.isFinite(budget) ? Math.max(0, budget) : 0);
  let backoffMs = 5;
  for (;;) {
    options.signal?.throwIfAborted();
    let acquired = false, enteredCustody = false, seen: string | undefined;
    let value!: T;
    try {
      await mesh.leaseCustody(file, () => {
        enteredCustody = true;
        options.signal?.throwIfAborted();
        const staging = fs.mkdtempSync(`${lock}.pending-`);
        try {
          fs.writeFileSync(path.join(staging, "owner"), owner, { flag: "wx", mode: 0o600 });
          try { fs.renameSync(staging, lock); acquired = true; }
          catch (error) {
            if (!["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes((error as NodeJS.ErrnoException).code ?? "")) throw error;
          }
        } finally {
          if (!acquired) fs.rmSync(staging, { recursive: true, force: true });
        }
        if (!acquired) {
          seen = ownerOf(lock);
          if (performance.now() < deadline && recoverable(seen, host) && ownerOf(lock) === seen) {
            const aside = `${lock}.dead-${randomUUID()}`;
            renameAtomic(lock, aside);
            fs.rmSync(aside, { recursive: true, force: true });
          }
          return;
        }
        try {
          options.signal?.throwIfAborted();
          if (ownerOf(lock) !== owner) {
            throw new HostLeaseLockLostError(lock);
          }
          value = operation(); // Lease token CAS and its write/remove are one synchronous custody commit.
        } finally {
          if (ownerOf(lock) === owner) {
            const aside = `${lock}.released-${randomUUID()}`;
            renameAtomic(lock, aside);
            fs.rmSync(aside, { recursive: true, force: true });
          }
        }
      }, 0);
    } catch (error) {
      if (enteredCustody || !isMeshLockTimeout(error)) throw error;
    }
    if (acquired) return value;
    const remaining = deadline - performance.now();
    if (remaining <= 0) throw new HostLeaseLockBusyError(lock);
    if (enteredCustody) {
      if (ownerOf(lock) !== seen) continue; // Our recovery (or a release) needs no wait.
      const incumbent = receipt(seen);
      const untilExpiry = incumbent?.host !== host && incumbent?.incarnationToken !== undefined && incumbent.expiresAt !== undefined
        ? incumbent.expiresAt - Date.now() : remaining;
      try { await waitForRemoval(lock, seen, Math.min(remaining, Math.max(0, untilExpiry)), options.signal); continue; }
      catch (error) { options.signal?.throwIfAborted(); /* Filesystems without usable watch events: bounded fallback below. */ }
    }
    // Reviewed exception: active acquisition wait, not an idle poll. Per-host custody exposes no
    // removal event, and some filesystems reject fs.watch. Monotonic, abortable, finite.
    await sleep(Math.min(backoffMs, Math.max(0, deadline - performance.now())), undefined, { signal: options.signal });
    backoffMs = Math.min(250, backoffMs * 2);
  }
};
