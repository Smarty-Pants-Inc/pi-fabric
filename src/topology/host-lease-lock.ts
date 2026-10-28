import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { isMeshLockTimeout, MeshLockTimeoutError, renameAtomic, readPhysicalHostIdentity, validBootId,
  type PhysicalHostIdentity } from "../core/atomic-write.js";
import { completeLockIdentity, lockIdentityVerdict, readLockIdentity, readLockIdentitySync, retainIdentityStaleLock, warnLegacyLockOwner,
  type LockIdentity } from "../core/lock-identity.js";

export interface HostLeaseMesh {
  readonly root: string;
  leaseCustody<T>(file: string, operation: () => T | Promise<T>, timeoutMs?: number, options?: { ownIncarnation?: string | undefined }): Promise<T>;
}

export const hostLeaseAdditionsGatePath = (root: string): string => path.join(root, "host-leases", ".additions");

/** Lease namespace custody blocks new filenames while a mirror brackets its directory snapshot.
 * Existing renewals need only their per-host gate. A creator takes additions before its host gate;
 * if a prior removal races the initial existence check, retry in that same stable order. */
/** Namespace custody blocks new lease filenames. Existing renewals use only their per-host gate. */
export const withHostLeaseDirectoryCustody = <T>(mesh: HostLeaseMesh,
  operation: () => T | Promise<T>, timeoutMs = 0, options: { ownIncarnation?: string | undefined } = {}): Promise<T> =>
  mesh.leaseCustody(hostLeaseAdditionsGatePath(mesh.root), operation, timeoutMs, options);

/**
 * smarty-dev#13645: how long a NATIVE lease creator waits for the shared additions gate. That gate is one
 * per mesh, so with zero wait two hosts that create their first lease together (concurrent starts,
 * first-time registrations) refused each other, which main's per-host gates never did. Other creators,
 * the reaper and a mirror's first-install effect hold it for one short synchronous commit; a mirror's
 * custody pass holds it longer, and a creator that outwaits this bound still gets the one-attempt busy
 * refusal its caller already retries. The waiter holds no lease gate (additions -> host order), so the
 * wait cannot close a cycle. Bridge effects keep zero wait (re-audit P2-1: busy is surfaced and retried).
 */
export const HOST_LEASE_ADDITIONS_WAIT_MS = 2_000;

export const withHostLeaseFileCustody = async <T>(mesh: HostLeaseMesh, file: string,
  operation: () => T | Promise<T>, timeoutMs = 0, options: { ownIncarnation?: string | undefined } = {},
  additionsTimeoutMs = timeoutMs): Promise<T> => {
  const target = () => mesh.leaseCustody(file, operation, timeoutMs, options);
  if (!fs.existsSync(file)) return withHostLeaseDirectoryCustody(mesh, target, additionsTimeoutMs, options);
  let missing = false;
  const current = await mesh.leaseCustody(file, () => {
    if (!fs.existsSync(file)) { missing = true; return undefined; }
    return operation();
  }, timeoutMs, options);
  return missing ? withHostLeaseDirectoryCustody(mesh, target, additionsTimeoutMs, options) : current as T;
};
export interface HostLeaseLockOptions {
  signal?: AbortSignal;
  ownIncarnation?: string | undefined;
  /** Publication/registry custody must never wait or recover another lock. */
  timeoutMs?: number;
  /** Remote recovery is permitted only after this incarnation's lease deadline. */
  lease?: { incarnationToken: string; expiresAt: number };
}
export class HostLeaseLockBusyError extends MeshLockTimeoutError {
  readonly busyCode = "FABRIC_HOST_LEASE_LOCK_BUSY";
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
  start?: string | undefined;
  bootId?: string | undefined;
  namespace?: string | undefined;
  incarnationToken?: string;
  expiresAt?: number;
  physical?: PhysicalHostIdentity;
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
const recoverable = (text: string | undefined, physical: PhysicalHostIdentity | undefined): boolean => {
  const owner = receipt(text);
  if (!owner) return false; // Historical/unknown receipts have no trustworthy host identity.
  if (!physical || owner.physical?.machineId !== physical.machineId || !validBootId(owner.physical?.bootId)) {
    return owner.incarnationToken !== undefined && owner.expiresAt !== undefined && owner.expiresAt <= Date.now();
  }
  if (owner.physical.bootId !== physical.bootId) return true; // Prior boot has no surviving processes.
  try { process.kill(owner.pid, 0); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
};

/** Subscribe before the reread, so a release between acquisition and watch is not lost.
 * The one timer bounds this active acquisition (or wakes at a remote lease deadline). */
const waitForRemoval = (lock: string, seen: string | undefined, waitMs: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined, timer: NodeJS.Timeout | undefined, finished = false;
    const finish = (error?: unknown) => {
      if (finished) return;
      finished = true;
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
 * after reading its lease cannot write across a recovered receipt. The same per-host custody
 * domain also fences lease-owned shared-state transactions. Both backends use the same lease CAS. */
export const withHostLeaseLock = async <T>(mesh: HostLeaseMesh, file: string,
  operation: () => T, options: HostLeaseLockOptions = {}): Promise<T> => {
  const directory = path.join(mesh.root, "host-lease-locks");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const lock = path.join(directory, path.basename(file, ".json"));
  const host = os.hostname();
  const physical = readPhysicalHostIdentity();
  const budget = options.timeoutMs ?? 0;
  const prepared = Object.hasOwn(options, "ownIncarnation");
  // A cold Linux zero-budget publisher can read its own procfs identity synchronously; it must not
  // accidentally mark an ABSENT caller birth as explicitly prepared UNKNOWN. Other platforms never
  // start native commands on a zero-budget publication, while an ordinary wait can prepare them.
  const identity = !prepared && process.platform === "linux" ? readLockIdentitySync() : await readLockIdentity({
    ...(prepared ? { start: options.ownIncarnation } : {}),
    prepared: prepared || !Number.isFinite(budget) || budget <= 0 });
  const owner = JSON.stringify({ token: randomUUID(), host, ...identity, ...(physical ? { physical } : {}), ...options.lease } satisfies LockOwner);
  const deadline = performance.now() + (Number.isFinite(budget) ? Math.max(0, budget) : 0);
  for (;;) {
    options.signal?.throwIfAborted();
    let acquired = false, enteredCustody = false, seenComplete = false, seen: string | undefined;
    let value!: T;
    try {
      await withHostLeaseFileCustody(mesh, file, async () => {
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
          const incumbent = receipt(seen);
          const identity: LockIdentity | undefined = incumbent ? { pid: incumbent.pid,
            start: typeof incumbent.start === "string" ? incumbent.start : undefined,
            bootId: typeof incumbent.bootId === "string" ? incumbent.bootId : undefined,
            namespace: typeof incumbent.namespace === "string" ? incumbent.namespace : undefined } : undefined;
          seenComplete = identity !== undefined && await completeLockIdentity(identity);
          if (seen !== undefined && !seenComplete) warnLegacyLockOwner(lock, seen);
          if (seen === undefined || !identity || performance.now() >= deadline) return;
          const stat = fs.lstatSync(lock);
          if (!stat.isDirectory()) return;
          // Keep custody across the asynchronous identity read and the retained rename. Complete receipts
          // NEVER fall back to lease expiry; incomplete/legacy receipts keep their existing locality rules.
          const reason = seenComplete
            ? await lockIdentityVerdict(identity, Date.now() + Math.max(0, deadline - performance.now()))
            : recoverable(seen, physical) ? "unknown" : undefined;
          options.signal?.throwIfAborted();
          if (reason !== undefined && reason !== "live" && !(seenComplete && reason === "unknown") && performance.now() < deadline) {
            // JSON owners have no creation timestamp; the pinned directory mtime supplies a stable date.
            retainIdentityStaleLock(lock, stat, seen, Math.max(0, Math.trunc(stat.mtimeMs)), reason, identity,
              () => options.signal?.throwIfAborted());
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
      }, 0, Object.hasOwn(options, "ownIncarnation") ? { ownIncarnation: options.ownIncarnation } : {},
      // The additions wait never outlasts what is left of the caller's own budget (audit P2-3, N1 note 1).
      // That wait takes no signal; an abort during it is observed as soon as it ends, below (N1 note 2).
      options.timeoutMs !== undefined && Number.isFinite(options.timeoutMs)
        ? Math.max(0, Math.min(HOST_LEASE_ADDITIONS_WAIT_MS, deadline - performance.now())) : HOST_LEASE_ADDITIONS_WAIT_MS);
    } catch (error) {
      options.signal?.throwIfAborted();
      if (enteredCustody || !isMeshLockTimeout(error)) throw error;
    }
    if (acquired) return value;
    const remaining = deadline - performance.now();
    if (remaining <= 0) throw new HostLeaseLockBusyError(lock);
    if (enteredCustody) {
      if (ownerOf(lock) !== seen) continue; // Our recovery (or a release) needs no wait.
      const incumbent = receipt(seen);
      const untilExpiry = !seenComplete && (!physical || incumbent?.physical?.machineId !== physical.machineId ||
        !validBootId(incumbent?.physical?.bootId)) && incumbent?.incarnationToken !== undefined && incumbent.expiresAt !== undefined
        ? incumbent.expiresAt - Date.now() : remaining;
      if (untilExpiry <= 0) throw new HostLeaseLockBusyError(lock);
      try { await waitForRemoval(lock, seen, Math.min(remaining, Math.max(0, untilExpiry)), options.signal); continue; }
      catch { options.signal?.throwIfAborted(); throw new HostLeaseLockBusyError(lock); }
    }
    // No event source for a busy commit gate: ONE attempt, then the caller owns retry policy.
    throw new HostLeaseLockBusyError(lock);
  }
};
