import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { MeshLockTimeoutError } from "../core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "./custody-lock.js";
import { MeshLock } from "./mesh-lock.js";

/**
 * The migration fence (smarty-dev#6477, org decision 10-08): `custody.lock` then the mesh `.lock` (the
 * order of withMeshCustody). Every operation that installs or removes the moved marker or moves the
 * backend flag holds both for its whole section: the import, cutover and rollback (backend-migration.ts)
 * and a fixture's `initialize: "create"` (state-sqlite.ts). File-mode writers commit under `.lock` and
 * check the fence (assertFileStateWritable) right before their rename, so a writer either commits
 * before the fenced section (which then sees its state.json) or is refused after it.
 * A leaf module: state-sqlite.ts and backend-migration.ts both use it without importing each other.
 */
export interface MeshFenceOptions {
  /** The mesh `.lock` protocol. Default 1 (the migration tool's). */
  lockProtocol?: 1 | 2;
  /** Budget for each fence lock (`custody.lock`, then `.lock`). */
  lockTimeoutMs: number;
}

/** Holds the fence across awaits until the operation settles. */
export const holdMeshFence = async <T>(root: string, options: MeshFenceOptions,
  operation: (lock: MeshLock) => Promise<T> | T): Promise<T> => {
  const releaseCustody = await acquireMeshCustodyLock(root, options.lockTimeoutMs);
  try {
    const lock = new MeshLock(root, { lockProtocol: options.lockProtocol ?? 1, lockTimeoutMs: options.lockTimeoutMs }, () => undefined);
    return await lock.withLockAcrossAwait(async () => operation(lock), options.lockTimeoutMs, "other");
  } finally { releaseCustody(); }
};

const sleepSync = (ms: number): void => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); };
const codeOf = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;

/** One attempt at `custody.lock` with its own wire (a complete owner published by rename). */
const tryCustody = (lock: string, token: string, record: string): boolean => {
  const staging = fs.mkdtempSync(`${lock}.pending.${token}.`);
  try {
    fs.writeFileSync(path.join(staging, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 });
    if (fs.lstatSync(lock, { throwIfNoEntry: false }) !== undefined) return false;
    try { fs.renameSync(staging, lock); return true; } catch (error) {
      if (["EEXIST", "ENOTEMPTY", "EPERM", "EACCES"].includes(codeOf(error) ?? "")) return false;
      throw error;
    }
  } finally { fs.rmSync(staging, { recursive: true, force: true }); }
};

/** One attempt at the mesh `.lock` with the protocol-1 wire (mkdir, then the owner record). */
const tryMeshLock = (lock: string, record: string): boolean => {
  try { fs.mkdirSync(lock, { mode: 0o700 }); } catch (error) { if (codeOf(error) === "EEXIST") return false; throw error; }
  try { fs.writeFileSync(path.join(lock, "owner"), record, { encoding: "utf8", flag: "wx", mode: 0o600 }); return true; }
  catch (error) {
    if (codeOf(error) === "EEXIST") return false; // a recoverer's successor owns it now: never clean it
    try { fs.rmdirSync(lock); } catch { /* not ours to clean any more */ }
    throw error;
  }
};

/**
 * Release by a verified take of the owner file, then `rmdir` (pi-fabric#694 P1, smarty-dev#6477). The lock
 * DIRECTORY is never renamed, so a successor's lock is never detached.
 *
 * 1. Read `<lock>/owner`; anything but our exact record (a unique token) means it is not ours: return.
 * 2. Take the owner file by rename to a private name beside the lock (the only atomic conditional unlink:
 *    a path unlink cannot check what it removes). Re-read the taken file: it is the verdict.
 * 3. Ours: unlink the taken file, then `rmdir(<lock>)`. rmdir fails closed (ENOTEMPTY) if a successor
 *    already published its owner there; a successor still between its mkdir and its owner write loses its
 *    directory and its `wx` owner write fails (ENOENT), so it never enters (tryMeshLock above,
 *    mesh-lock.ts:254-268). Protocol-2 and custody acquirers publish complete directories by rename.
 *    The directory is ownerless only between the take and the rmdir; the take itself refreshes its mtime,
 *    so the ownerless-directory grace (mesh-lock.ts:353) cannot pass inside that window.
 * 4. Not ours (only if the precondition below was broken): put the foreign record back with a no-clobber
 *    hard link and leave its directory in place; if the name has another owner now, keep the taken file.
 *
 * Precondition (why the step 1 -> 2 gap is not a real window): a LIVE owner's lock is never replaced. Only
 * three code paths remove a canonical `.lock` or `custody.lock` that holds an owner record, in this tree and
 * in the 13d1bbef release (byte-identical mesh-lock.ts, custody-lock.ts and core/atomic-write.ts):
 * - the holder's own release, gated on its unique token (here; mesh-lock.ts:217-229; custody-lock.ts:91-101);
 * - MeshLock #clearStaleLock (mesh-lock.ts:343-413): renames a complete receipt only if its pid is dead
 *   (`process.kill(pid, 0)` gives ESRCH, :86-96) or alive with a VALID recorded incarnation that differs
 *   from the native reading, i.e. pid reuse (:378-398). A live pid with no incarnation line (the fence's
 *   three-line wire) returns at :379. Age (staleLockMs) gates only the EMPTY ownerless directory (:352-353),
 *   removed by rmdir (:359), which fails on a directory that holds an owner file;
 * - clearDeadCustodyLock (custody-lock.ts:107-134): the same dead/different-incarnation rule (:119-126), no
 *   age rule, never an ownerless directory (:113).
 * The age-based reapers (core/atomic-write.ts withExclusiveFileLock, state-sqlite.ts tryLockFile) never
 * name `.lock` or `custody.lock`. The fence's synchronous acquirer recovers nothing (holdMeshFenceSync
 * below). So while the owner's pid is alive in the same pid namespace, its record stays at `<lock>/owner`
 * until it releases (tests/mesh-fence-lock.test.ts proves it for both trees). `hooks` is a test seam for
 * the interleavings.
 */
export const releaseFenceLock = (lock: string, token: string, record: string,
  hooks: { afterCheck?: () => void; afterTake?: () => void } = {}): void => {
  const ownerPath = path.join(lock, "owner");
  try { if (fs.readFileSync(ownerPath, "utf8") !== record) return; } catch { return; /* already recovered or replaced */ }
  hooks.afterCheck?.();
  const taken = `${lock}.released.${token}`;
  try { fs.renameSync(ownerPath, taken); } catch { return; /* already recovered or replaced */ }
  hooks.afterTake?.();
  let ours = false;
  try { ours = fs.readFileSync(taken, "utf8") === record; } catch { /* unreadable: not provably ours */ }
  if (ours) {
    try { fs.unlinkSync(taken); } catch { /* private name; best effort */ }
    try { fs.rmdirSync(lock); } catch { /* a successor's owner is in it, or it is gone: never ours to remove */ }
    return;
  }
  try { fs.linkSync(taken, ownerPath); fs.unlinkSync(taken); }
  catch { /* the name has another owner or is gone: keep the foreign record, never overwrite one */ }
};

/**
 * The fence was busy for a synchronous acquirer. It carries SQLITE_BUSY's errcode so a synchronous open's
 * caller retries it exactly as a busy database (state-backend.ts falls back to the asynchronous open).
 */
export class MeshFenceBusyError extends MeshLockTimeoutError {
  readonly errcode = 5;
}

/**
 * The same fence for a synchronous caller (`SqliteStateStore.openSync` with `initialize: "create"`): attempts
 * at each lock until `timeoutMs` (0: one attempt) with short synchronous sleeps, the locks' own wire formats,
 * no stale-owner recovery (a dead holder is recovered by the next asynchronous acquirer). A synchronous
 * waiter blocks the event loop, so it can never outwait an in-process holder: callers pass 0 and retry.
 */
export const holdMeshFenceSync = <T>(root: string, timeoutMs: number, operation: () => T): T => {
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const deadline = Date.now() + Math.max(0, timeoutMs);
  const acquire = (lock: string, attempt: (token: string, record: string) => boolean): (() => void) => {
    const token = randomUUID();
    const record = `${token}\n${process.pid}\n${Date.now()}\n`;
    let attempts = 0;
    let maxGapMs = 0;
    let last = Date.now();
    for (;;) {
      const now = Date.now();
      if (attempts > 0) maxGapMs = Math.max(maxGapMs, now - last);
      attempts += 1;
      last = now;
      if (attempt(token, record)) return () => releaseFenceLock(lock, token, record);
      if (Date.now() >= deadline) {
        let pid: string | undefined;
        try { pid = fs.readFileSync(path.join(lock, "owner"), "utf8").split("\n")[1]; } catch { /* ownerless or gone */ }
        throw new MeshFenceBusyError(` (${path.basename(lock)} ${lock}${pid ? `, held by pid ${pid}` : ""})`, attempts, maxGapMs);
      }
      sleepSync(Math.min(Math.max(1, deadline - Date.now()), 2 + Math.floor(Math.random() * 4)));
    }
  };
  const custody = path.join(root, MESH_CUSTODY_LOCK_NAME);
  const releaseCustody = acquire(custody, (token, record) => tryCustody(custody, token, record));
  try {
    const mesh = path.join(root, ".lock");
    const releaseMesh = acquire(mesh, (_token, record) => tryMeshLock(mesh, record));
    try { return operation(); } finally { releaseMesh(); }
  } finally { releaseCustody(); }
};
