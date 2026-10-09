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

/** `pinFenceLock`'s verdict for a lock whose directory no longer holds our record: we do not hold it. */
export const FOREIGN_LOCK = -1;
const PIN_ROOT = "/proc/self/fd";

/**
 * Pin the lock DIRECTORY we just created (pi-fabric#694 P1, smarty-dev#6477): an fd on it, verified to hold our
 * record through `/proc/self/fd/<fd>/owner`. The fd follows the directory inode, never the shared name, so every
 * later step on `<pin>/owner` can only touch the file inside OUR directory. Returns the fd; `undefined` where
 * `/proc/self/fd` cannot traverse a directory (not Linux): the release falls back to the shared path;
 * FOREIGN_LOCK when the directory at the name does not hold our record (then we never held it).
 */
export const pinFenceLock = (lock: string, record: string): number | undefined => {
  if (process.platform !== "linux") return undefined;
  let fd: number;
  try { fd = fs.openSync(lock, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0)); } catch { return FOREIGN_LOCK; }
  let owner: string | undefined;
  try { owner = fs.readFileSync(path.join(PIN_ROOT, String(fd), "owner"), "utf8"); } catch (error) {
    if (codeOf(error) !== "ENOENT" || !fs.existsSync(path.join(PIN_ROOT, String(fd)))) { fs.closeSync(fd); return undefined; }
  }
  if (owner === record) return fd;
  fs.closeSync(fd);
  return FOREIGN_LOCK;
};

const sameInode = (a: fs.Stats | undefined, b: fs.Stats): boolean => a !== undefined && a.isDirectory() && a.dev === b.dev && a.ino === b.ino;

/**
 * Release without any step that can detach a successor (pi-fabric#694 P1, smarty-dev#6477, third design).
 * The shared name is never renamed, and no file is ever moved out of `<lock>`.
 *
 * Pinned (Linux, `pin` from pinFenceLock):
 * 1. `<pin>/owner` must be our record (a unique token). It is read through the fd: OUR directory inode, wherever
 *    it is now. Nothing ever takes an owner file out of a directory: every recoverer renames or rmdirs the whole
 *    directory (mesh-lock.ts:359,408; custody-lock.ts:96,129; the same in 13d1bbef), and every writer of
 *    `owner` uses `wx`, which fails while ours exists. So our verified record stays our record until we unlink it.
 * 2. Our directory must still be at `<lock>` (same dev and inode). Moved away (a recoverer's `.dead.` receipt) or
 *    replaced: leave everything alone, the receipt stays non-empty and the successor untouched.
 * 3. Unlink `<pin>/owner`: through the fd, so it can only remove the file verified in step 1.
 * 4. `rmdir(<lock>)` only while the name still holds our inode. rmdir fails closed (ENOTEMPTY) on any record,
 *    so it can never remove a recorded successor; at worst it removes the EMPTY directory of a protocol-1
 *    acquirer between its mkdir and its owner write, whose `wx` write then fails (ENOENT) and it never enters.
 * Residual: only step 3 can follow a broken precondition, and only when our directory is renamed to a `.dead.`
 * receipt between steps 2 and 3, i.e. a recoverer removed a LIVE owner's lock (no code path does: mesh-lock.ts:
 * 343-413, custody-lock.ts:107-134, proven by tests/mesh-fence-lock.test.ts for both trees). Even then it
 * empties our own receipt; it never touches a successor's record.
 *
 * Unpinned (no `/proc/self/fd`): the owner is taken INSIDE `<lock>` (rename to `owner.released.<token>`, never
 * out of the directory), verified, and either removed with an rmdir or linked back without clobbering; a foreign
 * record that cannot go back stays in `<lock>`, so the directory stays non-empty and nobody else can mkdir,
 * publish into or rmdir it. Residual there: only after a live owner's lock was replaced (the same precondition)
 * and only against a protocol-1 writer paused since an mkdir whose directory was reaped.
 * `hooks` is a test seam for the interleavings.
 */
export const releaseFenceLock = (lock: string, record: string,
  hooks: { afterCheck?: () => void; afterTake?: () => void } = {}, pin?: number): void => {
  if (pin === FOREIGN_LOCK) return;
  if (pin !== undefined) {
    try {
      const pinned = path.join(PIN_ROOT, String(pin));
      try { if (fs.readFileSync(path.join(pinned, "owner"), "utf8") !== record) return; } catch { return; }
      const ours = fs.fstatSync(pin);
      hooks.afterCheck?.();
      if (!sameInode(fs.lstatSync(lock, { throwIfNoEntry: false }), ours)) return; // moved or replaced: not ours to touch
      try { fs.unlinkSync(path.join(pinned, "owner")); } catch { return; }
      hooks.afterTake?.();
      if (!sameInode(fs.lstatSync(lock, { throwIfNoEntry: false }), ours)) return;
      try { fs.rmdirSync(lock); } catch { /* a successor's owner is in it: never ours to remove */ }
    } finally { try { fs.closeSync(pin); } catch { /* already closed */ } }
    return;
  }
  const ownerPath = path.join(lock, "owner");
  try { if (fs.readFileSync(ownerPath, "utf8") !== record) return; } catch { return; }
  hooks.afterCheck?.();
  const taken = path.join(lock, `owner.released.${record.split("\n")[0] ?? ""}`);
  try { fs.renameSync(ownerPath, taken); } catch { return; }
  hooks.afterTake?.();
  let ours = false;
  try { ours = fs.readFileSync(taken, "utf8") === record; } catch { /* unreadable: not provably ours */ }
  if (ours) {
    try { fs.unlinkSync(taken); } catch { /* best effort */ }
    try { fs.rmdirSync(lock); } catch { /* a successor's owner is in it: never ours to remove */ }
    return;
  }
  try { fs.linkSync(taken, ownerPath); fs.unlinkSync(taken); }
  catch { /* another owner holds the name: the foreign record stays inside <lock>, never deleted */ }
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
      if (attempt(token, record)) {
        const pin = pinFenceLock(lock, record);
        if (pin !== FOREIGN_LOCK) return () => releaseFenceLock(lock, record, {}, pin);
      }
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
