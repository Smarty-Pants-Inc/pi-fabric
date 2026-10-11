import crypto, { randomUUID } from "node:crypto";
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
 * Unpinned (no `/proc/self/fd`): the owner is taken INSIDE `<lock>` (rename to `r.<8 random hex>`, never
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
  const taken = takeOwner(lock, ownerPath);
  if (taken === undefined) return;
  hooks.afterTake?.();
  let ours = false;
  try { ours = fs.readFileSync(taken, "utf8") === record; } catch { /* unreadable: not provably ours */ }
  if (ours) {
    try { fs.unlinkSync(taken); } catch { /* best effort */ }
    removeReleasedLock(lock);
    return;
  }
  try { fs.linkSync(taken, ownerPath); fs.unlinkSync(taken); }
  catch { /* another owner holds the name: the foreign record stays inside <lock>, never deleted */ }
};

let takeRefusedWarned = false;
/**
 * Move `<lock>/owner` to a fresh short name inside `<lock>` without ever replacing an existing entry. Short:
 * Windows takes the unpinned release, and a deep mesh root must stay under MAX_PATH (smarty-dev#8777). A random
 * name can collide, and rename (or MoveFileEx REPLACE_EXISTING) replaces whatever holds it, a symlink included, so
 * the move is link (EEXIST never replaces) then unlink: a collision retries with a new name, 5 times, then the
 * release fails closed with the owner left in place. Only releaseFenceLock reads the name back.
 */
const takeOwner = (lock: string, ownerPath: string): string | undefined => {
  for (let attempt = 0; attempt < 5; attempt++) {
    const taken = path.join(lock, `r.${crypto.randomBytes(4).toString("hex")}`);
    try {
      fs.linkSync(ownerPath, taken);
    } catch (error) {
      const code = codeOf(error);
      if (code === "EEXIST") continue;
      if (code !== "EPERM" && code !== "ENOTSUP" && code !== "EOPNOTSUPP") return undefined;
      // ponytail: a filesystem without hard links gets a guarded rename, and only inside OUR private lock
      // directory (our uid, mode 0700), where no other user can plant an entry between the check and the rename.
      try {
        const directory = fs.lstatSync(lock);
        const ours = directory.isDirectory() && (process.getuid === undefined || directory.uid === process.getuid()) &&
          (process.platform === "win32" || (directory.mode & 0o077) === 0);
        if (!ours || fs.existsSync(taken)) return undefined;
        fs.renameSync(ownerPath, taken);
        return taken;
      } catch { return undefined; }
    }
    try { fs.unlinkSync(ownerPath); } catch { try { fs.unlinkSync(taken); } catch { /* best effort */ } return undefined; }
    return taken;
  }
  if (!takeRefusedWarned) {
    takeRefusedWarned = true;
    try { console.warn(`[pi-fabric] mesh lock ${lock} not released: no free release name after 5 tries; the owner stays`); } catch { /* never mask */ }
  }
  return undefined;
};

let releaseLeftWarned = false;
/**
 * rmdir the released `<lock>` (unpinned path). On Windows the unlinked owner can stay delete-pending, or another
 * process can still hold a handle, for a moment: rmdir then fails ENOTEMPTY, EPERM or EBUSY (pi-fabric#889 CI).
 * ponytail: the retry stays synchronous and short, 5 attempts 20 ms apart (~80 ms of event-loop block at worst):
 * the callers release synchronously (holdMeshFenceSync, the custody release `() => void`, MeshLock's finally),
 * and a longer block would stall every other fiber on this loop for a cosmetic cleanup.
 * Race with a successor: between attempts a successor can only publish into THIS directory (a resumed protocol-1
 * `wx` owner write; mkdir and the protocol-2 rename both fail while the name exists). Safety does not rest on the
 * owner check below: rmdir removes only an EMPTY directory, so once a successor's owner is in it every later
 * attempt fails ENOTEMPTY; the check only stops retrying early. Still failing: log once and leave the empty
 * directory (the earlier behaviour); the ownerless-lock recovery reclaims it after its grace.
 */
const removeReleasedLock = (lock: string): void => {
  for (let attempt = 1; ; attempt++) {
    try { fs.rmdirSync(lock); return; } catch (error) {
      const code = codeOf(error);
      if (!(code === "ENOTEMPTY" || code === "EPERM" || code === "EBUSY") || fs.existsSync(path.join(lock, "owner"))) return;
      if (attempt >= 5) {
        if (!releaseLeftWarned) {
          releaseLeftWarned = true;
          try { console.warn(`[pi-fabric] mesh lock ${lock} released but its empty directory stays (${code}); stale-lock recovery reclaims it`); } catch { /* never mask a release */ }
        }
        return;
      }
      sleepSync(20);
    }
  }
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
