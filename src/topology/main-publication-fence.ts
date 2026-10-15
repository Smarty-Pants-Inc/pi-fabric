/**
 * Root Main publication fence for an offline actor removal (smarty-dev#7817).
 *
 * The remover holds an exclusive kernel lock on the fence file for its whole check-plus-delete; the kernel
 * drops it when the remover dies, so a held fence never goes stale by time and a dead holder's never
 * outlives it. The record (who, when) is written complete for the audit. While the lock is held, the
 * root's Main refuses to publish its root participant record, in the same atomic step as that write
 * (under the participant key lock, or inside the state transaction). The remover checks under those
 * same fences while holding the lock, so a Main that published first is seen and a Main that publishes
 * later is refused. Linux uses flock(2), Darwin uses open-time flock locks, and Windows uses exclusive
 * handle sharing (smarty-dev#11335). Where the fence cannot be held, no root actor tree deletion runs.
 */
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { FileLockBusy, kernelFenceAvailable, lockFile } from "../residency/file-lock.js";

export class MainPublicationFencedError extends Error {
  readonly code = "FABRIC_MAIN_PUBLICATION_FENCED";
  constructor(rootId: string) {
    super(`Root ${rootId} is fenced by an operator actor removal in progress; its Main participant is not published until it ends`);
  }
}

interface FenceRecord { format: 1; pid: number; host: string; createdAt: string }

export const mainPublicationFencePath = (meshRoot: string, rootId: string): string =>
  path.join(meshRoot, "main-publication-fences", createHash("sha256").update(rootId).digest("hex") + ".json");

// Node forwards numeric flags unchanged: Darwin open(2) bits and Windows libuv's exclusive sharing bit.
const DARWIN_O_EXLOCK = 0x20;
const DARWIN_O_SHLOCK = 0x10;
const DARWIN_O_NONBLOCK = 0x4;
const WINDOWS_UV_FS_O_EXLOCK = 0x10000000;

/** Open and validate a portable fence, acquiring its kernel lock as part of the open itself. */
const openPortableFence = (file: string, exclusive: boolean): number => {
  const windows = process.platform === "win32";
  const uid = windows ? undefined : process.getuid?.();
  // Windows O_NOFOLLOW is 0: reject links/non-files before open, then compare bigint inode/dev after it.
  const current = fs.lstatSync(file, { bigint: true });
  if (!current.isFile() || (!windows && (uid === undefined || current.uid !== BigInt(uid)))) {
    throw new Error(`${file} is not a safe regular fence file`);
  }
  const flags = windows
    ? (exclusive ? fs.constants.O_RDWR | WINDOWS_UV_FS_O_EXLOCK : fs.constants.O_RDONLY)
    : (exclusive ? fs.constants.O_RDWR | DARWIN_O_EXLOCK : fs.constants.O_RDONLY | DARWIN_O_SHLOCK) |
      DARWIN_O_NONBLOCK | fs.constants.O_NOFOLLOW;
  const fd = fs.openSync(file, flags);
  try {
    const opened = fs.fstatSync(fd, { bigint: true });
    if (!opened.isFile() || (!windows && (uid === undefined || opened.uid !== BigInt(uid)))) {
      throw new Error(`${file} is not a safe regular fence file`);
    }
    if (current.ino !== opened.ino || current.dev !== opened.dev) {
      throw new Error(`The Main publication fence changed while opening ${file}`);
    }
    // Darwin can check the locked pathname too; Windows must not require lstat while share-zero is held.
    if (!windows) {
      const named = fs.lstatSync(file, { bigint: true });
      if (named.ino !== opened.ino || named.dev !== opened.dev) {
        throw new Error(`The Main publication fence changed while opening ${file}`);
      }
    }
    return fd;
  } catch (error) { fs.closeSync(fd); throw error; }
};

/** Cheap and synchronous, for the Main's publish step. No fence file: not fenced. A fence file: held
 * exactly while its kernel lock is held (a non-blocking probe fails). Unknown: fenced. */
export const mainPublicationFenced = (meshRoot: string, rootId: string): boolean => {
  // Unsupported platforms cannot take a fence or start a root actor tree deletion.
  if (process.platform !== "linux" && process.platform !== "darwin" && process.platform !== "win32") return false;
  const file = mainPublicationFencePath(meshRoot, rootId);
  try { fs.lstatSync(file); }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  // lstat above is only an existence/error check, never Windows lock authority.
  if (process.platform === "darwin" || process.platform === "win32") {
    try { fs.closeSync(openPortableFence(file, false)); return false; }
    catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  }
  // No pid or time guess stands in for the kernel lock (smarty-dev#8090).
  const probe = spawnSync("flock", ["-s", "-n", file, "true"], { stdio: "ignore", timeout: 5_000 });
  return probe.status !== 0; // 1: held; anything else (no flock, error): fail closed
};

/** Create the fence file with its complete record (temp file, then link; EEXIST keeps the existing one). */
const ensureFenceFile = (file: string, record: FenceRecord): void => {
  const tmp = `${file}.${process.pid}.${randomUUID()}.tmp`;
  try {
    const fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeSync(fd, JSON.stringify(record)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try { fs.linkSync(tmp, file); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  } finally { fs.rmSync(tmp, { force: true }); }
};

/** Refuse where this platform has no kernel Main publication fence: no root actor tree deletion runs there
 * (smarty-dev#8090, smarty-dev#11335). Callers run it before any claim or mutation. */
export const assertMainPublicationFencePlatform = (rootId: string): void => {
  if (process.platform !== "linux" && process.platform !== "darwin" && process.platform !== "win32") {
    throw new Error(`Root actor tree deletion needs the Main publication fence, which this platform (${process.platform}) lacks ` +
      `(smarty-dev#8090); follow-up smarty-dev#11335; the root ${rootId} actor tree is kept`);
  }
};

/** Take the fence: an exclusive kernel lock on the fence file, held until release. A held fence refuses; a stale
 * one (its holder died, so the kernel dropped its lock) is taken over by locking it, then rewriting it. */
export const takeMainPublicationFence = async (meshRoot: string, rootId: string): Promise<() => void> => {
  // Before any file is made: without a kernel lock there is no fence, and nothing is deleted (smarty-dev#8090).
  const kept = `the root ${rootId} actor tree is kept`;
  assertMainPublicationFencePlatform(rootId);
  if (process.platform === "linux" && !kernelFenceAvailable()) throw new Error(`The Main publication fence needs flock(1) and setpriv, not found; ${kept}`);
  const file = mainPublicationFencePath(meshRoot, rootId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const record: FenceRecord = { format: 1, pid: process.pid, host: os.hostname(), createdAt: new Date().toISOString() };
  if (process.platform === "darwin" || process.platform === "win32") {
    let fd: number;
    try { ensureFenceFile(file, record); fd = openPortableFence(file, true); }
    catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === "EAGAIN" || code === "EBUSY") throw new Error("Another operator actor removal holds this root's Main publication fence");
      throw new Error(`The Main publication fence could not be locked (${error instanceof Error ? error.message : String(error)}); ${kept}`);
    }
    try {
      // Only the kernel lock authorizes takeover: rewrite the complete audit, never consult PID or age.
      const bytes = Buffer.from(JSON.stringify(record));
      fs.ftruncateSync(fd, 0); fs.writeSync(fd, bytes, 0, bytes.length, 0); fs.fsyncSync(fd);
      let released = false;
      return () => {
        if (released) return;
        released = true;
        // Keep the same inode after close/death; unlink is neither needed nor safe for portable release.
        fs.closeSync(fd);
      };
    } catch (error) { fs.closeSync(fd); throw error; }
  }
  for (let attempt = 0; ; attempt++) {
    ensureFenceFile(file, record);
    let fd: number;
    try { fd = await lockFile(file, 0, true); }
    catch (error) {
      if (error instanceof FileLockBusy) throw new Error("Another operator actor removal holds this root's Main publication fence");
      throw new Error(`The Main publication fence could not be locked (${error instanceof Error ? error.message : String(error)}); ${kept}`);
    }
    try {
      // The lock must be on the file at the path: a releasing holder may have unlinked it meanwhile.
      let current: fs.Stats | undefined;
      try { current = fs.lstatSync(file); } catch { /* unlinked: retry */ }
      const locked = fs.fstatSync(fd);
      if (current && current.ino === locked.ino && current.dev === locked.dev) {
        const bytes = Buffer.from(JSON.stringify(record));
        fs.ftruncateSync(fd, 0); fs.writeSync(fd, bytes, 0, bytes.length, 0); fs.fsyncSync(fd);
        let released = false;
        return () => {
          if (released) return;
          released = true;
          // Unlink while still locked, then drop the lock; the descriptor closes even when the unlink fails.
          try { fs.rmSync(file, { force: true }); } finally { fs.closeSync(fd); }
        };
      }
    } catch (error) { fs.closeSync(fd); throw error; }
    fs.closeSync(fd);
    if (attempt >= 3) throw new Error("The root's Main publication fence kept changing; retry the removal");
  }
};

const held = new Map<string, { count: number; release: Promise<() => void> }>();

/** Take this root's fence, shared (reference-counted) by the removals of this process. */
export const acquireMainPublicationFence = async (meshRoot: string, rootId: string): Promise<() => void> => {
  const key = `${path.resolve(meshRoot)}\0${rootId}`;
  let entry = held.get(key);
  if (entry) entry.count += 1;
  else {
    entry = { count: 1, release: takeMainPublicationFence(meshRoot, rootId) };
    held.set(key, entry);
    const failed = entry;
    entry.release.catch(() => { if (held.get(key) === failed) held.delete(key); });
  }
  const releaseFence = await entry.release;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    if (--entry!.count === 0) { held.delete(key); releaseFence(); }
  };
};

/** Test seam only: awaited under the held fence, before the final check and between it and the delete. */
export const actorTreeDeletionHooks: {
  beforeCheck?: () => void | Promise<void>;
  beforeDelete?: () => void | Promise<void>;
} = {};

/**
 * THE path that deletes (or moves to its archive) a root's whole actor tree (smarty-dev#7817, smarty-dev#8090): the
 * offline archive move, the live host's ActorManager cleanup, and a non-persistent manager's close. Hold the
 * root's Main publication fence, run the final check under it, delete, release in finally. A Main cannot
 * publish its root participant between the check and the delete. The fence is taken unconditionally, before the
 * hooks, the check and the delete: where it cannot be held (missing Linux helpers, an unsupported platform, or
 * a portable open/validation failure) this throws and nothing is checked or deleted; the tree is kept.
 */
export const deleteRootActorTree = async (meshRoot: string, rootId: string, remove: () => void | Promise<void>,
  check?: () => void | Promise<void>): Promise<void> => {
  const release = await acquireMainPublicationFence(meshRoot, rootId);
  try {
    await actorTreeDeletionHooks.beforeCheck?.();
    await check?.();
    await actorTreeDeletionHooks.beforeDelete?.();
    await remove();
  } finally { release(); }
};
