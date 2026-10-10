/**
 * Root Main publication fence for an offline actor removal (smarty-dev#7817).
 *
 * The remover holds an exclusive flock(2) on the fence file for its whole check-plus-delete; the kernel
 * drops it when the remover dies, so a held fence never goes stale by time and a dead holder's never
 * outlives it. The record (who, when) is written complete for the audit. While the lock is held, the
 * root's Main refuses to publish its root participant record, in the same atomic step as that write
 * (under the participant key lock, or inside the state transaction). The remover checks under those
 * same fences while holding the lock, so a Main that published first is seen and a Main that publishes
 * later is refused.
 */
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { FileLockBusy, lockFile } from "../residency/file-lock.js";

export class MainPublicationFencedError extends Error {
  readonly code = "FABRIC_MAIN_PUBLICATION_FENCED";
  constructor(rootId: string) {
    super(`Root ${rootId} is fenced by an operator actor removal in progress; its Main participant is not published until it ends`);
  }
}

interface FenceRecord { format: 1; pid: number; host: string; createdAt: string }

export const mainPublicationFencePath = (meshRoot: string, rootId: string): string =>
  path.join(meshRoot, "main-publication-fences", createHash("sha256").update(rootId).digest("hex") + ".json");

/** Cheap and synchronous, for the Main's publish step. No fence file: not fenced. A fence file: held
 * exactly while its exclusive flock is held (a non-blocking shared flock test fails). Unknown: fenced. */
export const mainPublicationFenced = (meshRoot: string, rootId: string): boolean => {
  const file = mainPublicationFencePath(meshRoot, rootId);
  try { fs.lstatSync(file); }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  if (process.platform === "win32") {
    // ponytail: no flock on Windows; a same-host pid check stands in (kill 0: ESRCH means stale). The offline
    // remover itself refuses on Windows (smarty-dev#7858), so this only keeps a stray record from blocking forever.
    try {
      const record = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<FenceRecord>;
      if (typeof record.pid !== "number" || record.host !== os.hostname()) return true;
      process.kill(record.pid, 0);
      return true;
    } catch (error) { return (error as NodeJS.ErrnoException).code !== "ESRCH"; }
  }
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

/** Take the fence: an exclusive flock on the fence file, held until release. A held fence refuses; a stale
 * one (its holder died, so the kernel dropped its lock) is taken over by locking it, then rewriting it. */
export const takeMainPublicationFence = async (meshRoot: string, rootId: string): Promise<() => void> => {
  if (process.platform === "win32") throw new Error("The Main publication fence needs flock (smarty-dev#7858)");
  const file = mainPublicationFencePath(meshRoot, rootId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const record: FenceRecord = { format: 1, pid: process.pid, host: os.hostname(), createdAt: new Date().toISOString() };
  for (let attempt = 0; ; attempt++) {
    ensureFenceFile(file, record);
    let fd: number;
    try { fd = await lockFile(file, 0, true); }
    catch (error) {
      if (error instanceof FileLockBusy) throw new Error("Another operator actor removal holds this root's Main publication fence");
      throw error;
    }
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
        fs.rmSync(file, { force: true }); // unlink while still locked, then drop the lock
        fs.closeSync(fd);
      };
    }
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
    entry.release.catch(() => held.delete(key));
  }
  const releaseFence = await entry.release;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    if (--entry!.count === 0) { held.delete(key); releaseFence(); }
  };
};

/**
 * THE path that removes (moves to its archive) a root's actor tree in an offline removal (smarty-dev#7817): hold the root's
 * Main publication fence, run the final check under it, delete, release in finally. A Main cannot publish
 * its root participant between the check and the delete. The live host path is smarty-dev#8090.
 */
export const deleteRootActorTree = async (meshRoot: string, rootId: string, remove: () => void | Promise<void>,
  check?: () => void | Promise<void>): Promise<void> => {
  const release = await acquireMainPublicationFence(meshRoot, rootId);
  try {
    await check?.();
    await remove();
  } finally { release(); }
};
