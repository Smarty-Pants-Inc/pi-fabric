/**
 * Root Main publication fence for an offline actor removal (smarty-dev#7817).
 *
 * The remover writes this marker (O_EXCL) BEFORE its first liveness check and removes it when it
 * finishes. While it exists and its writer is alive, the root's Main refuses to publish its root
 * participant record, in the same atomic step as that write (under the participant key lock, or
 * inside the state transaction). The remover checks under those same fences after the marker is
 * visible, so a Main that published first is seen and a Main that publishes later is refused.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";

/** Linux /proc/<pid>/stat field 22 (start ticks), after comm. */
const processStartTime = (pid: number): string | undefined => {
  if (process.platform !== "linux") return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
  } catch { return undefined; }
};

export class MainPublicationFencedError extends Error {
  readonly code = "FABRIC_MAIN_PUBLICATION_FENCED";
  constructor(rootId: string) {
    super(`Root ${rootId} is fenced by an operator actor removal in progress; its Main participant is not published until it ends`);
  }
}

interface FenceRecord { format: 1; pid: number; host: string; startTime: string | null; createdAt: string; expiresAt: number }

/** ponytail: one fixed lifetime instead of cross-host liveness. Any fence past expiresAt is stale on every
 * host and platform; a remover refuses its delete once its own fence is past it (smarty-dev#7817). */
export const MAIN_PUBLICATION_FENCE_TTL_MS = 10 * 60 * 1000;

export const mainPublicationFencePath = (meshRoot: string, rootId: string): string =>
  path.join(meshRoot, "main-publication-fences", createHash("sha256").update(rootId).digest("hex") + ".json");

/** True while a live remover holds the fence. Unreadable bytes are a fence (fail closed); a fence
 * whose writer is gone (crash) or reused its pid is stale. */
export const mainPublicationFenced = (meshRoot: string, rootId: string): boolean => {
  let raw: string;
  try { raw = fs.readFileSync(mainPublicationFencePath(meshRoot, rootId), "utf8"); }
  catch (error) { return (error as NodeJS.ErrnoException).code !== "ENOENT"; }
  let record: Partial<FenceRecord>;
  try { record = JSON.parse(raw) as Partial<FenceRecord>; } catch { return true; }
  if (typeof record.expiresAt === "number" && Date.now() > record.expiresAt) return false;
  if (typeof record.pid !== "number" || record.host !== os.hostname()) return true;
  if (!fs.existsSync(`/proc/${record.pid}`) && process.platform === "linux") return false;
  if (process.platform !== "linux") {
    try { process.kill(record.pid, 0); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; }
    return true;
  }
  const current = processStartTime(record.pid);
  return current === undefined || record.startTime === null || current === record.startTime;
};

/** Take the fence (O_EXCL); a stale one is replaced, a live one refuses. Returns its release. */
export const takeMainPublicationFence = (meshRoot: string, rootId: string): (() => void) & { expiresAt: number } => {
  const file = mainPublicationFencePath(meshRoot, rootId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const record: FenceRecord = { format: 1, pid: process.pid, host: os.hostname(),
    startTime: processStartTime(process.pid) ?? null, createdAt: new Date().toISOString(),
    expiresAt: Date.now() + MAIN_PUBLICATION_FENCE_TTL_MS };
  // Never a fence without a complete record: write a unique temp file in full (O_EXCL, fsync), then
  // link it into place (atomic; EEXIST if a fence exists), and unlink the temp in finally. A crash
  // leaves at most a *.tmp file, which is never the fence.
  for (let attempt = 0; ; attempt++) {
    const tmp = `${file}.${process.pid}.${randomUUID()}.tmp`;
    try {
      const fd = fs.openSync(tmp, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
      try { fs.writeSync(fd, JSON.stringify(record)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      fs.linkSync(tmp, file);
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST" || attempt > 0) throw error;
      if (mainPublicationFenced(meshRoot, rootId)) throw new Error("Another operator actor removal holds this root's Main publication fence");
      fs.rmSync(file, { force: true }); // stale: its remover is gone
    } finally { fs.rmSync(tmp, { force: true }); }
  }
  return Object.assign(() => {
    try {
      const current = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<FenceRecord>;
      if (current.pid === record.pid && current.createdAt === record.createdAt) fs.rmSync(file, { force: true });
    } catch { /* already gone */ }
  }, { expiresAt: record.expiresAt });
};

const held = new Map<string, { count: number; release: (() => void) & { expiresAt: number } }>();

/** Take this root's fence, shared (reference-counted) by the removals of this process. */
export const acquireMainPublicationFence = (meshRoot: string, rootId: string): (() => void) & { expiresAt: number } => {
  const key = `${path.resolve(meshRoot)}\0${rootId}`;
  let entry = held.get(key);
  if (entry) entry.count += 1;
  else { entry = { count: 1, release: takeMainPublicationFence(meshRoot, rootId) }; held.set(key, entry); }
  let released = false;
  return Object.assign(() => {
    if (released) return;
    released = true;
    if (--entry!.count === 0) { held.delete(key); entry!.release(); }
  }, { expiresAt: entry.release.expiresAt });
};

/**
 * THE path that deletes a root's actor tree (smarty-dev#7817): take the root's Main publication fence,
 * run the final check under it, delete, release in finally. A Main cannot publish its root participant
 * between the check and the delete. Every actor-tree delete (the offline remover and every
 * ActorManager cleanup, which the resident host's live remove uses) goes through here.
 */
export const deleteRootActorTree = async (meshRoot: string, rootId: string, remove: () => void,
  check?: () => void | Promise<void>): Promise<void> => {
  const release = acquireMainPublicationFence(meshRoot, rootId);
  try {
    await check?.();
    // An expired fence no longer stops a Main publishing: never delete under one.
    if (Date.now() >= release.expiresAt) {
      throw new Error(`The removal's Main publication fence expired at ${new Date(release.expiresAt).toISOString()}; refusing to delete the actor tree (retry the removal)`);
    }
    remove();
  } finally { release(); }
};
