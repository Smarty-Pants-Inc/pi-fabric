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
import { createHash } from "node:crypto";

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

interface FenceRecord { format: 1; pid: number; host: string; startTime: string | null; createdAt: string }

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
export const takeMainPublicationFence = (meshRoot: string, rootId: string): (() => void) => {
  const file = mainPublicationFencePath(meshRoot, rootId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const record: FenceRecord = { format: 1, pid: process.pid, host: os.hostname(),
    startTime: processStartTime(process.pid) ?? null, createdAt: new Date().toISOString() };
  for (let attempt = 0; ; attempt++) {
    try {
      const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
      try { fs.writeSync(fd, JSON.stringify(record)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST" || attempt > 0) throw error;
      if (mainPublicationFenced(meshRoot, rootId)) throw new Error("Another operator actor removal holds this root's Main publication fence");
      fs.rmSync(file, { force: true }); // stale: its remover is gone
    }
  }
  return () => {
    try {
      const current = JSON.parse(fs.readFileSync(file, "utf8")) as Partial<FenceRecord>;
      if (current.pid === record.pid && current.createdAt === record.createdAt) fs.rmSync(file, { force: true });
    } catch { /* already gone */ }
  };
};
