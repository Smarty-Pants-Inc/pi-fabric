import fs from "node:fs";
import path from "node:path";
import { lockFile } from "./file-lock.js";
import { residentRoot } from "./protocol.js";

/** Native Main lifetime ownership fence, separate from the resident host's host.lock.
 * The startup path acquires it before publication/load, and retains it until shutdown.
 * Prune holds BOTH fences through its commits. Never unlink either inode. */
export const nativeMainStartupLock = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-start.lock");

/** Acquire under the existing startup boundary; the caller must keep the returned
 * release until ownership writers drain at shutdown/reinitialization (or load fails).
 * Process death drops the flock even if every heartbeat has expired.
 * Non-Linux cannot destructively prune. On Linux a missing/broken helper fails closed:
 * another Pi process may have a different PATH, so its capability probe is not ours. */
export const acquireNativeMainStartupFence = async (meshRoot: string, rootId: string): Promise<() => void> => {
  if (process.platform !== "linux") return () => {};
  const file = nativeMainStartupLock(meshRoot, rootId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const fd = await lockFile(file, 120, true);
  try {
    const held = fs.fstatSync(fd), current = fs.lstatSync(file);
    if (!current.isFile() || current.dev !== held.dev || current.ino !== held.ino) throw new Error("Native Main startup fence changed");
    return () => fs.closeSync(fd);
  } catch (error) { fs.closeSync(fd); throw error; }
};
