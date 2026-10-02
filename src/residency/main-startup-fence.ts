import fs from "node:fs";
import path from "node:path";
import { lockFile } from "./file-lock.js";
import { residentRoot } from "./protocol.js";

/** Short native ownership fence, separate from the resident host's lifetime host.lock.
 * Prune holds BOTH fences through its destructive commits. Never unlink either inode. */
export const nativeMainStartupLock = (meshRoot: string, rootId: string): string =>
  path.join(residentRoot(meshRoot, rootId), "main-start.lock");

/** Publish the first live lease and load the registry while holding prune's native fence.
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
