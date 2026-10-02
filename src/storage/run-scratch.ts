import fs from "node:fs";
import path from "node:path";
import { removeTree } from "../agents/rm.js";
import { ownedStat } from "./scratch.js";

export const RUN_TMP_DIRECTORY = "tmp";
export const runTmpDirectory = (runDirectory: string): string => path.resolve(runDirectory, RUN_TMP_DIRECTORY);

/** Runner-owned namespace, never the caller's TMPDIR. Reuse the scratch owner/link check. */
export const createRunTmpDirectory = (runDirectory: string): string => {
  if (!ownedStat(runDirectory)?.isDirectory()) throw new Error("Unsafe Fabric run directory for scratch");
  const directory = runTmpDirectory(runDirectory);
  try {
    fs.mkdirSync(directory, { mode: 0o700 });
    // As for createScratch, set the exact mode on our new allocation despite umask.
    if (process.platform !== "win32") fs.chmodSync(directory, 0o700);
  }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  const stat = ownedStat(directory);
  if (!stat?.isDirectory() || (process.platform !== "win32" && (stat.mode & 0o777) !== 0o700)) {
    throw new Error("Unsafe Fabric run scratch directory (requires owner-only access)");
  }
  // ponytail: Windows mkdir mode bits do not install an owner-only ACL. The existing
  // Windows helper validates ACLs but does not create them; privacy there is inherited.
  return directory;
};

/** Exit/unsettled custody is checked by the runner before calling this helper. */
export const removeRunTmpDirectory = async (runDirectory: string): Promise<void> => {
  const directory = runTmpDirectory(runDirectory);
  if (!ownedStat(runDirectory)?.isDirectory() || !ownedStat(directory)?.isDirectory()) return;
  // Native recursive rm unlinks scratch symlinks without following their targets.
  await removeTree(directory);
};

/** Retention owns arbitrary scratch names, not arbitrary paths/foreign objects. */
export const safeRunTmpTree = (directory: string, expired: () => boolean, depth = 0): boolean => {
  if (expired() || depth > 32 || !ownedStat(directory)?.isDirectory()) return false;
  try {
    for (const name of fs.readdirSync(directory)) {
      if (expired()) return false;
      const file = path.join(directory, name);
      const stat = ownedStat(file);
      if (!stat) return false;
      if (stat.isFile()) continue;
      if (stat.isDirectory() && safeRunTmpTree(file, expired, depth + 1)) continue;
      return false;
    }
    return true;
  } catch { return false; }
};
