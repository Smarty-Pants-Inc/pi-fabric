import fs from "node:fs";
import path from "node:path";
import { writeJsonAtomic } from "../core/atomic-write.js";
import { windowsDataRoot } from "./windows-temp-root.js";
import { ownedStat } from "./scratch.js";

export const RUN_TMP_DIRECTORY = "tmp";
export const UNRESOLVED_SCRATCH_FILE = "unresolved-scratch.json";

/** No supported process transport currently contains every inheriting descendant.
 * A missing fence is not proof either (legacy runs, crash during allocation).
 * Do not accept worker PID death, terminal status, age or nested-run records as
 * a complete, identity-bound scope exit receipt. Unknown custody stays fenced. */
export const runScratchExitVeto = (runDirectory: string): string | undefined => {
  for (const name of [UNRESOLVED_SCRATCH_FILE, RUN_TMP_DIRECTORY]) {
    try { fs.lstatSync(path.join(runDirectory, name)); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      return "scratch writer exit is unconfirmed: custody inspection failed";
    }
    return "scratch writer exit is unconfirmed: no complete descendant scope exit receipt";
  }
};
export const runTmpDirectory = (runDirectory: string): string => path.resolve(runDirectory, RUN_TMP_DIRECTORY);

/** Runner-owned namespace, never the caller's TMPDIR. Reuse the scratch owner/link check. */
export const createRunTmpDirectory = (runDirectory: string): string => {
  if (!ownedStat(runDirectory)?.isDirectory()) throw new Error("Unsafe Fabric run directory for scratch");
  // Mode bits prove nothing on Windows. Inspect the run root before any write,
  // including explicit/resident roots that bypass fabricDataRoot().
  if (process.platform === "win32") windowsDataRoot(runDirectory, { private: true });
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
  if (process.platform === "win32") windowsDataRoot(directory, { private: true });
  // Persist before a worker can inherit this namespace. The native ChildProcess
  // close/exit receipt covers only that worker, not redirected/detached tools.
  // Until the transport owns a contained, identity-bound descendant scope, no
  // complete exit receipt can be issued. Retain rather than guess or signal PIDs.
  const fence = path.join(runDirectory, UNRESOLVED_SCRATCH_FILE);
  if (fs.existsSync(fence) && !ownedStat(fence)?.isFile()) throw new Error("Unsafe Fabric scratch custody fence");
  writeJsonAtomic(fence, { version: 1, reason: "uncontained process descendants", runDirectory: path.resolve(runDirectory) }, { durable: true });
  return directory;
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
