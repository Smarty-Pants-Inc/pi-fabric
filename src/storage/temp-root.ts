import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { refuseSharedSticky } from "./private-dir-policy.js";
import { windowsDataRoot } from "./windows-temp-root.js";

/**
 * macOS (smarty-dev#9527): refuse a root-owned sticky shared ancestor (/tmp, /private/tmp) of the data root BEFORE
 * anything is created at or below it (the override mkdir walk below, or a caller's mkdtemp such as AgentManager's
 * pi-fabric-runs-). Read-only: lstats the existing chain from `/`, following a symlink (the system aliases
 * /tmp -> private/tmp, /var -> private/var) only to stat what it names; stops at the first missing component.
 * Ownership and symlink policy stay with the existing validators.
 */
const refuseDarwinSharedSticky = (directory: string): void => {
  if (process.platform !== "darwin") return;
  const target = path.resolve(directory);
  let walked = path.parse(target).root;
  const names = target.slice(walked.length).split(path.sep).filter(Boolean);
  for (let index = 0; ; walked = path.join(walked, names[index++]!)) {
    let stat: fs.Stats | undefined;
    try {
      stat = fs.lstatSync(walked);
      if (stat.isSymbolicLink()) stat = fs.statSync(walked);
    } catch (error) {
      const errno = (error as NodeJS.ErrnoException).code;
      if (errno === "ENOENT" || errno === "ENOTDIR") return; // nothing exists below: no shared ancestor left to pass
      throw error;
    }
    if (!stat.isDirectory()) return; // the existing validators refuse it
    refuseSharedSticky(stat, walked, target);
    if (index >= names.length) return;
  }
};

/** File data only: socket/pipe paths must keep their short transport-specific roots. */
export const fabricDataRoot = (): string => {
  const root = process.env.PI_FABRIC_TMPDIR;
  if (!root) {
    const fallback = os.tmpdir();
    refuseDarwinSharedSticky(fallback);
    return fallback;
  }
  if (process.platform === "win32") return windowsDataRoot(root);
  if (!path.isAbsolute(root)) throw new Error("PI_FABRIC_TMPDIR must be an absolute path");
  const fail = (directory: string, reason: string): never => {
    throw new Error(`PI_FABRIC_TMPDIR is unsafe: ${directory} ${reason}`);
  };
  // Match the Herdr ledger's namespace criteria. Walk from the filesystem root so no
  // mkdir/write traverses an unchecked ancestor. Never chmod an existing path.
  const directory = path.resolve(root);
  refuseDarwinSharedSticky(directory); // before the mkdir walk below
  const chain: string[] = [];
  for (let current = directory; ; current = path.dirname(current)) {
    chain.unshift(current);
    if (path.dirname(current) === current) break;
  }
  const uid = process.getuid!();
  for (const current of chain) {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(current); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      try { fs.mkdirSync(current, { mode: 0o700 }); }
      catch (creationError) {
        if ((creationError as NodeJS.ErrnoException).code !== "EEXIST") throw creationError;
      }
      stat = fs.lstatSync(current);
    }
    if (stat.isSymbolicLink() || !stat.isDirectory()) fail(current, "is not a real directory");
    const final = current === directory;
    if (stat.uid !== uid && (final || stat.uid !== 0)) fail(current, "is owned by another user");
    if ((stat.mode & 0o022) !== 0 && (final || (stat.mode & 0o1000) === 0)) fail(current, "is writable by other users");
  }
  return directory;
};
