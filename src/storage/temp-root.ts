import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { refuseSharedSticky, UnsafeDirectory } from "./private-dir-policy.js";
import { windowsDataRoot } from "./windows-temp-root.js";

/** Reject an existing non-directory instead of leaving it in a missing suffix. */
const refuseNonDirectory = (stat: fs.Stats | undefined, walked: string, target: string): void => {
  if (stat?.isDirectory()) return;
  throw new UnsafeDirectory(`${walked} ${stat ? "is not a directory" : "has an ancestor that is not a directory"}: `
    + `Fabric cannot keep ${target} private under it (smarty-dev#9527)`);
};

/** Symlink-following stat of an existing component: ENOTDIR is the same non-directory refusal; any other error throws. */
const statExisting = (file: string, target: string): fs.Stats => {
  try { return fs.statSync(file); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOTDIR") refuseNonDirectory(undefined, file, target);
    throw error;
  }
};

/**
 * macOS (smarty-dev#9527): refuse a root-owned sticky shared ancestor (/tmp, /private/tmp) of the data root BEFORE
 * anything is created at or below it (the override mkdir walk below, or a caller's mkdtemp such as AgentManager's
 * pi-fabric-runs-). Read-only: lstats the existing chain from `/`, following a symlink (the system aliases
 * /tmp -> private/tmp, /var -> private/var) only to stat what it names; stops at the first missing component.
 * Then checks the PHYSICAL chain of the deepest existing directory (realpath), so a symlink lexically outside the
 * shared directory cannot name a child inside it. A dangling or unresolvable symlink, or any realpath/stat error on
 * the existing prefix, throws (fail-closed, nothing written); an existing component (lexical or physical) that is not a
 * directory in its stat snapshot, or an lstat/stat ENOTDIR, is refused; only a missing lexical component (lstat ENOENT)
 * ends the walk, and the missing suffix is never realpath'ed. Ownership and symlink policy stay with the existing validators.
 * Returns the vetted physical path (realpath of the deepest existing directory joined with the lexical missing suffix)
 * so callers never re-follow a mutable alias chain after the check; any other platform gets its input back unchanged.
 */
const refuseDarwinSharedSticky = (directory: string): string => {
  if (process.platform !== "darwin") return directory;
  const target = path.resolve(directory);
  let walked = path.parse(target).root;
  const names = target.slice(walked.length).split(path.sep).filter(Boolean);
  let deepest = walked;
  for (let index = 0; ; walked = path.join(walked, names[index++]!)) {
    let stat: fs.Stats;
    try { stat = fs.lstatSync(walked); }
    catch (error) {
      const errno = (error as NodeJS.ErrnoException).code;
      if (errno === "ENOENT") break; // missing component: nothing exists below
      if (errno === "ENOTDIR") refuseNonDirectory(undefined, walked, target); // fail-closed, never a missing suffix
      throw error;
    }
    if (stat.isSymbolicLink()) stat = statExisting(walked, target); // dangling/unresolvable link throws: refuse, never create
    refuseNonDirectory(stat, walked, target); // this snapshot decides, even if the target later becomes a directory
    refuseSharedSticky(stat, walked, target);
    deepest = walked;
    if (index >= names.length) break;
  }
  const physical = fs.realpathSync(deepest); // throws on any error: fail-closed before any write
  let real = path.parse(physical).root;
  for (const name of ["", ...physical.slice(real.length).split(path.sep).filter(Boolean)]) {
    real = path.join(real, name);
    const realStat = statExisting(real, target);
    refuseNonDirectory(realStat, real, target);
    refuseSharedSticky(realStat, real, target);
  }
  return path.join(physical, path.relative(deepest, target));
};

/** File data only: socket/pipe paths must keep their short transport-specific roots. */
export const fabricDataRoot = (): string => {
  const root = process.env.PI_FABRIC_TMPDIR;
  if (!root) {
    return refuseDarwinSharedSticky(os.tmpdir()); // vetted canonical path, never the lexical alias
  }
  if (process.platform === "win32") return windowsDataRoot(root);
  if (!path.isAbsolute(root)) throw new Error("PI_FABRIC_TMPDIR must be an absolute path");
  const fail = (directory: string, reason: string): never => {
    throw new Error(`PI_FABRIC_TMPDIR is unsafe: ${directory} ${reason}`);
  };
  // Match the Herdr ledger's namespace criteria. Walk from the filesystem root so no
  // mkdir/write traverses an unchecked ancestor. Never chmod an existing path.
  const directory = path.resolve(root);
  const vetted = refuseDarwinSharedSticky(directory); // before the mkdir walk below
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
  return vetted;
};
