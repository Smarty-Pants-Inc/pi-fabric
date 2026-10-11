import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

/**
 * The one removal path of every retention and scratch deleter (smarty-dev#7916). A deleter judges a target by
 * path (its stat, age and contents) and then hands that judged stat here. This removes only that inode:
 *
 * 1. Open the parent O_DIRECTORY|O_NOFOLLOW and the target O_PATH|O_NOFOLLOW (|O_DIRECTORY for a directory) through
 *    the parent's fd, fstat it: uid must be geteuid, dev/ino and type must equal the judged stat, and its mtime must
 *    be unchanged (the age re-check: a refreshed target is not the judged one).
 * 2. Walk the whole tree through fds; every entry must be ours. Any doubt: nothing is changed. A dry run stops here,
 *    so it runs the same judgment and verification as an apply, minus the rename and the delete.
 * 3. Rename it into a same-directory graveyard (`.retention-graveyard-<pid>`), fstat it there again (dev/ino/uid).
 *    A mismatch leaves it in the graveyard, reported, and deletes nothing.
 * 4. `proceed()` (a caller's last check, for example a registry re-read) may keep it: it is moved back.
 * 5. Remove it directory-relative, entry by entry, each verified again before its unlink or rmdir.
 *
 * Directory-relative operations go through `/proc/self/fd/<fd>/<name>` (Node has no unlinkat or renameat; the same
 * pin as mesh/fence-lock.ts). Where that is unavailable (not Linux: macOS, Windows), a deleter that main already ran
 * in-process passes its unchanged main-branch removal as `fallback`, so nothing regresses there; without a fallback
 * nothing is deleted. The verified path off Linux is carried by smarty-dev#7858.
 * Residual: unlink and rmdir take a name, so a same-uid process that finds the random graveyard name and swaps an
 * entry inside our private graveyard between step 5's check and its unlink can make us remove its own file. That
 * file is already its own; no other user's file and nothing outside the graveyard can be reached.
 */

export type VerifiedRemoveResult = { removed: true; bytes: number } | { removed: false; reason: string; graveyard?: string };

/** Per-deleter counts for the retention CLI's report: removed (or, in a dry run, would be), bytes, refusals by reason.
 * ponytail: a module-level tally the one-shot CLI turns on; in-process sweeps leave it off and pay nothing. */
export interface DeleterTally { removed: number; bytes: number; refused: Record<string, number>; examples: Record<string, string[]> }
let tally: Record<string, DeleterTally> | undefined;
export const startRemovalTally = (): Record<string, DeleterTally> => (tally = {});
export const stopRemovalTally = (): void => { tally = undefined; };
const entry = (deleter: string): DeleterTally => (tally![deleter] ??= { removed: 0, bytes: 0, refused: {}, examples: {} });
export const countRemoval = (deleter: string, bytes: number): void => {
  if (!tally) return;
  const item = entry(deleter); item.removed++; item.bytes += bytes;
};
/** A refusal (by the deleter's own judgment or by this helper). `reason` is a fixed category; up to 3 example paths. */
export const countRefusal = (deleter: string, reason: string, target?: string): void => {
  if (!tally) return;
  const item = entry(deleter);
  item.refused[reason] = (item.refused[reason] ?? 0) + 1;
  if (target !== undefined) { const examples = item.examples[reason] ??= []; if (examples.length < 3) examples.push(target); }
};

/** Test seam: runs after the judgment, right before the first open (a swap between judgment and delete). */
export const verifiedRemoveTestHooks: { beforeOpen?: (target: string) => void } = {};

// O_PATH (Linux): an fd for fstat and /proc traversal only; never reads a file, never blocks on a FIFO.
const O_PATH = 0o10000000;
const NOFOLLOW = fs.constants.O_NOFOLLOW ?? 0;
const DIRECTORY = fs.constants.O_DIRECTORY ?? 0;
const MAX_DEPTH = 64;
const at = (fd: number, name?: string): string => name === undefined ? `/proc/self/fd/${fd}` : `/proc/self/fd/${fd}/${name}`;
const code = (error: unknown): string => (error as NodeJS.ErrnoException)?.code ?? "error";

class Refusal extends Error {}
const refuse = (reason: string): never => { throw new Refusal(reason); };

/** fstat through an O_PATH|O_NOFOLLOW fd. A symbolic link is opened as itself, never followed. */
const fdStat = (file: string, directory: boolean): fs.Stats => {
  let fd: number;
  try { fd = fs.openSync(file, O_PATH | NOFOLLOW | (directory ? DIRECTORY : 0)); }
  catch (error) { return refuse(code(error) === "ENOTDIR" ? "type changed since judgment" : `open failed (${code(error)})`); }
  try { return fs.fstatSync(fd); } finally { fs.closeSync(fd); }
};
const same = (a: fs.Stats, b: fs.Stats): boolean => a.dev === b.dev && a.ino === b.ino;
const kind = (stat: fs.Stats): string => stat.isDirectory() ? "directory" : stat.isFile() ? "file" : stat.isSymbolicLink() ? "symlink" : "other";
/** The judged inode, ours, same type, same mtime. `moved`: after the rename only dev/ino/uid/type are compared. */
const verify = (file: string, judged: fs.Stats, uid: number, moved = false): void => {
  const stat = fdStat(file, judged.isDirectory());
  if (stat.isSymbolicLink()) refuse("target is a symbolic link");
  if (stat.uid !== uid) refuse("target is not owned by this user");
  if (!same(stat, judged)) refuse("target inode was replaced since judgment");
  if (kind(stat) !== kind(judged)) refuse("type changed since judgment");
  if (!moved && stat.mtimeMs !== judged.mtimeMs) refuse("target was modified since judgment");
};
const open = (file: string): number => {
  try { return fs.openSync(file, O_PATH | NOFOLLOW | DIRECTORY); }
  catch (error) { return refuse(`open failed (${code(error)})`); }
};
/** Every entry below `fd` must be ours; directories are entered only through an fd whose fstat matches the lstat. */
const verifyTree = (fd: number, uid: number, depth: number): number => {
  if (depth > MAX_DEPTH) refuse("tree is too deep");
  let bytes = 0;
  for (const name of fs.readdirSync(at(fd))) {
    const stat = fs.lstatSync(at(fd, name));
    if (stat.uid !== uid) refuse("an entry in the tree is not owned by this user");
    if (!stat.isDirectory()) { bytes += stat.isFile() ? stat.size : 0; continue; }
    const child = open(at(fd, name));
    try {
      if (!same(fs.fstatSync(child), stat)) refuse("an entry in the tree changed during the walk");
      bytes += verifyTree(child, uid, depth + 1);
    } finally { fs.closeSync(child); }
  }
  return bytes;
};
/** Remove `name` below `parent`, each entry verified again; unlink and rmdir never follow a link. */
const removeTree = (parent: number, name: string, uid: number, depth: number): void => {
  if (depth > MAX_DEPTH) refuse("tree is too deep");
  const stat = fs.lstatSync(at(parent, name));
  if (stat.uid !== uid) refuse("an entry in the tree is not owned by this user");
  if (!stat.isDirectory()) { fs.unlinkSync(at(parent, name)); return; }
  const fd = open(at(parent, name));
  try {
    if (!same(fs.fstatSync(fd), stat)) refuse("an entry in the tree changed during removal");
    for (const child of fs.readdirSync(at(fd))) removeTree(fd, child, uid, depth + 1);
  } finally { fs.closeSync(fd); }
  fs.rmdirSync(at(parent, name));
};

export const verifiedRemove = (target: string, judged: fs.Stats | undefined, options: {
  /** The deleter's name, for the report. */
  deleter: string;
  dryRun?: boolean;
  /** The caller's last check, after the move and its re-verify, right before the delete. False keeps it (moved back). */
  proceed?: () => boolean;
  /** Not Linux only: main's unchanged path-based removal (smarty-dev#7858 carries the verified path there). Returns
   * whether it removed the target. Never used on Linux. */
  fallback?: () => boolean;
  /** Bytes this removal frees, when not the whole target (a compaction replaces it with a smaller tail). */
  freed?: number;
}): VerifiedRemoveResult => {
  const { deleter } = options;
  const no = (reason: string, graveyard?: string): VerifiedRemoveResult => {
    countRefusal(deleter, reason, target);
    return graveyard === undefined ? { removed: false, reason } : { removed: false, reason, graveyard };
  };
  if (process.platform !== "linux" && options.fallback) {
    if (options.dryRun) return { removed: true, bytes: options.freed ?? (judged?.isFile() ? judged.size : 0) };
    let removed = false;
    try { removed = options.fallback(); } catch { /* main's best effort */ }
    return removed ? { removed: true, bytes: 0 } : no("removal failed (main's path on this platform)");
  }
  if (typeof process.getuid !== "function" || typeof process.geteuid !== "function") {
    return no("file ownership cannot be proven on this platform (no getuid); nothing is deleted");
  }
  if (process.platform !== "linux") return no("directory-relative removal needs Linux /proc/self/fd; nothing is deleted");
  if (!judged) return no("no judged stat");
  const name = path.basename(target);
  if (!name || name === "." || name === ".." || path.dirname(target) === target) return no("invalid target name");
  let parent: number | undefined, grave: number | undefined, graveName: string | undefined, moved: string | undefined;
  try {
    verifiedRemoveTestHooks.beforeOpen?.(target);
    const uid = process.geteuid();
    try { parent = fs.openSync(path.dirname(target), fs.constants.O_RDONLY | NOFOLLOW | DIRECTORY); }
    catch (error) { return no(`parent open failed (${code(error)})`); }
    const parentStat = fs.fstatSync(parent);
    if (parentStat.uid !== uid) return no("parent directory is not owned by this user");
    // The pin must resolve to the opened directory, or no directory-relative step is possible.
    try { if (!same(fs.statSync(at(parent)), parentStat)) return no("/proc/self/fd does not resolve the parent; nothing is deleted"); }
    catch { return no("/proc/self/fd is unavailable; nothing is deleted"); }
    verify(at(parent, name), judged, uid);
    let bytes = judged.isFile() ? judged.size : 0;
    if (judged.isDirectory()) {
      const fd = open(at(parent, name));
      try {
        if (!same(fs.fstatSync(fd), judged)) refuse("target inode was replaced since judgment");
        bytes = verifyTree(fd, uid, 0);
      } finally { fs.closeSync(fd); }
    } else if (!judged.isFile()) refuse("target is not a regular file or directory");
    if (options.freed !== undefined) bytes = options.freed;
    if (options.dryRun) { countRemoval(deleter, bytes); return { removed: true, bytes }; }

    graveName = `.retention-graveyard-${process.pid}`;
    try { fs.mkdirSync(at(parent, graveName), { mode: 0o700 }); }
    catch (error) { if (code(error) !== "EEXIST") throw error; }
    grave = open(at(parent, graveName));
    const graveStat = fs.fstatSync(grave);
    if (graveStat.uid !== uid || (graveStat.mode & 0o077) !== 0) { grave = (fs.closeSync(grave), undefined); return no("graveyard is not our private directory"); }
    const entryName = `${name}.${randomUUID()}`;
    fs.renameSync(at(parent, name), at(grave, entryName));
    moved = entryName;
    const graveyard = path.join(path.dirname(target), graveName, moved);
    try { verify(at(grave, moved), judged, uid, true); }
    catch (error) {
      // Whatever we moved is not the judged inode: it stays in the graveyard, untouched and reported.
      const reason = error instanceof Refusal ? error.message : `verify failed (${code(error)})`;
      moved = undefined;
      return no(`${reason} (swapped during removal; left in the graveyard)`, graveyard);
    }
    let keep: boolean;
    try { keep = options.proceed ? !options.proceed() : false; } catch { keep = true; }
    if (keep) {
      // Back to its name, never over anything that appeared there: link fails on EEXIST; a directory moves back
      // only while the name is absent. Otherwise it stays in the graveyard and is reported.
      try {
        if (judged.isDirectory()) {
          try { fs.lstatSync(at(parent, name)); return no("kept by the final check; name is taken, left in the graveyard", graveyard); }
          catch (error) { if (code(error) !== "ENOENT") throw error; }
          fs.renameSync(at(grave, moved), at(parent, name));
        } else {
          fs.linkSync(at(grave, moved), at(parent, name));
          fs.unlinkSync(at(grave, moved));
        }
        moved = undefined;
        return no("kept by the final check");
      } catch (error) {
        moved = undefined;
        return no(`kept by the final check; move back failed (${code(error)}), left in the graveyard`, graveyard);
      }
    }
    try { removeTree(grave, moved, uid, 0); }
    catch (error) {
      const reason = error instanceof Refusal ? error.message : `removal failed (${code(error)})`;
      moved = undefined;
      return no(`${reason}; the rest is left in the graveyard`, graveyard);
    }
    moved = undefined;
    countRemoval(deleter, bytes);
    return { removed: true, bytes };
  } catch (error) {
    return no(error instanceof Refusal ? error.message : `removal failed (${code(error)})`);
  } finally {
    if (grave !== undefined) fs.closeSync(grave);
    // An empty graveyard goes; one that holds a reported entry stays (rmdir fails closed on any entry).
    if (parent !== undefined) {
      if (graveName !== undefined && moved === undefined) { try { fs.rmdirSync(at(parent, graveName)); } catch { /* holds a reported entry */ } }
      fs.closeSync(parent);
    }
  }
};
