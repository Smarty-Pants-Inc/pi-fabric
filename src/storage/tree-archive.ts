import fs from "node:fs";
import path from "node:path";

/**
 * One whole-tree archive move by a single rename(2) (smarty-dev#8159), shared by offline actor removal
 * (`remove-offline.ts`) and, since smarty-dev#16444 (R-keep-all-traces), by the live actor removal and the
 * non-persistent root-tree reset (`session-archive.ts` `archiveTree`). Moved here from remove-offline.ts; one addition
 * (smarty-dev#16444, pi-fabric#1568 round 3): the source is checked against its pin (and the judged route against the
 * held parent) right BEFORE the rename too, so a swapped tree is refused with nothing moved, not moved and moved back.
 */
export const ARCHIVE_CHANGED = "archive path changed during removal; refusing (smarty-dev#8159)";

/** The identity (st_dev, inode) of each archive path component, pinned at the first check. */
export interface ChainLink { path: string; dev: number; ino: number; follow: boolean }
export type ArchiveChain = ChainLink[];
export const linkOf = (file: string, stat: fs.Stats, follow = false): ChainLink => ({ path: file, dev: stat.dev, ino: stat.ino, follow });

/** Re-verify the whole pinned chain: same identity, still a real directory (no link). Synchronous. */
export const reverifyChain = (chain: ArchiveChain): void => {
  for (const link of chain) {
    let stat: fs.Stats;
    try { stat = link.follow ? fs.statSync(link.path) : fs.lstatSync(link.path); }
    catch { throw new Error(`${ARCHIVE_CHANGED}: ${link.path}`); }
    if (stat.dev !== link.dev || stat.ino !== link.ino || !stat.isDirectory() || (!link.follow && stat.isSymbolicLink())) {
      throw new Error(`${ARCHIVE_CHANGED}: ${link.path}`);
    }
  }
};

/** The actor directory itself, pinned by identity. `uid`: the owner it was judged with (default: this OS user). */
export interface PinnedActorRoot { dev: number; ino: number; uid?: number }

/** A pre-rename refusal: the entry is not the pinned directory, so nothing moved. */
export const TREE_REPLACED = "Actor directory was replaced before the archive rename (the entry is not the pinned directory)";
/** A pre-rename refusal: the judged route no longer reaches the held parent, so nothing moved. */
export const ROUTE_CHANGED = "the route to the actor directory changed since judgment (its parent is not the held directory)";

/** smarty-dev#16444: the entry at `entry` (never followed) is the pinned directory: a real directory with the pinned
 * st_dev/st_ino and owner. Throws before anything moves otherwise. */
export const assertPinnedDirectory = (entry: string, pin: PinnedActorRoot): void => {
  let stat: fs.Stats | undefined;
  try { stat = fs.lstatSync(entry); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  const uid = pin.uid ?? process.getuid?.();
  if (!stat || stat.isSymbolicLink() || !stat.isDirectory() || stat.dev !== pin.dev || stat.ino !== pin.ino ||
      (uid !== undefined && stat.uid !== uid)) {
    throw new Error(`${TREE_REPLACED}; nothing moved, refusing: ${entry}`);
  }
};

/** smarty-dev#16444: on Linux a live removal renames through its held parent (`source` is
 * `/proc/self/fd/<parent>/./<name>`), so a swapped ancestor cannot redirect the rename. It is also refused, as the
 * old verifiedRemove did: the judged lexical `route`'s parent, opened without following a link, must still be that
 * held parent (same st_dev/st_ino), and the route must still name the pinned directory. Nothing moves otherwise. */
export const assertRoute = (route: string, source: string, pin: PinnedActorRoot): void => {
  const held = fs.statSync(path.dirname(source));
  const flags = fs.constants.O_RDONLY | fs.constants.O_DIRECTORY | (fs.constants.O_NOFOLLOW ?? 0);
  let fd: number;
  try { fd = fs.openSync(path.dirname(route), flags); }
  catch (error) { throw new Error(`${ROUTE_CHANGED} (${(error as NodeJS.ErrnoException).code}); nothing moved, refusing: ${path.dirname(route)}`); }
  try {
    const now = fs.fstatSync(fd);
    if (!now.isDirectory() || now.dev !== held.dev || now.ino !== held.ino) {
      throw new Error(`${ROUTE_CHANGED}; nothing moved, refusing: ${path.dirname(route)}`);
    }
  } finally { fs.closeSync(fd); }
  assertPinnedDirectory(route, pin);
};

/** Move the pinned actor directory into the archive by one rename(2) to a child name of the archive
 * directory. Right before the rename the entry must be the pinned directory (and, with `route`, the judged route must
 * still reach the held parent, `assertRoute`): a swapped tree, link or ancestor is refused with nothing moved
 * (smarty-dev#16444). rename never follows the source: an entry swapped in after that check moves as itself, and
 * the post-check refuses (moving it back). EXDEV refuses; there is no copy fallback. */
// ponytail: the whole chain and the source are re-verified synchronously right before each rename; the residual is
// the few syscalls between that check and rename(2), open only to this same OS user and caught by the post-check
// (smarty-dev#7800 carries a directory-fd based rename that closes it).
export const moveActorTreeToArchive = (actorDirectory: string, pin: PinnedActorRoot, archive: string, chain: ArchiveChain,
  name = "tree", route?: string): string => {
  const target = path.join(archive, name);
  reverifyChain(chain);
  try { fs.lstatSync(target); throw new Error(`Archive target already exists: ${target}`); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  if (route !== undefined && route !== actorDirectory) assertRoute(route, actorDirectory, pin);
  assertPinnedDirectory(actorDirectory, pin);
  try { fs.renameSync(actorDirectory, target); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EXDEV") {
      throw new Error(`Actor directory and archive root are on different filesystems (EXDEV); refusing, no copy: ${actorDirectory}`);
    }
    throw error;
  }
  const moved = fs.lstatSync(target);
  const uid = pin.uid ?? process.getuid?.();
  if (moved.isSymbolicLink() || !moved.isDirectory() || moved.dev !== pin.dev || moved.ino !== pin.ino || (uid !== undefined && moved.uid !== uid)) {
    let restored = false;
    try { fs.lstatSync(actorDirectory); }
    catch {
      try { reverifyChain(chain); fs.renameSync(target, actorDirectory); restored = true; } catch { /* reported below */ }
    }
    throw new Error(`Actor directory was replaced before the archive rename (moved entry is not the pinned directory); ` +
      `${restored ? "moved back" : `left at ${target}`}; refusing`);
  }
  return target;
};
