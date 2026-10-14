import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";

// Native Node type-stripping callers need a real .ts path; warnings must remain synchronous.
// ponytail: import the same sink directly, not a second fallback or an async warning queue.
// @ts-expect-error TS5097: tsc emits declarations only; esbuild resolves this source into the shared sink chunk.
import { fabricWarn } from "../core/agent-dir.ts";

/**
 * The one removal path of every retention and scratch deleter (smarty-dev#7916). A deleter judges a target by
 * path (its stat, age and contents) and then hands that judged stat here. This removes only that inode:
 *
 * 1. Open the parent O_DIRECTORY|O_NOFOLLOW and the target O_PATH|O_NOFOLLOW (|O_DIRECTORY for a directory) through
 *    the parent's fd, fstat it: uid must be geteuid, dev/ino and type must equal the judged stat, and its mtime must
 *    be unchanged (the age re-check: a refreshed target is not the judged one).
 * 2. Walk the whole tree through fds; every entry must be ours. The walk keeps a snapshot of every entry (dev/ino/uid/
 *    type/mtime and each directory's exact name set). Any doubt: nothing is changed. A dry run stops here, so it runs
 *    the same judgment and verification as an apply, minus the rename and the delete.
 * 3. Rename it into a same-directory graveyard (`.retention-graveyard-<pid>`) and revalidate the whole snapshot there
 *    (mtime too: a rename keeps it), deleting nothing. A mismatch leaves it in the graveyard, reported.
 * 4. `proceed()` (a caller's last check, for example a registry re-read) may keep it: it is moved back. Otherwise the
 *    whole snapshot is revalidated again, deleting nothing.
 * 5. Remove it directory-relative, entry by entry, each compared with its snapshot again before its unlink or rmdir.
 *    A name not in the snapshot never gets deletion authority. Before an rmdir only identity/owner/type are compared
 *    (our own child removals change its mtime).
 *
 * Directory-relative operations go through `/proc/self/fd/<fd>/<name>` (Node has no unlinkat or renameat; the same
 * pin as mesh/fence-lock.ts). The verified path above is Linux only. Off Linux main's branch is unchanged: with the
 * operator's Windows helper switched on it removes (below); otherwise a deleter that main already ran in-process passes
 * its unchanged main-branch removal as `fallback` (never used on Linux), so nothing regresses there; without a
 * fallback nothing is deleted. That off-Linux path-based removal is the accepted residual of smarty-dev#7858 and
 * smarty-dev#10678 (none of the checks above apply to it). On Linux, missing getuid/geteuid deletes nothing.
 * Platform-cause refusals, a failed fallback and every writable-entry refusal (initial or after the move) are counted
 * and logged (`reportRemovalSkip`).
 * Every entry in the tree (symbolic links excepted, never followed: see `entryClosed`) and the target itself must be
 * ours and closed (no group/other write), checked at the walk, at each recheck and again at each removal step. For
 * directories: no other uid can then change any entry, by path (the 0700 graveyard blocks it) or through a
 * descriptor opened earlier (write permission follows the current mode, which only the owner or root can change).
 * Residual: unlink and rmdir take a name, so only our own uid (which owns every entry already) or root can swap an
 * entry between a check and its unlink. No other user's file and nothing outside the graveyard can be reached.
 */

export type VerifiedRemoveResult = { removed: true; bytes: number } | { removed: false; reason: string; graveyard?: string };
/** The judged state of one tree entry. `children` (directories only) is the exact name set seen at judgment. */
type Snap = { dev: number; ino: number; uid: number; kind: string; mtimeMs: number; children?: Map<string, Snap> | undefined };

/** Per-deleter counts for the retention CLI's report: removed (or, in a dry run, would be), bytes, refusals by reason.
 * ponytail: a module-level tally the one-shot CLI turns on; in-process sweeps leave it off and pay nothing. */
export interface DeleterTally {
  removed: number; bytes: number; refused: Record<string, number>;
  /** Bytes held by refused targets, by reason, where the deleter measured them. */
  refusedBytes: Record<string, number>;
  examples: Record<string, string[]>;
  /** Graveyard paths of refused targets left moved there (callers may discard `result.graveyard`), resolved from the
   * held container at refusal time. Where that location cannot be proven, an `unresolved: ...` entry names the held
   * container's dev/ino and the entry name instead of a path. */
  graveyards?: string[];
}
let tally: Record<string, DeleterTally> | undefined;
export const startRemovalTally = (): Record<string, DeleterTally> => (tally = {});
export const stopRemovalTally = (): void => { tally = undefined; };

/** smarty-dev#7858: Windows verified removal (the native helper) is on only where it is switched on: the operator's
 * `retention --apply` sweep. In-process sweeps keep main's path-based fallback.
 * ponytail: one helper process per removal costs tens of ms on Windows; resident sweeps of thousands of runs, and
 * their per-run metadata budget (actor-manager-startup #4250), cannot carry that (pi-fabric#913 Windows CI). */
let windowsVerified = false;
export const setWindowsVerifiedRemoval = (on: boolean): void => { windowsVerified = on; };
/** Whether a report is being collected: deleters compute detailed refusal reasons and sizes only then. */
export const removalTallyOn = (): boolean => tally !== undefined;
const entry = (deleter: string): DeleterTally => (tally![deleter] ??= { removed: 0, bytes: 0, refused: {}, refusedBytes: {}, examples: {} });
export const countRemoval = (deleter: string, bytes: number): void => {
  if (!tally) return;
  const item = entry(deleter); item.removed++; item.bytes += bytes;
};
/** A refusal (by the deleter's own judgment or by this helper). `reason` is a fixed category; up to 3 example paths. */
export const countRefusal = (deleter: string, reason: string, target?: string, bytes = 0): void => {
  if (!tally) return;
  const item = entry(deleter);
  item.refused[reason] = (item.refused[reason] ?? 0) + 1;
  if (bytes) item.refusedBytes[reason] = (item.refusedBytes[reason] ?? 0) + bytes;
  if (target !== undefined) { const examples = item.examples[reason] ??= []; if (examples.length < 3) examples.push(target); }
};

/** pi-fabric#1014 SEC P2-2: a removal skipped for a platform cause (no ownership proof, no /proc/self/fd), a failed
 * scratch path removal, or (round 1 P1) a target refused at any step because an entry is writable by group
 * or others (for example a 0664 file created under umask 002, which this helper now refuses and never chmod-heals) is
 * not silent where no report tally is on (in-process sweeps). `platformRemovalSkips` keeps its name for compatibility
 * and now includes those writable-entry reasons. Cumulative counts by
 * `deleter: reason` (fixed categories, so bounded), and a synchronous console warning (no timer) the first time each
 * key is skipped and again only at counts 10, 100, 1000, ..., each line with the count and the current example target.
 * Under a tally the report carries it instead (the caller counts it with countRefusal). */
const skipCounts: Record<string, number> = {};
export const platformRemovalSkips = (): Record<string, number> => ({ ...skipCounts });
export const reportRemovalSkip = (deleter: string, reason: string, target: string): void => {
  if (tally) return;
  const key = `${deleter}: ${reason}`;
  const count = skipCounts[key] = (skipCounts[key] ?? 0) + 1;
  // ponytail: log only at exact powers of ten (1, 10, 100, ...), so a key warns O(log n) times.
  if (count === 10 ** Math.floor(Math.log10(count))) fabricWarn(`[pi-fabric] cleanup skipped (smarty-dev#7858): ${key} x${count} (e.g. ${target})`);
};

/** Test seam: runs after the judgment, right before the first open (a swap between judgment and delete). */
export const verifiedRemoveTestHooks: { beforeOpen?: (target: string) => void;
  /** Windows: runs after the tree walk, right before the helper opens the entries (the ancestor-swap test). */
  beforeHelper?: (target: string) => void } = {};

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
/** No other uid can change a directory's entries: ours (checked by callers) and not group/other-writable. Write
 * permission is checked against the CURRENT mode at each create/unlink/rename, also through a descriptor opened
 * earlier, and only the owner or root can change the mode. A POSIX ACL that grants another user write shows as the
 * group write bit (the ACL mask), so it is refused too. (pi-fabric#850 round 1 P1.) */
const closed = (stat: fs.Stats): boolean => (stat.mode & 0o022) === 0;
/** pi-fabric#1014 round 1 P1: EVERY entry kind must be closed, not only directories. Another uid may open a group/other-
 * writable regular file (or FIFO, socket, device node) for write, and that descriptor writes after the final mtime check,
 * so deleting it would delete new data. Refused at judgment, at each no-delete recheck and right before each unlink:
 * this refuses an entry writable now or made writable since judgment. It does NOT prove no descriptor was opened while
 * an earlier mode allowed it (chmod does not revoke open descriptors); that last-check window is #10678's design cut,
 * not an atomic claim. A symbolic link IN the tree is exempt (owner decision); a symbolic link as the target is still
 * refused (`verify`). Never chmod-healed. */
const entryClosed = (stat: fs.Stats): void => {
  // ponytail: a symlink's 0777 mode means nothing on Linux, and the dirfd unlink removes only the link (never followed), so no other-uid writer can reach target data through it.
  if (stat.isSymbolicLink() || closed(stat)) return;
  refuse(stat.isDirectory() ? "a directory in the tree is writable by group or others"
    : stat.isFile() ? "a regular file in the tree is writable by group or others"
    : "an entry in the tree is writable by group or others");
};
/** Where a held graveyard entry is now (old audit finding 3): the opened container's current path, read from its
 * /proc/self/fd link and proven to be that container by dev/ino (before and after the entry check), joined with the
 * entry name, proven to be the same object as the name below the fd. The lexical path built before proceed() can be
 * stale: the container can be renamed while pinned. Undefined when that cannot be proven (also a deleted container,
 * whose link ends in " (deleted)"). Reporting only: nothing is moved or deleted here. */
const located = (grave: number, entryName: string): string | undefined => {
  try {
    const dir = fs.readlinkSync(at(grave));
    if (!path.isAbsolute(dir)) return undefined;
    const held = fs.fstatSync(grave);
    const container = (): boolean => { const now = fs.lstatSync(dir); return now.isDirectory() && same(now, held); };
    if (!container()) return undefined;
    const file = path.join(dir, entryName);
    if (!same(fs.lstatSync(file), fs.lstatSync(at(grave, entryName)))) return undefined;
    return container() ? file : undefined;
  } catch { return undefined; }
};
const kind = (stat: fs.Stats): string => stat.isDirectory() ? "directory" : stat.isFile() ? "file" : stat.isSymbolicLink() ? "symlink" : "other";
/** The judged inode, ours, same type, same mtime (also after the graveyard move: a rename keeps the mtime). */
const verify = (file: string, judged: fs.Stats, uid: number): void => {
  // The judged stat too: a root judged writable and chmod-closed before the open may still be held open for write by
  // another uid (chmod does not revoke a descriptor), so the current mode alone is not enough.
  if (judged.isSymbolicLink()) refuse("target is a symbolic link");
  entryClosed(judged);
  const stat = fdStat(file, judged.isDirectory());
  if (stat.isSymbolicLink()) refuse("target is a symbolic link");
  if (stat.uid !== uid) refuse("target is not owned by this user");
  if (!same(stat, judged)) refuse("target inode was replaced since judgment");
  if (kind(stat) !== kind(judged)) refuse("type changed since judgment");
  if (stat.mtimeMs !== judged.mtimeMs) refuse("target was modified since judgment");
  entryClosed(stat);
};
const open = (file: string): number => {
  try { return fs.openSync(file, O_PATH | NOFOLLOW | DIRECTORY); }
  catch (error) { return refuse(`open failed (${code(error)})`); }
};
const snapOf = (stat: fs.Stats, children?: Map<string, Snap>): Snap =>
  ({ dev: stat.dev, ino: stat.ino, uid: stat.uid, kind: kind(stat), mtimeMs: stat.mtimeMs, children });
/** Closed mode first (throws a Refusal with the writable category, also when dev/ino/mtime match), then identity,
 * owner and type; with `mtime` also the judged mtime. Every last check before an unlink or rmdir goes through here. */
const matches = (stat: fs.Stats, snap: Snap, mtime = true): boolean => {
  entryClosed(stat);
  return stat.dev === snap.dev && stat.ino === snap.ino &&
    stat.uid === snap.uid && kind(stat) === snap.kind && (!mtime || stat.mtimeMs === snap.mtimeMs);
};
const sameNames = (names: string[], children: Map<string, Snap>): void => {
  if (names.length !== children.size || names.some(name => !children.has(name))) refuse("the tree's entries changed since judgment");
};
/** Enter directory `name` below `fd` through an fd whose fstat is the snapshotted directory (owner, type, identity,
 * mtime) and closed (no group/other write). */
const enter = (fd: number, name: string, snap: Snap, body: (child: number) => void): void => {
  const child = open(at(fd, name));
  try {
    const opened = fs.fstatSync(child);
    if (!matches(opened, snap)) refuse("an entry in the tree changed since judgment");
    if (!closed(opened)) refuse("a directory in the tree is writable by group or others");
    body(child);
  } finally { fs.closeSync(child); }
};
/** Every entry below `fd` must be ours; each is recorded in `children`; directories are entered only through an fd
 * whose fstat matches the lstat. Returns the regular-file bytes. */
const verifyTree = (fd: number, uid: number, depth: number, children: Map<string, Snap>): number => {
  if (depth > MAX_DEPTH) refuse("tree is too deep");
  let bytes = 0;
  for (const name of fs.readdirSync(at(fd))) {
    const stat = fs.lstatSync(at(fd, name));
    if (stat.uid !== uid) refuse("an entry in the tree is not owned by this user");
    entryClosed(stat);
    if (!stat.isDirectory()) { children.set(name, snapOf(stat)); bytes += stat.isFile() ? stat.size : 0; continue; }
    const snap = snapOf(stat, new Map());
    children.set(name, snap);
    enter(fd, name, snap, child => { bytes += verifyTree(child, uid, depth + 1, snap.children!); });
  }
  return bytes;
};
/** The full no-delete revalidation: the exact name sets and every entry's dev/ino/uid/type/mtime. */
const recheckTree = (fd: number, children: Map<string, Snap>, depth: number): void => {
  if (depth > MAX_DEPTH) refuse("tree is too deep");
  const names = fs.readdirSync(at(fd));
  sameNames(names, children);
  for (const name of names) {
    const snap = children.get(name)!;
    const stat = fs.lstatSync(at(fd, name));
    if (!matches(stat, snap)) refuse("an entry in the tree changed since judgment");
    if (snap.children) enter(fd, name, snap, child => recheckTree(child, snap.children!, depth + 1));
  }
};
/** Remove `name` below `parent`, each entry verified again; unlink and rmdir never follow a link.
 * Binding (pi-fabric#850 round 1 P1): `parent` is an fd on a directory proven ours and closed (no group/other write)
 * when it was opened, and checked again here, so between this lstat and the unlink no other uid can create, replace
 * or rename an entry in it, even through a descriptor opened before the move (write permission is checked against
 * the current mode). The whole tree also sits below our 0700 graveyard, so no other uid can reach it by path. The
 * name-based unlink is therefore exposed only to our own uid (which already owns every entry) and root: the check
 * before each unlink/rmdir is not atomic with it. Only snapshotted names are removed. */
const removeTree = (parent: number, name: string, snap: Snap, uid: number, depth: number): void => {
  if (depth > MAX_DEPTH) refuse("tree is too deep");
  const held = fs.fstatSync(parent);
  if (held.uid !== uid || !closed(held)) refuse("a directory in the tree is writable by group or others");
  const stat = fs.lstatSync(at(parent, name));
  if (!matches(stat, snap)) refuse("an entry in the tree changed since judgment");
  if (!snap.children) { fs.unlinkSync(at(parent, name)); return; }
  const children = snap.children;
  enter(parent, name, snap, fd => {
    const names = fs.readdirSync(at(fd));
    sameNames(names, children);
    for (const child of names) removeTree(fd, child, children.get(child)!, uid, depth + 1);
  });
  // Our own child removals changed its mtime: identity, owner, type and closed mode only.
  const emptied = fs.lstatSync(at(parent, name));
  if (!matches(emptied, snap, false)) refuse("an entry in the tree changed since judgment");
  fs.rmdirSync(at(parent, name));
};

/** Regular-file bytes below a path, by lstat (no link followed); for the off-Linux report only. */
const pathBytes = (file: string, depth = 0): number => {
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isDirectory()) return stat.isFile() ? stat.size : 0;
    if (depth >= MAX_DEPTH) return 0;
    return fs.readdirSync(file).reduce((sum, name) => sum + pathBytes(path.join(file, name), depth + 1), 0);
  } catch { return 0; }
};

// smarty-dev#7858: Windows verified removal through the native helper (native/fabric-win-remove.c). It opens every
// entry without following a reparse point, verifies ALL on their handles (owner SID = this token's user or default
// owner, the judged volume serial and file index, the kind) and only then deletes each BY HANDLE, children first.
// Replaced at build time (scripts/build.mjs define); tests set the global from dist/native/manifest.json.
declare const __FABRIC_NATIVE_DIGESTS__: string | undefined;
const WIN_HELPER = "fabric-win-remove.exe";
const MAX_WIN_ENTRIES = 4096;
/** The helper, when present and its bytes equal the digest embedded at build time; else undefined (fail closed).
 * ponytail: hash, then exec by path (Windows has no fd exec): a same-user rewrite in between is the accepted
 * smarty-dev#7800 class, as for the lease helper. Located beside the bundle (dist/<dir>/ or dist/) or from src/. */
export const windowsRemoveHelper = (): string | undefined => {
  // os.type() names the real OS: tests that force process.platform to win32 on Linux keep main's Windows paths.
  if (process.platform !== "win32" || os.type() !== "Windows_NT") return undefined;
  // Hashed again before each spawn (pi-fabric#913 round 4 P2): only the operator's sweep spawns it, so the cost is
  // per CLI removal, not per resident run.
  return findWindowsRemoveHelper();
};
const findWindowsRemoveHelper = (): string | undefined => {
  let expected: string | undefined;
  try { expected = (JSON.parse(__FABRIC_NATIVE_DIGESTS__ as string) as Record<string, string>)[WIN_HELPER]; } catch { return undefined; }
  if (typeof expected !== "string" || !/^[0-9a-f]{64}$/.test(expected)) return undefined;
  for (const relative of ["../../dist/native/", "../dist/native/"]) {
    const file = fileURLToPath(new URL(`${relative}${WIN_HELPER}`, import.meta.url));
    try {
      if (!fs.lstatSync(file).isFile()) continue;
      if (createHash("sha256").update(fs.readFileSync(file)).digest("hex") === expected) return file;
    } catch { /* next candidate */ }
  }
  return undefined;
};
/** The tree below a judged target, children first, by lstat with exact (bigint) ids; no reparse point is entered. */
const windowsEntries = (target: string, judged: fs.Stats): Array<{ file: string; stat: fs.BigIntStats }> => {
  const entries: Array<{ file: string; stat: fs.BigIntStats }> = [];
  const walk = (file: string, depth: number): void => {
    if (depth > MAX_DEPTH) refuse("tree is too deep");
    const stat = fs.lstatSync(file, { bigint: true });
    if (stat.isSymbolicLink()) refuse("a reparse point in the tree");
    if (!stat.isFile() && !stat.isDirectory()) refuse("an entry in the tree is not a regular file or directory");
    if (stat.isDirectory()) for (const name of fs.readdirSync(file)) walk(path.join(file, name), depth + 1);
    if (entries.length >= MAX_WIN_ENTRIES) refuse("tree is too large");
    entries.push({ file, stat });
  };
  walk(target, 0);
  const top = entries.at(-1)!.stat;
  // The judged stat's ino is a double (a 64-bit NTFS index may round): the exact id goes to the helper from here.
  if (Number(top.dev) !== judged.dev || Number(top.ino) !== judged.ino) refuse("target inode was replaced since judgment");
  if (kind(judged) !== (top.isDirectory() ? "directory" : "file")) refuse("type changed since judgment");
  // A bigint stat's mtimeMs is whole milliseconds; NTFS keeps 100 ns: compare the judged double with a plain lstat.
  if (fs.lstatSync(target).mtimeMs !== judged.mtimeMs) refuse("target was modified since judgment");
  return entries;
};
/** The judged last-write time as a Windows FILETIME (100 ns since 1601): libuv derives mtimeNs from it exactly. */
const fileTime = (stat: fs.BigIntStats): string => (stat.mtimeNs / 100n + 116_444_736_000_000_000n).toString(16);
/** The helper kept an entry refreshed after its judgment (same id, new time or size): a final-check keep. */
class Kept extends Refusal {}
const windowsRemove = (helper: string, entries: Array<{ file: string; stat: fs.BigIntStats }>, dryRun: boolean): void => {
  // pi-fabric#913 round 4: the judged time and size go along, and the helper compares them on its handle.
  const args = entries.flatMap(({ file, stat }) => [stat.isDirectory() ? "d" : "f", stat.dev.toString(16), stat.ino.toString(16),
    fileTime(stat), stat.size.toString(16), path.toNamespacedPath(file)]);
  if (windowsRemoveHelper() !== helper) refuse("the Windows removal helper no longer matches its digest"); // re-hashed per spawn
  const result = spawnSync(helper, dryRun ? ["--dry-run", ...args] : args, { encoding: "utf8", timeout: 60_000, windowsHide: true });
  const line = (result.stdout ?? "").split(/\r?\n/)[0] ?? "";
  if (result.status === 0 && /^ok \d+/.test(line)) return;
  const match = /^(refused|failed|kept) (-?\d+) (.+)$/.exec(line);
  if (!match) refuse(`the Windows removal helper failed (${result.error?.message ?? `exit ${String(result.status)}`})`);
  const entry = entries[Number(match![2])]?.file ?? "the helper";
  if (match![1] === "kept") throw new Kept(`kept by the final check (${entry} ${match![3]})`);
  refuse(match![1] === "refused" ? `${match![3]} (${entry}); nothing is deleted` : `removal failed at ${entry} (${match![3]})`);
};

/** Windows: `file` (its exact id from a bigint stat) is a private record, proven on a handle by the verified helper:
 * ours, not a reparse point, and no SID but ours, SYSTEM or Administrators may write it (pi-fabric#913 P1). */
export const windowsPrivate = (file: string, stat: fs.BigIntStats): boolean => {
  const helper = windowsRemoveHelper();
  if (!helper || !stat.isFile()) return false;
  const result = spawnSync(helper, ["--private", stat.dev.toString(16), stat.ino.toString(16), path.toNamespacedPath(file)],
    { encoding: "utf8", timeout: 60_000, windowsHide: true });
  return result.status === 0 && /^ok 1 private/.test(result.stdout ?? "");
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
    // Every writable-entry refusal, before or after the move, is visible outside a tally (a no-op under one).
    if (reason.includes("writable by group or others")) reportRemovalSkip(deleter, reason, target);
    countRefusal(deleter, reason, target);
    if (graveyard !== undefined && tally) (entry(deleter).graveyards ??= []).push(graveyard);
    return graveyard === undefined ? { removed: false, reason } : { removed: false, reason, graveyard };
  };
  /** A platform-cause refusal: also counted and logged where no report tally is on (pi-fabric#1014). */
  const skip = (reason: string): VerifiedRemoveResult => { reportRemovalSkip(deleter, reason, target); return no(reason); };
  // smarty-dev#7858: on Windows with the verified helper, it takes the place of main's path-based fallback.
  const windowsHelper = windowsVerified ? windowsRemoveHelper() : undefined;
  if (windowsHelper) {
    if (!judged) return no("no judged stat");
    try {
      verifiedRemoveTestHooks.beforeOpen?.(target);
      const entries = windowsEntries(target, judged);
      verifiedRemoveTestHooks.beforeHelper?.(target);
      const bytes = options.freed ?? entries.reduce((sum, { stat }) => sum + (stat.isFile() ? Number(stat.size) : 0), 0);
      if (options.dryRun) { windowsRemove(windowsHelper, entries, true); countRemoval(deleter, bytes); return { removed: true, bytes }; }
      if (!options.proceed) { windowsRemove(windowsHelper, entries, false); countRemoval(deleter, bytes); return { removed: true, bytes }; }
      // The caller's final check runs with the target off its name, as on Linux (step 4): a compaction publishes its
      // new tail at the free name (pi-fabric#913 Windows CI). The move is by path; the helper then verifies every
      // moved entry by its judged id, so a swapped entry is refused and stays at the grave name, reported.
      const grave = path.join(path.dirname(target), `.retention-graveyard-${process.pid}-${randomUUID()}`);
      fs.renameSync(target, grave);
      const moved = entries.map(({ file, stat }) => ({ file: grave + file.slice(target.length), stat }));
      let kept: string | undefined;
      try { windowsRemove(windowsHelper, moved, true); }
      catch (error) {
        if (!(error instanceof Kept)) return no(`${error instanceof Refusal ? error.message : code(error)} (swapped during removal; left at ${grave})`, grave);
        kept = error.message; // refreshed since judgment: moved back below, like a final-check keep
      }
      let keep: boolean;
      try { keep = kept !== undefined || !options.proceed(); } catch { keep = true; }
      if (keep) {
        // ponytail: lstat, then rename back (Windows rename replaces): a same-user create in between is the
        // smarty-dev#7800 class; the Linux path has the same check-then-move.
        try { fs.lstatSync(target); return no("kept by the final check; name is taken, left in the graveyard", grave); }
        catch { /* the name is free */ }
        try { fs.renameSync(grave, target); } catch (error) { return no(`kept by the final check; move back failed (${code(error)}), left in the graveyard`, grave); }
        return no(kept ?? "kept by the final check");
      }
      try { windowsRemove(windowsHelper, moved, false); }
      catch (error) { return no(`${error instanceof Refusal ? error.message : code(error)}; the rest is left in the graveyard`, grave); }
      countRemoval(deleter, bytes);
      return { removed: true, bytes };
    } catch (error) {
      return no(error instanceof Refusal ? error.message : `removal failed (${code(error)})`);
    }
  }
  if (process.platform !== "linux" && options.fallback) {
    // Main's path-based removal (smarty-dev#7858), counted like the verified path (pi-fabric#850 round 1 P2). The size
    // walk runs only for a report: in-process sweeps keep main's filesystem cost (actor-manager-startup #4250 guard).
    const bytes = options.freed ?? (tally ? pathBytes(target) : 0);
    if (options.dryRun) { countRemoval(deleter, bytes); return { removed: true, bytes }; }
    let removed = false;
    try { removed = options.fallback(); } catch { /* main's best effort */ }
    // A failed fallback is not silent outside a tally either (logged; no metadata work, pi-fabric#1014).
    if (!removed) return skip("removal failed (main's path on this platform)");
    countRemoval(deleter, bytes);
    return { removed: true, bytes };
  }
  // No ownership proof without getuid/geteuid: nothing is deleted (Linux included).
  if (typeof process.getuid !== "function" || typeof process.geteuid !== "function") {
    return skip("file ownership cannot be proven on this platform (no getuid); nothing is deleted");
  }
  if (process.platform !== "linux") return skip("directory-relative removal needs Linux /proc/self/fd; nothing is deleted");
  if (!judged) return no("no judged stat");
  const name = path.basename(target);
  if (!name || name === "." || name === ".." || path.dirname(target) === target) return no("invalid target name");
  let parent: number | undefined, grave: number | undefined, graveName: string | undefined, moved: string | undefined;
  let graveStat: fs.Stats | undefined, parentStat: fs.Stats | undefined;
  /** The held parent is still ours, a directory and closed (no group/other write): rechecked on the fd right before
   * each step that changes its namespace after proceed() (rollback, container cleanup), so a chmod since the open
   * stops them. It does not exclude our own uid or root (not atomic with the name operation). */
  const parentHeld = (): boolean => {
    try { const now = fs.fstatSync(parent!); return now.isDirectory() && now.uid === parentStat!.uid && closed(now); }
    catch { return false; }
  };
  try {
    verifiedRemoveTestHooks.beforeOpen?.(target);
    const uid = process.geteuid();
    try { parent = fs.openSync(path.dirname(target), fs.constants.O_RDONLY | NOFOLLOW | DIRECTORY); }
    catch (error) { return no(`parent open failed (${code(error)})`); }
    parentStat = fs.fstatSync(parent);
    if (parentStat.uid !== uid) return no("parent directory is not owned by this user");
    // Old audit finding 1: a user-owned parent other uids can write would expose rollback and container cleanup (name
    // operations) to their creates/replaces. Refused before any move or delete; no mode is changed.
    if (!parentStat.isDirectory() || !closed(parentStat)) return no("parent directory is writable by group or others");
    // The pin must resolve to the opened directory, or no directory-relative step is possible.
    try { if (!same(fs.statSync(at(parent)), parentStat)) return skip("/proc/self/fd does not resolve the parent; nothing is deleted"); }
    catch { return skip("/proc/self/fd is unavailable; nothing is deleted"); }
    verify(at(parent, name), judged, uid);
    let bytes = judged.isFile() ? judged.size : 0;
    const snap = snapOf(judged, judged.isDirectory() ? new Map() : undefined);
    if (judged.isDirectory()) {
      const fd = open(at(parent, name));
      try {
        if (!same(fs.fstatSync(fd), judged)) refuse("target inode was replaced since judgment");
        bytes = verifyTree(fd, uid, 0, snap.children!);
      } finally { fs.closeSync(fd); }
    } else if (!judged.isFile()) refuse("target is not a regular file or directory");
    if (options.freed !== undefined) bytes = options.freed;
    if (options.dryRun) { countRemoval(deleter, bytes); return { removed: true, bytes }; }

    graveName = `.retention-graveyard-${process.pid}`;
    try { fs.mkdirSync(at(parent, graveName), { mode: 0o700 }); }
    catch (error) { if (code(error) !== "EEXIST") throw error; }
    grave = open(at(parent, graveName));
    graveStat = fs.fstatSync(grave);
    if (graveStat.uid !== uid || (graveStat.mode & 0o077) !== 0) { grave = (fs.closeSync(grave), undefined); graveStat = undefined; return no("graveyard is not our private directory"); }
    const entryName = `${name}.${randomUUID()}`;
    fs.renameSync(at(parent, name), at(grave, entryName));
    moved = entryName;
    /** A refusal that leaves the entry in the graveyard, reported where it actually is now (see `located`). */
    const left = (reason: string): VerifiedRemoveResult => {
      const file = located(grave!, entryName);
      if (file !== undefined) return no(reason, file);
      let held = "unknown";
      try { const stat = fs.fstatSync(grave!); held = `dev ${stat.dev} ino ${stat.ino}`; } catch { /* reported as unknown */ }
      const category = `${reason}; graveyard location unresolved`;
      no(category); // the shared refusal side effects: one tally count, or (no tally) the writable-mode skip log
      if (tally) (entry(deleter).graveyards ??= []).push(`unresolved: held graveyard container ${held}, entry ${entryName}`);
      return { removed: false, reason: `${category} (held container ${held}, entry ${entryName})` };
    };
    // The full no-delete revalidation of the snapshot, below the private graveyard.
    const recheck = (): void => {
      verify(at(grave!, moved!), judged, uid);
      if (snap.children) enter(grave!, moved!, snap, fd => recheckTree(fd, snap.children!, 1));
    };
    try { recheck(); }
    catch (error) {
      // Whatever we moved is not the judged inode: it stays in the graveyard, untouched and reported.
      const reason = error instanceof Refusal ? error.message : `verify failed (${code(error)})`;
      moved = undefined;
      return left(`${reason} (swapped during removal; left in the graveyard)`);
    }
    let keep: boolean;
    try { keep = options.proceed ? !options.proceed() : false; } catch { keep = true; }
    // A keep also needs a fresh snapshot: file rollback unlinks the graveyard hardlink, so it must not gain
    // authority to unlink a replacement or refreshed entry merely because the caller vetoed removal.
    try { recheck(); }
    catch (error) {
      const reason = error instanceof Refusal ? error.message : `verify failed (${code(error)})`;
      moved = undefined;
      return left(`${reason} (changed during removal; left in the graveyard)`);
    }
    if (keep) {
      // Back to its name, never over anything that appeared there: link fails on EEXIST; a directory moves back
      // only while the name is absent. Otherwise it stays in the graveyard and is reported.
      if (!parentHeld()) return left("kept by the final check; parent directory is writable by group or others, left in the graveyard");
      try {
        if (judged.isDirectory()) {
          try { fs.lstatSync(at(parent, name)); return left("kept by the final check; name is taken, left in the graveyard"); }
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
        return left(`kept by the final check; move back failed (${code(error)}), left in the graveyard`);
      }
    }
    try { removeTree(grave, moved, snap, uid, 0); }
    catch (error) {
      const reason = error instanceof Refusal ? error.message : `removal failed (${code(error)})`;
      moved = undefined;
      return left(`${reason}; the rest is left in the graveyard`);
    }
    moved = undefined;
    countRemoval(deleter, bytes);
    return { removed: true, bytes };
  } catch (error) {
    // Refusals reaching here precede the move (later ones are `left`); `no` reports writable-entry reasons.
    return no(error instanceof Refusal ? error.message : `removal failed (${code(error)})`);
  } finally {
    if (grave !== undefined) fs.closeSync(grave);
    // Cleanup changes the parent's namespace: only while the held parent is still ours and closed (else it stays).
    // An empty graveyard goes; one that holds a reported entry stays (rmdir fails closed on any entry). The name is
    // removed only while it is still the container we opened (dev/ino/uid/directory/private): a replacement stays.
    if (parent !== undefined) {
      if (graveName !== undefined && moved === undefined && graveStat !== undefined && parentHeld()) {
        try {
          const now = fs.lstatSync(at(parent, graveName));
          if (now.isDirectory() && same(now, graveStat) && now.uid === graveStat.uid && (now.mode & 0o077) === 0) fs.rmdirSync(at(parent, graveName));
        } catch { /* holds a reported entry, or gone */ }
      }
      fs.closeSync(parent);
    }
  }
};
