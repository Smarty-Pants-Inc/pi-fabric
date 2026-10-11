import fs from "node:fs";
import path from "node:path";

/**
 * Private (0700) directories under any umask, reached only through trusted directories (smarty-dev#9016,
 * pi-fabric#875 rounds 1-2).
 *
 * `mkdirSync`'s `mode` is filtered by the umask (0177 gives 0600, with no search bit), so each directory this module
 * creates is set to exactly 0700 through its own descriptor (fchmod), never by a path chmod.
 *
 * On Linux the walk starts at `/` and holds a descriptor on each directory it passes. Each next component is
 * created and opened RELATIVE to that descriptor, through `/proc/self/fd/<parent fd>/<name>`.
 * ponytail: Node has no mkdirat or openat. A `/proc/self/fd/<fd>` path pins the parent by its descriptor (the same pin
 * as mesh/fence-lock.ts and verified-remove.ts), and O_NOFOLLOW guards the last component, so a symlink or a rename
 * anywhere above cannot redirect the walk.
 *
 * Every directory the walk passes or meets must be safe:
 * - a real directory (a symlink is accepted only when root owns the link, which is system layout);
 * - owned by us or by root;
 * - not group- or other-writable, unless root owns it and it is sticky (like /tmp).
 * Anything else throws: the run fails loudly rather than writing into an unsafe tree.
 * A directory that appears between our mkdir and our open (a competing creator) is validated the same way, never
 * skipped. Directories that already exist are never changed (collecting old 0775 runs is smarty-dev#9361).
 *
 * Off Linux there is no /proc/self/fd, so path-based mkdir and validation are used, with the same checks.
 */
const NOFOLLOW_DIRECTORY = fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0);
const FOLLOW_DIRECTORY = fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0);
const at = (fd: number, name: string): string => `/proc/self/fd/${fd}/${name}`;
const code = (error: unknown): string | undefined => (error as NodeJS.ErrnoException)?.code;

/** Test seam: runs right before a missing component is created (a competing creator wins the race here). */
export const privateDirTestHooks: { beforeCreate?: (directory: string) => void } = {};

class UnsafeDirectory extends Error {}
/** A directory we may pass or use: ours or root's; not group/other-writable unless root-owned and sticky. */
const safeDirectory = (stat: fs.Stats, where: string): void => {
  const uid = process.getuid?.();
  if (!stat.isDirectory()) throw new UnsafeDirectory(`${where} is not a directory`);
  if (uid === undefined) return;
  if (stat.uid !== uid && stat.uid !== 0) throw new UnsafeDirectory(`${where} is owned by uid ${stat.uid}, not by us or root`);
  if ((stat.mode & 0o022) !== 0 && !(stat.uid === 0 && (stat.mode & 0o1000) !== 0)) {
    throw new UnsafeDirectory(`${where} is writable by group or others (mode ${(stat.mode & 0o7777).toString(8)})`);
  }
};

/** fchmod 0700 through an O_NOFOLLOW directory fd, only when fstat proves our own directory. A link, a file, a
 * foreign or vanished entry: false, nothing changed. Windows has no POSIX modes: true, nothing to do. */
export const closeDirectory = (directory: string): boolean => {
  if (process.platform === "win32") return true;
  let fd: number;
  try { fd = fs.openSync(directory, NOFOLLOW_DIRECTORY); } catch { return false; }
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid())) return false;
    fs.fchmodSync(fd, 0o700);
    return true;
  } finally { fs.closeSync(fd); }
};

/** A private directory held open: `path` reaches exactly the validated directory while `close` has not run. On Linux it
 * is `/proc/self/fd/<fd>`, so a later rename or symlink of any ancestor cannot redirect writes made through it. */
export interface PrivateDirectory { path: string; close(): void }

const openLinux = (target: string): PrivateDirectory => {
  let fd = fs.openSync("/", NOFOLLOW_DIRECTORY);
  try {
    safeDirectory(fs.fstatSync(fd), "/");
    let walked = "/";
    for (const name of target.split(path.sep).filter(Boolean)) {
      walked = path.join(walked, name);
      let child: number | undefined;
      let created = false;
      try { child = fs.openSync(at(fd, name), NOFOLLOW_DIRECTORY); }
      catch (error) {
        if (code(error) === "ELOOP" || code(error) === "ENOTDIR") {
          // A symlink: accepted only as system layout (the link is root's); its target is validated below.
          const link = fs.lstatSync(at(fd, name));
          if (!link.isSymbolicLink() || link.uid !== 0) throw new UnsafeDirectory(`${walked} is a symbolic link or not a directory`);
          child = fs.openSync(at(fd, name), FOLLOW_DIRECTORY);
        } else if (code(error) === "ENOENT") {
          privateDirTestHooks.beforeCreate?.(walked);
          try { fs.mkdirSync(at(fd, name), { mode: 0o700 }); created = true; }
          catch (mkdirError) { if (code(mkdirError) !== "EEXIST") throw mkdirError; } // a competing creator: validated below
          try { child = fs.openSync(at(fd, name), NOFOLLOW_DIRECTORY); }
          catch (openError) { throw new UnsafeDirectory(`${walked} changed while it was created (${code(openError)})`); }
        } else throw error;
      }
      try {
        const stat = fs.fstatSync(child);
        if (created && stat.isDirectory() && stat.uid === process.getuid?.()) fs.fchmodSync(child, 0o700);
        else safeDirectory(stat, walked);
      } catch (error) { fs.closeSync(child); throw error; }
      fs.closeSync(fd);
      fd = child;
    }
  } catch (error) { fs.closeSync(fd); throw error; }
  const held = fd;
  return { path: `/proc/self/fd/${held}`, close: () => fs.closeSync(held) };
};

const openPortable = (target: string): PrivateDirectory => {
  const missing: string[] = [];
  let current = target;
  for (; !fs.lstatSync(current, { throwIfNoEntry: false }); current = path.dirname(current)) {
    missing.unshift(current);
    if (path.dirname(current) === current) break;
  }
  if (process.platform !== "win32") {
    for (let ancestor = current; ; ancestor = path.dirname(ancestor)) {
      if (!missing.includes(ancestor)) safeDirectory(fs.lstatSync(ancestor), ancestor);
      if (path.dirname(ancestor) === ancestor) break;
    }
  }
  for (const component of missing) {
    privateDirTestHooks.beforeCreate?.(component);
    try { fs.mkdirSync(component, { mode: 0o700 }); }
    catch (error) {
      if (code(error) !== "EEXIST") throw error;
      if (process.platform !== "win32") safeDirectory(fs.lstatSync(component), component); // a competing creator: validated
      continue;
    }
    if (!closeDirectory(component)) throw new UnsafeDirectory(`Cannot make ${component} private: it is not our directory`);
  }
  return { path: target, close: () => undefined };
};

/** Create or validate `directory` (see the module comment) and hold it open. Throws on any unsafe directory. */
export const openPrivateDir = (directory: string): PrivateDirectory => {
  const target = path.resolve(directory);
  return process.platform === "linux" && fs.existsSync("/proc/self/fd") ? openLinux(target) : openPortable(target);
};

/** `mkdir -p` whose created directories are exactly 0700, through trusted directories only. Throws on any unsafe one. */
export const mkdirPrivate = (directory: string): void => { openPrivateDir(directory).close(); };
