import fs from "node:fs";
import path from "node:path";

/**
 * Private (0700) directories under any umask (smarty-dev#9016, pi-fabric#875 round 1 P1-1). `mkdirSync`'s `mode` is
 * filtered by the umask: under 0177 it gives 0600, with no search bit, so nothing could be created or written below.
 * Each missing component is created alone and set to exactly 0700 through its own descriptor (open
 * O_DIRECTORY|O_NOFOLLOW, fstat: a directory owned by us, then fchmod), never by a path chmod, before the next
 * component is created inside it. Directories that already exist are left as they are (collecting old 0775 runs is
 * smarty-dev#9361).
 */
const DIRECTORY_FLAGS = fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0);

/** fchmod 0700 through an O_NOFOLLOW directory fd, only when fstat proves our own directory. A link, a file, a
 * foreign or vanished entry: false, nothing changed. Windows has no POSIX modes: true, nothing to do. */
export const closeDirectory = (directory: string): boolean => {
  if (process.platform === "win32") return true;
  let fd: number;
  try { fd = fs.openSync(directory, DIRECTORY_FLAGS); } catch { return false; }
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid())) return false;
    fs.fchmodSync(fd, 0o700);
    return true;
  } finally { fs.closeSync(fd); }
};

/** `mkdir -p` whose created directories are exactly 0700. Throws when a directory it just created cannot be proven
 * ours (swapped between the mkdir and the open). */
export const mkdirPrivate = (directory: string): void => {
  const missing: string[] = [];
  for (let current = path.resolve(directory); !fs.lstatSync(current, { throwIfNoEntry: false }); current = path.dirname(current)) {
    missing.unshift(current);
    if (path.dirname(current) === current) break;
  }
  for (const component of missing) {
    try { fs.mkdirSync(component, { mode: 0o700 }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") continue; // made concurrently by another writer: not ours to change
      throw error;
    }
    if (!closeDirectory(component)) throw new Error(`Cannot make ${component} private: it is not our directory`);
  }
};
