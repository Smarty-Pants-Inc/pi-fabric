import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/**
 * Private (0700) directories under any umask, reached only through trusted directories (smarty-dev#9016,
 * pi-fabric#875 rounds 1-4).
 *
 * `mkdirSync`'s `mode` is filtered by the umask (0177 gives 0600, with no search bit), so each directory this module
 * creates is set to exactly 0700 through its own descriptor (fchmod), never by a path chmod.
 *
 * The walk starts at `/` and holds a descriptor on each directory it passes. Each next component is created and
 * opened RELATIVE to that descriptor, through `<fd base>/<parent fd>/<name>`.
 * ponytail: Node has no mkdirat or openat. The fd base is `/proc/self/fd` (Linux) or `/dev/fd` (macOS), whichever is
 * proven at first use to resolve a held directory; it pins the parent by its descriptor (the same pin as
 * mesh/fence-lock.ts and verified-remove.ts), and O_NOFOLLOW guards the last component, so a symlink or a rename
 * anywhere above cannot redirect the walk. With neither base there is no path-based fallback: creation fails closed.
 * Windows has no POSIX owners or modes: main's plain recursive mkdir stays there.
 *
 * Every directory the walk passes or meets must be safe:
 * - a real directory (a symlink is accepted only when root owns the link, which is system layout);
 * - owned by us or by root;
 * - not group- or other-writable, unless root owns it and it is sticky (like /tmp).
 * Anything else throws: the run fails loudly rather than writing into an unsafe tree.
 * A directory that appears between our mkdir and our open (a competing creator) is validated the same way, never
 * skipped. Directories that already exist are never changed, except the self-heal below.
 */
const NOFOLLOW_DIRECTORY = fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0);
const FOLLOW_DIRECTORY = fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0);
const code = (error: unknown): string | undefined => (error as NodeJS.ErrnoException)?.code;

/** Test seams: `beforeCreate` runs right before a missing component is created (a competing creator wins the race
 * here); `stat` may replace an existing component's fstat and `linkStat` a symlink component's lstat (a foreign or a
 * root owner cannot be staged without root); `fdBases` replaces the candidate fd bases. */
export const privateDirTestHooks: {
  beforeCreate?: (directory: string) => void;
  stat?: (directory: string, stat: fs.Stats) => fs.Stats;
  linkStat?: (directory: string, stat: fs.Stats) => fs.Stats;
  fdBases?: string[];
} = {};

let provenBase: { candidates: string; base: string | undefined } | undefined;
/** The first fd base that resolves a held directory (`<base>/<fd>/.` is that directory), or undefined. */
export const fdBase = (): string | undefined => {
  const candidates = privateDirTestHooks.fdBases ?? ["/proc/self/fd", "/dev/fd"];
  if (provenBase?.candidates === candidates.join("\0")) return provenBase.base;
  let found: string | undefined;
  const fd = fs.openSync(os.tmpdir(), FOLLOW_DIRECTORY);
  try {
    const held = fs.fstatSync(fd);
    for (const base of candidates) {
      try {
        const seen = fs.statSync(`${base}/${fd}/.`);
        if (seen.dev === held.dev && seen.ino === held.ino) { found = base; break; }
      } catch { /* not available here */ }
    }
  } finally { fs.closeSync(fd); }
  provenBase = { candidates: candidates.join("\0"), base: found };
  return found;
};

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

/**
 * Self-heal inside Fabric's own root (pi-fabric#875 round 3): an EXISTING directory at or below `anchor` (the
 * Fabric-owned root the caller passes: the mesh root, or the agent run root) that is a real directory, owned by our
 * uid and group- or other-writable is tightened through its held fd (mode & ~022), with one log line per directory.
 * A Main under umask 002 left such containers (35 on Ryzen 1, e.g. residency/<hash>/runs at 0775).
 * ponytail: tightening our own directory is monotonic and only removes access. Entries already inside are not
 * trusted by it: the next name is still created through the fd, an EEXIST is still validated, and archive entries
 * are created O_EXCL|O_NOFOLLOW (writeArchiveFile). Never through a symlink: once the walk follows one (even
 * root's), nothing beyond it is healed. Above the anchor, and for a root-owned or foreign-owned directory anywhere,
 * the refusal rules hold unchanged.
 */
const TIGHTENED_LOG_LIMIT = 1024;
const tightened = new Set<string>();
const underAnchor = (walked: string, anchor: string | undefined): boolean =>
  anchor !== undefined && (walked === anchor || walked.startsWith(anchor.endsWith(path.sep) ? anchor : anchor + path.sep));
const tighten = (fd: number, stat: fs.Stats, walked: string): fs.Stats => {
  if (!stat.isDirectory() || stat.uid !== process.getuid?.() || (stat.mode & 0o022) === 0) return stat;
  const mode = stat.mode & 0o7777 & ~0o022;
  fs.fchmodSync(fd, mode);
  if (!tightened.has(walked)) {
    if (tightened.size >= TIGHTENED_LOG_LIMIT) tightened.clear(); // bounded: a repeat line at worst
    tightened.add(walked);
    console.warn(`[pi-fabric] tightened ${walked} from ${(stat.mode & 0o7777).toString(8)} to ${mode.toString(8)}`);
  }
  return fs.fstatSync(fd);
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

/** A private directory held open. `path` reaches exactly the validated directory (`<fd base>/<fd>`) while `close` has
 * not run, so a later rename or symlink of any ancestor cannot redirect writes made through it. `fd` is undefined on
 * Windows (main's behaviour: `path` is the plain path). */
export interface PrivateDirectory { path: string; fd: number | undefined; close(): void }

const openHeld = (target: string, anchor: string | undefined, base: string): PrivateDirectory => {
  const at = (fd: number, name: string): string => `${base}/${fd}/${name}`;
  let fd = fs.openSync("/", NOFOLLOW_DIRECTORY);
  try {
    safeDirectory(fs.fstatSync(fd), "/");
    let walked = "/";
    let healable = true;
    for (const name of target.split(path.sep).filter(Boolean)) {
      walked = path.join(walked, name);
      let child: number | undefined;
      let created = false;
      try { child = fs.openSync(at(fd, name), NOFOLLOW_DIRECTORY); }
      catch (error) {
        if (code(error) === "ELOOP" || code(error) === "ENOTDIR") {
          // A symlink: accepted only as system layout (the link is root's); its target is validated below, and
          // nothing at or beyond it is healed (pi-fabric#875 round 4 P2-a).
          const raw = fs.lstatSync(at(fd, name));
          const link = privateDirTestHooks.linkStat?.(walked, raw) ?? raw;
          if (!link.isSymbolicLink() || link.uid !== 0) throw new UnsafeDirectory(`${walked} is a symbolic link or not a directory`);
          child = fs.openSync(at(fd, name), FOLLOW_DIRECTORY);
          healable = false;
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
        else {
          const seen = privateDirTestHooks.stat?.(walked, stat) ?? stat;
          safeDirectory(!created && healable && underAnchor(walked, anchor) ? tighten(child, seen, walked) : seen, walked);
        }
      } catch (error) { fs.closeSync(child); throw error; }
      fs.closeSync(fd);
      fd = child;
    }
  } catch (error) { fs.closeSync(fd); throw error; }
  const held = fd;
  return { path: `${base}/${held}`, fd: held, close: () => fs.closeSync(held) };
};

/** Create or validate `directory` (see the module comment) and hold it open. `anchor`: the caller's own Fabric root,
 * at or below which our own group/other-writable directories are tightened. Throws on any unsafe directory, and when
 * no fd base works (no path-based fallback). */
export const openPrivateDir = (directory: string, anchor?: string): PrivateDirectory => {
  const target = path.resolve(directory);
  if (process.platform === "win32") {
    fs.mkdirSync(target, { recursive: true });
    return { path: target, fd: undefined, close: () => undefined };
  }
  const base = fdBase();
  if (base === undefined) throw new UnsafeDirectory(`cannot create ${target} safely: no fd-relative path (/proc/self/fd or /dev/fd) on this host`);
  return openHeld(target, anchor === undefined ? undefined : path.resolve(anchor), base);
};

/** `mkdir -p` whose created directories are exactly 0700, through trusted directories only. Throws on any unsafe one. */
export const mkdirPrivate = (directory: string, anchor?: string): void => { openPrivateDir(directory, anchor).close(); };

/**
 * Write one archive entry inside a held private directory (pi-fabric#875 round 4 SEC P1). A directory healed by the
 * self-heal above may hold entries planted while it was writable, so nothing is written through an existing name:
 * - a new entry is created `O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW`, mode 0600, through the held fd;
 * - an existing entry (a re-archive of the same run) is replaced only when an O_NOFOLLOW open proves it a regular file
 *   we own with one link: the bytes go to a fresh O_EXCL temp in the held directory, then rename replaces the name
 *   (rename replaces the link itself and never follows it);
 * - anything else (a symlink, a foreign or hard-linked file, a directory) refuses that entry: false.
 * `bytes` or `source` (read through one O_NOFOLLOW fd; a regular file) gives the content. `durable` fsyncs the file and
 * the held directory.
 */
export const writeArchiveFile = (held: PrivateDirectory, name: string, content: { bytes: Buffer } | { source: string }, durable = false): boolean => {
  const bytes = "bytes" in content ? content.bytes : (() => {
    const fd = fs.openSync(content.source, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
    try {
      if (!fs.fstatSync(fd).isFile()) throw new UnsafeDirectory(`${content.source} is not a regular file`);
      return fs.readFileSync(fd);
    } finally { fs.closeSync(fd); }
  })();
  const create = (entry: string): void => {
    const fd = fs.openSync(path.join(held.path, entry),
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeFileSync(fd, bytes); if (durable) fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  };
  const syncDirectory = (): void => { if (durable && held.fd !== undefined) fs.fsyncSync(held.fd); };
  try { create(name); syncDirectory(); return true; }
  catch (error) { if (code(error) !== "EEXIST") throw error; }
  // A re-archive: replace only our own regular file, by rename over it.
  let existing: number;
  try { existing = fs.openSync(path.join(held.path, name), fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0)); }
  catch { return false; }
  try {
    const stat = fs.fstatSync(existing);
    if (!stat.isFile() || stat.nlink !== 1 || (process.getuid && stat.uid !== process.getuid())) return false;
  } finally { fs.closeSync(existing); }
  const temp = `.${name}.${process.pid}.${randomUUID()}.tmp`;
  create(temp);
  try { fs.renameSync(path.join(held.path, temp), path.join(held.path, name)); }
  catch (error) { try { fs.unlinkSync(path.join(held.path, temp)); } catch { /* best effort */ } throw error; }
  syncDirectory();
  return true;
};
