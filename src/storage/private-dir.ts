import { randomUUID } from "node:crypto";
import { diagnostics } from "../core/diagnostics.js";
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
 * anywhere above cannot redirect the walk. Linux without a working /proc/self/fd fails closed. Where no base resolves a
 * held directory elsewhere (macOS: /dev/fd/N is not traversable), the validated walk (openWalked) is used instead.
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
  /** Replaces process.platform for the fallback choice (a darwin host on a Linux runner). */
  platform?: NodeJS.Platform;
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
    diagnostics.warn("src/storage/private-dir.ts", `[pi-fabric] tightened ${walked} from ${(stat.mode & 0o7777).toString(8)} to ${mode.toString(8)}`);
  }
  return fs.fstatSync(fd);
};

/** fchmod 0700 through an O_NOFOLLOW directory fd, only when fstat proves our own directory. A link, a file, a
 * foreign or vanished entry: false, nothing changed. Windows has no POSIX modes: true, nothing to do. */
export const closeDirectory = (directory: string): boolean => {
  if (process.platform === "win32") return true;
  let fd: number;
  let before: fs.Stats | undefined;
  try { fd = fs.openSync(directory, NOFOLLOW_DIRECTORY); }
  catch (error) {
    // A copy made under a umask such as 0400 leaves our own fresh directory unreadable (0300): give the owner read
    // and search back by path, then open it. ponytail: only for a real directory we own (lstat), and its use here is
    // a tree this archive just created inside a held 0700 directory, so only our own uid could swap it in between.
    if (code(error) !== "EACCES") return false;
    before = fs.lstatSync(directory, { throwIfNoEntry: false });
    if (!before?.isDirectory() || (process.getuid && before.uid !== process.getuid())) return false;
    try { fs.chmodSync(directory, 0o700); fd = fs.openSync(directory, NOFOLLOW_DIRECTORY); } catch { return false; }
  }
  try {
    const stat = fs.fstatSync(fd);
    if (before && (stat.dev !== before.dev || stat.ino !== before.ino)) return false;
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
          // Right after OUR mkdir (never on EEXIST): 0700 through the same fd-pinned path, before the open. A umask such as
          // 0400 leaves 0300, which O_RDONLY cannot open (pi-fabric#875 round 5 CODE P1). Our fresh directory, in a
          // held parent, cannot be swapped by another uid.
          try { fs.mkdirSync(at(fd, name), { mode: 0o700 }); created = true; fs.chmodSync(at(fd, name), 0o700); }
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

/**
 * The walk where no fd base resolves a held directory (macOS: /dev/fd/N is not traversable). Each component is lstat'ed
 * and validated with the same rules, created with mkdir 0700 (an EEXIST is validated again, never skipped), then opened
 * O_DIRECTORY|O_NOFOLLOW and required to be the same dev and ino as the lstat (a swap before the open refuses); a
 * created one is set to 0700 and a healed one tightened through that fd. Archive leaves are still created
 * O_EXCL|O_NOFOLLOW, by full path (writeArchiveFile).
 * ponytail: macOS has no traversable /dev/fd/N and Node has no openat, so a swap of an already validated ancestor
 * between its check and a later use remains possible there (a named residual, its own follow-up issue). Linux uses the
 * fd-pinned walk (openHeld). Strictly stronger than main, which validated nothing.
 */
const openWalked = (target: string, anchor: string | undefined): PrivateDirectory => {
  safeDirectory(fs.lstatSync("/"), "/");
  let fd = fs.openSync("/", NOFOLLOW_DIRECTORY);
  try {
    let walked = "/";
    let healable = true;
    for (const name of target.split(path.sep).filter(Boolean)) {
      walked = path.join(walked, name);
      let stat = fs.lstatSync(walked, { throwIfNoEntry: false });
      let created = false;
      if (!stat) {
        privateDirTestHooks.beforeCreate?.(walked);
        try { fs.mkdirSync(walked, { mode: 0o700 }); created = true; fs.chmodSync(walked, 0o700); } // see openHeld (umask 0400)
        catch (error) { if (code(error) !== "EEXIST") throw error; } // a competing creator: validated below
        stat = fs.lstatSync(walked);
      }
      let flags = NOFOLLOW_DIRECTORY;
      if (stat.isSymbolicLink()) {
        const link = privateDirTestHooks.linkStat?.(walked, stat) ?? stat;
        if (link.uid !== 0) throw new UnsafeDirectory(`${walked} is a symbolic link or not a directory`);
        created = false; healable = false; flags = FOLLOW_DIRECTORY;
        stat = fs.statSync(walked);
      }
      if (!stat.isDirectory()) throw new UnsafeDirectory(`${walked} is a symbolic link or not a directory`);
      let child: number;
      try { child = fs.openSync(walked, flags); }
      catch (error) { throw new UnsafeDirectory(`${walked} changed while it was validated (${code(error)})`); }
      try {
        const opened = fs.fstatSync(child);
        if (opened.dev !== stat.dev || opened.ino !== stat.ino) throw new UnsafeDirectory(`${walked} changed while it was validated`);
        if (created && opened.isDirectory() && opened.uid === process.getuid?.()) fs.fchmodSync(child, 0o700);
        else {
          const seen = privateDirTestHooks.stat?.(walked, opened) ?? opened;
          safeDirectory(healable && underAnchor(walked, anchor) ? tighten(child, seen, walked) : seen, walked);
        }
      } catch (error) { fs.closeSync(child); throw error; }
      fs.closeSync(fd);
      fd = child;
    }
  } catch (error) { fs.closeSync(fd); throw error; }
  const held = fd;
  return { path: target, fd: held, close: () => fs.closeSync(held) };
};

/** Create or validate `directory` (see the module comment) and hold it open. `anchor`: the caller's own Fabric root,
 * at or below which our own group/other-writable directories are tightened. Throws on any unsafe directory. Linux
 * needs a working /proc/self/fd (else it fails closed); another POSIX host without a traversable fd base (macOS) uses
 * the validated walk; Windows keeps main's plain recursive mkdir. */
export const openPrivateDir = (directory: string, anchor?: string): PrivateDirectory => {
  const target = path.resolve(directory);
  const platform = privateDirTestHooks.platform ?? process.platform;
  if (platform === "win32") {
    fs.mkdirSync(target, { recursive: true });
    return { path: target, fd: undefined, close: () => undefined };
  }
  const root = anchor === undefined ? undefined : path.resolve(anchor);
  const base = fdBase();
  if (base !== undefined) return openHeld(target, root, base);
  if (platform === "linux") throw new UnsafeDirectory(`cannot create ${target} safely: /proc/self/fd does not resolve a held directory on this Linux host`);
  return openWalked(target, root);
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
    // Windows has no O_NOFOLLOW: fstat alone would accept a symlink's target
    // and falsely certify its bytes as an archive of the source entry.
    const before = fs.lstatSync(content.source);
    if (!before.isFile()) throw new UnsafeDirectory(`${content.source} is not a regular file`);
    const fd = fs.openSync(content.source, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
    try {
      const opened = fs.fstatSync(fd), named = fs.lstatSync(content.source);
      if (!opened.isFile() || !named.isFile() || opened.dev !== before.dev || opened.ino !== before.ino ||
          opened.dev !== named.dev || opened.ino !== named.ino) {
        throw new UnsafeDirectory(`${content.source} changed while it was opened`);
      }
      return fs.readFileSync(fd);
    } finally { fs.closeSync(fd); }
  })();
  const create = (entry: string): void => {
    const fd = fs.openSync(path.join(held.path, entry),
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    // 0600 whatever the umask (0400 would leave 0200), through the creating fd.
    try { fs.fchmodSync(fd, 0o600); fs.writeFileSync(fd, bytes); if (durable) fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
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
