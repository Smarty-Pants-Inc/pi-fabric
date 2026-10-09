/**
 * The root and file gate for every SQLite open of mesh state (pi-fabric#694 SEC/CODE rounds 7-10, smarty-dev#6477).
 * Its own small module so the file-mode writer fence (backend-fence.ts, startup graph) uses the same gate as the
 * lazy SQLite store, import, cutover, rollback, status and the state projector. Refuse, never repair a root.
 */
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

export class MeshStateUnsupportedError extends Error {
  readonly code = "FABRIC_MESH_STATE_UNSUPPORTED";
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "MeshStateUnsupportedError";
  }
}

/**
 * A private path: not a symbolic link; the expected type; owned by this uid; no group or other write. A state FILE
 * must also be owner-only (0600): mesh state holds credentials (smarty-dev#6787). An owned file with group/other
 * read bits is tightened in place, through an O_NOFOLLOW descriptor whose dev/ino must match the lstat, then
 * re-checked. Anything else is refused (FABRIC_MESH_STATE_UNSUPPORTED). A missing path passes. Windows has no uid
 * or mode bits here, so only the type and symlink checks apply.
 */
export const assertPrivatePath = (file: string, kind: "directory" | "file"): void => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return;
  const posix = process.platform !== "win32" && typeof process.getuid === "function";
  const uid = posix ? process.getuid!() : undefined;
  const why = stat.isSymbolicLink() ? "is a symbolic link"
    : kind === "directory" && !stat.isDirectory() ? "is not a directory"
    : kind === "file" && !stat.isFile() ? "is not a regular file"
    : uid !== undefined && stat.uid !== uid ? `is owned by uid ${stat.uid}, not this process's uid ${uid}`
    : posix && (stat.mode & 0o022) !== 0 ? `is group or other writable (mode ${(stat.mode & 0o777).toString(8)})`
    : posix && kind === "file" && (stat.mode & 0o077) !== 0 && !tightenOwnerOnly(file, stat)
      ? `is group or other readable (mode ${(stat.mode & 0o777).toString(8)}) and could not be made 0600`
    : undefined;
  if (why) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses ${file}: it ${why}`);
};

// chmod 0600 the very file that was checked: O_NOFOLLOW never follows a swapped-in link, and dev/ino must match.
const tightenOwnerOnly = (file: string, checked: fs.Stats): boolean => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
    const opened = fs.fstatSync(fd);
    if (opened.dev !== checked.dev || opened.ino !== checked.ino || !opened.isFile()) return false;
    fs.fchmodSync(fd, 0o600);
    return (fs.fstatSync(fd).mode & 0o077) === 0;
  } catch { return false; }
  finally { if (fd !== undefined) fs.closeSync(fd); }
};

/**
 * The ONE gate for every SQLite open or create of mesh state: SqliteStateStore (sync and async), the
 * backend-migration FenceDb (import, cutover, rollback, abort-rollback, status), the state projector's second
 * connection and the file-mode writer fence's meta read.
 *
 * 1. Ancestors (StrictModes), checked on the LEXICAL path (lstat; each symbolic link must be ours or root's and
 *    its target path is checked the same way, recursively) and on the REAL path (stat): every directory must be
 *    owned by this uid or root, not writable by other, nor by group, unless sticky; a group-writable directory is
 *    allowed only when its group is private (`privateGroup`). Nobody else can then rename or replace the root.
 * 2. Root: `mkdir` 0700 when creating; then opened O_NOFOLLOW|O_DIRECTORY and checked ON THAT FD: a directory,
 *    the same dev/ino as the name's lstat, owned by this uid, mode exactly 0700.
 * 3. Files: state.db is created `wx` 0600 under umask 077 (SQLite gives -wal/-shm state.db's mode); an existing
 *    state.db, -wal or -shm must be a regular file of ours, owner-only (assertPrivatePath).
 * 4. The root fd stays open across SQLite's open (node:sqlite opens by path; Node has no openat). After it, on
 *    Linux, the descriptor SQLite opened for state.db must have the dev/ino of state.db inside the PINNED
 *    directory (`/proc/self/fd/<root fd>/state.db`), so the database is proven to be in the checked root; the
 *    root's name must still resolve to the pinned dev/ino; the files are checked again. Otherwise the connection
 *    is closed and the open refused. SQLite derives -wal/-shm from the path it resolved at this open; after it,
 *    rule 1 is what keeps that directory ours. ponytail: no native openat VFS; the residual (same uid, or an
 *    ancestor this gate would refuse) is smarty-dev#7451.
 * Windows: the symlink and type checks only.
 */
export const openPrivateStateDb = <T extends { close(): void }>(root: string, create: boolean, open: (file: string) => T): T => {
  const refuse = (why: string): never => {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses root ${root}: ${why} (fix: chmod 700 ${root}, owned by this user, in a directory nobody else can write)`);
  };
  const file = path.join(root, "state.db");
  const posix = process.platform !== "win32" && typeof process.getuid === "function";
  if (create) fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  const named = fs.lstatSync(root);
  if (named.isSymbolicLink()) refuse("it is a symbolic link");
  if (!named.isDirectory()) refuse("it is not a directory");
  if (!posix) return withOwnerOnlyUmask(() => createAndOpen(file, create, open));
  const uid = process.getuid!();
  const unsafe = unsafeAncestor(path.resolve(root), uid);
  if (unsafe) refuse(`its ancestor ${unsafe}`);
  let fd: number | undefined;
  try {
    try { fd = fs.openSync(root, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | (fs.constants.O_DIRECTORY ?? 0)); }
    catch { refuse("it cannot be opened as a directory without following links"); }
    const pinned = fs.fstatSync(fd!);
    if (!pinned.isDirectory()) refuse("it is not a directory");
    if (pinned.dev !== named.dev || pinned.ino !== named.ino) refuse("it changed while it was checked");
    if (pinned.uid !== uid) refuse(`it is owned by uid ${pinned.uid}, not this process's uid ${uid}`);
    if ((pinned.mode & 0o777) !== 0o700) refuse(`it has mode ${(pinned.mode & 0o777).toString(8)}; it must be exactly 0700`);
    const before = ownDescriptors();
    const db = withOwnerOnlyUmask(() => createAndOpen(file, create, open));
    try {
      if (before) {
        const why = openedOutsidePinned(before, fd!);
        if (why) refuse(why);
      }
      const after = fs.lstatSync(root, { throwIfNoEntry: false });
      if (!after || after.isSymbolicLink() || after.dev !== pinned.dev || after.ino !== pinned.ino) refuse("it was replaced while SQLite opened it");
      assertPrivateStateFiles(file);
    } catch (error) {
      try { db.close(); } catch { /* refusing anyway */ }
      throw error;
    }
    return db;
  } finally { if (fd !== undefined) fs.closeSync(fd); }
};

const createAndOpen = <T>(file: string, create: boolean, open: (file: string) => T): T => {
  if (create) {
    // O_EXCL: when this succeeds no connection in this process can have the file open, so closing drops no lock.
    try { fs.closeSync(fs.openSync(file, "wx", 0o600)); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  }
  assertPrivateStateFiles(file); // an existing state.db, -wal or -shm must be ours (pi-fabric#694 P2-E)
  return open(file);
};

const assertPrivateStateFiles = (file: string): void => {
  assertPrivatePath(path.dirname(file), "directory"); // nobody else may swap files in the root
  for (const name of [file, `${file}-wal`, `${file}-shm`]) assertPrivatePath(name, "file");
};

/**
 * This process's open descriptors (Linux /proc); undefined where /proc is unavailable. The listing's own directory
 * descriptor is closed by the time it returns and its number is reused at once, so only descriptors still open
 * (fstat succeeds) count; otherwise SQLite's descriptor could take that number and look "already open".
 */
const ownDescriptors = (): Set<string> | undefined => {
  let names: string[];
  try { names = fs.readdirSync("/proc/self/fd"); } catch { return undefined; }
  return new Set(names.filter((name) => { try { fs.fstatSync(Number(name)); return true; } catch { return false; } }));
};

/**
 * Why SQLite's new state.db descriptor is not the state.db in the pinned directory, or undefined when it is.
 * `/proc/self/fd/<fd>/state.db` resolves through the pinned directory itself, never through the root's name.
 */
const openedOutsidePinned = (before: Set<string>, rootFd: number): string | undefined => {
  let expected: fs.Stats;
  try { expected = fs.lstatSync(`/proc/self/fd/${rootFd}/state.db`); }
  catch { return "its state.db is missing from the checked directory after the open"; }
  let fresh = false;
  let held = false;
  for (const name of ownDescriptors() ?? []) {
    let target: string;
    try { target = fs.readlinkSync(`/proc/self/fd/${name}`); } catch { continue; }
    if (path.basename(target) !== "state.db") continue;
    let opened: fs.Stats;
    try { opened = fs.fstatSync(Number(name)); } catch { continue; }
    const same = opened.dev === expected.dev && opened.ino === expected.ino;
    if (!before.has(name)) {
      if (!same) return `SQLite opened ${target}, which is not the state.db in the checked directory`;
      fresh = true;
    } else if (same) held = true;
  }
  // No new descriptor: SQLite reused one it already holds for this process (os_unix.c findReusableFd keeps the
  // descriptor of a closed connection while another holds POSIX locks, keyed by the dev/ino it found at the path).
  // That is sound only when this process already holds the pinned state.db.
  return fresh || held ? undefined : "SQLite's state.db descriptor could not be matched to the checked directory";
};

/** The first path component another local user could use to replace what is below `root`, described; else undefined. */
const unsafeAncestor = (root: string, uid: number): string | undefined =>
  unsafeLexical(path.dirname(root), uid, 0) ?? unsafeChain(fs.realpathSync(path.dirname(root)), uid, fs.statSync);

// Every component of `dir` as named, from `dir` up to "/": directories by the StrictModes rule; a symbolic link
// must be ours or root's (its directory is the next one up), and its target path is checked the same way.
const unsafeLexical = (dir: string, uid: number, depth: number): string | undefined => {
  if (depth > 40) return `${dir}: too many levels of symbolic links`;
  for (let current = dir; ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    if (stat.isSymbolicLink()) {
      if (stat.uid !== uid && stat.uid !== 0) return `${current} is a symbolic link owned by uid ${stat.uid}`;
      const why = unsafeLexical(path.resolve(path.dirname(current), fs.readlinkSync(current)), uid, depth + 1);
      if (why) return why;
    } else {
      const why = unsafeDirectory(current, stat, uid);
      if (why) return why;
    }
    if (path.dirname(current) === current) return undefined;
  }
};

const unsafeChain = (dir: string, uid: number, stat: (file: string) => fs.Stats): string | undefined => {
  for (let current = dir; ; current = path.dirname(current)) {
    const why = unsafeDirectory(current, stat(current), uid);
    if (why) return why;
    if (path.dirname(current) === current) return undefined;
  }
};

const unsafeDirectory = (dir: string, stat: fs.Stats, uid: number): string | undefined => {
  const sticky = (stat.mode & 0o1000) !== 0;
  const mode = (stat.mode & 0o7777).toString(8);
  if (stat.uid !== uid && stat.uid !== 0) return `${dir} is owned by uid ${stat.uid}`;
  if ((stat.mode & 0o002) !== 0 && !sticky) return `${dir} is writable by other (mode ${mode})`;
  if ((stat.mode & 0o020) !== 0 && !sticky && !privateGroup(stat.gid, uid))
    return `${dir} is writable by group ${stat.gid}, which is not provably private to this user (mode ${mode})`;
  return undefined;
};

/** The databases the name service reads for users and groups; a group is judged only when all are enumerable. */
const ENUMERABLE_SOURCES = new Set(["files", "systemd"]);

/**
 * A group is private when nobody but `uid` can act as it, judged through the name service (getent, so NSS sources
 * count, not only /etc): no account other than `uid` has it as primary group and its member list names nobody
 * else. Fail closed (not private) unless nsswitch.conf's passwd and group sources are all enumerable (files,
 * systemd; an LDAP/SSSD/NIS source may hold members getent cannot list) and both lookups succeed.
 */
export const privateGroup = (gid: number, uid: number, io: {
  nsswitch?: () => string; getent?: (database: "passwd" | "group", key?: string) => string;
} = {}): boolean => {
  try {
    const nsswitch = (io.nsswitch ?? (() => fs.readFileSync("/etc/nsswitch.conf", "utf8")))();
    for (const database of ["passwd", "group"]) {
      const line = nsswitch.split("\n").map((l) => l.replace(/#.*/, "").trim()).find((l) => l.startsWith(`${database}:`));
      if (!line) return false;
      const sources = line.slice(database.length + 1).replace(/\[[^\]]*\]/g, " ").split(/\s+/).filter(Boolean);
      if (sources.length === 0 || sources.some((source) => !ENUMERABLE_SOURCES.has(source))) return false;
    }
    const getent = io.getent ?? ((database, key) =>
      execFileSync("getent", key === undefined ? [database] : [database, key], { encoding: "utf8", timeout: 5_000, stdio: ["ignore", "pipe", "ignore"] }));
    const users = getent("passwd").split("\n").map((line) => line.split(":")).filter((f) => f.length >= 4);
    const self = users.find((f) => Number(f[2]) === uid)?.[0];
    if (self === undefined) return false;
    if (users.some((f) => Number(f[3]) === gid && Number(f[2]) !== uid)) return false;
    const entry = getent("group", String(gid)).split("\n").map((line) => line.split(":")).find((f) => f.length >= 4 && Number(f[2]) === gid);
    if (!entry) return false;
    return (entry[3] ?? "").split(",").map((name) => name.trim()).filter(Boolean).every((name) => name === self);
  } catch { return false; }
};

/**
 * Run `create` (synchronous: our 0600 state.db create and SQLite's first open) under umask 077, so nothing it
 * creates can be wider than owner-only. A worker thread cannot set the umask (ERR_WORKER_UNSUPPORTED_OPERATION):
 * there the 0600 create and the 0700 root still hold. (Process-wide; smarty-dev#7451 item 1.)
 */
export const withOwnerOnlyUmask = <T>(create: () => T): T => {
  let previous: number | undefined;
  try { previous = process.umask(0o077); } catch { previous = undefined; }
  try { return create(); }
  finally { if (previous !== undefined) process.umask(previous); }
};
