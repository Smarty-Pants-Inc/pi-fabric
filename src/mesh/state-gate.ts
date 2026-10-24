/**
 * The one gate for every SQLite connection to a mesh state root's state.db (smarty-dev#7548): the store
 * (state-sqlite.ts), the backend tool's FenceDb (backend-migration.ts), the file-mode writer fence's meta read
 * (backend-fence.ts) and the projector (state-projector.ts). A leaf module (node:fs, node:path and the Windows ACL
 * reader windows-state-acl.ts, types otherwise), so the file-mode fence can use it without loading the SQLite store.
 * Windows ACL findings are advisory unless PI_FABRIC_MESH_STATE_ACL_ENFORCE is exactly "1" at call time;
 * filesystem, file-type, inode and POSIX checks remain mandatory.
 */
import fs from "node:fs";
import path from "node:path";
import type { SqliteConnection, SqliteOpener } from "./state-sqlite.js";
import { windowsStateAclRefusal } from "./windows-state-acl.js";

export class MeshStateUnsupportedError extends Error {
  readonly code = "FABRIC_MESH_STATE_UNSUPPORTED";
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "MeshStateUnsupportedError";
  }
}

// WAL needs a coherent shared memory map between processes on one host (review-opus P2-7).
const NON_LOCAL_FILESYSTEMS = new Set([
  "nfs", "nfs4", "cifs", "smb3", "smbfs", "9p", "virtiofs", "fuse.sshfs", "sshfs", "fuse.rclone", "fuse.gcsfuse",
  "fuse.s3fs", "ceph", "fuse.ceph", "glusterfs", "fuse.glusterfs", "lustre", "afs", "davfs", "fuse.davfs2", "gpfs",
  "ncpfs", "coda", "fuse.vmhgfs-fuse", "vboxsf", "prl_fs",
]);
// statfs magic numbers, used only when /proc/self/mountinfo is unreadable.
const NON_LOCAL_MAGIC = new Map<number, string>([
  [0x6969, "nfs"], [0x517b, "smbfs"], [0xff534d42, "cifs"], [0xfe534d42, "smb2"], [0x01021997, "9p"],
]);

const unescapeMount = (value: string): string => value.replace(/\\([0-7]{3})/g, (_, octal: string) => String.fromCharCode(parseInt(octal, 8)));

const nearestExisting = (target: string): string => {
  let current = path.resolve(target);
  for (;;) {
    try { return fs.realpathSync(current); } catch { /* walk up */ }
    const parent = path.dirname(current);
    if (parent === current) return current;
    current = parent;
  }
};

/**
 * Why a root cannot hold a SQLite WAL database, or undefined when it can. Refuses network and
 * virtual filesystems (NFS, SMB/CIFS, sshfs, 9p, virtiofs, ...) and Windows UNC paths. macOS and
 * mapped Windows drives cannot be classified without native code: they are documented as local-only.
 */
export const filesystemRefusal = (root: string,
  input: { platform?: NodeJS.Platform; mountinfo?: string } = {}): string | undefined => {
  const platform = input.platform ?? process.platform;
  if (platform === "win32") {
    const normalized = root.replace(/\//g, "\\");
    if (/^\\\\\?\\UNC\\/i.test(normalized)) return `network path ${root}`;
    if (/^\\\\[?.]\\/.test(normalized)) return undefined;
    if (normalized.startsWith("\\\\")) return `network path ${root}`;
    return undefined;
  }
  if (platform !== "linux") return undefined;
  // A supplied mountinfo describes a Linux system: resolve with POSIX rules whatever the host is.
  const target = input.mountinfo === undefined ? nearestExisting(root) : path.posix.resolve(root);
  let mountinfo = input.mountinfo;
  if (mountinfo === undefined) {
    try { mountinfo = fs.readFileSync("/proc/self/mountinfo", "utf8"); } catch { mountinfo = undefined; }
  }
  if (mountinfo !== undefined) {
    let best: { mount: string; type: string } | undefined;
    for (const line of mountinfo.split("\n")) {
      const separator = line.indexOf(" - ");
      if (separator < 0) continue;
      const mount = unescapeMount(line.slice(0, separator).split(" ")[4] ?? "");
      const type = line.slice(separator + 3).split(" ")[0] ?? "";
      if (!mount.startsWith("/")) continue;
      const inside = target === mount || mount === "/" || target.startsWith(mount.endsWith("/") ? mount : `${mount}/`);
      if (inside && (!best || mount.length >= best.mount.length)) best = { mount, type };
    }
    if (best) return NON_LOCAL_FILESYSTEMS.has(best.type) ? `${best.type} filesystem at ${best.mount}` : undefined;
  }
  try {
    const type = Number(fs.statfsSync(target).type) >>> 0;
    const name = NON_LOCAL_MAGIC.get(type);
    return name ? `${name} filesystem at ${target}` : undefined;
  } catch { return undefined; }
};

/**
 * pi-fabric#694 P2-D/P2-E (+ astra c6076385095): an existing state path (state.db, its -wal/-shm, the flag
 * directory, the root) is used only when it is ours: lstat, so a symbolic link is never followed; the expected
 * type; owned by this uid; no group or other write. A state FILE must also be owner-only (0600): mesh state holds
 * credentials (smarty-dev#6787). An owned file with group/other bits is tightened in place, through an
 * O_NOFOLLOW descriptor whose dev/ino must match the lstat, then re-checked. Anything else is refused
 * (FABRIC_MESH_STATE_UNSUPPORTED). A missing path passes. Windows retains the type/symlink checks, but has no uid
 * or mode bits: an existing path gets an advisory native ACL read, enforced only with
 * PI_FABRIC_MESH_STATE_ACL_ENFORCE=1: the path itself as a strict directory or file target (the elevated
 * Administrators owner exception included), every parent as an ancestor. Standalone requests always get a fresh
 * native proof. State-root callers instead use the full root plan, whose successful Windows proof is process-cached
 * by root/DB identity in the ACL reader; checkpoint flag writers reuse that same plan plus cheap directory checks.
 * The read-only flag scan uses assertGatedFlagDirectory instead.
 */
export const assertPrivatePath = (file: string, kind: "directory" | "file"): fs.Stats | undefined => {
  const stat = checkPrivatePath(file, kind);
  if (stat && process.platform === "win32") assertWindowsAcl(file, { role: kind, files: [], directories: [] }, false);
  return stat;
};

/**
 * smarty-dev#8920: the checkpoint flag directory for the READ-ONLY advisory scan (checkpointFlagRaised, polled on
 * every writer-lock attempt of an open store). POSIX: exactly assertPrivatePath (lstat, type, uid, mode). Windows:
 * the lstat half only (no symbolic link, a directory), and no native ACL read, so polling never spawns a child. The
 * native snapshot includes this directory at root admission (assertPrivateStateRoot names it among its strict targets).
 * Findings warn by default; only opt-in enforcement establishes the following privacy guarantees: an existing foreign
 * grant (inherit-only included) refuses admission; a directory created later inherits the validated root's DACL,
 * whose inheritable grants were judged as strictly. Re-ACLing or replacing it needs WRITE_DAC, ownership or delete
 * rights that enforcement refused to every foreign SID. The lstat runs on every call, independently of the reader's
 * root/DB identity cache. Same-identity user/Admin DACL mutations after admission are outside that cache's contract.
 * Writers use the same advisory-or-enforced root plan and this cheap directory check before writing.
 */
export const assertGatedFlagDirectory = (dir: string): fs.Stats | undefined => checkPrivatePath(dir, "directory");

// The lstat half of assertPrivatePath. Windows: type and symlink checks only; the gate's callers use the matching
// root plan (assertWindowsAcl) covering state files, flag directory and ancestors, with identity caching in the reader.
const checkPrivatePath = (file: string, kind: "directory" | "file"): fs.Stats | undefined => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return undefined;
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
  return stat;
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
 * smarty-dev#7784 (security-gap; pi-fabric#730 CODE c6085070307, pi-fabric#733 CODE c6085492681): bind the SQLite
 * connection to the very state.db that assertPrivateStateFiles validated (its lstat is the baseline, not a later one).
 * node:sqlite has no fd API, but SQLite's unix VFS opens the main file inside the constructor, on a descriptor of its
 * own even when another connection has the file open. So, before any SQL: snapshot /proc/self/fd, open, and require
 * exactly one NEW descriptor whose link names this state.db, on the validated dev/ino, with the path still naming it
 * (or SQLite's reuse of a descriptor parked on that inode, proven below).
 * A swap (a link to another mesh's database, or a renamed-in file, even one put back at once) gives that descriptor
 * another link or identity: zero or several candidates, or a mismatch, closes the connection and fails closed, as
 * does Linux without /proc/self/fd. Other platforms keep only the post-open lstat compare (residual on #7784).
 */
const openPinned = (file: string, open: SqliteOpener, validated: fs.Stats): SqliteConnection => {
  const refuse = (why: string) => new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses ${file}: ${why}`);
  const linux = process.platform === "linux";
  const name = path.join(fs.realpathSync(path.dirname(file)), path.basename(file));
  const before = linux ? fdSnapshot() : undefined;
  if (linux && before === undefined) throw refuse("/proc/self/fd is unavailable, so the open cannot be bound to the checked file");
  const db = open(file);
  try {
    if (before !== undefined) {
      const after = fdSnapshot();
      if (after === undefined) throw refuse("/proc/self/fd is unavailable, so the open cannot be bound to the checked file");
      const link = (fd: string) => { try { return fs.readlinkSync(`/proc/self/fd/${fd}`); } catch { return undefined; } };
      const target = `${validated.dev}:${validated.ino}`;
      const added = [...after].filter(([fd, identity]) => before.get(fd) !== identity);
      const mains = added.filter(([fd]) => [name, `${name} (deleted)`].includes(link(fd) ?? ""));
      if (mains.length > 1) throw refuse(`the connection's own descriptor on it is ambiguous (${mains.length} new)`);
      if (mains.length === 1 && mains[0]![1] !== target) throw refuse("the connection opened a different file than the one checked");
      // Zero new: SQLite's unix VFS took back a descriptor it parked (setPendingFd) when an earlier connection in this
      // process closed while another one still had the inode open; findReusableFd hands it to the next open of the
      // inode it stat()s, so it is unproven by itself. It is proven when no regular descriptor appeared at all and
      // the validated inode is the ONLY inode with a parked descriptor (descriptors on it beyond this module's live
      // connections): then the reused one can only be that. Residual on #7784: a SQLite connection in this process
      // that bypasses this module.
      // smarty-dev#7548: "only" is scoped to this state root's directory. A process may hold another root (the
      // shadow-mode resident host: state-shadow/ and state-projector/), and its parked descriptor must not fail this
      // root closed. Residual: a swap of this root's state.db to another root this process holds, while both have a
      // parked descriptor; that needs write access to this private root (same uid or root, #7548's accepted class).
      if (mains.length === 0) {
        const fds = new Map<string, number>();
        for (const [, identity] of before) fds.set(identity, (fds.get(identity) ?? 0) + 1);
        const parked = (identity: string) => (fds.get(identity) ?? 0) - (liveConnections.get(identity) ?? 0);
        const parkedOther = (sameRoot: boolean) => [...liveConnections.keys()].some(identity =>
          identity !== target && parked(identity) > 0 && (liveRoots.get(identity) === path.dirname(name)) === sameRoot);
        const elsewhere = parkedOther(true) || parkedOther(false);
        if (added.length > 0 || parked(target) < 1 || parkedOther(true)) {
          throw refuse(`the connection's own descriptor on it is not proven (0 new, ${added.length} other, ` +
            `${Math.max(0, parked(target))} parked here${elsewhere ? ", parked elsewhere" : ""})`);
        }
      }
    }
    const now = fs.lstatSync(file, { throwIfNoEntry: false });
    if (!now?.isFile() || now.dev !== validated.dev || now.ino !== validated.ino) throw refuse("it was replaced while opening");
    return tracked(db, `${validated.dev}:${validated.ino}`, path.dirname(name));
  } catch (error) {
    try { db.close(); }
    catch (closeError) {
      // smarty-dev#11466: a failed close rides on the original error (message, code unchanged) for the caller's cleanup.
      if (error instanceof Error) { try { Object.assign(error, { sqliteCloseError: closeError }); } catch { /* keep the original */ } }
    }
    throw error;
  }
};

// This module's live connections per "dev:ino" (openPinned's reuse proof). Closing a connection releases its count.
const liveConnections = new Map<string, number>();
// The real root directory of each live "dev:ino" (the reuse proof's scope, smarty-dev#7548).
const liveRoots = new Map<string, string>();
const tracked = (db: SqliteConnection, identity: string, root: string): SqliteConnection => {
  liveConnections.set(identity, (liveConnections.get(identity) ?? 0) + 1);
  liveRoots.set(identity, root);
  const close = db.close.bind(db);
  let open = true;
  db.close = () => {
    if (open) {
      open = false;
      const left = (liveConnections.get(identity) ?? 1) - 1;
      if (left > 0) liveConnections.set(identity, left);
      else { liveConnections.delete(identity); liveRoots.delete(identity); }
    }
    close();
  };
  return db;
};

// smarty-dev#9501: descriptor numbers this process holds on purpose on state files it created (owned-import-files.ts
// keeps each O_EXCL create open through the run). They are never SQLite's, so they are not parked descriptors: the
// snapshots below leave them out. The token makes the disposer idempotent and harmless after the number was released
// and registered again. Internal: this trusts the caller to register ONLY its own ownership pins, and to unregister
// a number once it stops owning it; a registered number is invisible to the descriptor proof.
const ownershipPins = new Map<number, object>();
/** Exclude `fd` from openPinned's descriptor proof while it is pinned; the returned disposer never throws. */
export const registerStateFileOwnershipPin = (fd: number): (() => void) => {
  const token = {};
  ownershipPins.set(fd, token);
  return () => { if (ownershipPins.get(fd) === token) ownershipPins.delete(fd); };
};

// fd -> "dev:ino" of each open regular file. Compared by identity as well as number: the listing's own directory fd
// is closed again at once, so SQLite may get that very number.
const fdSnapshot = (): Map<string, string> | undefined => {
  let names: string[];
  try { names = fs.readdirSync("/proc/self/fd"); } catch { return undefined; }
  const identities = new Map<string, string>();
  for (const fd of names) {
    if (ownershipPins.has(Number(fd))) continue; // an ownership pin, not a SQLite descriptor (smarty-dev#9501)
    try { const stat = fs.fstatSync(Number(fd)); if (stat.isFile()) identities.set(fd, `${stat.dev}:${stat.ino}`); }
    catch { /* closed meanwhile (the listing's own fd) */ }
  }
  return identities;
};

/**
 * The ONE gate for every connection to a state root's state.db. A local filesystem, mandatory POSIX
 * privacy checks, advisory Windows ACL checks (enforced only by explicit opt-in), mandatory file-type checks and
 * a connection bound to the checked inode (openPinned).
 * `create` (smarty-dev#8920, Windows creation paths only): the exclusive create of a missing state.db, run AFTER the
 * root half (filesystem, ancestors, root, state files and flag directory: the normal root plan) and BEFORE the
 * state files' lstat and the pinned open, so a refused root still gets no file (#846 SEC P2). When state.db was missing
 * for that first proof, a second full native proof follows the callback before the open. Existing state.db reuses
 * the reader's successful admission proof only for the same root/DB identities and target plan.
 * It skips nothing: every check below runs as without it. A state.db it creates
 * inherits the root's DACL, whose inheritable grants the read just judged (advisory unless enforcement is enabled).
 * POSIX callers keep their own sequence
 * (assertPrivateStateRoot, the create, then this gate without `create`).
 */
export const openPrivateStateDb = (file: string, open: SqliteOpener, create?: () => void): SqliteConnection => {
  // A no-create open must prove DB presence in the native snapshot immediately. A create path gets the normal
  // pre-create allowance for confirmed absence, then the mandatory post-create proof below requires presence.
  const requireDatabase = process.platform === "win32" && create === undefined;
  assertPrivateStateRoot(path.dirname(file), path.basename(file), requireDatabase);
  create?.();
  // Always rerun the normal Windows root gate after the callback. Stable known IDs hit the complete cached proof;
  // unknown IDs and newly created DBs take a fresh native proof, so an EEXIST/planted winner is never lstat-only.
  if (process.platform === "win32" && create !== undefined) assertPrivateStateRoot(path.dirname(file), path.basename(file), true);
  return openPinned(file, open, assertPrivateStateFiles(file));
};

/**
 * Prime an EXISTING Windows state.db's normal root proof before any bounded lock/busy operation. No SQLite open,
 * schema work or creation: a missing DB returns after one lstat and retains the gate's fresh pre/post-create proofs.
 * Successful identity-keyed caching (and cheap lexical ancestor/reparse guards on hits) belongs to the ACL reader.
 */
export const admitExistingStateRoot = (root: string): void => {
  if (process.platform !== "win32") return;
  try { if (fs.lstatSync(path.join(root, "state.db"), { throwIfNoEntry: false }) === undefined) return; }
  catch { /* Indeterminate presence is not absence: let the full native proof decide, without caching unknown IDs. */ }
  assertPrivateStateRoot(root, "state.db", true);
};

/** The root half of the gate: run before creating state.db; Windows ACL findings warn unless opted into enforcement. */
export const assertPrivateStateRoot = (root: string, db = "state.db", requireDatabase = false): void => {
  const refusal = filesystemRefusal(root);
  if (refusal) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state needs a local filesystem: ${refusal}`);
  assertPrivateAncestors(root);
  checkPrivatePath(root, "directory");
  if (process.platform === "win32") assertWindowsAcl(root, { ...stateTargets(root, db), requireDatabase }, false);
};

/**
 * smarty-dev#8920: creates a missing state root (mode 0700). POSIX: the same recursive mkdir as before, nothing
 * else. Windows defaults to recursive mkdir plus an advisory root snapshot, without ACL preflights or mutation.
 * Only PI_FABRIC_MESH_STATE_ACL_ENFORCE=1 enables the volume-down lexical checks and ancestor ACL preflights BEFORE
 * each non-recursive mkdir, followed by an explicit owner and protected private DACL on the root we just created.
 * In enforcement mode an EEXIST winner is refused, never repaired or adopted; existing roots are gated at admission/open.
 * Neither mode repairs an existing root. Windows callers run this BEFORE the fence
 * (custody-lock.ts and fence-lock.ts create the root too, untracked).
 */
export const createStateRoot = (root: string): void => {
  if (process.platform !== "win32") { fs.mkdirSync(root, { recursive: true, mode: 0o700 }); return; }
  if (!windowsAclEnforced()) {
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    checkPrivatePath(root, "directory");
    assertWindowsAcl(root, stateTargets(root, "state.db"), false);
    return;
  }
  const chain = [path.resolve(root)];
  while (path.dirname(chain[0]!) !== chain[0]) chain.unshift(path.dirname(chain[0]!));
  const refuse = (why: string) => new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses ${root}: ${why}`);
  // Start at the volume: lstat of a descendant alone would follow an unchecked junction in its parent path.
  const existing = (): number => {
    for (const [index, component] of chain.entries()) {
      const stat = fs.lstatSync(component, { throwIfNoEntry: false });
      if (!stat) return index;
      if (stat.isSymbolicLink()) throw refuse(`${component} is a reparse point (junction or symbolic link); `
        + "set the mesh state root to a path with no junctions or symbolic links in it");
      if (!stat.isDirectory()) throw refuse(`${component} is not a directory`);
    }
    return chain.length;
  };
  const firstMissing = existing();
  if (firstMissing === chain.length) return;
  for (let index = firstMissing; index < chain.length; index += 1) {
    const parent = chain[index - 1];
    if (!parent || existing() !== index) throw refuse("its directory chain changed before creation");
    // The immediate parent is a creation boundary: its foreign inheritable write grants must not reach the new child.
    // Distant ancestors retain the ordinary add-child/inherit-only policy (the default profile/volume controls).
    assertWindowsAcl(parent, { role: "creation-parent", files: [], directories: [] }, false);
    if (existing() !== index) throw refuse("its directory chain changed during the ancestor ACL read");
    try { fs.mkdirSync(chain[index]!, { mode: 0o700 }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") throw refuse(`${chain[index]} appeared during creation; it was not adopted`);
      throw error;
    }
  }
  if (existing() !== chain.length) throw refuse("its directory chain changed before setting the private ACL");
  assertWindowsAcl(root, stateTargets(root, "state.db"), true);
};

// The flag directory's name (state-sqlite.ts CHECKPOINT_FLAGS): checked with the state files when it exists.
const CHECKPOINT_FLAGS = "state-checkpoint.flags";
const stateTargets = (root: string, db: string) => {
  const file = path.join(root, db);
  return { files: [file, `${file}-wal`, `${file}-shm`, `${file}-journal`], directories: [path.join(root, CHECKPOINT_FLAGS)] };
};
// The normal root plan must match at early admission, open and checkpoint flag writes. The ACL reader alone caches
// verified successful root-role proofs for stable root+DB identities (volume and lossless file IDs, never ctime).
// Missing/unknown DB identity and unverified tooling fallbacks never cache; standalone target, creation-parent and
// protect requests stay fresh. A missing DB still gets full pre/post-create snapshots. Only enforcement creates N
// missing directories with N creation-parent preflights plus protect/readback; those cannot satisfy the open's plan.
type AclTargets = { role?: "root" | "directory" | "file" | "ancestor" | "creation-parent"; files: string[]; directories: string[]; requireDatabase?: boolean };
// Read on every call: removing the setting restores advisory startup without changing any ACL.
const windowsAclEnforced = (): boolean => process.env.PI_FABRIC_MESH_STATE_ACL_ENFORCE === "1";
// Never evict: a busy root must not start warning again on each poll after other roots fill the bound.
const warnedWindowsAclRoots = new Set<string>();
const MAX_WINDOWS_ACL_WARNINGS = 64;
const windowsAclFinding = (root: string, why: string, cause?: unknown): void => {
  // Enforcement precedes warning dedupe, so a previous advisory can never authorize an enforced open.
  if (windowsAclEnforced()) {
    throw new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses ${root}: ${why}`, cause === undefined ? undefined : { cause });
  }
  const key = path.resolve(root).toLowerCase();
  if (warnedWindowsAclRoots.has(key) || warnedWindowsAclRoots.size >= MAX_WINDOWS_ACL_WARNINGS) return;
  warnedWindowsAclRoots.add(key);
  process.emitWarning(`[pi-fabric] Fabric mesh SQLite state Windows ACL privacy finding for ${root}: ${why}. `
    + "Continuing with advisory ACL checks. Set PI_FABRIC_MESH_STATE_ACL_ENFORCE=1 to enforce refusal; "
    + "remove/unset PI_FABRIC_MESH_STATE_ACL_ENFORCE to restore warning-only startup (no ACL changes needed).",
    { code: "FABRIC_MESH_STATE_ACL_ADVISORY" });
};
const assertWindowsAcl = (root: string, targets: AclTargets, protect: boolean): void => {
  let real: string;
  // The real chain is the parent's: the target itself is never followed; its reparse finding uses the same policy.
  try { real = path.join(fs.realpathSync.native(path.dirname(path.resolve(root))), path.basename(path.resolve(root))); }
  catch (error) { windowsAclFinding(root, "its real path is unreadable", error); return; }
  const why = windowsStateAclRefusal({ root, real, protect, ...targets });
  if (why) windowsAclFinding(root, why);
};

// StrictModes (smarty-dev#7548): every ancestor of the root, on the lexical and the real chain, is owned by this
// uid or root, and a directory among them is not group or other writable unless sticky (like /tmp). Otherwise
// another user could swap the root out from under the checks.
const assertPrivateAncestors = (root: string): void => {
  // Windows: no uid or mode bits; the ACL snapshot (assertWindowsAcl, smarty-dev#8920) judges the ancestors.
  if (process.platform === "win32" || typeof process.getuid !== "function") return;
  const uid = process.getuid();
  const above = (dir: string): string[] => {
    const chain: string[] = [];
    for (let at = path.dirname(dir); ; at = path.dirname(at)) { chain.push(at); if (at === path.dirname(at)) return chain; }
  };
  const lexical = path.resolve(root);
  for (const dir of new Set([...above(lexical), ...above(fs.realpathSync(lexical))])) {
    const stat = fs.lstatSync(dir);
    const why = stat.uid !== uid && stat.uid !== 0 ? `is owned by uid ${stat.uid}, not this process's uid ${uid} or root`
      : !stat.isSymbolicLink() && (stat.mode & 0o022) !== 0 && (stat.mode & 0o1000) === 0
        ? `is group or other writable without the sticky bit (mode ${(stat.mode & 0o7777).toString(8)})` : undefined;
    if (why) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses ${root}: its ancestor ${dir} ${why}`);
  }
};

// The validated lstat of state.db is returned: openPinned binds the connection to it.
const assertPrivateStateFiles = (file: string): fs.Stats => {
  // lstat only: the matching root proof (assertPrivateStateRoot, just before) covers these Windows ACLs.
  checkPrivatePath(path.dirname(file), "directory"); // type checks always; Windows ACL privacy is opt-in
  for (const name of [`${file}-wal`, `${file}-shm`]) checkPrivatePath(name, "file");
  const stat = checkPrivatePath(file, "file");
  if (!stat?.isFile()) throw new MeshStateUnsupportedError(`Fabric mesh SQLite state refuses ${file}: it is not a regular file`);
  return stat;
};
