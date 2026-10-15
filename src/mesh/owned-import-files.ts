/**
 * smarty-dev#9501: the files an import or cutover run creates in the mesh root, and their quarantine when the switch is
 * refused before its importing commit. The root is held by a no-follow directory fd for the whole fenced section.
 * Every create and every move goes through `/proc/self/fd/<fd>/<name>`, so renaming or swapping the root path
 * cannot redirect either one. A file counts as this run's only when this run created it with O_EXCL. That FILE
 * descriptor stays open from the create through the database close and the cleanup, and is released by `close`, except
 * after a failed database close: then it stays open until the process exits (see `keepAfterFailure`). A name
 * counts as this run's file only while its lstat matches the CURRENT fstat of the held descriptor: dev/ino, size, and
 * birth time where the held descriptor reports one. Before any move, every held descriptor and every name is checked:
 * any mismatch or lost descriptor keeps every file and reports once.
 * Only a working `/proc/self/fd` traversal of the held fd pins the root. It is probed on every platform; there is no
 * platform-name gate and no `/dev/fd` fallback (fabric-store, #9501; macOS residual smarty-dev#11066, Windows
 * smarty-dev#7858). Without it nothing is removed: the run reports what it kept (reason `no-held-dir-fd`), never a
 * path-based unlink.
 * The cleanup never unlinks or rmdirs. It frees the original names by rename into a fresh 0700 mkdtemp directory.
 * A rename can replace a destination only if another process creates that exact random name inside our private
 * quarantine between mkdtemp and rename. No honest writer does that: the name is unguessable and the directory is new
 * and private. A process that does is a deliberately hostile same-UID process, which can already delete any file in
 * the mesh root directly. Isolating same-UID processes is smarty-dev#11066. Data written by honest writers, including
 * a late replacement at the original name, is never removed. Mismatches are linked back without clobbering; the
 * quarantine link stays too. Operators inspect the reported quarantine and remove it manually when no longer needed.
 */
import fs from "node:fs";
import path from "node:path";
import { randomBytes } from "node:crypto";
import { registerStateFileOwnershipPin } from "./state-gate.js";

const STATE_DB = "state.db";
/** Move order: the sidecars first. A WAL kept without its database could otherwise meet a new state.db. */
const SIDECARS = [`${STATE_DB}-wal`, `${STATE_DB}-shm`];
const PROC_FD = "/proc/self/fd";

/**
 * Stable reasons for kept files: `no-held-dir-fd` (no usable /proc/self/fd traversal), `close-failed` (the database
 * close threw: nothing is moved), `cleanup-failed` (a check or move threw), `not-this-runs-file` (a name no longer
 * names the inode this run created, or existed before it), `quarantined-after-refusal` (this run's files were moved).
 */
export type RetainReason = "no-held-dir-fd" | "close-failed" | "cleanup-failed" | "not-this-runs-file" | "quarantined-after-refusal";
/** `retained` names are root-relative: a state.db name, or `.import-quarantine-*` (and `/<name>`) left in quarantine. */
export interface RefusalCleanupReport { cleanupWarnings: string[]; retained: string[]; reason?: RetainReason }

export interface OwnedImportFiles {
  /** True when this run created state.db (pinned or not). */
  readonly createdDb: boolean;
  /** True when the root is held by a fd that /proc/self/fd traverses: only then can created files be removed. */
  readonly pinned: boolean;
  /** Exclusive create of `name` in the root (0600). True when this run created it; false when it already existed. */
  create(name: string): boolean;
  /** The SQLite sidecars of a state.db this run created, made empty before SQLite opens them (call after the WAL pragma). */
  createSidecars(): void;
  /**
   * After a refusal before the importing commit, with the database connection closed and the fence still held: move
   * the files this run created into quarantine, never delete them. Report what stays (empty when nothing was created).
   */
  removeAfterRefusal(): RefusalCleanupReport;
  /**
   * Fail closed: remove nothing and report the state.db names actually present (listed through the held root when
   * pinned), with `reason` and the cause. Never throws. `close-failed` also keeps every created-file descriptor open
   * until the process exits: `close` then releases only the root.
   */
  keepAfterFailure(reason: "close-failed" | "cleanup-failed", cause: unknown): RefusalCleanupReport;
  /**
   * Release every held created-file descriptor, then the root fd. Idempotent. The caller calls it after it has ended
   * its use of the database. After `keepAfterFailure("close-failed")` it releases only the root fd.
   */
  close(): void;
}

interface Identity { dev: number; ino: number }
/** A created file's descriptor, held from its O_EXCL create through `close`; `fd` is undefined once released. */
interface Held extends Identity {
  fd: number | undefined;
  /** Drops `fd` from the state gate's ownership pins (idempotent); called when this run stops owning the number. */
  unregister: () => void;
}

const messageOf = (error: unknown): string => error instanceof Error ? error.message : String(error);

/** Hold `root` for the fenced section. Never throws: a root it cannot hold is unpinned (nothing will be removed). */
export const pinImportRoot = (root: string): OwnedImportFiles => {
  let fd: number | undefined;
  let rootId: Identity | undefined;
  let pinnedDir: string | undefined;
  try {
    // O_DIRECTORY/O_NOFOLLOW are absent where the platform lacks them; the /proc/self/fd probe then decides.
    fd = fs.openSync(root, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
    const held = fs.fstatSync(fd);
    const named = fs.lstatSync(root);
    if (held.isDirectory() && named.isDirectory() && named.dev === held.dev && named.ino === held.ino) {
      const through = fs.statSync(`${PROC_FD}/${fd}/.`, { throwIfNoEntry: false });
      if (through?.isDirectory() && through.dev === held.dev && through.ino === held.ino) {
        pinnedDir = `${PROC_FD}/${fd}`;
        rootId = { dev: held.dev, ino: held.ino };
      }
    }
  } catch { /* unpinned */ }
  if (pinnedDir === undefined && fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } fd = undefined; }
  const owned = new Map<string, Held>();
  /** Sticky: set by keepAfterFailure("close-failed"); `close` then never closes a created-file descriptor. */
  let keepPins = false;
  /** The held descriptor's CURRENT fstat; throws when it is released, gone, or no longer the inode this run created. */
  const current = (held: Held): fs.Stats => {
    if (held.fd === undefined) throw new Error("this run's descriptor was already released");
    const now = fs.fstatSync(held.fd);
    if (!now.isFile() || now.dev !== held.dev || now.ino !== held.ino) throw new Error(`descriptor ${held.fd} no longer holds the file this run created`);
    return now;
  };
  /**
   * The held CURRENT fstat against the name's CURRENT lstat: same inode, same size, and, when the held birth time is
   * known (> 0), the same birth time (a named 0 then fails). Both unknown (0) is allowed.
   */
  const sameFile = (held: fs.Stats, named: fs.Stats | undefined): boolean => !!named && named.isFile()
    && named.dev === held.dev && named.ino === held.ino && named.size === held.size
    && (!(held.birthtimeMs > 0) || named.birthtimeMs === held.birthtimeMs);
  /** The state.db names present now, through the held root when pinned. Throws when a name cannot be checked. */
  const present = (): string[] => [STATE_DB, ...SIDECARS]
    .filter(name => fs.lstatSync(path.join(pinnedDir ?? root, name), { throwIfNoEntry: false }) !== undefined);
  const files: OwnedImportFiles = {
    get createdDb() { return owned.has(STATE_DB); },
    get pinned() { return pinnedDir !== undefined; },
    create(name) {
      let descriptor: number;
      try {
        descriptor = pinnedDir === undefined
          ? fs.openSync(path.join(root, name), "wx", 0o600)
          : fs.openSync(path.join(pinnedDir, name),
            fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
      } catch (error) {
        // EEXIST: not this run's file, and no descriptor of ours to close.
        if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
        throw error;
      }
      // ponytail: held until close(), pinned or not (libuv opens with FILE_SHARE_READ|WRITE|DELETE on Windows, so SQLite's
      // own open is unaffected). On POSIX, closing any fd of an inode drops this process's fcntl locks on it, so close()
      // runs only after the caller ended its database use. If the database close threw, the connection may still be
      // open: keepAfterFailure("close-failed") then keeps these fds open until the process exits.
      // The state gate's fd proof skips this pin: it is not a parked SQLite descriptor (smarty-dev#9501).
      try {
        const stat = fs.fstatSync(descriptor);
        owned.set(name, { fd: descriptor, dev: stat.dev, ino: stat.ino, unregister: registerStateFileOwnershipPin(descriptor) });
      }
      catch (error) { try { fs.closeSync(descriptor); } catch { /* closed */ } throw error; }
      return true;
    },
    createSidecars() {
      // ponytail: only after the WAL pragma (state.db is then non-empty and SQLite adopts these inodes; a zero-page
      // database would make SQLite delete a -wal it finds). Unpinned roots keep the existing behavior: SQLite creates them.
      if (pinnedDir === undefined || !files.createdDb) return;
      for (const name of SIDECARS) files.create(name);
    },
    removeAfterRefusal() {
      const cleanupWarnings: string[] = [];
      const retained: string[] = [];
      if (!files.createdDb) return { cleanupWarnings, retained };
      if (pinnedDir === undefined) {
        const reason = "no-held-dir-fd";
        for (const name of [STATE_DB, ...SIDECARS]) if (fs.lstatSync(path.join(root, name), { throwIfNoEntry: false })) retained.push(name);
        if (retained.length > 0) {
          cleanupWarnings.push(`kept ${retained.join(", ")} in ${root} after the refusal: no held directory fd (/proc/self/fd) to remove them safely (${reason})`);
        }
        return { cleanupWarnings, retained, reason };
      }
      // Preflight before any move: every held descriptor still holds its inode, and every present name is that inode.
      // One bad name keeps EVERYTHING with one warning, so no sidecar moves first. A throw here (a gone descriptor, a
      // failed lstat) reaches the caller's keepAfterFailure("cleanup-failed"): one warning, nothing moved either.
      const mismatched: string[] = [];
      for (const name of [...SIDECARS, STATE_DB]) {
        const held = owned.get(name);
        const now = held && current(held);
        const named = fs.lstatSync(path.join(pinnedDir!, name), { throwIfNoEntry: false });
        if (named && !(now && sameFile(now, named))) mismatched.push(`${root}/${name} (${held ? "replaced" : "it existed before"})`);
      }
      if (mismatched.length > 0) {
        const kept = present();
        return { cleanupWarnings: [`kept ${kept.length > 0 ? kept.join(", ") : "no state.db files"} in ${root} after the refusal, moved nothing: `
          + `not the file this run created: ${mismatched.join(", ")}`], retained: kept, reason: "not-this-runs-file" };
      }
      let failed = false;
      let foreign = false;
      const removeOne = (name: string): void => {
        const at = path.join(pinnedDir!, name);
        let found: fs.Stats | undefined;
        try { found = fs.lstatSync(at, { throwIfNoEntry: false }); }
        catch (error) { failed = true; retained.push(name); cleanupWarnings.push(`could not check ${root}/${name}: ${messageOf(error)}`); return; }
        if (!found) return; // SQLite's own close removes its sidecars
        const mine = owned.get(name);
        // Per check, the held descriptor's CURRENT fstat; a descriptor gone mid-cleanup counts as not this run's (kept).
        const isMine = (stat: fs.Stats): boolean => { try { return !!mine && sameFile(current(mine), stat); } catch { return false; } };
        if (!isMine(found)) {
          foreign = true;
          retained.push(name);
          cleanupWarnings.push(`kept ${root}/${name}: it is not the file this run created (${mine ? "replaced" : "it existed before"})`);
          return;
        }
        const keep = (kept: string, warning: string): void => { failed = true; retained.push(kept); cleanupWarnings.push(warning); };
        // ponytail: no unlink/rmdir, even after checking the moved inode. Fresh random private destinations protect
        // honest writers; deliberately hostile same-UID writes inside the quarantine require isolation (#11066).
        let slot: string;
        let entry: string;
        // The entry name is drawn before mkdtemp: an RNG failure must not leave an unreported empty quarantine directory.
        try { entry = `${name}-${randomBytes(16).toString("hex")}`; slot = path.basename(fs.mkdtempSync(path.join(pinnedDir!, ".import-quarantine-"))); }
        catch (error) { keep(name, `could not make a quarantine directory in ${root} for ${name}: ${messageOf(error)}`); return; }
        const moved = path.join(pinnedDir!, slot, entry);
        const qname = `${slot}/${entry}`;
        const inspect = `inspect ${root}/${slot}, then remove it manually once its contents are not needed`;
        try { fs.renameSync(at, moved); }
        catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "ENOENT") keep(name, `could not move ${root}/${name} to quarantine: ${messageOf(error)}`);
          retained.push(slot);
          cleanupWarnings.push(`kept the empty quarantine directory ${root}/${slot}: ${inspect}`);
          return;
        }
        retained.push(qname);
        let inSlot: fs.Stats;
        try { inSlot = fs.lstatSync(moved); }
        catch (error) { failed = true; cleanupWarnings.push(`kept ${root}/${qname} (moved from ${name}), could not check it: ${messageOf(error)}; ${inspect}`); return; }
        if (isMine(inSlot)) { cleanupWarnings.push(`moved this run's ${root}/${name} to ${root}/${qname}: ${inspect}`); return; }
        // A foreign entry gets a no-clobber link back; its quarantine link is never dropped.
        foreign = true;
        try { fs.linkSync(moved, at); }
        catch (error) {
          failed = true;
          cleanupWarnings.push(`kept ${root}/${qname}: it replaced ${name} before the move and could not be put back: ${messageOf(error)}; ${inspect}`);
          try { if (fs.lstatSync(at, { throwIfNoEntry: false })) retained.push(name); }
          catch { retained.push(name); }
          return;
        }
        retained.push(name);
        cleanupWarnings.push(`kept ${root}/${name}: it is not the file this run created (replaced); a second link stays at ${root}/${qname}: ${inspect}`);
      };
      for (const name of SIDECARS) removeOne(name);
      // Only sidecars still under their original names block the database move, not quarantined sidecars.
      let stayed: string[];
      try { stayed = SIDECARS.filter(name => fs.lstatSync(path.join(pinnedDir!, name), { throwIfNoEntry: false }) !== undefined); }
      catch (error) { failed = true; stayed = [...SIDECARS]; cleanupWarnings.push(`could not check the sidecars in ${root}: ${messageOf(error)}`); }
      if (stayed.length > 0) {
        retained.push(STATE_DB);
        cleanupWarnings.push(`kept ${root}/${STATE_DB}: ${stayed.join(" and ")} stayed`);
      } else removeOne(STATE_DB);
      if (retained.length === 0) return { cleanupWarnings, retained };
      return { cleanupWarnings, retained, reason: failed ? "cleanup-failed" : foreign ? "not-this-runs-file" : "quarantined-after-refusal" };
    },
    keepAfterFailure(reason, cause) {
      let retained: string[];
      let unlisted = "";
      try { retained = present(); }
      catch (error) {
        // Could not list: report every name this run created (conservative, it may name a file already gone).
        retained = [...SIDECARS, STATE_DB].filter(name => owned.has(name));
        unlisted = `; could not list ${root}: ${messageOf(error)}`;
      }
      const what = retained.length > 0 ? retained.join(", ") : "no state.db files";
      const step = reason === "close-failed" ? "the database close failed" : "the cleanup failed";
      let pins = "";
      if (reason === "close-failed") {
        // ponytail: intentional fd retention, on the failed-close path only. The connection may still be open, and on
        // POSIX closing ANY descriptor of an inode drops every fcntl lock this process holds on it, SQLite's included.
        // So the created-file descriptors stay open until the process exits; the kernel releases them then.
        keepPins = true;
        const held = [...SIDECARS, STATE_DB].filter(name => owned.get(name)?.fd !== undefined).map(name => `${root}/${name}`);
        if (held.length > 0) {
          pins = `; this run's descriptors on ${held.join(", ")} stay open until the process exits (closing one would drop `
            + "this process's POSIX fcntl locks on that inode, which the unclosed connection may hold)";
        }
      }
      return { cleanupWarnings: [`kept ${what} in ${root} after the refusal, removed nothing: ${step} (${reason}): ${messageOf(cause)}${unlisted}${pins}`],
        retained, reason };
    },
    close() {
      // Idempotent; children before the root; every close is tried even when another fails. A number whose fstat no
      // longer shows the captured inode is never closed: it may now be another owner's descriptor (SQLite's), and
      // closing it would break that owner or drop its POSIX locks. Leaking an already-lost descriptor is the safe side.
      const release = (descriptor: number, id: Identity): void => {
        try { const now = fs.fstatSync(descriptor); if (now.dev === id.dev && now.ino === id.ino) fs.closeSync(descriptor); }
        catch { /* gone or already closed */ }
      };
      // After a failed database close the created-file descriptors are kept on purpose (keepAfterFailure).
      if (!keepPins) {
        for (const held of owned.values()) {
          if (held.fd !== undefined) release(held.fd, held);
          held.fd = undefined;
          // Released, or lost to another owner: either way this run no longer owns the number.
          held.unregister();
        }
      }
      if (fd !== undefined && rootId) release(fd, rootId);
      fd = undefined;
      rootId = undefined;
      pinnedDir = undefined;
    },
  };
  return files;
};
