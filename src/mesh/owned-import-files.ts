/**
 * smarty-dev#9501: the files an import or cutover run creates in the mesh root, and their removal when the switch is
 * refused before its importing commit. The root is held by a no-follow directory fd for the whole fenced section.
 * Every create and every removal goes through `/proc/self/fd/<fd>/<name>`, so renaming or swapping the root path
 * cannot redirect either one. A file counts as this run's only when this run created it with O_EXCL, and its dev/ino
 * comes from an fstat of that very descriptor. A name counts as this run's file only while its lstat still shows that
 * inode.
 * Only a working `/proc/self/fd` traversal of the held fd pins the root. It is probed on every platform; there is no
 * platform-name gate and no `/dev/fd` fallback (fabric-store, #9501; macOS residual smarty-dev#11066, Windows
 * smarty-dev#7858). Without it nothing is removed: the run reports what it kept (reason `no-held-dir-fd`), never a
 * path-based unlink.
 */
import fs from "node:fs";
import path from "node:path";

const STATE_DB = "state.db";
/** Removal order: the sidecars first. A WAL kept without its database could otherwise meet a new state.db. */
const SIDECARS = [`${STATE_DB}-wal`, `${STATE_DB}-shm`];
const PROC_FD = "/proc/self/fd";

/**
 * Stable reasons for kept files: `no-held-dir-fd` (no usable /proc/self/fd traversal), `close-failed` (the database
 * close threw: nothing is removed), `cleanup-failed` (a check or removal threw), `not-this-runs-file` (a name no longer
 * names the inode this run created, or existed before it).
 */
export type RetainReason = "no-held-dir-fd" | "close-failed" | "cleanup-failed" | "not-this-runs-file";
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
   * After a refusal before the importing commit, with the database connection closed and the fence still held: remove
   * the files this run created whose names still name them. Returns what to report (empty when nothing was created).
   */
  removeAfterRefusal(): RefusalCleanupReport;
  /**
   * Fail closed: remove nothing and report the state.db names actually present (listed through the held root when
   * pinned), with `reason` and the cause. Never throws.
   */
  keepAfterFailure(reason: "close-failed" | "cleanup-failed", cause: unknown): RefusalCleanupReport;
  close(): void;
}

interface Identity { dev: number; ino: number }

const messageOf = (error: unknown): string => error instanceof Error ? error.message : String(error);

/** Hold `root` for the fenced section. Never throws: a root it cannot hold is unpinned (nothing will be removed). */
export const pinImportRoot = (root: string): OwnedImportFiles => {
  let fd: number | undefined;
  let pinnedDir: string | undefined;
  try {
    // O_DIRECTORY/O_NOFOLLOW are absent where the platform lacks them; the /proc/self/fd probe then decides.
    fd = fs.openSync(root, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
    const held = fs.fstatSync(fd);
    const named = fs.lstatSync(root);
    if (held.isDirectory() && named.isDirectory() && named.dev === held.dev && named.ino === held.ino) {
      const through = fs.statSync(`${PROC_FD}/${fd}/.`, { throwIfNoEntry: false });
      if (through?.isDirectory() && through.dev === held.dev && through.ino === held.ino) pinnedDir = `${PROC_FD}/${fd}`;
    }
  } catch { /* unpinned */ }
  if (pinnedDir === undefined && fd !== undefined) { try { fs.closeSync(fd); } catch { /* closed */ } fd = undefined; }
  const owned = new Map<string, Identity | undefined>();
  /** The state.db names present now, through the held root when pinned. Throws when a name cannot be checked. */
  const present = (): string[] => [STATE_DB, ...SIDECARS]
    .filter(name => fs.lstatSync(path.join(pinnedDir ?? root, name), { throwIfNoEntry: false }) !== undefined);
  const files: OwnedImportFiles = {
    get createdDb() { return owned.has(STATE_DB); },
    get pinned() { return pinnedDir !== undefined; },
    create(name) {
      try {
        if (pinnedDir === undefined) {
          fs.closeSync(fs.openSync(path.join(root, name), "wx", 0o600));
          owned.set(name, undefined);
          return true;
        }
        const descriptor = fs.openSync(path.join(pinnedDir, name),
          fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW, 0o600);
        try {
          const stat = fs.fstatSync(descriptor);
          owned.set(name, { dev: stat.dev, ino: stat.ino });
        } finally { fs.closeSync(descriptor); }
        return true;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
        throw error;
      }
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
      let failed = false;
      const removeOne = (name: string): void => {
        const at = path.join(pinnedDir!, name);
        let found: fs.Stats | undefined;
        try { found = fs.lstatSync(at, { throwIfNoEntry: false }); }
        catch (error) { failed = true; retained.push(name); cleanupWarnings.push(`could not check ${root}/${name}: ${messageOf(error)}`); return; }
        if (!found) return; // SQLite's own close removes its sidecars
        const mine = owned.get(name);
        if (!mine || !found.isFile() || found.dev !== mine.dev || found.ino !== mine.ino) {
          retained.push(name);
          cleanupWarnings.push(`kept ${root}/${name}: it is not the file this run created (${mine ? "replaced" : "it existed before"})`);
          return;
        }
        try { fs.unlinkSync(at); }
        catch (error) { failed = true; retained.push(name); cleanupWarnings.push(`could not remove ${root}/${name}: ${messageOf(error)}`); }
      };
      for (const name of SIDECARS) removeOne(name);
      if (retained.length > 0) {
        retained.push(STATE_DB);
        cleanupWarnings.push(`kept ${root}/${STATE_DB}: its ${SIDECARS.filter(name => retained.includes(name)).join(" and ")} stayed`);
      } else removeOne(STATE_DB);
      if (retained.length === 0) return { cleanupWarnings, retained };
      return { cleanupWarnings, retained, reason: failed ? "cleanup-failed" : "not-this-runs-file" };
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
      return { cleanupWarnings: [`kept ${what} in ${root} after the refusal, removed nothing: ${step} (${reason}): ${messageOf(cause)}${unlisted}`],
        retained, reason };
    },
    close() {
      if (fd === undefined) return;
      try { fs.closeSync(fd); } catch { /* closed */ }
      fd = undefined;
      pinnedDir = undefined;
    },
  };
  return files;
};
