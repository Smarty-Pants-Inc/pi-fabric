import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { debuglog } from "node:util";
import type { FabricWritePolicy } from "../agents/write-guard.js";
import { createHash, randomUUID } from "node:crypto";
import { withExclusiveFileLock } from "../core/atomic-write.js";
import { openPrivateDir, writeArchiveFile } from "../storage/private-dir.js";

export interface FinalAnswerReceipt {
  version: 1;
  id: string;
  runId: string;
  recordedAt: number;
  text: string;
}

export const MAX_FINAL_ANSWER_BYTES = 8 * 1024 * 1024;
export class FabricTaskControlDirectoryError extends Error {
  override readonly name = "FabricTaskControlDirectoryError";
  readonly code = "FABRIC_TASK_CONTROL_UNSAFE";
}

/** Current layout marker written by hosts into run status (advisory downgrade fence only). */
export const TASK_CONTROL_LAYOUT = "central-v1" as const;
/** Old layout: `<run>.ctl` sibling. Read-only compatibility. */
export const legacyTaskControlSibling = (runDirectory: string): string => path.resolve(runDirectory) + ".ctl";

const inside = (root: string, target: string): boolean => target === root || target.startsWith(root.endsWith(path.sep) ? root : root + path.sep);
/** Physical path of a possibly not-yet-existing target: realpath of its deepest existing ancestor. */
const physicalPath = (value: string): string => {
  const resolved = path.resolve(value);
  let ancestor = resolved;
  for (;;) {
    try { fs.lstatSync(ancestor); break; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    const parent = path.dirname(ancestor);
    if (parent === ancestor) throw new FabricTaskControlDirectoryError("Cannot resolve task control path");
    ancestor = parent;
  }
  return path.join(fs.realpathSync(ancestor), path.relative(ancestor, resolved));
};
const temporaryRoots = (): string[] => [os.tmpdir(), process.env.TMPDIR, process.env.TMP, process.env.TEMP, "/tmp", "/var/tmp", "/dev/shm"]
  .filter((value): value is string => typeof value === "string" && path.isAbsolute(value))
  .flatMap(value => { try { return [path.resolve(value), physicalPath(value)]; } catch { return [path.resolve(value)]; } });

/**
 * Hardened ancestry: every existing component from `/` to `target` is a real directory
 * (never a link alias), owned by this uid or root, and not group/other-writable unless
 * it is a root-owned sticky directory strictly above the state home. Components at or
 * below `privateFrom` must be owned by this uid and exactly private.
 */
const assertOwnedAncestry = (target: string, privateFrom: string): void => {
  if (process.platform === "win32") return;
  const uid = process.getuid?.();
  const parts = path.resolve(target).split(path.sep).filter(Boolean);
  let current: string = path.sep;
  for (let index = 0; index <= parts.length; index++) {
    if (index > 0) current = path.join(current, parts[index - 1]!);
    let stat: fs.Stats;
    try { stat = fs.lstatSync(current); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
    if (stat.isSymbolicLink() || !stat.isDirectory()) throw new FabricTaskControlDirectoryError(`Task control root ancestor ${current} must be a real directory, not a link alias`);
    const owned = uid === undefined || stat.uid === uid;
    if (!owned && stat.uid !== 0) throw new FabricTaskControlDirectoryError(`Task control root ancestor ${current} has a foreign owner`);
    if (inside(privateFrom, current)) {
      if (!owned || (stat.mode & 0o077) !== 0) throw new FabricTaskControlDirectoryError(`Task control root ${current} must be an owned private directory`);
    } else if ((stat.mode & 0o022) !== 0 && !(stat.uid === 0 && (stat.mode & 0o1000) !== 0)) {
      throw new FabricTaskControlDirectoryError(`Task control root ancestor ${current} is writable by other users`);
    }
  }
};
const stateHome = (): string => {
  const configured = process.env.XDG_STATE_HOME;
  // XDG requires an absolute path; a relative value is ignored per the spec.
  return configured && path.isAbsolute(configured) ? path.resolve(configured) : path.join(os.homedir(), ".local", "state");
};
/**
 * Stable contract: the central per-user Fabric task-control root. It is outside any
 * temporary directory (os.tmpdir, caller TMPDIR/TMP/TEMP, /tmp, /var/tmp, /dev/shm),
 * has no symlink alias and hardened owned ancestry. Task write grants overlapping it
 * are refused separately by assertTaskControlWriteRoots. Pure: never creates.
 */
export const taskControlRoot = (): string => {
  const home = stateHome(), root = path.join(home, "pi-fabric", "task-control");
  const physical = physicalPath(root);
  if (physical !== root) throw new FabricTaskControlDirectoryError(`Task control root ${root} must not traverse a link alias`);
  for (const temporary of temporaryRoots()) {
    if (inside(temporary, root) || inside(temporary, physical)) {
      throw new FabricTaskControlDirectoryError(`Task control root ${root} must not live inside temporary directory ${temporary}; set XDG_STATE_HOME outside temp`);
    }
  }
  assertOwnedAncestry(root, path.join(home, "pi-fabric"));
  return root;
};
/** Host-only: create the central root (0700, owned) under a validated state home. */
export const ensureTaskControlRoot = (): string => {
  const root = taskControlRoot();
  // Every missing component (an arbitrarily deep absolute XDG_STATE_HOME) is created
  // 0700; existing ancestry was validated above and is re-validated below.
  fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  // Re-validate after creation: a concurrent alias or foreign directory still refuses.
  if (taskControlRoot() !== root) throw new FabricTaskControlDirectoryError("Task control root changed during creation");
  return root;
};
/**
 * Stable contract: the central control destination for a run. Deterministic: the
 * run id (basename) plus a digest of the run's physical path, so archives that reuse
 * a run id under another path never collide with the live run's controls.
 */
export const taskControlDirectory = (runDirectory: string): string => {
  const physical = physicalPath(runDirectory);
  const id = path.basename(physical).replace(/[^A-Za-z0-9._-]/g, "_").replace(/^\.+/, "_").slice(0, 96) || "_";
  const digest = createHash("sha256").update(physical).digest("hex").slice(0, 32);
  return path.join(taskControlRoot(), `${id}--${digest}`);
};
export const taskSteerFile = (runDirectory: string): string => path.join(taskControlDirectory(runDirectory), "steer.jsonl");

const withDirectory = <T>(directory: string, privateMode: boolean, operation: (directory: string, fd: number) => T): T => {
  const before = fs.lstatSync(directory);
  if (before.isSymbolicLink() || fs.realpathSync(directory) !== path.resolve(directory)) throw new FabricTaskControlDirectoryError("Task control directory must not traverse a link");
  const fd = fs.openSync(directory, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isDirectory() || stat.dev !== before.dev || stat.ino !== before.ino || (process.getuid && stat.uid !== process.getuid()) ||
        (process.platform !== "win32" && (stat.mode & (privateMode ? 0o077 : 0o022)) !== 0)) {
      throw new FabricTaskControlDirectoryError("Task control directory must be an owned private directory");
    }
    // Paths through a held fd are synchronous-only; never retain one for an ACK.
    return operation(process.platform === "linux" ? `/proc/self/fd/${fd}` : directory, fd);
  } finally { fs.closeSync(fd); }
};
/** Strict WRITE/lock namespace. A reader never creates or repairs this sibling. */
export const withTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T =>
  withDirectory(taskControlDirectory(runDirectory), true, operation);

/** Check every intermediate directory too, not only the no-follow file leaf. */
const withControlPath = <T>(directory: string, relative: string, create: boolean, operation: (file: string, parent: string, fd: number) => T): T => {
  const parts = relative.split(/[\\/]/);
  if (path.isAbsolute(relative) || parts.some(part => !part || part === "." || part === "..")) throw new FabricTaskControlDirectoryError("Invalid task control relative path");
  const walk = (parent: string, index: number): T => {
    const fd = fs.openSync(index === 0 ? parent + path.sep + "." : parent, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
    try {
      const stat = fs.fstatSync(fd);
      if (!stat.isDirectory() || (process.getuid && stat.uid !== process.getuid()) || (process.platform !== "win32" && (stat.mode & 0o022))) {
        throw new FabricTaskControlDirectoryError("Unsafe task control subdirectory");
      }
      const pinned = process.platform === "linux" ? `/proc/self/fd/${fd}` : parent;
      if (index === parts.length - 1) return operation(path.join(pinned, parts[index]!), pinned, fd);
      const child = path.join(pinned, parts[index]!);
      if (create) {
        try { fs.mkdirSync(child, { mode: 0o700 }); fs.chmodSync(child, 0o700); if (process.platform !== "win32") fs.fsyncSync(fd); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      }
      return walk(child, index + 1);
    } finally { fs.closeSync(fd); }
  };
  return walk(directory, 0);
};
const entryExists = (entry: string): boolean => {
  try { fs.lstatSync(entry); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
};
const centralExists = (runDirectory: string): boolean => entryExists(taskControlDirectory(runDirectory));
const oldSiblingExists = (runDirectory: string): boolean => entryExists(legacyTaskControlSibling(runDirectory));
/** Old sibling-v1 controls: validated private, read-only (never a write/lock namespace). */
const withOldSibling = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T =>
  withDirectory(legacyTaskControlSibling(runDirectory), true, operation);
const runLayout = (runDirectory: string): unknown => {
  try {
    const value = readTaskRunStatus<Record<string, unknown>>(runDirectory);
    return value && typeof value === "object" && !Array.isArray(value) ? value.taskControlLayout : undefined;
  } catch { return undefined; }
};
export const readTaskRunStatus = <T>(runDirectory: string): T => withDirectory(path.resolve(runDirectory), false,
  directory => JSON.parse(readBoundedControlFile(path.join(directory, "status.json"), 1024 * 1024)) as T);
/** Status is advisory: it may EXCLUDE old runs, never authorize fallback under a sibling. */
const legacyRecord = (runDirectory: string): boolean => {
  const value = readTaskRunStatus<Record<string, unknown>>(runDirectory);
  // Old retention records may omit route identity, and archives keep the
  // original id under an alias. This selects READ compatibility only; native
  // exit/identity predicates still decide collection. A layout field always
  // excludes fallback, and an existing sibling requires host-owned authority.
  return !!value && typeof value === "object" && !Array.isArray(value) &&
    ["running", "completed", "failed", "stopped", "timed_out"].includes(String(value.status)) && !("taskControlLayout" in value);
};
const legacyIdentity = (runDirectory: string): { version: 1; runId: string; dev: string; ino: string } => {
  const stat = fs.lstatSync(runDirectory, { bigint: true });
  return { version: 1, runId: path.basename(runDirectory), dev: String(stat.dev), ino: String(stat.ino) };
};
const hasLegacyAuthority = (runDirectory: string, directory: string): boolean => {
  try {
    const value = JSON.parse(readBoundedControlFile(path.join(directory, ".legacy-read.json"), MAX_TASK_CONTROL_BYTES));
    return JSON.stringify(value) === JSON.stringify(legacyIdentity(runDirectory));
  } catch { return false; }
};
/** Exported selection for retention. Validate even when the selected file is missing. */
/**
 * Read selection, central first. Then, only for runs NOT marked central-v1, a
 * validated old sibling-v1 directory, then the in-run legacy layout. Fallback is
 * read-only and never authorizes writes; a marked run with missing controls refuses.
 */
export const taskControlReadDirectory = (runDirectory: string): string => {
  if (centralExists(runDirectory)) return withTaskControlDirectory(runDirectory, () => taskControlDirectory(runDirectory));
  if (runLayout(runDirectory) !== TASK_CONTROL_LAYOUT && oldSiblingExists(runDirectory)) {
    return withOldSibling(runDirectory, () => legacyTaskControlSibling(runDirectory));
  }
  if (legacyRecord(runDirectory)) return path.resolve(runDirectory);
  throw new FabricTaskControlDirectoryError("Missing protected task control directory; legacy fallback refused");
};
export const withTaskControlReadDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T => {
  const directory = taskControlReadDirectory(runDirectory);
  return withDirectory(directory, directory !== path.resolve(runDirectory), operation);
};
/**
 * Read-only legacy source for a run whose central controls were bootstrapped from an
 * old layout: the validated old sibling if present, else the in-run directory.
 * Authority comes from the host-written central `.legacy-read.json`, never status.
 */
type LegacySource = { directory: string; privateMode: boolean };
/**
 * Ordered read-only legacy sources, NEWEST first (overlay order after the selected
 * directory): old sibling-v1, then in-run. Each step needs host-written inode-bound
 * `.legacy-read.json` authority from the layer above it; status never authorizes it.
 */
const legacySources = (runDirectory: string): LegacySource[] => {
  const run = path.resolve(runDirectory);
  const sibling = { directory: legacyTaskControlSibling(run), privateMode: true }, inRun = { directory: run, privateMode: false };
  const siblingAdmitsInRun = (): boolean => withOldSibling(run, directory => hasLegacyAuthority(run, directory));
  if (centralExists(run)) {
    if (!withTaskControlDirectory(run, directory => hasLegacyAuthority(run, directory))) return [];
    // Bootstrapped from sibling-v1: the old sibling, plus the in-run rows it was itself
    // bootstrapped over (tri-layout); else bootstrapped directly from in-run legacy.
    if (!oldSiblingExists(run)) return [inRun];
    return siblingAdmitsInRun() ? [sibling, inRun] : [sibling];
  }
  if (runLayout(run) === TASK_CONTROL_LAYOUT) return [];
  // Un-bootstrapped old sibling is the primary read source; its own host-written
  // legacy binding (old sibling-over-legacy bootstrap) still admits in-run rows.
  if (oldSiblingExists(run)) return siblingAdmitsInRun() ? [inRun] : [];
  return legacyRecord(run) ? [inRun] : [];
};
/** Shared ordered-source helper: hold every eligible legacy source pinned at once (newest first). */
export const withLegacyTaskControlDirectories = <T>(runDirectory: string, operation: (sources: Array<{ directory: string; fd: number }>) => T): T => {
  const sources = legacySources(runDirectory);
  const hold = (index: number, held: Array<{ directory: string; fd: number }>): T => index === sources.length ? operation(held)
    : withDirectory(sources[index]!.directory, sources[index]!.privateMode, (directory, fd) => hold(index + 1, [...held, { directory, fd }]));
  return hold(0, []);
};
/** Visit every eligible legacy source in order; returns one result per visited source. */
export const visitLegacyTaskControlDirectories = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T[] =>
  withLegacyTaskControlDirectories(runDirectory, sources => sources.map(source => operation(source.directory, source.fd)));
/** First source where `operation` does not raise ENOENT (single-file lookups). */
const firstLegacy = <T>(runDirectory: string, operation: (directory: string) => T): T | undefined =>
  withLegacyTaskControlDirectories(runDirectory, sources => {
    let missing: unknown;
    for (const source of sources) {
      try { return operation(source.directory); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; missing ??= error; }
    }
    if (missing) throw missing;
    return undefined;
  });
/** Compatibility: the newest legacy source only. Prefer the ordered visitor. */
export const withLegacyTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T | undefined =>
  withLegacyTaskControlDirectories(runDirectory, sources => sources.length ? operation(sources[0]!.directory, sources[0]!.fd) : undefined);
const createCentral = (runDirectory: string, legacy: boolean): void => {
  ensureTaskControlRoot();
  const control = taskControlDirectory(runDirectory), parent = openPrivateDir(path.dirname(control), path.dirname(control));
  try {
    const child = path.join(parent.path, path.basename(control));
    let created = false;
    try { fs.mkdirSync(child, { mode: 0o700 }); created = true; fs.chmodSync(child, 0o700); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
    withTaskControlDirectory(runDirectory, (_directory, fd) => { if (process.platform !== "win32") fs.fsyncSync(fd); });
    if (created && legacy) writeTaskControlJson(runDirectory, ".legacy-read.json", legacyIdentity(runDirectory));
    if (parent.fd !== undefined) fs.fsyncSync(parent.fd);
  } finally { parent.close(); }
};
/**
 * Host bootstrap only, into the central namespace. Never recreate missing controls for
 * a central-v1-marked run, nor for a sibling-v1 run whose old sibling is missing.
 * An unmarked legacy run or an intact sibling-v1 run gains central controls plus a
 * host-owned `.legacy-read.json` binding so its old rows stay readable.
 */
export const initializeTaskControlDirectory = (runDirectory: string): void => {
  if (centralExists(runDirectory)) { withTaskControlDirectory(runDirectory, () => undefined); return; }
  let legacy = false;
  try {
    const layout = readTaskRunStatus<Record<string, unknown>>(runDirectory)?.taskControlLayout;
    if (layout === "sibling-v1" && oldSiblingExists(runDirectory)) { withOldSibling(runDirectory, () => undefined); legacy = true; }
    else {
      legacy = legacyRecord(runDirectory);
      if (!legacy) throw new FabricTaskControlDirectoryError("Missing protected task control directory; refusing recreation");
    }
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  createCentral(runDirectory, legacy);
};
/**
 * Archive publication ONLY: create (0700) or validate existing central controls for an
 * archive destination, ignoring its copied (possibly central-v1-marked) status. The
 * archive path identity differs from the live run's, so this never revives or aliases
 * a live run's missing controls. Live runs must use initializeTaskControlDirectory.
 */
export const createArchiveTaskControlDirectory = (archiveRunDirectory: string): void => createCentral(archiveRunDirectory, false);
/** Atomic publication stays on the held namespace; no realpath escape or /proc ancestor fsync. */
export const writeTaskControlJson = (runDirectory: string, relative: string, value: unknown): void => withTaskControlDirectory(runDirectory, directory =>
  withControlPath(directory, relative, true, (_file, parent, fd) => {
    const bytes = Buffer.from(JSON.stringify(value));
    if (bytes.length > MAX_TASK_CONTROL_BYTES) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
    if (!writeArchiveFile({ path: parent, fd, close() {} }, path.basename(relative), { bytes }, true)) {
      throw new FabricTaskControlDirectoryError("Refusing replacement of unsafe task control file");
    }
  }));

/** Cleanup never mutates a legacy source and never follows a linked subdirectory. */
export const removeTaskControlFile = (runDirectory: string, relative: string): void => withTaskControlDirectory(runDirectory, directory => {
  try { withControlPath(directory, relative, false, file => {
    const fd = openTaskControlFile(file); fs.closeSync(fd);
    fs.rmSync(file);
  }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
});

/**
 * Compatibility filter ONLY for validated old sibling-v1 `.ctl` entries in run trees.
 * Central controls never appear in run trees, so this never reports a central entry.
 */
export const isTaskControlDirectoryEntry = (root: string, name: string): boolean => {
  if (!name.endsWith(".ctl")) return false;
  if (name.slice(0, -4).endsWith(".ctl")) throw new FabricTaskControlDirectoryError("A task control directory cannot admit another control sibling");
  const run = path.join(root, name.slice(0, -4));
  withDirectory(path.resolve(run), false, () => undefined);
  withOldSibling(run, () => undefined);
  return true;
};

const taskControlDebug = debuglog("pi-fabric:task-control");
let unconfinedOverlapLogged = false;
/**
 * Refuse any task write grant that overlaps the central root: a grant on the root or
 * any rename-capable ancestor (including the state home) or anything inside it (own
 * or another run's, e.g. a parent's controls granted to a child, SEC01).
 */
export const assertTaskControlWriteRoots = (runDirectory: string, cwd: string, policy?: FabricWritePolicy): void => {
  const physical = physicalPath;
  const control = physical(taskControlRoot()), parent = path.dirname(control);
  void taskControlDirectory(runDirectory); // validates run identity resolution
  const roots = [...(policy?.readOnly ? [] : policy?.writableRoots ?? []), cwd, runDirectory, path.join(runDirectory, "tmp"),
    ...[".cache", ".npm", ".bun"].map(name => path.join(os.homedir(), name)), "/dev/null", "/dev/tty", "/dev/pts"];
  try { roots.push(execFileSync("git", ["rev-parse", "--path-format=absolute", "--git-common-dir"], { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim()); }
  catch { /* A non-Git cwd has no common-Git write grant. */ }
  for (const root of roots) {
    const writable = physical(root);
    if (inside(writable, parent) || inside(control, writable)) {
      if (!policy) {
        // Preserve reviewed #11308: undefined policy is unconfined same-uid
        // authority, not an implicit write policy. Relocation cannot isolate it.
        if (!unconfinedOverlapLogged) {
          unconfinedOverlapLogged = true;
          taskControlDebug(`unconfined task control directory overlaps ${root}; accepted residual smarty-dev#11308`);
        }
        return;
      }
      throw new FabricTaskControlDirectoryError(
        `Task control root ${control} or its rename-capable parent overlaps writable root ${root}; task write grants must not cover the Fabric state control root`);
    }
  }
};

export const MAX_TASK_CONTROL_BYTES = 64 * 1024;
export const MAX_TASK_CONTROL_ROWS = 256;
/** Shared journal/receipt/registry validation; never open a FIFO in blocking mode. */
export const taskControlFileStat = (fd: number): fs.BigIntStats => {
  const stat = fs.fstatSync(fd, { bigint: true });
  if (!stat.isFile() || stat.nlink !== 1n || (process.getuid && stat.uid !== BigInt(process.getuid()))) {
    throw new FabricTaskControlDirectoryError("Task control file must be an owned regular file with one link");
  }
  return stat;
};
export const openTaskControlFile = (file: string): number => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0));
  try { taskControlFileStat(fd); return fd; }
  catch (error) { fs.closeSync(fd); throw error; }
};
const readBoundedControlFile = (file: string, maxBytes: number): string => {
  const fd = openTaskControlFile(file);
  try {
    if (taskControlFileStat(fd).size > BigInt(maxBytes)) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
    // A regular file can grow after fstat. Read at most the bound plus one byte,
    // not readFileSync(fd), which follows an attacker-controlled growing EOF.
    const chunks: Buffer[] = [], buffer = Buffer.alloc(16 * 1024);
    let bytes = 0;
    for (;;) {
      const length = fs.readSync(fd, buffer, 0, Math.min(buffer.length, maxBytes + 1 - bytes), bytes);
      if (!length) break;
      bytes += length;
      if (bytes > maxBytes) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
      chunks.push(Buffer.from(buffer.subarray(0, length)));
    }
    return Buffer.concat(chunks, bytes).toString("utf8");
  } finally { fs.closeSync(fd); }
};
export const withTaskControlSubdirectory = <T>(directory: string, relative: string, operation: (directory: string, fd: number) => T): T =>
  withControlPath(directory, path.join(relative, ".entry"), false, (_file, parent, fd) => operation(parent, fd));
/** The returned FILE fd remains valid after the directory callback closes; no fd path escapes. */
export const openLegacyTaskControlFile = (runDirectory: string, relative: string): number | undefined =>
  firstLegacy(runDirectory, directory => withControlPath(directory, relative, false, openTaskControlFile));
/** Every legacy source's copy of `relative` (newest first); missing copies are skipped. */
export const openLegacyTaskControlFiles = (runDirectory: string, relative: string): number[] => {
  // A later source may fail (unsafe/FIFO/etc.) after earlier fds opened: close them
  // all before rethrowing; on success every fd is owned by the caller.
  const fds: number[] = [];
  try {
    withLegacyTaskControlDirectories(runDirectory, sources => {
      for (const source of sources) {
        try { fds.push(withControlPath(source.directory, relative, false, openTaskControlFile)); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      }
    });
    return fds;
  } catch (error) {
    for (const fd of fds) { try { fs.closeSync(fd); } catch { /* best-effort cleanup */ } }
    throw error;
  }
};
export const openTaskControlReadFile = (runDirectory: string, relative: string): number => {
  const open = (directory: string): number => withControlPath(directory, relative, false, openTaskControlFile);
  try { return withTaskControlReadDirectory(runDirectory, open); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    const legacy = firstLegacy(runDirectory, open);
    if (legacy !== undefined) return legacy;
    throw error;
  }
};
export const readTaskControlJsonAt = <T>(directory: string, relative: string, maxBytes = MAX_TASK_CONTROL_BYTES): T | undefined => {
  try { return JSON.parse(withControlPath(directory, relative, false, file => readBoundedControlFile(file, maxBytes))) as T; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return; throw error; }
};
export const readTaskControlFile = (runDirectory: string, relative: string, maxBytes = MAX_TASK_CONTROL_BYTES): string | undefined => {
  const read = (directory: string): string => withControlPath(directory, relative, false, file => readBoundedControlFile(file, maxBytes));
  try { return withTaskControlReadDirectory(runDirectory, read); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    try { return firstLegacy(runDirectory, read); }
    catch (legacyError) { if ((legacyError as NodeJS.ErrnoException).code === "ENOENT") return; throw legacyError; }
  }
};
export const readTaskControlJson = <T>(runDirectory: string, relative: string, maxBytes = MAX_TASK_CONTROL_BYTES): T | undefined => {
  const contents = readTaskControlFile(runDirectory, relative, maxBytes);
  return contents === undefined ? undefined : JSON.parse(contents) as T;
};
const readReceipt = (directory: string, runId: string, legacyRun?: string): FinalAnswerReceipt | undefined => {
  let value: FinalAnswerReceipt | undefined;
  try {
    const parsed = JSON.parse(readBoundedControlFile(path.join(directory, "final-answer.json"), MAX_FINAL_ANSWER_BYTES));
    if (parsed?.version === 1 && parsed.runId === runId && typeof parsed.id === "string" && parsed.id.length > 0 &&
        Number.isSafeInteger(parsed.recordedAt) && parsed.recordedAt > 0 && typeof parsed.text === "string") value = parsed;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT" && legacyRun && hasLegacyAuthority(legacyRun, directory)) {
      // Exactly one legacy result may establish finality; conflicting copies never do.
      return withLegacyTaskControlDirectories(legacyRun, sources => {
        const present = legacyAnswerDirectories(sources);
        return present.length === 1 ? readReceipt(present[0]!, runId) : undefined;
      });
    }
    /* Missing or invalid bounded receipts never establish finality. */
  }
  // Central answer plus ANY eligible legacy answer is a duplicate: never final.
  // Checked outside the try so a legacy-source ENOENT cannot reach the fallback.
  if (value && legacyRun && withLegacyTaskControlDirectories(legacyRun, sources => legacyAnswerDirectories(sources).length) > 0) return undefined;
  return value;
};
/** Distinct directories (by dev/ino, so selected in-run == legacy in-run counts once) holding an answer. */
const answerSourceCount = (runDirectory: string): number => {
  const keys = new Set<string>();
  const add = (directory: string, fd: number): void => {
    if (!fs.lstatSync(path.join(directory, "final-answer.json"), { throwIfNoEntry: false })) return;
    const stat = fs.fstatSync(fd, { bigint: true });
    keys.add(`${stat.dev}:${stat.ino}`);
  };
  withTaskControlReadDirectory(runDirectory, add);
  withLegacyTaskControlDirectories(runDirectory, sources => { for (const source of sources) add(source.directory, source.fd); });
  return keys.size;
};
export const readFinalAnswerReceipt = (runDirectory: string, runId: string): FinalAnswerReceipt | undefined => {
  try {
    const value = readTaskControlJson<FinalAnswerReceipt>(runDirectory, "final-answer.json", MAX_FINAL_ANSWER_BYTES);
    if (value?.version === 1 && value.runId === runId && typeof value.id === "string" && value.id.length > 0 &&
        Number.isSafeInteger(value.recordedAt) && value.recordedAt > 0 && typeof value.text === "string" &&
        // Exactly ONE distinct source (selected + eligible legacy layers) may hold an
        // answer; central+legacy or sibling+in-run duplicates never establish finality.
        answerSourceCount(runDirectory) === 1) return value;
  } catch (error) {
    if (error instanceof FabricTaskControlDirectoryError && !/byte bound|regular file/.test(error.message)) throw error;
    // Malformed/unsafe receipts never establish finality or authorize fallback.
  }
  return undefined;
};

/** Pinned legacy sources (all layers) that hold a final-answer entry. */
const legacyAnswerDirectories = (sources: Array<{ directory: string }>): string[] => sources
  .filter(source => !!fs.lstatSync(path.join(source.directory, "final-answer.json"), { throwIfNoEntry: false }))
  .map(source => source.directory);
const locked = <T>(directory: string, operation: () => T): T => withExclusiveFileLock({
  directory, lockName: ".task-final-answer.lock", attempts: 200, delayMs: 5,
  timeoutMessage: "Task final-answer publication/injection lock timed out",
}, operation);
/** Reader and lock use the SAME pinned host-owned directory. */
export const withTaskFinalAnswerLock = <T>(runDirectory: string, operation: (read: (runId: string) => FinalAnswerReceipt | undefined) => T): T =>
  withTaskControlDirectory(runDirectory, directory => locked(directory, () => operation(runId => readReceipt(directory, runId, runDirectory))));

/** Serialize, size-check and fsync staging BEFORE the publication/injection lock. */
export const saveFinalAnswerReceipt = (runDirectory: string, runId: string, text: string): FinalAnswerReceipt => {
  if (Buffer.byteLength(text) > MAX_FINAL_ANSWER_BYTES) throw new RangeError("Final answer exceeds the 8 MiB receipt bound");
  const receipt: FinalAnswerReceipt = { version: 1, id: randomUUID(), runId, recordedAt: Date.now(), text };
  const contents = JSON.stringify(receipt);
  if (Buffer.byteLength(contents) > MAX_FINAL_ANSWER_BYTES) throw new RangeError("Final answer exceeds the 8 MiB receipt bound");
  return withTaskControlDirectory(runDirectory, (directory, directoryFd) => {
    const temporary = path.join(directory, `.final-answer.${process.pid}.${receipt.id}.tmp`);
    const fd = fs.openSync(temporary, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeFileSync(fd, contents); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try {
      const published = locked(directory, () => {
        const file = path.join(directory, "final-answer.json");
        // Never replace an existing result in ANY legacy layer (old sibling and in-run).
        if (fs.lstatSync(file, { throwIfNoEntry: false }) || (hasLegacyAuthority(runDirectory, directory) &&
            withLegacyTaskControlDirectories(runDirectory, sources => legacyAnswerDirectories(sources).length > 0))) return false;
        fs.renameSync(temporary, file); // no serialization, payload read or fsync under this lock
        return true;
      });
      if (process.platform !== "win32") fs.fsyncSync(directoryFd);
      const result = published ? receipt : readReceipt(directory, runId, runDirectory);
      if (!result) throw new Error("Invalid or mismatched final-answer receipt already exists");
      return result;
    } finally { fs.rmSync(temporary, { force: true }); }
  });
};

/** Text alongside tool calls is progress, not a final answer; truncation/retry isn't success. */
export const finalAssistantText = (message: unknown): string | undefined => {
  if (!message || typeof message !== "object") return undefined;
  const value = message as { role?: unknown; stopReason?: unknown; content?: unknown };
  if (value.role !== "assistant" || value.stopReason !== "stop" || !Array.isArray(value.content) ||
      value.content.some(block => block?.type === "toolCall")) return undefined;
  return value.content.filter(block => block?.type === "text" && typeof block.text === "string")
    .map(block => block.text).join("");
};
