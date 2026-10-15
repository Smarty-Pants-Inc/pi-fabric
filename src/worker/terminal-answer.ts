import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execFileSync } from "node:child_process";
import { debuglog } from "node:util";
import type { FabricWritePolicy } from "../agents/write-guard.js";
import { createHash, randomUUID } from "node:crypto";
import { withExclusiveFileLock } from "../core/atomic-write.js";
import { openPrivateDir, privateDirTestHooks, writeArchiveFile } from "../storage/private-dir.js";
import { assertTaskRunPathUnaliased } from "./task-run-path.js";

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
/** Old layout: `<run>.ctl` sibling. Never an authority source; protected snapshots only. */
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
  openPrivateDir(root, root).close();
  // Re-validate after creation: a concurrent alias or foreign directory still refuses.
  if (taskControlRoot() !== root) throw new FabricTaskControlDirectoryError("Task control root changed during creation");
  return root;
};
/**
 * Stable contract: the central control destination for a run. Deterministic: the
 * lexical run id (basename) plus a digest of the UNRESOLVED lexical run path (never
 * realpath), so archives that reuse a run id under another path never collide, and a
 * planted alias (symlink/junction/reparse) cannot map onto another run's controls.
 */
export const taskControlDirectory = (runDirectory: string): string => {
  const lexical = path.resolve(runDirectory);
  assertTaskRunPathUnaliased(lexical, message => new FabricTaskControlDirectoryError(message));
  const id = path.basename(lexical).replace(/[^A-Za-z0-9._-]/g, "_").replace(/^\.+/, "_").slice(0, 96) || "_";
  const digest = createHash("sha256").update(lexical).digest("hex").slice(0, 32);
  return path.join(taskControlRoot(), `${id}--${digest}`);
};
export const taskSteerFile = (runDirectory: string): string => path.join(taskControlDirectory(runDirectory), "steer.jsonl");

/** Windows cannot open a directory: as openPrivateDir there, use the checked no-follow path (lstat) with no fd. */
const openDir = (directory: string): number | undefined => (privateDirTestHooks.platform ?? process.platform) === "win32" ? undefined :
  fs.openSync(directory, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | (fs.constants.O_NOFOLLOW ?? 0));
export const statDir = (fd: number | undefined, directory: string): fs.BigIntStats => {
  if (fd !== undefined) return fs.fstatSync(fd, { bigint: true });
  // No held fd: a junction/reparse alias may lstat as a directory, so require realpath equality too.
  const stat = fs.lstatSync(directory, { bigint: true });
  if (fs.realpathSync(directory) !== path.resolve(directory)) throw new FabricTaskControlDirectoryError("Task control directory must not traverse a link");
  return stat;
};
const closeDir = (fd: number | undefined): void => { if (fd !== undefined) fs.closeSync(fd); };
const heldDir = (directory: string, fd: number | undefined) => ({ path: directory, fd, close() {} });
const withDirectory = <T>(directory: string, privateMode: boolean, operation: (directory: string, fd: number | undefined) => T): T => {
  const before = fs.lstatSync(directory, { bigint: true });
  if (before.isSymbolicLink() || fs.realpathSync(directory) !== path.resolve(directory)) throw new FabricTaskControlDirectoryError("Task control directory must not traverse a link");
  const fd = openDir(directory);
  try {
    const stat = statDir(fd, directory);
    if (!stat.isDirectory() || stat.dev !== before.dev || stat.ino !== before.ino || (process.getuid && stat.uid !== BigInt(process.getuid())) ||
        (process.platform !== "win32" && (stat.mode & (privateMode ? 0o077n : 0o022n)) !== 0n)) {
      throw new FabricTaskControlDirectoryError("Task control directory must be an owned private directory");
    }
    // Paths through a held fd are synchronous-only; never retain one for an ACK.
    return operation(pinnedPath(fd, directory), fd);
  } finally { closeDir(fd); }
};
/** Strict WRITE/lock namespace. A reader never creates or repairs this sibling. */
export const withTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number | undefined) => T): T =>
  withDirectory(taskControlDirectory(runDirectory), true, operation);

/** Check every intermediate directory too, not only the no-follow file leaf. */
const withControlPath = <T>(directory: string, relative: string, create: boolean, operation: (file: string, parent: string, fd: number | undefined) => T): T => {
  const parts = relative.split(/[\\/]/);
  if (path.isAbsolute(relative) || parts.some(part => !part || part === "." || part === "..")) throw new FabricTaskControlDirectoryError("Invalid task control relative path");
  const walk = (parent: string, index: number): T => {
    const opened = index === 0 ? parent + path.sep + "." : parent, fd = openDir(opened);
    try {
      const stat = statDir(fd, opened);
      if (!stat.isDirectory() || (process.getuid && stat.uid !== BigInt(process.getuid())) || (process.platform !== "win32" && (stat.mode & 0o022n))) {
        throw new FabricTaskControlDirectoryError("Unsafe task control subdirectory");
      }
      const pinned = pinnedPath(fd, parent);
      if (index === parts.length - 1) return operation(path.join(pinned, parts[index]!), pinned, fd);
      const child = path.join(pinned, parts[index]!);
      if (create) {
        try { fs.mkdirSync(child, { mode: 0o700 }); fs.chmodSync(child, 0o700); fsyncFd(fd); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      }
      return walk(child, index + 1);
    } finally { closeDir(fd); }
  };
  return walk(directory, 0);
};
const entryExists = (entry: string): boolean => {
  try { fs.lstatSync(entry); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
};
const centralExists = (runDirectory: string): boolean => entryExists(taskControlDirectory(runDirectory));
const oldSiblingExists = (runDirectory: string): boolean => entryExists(legacyTaskControlSibling(runDirectory));
/** Old sibling-v1 directory shape check (archive discovery only); NEVER a read source. */
const withOldSibling = <T>(runDirectory: string, operation: (directory: string, fd: number | undefined) => T): T =>
  withDirectory(legacyTaskControlSibling(runDirectory), true, operation);
export const readTaskRunStatus = <T>(runDirectory: string): T => withDirectory(path.resolve(runDirectory), false,
  directory => JSON.parse(readBoundedControlFile(path.join(directory, "status.json"), 1024 * 1024)) as T);

// ---- Protected v2 authority (DELTA-SEC-01) ----------------------------------------------
type Identity = Readonly<{ dev: string; ino: string }>;
export type LegacyKind = "in-run" | "sibling";
/** A protected snapshot layer (NEVER a raw directory) plus its host-captured origin. */
export type ProtectedLegacySource = { directory: string; fd: number; origin: { kind: LegacyKind; dev: string; ino: string } };
/** Option 2: current-only authority. `snapshot` is ALWAYS null; any other value refuses. */
type BindingV2 = { version: 2; runId: string; dev: string; ino: string; snapshot: null };
const BINDING = ".legacy-read.json", SNAPSHOT = ".legacy-snapshot";
const unsafe = (message: string): never => { throw new FabricTaskControlDirectoryError(`FABRIC_LEGACY_SNAPSHOT_REQUIRED: ${message}`); };
/** Reserved protected metadata: public write/remove never touches these names. */
const assertPublicControlName = (relative: string): void => {
  // Colons are refused on every platform: Win32 uses them for NTFS alternate
  // data streams, and public control names have no valid colon-bearing form.
  if (relative.includes(":")) unsafe("Colon-bearing task control names are not publicly writable");
  // Fail closed on every platform for NTFS/APFS case aliases and Win32's
  // trailing-dot/space aliases, including a reserved directory component.
  const first = relative.split(/[\\/]/)[0]?.replace(/[. ]+$/u, "").toLowerCase();
  if (first === BINDING || first === SNAPSHOT) unsafe("Protected task control metadata is not publicly writable");
};
const identityOf = (stat: fs.BigIntStats): Identity => ({ dev: String(stat.dev), ino: String(stat.ino) });
const canonicalId = (value: unknown): value is string => typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value);
const exactKeys = (value: unknown, keys: string[]): value is Record<string, unknown> =>
  !!value && typeof value === "object" && !Array.isArray(value) && Object.keys(value).sort().join() === [...keys].sort().join();
const sameId = (a: Identity, b: Identity): boolean => a.dev === b.dev && a.ino === b.ino;
/** Strict v2 current-only: v1, unknown versions/keys and ANY non-null snapshot refuse. */
const parseBinding = (text: string): BindingV2 => {
  let value: unknown;
  try { value = JSON.parse(text); } catch { return unsafe("Malformed protected task control binding"); }
  if (!exactKeys(value, ["version", "runId", "dev", "ino", "snapshot"]) || value.version !== 2 || typeof value.runId !== "string" ||
      !canonicalId(value.dev) || !canonicalId(value.ino)) return unsafe("Unsupported or unbound task control binding; legacy history refused");
  if (value.snapshot !== null) return unsafe("Non-null legacy snapshot binding is not supported; refusing (bytes preserved)");
  return value as unknown as BindingV2;
};
const pinnedPath = (fd: number | undefined, fallback: string): string => fd !== undefined && process.platform === "linux" ? `/proc/self/fd/${fd}` : fallback;
/** Open a protected private child directory under a held parent; check exact identity. */
const openHeldChild = (parent: string, name: string, expected?: Identity): number | undefined => {
  const child = path.join(parent, name), fd = openDir(child);
  try {
    const stat = statDir(fd, child);
    if (!stat.isDirectory() || (process.getuid && stat.uid !== BigInt(process.getuid())) || (process.platform !== "win32" && (stat.mode & 0o077n) !== 0n) ||
        (expected && !sameId(identityOf(stat), expected))) unsafe("Protected task control directory identity mismatch");
    return fd;
  } catch (error) { closeDir(fd); throw error; }
};
const runIdentity = (runDirectory: string): Identity => identityOf(fs.lstatSync(path.resolve(runDirectory), { bigint: true }));
const fsyncFd = (fd: number | undefined): void => { if (fd !== undefined && process.platform !== "win32") fs.fsyncSync(fd); };
const closeAll = (fds: number[]): void => { for (const fd of fds) { try { fs.closeSync(fd); } catch { /* closing a partial batch */ } } };
/**
 * The ONLY historical source selection: a validated current central v2 `snapshot:null`
 * binding for the current run lifetime. Missing, v1, unsupported, non-null or damaged
 * binding, or a stray `.legacy-snapshot`, is an unsafe error (bytes preserved), never
 * "no sources". Valid -> always `[]` (the typed origin shape is retained for readers).
 * Validation only: no per-read fsync.
 */
export const withLegacyTaskControlDirectories = <T>(runDirectory: string, operation: (sources: ProtectedLegacySource[]) => T): T => {
  if (!centralExists(runDirectory)) unsafe("Missing protected task control directory; legacy fallback refused");
  return withTaskControlDirectory(runDirectory, central => {
    let text: string;
    try { text = readBoundedControlFile(path.join(central, BINDING), MAX_TASK_CONTROL_BYTES); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return unsafe("Unbound central task controls; refusing (bytes preserved)");
      throw error;
    }
    const binding = parseBinding(text);
    if (binding.runId !== path.basename(path.resolve(runDirectory)) || !sameId(binding, runIdentity(runDirectory))) unsafe("Task control binding does not match the current run lifetime");
    if (entryExists(path.join(central, SNAPSHOT))) unsafe("Stray protected legacy snapshot present; refusing (bytes preserved)");
    return operation([]);
  });
};
/**
 * READ-ONLY pre-v2 selection: no central entry (even dangling) or stage, no old sibling,
 * no in-run protected metadata, and an owned bounded status WITHOUT any taskControlLayout
 * claim. Write/publication helpers never consult this; they stay central-only.
 */
const legacyReadOnly = (runDirectory: string): boolean => {
  const control = taskControlDirectory(runDirectory), run = path.resolve(runDirectory);
  if (entryExists(control) || entryExists(`${control}.stage`) || oldSiblingExists(run) ||
      entryExists(path.join(run, BINDING)) || entryExists(path.join(run, SNAPSHOT))) return false;
  let record: unknown;
  try { record = readTaskRunStatus<unknown>(run); } catch { return false; }
  return !!record && typeof record === "object" && !Array.isArray(record) && !("taskControlLayout" in record);
};
/** Read-only source list: `[]` for an unmarked pre-v2 run (the run itself is selected), else strict. */
const withReadSources = <T>(runDirectory: string, operation: (sources: ProtectedLegacySource[]) => T): T =>
  legacyReadOnly(runDirectory) ? operation([]) : withLegacyTaskControlDirectories(runDirectory, operation);
/** Read selection: the validated current central directory, or the held run of an unmarked pre-v2 run. */
export const taskControlReadDirectory = (runDirectory: string): string => {
  if (legacyReadOnly(runDirectory)) return path.resolve(runDirectory);
  if (!centralExists(runDirectory)) unsafe("Missing protected task control directory; legacy fallback refused");
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
  return taskControlDirectory(runDirectory);
};
export const withTaskControlReadDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number | undefined) => T): T => {
  const selected = taskControlReadDirectory(runDirectory);
  return selected === path.resolve(runDirectory) ? withDirectory(selected, false, operation) : withTaskControlDirectory(runDirectory, operation);
};
/** Visit every protected source in order; returns one result per visited source. */
export const visitLegacyTaskControlDirectories = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T[] =>
  withLegacyTaskControlDirectories(runDirectory, sources => sources.map(source => operation(source.directory, source.fd)));
/** First protected source where `operation` does not raise ENOENT (single-file lookups). */
const firstLegacy = <T>(runDirectory: string, operation: (directory: string) => T): T | undefined =>
  withReadSources(runDirectory, sources => {
    let missing: unknown;
    for (const source of sources) {
      try { return operation(source.directory); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; missing ??= error; }
    }
    if (missing) throw missing;
    return undefined;
  });
/** Compatibility: the newest protected source only. Prefer the ordered visitor. */
export const withLegacyTaskControlDirectory = <T>(runDirectory: string, operation: (directory: string, fd: number) => T): T | undefined =>
  withLegacyTaskControlDirectories(runDirectory, sources => sources.length ? operation(sources[0]!.directory, sources[0]!.fd) : undefined);

/**
 * Whole-namespace publication. Under the per-destination lock in the held protected root,
 * exclusively create ONE deterministic `<dest>.stage` (an existing stage is stale custody:
 * refused, never adopted or deleted), let `build` fill it, write the v2 binding LAST,
 * fsync, recheck the run identity, rename into the ABSENT destination, fsync the root,
 * then revalidate. Any failure preserves the stage and every raw byte.
 */
const publishCentral = (runDirectory: string, build: (stage: string) => BindingV2): void => {
  ensureTaskControlRoot();
  const control = taskControlDirectory(runDirectory), base = path.basename(control), stageName = `${base}.stage`;
  const root = openPrivateDir(path.dirname(control), path.dirname(control));
  try {
    withExclusiveFileLock({ directory: root.path, lockName: `.${base}.lock`, attempts: 200, delayMs: 5, timeoutMessage: "Task control publication lock timed out" }, () => {
      if (entryExists(path.join(root.path, base))) unsafe("Task control destination already exists; refusing to replace");
      try {
        const stage = path.join(root.path, stageName);
        fs.mkdirSync(stage, { mode: 0o700 });
        // The held private root protects this exclusive create. Restore owner
        // search removed by restrictive umasks before opening/writing the stage.
        if (process.platform !== "win32") fs.chmodSync(stage, 0o700);
      }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") unsafe("Outstanding task control stage exists; refusing (custody preserved)");
        throw error;
      }
      const stageFd = openHeldChild(root.path, stageName);
      try {
        const stage = pinnedPath(stageFd, path.join(root.path, stageName));
        const binding = build(stage);
        const bytes = Buffer.from(JSON.stringify(binding));
        if (bytes.length > MAX_TASK_CONTROL_BYTES) unsafe("Task control binding exceeds its byte bound");
        if (!writeArchiveFile(heldDir(stage, stageFd), BINDING, { bytes }, true)) unsafe("Refusing unsafe binding publication");
        fsyncFd(stageFd);
        if (!sameId(binding, runIdentity(runDirectory))) unsafe("Run identity changed during task control publication");
        if (entryExists(path.join(root.path, base))) unsafe("Task control destination appeared during publication");
        fs.renameSync(path.join(root.path, stageName), path.join(root.path, base));
      } finally { closeDir(stageFd); }
      fsyncFd(root.fd);
    });
  } finally { root.close(); }
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
};
const nullBinding = (runDirectory: string): BindingV2 =>
  ({ version: 2, runId: path.basename(path.resolve(runDirectory)), ...runIdentity(runDirectory), snapshot: null });
/** Raw entries that make an uninitialized run "unknown legacy" rather than fresh blank. */
const RAW_HISTORY = ["status.json", "steer.jsonl", "final-answer.json", "terminal-controls", "terminal-notices", BINDING, SNAPSHOT];
/** PRIVATE: null authority, called ONLY by createHostTaskRun after its exclusive mkdir. */
const createHostTaskControlDirectory = (run: string, allocated: Identity): void => {
  if (centralExists(run)) unsafe("Task control destination already exists; fresh host allocation refused");
  withDirectory(run, false, () => undefined);
  if (oldSiblingExists(run) || RAW_HISTORY.some(name => entryExists(path.join(run, name)))) {
    unsafe("Unknown legacy task history; refusing (bytes preserved)");
  }
  publishCentral(run, () => {
    const binding = nullBinding(run);
    if (!sameId(binding, allocated)) unsafe("Run identity changed after exclusive host allocation");
    return binding;
  });
};
/**
 * Named host producer for the manager's allocation seam: call it right after the early
 * path guard and BEFORE prepareRouteDispatch or any route/status/custody/task write.
 * Authority comes from THIS process's successful EXCLUSIVE mkdir of the leaf through the
 * held parent (Linux: the held /proc fd path; macOS: checked baseline walk; Windows: the
 * inherited fail-closed boundary outside), not from the directory looking blank. EEXIST
 * (or any existing sibling/central/stage) refuses and preserves bytes. Held vs named
 * parent dev/ino and the named run identity are verified before publishing current-only
 * `snapshot:null` central controls. A later EEXIST-tolerant mkdirPrivate is harmless.
 */
export const createHostTaskRun = (runDirectory: string): void => {
  const run = path.resolve(runDirectory), parent = path.dirname(run), leaf = path.basename(run);
  taskControlDirectory(run); // shared alias walk before any write
  // A trusted explicit or nested root may not exist yet; create its validated
  // ancestry, then exclusively allocate only the run leaf through the held parent.
  openPrivateDir(parent, parent).close();
  const allocated = withDirectory(parent, false, (held, parentFd): Identity => {
    const target = path.join(held, leaf), heldParent = identityOf(statDir(parentFd, held));
    try { fs.mkdirSync(target, { mode: 0o700 }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") unsafe("Run directory already exists; exclusive host allocation refused");
      throw error;
    }
    fs.chmodSync(target, 0o700);
    const leafFd = openDir(target);
    try {
      const created = statDir(leafFd, target);
      if (!created.isDirectory() || (process.getuid && created.uid !== BigInt(process.getuid()))) unsafe("Allocated run is not an owned directory");
      fsyncFd(parentFd);
      if (!sameId(heldParent, identityOf(fs.lstatSync(parent, { bigint: true })))) {
        unsafe("Run parent changed during exclusive host allocation");
      }
      const identity = identityOf(created);
      if (!sameId(identity, runIdentity(run))) unsafe("Named run does not match the exclusively allocated directory");
      return identity;
    } finally { closeDir(leafFd); }
  });
  createHostTaskControlDirectory(run, allocated);
};
/**
 * Validation ONLY: existing central with a valid v2 binding. Missing central, unbound,
 * v1 or unsupported binding refuses with every byte preserved; never mints authority.
 */
export const initializeTaskControlDirectory = (runDirectory: string): void => {
  if (!centralExists(runDirectory)) unsafe("Missing protected task control directory; refusing recreation (bytes preserved)");
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
};
/**
 * Archive publication ONLY: the archive's OWN fresh `snapshot:null` authority (copied
 * status allowed); never copies a live binding. Existing destination: validation only.
 */
export const createArchiveTaskControlDirectory = (archiveRunDirectory: string): void => {
  if (centralExists(archiveRunDirectory)) { withLegacyTaskControlDirectories(archiveRunDirectory, () => undefined); return; }
  withDirectory(path.resolve(archiveRunDirectory), false, () => undefined);
  publishCentral(archiveRunDirectory, () => nullBinding(archiveRunDirectory));
};

/** Atomic publication stays on the held namespace; no realpath escape or /proc ancestor fsync. */
export const writeTaskControlJson = (runDirectory: string, relative: string, value: unknown): void => {
  assertPublicControlName(relative);
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
  withTaskControlDirectory(runDirectory, directory =>
    withControlPath(directory, relative, true, (_file, parent, fd) => {
      const bytes = Buffer.from(JSON.stringify(value));
      if (bytes.length > MAX_TASK_CONTROL_BYTES) throw new FabricTaskControlDirectoryError("Task control file exceeds its byte bound");
      if (!writeArchiveFile(heldDir(parent, fd), path.basename(relative), { bytes }, true)) {
        throw new FabricTaskControlDirectoryError("Refusing replacement of unsafe task control file");
      }
    }));
};

/** Cleanup never mutates protected metadata/snapshots and never follows a linked subdirectory. */
export const removeTaskControlFile = (runDirectory: string, relative: string): void => {
  assertPublicControlName(relative);
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
  withTaskControlDirectory(runDirectory, directory => {
    try { withControlPath(directory, relative, false, file => {
      const fd = openTaskControlFile(file); fs.closeSync(fd);
      fs.rmSync(file);
    }); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  });
};

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
export const withTaskControlSubdirectory = <T>(directory: string, relative: string, operation: (directory: string, fd: number | undefined) => T): T =>
  withControlPath(directory, path.join(relative, ".entry"), false, (_file, parent, fd) => operation(parent, fd));
/** The returned FILE fd remains valid after the directory callback closes; no fd path escapes. */
export const openLegacyTaskControlFile = (runDirectory: string, relative: string): number | undefined =>
  firstLegacy(runDirectory, directory => withControlPath(directory, relative, false, openTaskControlFile));
/** Every protected source's copy of `relative` (newest first); missing copies are skipped. */
export const openLegacyTaskControlFiles = (runDirectory: string, relative: string): number[] => {
  // A later source may fail (unsafe/FIFO/etc.) after earlier fds opened: close them
  // all before rethrowing; on success every fd is owned by the caller.
  const fds: number[] = [];
  try {
    withReadSources(runDirectory, sources => {
      for (const source of sources) {
        try { fds.push(withControlPath(source.directory, relative, false, openTaskControlFile)); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
      }
    });
    return fds;
  } catch (error) {
    closeAll(fds);
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
const parseReceipt = (directory: string, runId: string): FinalAnswerReceipt | undefined => {
  try {
    const parsed = JSON.parse(readBoundedControlFile(path.join(directory, "final-answer.json"), MAX_FINAL_ANSWER_BYTES));
    if (parsed?.version === 1 && parsed.runId === runId && typeof parsed.id === "string" && parsed.id.length > 0 &&
        Number.isSafeInteger(parsed.recordedAt) && parsed.recordedAt > 0 && typeof parsed.text === "string") return parsed;
  } catch { /* Missing or invalid bounded receipts never establish finality. */ }
  return undefined;
};
const hasAnswerEntry = (directory: string): boolean => !!fs.lstatSync(path.join(directory, "final-answer.json"), { throwIfNoEntry: false });
/**
 * Finality over the held central directory plus validated protected sources: exactly ONE
 * source may hold an answer entry (identical/malformed duplicates refuse); a malformed
 * single entry never falls back to another source.
 */
const readReceipt = (directory: string, runId: string, runDirectory: string): FinalAnswerReceipt | undefined =>
  withReadSources(runDirectory, sources => {
    const present = [directory, ...sources.map(source => source.directory)].filter(hasAnswerEntry);
    return present.length === 1 ? parseReceipt(present[0]!, runId) : undefined;
  });
export const readFinalAnswerReceipt = (runDirectory: string, runId: string): FinalAnswerReceipt | undefined => {
  try {
    return withTaskControlReadDirectory(runDirectory, directory => readReceipt(directory, runId, runDirectory));
  } catch (error) {
    if (error instanceof FabricTaskControlDirectoryError && !/byte bound|regular file/.test(error.message)) throw error;
    // Malformed/unsafe receipts never establish finality or authorize fallback.
  }
  return undefined;
};

const locked = <T>(directory: string, operation: () => T): T => withExclusiveFileLock({
  directory, lockName: ".task-final-answer.lock", attempts: 200, delayMs: 5,
  timeoutMessage: "Task final-answer publication/injection lock timed out",
}, operation);
/** Reader and lock use the SAME pinned host-owned directory. */
export const withTaskFinalAnswerLock = <T>(runDirectory: string, operation: (read: (runId: string) => FinalAnswerReceipt | undefined) => T): T => {
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
  return withTaskControlDirectory(runDirectory, directory => locked(directory, () => operation(runId => readReceipt(directory, runId, runDirectory))));
};

/** Serialize, size-check and fsync staging BEFORE the publication/injection lock. */
export const saveFinalAnswerReceipt = (runDirectory: string, runId: string, text: string): FinalAnswerReceipt => {
  if (Buffer.byteLength(text) > MAX_FINAL_ANSWER_BYTES) throw new RangeError("Final answer exceeds the 8 MiB receipt bound");
  const receipt: FinalAnswerReceipt = { version: 1, id: randomUUID(), runId, recordedAt: Date.now(), text };
  const contents = JSON.stringify(receipt);
  if (Buffer.byteLength(contents) > MAX_FINAL_ANSWER_BYTES) throw new RangeError("Final answer exceeds the 8 MiB receipt bound");
  withLegacyTaskControlDirectories(runDirectory, () => undefined);
  return withTaskControlDirectory(runDirectory, (directory, directoryFd) => {
    const temporary = path.join(directory, `.final-answer.${process.pid}.${receipt.id}.tmp`);
    const fd = fs.openSync(temporary, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
    try { fs.writeFileSync(fd, contents); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try {
      const published = locked(directory, () => {
        const file = path.join(directory, "final-answer.json");
        // Never replace an existing result in central or ANY protected snapshot layer.
        if (hasAnswerEntry(directory) || withLegacyTaskControlDirectories(runDirectory, sources => sources.some(source => hasAnswerEntry(source.directory)))) return false;
        fs.renameSync(temporary, file); // no serialization, payload read or fsync under this lock
        return true;
      });
      fsyncFd(directoryFd);
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
