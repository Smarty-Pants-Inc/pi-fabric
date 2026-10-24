import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { syncDirectoryChain } from "../core/atomic-write.js";
import { assertPinnedDirectory, assertRoute, linkOf, moveActorTreeToArchive, type PinnedActorRoot } from "./tree-archive.js";

/**
 * R-keep-all-traces (smarty-dev#16444): an actor session trace is never deleted. Rotation history, retention,
 * actor removal and a root-tree reset MOVE what they would have deleted into a retained archive, by one
 * same-filesystem rename(2) (never a copy and then a delete; EXDEV refuses and keeps it in place), and record it in
 * the archive's append-only MANIFEST.jsonl: a fsynced `MOVING` intent BEFORE the rename (from, to, sha256), then
 * the parent directories are fsynced and a `MOVED-TO <archive path>` line follows it.
 *
 * - Beside the actor: `<actor dir>/session-archive/<stamp>-<sha256-12>.jsonl` (rotation history, one file each).
 * - Fleet level: actor removal renames the WHOLE actor directory (session.jsonl*, `.activation-<run>.jsonl`,
 *   `registry/messages.jsonl`, runs, its own session-archive/: every byte) to `<meshRoot>/session-archive/<root>/<actor>`
 *   (`<actor>.<stamp>` when that exists); a non-persistent root's reset renames the whole actor root to
 *   `<meshRoot>/session-archive/<root>/actor-root` (same rule). The fleet MANIFEST.jsonl is `<meshRoot>/session-archive/<root>/`.
 *
 * EXDEV (the tree and `<meshRoot>/session-archive/<root>` are on different filesystems or mounts): nothing moves and
 * nothing is copied (no copy fallback, by design); the tree stays where it is, byte-identical. A removal stays pending
 * and retries on every remove and host start; a root reset logs "kept" and leaves the tree. The way back is by hand:
 * an operator points `<meshRoot>/session-archive` (a symlink; not a bind mount, rename(2) refuses across mounts) at a
 * directory on the tree's own mount and retries the removal, which then renames and completes. A kept root-reset tree
 * may simply be moved by the operator into the archive. See docs/agents.md "Fresh actor sessions".
 *
 * TODO(smarty-dev#16444): nothing deletes for size. The archives grow until the trace-store offload moves them off
 * the host; that offload, not a local prune, is the only way a trace leaves.
 */
export const SESSION_ARCHIVE_DIR = "session-archive";
export const SESSION_ARCHIVE_MANIFEST = "MANIFEST.jsonl";
/** The name of a non-persistent root's whole actor tree in its fleet archive (an actor id is 32 hex: no clash). */
export const ROOT_TREE_ARCHIVE_NAME = "actor-root";

/** One archived trace: where it was, where it is now, its full sha256 and its size. `manifest: false`: the rename
 * happened, but a step after it failed (directory fsync or the MOVED-TO line); the MOVING line still names it. */
export interface SessionTraceMove { from: string; to: string; sha256: string; bytes: number; manifest?: false; problem?: string }
/** One whole tree moved into a fleet archive by one rename, with the inventory recorded in its MOVING line. */
export interface TreeArchiveMove { from: string; to: string; files: TreeFile[]; manifest: boolean; problem?: string }
export interface TreeFile { path: string; bytes: number; sha256?: string }

/** `session.jsonl` and every rotation, orphan or reseed backup of it (`session.jsonl.<anything>`). */
export const sessionTraceName = (name: string): boolean => /^session\.jsonl(?:\..+)?$/.test(name);
/** Every trace-bearing file name in an actor tree: session files, activation sessions and message logs. */
export const traceFileName = (name: string): boolean =>
  sessionTraceName(name) || /^\.activation-.+\.jsonl$/.test(name) || /^messages.*\.jsonl$/.test(name);

const compactStamp = (at = Date.now()): string => new Date(at).toISOString().replace(/[-:.]/g, "");
/** The rotation stamp in a backup name (session.jsonl.<stamp>[-n].bak), else the archive time. */
const traceStamp = (name: string): string => /\.(\d{8}T\d{9}Z)(?:[-.]|$)/.exec(name)?.[1] ?? compactStamp();
const code = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;
const text = (error: unknown): string => error instanceof Error ? error.message : String(error);

/** A path component for a root or actor id: readable, and safe on every platform (a root id may hold a colon). */
export const archiveSegment = (value: string): string => {
  const safe = value.replace(/[^A-Za-z0-9_.-]/g, "_").replace(/^\.+/, "_");
  return safe === value && safe ? safe : `${safe || "_"}-${createHash("sha256").update(value).digest("hex").slice(0, 8)}`;
};

/** The fleet archive of one root, or the archived tree of one actor (its first removal; later ones get a stamp). */
export const fleetSessionArchive = (meshRoot: string, rootId: string, actorId?: string): string =>
  actorId === undefined ? path.join(meshRoot, SESSION_ARCHIVE_DIR, archiveSegment(rootId))
    : path.join(meshRoot, SESSION_ARCHIVE_DIR, archiveSegment(rootId), archiveSegment(actorId));

/** The full sha256 of a regular file, read through one O_NOFOLLOW descriptor, with the fstat it was read under. */
export const sha256File = (file: string): { sha256: string; stat: fs.Stats } => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW ?? 0));
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile()) throw new Error(`session trace is not a regular file: ${file}`);
    const hash = createHash("sha256");
    const buffer = Buffer.allocUnsafe(1 << 20);
    for (let position = 0; ;) {
      const read = fs.readSync(fd, buffer, 0, buffer.length, position);
      if (read === 0) break;
      hash.update(buffer.subarray(0, read));
      position += read;
    }
    return { sha256: hash.digest("hex"), stat };
  } finally { fs.closeSync(fd); }
};

/** The archive directory: created 0700 when missing, and a real directory (never a link) either way. */
const ensureArchiveDirectory = (directory: string): fs.Stats => {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(directory);
  if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`session archive is not a real directory: ${directory}`);
  return stat;
};

/** A free name in the archive: `<base><ext>`, then `<base>.<n><ext>`. An archive entry is never overwritten. */
const freeName = (directory: string, base: string, ext: string): string => {
  for (let n = 0; ; n++) {
    const candidate = path.join(directory, n === 0 ? `${base}${ext}` : `${base}.${n}${ext}`);
    if (!fs.lstatSync(candidate, { throwIfNoEntry: false })) return candidate;
  }
};

/** One durable, append-only manifest line. */
const appendManifest = (directory: string, record: Record<string, unknown>): void => {
  const fd = fs.openSync(path.join(directory, SESSION_ARCHIVE_MANIFEST),
    fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT | (fs.constants.O_NOFOLLOW ?? 0), 0o600);
  try { fs.writeSync(fd, `${JSON.stringify(record)}\n`); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
};

/** Before a rename: the fsynced MOVING intent, and the archive directory fsynced so the manifest's own entry is on
 * disk. A failure throws before anything moved. */
const intend = (archive: string, record: Record<string, unknown>, sha256 = ""): void => {
  appendManifest(archive, { ...record, at: new Date().toISOString(), phase: "MOVING",
    line: `MOVING ${String(record.from)} -> ${String(record.to)}${sha256 ? ` sha256 ${sha256}` : ""}` });
  syncDirectoryChain(archive);
};
/** After a failed rename (nothing moved): best effort, so the MOVING line has its outcome. */
const failed = (archive: string, record: Record<string, unknown>, error: unknown): void => {
  try { appendManifest(archive, { ...record, at: new Date().toISOString(), phase: "MOVE-FAILED", error: text(error) }); }
  catch { /* the MOVING line without an outcome reads as "check both paths" */ }
};
/** After the rename: fsync both parent directories (fsync(2): the file's own fsync does not persist its directory
 * entry), then the MOVED-TO line. Never throws: the entry HAS moved, so a failure here is reported as
 * `manifest: false` (with the MOVING line as its record), never as "kept in place". */
const settle = (archive: string, sourceParent: string, record: Record<string, unknown>, problem?: string):
  { manifest: true } | { manifest: false; problem: string } => {
  try {
    syncDirectoryChain(archive);
    if (path.resolve(sourceParent) !== path.resolve(archive)) syncDirectoryChain(sourceParent);
    appendManifest(archive, { ...record, at: new Date().toISOString(), phase: "MOVED-TO",
      ...(problem ? { problem } : {}), line: `MOVED-TO ${String(record.to)}` });
  } catch (error) { return { manifest: false, problem: [problem, text(error)].filter(Boolean).join("; ") }; }
  return problem ? { manifest: false, problem } : { manifest: true };
};

/** Move one session trace into `archive` as `<stamp>-<sha256-12>.jsonl` by one rename, recorded MOVING then MOVED-TO in
 * the archive's MANIFEST.jsonl. `expect` is the judged identity: a different inode is refused before any change.
 * Throws (nothing moved) on EXDEV, a link, a non-file, a changed inode or a failed MOVING intent. */
export const archiveSessionTrace = (file: string, archive: string, meta: Record<string, unknown> = {},
  expect?: { dev: number; ino: number }): SessionTraceMove => {
  const { sha256, stat } = sha256File(file);
  if (expect && (stat.dev !== expect.dev || stat.ino !== expect.ino)) throw new Error(`session trace changed since judgment: ${file}`);
  ensureArchiveDirectory(archive);
  const to = freeName(archive, `${traceStamp(path.basename(file))}-${sha256.slice(0, 12)}`, ".jsonl");
  const current = fs.lstatSync(file);
  if (current.dev !== stat.dev || current.ino !== stat.ino) throw new Error(`session trace changed while it was hashed: ${file}`);
  const move: SessionTraceMove = { from: path.resolve(file), to: path.resolve(to), sha256, bytes: stat.size };
  const record = { format: 1, kind: "file", ...meta, ...move };
  intend(archive, record, sha256);
  try { fs.renameSync(file, to); }
  catch (error) {
    failed(archive, record, error);
    if (code(error) === "EXDEV") throw new Error(`session trace and its archive are on different filesystems (EXDEV); kept, no copy: ${file}`);
    throw error;
  }
  let problem: string | undefined;
  try {
    const moved = fs.lstatSync(to);
    if (moved.dev !== stat.dev || moved.ino !== stat.ino) problem = `archived entry is not the hashed trace: ${to}`;
  } catch (error) { problem = `archived entry not found after the rename: ${text(error)}`; }
  const settled = settle(archive, path.dirname(file), record, problem);
  return settled.manifest ? move : { ...move, manifest: false, problem: settled.problem };
};

const MAX_DEPTH = 32;
/** Every regular file below `root` (relative path and size; sha256 for trace-bearing names). Links are not followed. */
const treeInventory = (root: string): TreeFile[] => {
  const files: TreeFile[] = [];
  const walk = (directory: string, relative: string, depth: number): void => {
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(directory, { withFileTypes: true }); }
    catch (error) { if (code(error) === "ENOENT" || code(error) === "ENOTDIR") return; throw error; }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const file = path.join(directory, entry.name), name = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) { if (depth < MAX_DEPTH) walk(file, name, depth + 1); continue; }
      if (!entry.isFile()) continue;
      if (traceFileName(entry.name)) {
        const { sha256, stat } = sha256File(file);
        files.push({ path: name, bytes: stat.size, sha256 });
      } else files.push({ path: name, bytes: fs.lstatSync(file).size });
    }
  };
  walk(root, "", 0);
  return files;
};

/** Move a whole tree (an actor directory, or a root's actor tree) into `rootArchive` as `<base>` (`<base>.<stamp>` when
 * that exists) by ONE rename (`moveActorTreeToArchive`, shared with offline removal), recorded MOVING (with the
 * inventory and each trace's sha256) then MOVED-TO in `<rootArchive>/MANIFEST.jsonl`. `tree` is the lexical path (the
 * record and the parent fsync); `source` the path the rename uses (a held-parent /proc path on Linux).
 * Before any record and again right before the rename, `source` must be the pinned directory and, through a held
 * parent, `tree` must still reach that parent and name it (`assertRoute`): a tree or ancestor swapped since the
 * judgment is refused with nothing moved and nothing recorded.
 * Throws with the tree in place on EXDEV, a different inode, a changed route, a failed intent or a failed rename. */
export const archiveTree = (tree: string, rootArchive: string, base: string, pin: PinnedActorRoot,
  meta: Record<string, unknown> = {}, source = tree): TreeArchiveMove => {
  const route = source === tree ? undefined : tree;
  if (route !== undefined) assertRoute(route, source, pin);
  assertPinnedDirectory(source, pin);
  const placed = ensureArchiveDirectory(rootArchive);
  if (placed.dev !== pin.dev) throw new Error(`tree and its archive are on different filesystems (EXDEV); kept, no copy: ${tree}`);
  const to = !fs.lstatSync(path.join(rootArchive, base), { throwIfNoEntry: false }) ? path.join(rootArchive, base)
    : freeName(rootArchive, `${base}.${compactStamp()}`, "");
  const files = treeInventory(source);
  const record = { format: 1, kind: "tree", ...meta, from: path.resolve(tree), to: path.resolve(to) };
  intend(rootArchive, { ...record, files });
  try { moveActorTreeToArchive(source, pin, rootArchive, [linkOf(rootArchive, placed)], path.basename(to), route); }
  catch (error) { failed(rootArchive, record, error); throw error; }
  const settled = settle(rootArchive, path.dirname(path.resolve(tree)), record);
  return { from: record.from, to: record.to, files, ...settled };
};

/** A tree rename that completed before its caller recorded the discharge (a crash, a failed marker save): the pinned
 * directory is in `rootArchive` as `<base>` or `<base>.<stamp>`, found by its pinned st_dev/st_ino and owner (the
 * identity, not the name, is the proof). A MOVED-TO line is appended when the manifest has none for it (`recovered`);
 * like `settle`, a failure there is reported as `manifest: false`, never as "not archived". Returns undefined when
 * the pinned tree is not there. */
export const recoverArchivedTree = (tree: string, rootArchive: string, base: string, pin: PinnedActorRoot,
  meta: Record<string, unknown> = {}): { to: string; manifest: boolean; problem?: string } | undefined => {
  let names: string[];
  try { names = fs.readdirSync(rootArchive); }
  catch (error) { if (code(error) === "ENOENT") return undefined; throw error; }
  const uid = pin.uid ?? process.getuid?.();
  const name = names.find(entry => {
    if (entry !== base && !entry.startsWith(`${base}.`)) return false;
    const stat = fs.lstatSync(path.join(rootArchive, entry), { throwIfNoEntry: false });
    return !!stat && stat.isDirectory() && !stat.isSymbolicLink() && stat.dev === pin.dev && stat.ino === pin.ino &&
      (uid === undefined || stat.uid === uid);
  });
  if (name === undefined) return undefined;
  const to = path.resolve(rootArchive, name);
  try {
    let settled = false;
    try {
      settled = fs.readFileSync(path.join(rootArchive, SESSION_ARCHIVE_MANIFEST), "utf8").split("\n").some(line => {
        try { const entry = JSON.parse(line) as { phase?: unknown; to?: unknown }; return entry.phase === "MOVED-TO" && entry.to === to; }
        catch { return false; }
      });
    } catch (error) { if (code(error) !== "ENOENT") throw error; }
    if (!settled) {
      appendManifest(rootArchive, { format: 1, kind: "tree", ...meta, from: path.resolve(tree), to, at: new Date().toISOString(),
        phase: "MOVED-TO", recovered: true, line: `MOVED-TO ${to}` });
    }
  } catch (error) { return { to, manifest: false, problem: text(error) }; }
  return { to, manifest: true };
};
