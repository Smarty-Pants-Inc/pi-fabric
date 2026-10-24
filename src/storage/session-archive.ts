import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";

/**
 * R-keep-all-traces (smarty-dev#16444): an actor session trace is never deleted. Rotation history, retention,
 * actor removal and a root-tree reset MOVE every trace they would have deleted into a retained archive, by one
 * same-filesystem rename(2) (never a copy and then a delete; EXDEV refuses and keeps the trace in place), and record
 * its full sha256 and a `MOVED-TO <archive path>` line in the archive's append-only MANIFEST.jsonl.
 *
 * - Beside the actor: `<actor dir>/session-archive/<stamp>-<sha256-12>.jsonl` (rotation history).
 * - Fleet level, before an actor tree or a root's actor tree is removed:
 *   `<meshRoot>/session-archive/<rootId>/<actorId>/`.
 *
 * TODO(smarty-dev#16444): nothing deletes for size. The archives grow until the trace-store offload moves them off
 * the host; that offload, not a local prune, is the only way a trace leaves.
 */
export const SESSION_ARCHIVE_DIR = "session-archive";
export const SESSION_ARCHIVE_MANIFEST = "MANIFEST.jsonl";

/** One archived trace: where it was, where it is now, its full sha256 and its size. */
export interface SessionTraceMove { from: string; to: string; sha256: string; bytes: number }
/** A whole per-actor session archive moved into the fleet archive by one rename. */
export interface SessionArchiveDirectoryMove { from: string; to: string; files: string[] }

/** `session.jsonl` and every rotation, orphan or reseed backup of it (`session.jsonl.<anything>`). */
export const sessionTraceName = (name: string): boolean => /^session\.jsonl(?:\..+)?$/.test(name);

const compactStamp = (at = Date.now()): string => new Date(at).toISOString().replace(/[-:.]/g, "");
/** The rotation stamp in a backup name (session.jsonl.<stamp>[-n].bak), else the archive time. */
const traceStamp = (name: string): string => /\.(\d{8}T\d{9}Z)(?:[-.]|$)/.exec(name)?.[1] ?? compactStamp();
const code = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;

/** A path component for a root or actor id: readable, and safe on every platform (a root id may hold a colon). */
export const archiveSegment = (value: string): string => {
  const safe = value.replace(/[^A-Za-z0-9_.-]/g, "_").replace(/^\.+/, "_");
  return safe === value && safe ? safe : `${safe || "_"}-${createHash("sha256").update(value).digest("hex").slice(0, 8)}`;
};

/** The fleet archive of one actor (or, without an actor id, of one root) under the mesh root. */
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
const ensureArchiveDirectory = (directory: string): void => {
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(directory);
  if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`session archive is not a real directory: ${directory}`);
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

/** Move one session trace into `archive` as `<stamp>-<sha256-12>.jsonl` by one rename, then record it in the
 * archive's MANIFEST.jsonl. `expect` is the judged identity: a different inode is refused before any change. Throws
 * (nothing moved) on EXDEV, a link, a non-file or a changed inode. */
export const archiveSessionTrace = (file: string, archive: string, meta: Record<string, unknown> = {},
  expect?: { dev: number; ino: number }): SessionTraceMove => {
  const { sha256, stat } = sha256File(file);
  if (expect && (stat.dev !== expect.dev || stat.ino !== expect.ino)) throw new Error(`session trace changed since judgment: ${file}`);
  ensureArchiveDirectory(archive);
  const to = freeName(archive, `${traceStamp(path.basename(file))}-${sha256.slice(0, 12)}`, ".jsonl");
  const current = fs.lstatSync(file);
  if (current.dev !== stat.dev || current.ino !== stat.ino) throw new Error(`session trace changed while it was hashed: ${file}`);
  try { fs.renameSync(file, to); }
  catch (error) {
    if (code(error) === "EXDEV") throw new Error(`session trace and its archive are on different filesystems (EXDEV); kept, no copy: ${file}`);
    throw error;
  }
  const moved = fs.lstatSync(to);
  if (moved.dev !== stat.dev || moved.ino !== stat.ino) throw new Error(`archived entry is not the hashed trace: ${to}`);
  const move: SessionTraceMove = { from: path.resolve(file), to: path.resolve(to), sha256, bytes: stat.size };
  appendManifest(archive, { format: 1, at: new Date().toISOString(), kind: "file", ...meta, ...move, line: `MOVED-TO ${move.to}` });
  return move;
};

/** Move a whole per-actor session archive (its traces and its own manifest) into `archive` by one rename. */
const archiveSessionDirectory = (directory: string, archive: string, meta: Record<string, unknown>): SessionArchiveDirectoryMove => {
  const stat = fs.lstatSync(directory);
  if (stat.isSymbolicLink() || !stat.isDirectory()) throw new Error(`session archive is not a real directory: ${directory}`);
  const files = fs.readdirSync(directory).sort();
  ensureArchiveDirectory(archive);
  const to = freeName(archive, `${compactStamp()}-actor-${SESSION_ARCHIVE_DIR}`, "");
  try { fs.renameSync(directory, to); }
  catch (error) {
    if (code(error) === "EXDEV") throw new Error(`session archive and the fleet archive are on different filesystems (EXDEV); kept, no copy: ${directory}`);
    throw error;
  }
  const moved = fs.lstatSync(to);
  if (moved.dev !== stat.dev || moved.ino !== stat.ino) throw new Error(`archived entry is not the session archive: ${to}`);
  const move: SessionArchiveDirectoryMove = { from: path.resolve(directory), to: path.resolve(to), files };
  appendManifest(archive, { format: 1, at: new Date().toISOString(), kind: "directory", ...meta, ...move, line: `MOVED-TO ${move.to}` });
  return move;
};

const MAX_DEPTH = 16;
/** Every session trace below `root`: `session.jsonl*` regular files, and `session-archive` directories (not entered).
 * No link is followed; a link is not a trace (removing it removes no trace bytes). */
export const sessionTracesUnder = (root: string): { files: string[]; archives: string[] } => {
  const files: string[] = [], archives: string[] = [];
  const walk = (directory: string, depth: number): void => {
    let entries: fs.Dirent[];
    try { entries = fs.readdirSync(directory, { withFileTypes: true }); }
    catch (error) { if (code(error) === "ENOENT" || code(error) === "ENOTDIR") return; throw error; }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const file = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        if (entry.name === SESSION_ARCHIVE_DIR) archives.push(file);
        else if (depth < MAX_DEPTH) walk(file, depth + 1);
        else throw new Error(`tree is too deep to prove it holds no session trace: ${file}`);
      } else if (entry.isFile() && sessionTraceName(entry.name)) files.push(file);
    }
  };
  walk(root, 0);
  return { files, archives };
};

/** Before `tree` is removed: every session trace below it moves into `archive` (a fleet archive), each with a manifest
 * line. Throws on the first failure; the caller must then keep the tree (fail closed). An absent tree moves nothing. */
export const archiveSessionTracesBeforeRemoval = (tree: string, archive: string, meta: Record<string, unknown> = {}):
  { files: SessionTraceMove[]; directories: SessionArchiveDirectoryMove[] } => {
  const { files, archives } = sessionTracesUnder(tree);
  return {
    files: files.map(file => archiveSessionTrace(file, archive, meta)),
    directories: archives.map(directory => archiveSessionDirectory(directory, archive, meta)),
  };
};

/** Before a root's whole actor tree is removed: each actor directory's traces into `<root archive>/<actorId>/`, and any
 * trace directly in the root into `<root archive>/_root/`. Throws on the first failure; the caller keeps the tree. */
export const archiveRootSessionTraces = (actorRoot: string, rootArchive: string, meta: Record<string, unknown> = {}): number => {
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(actorRoot, { withFileTypes: true }); }
  catch (error) { if (code(error) === "ENOENT") return 0; throw error; }
  let moved = 0;
  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    const file = path.join(actorRoot, entry.name);
    if (entry.isDirectory()) {
      const result = archiveSessionTracesBeforeRemoval(file, path.join(rootArchive, archiveSegment(entry.name)), { ...meta, actorId: entry.name });
      moved += result.files.length + result.directories.length;
    } else if (entry.isFile() && sessionTraceName(entry.name)) {
      archiveSessionTrace(file, path.join(rootArchive, "_root"), meta);
      moved++;
    }
  }
  return moved;
};
