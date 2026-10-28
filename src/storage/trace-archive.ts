import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";

type Identity = { dev: number; ino: number };
export interface TraceCopy {
  from: string; to: string; sha256: string; bytes: number; stat: fs.Stats;
}
export interface TraceTreeFile { path: string; bytes: number; sha256: string }
export interface TraceTreeCopy { from: string; to: string; files: TraceTreeFile[] }

const nofollow = fs.constants.O_NOFOLLOW ?? 0;
const sameIdentity = (a: Identity, b: Identity): boolean => a.dev === b.dev && a.ino === b.ino;
const stable = (a: fs.Stats, b: fs.Stats): boolean => sameIdentity(a, b) &&
  a.size === b.size && a.mode === b.mode && a.uid === b.uid && a.gid === b.gid &&
  a.nlink === b.nlink && a.mtimeMs === b.mtimeMs && a.ctimeMs === b.ctimeMs;
const changed = (file: string): Error => new Error(`Trace archive source or destination changed: ${file}`);
const check = (file: string, expected: fs.Stats): void => {
  if (!stable(expected, fs.lstatSync(file))) throw changed(file);
};
const inside = (directory: string, candidate: string): boolean => {
  const relative = path.relative(directory, candidate);
  return relative === "" || (!path.isAbsolute(relative) && relative !== ".." && !relative.startsWith(`..${path.sep}`));
};

/** Resolve missing archive components through their existing ancestor before creating anything. */
const prospectivePath = (directory: string): string => {
  let current = path.resolve(directory);
  const missing: string[] = [];
  while (!fs.lstatSync(current, { throwIfNoEntry: false })) {
    missing.unshift(path.basename(current));
    const parent = path.dirname(current);
    if (parent === current) throw new Error(`Archive has no existing ancestor: ${directory}`);
    current = parent;
  }
  return path.join(fs.realpathSync(current), ...missing);
};

/** Bind namespace ancestors by identity, not timestamps (other users may add sibling files). */
const routeGuard = (file: string): (() => void) => {
  const route: { file: string; stat: fs.Stats }[] = [];
  for (let current = path.dirname(file); ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error(`Archive route is not a real directory: ${current}`);
    route.push({ file: current, stat });
    if (path.dirname(current) === current) break;
  }
  return () => {
    for (const entry of route) {
      const stat = fs.lstatSync(entry.file);
      if (!stat.isDirectory() || !sameIdentity(stat, entry.stat)) throw changed(entry.file);
    }
  };
};

/** Match syncDirectoryChain's supported-platform behavior: Windows has no directory fsync.
 * File fsync, independent byte readback and all identity/metadata checks remain mandatory there.
 */
const syncDirectories = (directory: string): void => {
  if (process.platform === "win32") return;
  for (let current = directory; ; current = path.dirname(current)) {
    const before = fs.lstatSync(current);
    const fd = fs.openSync(current, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | nofollow);
    try {
      const opened = fs.fstatSync(fd);
      if (!opened.isDirectory() || !sameIdentity(before, opened)) throw changed(current);
      fs.fsyncSync(fd);
      if (!sameIdentity(opened, fs.lstatSync(current))) throw changed(current);
    } finally { fs.closeSync(fd); }
    if (path.dirname(current) === current) break;
  }
};

const prepareArchive = (directory: string, sourceTree?: string): string => {
  directory = path.resolve(directory);
  const proposed = prospectivePath(directory);
  if (sourceTree && inside(sourceTree, proposed)) throw new Error("Trace archive must be outside the source tree");
  const existing = fs.lstatSync(directory, { throwIfNoEntry: false });
  if (existing && (!existing.isDirectory() || existing.isSymbolicLink())) {
    throw new Error(`Trace archive is not a real directory: ${directory}`);
  }
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) throw changed(directory);
  const real = fs.realpathSync(directory);
  if (real !== proposed || (sourceTree && inside(sourceTree, real))) throw changed(directory);
  return real;
};

const writeAll = (fd: number, buffer: Buffer, length: number): void => {
  for (let offset = 0; offset < length; ) {
    const written = fs.writeSync(fd, buffer, offset, length - offset);
    if (written <= 0) throw new Error("Trace archive copy made no write progress");
    offset += written;
  }
};

/** Descriptor reads are bounded by the initial size; appenders cannot keep the copy alive forever. */
const digest = (fd: number, expected: fs.Stats, output?: number): { sha256: string; bytes: number } => {
  const hash = createHash("sha256");
  const buffer = Buffer.allocUnsafe(1 << 20);
  let bytes = 0;
  while (bytes < expected.size) {
    const read = fs.readSync(fd, buffer, 0, Math.min(buffer.length, expected.size - bytes), bytes);
    if (read === 0) throw changed("file truncated during copy/readback");
    hash.update(buffer.subarray(0, read));
    if (output !== undefined) writeAll(output, buffer, read);
    bytes += read;
  }
  if (fs.readSync(fd, buffer, 0, 1, bytes) !== 0 || !stable(expected, fs.fstatSync(fd))) {
    throw changed("file size or metadata during copy/readback");
  }
  return { sha256: hash.digest("hex"), bytes };
};

const readback = (file: string, expected: fs.Stats): { sha256: string; bytes: number } => {
  const fd = fs.openSync(file, fs.constants.O_RDONLY | nofollow);
  try {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || !stable(expected, stat)) throw changed(file);
    const result = digest(fd, stat);
    check(file, stat);
    return result;
  } finally { fs.closeSync(fd); }
};

const appendReceipt = (archive: string, record: Record<string, unknown>, source?: Identity): void => {
  const file = path.join(archive, "MANIFEST.jsonl");
  const previous = fs.lstatSync(file, { throwIfNoEntry: false });
  if (previous && (!previous.isFile() || previous.isSymbolicLink() || previous.nlink !== 1 ||
      (source && sameIdentity(previous, source)))) {
    throw new Error(`Trace archive manifest is not a private regular file: ${file}`);
  }
  const fd = fs.openSync(file, fs.constants.O_WRONLY | fs.constants.O_APPEND | fs.constants.O_CREAT | nofollow, 0o600);
  try {
    const opened = fs.fstatSync(fd);
    if (!opened.isFile() || opened.nlink !== 1 || (previous && !sameIdentity(previous, opened)) ||
        (source && sameIdentity(opened, source))) throw changed(file);
    const bytes = Buffer.from(JSON.stringify(record) + "\n");
    writeAll(fd, bytes, bytes.length);
    fs.fsyncSync(fd);
    if (!sameIdentity(opened, fs.lstatSync(file))) throw changed(file);
  } finally { fs.closeSync(fd); }
};

/** Shared byte-copy engine; the tree uses the same verification at its exact relative file names. */
const copyInto = (source: string, to: string, stat: fs.Stats): { sha256: string; bytes: number; placed: fs.Stats } => {
  const guardSource = routeGuard(source), guardDestination = routeGuard(to);
  check(source, stat);
  const fd = fs.openSync(source, fs.constants.O_RDONLY | nofollow);
  try {
    if (!stat.isFile() || !stable(stat, fs.fstatSync(fd))) throw changed(source);
    const output = fs.openSync(to, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | nofollow, 0o600);
    let copied: { sha256: string; bytes: number };
    let placed: fs.Stats;
    try {
      copied = digest(fd, stat, output);
      fs.fsyncSync(output);
      placed = fs.fstatSync(output);
      if (!placed.isFile() || placed.size !== copied.bytes) throw changed(to);
    } finally { fs.closeSync(output); }
    const verified = readback(to, placed);
    if (verified.sha256 !== copied.sha256 || verified.bytes !== copied.bytes) {
      throw new Error(`Trace archive readback verification failed: ${to}`);
    }
    const finalSource = digest(fd, stat);
    if (finalSource.sha256 !== copied.sha256 || finalSource.bytes !== copied.bytes) throw changed(source);
    guardSource();
    guardDestination();
    check(source, stat);
    check(to, placed);
    return { ...copied, placed };
  } finally { fs.closeSync(fd); }
};

/** Copy only: never renames, unlinks or truncates a source. Failed/partial archive artifacts are left in place.
 * Caller still owns writer serialization and must revalidate the returned source stat before deleting anything.
 * The destination is exclusive, fsynced, reopened and independently hashed before a durable receipt is returned.
 */
export const archiveTraceCopy = (file: string, archiveDirectory: string, meta: Record<string, unknown> = {},
  expect?: Identity): TraceCopy => {
  const from = path.resolve(file);
  const source = path.join(fs.realpathSync(path.dirname(from)), path.basename(from));
  const guardSource = routeGuard(source);
  const stat = fs.lstatSync(from);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(`Trace source is not a regular file: ${from}`);
  if (expect && !sameIdentity(stat, expect)) throw changed(from);
  const archive = prepareArchive(archiveDirectory);
  const guardArchive = routeGuard(path.join(archive, "entry"));
  let to: string;
  let copied: ReturnType<typeof copyInto>;
  for (;;) {
    // UUID prefix preserves the complete original name, including arbitrary log suffixes.
    to = path.join(archive, `${randomUUID()}-${path.basename(from)}`);
    try { copied = copyInto(source, to, stat); break; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  }
  const { sha256, bytes, placed } = copied;
  appendReceipt(archive, { ...meta, format: 1, phase: "COPY-READBACK", kind: "file", from, to,
    sha256, bytes, at: new Date().toISOString() }, stat);
  syncDirectories(archive);
  guardArchive();
  guardSource();
  if (fs.realpathSync(archiveDirectory) !== archive) throw changed(archiveDirectory);
  check(to, placed);
  check(from, stat);
  const finalSource = readback(source, stat);
  if (finalSource.sha256 !== sha256 || finalSource.bytes !== bytes) throw changed(from);
  check(from, stat);
  return { from, to, sha256, bytes, stat };
};

interface TreeEntry { path: string; stat: fs.Stats }
const inventory = (root: string): TreeEntry[] => {
  const entries: TreeEntry[] = [];
  const walk = (relative: string): void => {
    const file = path.join(root, relative);
    const stat = fs.lstatSync(file);
    if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile())) {
      throw new Error(`Trace tree contains a symlink or unsupported non-regular entry: ${file}`);
    }
    entries.push({ path: relative, stat });
    if (stat.isDirectory()) {
      for (const name of fs.readdirSync(file).sort()) walk(relative ? `${relative}/${name}` : name);
      check(file, stat);
    }
  };
  walk("");
  return entries;
};
const checkInventory = (root: string, expected: TreeEntry[]): void => {
  const current = inventory(root);
  if (current.length !== expected.length || current.some((entry, index) =>
    entry.path !== expected[index]!.path || !stable(entry.stat, expected[index]!.stat))) throw changed(root);
};

/** Complete regular-file tree, including empty directories, verified as one snapshot before return.
 * No symlink/unknown entry is followed or omitted. No source is ever removed, even on partial failure.
 */
export const archiveTraceTreeCopy = (directory: string, archiveDirectory: string, meta: Record<string, unknown> = {},
  expect?: Identity): TraceTreeCopy => {
  const from = path.resolve(directory);
  const rootStat = fs.lstatSync(from);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error(`Trace tree is not a real directory: ${from}`);
  if (expect && !sameIdentity(expect, rootStat)) throw changed(from);
  const source = fs.realpathSync(from);
  const guardSource = routeGuard(path.join(source, "entry"));
  const before = inventory(source);
  const archive = prepareArchive(archiveDirectory, source);
  const guardArchive = routeGuard(path.join(archive, "entry"));
  let to: string;
  for (;;) {
    to = path.join(archive, `${randomUUID()}-${path.basename(from)}`);
    try { fs.mkdirSync(to, { mode: 0o700 }); break; }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  }
  const files: TraceTreeFile[] = [];
  const destinations: { file: string; stat: fs.Stats; sha256: string }[] = [];
  for (const entry of before) {
    const sourceFile = path.join(source, entry.path);
    const target = path.join(to, entry.path);
    check(sourceFile, entry.stat);
    if (entry.stat.isDirectory()) {
      if (entry.path) fs.mkdirSync(target, { mode: 0o700 });
      continue;
    }
    const copy = copyInto(sourceFile, target, entry.stat);
    files.push({ path: entry.path, sha256: copy.sha256, bytes: copy.bytes });
    destinations.push({ file: target, stat: copy.placed, sha256: copy.sha256 });
  }
  const placedTree = inventory(to);
  if (placedTree.length !== before.length || placedTree.some((entry, index) => entry.path !== before[index]!.path ||
      entry.stat.isDirectory() !== before[index]!.stat.isDirectory())) throw changed(to);
  for (const entry of [...before].reverse()) {
    if (entry.stat.isDirectory()) syncDirectories(path.join(to, entry.path));
  }
  const assertSource = (): void => {
    guardSource();
    check(from, rootStat);
    if (fs.realpathSync(from) !== source) throw changed(from);
    checkInventory(source, before);
  };
  assertSource();
  appendReceipt(archive, { ...meta, format: 1, phase: "COPY-READBACK", kind: "tree", from, to, files, at: new Date().toISOString() });
  syncDirectories(archive);
  guardArchive();
  if (fs.realpathSync(archiveDirectory) !== archive) throw changed(archiveDirectory);
  for (const destination of destinations) {
    const verified = readback(destination.file, destination.stat);
    if (verified.sha256 !== destination.sha256) throw changed(destination.file);
  }
  checkInventory(to, placedTree);
  assertSource();
  return { from, to, files };
};
