import fs from "node:fs";
import { createHash } from "node:crypto";
import { dirname, basename, resolve } from "node:path";

export const RUN_LOG_BYTE_CAP = 128 * 1024 * 1024;
const noFollow = fs.constants.O_NOFOLLOW ?? 0;
const nonBlock = fs.constants.O_NONBLOCK ?? 0;

/** committed=true means representation publication happened; never promise that plain remains. */
export class RunLogCodecError extends Error {
  readonly code = "run-log-codec";
  constructor(
    readonly reason: string,
    readonly path: string,
    readonly committed = false,
    cause?: unknown,
  ) {
    super(`${reason}: ${path}${cause instanceof Error ? `: ${cause.message}` : ""}`, { cause });
    this.name = "RunLogCodecError";
  }
}

export interface HeldRunLogFile {
  path: string;
  fd: number | undefined;
  stat: fs.BigIntStats;
}
export interface HeldRunLogDirectory extends HeldRunLogFile {}

const absent = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === "ENOENT";
const owned = (stat: fs.BigIntStats): boolean => typeof process.getuid !== "function" || stat.uid === BigInt(process.getuid());
const identity = (left: fs.BigIntStats, right: fs.BigIntStats): boolean => left.dev === right.dev && left.ino === right.ino;
const version = (left: fs.BigIntStats, right: fs.BigIntStats): boolean => identity(left, right)
  && left.size === right.size && left.mtimeNs === right.mtimeNs && left.ctimeNs === right.ctimeNs
  && left.mode === right.mode && left.uid === right.uid && left.nlink === right.nlink;
const safeFile = (stat: fs.BigIntStats, links = 1n): boolean => stat.isFile() && !stat.isSymbolicLink() && owned(stat) && stat.nlink === links;
const safeDirectory = (stat: fs.BigIntStats): boolean => stat.isDirectory() && !stat.isSymbolicLink() && owned(stat)
  && (process.platform === "win32" || (stat.mode & 0o077n) === 0n);

/** Pin a real private directory, including its canonical pathname, throughout publication. */
export function holdRunLogDirectory(filePath: string): HeldRunLogDirectory {
  const path = dirname(resolve(filePath));
  const stat = fs.lstatSync(path, { bigint: true });
  if (!safeDirectory(stat) || fs.realpathSync(path) !== path) throw new RunLogCodecError("unsafe-directory", path);
  // Match private-dir.ts's existing Windows contract: no directory descriptor. Windows
  // retains lstat/realpath identity checks, but lacks POSIX directory-fsync crash durability.
  const fd = process.platform === "win32" ? undefined
    : fs.openSync(path, fs.constants.O_RDONLY | (fs.constants.O_DIRECTORY ?? 0) | noFollow);
  const directory = { path, fd, stat };
  try { assertRunLogDirectory(directory); return directory; }
  catch (error) { try { closeRunLogFile(directory); } catch { /* preserve custody failure */ } throw error; }
}

export function assertRunLogDirectory(directory: HeldRunLogDirectory): void {
  const current = fs.lstatSync(directory.path, { bigint: true });
  if (!safeDirectory(current) || !identity(current, directory.stat) || fs.realpathSync(directory.path) !== directory.path
    || (directory.fd !== undefined && !identity(fs.fstatSync(directory.fd, { bigint: true }), directory.stat))) {
    throw new RunLogCodecError("directory-changed", directory.path);
  }
}

/** lstat BEFORE open avoids blocking on a FIFO even where O_NOFOLLOW is unavailable. */
export function openRunLogFile(path: string, writable = false): HeldRunLogFile | undefined {
  let stat: fs.BigIntStats;
  try { stat = fs.lstatSync(path, { bigint: true }); }
  catch (error) { if (absent(error)) return; throw error; }
  if (!safeFile(stat)) throw new RunLogCodecError("unsafe-file", path);
  const fd = fs.openSync(path, (writable ? fs.constants.O_RDWR : fs.constants.O_RDONLY) | noFollow | nonBlock);
  const file = { path, fd, stat };
  try { assertRunLogFile(file); return file; }
  catch (error) { fs.closeSync(fd); throw error; }
}

export function createRunLogFile(path: string): HeldRunLogFile {
  const fd = fs.openSync(path, fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_RDWR | noFollow, 0o600);
  try {
    const file = { path, fd, stat: fs.fstatSync(fd, { bigint: true }) };
    assertRunLogFile(file);
    return file;
  } catch (error) {
    try { fs.closeSync(fd); } catch { /* preserve initial custody failure */ }
    throw error;
  }
}

export function assertRunLogFile(file: HeldRunLogFile, links = 1n): void {
  const current = fs.lstatSync(file.path, { bigint: true });
  if (!safeFile(current, links) || !version(current, { ...file.stat, nlink: links } as fs.BigIntStats)
    || (file.fd !== undefined && !version(fs.fstatSync(file.fd, { bigint: true }), current))) {
    throw new RunLogCodecError("file-changed", file.path);
  }
}

/** Only after our writes: retain physical identity, then pin the completed artifact's version. */
export function sealRunLogFile(file: HeldRunLogFile): void {
  if (file.fd === undefined) throw new RunLogCodecError("closed-file", file.path);
  const stat = fs.fstatSync(file.fd, { bigint: true });
  if (!safeFile(stat) || !identity(stat, file.stat)) throw new RunLogCodecError("file-changed", file.path);
  file.stat = stat;
  assertRunLogFile(file);
}

export function closeRunLogFile(file: HeldRunLogFile | undefined): void {
  if (file?.fd !== undefined) {
    const fd = file.fd;
    // close can report an error after releasing the handle. Never retry its reused number.
    file.fd = undefined;
    fs.closeSync(fd);
  }
}

export function syncRunLogDirectory(directory: HeldRunLogDirectory): void {
  assertRunLogDirectory(directory);
  if (directory.fd !== undefined) fs.fsyncSync(directory.fd);
}

export function hashRunLogFile(file: HeldRunLogFile): string {
  assertRunLogFile(file);
  const hash = createHash("sha256");
  const chunk = Buffer.allocUnsafe(64 * 1024);
  let offset = 0;
  const size = Number(file.stat.size);
  while (offset < size) {
    const count = fs.readSync(file.fd!, chunk, 0, Math.min(chunk.length, size - offset), offset);
    if (count <= 0) throw new RunLogCodecError("short-read", file.path);
    hash.update(chunk.subarray(0, count));
    offset += count;
  }
  assertRunLogFile(file);
  return hash.digest("hex");
}
export const hashRunLogBytes = (bytes: Buffer): string => createHash("sha256").update(bytes).digest("hex");

export async function decodeOwnedRunLog(file: HeldRunLogFile): Promise<Buffer> {
  assertRunLogFile(file);
  if (file.stat.size > BigInt(RUN_LOG_BYTE_CAP)) throw new RunLogCodecError("encoded-too-large", file.path);
  const encoded = Buffer.allocUnsafe(Number(file.stat.size));
  let offset = 0;
  while (offset < encoded.length) {
    const count = fs.readSync(file.fd!, encoded, offset, encoded.length - offset, offset);
    if (count <= 0) throw new RunLogCodecError("short-read", file.path);
    offset += count;
  }
  assertRunLogFile(file);
  // One parser for readers, retirement verification AND restore. No writer-side frame parser.
  const { decodeRunLogFrame } = await import("../run-log-source.js");
  try { return decodeRunLogFrame(encoded, file.path); }
  catch (error) { throw new RunLogCodecError("invalid-frame", file.path, false, error); }
}

export interface RunLogCompression {
  kind: "compressed" | "already-compressed" | "skipped";
  reason?: string;
  originalBytes?: number;
  compressedBytes?: number;
}

/** Caller owns TRUE terminal custody: no writer, retry, relaunch, reattach or native-close debt. */
export async function compressTerminalRunLog(eventsPath: string): Promise<RunLogCompression> {
  const path = resolve(eventsPath);
  if (basename(path) !== "events.jsonl") throw new RunLogCodecError("invalid-path", path);
  let directory: HeldRunLogDirectory | undefined;
  let source: HeldRunLogFile | undefined;
  let target: HeldRunLogFile | undefined;
  let phase = "custody";
  let committed = false;
  try {
    directory = holdRunLogDirectory(path);
    // Windows FlushFileBuffers requires O_RDWR, even though source content is never written.
    source = openRunLogFile(path, true);
    if (!source) {
      target = openRunLogFile(`${path}.zst`);
      if (!target) return { kind: "skipped", reason: "source-missing" };
      const decoded = await decodeOwnedRunLog(target);
      assertRunLogDirectory(directory);
      assertRunLogFile(target);
      return { kind: "already-compressed", originalBytes: decoded.length, compressedBytes: Number(target.stat.size) };
    }
    const originalBytes = Number(source.stat.size);
    if (source.stat.size > BigInt(RUN_LOG_BYTE_CAP)) return { kind: "skipped", reason: "plain-too-large", originalBytes };
    fs.fsyncSync(source.fd!);
    const originalHash = hashRunLogFile(source);
    target = openRunLogFile(`${path}.zst`, true);
    if (!target) {
      const zlib = await import("node:zlib");
      if (typeof zlib.createZstdCompress !== "function") return { kind: "skipped", reason: "codec-unavailable", originalBytes };
      const { Transform, Readable } = await import("node:stream");
      const { pipeline } = await import("node:stream/promises");
      assertRunLogDirectory(directory);
      assertRunLogFile(source);
      phase = "exclusive-create";
      // Never retry EEXIST as permission to replace or delete somebody else's target.
      target = createRunLogFile(`${path}.zst`);
      phase = "encode";
      let encodedBytes = 0;
      const bound = new Transform({ transform(chunk: Buffer, _encoding, callback) {
        encodedBytes += chunk.length;
        callback(encodedBytes > RUN_LOG_BYTE_CAP ? new RunLogCodecError("encoded-too-large", `${path}.zst`) : null, chunk);
      } });
      const input = originalBytes === 0 ? Readable.from([])
        : fs.createReadStream(path, { fd: source.fd!, autoClose: false, start: 0, end: originalBytes - 1 });
      await pipeline(input, zlib.createZstdCompress({ params: {
        [zlib.constants.ZSTD_c_compressionLevel]: 3,
        [zlib.constants.ZSTD_c_checksumFlag]: 1,
      } }), bound, fs.createWriteStream(target.path, { fd: target.fd!, autoClose: false }));
      sealRunLogFile(target);
    }
    phase = "verify";
    fs.fsyncSync(target.fd!);
    const decoded = await decodeOwnedRunLog(target);
    if (hashRunLogBytes(decoded) !== originalHash) throw new RunLogCodecError("content-mismatch", target.path);
    assertRunLogFile(source);
    if (hashRunLogFile(source) !== originalHash) throw new RunLogCodecError("source-changed", path);
    fs.fsyncSync(source.fd!);
    syncRunLogDirectory(directory);
    const compressedBytes = Number(target.stat.size);
    // Release ALL owned file handles before Windows unlink; recheck paths after close.
    closeRunLogFile(source);
    closeRunLogFile(target);
    assertRunLogDirectory(directory);
    assertRunLogFile(source);
    assertRunLogFile(target);
    phase = "retire-plain";
    fs.unlinkSync(path);
    committed = true;
    phase = "post-commit-sync";
    syncRunLogDirectory(directory);
    return { kind: "compressed", originalBytes, compressedBytes };
  } catch (error) {
    if (error instanceof RunLogCodecError && !committed) throw error;
    throw new RunLogCodecError(phase, path, committed, error);
  } finally {
    // An incomplete exclusive final artifact is deliberate retry evidence. NEVER delete it here.
    for (const file of [source, target, directory]) { try { closeRunLogFile(file); } catch { /* preserve primary error */ } }
  }
}
