import fs from "node:fs";
import type { BigIntStats } from "node:fs";

const FRAME_CAP = 128 * 1024 * 1024;
const CACHE_BYTES = 16 * 1024 * 1024;
const CACHE_ENTRIES = 32;
const decodedCache = new Map<string, Buffer>();
let cacheBytes = 0;

export class RunLogReadError extends Error {
  readonly code = "run-log-codec";
  constructor(readonly path: string, readonly reason: string, options?: ErrorOptions) {
    super(`run-log-codec: ${path}: ${reason}`, options);
    this.name = "RunLogReadError";
  }
}

export interface RunLogSource {
  /** Decoded byte length; stat.size remains the physical artifact size. */
  size: number;
  stat: BigIntStats;
  generation: string;
  read(buffer: Buffer, offset: number, length: number, position: number): number;
  close(): void;
}

const ioError = (path: string, error: unknown): RunLogReadError => {
  if (error instanceof RunLogReadError) return error;
  const code = (error as NodeJS.ErrnoException)?.code;
  return new RunLogReadError(path, `${code ?? "read failed"}: ${error instanceof Error ? error.message : String(error)}`, { cause: error });
};
const missing = (error: unknown): boolean => (error as NodeJS.ErrnoException)?.code === "ENOENT";

const openPhysical = (path: string): { descriptor: number; stat: BigIntStats; encoded: boolean } => {
  // Windows compression/custody is off: retain main's plain open/fstat/read path.
  const plainOnly = process.platform === "win32";
  const noFollow = fs.constants.O_NOFOLLOW ?? 0;
  const flags = plainOnly ? "r" : fs.constants.O_RDONLY | noFollow | (fs.constants.O_NONBLOCK ?? 0);
  const candidate = (physicalPath: string, encoded: boolean) => {
    // POSIX uses its atomic flag, or binds a pre-open regular file to the held FD.
    const expected = plainOnly || noFollow ? undefined : fs.lstatSync(physicalPath, { bigint: true });
    if (expected && (expected.isSymbolicLink() || !expected.isFile())) {
      throw new RunLogReadError(physicalPath, "not a regular no-follow source");
    }
    const descriptor = fs.openSync(physicalPath, flags);
    try {
      const stat = fs.fstatSync(descriptor, { bigint: true });
      if (!stat.isFile()) throw new RunLogReadError(physicalPath, "not a regular file");
      if (expected && (stat.dev !== expected.dev || stat.ino !== expected.ino)) {
        throw new RunLogReadError(physicalPath, "physical source changed while opening");
      }
      return { descriptor, stat, encoded };
    } catch (error) {
      try { fs.closeSync(descriptor); } catch {}
      throw ioError(physicalPath, error);
    }
  };
  try { return candidate(path, false); } catch (error) {
    if (!missing(error)) throw ioError(path, error);
    // Windows and native session/mesh logs retain their existing plain format.
    if (plainOnly || !/(?:^|[/\\])events\.jsonl$/.test(path)) throw error;
  }
  try { return candidate(`${path}.zst`, true); } catch (error) {
    if (missing(error)) throw error;
    throw ioError(`${path}.zst`, error);
  }
};

/** Physical metadata only: never load zlib or decode a compressed artifact. */
export const statRunLogSource = (path: string): BigIntStats => {
  const opened = openPhysical(path);
  try { return opened.stat; } finally {
    try { fs.closeSync(opened.descriptor); } catch (error) { throw ioError(opened.encoded ? `${path}.zst` : path, error); }
  }
};

/** Require one complete standard checksummed frame; Node verifies the checksum value. */
export const validateRunLogZstdFrame = (encoded: Buffer, path = "events.jsonl.zst"): void => {
  const invalid = (reason: string): never => { throw new RunLogReadError(path, reason); };
  if (encoded.length > FRAME_CAP) invalid("encoded input exceeds 128 MiB cap");
  if (encoded.length < 6) invalid("truncated zstd frame header");
  if (encoded.readUInt32LE(0) !== 0xfd2fb528) invalid("expected a standard zstd frame");
  const header = encoded[4]!;
  if ((header & 0x18) !== 0 || (header & 3) !== 0) invalid("unsupported zstd frame header");
  if ((header & 4) === 0) invalid("zstd frame checksum is required");
  const single = (header & 0x20) !== 0;
  const contentBytes = [single ? 1 : 0, 2, 4, 8][header >>> 6]!;
  let offset = 5;
  if (!single) {
    const window = encoded[offset++]!;
    const base = 2 ** (10 + (window >>> 3));
    if (base + (base / 8) * (window & 7) > FRAME_CAP) invalid("zstd window exceeds 128 MiB cap");
  }
  if (offset + contentBytes > encoded.length) invalid("truncated zstd content size");
  if (contentBytes > 0) {
    const size = contentBytes === 8 ? encoded.readBigUInt64LE(offset)
      : BigInt(encoded.readUIntLE(offset, contentBytes) + (contentBytes === 2 ? 256 : 0));
    if (size > BigInt(FRAME_CAP)) invalid("decoded output exceeds 128 MiB cap");
  }
  offset += contentBytes;
  let last = false;
  do {
    if (offset + 3 > encoded.length) invalid("truncated zstd block header");
    const block = encoded.readUIntLE(offset, 3);
    const type = (block >>> 1) & 3;
    const size = block >>> 3;
    if (type === 3 || size > 128 * 1024) invalid("invalid zstd block type or size");
    last = (block & 1) !== 0;
    offset += 3 + (type === 1 ? 1 : size);
    if (offset > encoded.length) invalid("truncated zstd block");
  } while (!last);
  if (offset + 4 !== encoded.length) invalid("truncated checksum or trailing zstd bytes");
};

/** Shared by read-through, writer verification and additive restore. */
export const decodeRunLogFrame = (encoded: Buffer, path: string): Buffer => {
  validateRunLogZstdFrame(encoded, path);
  try {
    const zlib = process.getBuiltinModule("node:zlib") as typeof import("node:zlib") | undefined;
    if (!zlib?.zstdDecompressSync) throw new Error("this Node runtime has no built-in zstd decoder");
    return zlib.zstdDecompressSync(encoded, { maxOutputLength: FRAME_CAP });
  } catch (error) {
    throw ioError(path, error);
  }
};

const cacheKey = (stat: BigIntStats): string => `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeNs}`;
const samePhysical = (a: BigIntStats, b: BigIntStats): boolean => cacheKey(a) === cacheKey(b);

const decodedFromDescriptor = (descriptor: number, stat: BigIntStats, path: string): Buffer => {
  if (stat.size > BigInt(FRAME_CAP)) throw new RunLogReadError(path, "encoded input exceeds 128 MiB cap");
  const key = cacheKey(stat);
  const cached = decodedCache.get(key);
  if (cached) {
    decodedCache.delete(key);
    decodedCache.set(key, cached);
    return cached;
  }
  const encoded = Buffer.allocUnsafe(Number(stat.size));
  let offset = 0;
  while (offset < encoded.length) {
    const count = fs.readSync(descriptor, encoded, offset, encoded.length - offset, offset);
    if (count <= 0) throw new RunLogReadError(path, "truncated compressed artifact");
    offset += count;
  }
  if (!samePhysical(stat, fs.fstatSync(descriptor, { bigint: true }))) {
    throw new RunLogReadError(path, "compressed artifact changed while reading");
  }
  const decoded = decodeRunLogFrame(encoded, path);
  if (decoded.length <= CACHE_BYTES) {
    decodedCache.set(key, decoded);
    cacheBytes += decoded.length;
    while (decodedCache.size > CACHE_ENTRIES || cacheBytes > CACHE_BYTES) {
      const oldest = decodedCache.keys().next().value!;
      cacheBytes -= decodedCache.get(oldest)!.length;
      decodedCache.delete(oldest);
    }
  }
  return decoded;
};

/** Plain-first; POSIX has ENOENT-only zstd fallback. The held artifact supplies physical generation. */
export const openRunLogSource = (path: string): RunLogSource => {
  const { descriptor, stat, encoded } = openPhysical(path);
  const physicalPath = encoded ? `${path}.zst` : path;
  try {
    const decoded = encoded ? decodedFromDescriptor(descriptor, stat, physicalPath) : undefined;
    const size = decoded?.length ?? Number(stat.size);
    if (!Number.isSafeInteger(size)) throw new RunLogReadError(path, "file size exceeds safe byte offsets");
    let closed = false;
    return {
      size, stat, generation: `${stat.dev}:${stat.ino}`,
      read(buffer, offset, length, position) {
        if (closed) throw new RunLogReadError(physicalPath, "source is closed");
        try {
          return decoded ? decoded.copy(buffer, offset, position, position + length)
            : fs.readSync(descriptor, buffer, offset, length, position);
        } catch (error) { throw ioError(physicalPath, error); }
      },
      close() {
        if (closed) return;
        closed = true;
        try { fs.closeSync(descriptor); } catch (error) { throw ioError(physicalPath, error); }
      },
    };
  } catch (error) {
    try { fs.closeSync(descriptor); } catch {}
    throw ioError(physicalPath, error);
  }
};

export const readRunLogBuffer = (path: string): Buffer => {
  const source = openRunLogSource(path);
  try {
    if (source.size > FRAME_CAP) throw new RunLogReadError(path, "decoded output exceeds 128 MiB cap");
    const buffer = Buffer.allocUnsafe(source.size);
    let offset = 0;
    while (offset < buffer.length) {
      const count = source.read(buffer, offset, buffer.length - offset, offset);
      if (count <= 0) throw new RunLogReadError(path, "source changed while reading");
      offset += count;
    }
    return buffer;
  } finally { source.close(); }
};

export const readRunLogText = (path: string): string => readRunLogBuffer(path).toString("utf8");
