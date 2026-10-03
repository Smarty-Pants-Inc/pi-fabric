import fs from "node:fs";

interface SessionTextCache {
  dev: bigint;
  ino: bigint;
  size: number;
  mtimeNs: bigint;
  text: string;
}

const cache = new Map<string, SessionTextCache>();

const statFile = (file: string): fs.BigIntStats | undefined => {
  try {
    const stat = fs.statSync(file, { bigint: true });
    return stat.isFile() ? stat : undefined;
  } catch {
    return undefined;
  }
};

const readTail = (file: string, offset: number, size: number): Buffer | undefined => {
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, fs.constants.O_RDONLY);
    const stat = fs.fstatSync(fd, { bigint: true });
    if (!stat.isFile() || stat.size !== BigInt(size)) return undefined;
    const length = size - offset;
    const buffer = Buffer.alloc(Math.max(0, length));
    let read = 0;
    while (read < buffer.length) {
      const count = fs.readSync(fd, buffer, read, buffer.length - read, offset + read);
      if (count <= 0) return undefined;
      read += count;
    }
    return buffer;
  } catch {
    return undefined;
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); } catch { /* best effort */ }
    }
  }
};

/**
 * Read a session JSONL file without repeating an unchanged large read. Appends
 * are fetched from the previous byte offset; inode replacement and truncation
 * deliberately fall back to a complete read.
 */
export const readSessionText = (file: string): string | undefined => {
  const observed = statFile(file);
  if (!observed) {
    cache.delete(file);
    return undefined;
  }
  const previous = cache.get(file);
  const size = Number(observed.size);
  if (previous && previous.dev === observed.dev && previous.ino === observed.ino) {
    if (previous.size === size && previous.mtimeNs === observed.mtimeNs) return previous.text;
    if (size > previous.size) {
      const tail = readTail(file, previous.size, size);
      if (tail !== undefined) {
        const text = previous.text + tail.toString("utf8");
        const verified = statFile(file);
        if (verified && verified.dev === observed.dev && verified.ino === observed.ino &&
            Number(verified.size) === size) {
          cache.set(file, { dev: verified.dev, ino: verified.ino, size, mtimeNs: verified.mtimeNs, text });
          return text;
        }
      }
    }
  }
  try {
    const text = fs.readFileSync(file, "utf8");
    const verified = statFile(file);
    if (verified) {
      cache.set(file, {
        dev: verified.dev,
        ino: verified.ino,
        size: Number(verified.size),
        mtimeNs: verified.mtimeNs,
        text,
      });
    }
    return text;
  } catch {
    cache.delete(file);
    return undefined;
  }
};

/** Test-only cleanup hook; production callers never need to invalidate globally. */
export const clearSessionTextCache = (): void => cache.clear();
