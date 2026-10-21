import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { syncDirectoryChain } from "../core/atomic-write.js";

const sameGeneration = (left: fs.Stats, right: fs.Stats): boolean =>
  left.isFile() && right.isFile() && left.dev === right.dev && left.ino === right.ino &&
  left.size === right.size && left.mtimeMs === right.mtimeMs && left.nlink === 1 && right.nlink === 1 &&
  (!process.getuid || left.uid === process.getuid()) && (!process.getuid || right.uid === process.getuid());
const sameSourceGeneration = (left: fs.Stats, right: fs.Stats): boolean =>
  left.isFile() && right.isFile() && left.dev === right.dev && left.ino === right.ino &&
  left.size === right.size && left.mtimeMs === right.mtimeMs &&
  (!process.getuid || left.uid === process.getuid()) && (!process.getuid || right.uid === process.getuid());

const sameBytes = (left: number, right: number, size: number): boolean => {
  const a = Buffer.allocUnsafe(64 * 1024), b = Buffer.allocUnsafe(64 * 1024);
  for (let offset = 0; offset < size;) {
    const length = Math.min(a.length, size - offset);
    let aRead = 0, bRead = 0;
    while (aRead < length) {
      const count = fs.readSync(left, a, aRead, length - aRead, offset + aRead);
      if (count <= 0) return false;
      aRead += count;
    }
    while (bRead < length) {
      const count = fs.readSync(right, b, bRead, length - bRead, offset + bRead);
      if (count <= 0) return false;
      bRead += count;
    }
    if (!a.subarray(0, length).equals(b.subarray(0, length))) return false;
    offset += length;
  }
  return true;
};

/** Keep one exact, same-directory copy of a raw trace before a later compactor rewrites its live name. */
export const ensureTraceSnapshot = (sourceFile: string, snapshotFile: string, expected: fs.Stats,
  sourceDescriptor?: number, preferHardLink = false): boolean => {
  if (path.dirname(path.resolve(sourceFile)) !== path.dirname(path.resolve(snapshotFile))) return false;
  let source = sourceDescriptor;
  let ownsSource = false;
  let target: number | undefined;
  let temporary: string | undefined;
  let ownsTemporary = false;
  try {
    const noFollow = fs.constants.O_NOFOLLOW ?? 0;
    if (source === undefined) {
      source = fs.openSync(sourceFile, fs.constants.O_RDONLY | noFollow);
      ownsSource = true;
    }
    const generationMatches = preferHardLink ? sameSourceGeneration : sameGeneration;
    const opened = fs.fstatSync(source);
    const named = fs.lstatSync(sourceFile);
    if (!generationMatches(opened, expected) || !generationMatches(named, expected)) return false;
    try {
      const existing = fs.lstatSync(snapshotFile);
      if (!generationMatches(existing, existing) || existing.size !== expected.size || existing.mtimeMs !== expected.mtimeMs) return false;
      const existingFd = fs.openSync(snapshotFile, fs.constants.O_RDONLY | noFollow);
      try {
        const held = fs.fstatSync(existingFd);
        if (!generationMatches(held, existing) || !generationMatches(fs.fstatSync(source), opened)) return false;
        if (held.dev === opened.dev && held.ino === opened.ino) return generationMatches(fs.lstatSync(sourceFile), opened);
        if (!sameGeneration(existing, existing)) return false;
        return sameBytes(source, existingFd, expected.size) && generationMatches(fs.fstatSync(source), opened) &&
          generationMatches(fs.lstatSync(sourceFile), opened);
      } finally { fs.closeSync(existingFd); }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") return false;
    }

    if (preferHardLink) {
      let linkedSnapshot = false;
      try { fs.linkSync(sourceFile, snapshotFile); linkedSnapshot = true; }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
      }
      if (linkedSnapshot) {
        const linked = fs.lstatSync(snapshotFile);
        if (!sameSourceGeneration(linked, opened) || linked.dev !== opened.dev || linked.ino !== opened.ino ||
            !sameSourceGeneration(fs.fstatSync(source), opened) || !sameSourceGeneration(fs.lstatSync(sourceFile), opened)) return false;
        syncDirectoryChain(path.dirname(snapshotFile));
        return sameSourceGeneration(fs.fstatSync(source), opened) && sameSourceGeneration(fs.lstatSync(sourceFile), opened);
      }
    }

    temporary = `${snapshotFile}.${process.pid}.${randomUUID()}.tmp`;
    target = fs.openSync(temporary, fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | noFollow, 0o600);
    ownsTemporary = true;
    const buffer = Buffer.allocUnsafe(64 * 1024);
    for (let offset = 0; offset < expected.size;) {
      const length = Math.min(buffer.length, expected.size - offset);
      const count = fs.readSync(source, buffer, 0, length, offset);
      if (count !== length) return false;
      let written = 0;
      while (written < count) {
        const next = fs.writeSync(target, buffer, written, count - written);
        if (next <= 0) return false;
        written += next;
      }
      offset += count;
    }
    if (!generationMatches(fs.fstatSync(source), opened) || !generationMatches(fs.lstatSync(sourceFile), opened)) return false;
    fs.fchmodSync(target, 0o600);
    fs.futimesSync(target, expected.atimeMs / 1_000, expected.mtimeMs / 1_000);
    fs.fsyncSync(target);
    const staged = fs.fstatSync(target);
    if (!staged.isFile() || staged.size !== expected.size || staged.mtimeMs !== expected.mtimeMs) return false;
    fs.closeSync(target); target = undefined;
    try { fs.linkSync(temporary, snapshotFile); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") return false;
      const existing = fs.openSync(snapshotFile, fs.constants.O_RDONLY | noFollow);
      try { return sameBytes(source, existing, expected.size); } finally { fs.closeSync(existing); }
    }
    const archived = fs.lstatSync(snapshotFile);
    if (archived.dev !== staged.dev || archived.ino !== staged.ino || archived.size !== expected.size || archived.mtimeMs !== expected.mtimeMs) return false;
    fs.unlinkSync(temporary); temporary = undefined; ownsTemporary = false;
    const final = fs.lstatSync(snapshotFile);
    if (final.nlink !== 1 || !final.isFile() || final.size !== expected.size || final.mtimeMs !== expected.mtimeMs) return false;
    syncDirectoryChain(path.dirname(snapshotFile));
    return generationMatches(fs.fstatSync(source), opened) && generationMatches(fs.lstatSync(sourceFile), opened);
  } catch { return false; }
  finally {
    if (target !== undefined) { try { fs.closeSync(target); } catch {} }
    if (ownsSource && source !== undefined) { try { fs.closeSync(source); } catch {} }
    if (ownsTemporary && temporary !== undefined) { try { fs.unlinkSync(temporary); } catch {} }
  }
};
