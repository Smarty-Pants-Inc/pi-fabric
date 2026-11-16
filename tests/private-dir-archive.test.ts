import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { openPrivateDir, writeArchiveFile } from "../src/storage/private-dir.js";
import type { PrivateDirectory } from "../src/storage/private-dir.js";

const original = Buffer.from([0, 0x80, 0xff]);
const changed = Buffer.from([0, 0x81, 0xfe]); // Same length and UTF-8 decoding, different bytes.
const fixtures: { root: string; held: PrivateDirectory }[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  for (const { root, held } of fixtures.splice(0)) {
    held.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
const identity = (file: string) => { const { dev, ino } = fs.statSync(file); return { dev, ino }; };
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "private-dir-archive-"));
  const held = openPrivateDir(root);
  fixtures.push({ root, held });
  const name = "trace.bin", file = path.join(root, name);
  expect(writeArchiveFile(held, name, { bytes: original })).toBe(true);
  return { root, held, name, file };
};

// Keep: data safety at the archive custody boundary; real files exercise the bytes API and descriptor-only failures.
describe.skipIf(process.platform === "win32")("writeArchiveFile bytes retries", () => {
  it.each([1, 2])("preserves an identical entry with %i links and syncs its file and directory", links => {
    const { root, held, name, file } = setup();
    const custody = path.join(root, "custody.bin");
    if (links === 2) fs.linkSync(file, custody);
    const before = identity(file), synced: ReturnType<typeof identity>[] = [];
    const sync = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const { dev, ino } = fs.fstatSync(fd);
      synced.push({ dev, ino });
      sync(fd);
    });

    expect(writeArchiveFile(held, name, { bytes: Buffer.from(original) }, true)).toBe(true);
    expect(fs.readFileSync(file)).toEqual(original);
    expect(identity(file)).toEqual(before);
    expect(synced).toEqual([before, identity(root)]);
    if (links === 2) {
      expect(fs.readFileSync(custody)).toEqual(original);
      expect(identity(custody)).toEqual(before);
    }
  });

  it.each([false, true])("uses a writable existing Windows handle only for a durable retry (%s)", durable => {
    // Create the native held-directory fixture before changing only the retry's platform policy.
    const { root, held, name, file } = setup();
    const before = identity(file), synced: ReturnType<typeof identity>[] = [];
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    const open = fs.openSync, sync = fs.fsyncSync;
    const flagsByFd = new Map<number, string | number>();
    vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) => {
      const fd = open(target, flags, mode);
      flagsByFd.set(fd, flags);
      return fd;
    });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd), flags = flagsByFd.get(fd);
      if (stat.isFile() && (typeof flags !== "number" || (flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR)) === 0)) {
        throw Object.assign(new Error("Windows file fsync requires a writable handle"), { code: "EPERM" });
      }
      synced.push({ dev: stat.dev, ino: stat.ino });
      sync(fd);
    });

    try {
      Object.defineProperty(process, "platform", { ...platform, value: "win32" });
      expect(writeArchiveFile(held, name, { bytes: Buffer.from(original) }, durable)).toBe(true);
    } finally { Object.defineProperty(process, "platform", platform); }

    const access = durable ? fs.constants.O_RDWR : fs.constants.O_RDONLY;
    expect([...flagsByFd.values()]).toEqual([access | (fs.constants.O_NOFOLLOW ?? 0) | (fs.constants.O_NONBLOCK ?? 0)]);
    expect(synced).toEqual(durable ? [before, identity(root)] : []);
    expect(fs.readFileSync(file)).toEqual(original);
    expect(identity(file)).toEqual(before);
  });

  it("refuses a byte-identical destination symlink even without O_NOFOLLOW", () => {
    const { root, held, file } = setup();
    const name = "identical-link.bin", destination = path.join(root, name), entry = path.join(held.path, name);
    const before = identity(file);
    fs.symlinkSync(file, destination);
    // Match the source-open regression convention: strip the flag, keep real filesystem behavior.
    const open = fs.openSync;
    vi.spyOn(fs, "openSync").mockImplementation((target, flags, mode) =>
      open(target, target === entry && typeof flags === "number" ? flags & ~(fs.constants.O_NOFOLLOW ?? 0) : flags, mode));

    expect(writeArchiveFile(held, name, { bytes: Buffer.from(original) })).toBe(false);
    expect(fs.lstatSync(destination).isSymbolicLink()).toBe(true);
    expect(fs.readlinkSync(destination)).toBe(file);
    expect(fs.readFileSync(file)).toEqual(original);
    expect(identity(file)).toEqual(before);
  });

  it.each([
    [1, "equal-length", changed],
    [1, "different-length", Buffer.concat([changed, Buffer.from([0])])],
    [2, "equal-length", changed],
    [2, "different-length", Buffer.concat([changed, Buffer.from([0])])],
  ] as const)("replaces changed content only with one link (%i links, %s)", (links, _kind, bytes) => {
    const { root, held, name, file } = setup();
    const custody = path.join(root, "custody.bin");
    if (links === 2) fs.linkSync(file, custody);
    const before = identity(file);

    expect(writeArchiveFile(held, name, { bytes })).toBe(links === 1);
    expect(fs.readFileSync(file)).toEqual(links === 1 ? bytes : original);
    if (links === 1) expect(identity(file)).not.toEqual(before);
    else {
      expect(identity(file)).toEqual(before);
      expect(fs.readFileSync(custody)).toEqual(original);
      expect(identity(custody)).toEqual(before);
    }
  });

  it("refuses an identical foreign-owned entry before accepting its bytes", () => {
    const { held, name, file } = setup();
    const before = identity(file), fstat = fs.fstatSync;
    let injected = false;
    vi.spyOn(fs, "fstatSync").mockImplementation((fd, options) => {
      const stat = fstat(fd, options);
      if (stat.dev === before.dev && stat.ino === before.ino) {
        Object.assign(stat, { uid: Number(stat.uid) + 1 });
        injected = true;
      }
      return stat;
    });

    expect(writeArchiveFile(held, name, { bytes: original })).toBe(false);
    expect(injected).toBe(true);
    expect(fs.readFileSync(file)).toEqual(original);
    expect(identity(file)).toEqual(before);
  });

  it("refuses a descriptor comparison read failure instead of replacing a single-link entry", () => {
    const { held, name, file } = setup();
    const before = identity(file), read = fs.readFileSync;
    let injected = false;
    vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, options?: unknown) => {
      if (typeof target === "number") {
        const stat = fs.fstatSync(target);
        if (stat.dev === before.dev && stat.ino === before.ino) {
          injected = true;
          throw Object.assign(new Error("descriptor read failed"), { code: "EIO" });
        }
      }
      return read(target, options as BufferEncoding);
    }) as typeof fs.readFileSync);

    expect(writeArchiveFile(held, name, { bytes: changed })).toBe(false);
    expect(injected).toBe(true);
    expect(fs.readFileSync(file)).toEqual(original);
    expect(identity(file)).toEqual(before);
  });

  it.each(["file", "directory"])("propagates an identical entry's %s sync failure", stage => {
    const { held, name, file } = setup();
    const before = identity(file), sync = fs.fsyncSync, failure = new Error(`${stage} sync failed`);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if ((fd === held.fd) === (stage === "directory")) throw failure;
      sync(fd);
    });

    expect(() => writeArchiveFile(held, name, { bytes: original }, true)).toThrow(failure);
    expect(fs.readFileSync(file)).toEqual(original);
    expect(identity(file)).toEqual(before);
  });
});
