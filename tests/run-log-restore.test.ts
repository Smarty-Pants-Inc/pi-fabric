import fs from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { constants, zstdCompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const modulePath = "../src/storage/run-log-restore.js";
const implementation = await import(modulePath);
const restore = async (path: string, options?: { apply?: boolean }) => {
  expect(implementation?.restoreRunLog, "new restore API must exist").toBeTypeOf("function");
  return implementation!.restoreRunLog(path, options);
};
const frame = (bytes: Buffer) => zstdCompressSync(bytes, {
  params: { [constants.ZSTD_c_compressionLevel]: 3, [constants.ZSTD_c_checksumFlag]: 1 },
});
const content = Buffer.from('{"type":"message","text":"restore raw \\u0000 bytes"}\r\n{"type":"done"}\n');
const tempPattern = /^\.events\.jsonl\.restore-[1-9][0-9]*-[0-9a-f]{32}\.tmp$/;
let directory: string;
let encodedPath: string;
let plainPath: string;
let removed: string[];
beforeEach(() => {
  directory = fs.mkdtempSync(join(tmpdir(), "runlog-restore-"));
  fs.chmodSync(directory, 0o700);
  plainPath = join(directory, "events.jsonl");
  encodedPath = `${plainPath}.zst`;
  fs.writeFileSync(encodedPath, frame(content), { mode: 0o600 });
  removed = [];
  vi.spyOn(fs, "unlinkSync").mockImplementation(path => { removed.push(String(path)); });
});
afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  vi.restoreAllMocks();
});

const refuses = async (operation: Promise<unknown>, reason?: string) => {
  await expect(operation).rejects.toMatchObject({ code: "run-log-codec", committed: false, ...(reason ? { reason } : {}) });
  expect(fs.existsSync(plainPath)).toBe(false);
  expect(fs.existsSync(encodedPath)).toBe(true);
};

// __CUT1241__: native Windows compressed-artifact publication/custody is deferred; retain every assertion for Linux.
describe.skipIf(process.platform === "win32")("additive per-artifact run-log restore: dry-run or recorded temp cleanup", () => {
  it("defaults to dry-run with no writes and preserves compressed bytes", async () => {
    const before = fs.readFileSync(encodedPath);
    expect(await restore(encodedPath)).toMatchObject({ kind: "dry-run", originalBytes: content.length });
    expect(fs.readdirSync(directory)).toEqual(["events.jsonl.zst"]);
    expect(fs.readFileSync(encodedPath)).toEqual(before);
    expect(removed).toEqual([]);
  });

  it("applies exact-byte restore by private fsynced temp and no-replace link, preserving zst", async () => {
    const before = fs.readFileSync(encodedPath);
    const link = vi.spyOn(fs, "linkSync");
    expect(await restore(encodedPath, { apply: true })).toMatchObject({ kind: "restored", originalBytes: content.length });
    expect(fs.readFileSync(plainPath)).toEqual(content);
    expect(fs.readFileSync(encodedPath)).toEqual(before);
    expect(link).toHaveBeenCalledTimes(1);
    const [temporary, destination] = link.mock.calls[0]!;
    expect(String(destination)).toBe(plainPath);
    expect(dirname(String(temporary))).toBe(directory);
    expect(basename(String(temporary))).toMatch(tempPattern);
    // Windows has no POSIX mode guarantee; exclusive creation is asserted in the Windows probe below.
    if (process.platform !== "win32") expect(fs.statSync(plainPath).mode & 0o777).toBe(0o600);
    expect(removed).toEqual([String(temporary)]);
    // Recorded no-op cleanup deliberately leaves the crash alias: retention must fail closed on nlink=2.
    expect(fs.statSync(plainPath).nlink).toBe(2);
  });

  it("verifies existing matching plain without overwriting or creating a temporary", async () => {
    fs.writeFileSync(plainPath, content, { mode: 0o600 });
    const pin = fs.statSync(plainPath, { bigint: true });
    expect(await restore(encodedPath, { apply: true })).toMatchObject({ kind: "already-restored" });
    expect(fs.statSync(plainPath, { bigint: true }).ino).toBe(pin.ino);
    expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "events.jsonl.zst"]);
    expect(removed).toEqual([]);
  });

  it("refuses existing mismatched plain and leaves both byte-for-byte unchanged", async () => {
    fs.writeFileSync(plainPath, "not the same", { mode: 0o600 });
    const encoded = fs.readFileSync(encodedPath);
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ code: "run-log-codec", reason: "content-mismatch", committed: false });
    expect(fs.readFileSync(plainPath, "utf8")).toBe("not the same");
    expect(fs.readFileSync(encodedPath)).toEqual(encoded);
    expect(removed).toEqual([]);
  });

  it("does not replace a plain file appearing at atomic publication", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const original = fs.linkSync;
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      fs.writeFileSync(to, "foreign plain", { mode: 0o600 });
      original(from, to);
    });
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ code: "run-log-codec", committed: false });
    expect(fs.readFileSync(plainPath, "utf8")).toBe("foreign plain");
    expect(removed).toHaveLength(1);
    expect(basename(removed[0]!)).toMatch(tempPattern);
  });

  it.each(["EACCES", "ENOSPC"])("never publishes partial plain on temp-write failure %s", async code => {
    vi.spyOn(fs, "writeSync").mockImplementation(() => { throw Object.assign(new Error(code), { code }); });
    await refuses(restore(encodedPath, { apply: true }));
    expect(removed).toHaveLength(1);
    expect(basename(removed[0]!)).toMatch(tempPattern);
  });

  it("verifies temporary readback hash before publication", async () => {
    const original = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      original(fd);
      if (fs.fstatSync(fd).isFile()) fs.writeSync(fd, Buffer.from("!"), 0, 1, 0);
    });
    await refuses(restore(encodedPath, { apply: true }));
    expect(removed).toHaveLength(1);
  });

  it("revalidates compressed source identity before publishing", async () => {
    const original = fs.fsyncSync;
    let changed = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      original(fd);
      if (!changed && fs.fstatSync(fd).isFile()) {
        changed = true;
        fs.appendFileSync(encodedPath, "trailing corruption");
      }
    });
    await refuses(restore(encodedPath, { apply: true }));
  });

  it("closes source and temporary handles before the no-replace publication", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const originalOpen = fs.openSync;
    const originalClose = fs.closeSync;
    const originalSync = fs.fsyncSync;
    const held = new Set<number>();
    const synced = new Set<bigint>();
    let temporaryInode: bigint | undefined;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd, { bigint: true });
      expect(stat.isFile(), "win32 must not fsync a directory").toBe(true);
      synced.add(stat.ino);
      originalSync(fd);
    });
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      if (String(path) === directory) throw Object.assign(new Error("win32 directory open refused"), { code: "EISDIR" });
      const fd = originalOpen(path, flags, mode);
      if (tempPattern.test(basename(String(path)))) {
        expect(Number(flags) & (fs.constants.O_CREAT | fs.constants.O_EXCL)).toBe(fs.constants.O_CREAT | fs.constants.O_EXCL);
        expect(mode).toBe(0o600);
        temporaryInode = fs.fstatSync(fd, { bigint: true }).ino;
      }
      if (fs.fstatSync(fd).isFile()) held.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { held.delete(fd); originalClose(fd); });
    const originalLink = fs.linkSync;
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => { expect(held.size).toBe(0); originalLink(from, to); });
    await restore(encodedPath, { apply: true });
    expect(fs.readFileSync(plainPath)).toEqual(content);
    expect(temporaryInode).toBeTypeOf("bigint");
    expect(synced.has(temporaryInode!)).toBe(true);
    expect(removed).toHaveLength(1);
    expect(fs.existsSync(encodedPath)).toBe(true);
  });

  it("refuses recorded win32 directory replacement before restore publication", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const originalStat = fs.lstatSync;
    const originalClose = fs.closeSync;
    let closed = false;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isFile()) closed = true;
      originalClose(fd);
    });
    vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(originalStat, fs, [path, options]);
      return String(path) === directory && closed ? Object.assign(Object.create(stat), { ino: stat.ino + 1n }) : stat;
    }) as typeof fs.lstatSync);
    await refuses(restore(encodedPath, { apply: true }), "directory-changed");
    expect(removed).toEqual([]);
    expect(fs.readdirSync(directory).some(name => tempPattern.test(name))).toBe(true);
  });

  it("reports exact-temp cleanup contention as committed and keeps the recoverable alias", async () => {
    vi.mocked(fs.unlinkSync).mockImplementation(() => { throw Object.assign(new Error("Windows handle contention"), { code: "EPERM" }); });
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ code: "run-log-codec", reason: "restore-temp-cleanup", committed: true });
    expect(fs.readFileSync(plainPath)).toEqual(content);
    expect(fs.statSync(plainPath).nlink).toBe(2);
    expect(fs.existsSync(encodedPath)).toBe(true);
  });

  it("reports post-publication directory sync failure as a committed durability warning", async () => {
    const original = fs.fsyncSync;
    if (process.platform === "win32") {
      const originalStat = fs.lstatSync;
      vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
        if (removed.length && String(path) === directory) throw new Error("post-cleanup custody check");
        return Reflect.apply(originalStat, fs, [path, options]);
      }) as typeof fs.lstatSync);
    }
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (removed.length && fs.fstatSync(fd).isDirectory()) throw new Error("post-commit sync failed");
      original(fd);
    });
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ code: "run-log-codec", reason: "post-commit-sync", committed: true });
    expect(fs.readFileSync(plainPath)).toEqual(content);
  });

  it("rejects checksum bit flips instead of publishing changed history", async () => {
    const encoded = fs.readFileSync(encodedPath);
    encoded[encoded.length - 1] = encoded[encoded.length - 1]! ^ 0x80;
    fs.writeFileSync(encodedPath, encoded);
    await refuses(restore(encodedPath, { apply: true }), "invalid-frame");
    expect(removed).toEqual([]);
  });

  it("requires all four checksum bytes after plain retirement through restore", async () => {
    const encoded = frame(content);
    expect(await restore(encodedPath)).toMatchObject({ kind: "dry-run", originalBytes: content.length });
    for (let missing = 1; missing <= 4; missing++) {
      fs.writeFileSync(encodedPath, encoded.subarray(0, -missing));
      await refuses(restore(encodedPath, { apply: true }), "invalid-frame");
    }
    expect(removed).toEqual([]);
  });

  it.each(["trailing", "second-frame", "skippable", "no-checksum"])("refuses noncanonical frame input: %s", async scenario => {
    const encoded = frame(content);
    const invalid = scenario === "trailing" ? Buffer.concat([encoded, Buffer.from("x")])
      : scenario === "second-frame" ? Buffer.concat([encoded, encoded])
      : scenario === "skippable" ? Buffer.concat([Buffer.from([0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0]), encoded])
      : zstdCompressSync(content);
    fs.writeFileSync(encodedPath, invalid);
    await refuses(restore(encodedPath, { apply: true }), "invalid-frame");
    expect(removed).toEqual([]);
  });

  it("refuses encoded physical oversize before allocating it", async () => {
    fs.truncateSync(encodedPath, 128 * 1024 * 1024 + 1);
    await refuses(restore(encodedPath, { apply: true }));
    expect(removed).toEqual([]);
  });

  it("refuses a decoded-size bomb without creating plain or temporary", async () => {
    fs.writeFileSync(encodedPath, frame(Buffer.alloc(128 * 1024 * 1024 + 1, 0x61)));
    await refuses(restore(encodedPath, { apply: true }), "invalid-frame");
    expect(fs.readdirSync(directory)).toEqual(["events.jsonl.zst"]);
    expect(removed).toEqual([]);
  });

  it("refuses unsafe hardlinked compressed sources", async () => {
    fs.linkSync(encodedPath, join(directory, "alias"));
    await refuses(restore(encodedPath, { apply: true }));
    expect(removed).toEqual([]);
  });

  it("CLI defaults to dry-run, accepts only supplied artifacts, and requires explicit --apply", async () => {
    expect(implementation?.main).toBeTypeOf("function");
    const output: string[] = [];
    const io = { out: (line: string) => output.push(line), err: (line: string) => output.push(line) };
    expect(await implementation!.main([encodedPath], io)).toBe(0);
    expect(fs.existsSync(plainPath)).toBe(false);
    expect(output.join("\n")).toContain("dry-run");
    expect(await implementation!.main(["--scan", directory], io)).toBe(2);
    expect(await implementation!.main(["--apply", encodedPath], io)).toBe(0);
    expect(fs.readFileSync(plainPath)).toEqual(content);
    expect(fs.existsSync(encodedPath)).toBe(true);
  });
});
