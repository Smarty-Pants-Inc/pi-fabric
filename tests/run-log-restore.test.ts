import fs from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { constants, zstdCompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { main, restoreRunLog as restore, RUN_LOG_RESTORE_TEMP } from "../src/storage/run-log-restore.js";

// Data safety: restore must be additive, byte-identical and explicit, including failures.
const frame = (bytes: Buffer) => zstdCompressSync(bytes, { params: { [constants.ZSTD_c_checksumFlag]: 1 } });
const content = Buffer.from('{"text":"α😀"}\r\n{"done":true}\n');
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
let directory: string;
let encodedPath: string;
let plainPath: string;
const temporary = () => fs.readdirSync(directory).filter(name => RUN_LOG_RESTORE_TEMP.test(name));
beforeEach(() => {
  directory = fs.realpathSync(fs.mkdtempSync(join(tmpdir(), "runlog-restore-")));
  fs.chmodSync(directory, 0o700);
  plainPath = join(directory, "events.jsonl");
  encodedPath = `${plainPath}.zst`;
  fs.writeFileSync(encodedPath, frame(content), { mode: 0o600 });
});
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  vi.restoreAllMocks();
  fs.rmSync(directory, { recursive: true, force: true });
});
const refuses = async (operation: Promise<unknown>, reason?: string) => {
  await expect(operation).rejects.toMatchObject({ code: "run-log-codec", committed: false, ...(reason ? { reason } : {}) });
  expect(fs.existsSync(plainPath)).toBe(false);
  expect(fs.existsSync(encodedPath)).toBe(true);
};

describe("additive run-log restore", () => {
  it("defaults to dry-run and creates no files", async () => {
    const encoded = fs.readFileSync(encodedPath);
    expect(await restore(encodedPath)).toMatchObject({ kind: "dry-run", path: plainPath, originalBytes: content.length });
    expect(fs.readdirSync(directory)).toEqual(["events.jsonl.zst"]);
    expect(fs.readFileSync(encodedPath)).toEqual(encoded);
  });

  it.each([content, Buffer.alloc(0), Buffer.from([0, 255, 254, 13, 10, 128, 0])])("restores byte-identically and leaves source unchanged (%j)", async bytes => {
    fs.writeFileSync(encodedPath, frame(bytes));
    const encoded = fs.readFileSync(encodedPath);
    const link = vi.spyOn(fs, "linkSync");
    expect(await restore(encodedPath, { apply: true })).toMatchObject({ kind: "restored", originalBytes: bytes.length });
    expect(fs.readFileSync(plainPath)).toEqual(bytes);
    expect(fs.readFileSync(encodedPath)).toEqual(encoded);
    expect(link).toHaveBeenCalledTimes(1);
    expect(basename(String(link.mock.calls[0]![0]))).toMatch(RUN_LOG_RESTORE_TEMP);
    expect(link.mock.calls[0]![1]).toBe(plainPath);
    expect(fs.statSync(plainPath).nlink).toBe(1);
    if (process.platform !== "win32") expect(fs.statSync(plainPath).mode & 0o777).toBe(0o600);
    expect(temporary()).toEqual([]);
    const inode = fs.statSync(plainPath, { bigint: true }).ino;
    expect(await restore(encodedPath, { apply: true })).toMatchObject({ kind: "already-restored" });
    expect(fs.statSync(plainPath, { bigint: true }).ino).toBe(inode);
  });

  it("never overwrites divergent existing plain", async () => {
    fs.writeFileSync(plainPath, "other history", { mode: 0o600 });
    const encoded = fs.readFileSync(encodedPath);
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ reason: "content-mismatch", committed: false });
    expect(fs.readFileSync(plainPath, "utf8")).toBe("other history");
    expect(fs.readFileSync(encodedPath)).toEqual(encoded);
    expect(temporary()).toEqual([]);
  });

  it("refuses a competing plain appearing at atomic publication and retains evidence", async () => {
    const original = fs.linkSync;
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      fs.writeFileSync(to, "foreign", { mode: 0o600 });
      original(from, to);
    });
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ reason: "restore-publish", committed: false });
    expect(fs.readFileSync(plainPath, "utf8")).toBe("foreign");
    expect(fs.readFileSync(encodedPath)).toEqual(frame(content));
    expect(temporary()).toHaveLength(1);
    expect(fs.readFileSync(join(directory, temporary()[0]!))).toEqual(content);
  });

  it.each(["ENOSPC", "EACCES"])("preserves source and partial evidence on temporary write failure %s", async code => {
    vi.spyOn(fs, "writeSync").mockImplementation(() => { throw Object.assign(new Error(code), { code }); });
    await refuses(restore(encodedPath, { apply: true }), "restore-temp-write");
    expect(temporary()).toHaveLength(1);
    expect(fs.statSync(join(directory, temporary()[0]!)).nlink).toBe(1);
  });

  it("verifies temporary readback SHA-256 before publication", async () => {
    const original = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isFile()) fs.writeSync(fd, Buffer.from("!"), 0, 1, 0);
      original(fd);
    });
    await refuses(restore(encodedPath, { apply: true }), "restore-hash-mismatch");
    expect(temporary()).toHaveLength(1);
  });

  it("revalidates the compressed source before publishing", async () => {
    const original = fs.fsyncSync;
    let changed = false;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      original(fd);
      if (!changed && fs.fstatSync(fd).isFile()) {
        changed = true;
        fs.appendFileSync(encodedPath, "trailing bytes");
      }
    });
    await refuses(restore(encodedPath, { apply: true }), "file-changed");
  });

  it.each(["checksum", "trailing", "second-frame", "skippable", "no-checksum"])("rejects invalid frames and retains exact evidence: %s", async scenario => {
    const encoded = frame(content);
    const invalid = scenario === "trailing" ? Buffer.concat([encoded, Buffer.from("x")])
      : scenario === "second-frame" ? Buffer.concat([encoded, encoded])
      : scenario === "skippable" ? Buffer.concat([Buffer.from("502a4d1800000000", "hex"), encoded])
      : scenario === "no-checksum" ? zstdCompressSync(content) : Buffer.from(encoded);
    if (scenario === "checksum") invalid[invalid.length - 1] = invalid[invalid.length - 1]! ^ 1;
    fs.writeFileSync(encodedPath, invalid);
    await refuses(restore(encodedPath, { apply: true }), "invalid-frame");
    expect(fs.readFileSync(encodedPath)).toEqual(invalid);
    expect(temporary()).toEqual([]);
  });

  it("requires all checksum bytes (including truncations accepted by native zstd)", async () => {
    const encoded = frame(content);
    for (let missing = 1; missing <= 4; missing++) {
      fs.writeFileSync(encodedPath, encoded.subarray(0, -missing));
      await refuses(restore(encodedPath, { apply: true }), "invalid-frame");
    }
    expect(temporary()).toEqual([]);
  });

  it.each(["encoded", "decoded"])("bounds %s size before publishing or creating temporary", async scenario => {
    if (scenario === "encoded") fs.truncateSync(encodedPath, 128 * 1024 * 1024 + 1);
    else fs.writeFileSync(encodedPath, frame(Buffer.alloc(128 * 1024 * 1024 + 1)));
    await refuses(restore(encodedPath, { apply: true }));
    expect(temporary()).toEqual([]);
  });

  it.each(["source", "plain"])("refuses hardlinked %s", async scenario => {
    if (scenario === "plain") fs.writeFileSync(plainPath, content, { mode: 0o600 });
    fs.linkSync(scenario === "source" ? encodedPath : plainPath, join(directory, "alias"));
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ reason: "unsafe-file", committed: false });
    expect(temporary()).toEqual([]);
  });

  it("closes Windows file descriptors before no-replace publication and fsyncs only files", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const originalOpen = fs.openSync;
    const originalClose = fs.closeSync;
    const originalSync = fs.fsyncSync;
    const originalLink = fs.linkSync;
    const held = new Set<number>();
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      expect(String(path)).not.toBe(directory);
      const fd = originalOpen(path, flags, mode);
      if (RUN_LOG_RESTORE_TEMP.test(basename(String(path)))) {
        expect(Number(flags) & (fs.constants.O_CREAT | fs.constants.O_EXCL)).toBe(fs.constants.O_CREAT | fs.constants.O_EXCL);
        expect(mode).toBe(0o600);
      }
      held.add(fd);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { held.delete(fd); originalClose(fd); });
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => { expect(fs.fstatSync(fd).isFile()).toBe(true); originalSync(fd); });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => { expect(held.size).toBe(0); originalLink(from, to); });
    await restore(encodedPath, { apply: true });
    expect(held.size).toBe(0);
  });

  it("refuses directory replacement before restore publication", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const originalStat = fs.lstatSync;
    const originalClose = fs.closeSync;
    let closed = false;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { closed = true; originalClose(fd); });
    vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(originalStat, fs, [path, options]);
      return String(path) === directory && closed ? Object.assign(Object.create(stat), { ino: stat.ino + 1n }) : stat;
    }) as typeof fs.lstatSync);
    await refuses(restore(encodedPath, { apply: true }), "directory-changed");
    expect(temporary()).toHaveLength(1);
  });

  it.each(["cleanup", "post-commit-sync"])("reports %s failure as committed without losing either representation", async fault => {
    const originalSync = fs.fsyncSync;
    const originalStat = fs.lstatSync;
    if (fault === "cleanup") vi.spyOn(fs, "unlinkSync").mockImplementation(() => { throw new Error("busy"); });
    else {
      vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        if (fs.fstatSync(fd).isDirectory() && fs.existsSync(plainPath) && temporary().length === 0) throw new Error("sync failed");
        originalSync(fd);
      });
      if (process.platform === "win32") vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
        if (String(path) === directory && fs.existsSync(plainPath) && temporary().length === 0) throw new Error("custody failed");
        return Reflect.apply(originalStat, fs, [path, options]);
      }) as typeof fs.lstatSync);
    }
    await expect(restore(encodedPath, { apply: true })).rejects.toMatchObject({ code: "run-log-codec", committed: true });
    expect(fs.readFileSync(plainPath)).toEqual(content);
    expect(fs.readFileSync(encodedPath)).toEqual(frame(content));
    expect(fs.statSync(plainPath).nlink).toBe(fault === "cleanup" ? 2 : 1);
  });

  it("CLI defaults to dry-run, rejects scans/options and applies only explicitly supplied artifacts", async () => {
    const output: string[] = [];
    const io = { out: (line: string) => output.push(line), err: (line: string) => output.push(line) };
    expect(await main([encodedPath], io)).toBe(0);
    expect(output.join("\n")).toContain("dry-run");
    expect(fs.existsSync(plainPath)).toBe(false);
    expect(await main(["--scan", directory], io)).toBe(2);
    expect(await main(["--apply", encodedPath], io)).toBe(0);
    expect(fs.readFileSync(plainPath)).toEqual(content);
    expect(await main([join(directory, "missing", "events.jsonl.zst")], io)).toBe(1);
    expect(await main([], io)).toBe(2);
    expect(await main(["--help"], io)).toBe(0);
  });
});
