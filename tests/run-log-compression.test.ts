import fs from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { constants, zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readRunLogPage } from "../src/log-tail.js";

const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
const getuidDescriptor = Object.getOwnPropertyDescriptor(process, "getuid");
const modulePath = "../src/storage/run-log-compression.js";
const implementation = await import(modulePath);
const compress = async (path: string) => {
  expect(implementation?.compressTerminalRunLog, "new compressor API must exist").toBeTypeOf("function");
  return implementation!.compressTerminalRunLog(path);
};
const frame = (bytes: Buffer) => zstdCompressSync(bytes, {
  params: { [constants.ZSTD_c_compressionLevel]: 3, [constants.ZSTD_c_checksumFlag]: 1 },
});
const content = Buffer.from('{"type":"message","text":"exact raw bytes \\u0000"}\r\n{"type":"done"}\n');
let directory: string;
let events: string;
let removed: string[];
beforeEach(() => {
  directory = fs.mkdtempSync(join(tmpdir(), "runlog-codec-"));
  fs.chmodSync(directory, 0o700);
  events = join(directory, "events.jsonl");
  fs.writeFileSync(events, content, { mode: 0o600 });
  removed = [];
  // Product deletion is RECORDED ONLY locally. The source remains for inspection.
  vi.spyOn(fs, "unlinkSync").mockImplementation(path => { removed.push(String(path)); });
});
afterEach(() => {
  Object.defineProperty(process, "platform", platformDescriptor);
  if (getuidDescriptor) Object.defineProperty(process, "getuid", getuidDescriptor);
  else Reflect.deleteProperty(process, "getuid");
  vi.restoreAllMocks();
});

const failure = async (operation: Promise<unknown>, reason?: string) => {
  const error = await operation.then(() => undefined, value => value);
  expect(error).toMatchObject({ code: "run-log-codec", committed: false });
  expect(error.path).toBeTypeOf("string");
  if (reason) expect(error.reason).toBe(reason);
  expect(removed).toEqual([]);
  expect(fs.readFileSync(events)).toEqual(content);
  return error;
};

describe("terminal run-log compression: recorded retirement only", () => {
  it("publishes a checksummed standard frame, verifies exact bytes, and records plain retirement", async () => {
    const result = await compress(events);
    expect(result).toMatchObject({ kind: "compressed", originalBytes: content.length });
    const encoded = fs.readFileSync(`${events}.zst`);
    expect(encoded.readUInt32LE(0)).toBe(0xfd2fb528);
    expect(encoded[4]! & 4).toBe(4);
    expect(zstdDecompressSync(encoded)).toEqual(content);
    expect(result.compressedBytes).toBe(encoded.length);
    expect(removed).toEqual([events]);
    expect(fs.statSync(`${events}.zst`).nlink).toBe(1);
  });

  it("opens the source O_RDWR without truncation and closes every file before retirement (Windows contract)", async () => {
    const originalOpen = fs.openSync;
    const originalClose = fs.closeSync;
    Object.defineProperty(process, "platform", { value: "win32" });
    const held = new Set<number>();
    const synced = new Set<bigint>();
    const flags: number[] = [];
    const targetFlags: number[] = [];
    const originalSync = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd, { bigint: true });
      expect(stat.isFile(), "win32 must not fsync a directory").toBe(true);
      synced.add(stat.ino);
      originalSync(fd);
    });
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flag: number | string, mode?: number) => {
      if (String(path) === directory) throw Object.assign(new Error("win32 directory open refused"), { code: "EISDIR" });
      const fd = originalOpen(path, flag, mode);
      if (String(path) === events || String(path) === `${events}.zst`) {
        held.add(fd);
        if (String(path) === events) flags.push(Number(flag));
        else targetFlags.push(Number(flag));
      }
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { held.delete(fd); originalClose(fd); });
    vi.mocked(fs.unlinkSync).mockImplementation(path => { expect(held.size).toBe(0); removed.push(String(path)); });
    await compress(events);
    expect(flags[0]! & fs.constants.O_RDWR).toBe(fs.constants.O_RDWR);
    expect(flags[0]! & fs.constants.O_TRUNC).toBe(0);
    expect(targetFlags.some(flag => (flag & (fs.constants.O_CREAT | fs.constants.O_EXCL)) === (fs.constants.O_CREAT | fs.constants.O_EXCL))).toBe(true);
    expect(synced.has(fs.statSync(events, { bigint: true }).ino)).toBe(true);
    expect(synced.has(fs.statSync(`${events}.zst`, { bigint: true }).ino)).toBe(true);
    expect(removed).toEqual([events]);
  });

  it("retries an owned matching final frame without rewriting it", async () => {
    const encoded = frame(content);
    fs.writeFileSync(`${events}.zst`, encoded, { mode: 0o600 });
    const pin = fs.statSync(`${events}.zst`, { bigint: true });
    expect(await compress(events)).toMatchObject({ kind: "compressed" });
    expect(fs.statSync(`${events}.zst`, { bigint: true }).ino).toBe(pin.ino);
    expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
    expect(removed).toEqual([events]);
  });

  it("recognizes an owned complete already-compressed artifact without creating plain", async () => {
    const retired = join(directory, "retired");
    fs.mkdirSync(retired, { mode: 0o700 });
    const path = join(retired, "events.jsonl");
    fs.writeFileSync(`${path}.zst`, frame(content), { mode: 0o600 });
    expect(await compress(path)).toMatchObject({ kind: "already-compressed", originalBytes: content.length });
    expect(fs.existsSync(path)).toBe(false);
    expect(removed).toEqual([]);
  });

  it("distinguishes absent-both from a codec failure", async () => {
    const missing = join(directory, "missing");
    fs.mkdirSync(missing, { mode: 0o700 });
    expect(await compress(join(missing, "events.jsonl"))).toMatchObject({ kind: "skipped", reason: "source-missing" });
    expect(removed).toEqual([]);
  });

  it("keeps both representations on retry mismatch", async () => {
    const encoded = frame(Buffer.from("different history\n"));
    fs.writeFileSync(`${events}.zst`, encoded, { mode: 0o600 });
    await failure(compress(events), "content-mismatch");
    expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
  });

  it("rejects a corrupt existing checksum without retiring plain", async () => {
    const encoded = frame(content);
    encoded[encoded.length - 1] = encoded[encoded.length - 1]! ^ 0x80;
    fs.writeFileSync(`${events}.zst`, encoded, { mode: 0o600 });
    await failure(compress(events), "invalid-frame");
    expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
  });

  it("keeps plain and the partial final artifact after ENOSPC", async () => {
    // Fault the actual streaming destination, not the compressor or decoder.
    const originalWrite = fs.write;
    vi.spyOn(fs, "write").mockImplementation(((...args: unknown[]) => {
      const fd = args[0] as number;
      if (fs.fstatSync(fd).isFile() && fs.fstatSync(fd).ino === fs.statSync(`${events}.zst`).ino) {
        const callback = args.at(-1) as (error: Error) => void;
        callback(Object.assign(new Error("disk full"), { code: "ENOSPC" }));
        return;
      }
      return Reflect.apply(originalWrite, fs, args);
    }) as typeof fs.write);
    await failure(compress(events));
    expect(fs.existsSync(`${events}.zst`)).toBe(true);
  });

  it("does not touch a final target that appears before exclusive create", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const original = fs.openSync;
    let injected = false;
    const foreign = frame(Buffer.from("foreign"));
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      if (String(path) === `${events}.zst` && typeof flags === "number" && (flags & fs.constants.O_EXCL) && !injected) {
        injected = true;
        fs.writeFileSync(path, foreign, { mode: 0o600 });
      }
      return original(path, flags, mode);
    }) as typeof fs.openSync);
    await failure(compress(events));
    expect(fs.readFileSync(`${events}.zst`)).toEqual(foreign);
  });

  it("refuses a changed source after frame verification", async () => {
    const original = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      original(fd);
      if (fs.fstatSync(fd).isFile() && fs.fstatSync(fd).ino === fs.statSync(`${events}.zst`, { throwIfNoEntry: false })?.ino) {
        fs.appendFileSync(events, "late writer\n");
      }
    });
    await expect(compress(events)).rejects.toMatchObject({ code: "run-log-codec", committed: false });
    expect(removed).toEqual([]);
    expect(fs.readFileSync(events).subarray(0, content.length)).toEqual(content);
  });

  it("refuses a replaced private directory before commit", async () => {
    const original = fs.fsyncSync;
    const originalStat = fs.lstatSync;
    let changed = false;
    if (process.platform === "win32") vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(originalStat, fs, [path, options]);
      return changed && String(path) === directory ? Object.assign(Object.create(stat), { ino: stat.ino + 1n }) : stat;
    }) as typeof fs.lstatSync);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      original(fd);
      if (!changed && fs.fstatSync(fd).isFile() && fs.fstatSync(fd).ino === fs.statSync(`${events}.zst`, { throwIfNoEntry: false })?.ino) {
        changed = true;
        // Windows cannot reliably rename a directory containing open files: record its identity change.
        if (process.platform !== "win32") {
          fs.renameSync(directory, `${directory}-held`);
          fs.mkdirSync(directory, { mode: 0o700 });
          fs.writeFileSync(events, content, { mode: 0o600 });
        }
      }
    });
    await failure(compress(events));
    expect(fs.readFileSync(join(process.platform === "win32" ? directory : `${directory}-held`, "events.jsonl"))).toEqual(content);
  });

  it.each(["EPERM", "EACCES", "EBUSY"])("reports Windows unlink contention %s without claiming commit", async code => {
    Object.defineProperty(process, "platform", { value: "win32" });
    vi.mocked(fs.unlinkSync).mockImplementation(() => { throw Object.assign(new Error(code), { code }); });
    await failure(compress(events), "retire-plain");
    expect(zstdDecompressSync(fs.readFileSync(`${events}.zst`))).toEqual(content);
  });

  it("reports post-commit directory-sync failure as committed, never as plain preserved", async () => {
    const original = fs.fsyncSync;
    // Windows has no directory fsync; the same post-commit wrapper still revalidates custody.
    if (process.platform === "win32") {
      const originalStat = fs.lstatSync;
      vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
        if (removed.length && String(path) === directory) throw Object.assign(new Error("post-unlink custody check"), { code: "EIO" });
        return Reflect.apply(originalStat, fs, [path, options]);
      }) as typeof fs.lstatSync);
    }
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (removed.length && fs.fstatSync(fd).isDirectory()) throw Object.assign(new Error("sync after unlink"), { code: "EIO" });
      original(fd);
    });
    await expect(compress(events)).rejects.toMatchObject({ code: "run-log-codec", reason: "post-commit-sync", committed: true });
    expect(removed).toEqual([events]);
    expect(zstdDecompressSync(fs.readFileSync(`${events}.zst`))).toEqual(content);
  });

  it("skips an over-cap plain source observably without creating a target", async () => {
    fs.truncateSync(events, 128 * 1024 * 1024 + 1);
    expect(await compress(events)).toMatchObject({ kind: "skipped", reason: "plain-too-large" });
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(removed).toEqual([]);
  });

  it("rejects physical encoded oversize before allocation", async () => {
    fs.writeFileSync(`${events}.zst`, frame(content), { mode: 0o600 });
    fs.truncateSync(`${events}.zst`, 128 * 1024 * 1024 + 1);
    const alloc = vi.spyOn(Buffer, "allocUnsafe");
    await failure(compress(events));
    expect(alloc.mock.calls.every(([bytes]) => bytes <= 128 * 1024 * 1024)).toBe(true);
  });

  it.each(["source-link", "target-link", "source-symlink", "target-symlink", "public-directory", "foreign-owner", "fifo"])("refuses unsafe custody: %s", async scenario => {
    if (scenario === "source-link") fs.linkSync(events, join(directory, "alias"));
    if (scenario === "target-link") {
      fs.writeFileSync(`${events}.zst`, frame(content), { mode: 0o600 });
      fs.linkSync(`${events}.zst`, join(directory, "alias"));
    }
    // POSIX mode/owner faults are recorded even on Windows, which has no native POSIX contract.
    if (scenario === "public-directory") Object.defineProperty(process, "platform", { value: "linux" });
    if (scenario === "foreign-owner") Object.defineProperty(process, "getuid", {
      configurable: true, value: () => Number(fs.statSync(directory).uid),
    });
    // Symlink/foreign-owner/FIFO metadata are injected: no native special-file creation or deletion.
    const original = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(original, fs, [path, options]);
      if (String(path) === directory && scenario === "public-directory") {
        return Object.assign(Object.create(stat), { mode: typeof stat.mode === "bigint" ? stat.mode | 0o077n : stat.mode | 0o077 });
      }
      const selected = String(path) === (scenario.startsWith("target") ? `${events}.zst` : events);
      if (selected && scenario.endsWith("symlink")) return Object.assign(Object.create(stat), { isSymbolicLink: () => true });
      if (selected && scenario === "foreign-owner") return Object.assign(Object.create(stat), { uid: typeof stat.uid === "bigint" ? stat.uid + 1n : stat.uid + 1 });
      if (selected && scenario === "fifo") return Object.assign(Object.create(stat), { isFile: () => false, isFIFO: () => true });
      return stat;
    }) as typeof fs.lstatSync);
    if (scenario === "target-symlink") fs.writeFileSync(`${events}.zst`, frame(content), { mode: 0o600 });
    await failure(compress(events));
  });

  it.each(["symlink", "realpath-alias", "identity-change"])("refuses recorded win32 directory custody: %s", async scenario => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const originalStat = fs.lstatSync;
    const originalRealpath = fs.realpathSync;
    let retiring = false;
    const originalClose = fs.closeSync;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isFile()) retiring = true;
      originalClose(fd);
    });
    vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(originalStat, fs, [path, options]);
      if (String(path) !== directory) return stat;
      if (scenario === "symlink") return Object.assign(Object.create(stat), { isSymbolicLink: () => true });
      if (scenario === "identity-change" && retiring) return Object.assign(Object.create(stat), { ino: stat.ino + 1n });
      return stat;
    }) as typeof fs.lstatSync);
    if (scenario === "realpath-alias") vi.spyOn(fs, "realpathSync").mockImplementation(((path: fs.PathLike) =>
      String(path) === directory ? `${directory}-alias` : originalRealpath(path)) as typeof fs.realpathSync);
    const error = await failure(compress(events));
    expect(error.reason).toBe(scenario === "identity-change" ? "directory-changed" : "unsafe-directory");
  });

  it("closes the exclusively created target if its initial fstat fails", async () => {
    const originalOpen = fs.openSync;
    const originalStat = fs.fstatSync;
    const originalClose = fs.closeSync;
    let created: number | undefined;
    const closed: number[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: number | string, mode?: number) => {
      const fd = originalOpen(path, flags, mode);
      if (String(path) === `${events}.zst` && typeof flags === "number" && (flags & fs.constants.O_EXCL)) created = fd;
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: unknown) => {
      if (fd === created) throw Object.assign(new Error("initial target stat failed"), { code: "EIO" });
      return Reflect.apply(originalStat, fs, [fd, options]);
    }) as typeof fs.fstatSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { closed.push(fd); originalClose(fd); });
    try {
      await failure(compress(events));
      expect(created).toBeTypeOf("number");
      expect(closed.filter(fd => fd === created)).toHaveLength(1);
    } finally {
      // Test-owned leaked-handle recovery also runs on the deliberately red implementation.
      if (created !== undefined && !closed.includes(created)) originalClose(created);
    }
  });

  it("does not retry a failed close or retire plain after losing its source descriptor", async () => {
    const originalOpen = fs.openSync;
    const originalClose = fs.closeSync;
    let source: number | undefined;
    let injected = false;
    const closed: number[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: number | string, mode?: number) => {
      const fd = originalOpen(path, flags, mode);
      if (String(path) === events) source = fd;
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => {
      closed.push(fd);
      originalClose(fd);
      if (fd === source && !injected) {
        injected = true;
        throw Object.assign(new Error("close completed with an error"), { code: "EIO" });
      }
    });
    const error = await compress(events).then(() => undefined, value => value);
    // Snapshot before test readback opens can reuse the now-closed descriptor number.
    const sourceCloseAttempts = closed.filter(fd => fd === source).length;
    expect(error).toMatchObject({ code: "run-log-codec", committed: false });
    expect(sourceCloseAttempts).toBe(1);
    expect(removed).toEqual([]);
    expect(fs.readFileSync(events)).toEqual(content);
    expect(zstdDecompressSync(fs.readFileSync(`${events}.zst`))).toEqual(content);
  });

  // Real product unlink is forbidden locally. The existing Ubuntu/Windows hosted jobs own this proof.
  it.runIf(process.env.GITHUB_ACTIONS === "true")("HOSTED-ONLY Ubuntu+Windows compress -> read -> additive restore roundtrip with real unlink", async () => {
    vi.mocked(fs.unlinkSync).mockRestore();
    const expectedPage = readRunLogPage(events, 20);
    expect(await compress(events)).toMatchObject({ kind: "compressed", originalBytes: content.length });
    expect(fs.existsSync(events)).toBe(false);
    const encoded = fs.readFileSync(`${events}.zst`);
    const compressedPage = readRunLogPage(events, 20);
    expect(compressedPage.lines).toEqual(expectedPage.lines);
    expect(compressedPage.generation).not.toBe(expectedPage.generation);
    const { restoreRunLog } = await import("../src/storage/run-log-restore.js");
    expect(await restoreRunLog(`${events}.zst`)).toMatchObject({ kind: "dry-run" });
    expect(fs.existsSync(events)).toBe(false);
    expect(await restoreRunLog(`${events}.zst`, { apply: true })).toMatchObject({ kind: "restored" });
    expect(fs.readFileSync(events)).toEqual(content);
    expect(readRunLogPage(events, 20).lines).toEqual(expectedPage.lines);
    expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
    expect(fs.statSync(events).nlink).toBe(1);
    expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "events.jsonl.zst"]);
    expect(await restoreRunLog(`${events}.zst`, { apply: true })).toMatchObject({ kind: "already-restored" });
  });

  it("requires complete checksum bytes after plain retirement through the real raw-log entry", () => {
    const artifact = join(directory, "retired", "events.jsonl");
    fs.mkdirSync(join(directory, "retired"), { mode: 0o700 });
    const encoded = frame(content);
    fs.writeFileSync(`${artifact}.zst`, encoded, { mode: 0o600 });
    expect(readRunLogPage(artifact, 20).lines.map(line => line.raw)).toEqual(content.toString("utf8").trimEnd().split(/\r?\n/));
    // Node accepts these checksum-tail truncations; the shared structural validator MUST reject them.
    for (let missing = 1; missing <= 4; missing++) {
      fs.writeFileSync(`${artifact}.zst`, encoded.subarray(0, -missing), { mode: 0o600 });
      expect(() => readRunLogPage(artifact, 20), `missing ${missing} checksum bytes`).toThrowError(expect.objectContaining({ code: "run-log-codec" }));
    }
  });
});
