import fs from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { constants, zstdCompressSync, zstdDecompressSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi, type TestContext } from "vitest";
import { readRunLogPage } from "../src/log-tail.js";
import { readRunLogBuffer } from "../src/run-log-source.js";
import {
  compressTerminalRunLog as compress, RUN_LOG_COMPRESSION_TEMP, RunLogCodecError,
} from "../src/storage/run-log-compression.js";
import { restoreRunLog } from "../src/storage/run-log-restore.js";

// Data-safety boundary: real codec, files, publication, retirement and additive restore.
const frame = (bytes: Buffer) => zstdCompressSync(bytes, {
  params: { [constants.ZSTD_c_compressionLevel]: 3, [constants.ZSTD_c_checksumFlag]: 1 },
});
const content = Buffer.from('{"text":"α😀"}\r\n{"done":true}\n');
const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
const getuid = Object.getOwnPropertyDescriptor(process, "getuid");
const requireDurableCompression = (context: TestContext) => {
  if (process.platform === "win32") context.skip("platform: compressor publication/retirement requires durable directory fsync; Windows early skip and imported-frame restore are tested separately");
};
let directory: string;
let events: string;
const temporary = () => fs.readdirSync(directory).filter(name => RUN_LOG_COMPRESSION_TEMP.test(name));
beforeEach(() => {
  directory = fs.realpathSync(fs.mkdtempSync(join(tmpdir(), "runlog-codec-")));
  fs.chmodSync(directory, 0o700);
  events = join(directory, "events.jsonl");
  fs.writeFileSync(events, content, { mode: 0o600 });
});
afterEach(() => {
  Object.defineProperty(process, "platform", platform);
  if (getuid) Object.defineProperty(process, "getuid", getuid);
  else Reflect.deleteProperty(process, "getuid");
  vi.restoreAllMocks();
  fs.rmSync(directory, { recursive: true, force: true });
});
const preserves = async (operation: Promise<unknown>, reason?: string, committed = false) => {
  await expect(operation).rejects.toMatchObject({ code: "run-log-codec", committed, ...(reason ? { reason } : {}) });
  expect(fs.readFileSync(events)).toEqual(content);
};

describe("crash-safe terminal run-log compression", () => {
  it.for([content, Buffer.alloc(0), Buffer.from([0, 255, 254, 13, 10, 128, 0]), Buffer.from("a\r\nb\r\n")])(
    "roundtrips exact bytes through real retirement, reading and additive restore (%j)", async (bytes, context) => {
      requireDurableCompression(context);
      fs.writeFileSync(events, bytes);
      const page = readRunLogPage(events, 20);
      const result = await compress(events);
      expect(result).toMatchObject({ kind: "compressed", originalBytes: bytes.length });
      expect(fs.existsSync(events)).toBe(false);
      const encoded = fs.readFileSync(`${events}.zst`);
      expect(encoded.readUInt32LE(0)).toBe(0xfd2fb528);
      expect(encoded[4]! & 4).toBe(4);
      expect(zstdDecompressSync(encoded)).toEqual(bytes);
      expect(result.compressedBytes).toBe(encoded.length);
      expect(readRunLogBuffer(events)).toEqual(bytes);
      expect(readRunLogPage(events, 20).lines).toEqual(page.lines);
      expect(readRunLogPage(events, 20).generation).not.toBe(page.generation);
      expect(await restoreRunLog(`${events}.zst`)).toMatchObject({ kind: "dry-run" });
      expect(fs.existsSync(events)).toBe(false);
      expect(await restoreRunLog(`${events}.zst`, { apply: true })).toMatchObject({ kind: "restored" });
      expect(fs.readFileSync(events)).toEqual(bytes);
      expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
      expect(fs.statSync(events).nlink).toBe(1);
      expect(fs.statSync(`${events}.zst`).nlink).toBe(1);
      expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "events.jsonl.zst"]);
    },
  );

  it("fsyncs verified staged bytes then publishes atomically and durably before retiring plain", async context => {
    requireDurableCompression(context);
    const originalSync = fs.fsyncSync;
    const originalLink = fs.linkSync;
    const originalUnlink = fs.unlinkSync;
    const order: string[] = [];
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      const stat = fs.fstatSync(fd);
      order.push(stat.isDirectory() ? "directory-sync" : "file-sync");
      originalSync(fd);
    });
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      expect(basename(String(from))).toMatch(RUN_LOG_COMPRESSION_TEMP);
      expect(String(to)).toBe(`${events}.zst`);
      expect(fs.existsSync(to)).toBe(false);
      expect(zstdDecompressSync(fs.readFileSync(from))).toEqual(content);
      expect(order).toContain("file-sync");
      order.push("publish");
      originalLink(from, to);
    });
    vi.spyOn(fs, "unlinkSync").mockImplementation(path => {
      expect(fs.existsSync(events), "plain exists until final retirement").toBe(true);
      expect(zstdDecompressSync(fs.readFileSync(`${events}.zst`))).toEqual(content);
      expect(order.slice(order.indexOf("publish"))).toContain("directory-sync");
      order.push(String(path) === events ? "retire" : "temp-cleanup");
      originalUnlink(path);
    });
    await compress(events);
    expect(order.indexOf("publish")).toBeLessThan(order.indexOf("temp-cleanup"));
    expect(order.indexOf("temp-cleanup")).toBeLessThan(order.indexOf("retire"));
  });

  it("retries matching existing compressed bytes without replacement", async context => {
    requireDurableCompression(context);
    const encoded = frame(content);
    fs.writeFileSync(`${events}.zst`, encoded, { mode: 0o600 });
    const inode = fs.statSync(`${events}.zst`, { bigint: true }).ino;
    const link = vi.spyOn(fs, "linkSync");
    expect(await compress(events)).toMatchObject({ kind: "compressed" });
    expect(link).not.toHaveBeenCalled();
    expect(fs.statSync(`${events}.zst`, { bigint: true }).ino).toBe(inode);
    expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
    expect(await compress(events)).toMatchObject({ kind: "already-compressed", originalBytes: content.length });
  });

  it.each(["native", "win32"] as const)("keeps POSIX absent/oversize outcomes while win32 skips before discovery (%s)", async selectedPlatform => {
    if (selectedPlatform === "win32") Object.defineProperty(process, "platform", { value: "win32" });
    const windows = process.platform === "win32";
    fs.unlinkSync(events);
    expect(await compress(events)).toEqual({ kind: "skipped", reason: windows ? "platform" : "source-missing" });
    fs.writeFileSync(events, "", { mode: 0o600 });
    fs.truncateSync(events, 128 * 1024 * 1024 + 1);
    expect(await compress(events)).toEqual(windows ? { kind: "skipped", reason: "platform" }
      : { kind: "skipped", reason: "plain-too-large", originalBytes: 128 * 1024 * 1024 + 1 });
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(temporary()).toEqual([]);
  });

  it("keeps POSIX O_RDWR source access and propagates denied writes without publication", async context => {
    requireDurableCompression(context);
    const originalOpen = fs.openSync;
    const denied = Object.assign(new Error("source write access denied"), { code: "EACCES" });
    const sourceFlags: number[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      if (String(path) === events && (Number(flags) & (fs.constants.O_WRONLY | fs.constants.O_RDWR)) !== 0) {
        sourceFlags.push(Number(flags) & (fs.constants.O_WRONLY | fs.constants.O_RDWR));
        throw denied;
      }
      return originalOpen(path, flags, mode);
    }) as typeof fs.openSync);
    const sync = vi.spyOn(fs, "fsyncSync");
    const link = vi.spyOn(fs, "linkSync");
    const unlink = vi.spyOn(fs, "unlinkSync");
    const operation = compress(events);
    await preserves(operation, "custody");
    await expect(operation).rejects.toMatchObject({ cause: denied });
    expect(sourceFlags).toEqual([fs.constants.O_RDWR]);
    for (const activity of [sync, link, unlink]) expect(activity).not.toHaveBeenCalled();
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(temporary()).toEqual([]);
  });

  it.for(["mismatch", "checksum", "trailing", "truncated", "second-frame"])("preserves divergent/corrupt existing final: %s", async (scenario, context) => {
    requireDurableCompression(context);
    const encoded = frame(content);
    const invalid = scenario === "mismatch" ? frame(Buffer.from("other"))
      : scenario === "trailing" ? Buffer.concat([encoded, Buffer.from("x")])
      : scenario === "truncated" ? encoded.subarray(0, -1)
      : scenario === "second-frame" ? Buffer.concat([encoded, encoded]) : Buffer.from(encoded);
    if (scenario === "checksum") invalid[invalid.length - 1] = invalid[invalid.length - 1]! ^ 1;
    fs.writeFileSync(`${events}.zst`, invalid, { mode: 0o600 });
    await preserves(compress(events), scenario === "mismatch" ? "content-mismatch" : "invalid-frame");
    expect(fs.readFileSync(`${events}.zst`)).toEqual(invalid);
  });

  it("keeps exclusive FD custody on ENOSPC through immediate compression and a final collision", async context => {
    requireDurableCompression(context);
    const originalOpen = fs.openSync;
    const originalWrite = fs.write;
    const originalWriteSync = fs.writeSync;
    const originalClose = fs.close;
    const full = Object.assign(new Error("full"), { code: "ENOSPC" });
    const deferred: { fd: number; callback: fs.NoParamCallback | undefined }[] = [];
    const closing: Promise<void>[] = [];
    const failedFds = new Set<number>();
    let failing = true;
    let output: number | undefined;
    let nextOutput: number | undefined;
    const close = (fd: number, callback: fs.NoParamCallback | undefined) => {
      const receipt = new Promise<void>(resolve => originalClose(fd, error => {
        if (callback !== undefined) callback(error);
        resolve();
      }));
      closing.push(receipt);
      return receipt;
    };
    const dispatch = () => Promise.all(deferred.splice(0).map(({ fd, callback }) => close(fd, callback)));
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      const fd = originalOpen(path, flags, mode);
      if (failing && String(path) === events) failedFds.add(fd);
      if (RUN_LOG_COMPRESSION_TEMP.test(basename(String(path)))) {
        if (failing) { output = fd; failedFds.add(fd); }
        else nextOutput = fd;
      }
      return fd;
    }) as typeof fs.openSync);
    // Model a valid queued-close schedule, not elapsed time. On the old FD-backed
    // streams, dispatch the failed pipeline's close only once the next output
    // has reused its descriptor, BEFORE issuing that output's native write.
    const asyncClose = vi.spyOn(fs, "close").mockImplementation((fd, callback) => {
      if (failing && failedFds.has(fd)) deferred.push({ fd, callback });
      else { void close(fd, callback); }
    });
    vi.spyOn(fs, "write").mockImplementation(((...args: unknown[]) => {
      if (failing && args[0] === output) {
        process.nextTick(args.at(-1) as (error: Error) => void, full);
        return;
      }
      if (args[0] === nextOutput && deferred.length) {
        void dispatch().then(() => Reflect.apply(originalWrite, fs, args));
        return;
      }
      return Reflect.apply(originalWrite, fs, args);
    }) as typeof fs.write);
    vi.spyOn(fs, "writeSync").mockImplementation(((...args: unknown[]) => {
      if (failing && args[0] === output) throw full;
      return Reflect.apply(originalWriteSync, fs, args);
    }) as typeof fs.writeSync);
    try {
      await preserves(compress(events), "encode");
      expect(fs.existsSync(`${events}.zst`)).toBe(false);
      expect(temporary()).toHaveLength(1);
      failing = false;
      const nextDirectory = join(directory, "next");
      fs.mkdirSync(nextDirectory, { mode: 0o700 });
      const nextEvents = join(nextDirectory, "events.jsonl");
      fs.writeFileSync(nextEvents, content, { mode: 0o600 });
      const subsequent = await compress(nextEvents).then(result => ({ result }), error => ({ error }));
      await dispatch();
      await Promise.all(closing);
      expect(nextOutput).toBe(output); // The formerly vulnerable descriptor really was reused.
      expect(subsequent).toMatchObject({ result: { kind: "compressed", originalBytes: content.length } });
      expect(fs.existsSync(nextEvents)).toBe(false);
      expect(zstdDecompressSync(fs.readFileSync(`${nextEvents}.zst`))).toEqual(content);
      expect(asyncClose).not.toHaveBeenCalled(); // No deferred FD-close work at either pipeline boundary.

      const foreign = frame(Buffer.from("foreign"));
      const originalLink = fs.linkSync;
      vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
        fs.writeFileSync(to, foreign, { mode: 0o600 });
        originalLink(from, to);
      });
      // Retry the preserved first source: collision must still reach publication,
      // not fail early with encode/EBADF or overwrite either history.
      await preserves(compress(events), "publish-compressed");
      expect(fs.readFileSync(`${events}.zst`)).toEqual(foreign);
      expect(temporary()).toHaveLength(2); // Failed encode plus failed publication evidence.
    } finally {
      failing = false;
      await dispatch();
      await Promise.all(closing);
    }
  });

  it.for(["error", "zero-progress"])("preserves source and failed evidence on an encoding read %s", async (fault, context) => {
    requireDurableCompression(context);
    const originalRead = fs.readSync;
    const failure = Object.assign(new Error("source read failed"), { code: "EIO" });
    let injected = false;
    vi.spyOn(fs, "readSync").mockImplementation(((...args: unknown[]) => {
      if (!injected && temporary().length && fs.fstatSync(args[0] as number).ino === fs.statSync(events).ino) {
        injected = true;
        if (fault === "error") throw failure;
        return 0;
      }
      return Reflect.apply(originalRead, fs, args);
    }) as typeof fs.readSync);
    const operation = compress(events);
    await preserves(operation, fault === "error" ? "encode" : "short-read");
    if (fault === "error") await expect(operation).rejects.toMatchObject({ cause: failure });
    expect(injected).toBe(true);
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(temporary()).toHaveLength(1);
  });

  it("roundtrips bounded positional short reads and full short output writes", async context => {
    requireDurableCompression(context);
    const bytes = Buffer.from(Array.from({ length: 3 * 64 * 1024 + 137 }, (_, index) => (index * 131 + (index >>> 8)) & 255));
    fs.writeFileSync(events, bytes);
    const sourceInode = fs.statSync(events).ino;
    const originalRead = fs.readSync;
    const originalWrite = fs.writeSync;
    let sourceOffset = 0;
    let outputOffset = 0;
    let reads = 0;
    let writes = 0;
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
      if (temporary().length && fs.fstatSync(fd).ino === sourceInode) {
        expect(length).toBeLessThanOrEqual(64 * 1024);
        if (position === 0) sourceOffset = 0; // Verification re-reads the same source from the start.
        expect(position).toBe(sourceOffset);
        const count = originalRead(fd, buffer, offset, Math.min(length, 997), position);
        sourceOffset += count;
        reads++;
        return count;
      }
      return originalRead(fd, buffer, offset, length, position);
    }) as typeof fs.readSync);
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
      expect(position).toBe(outputOffset);
      const count = originalWrite(fd, buffer, offset, Math.min(length, 7), position);
      outputOffset += count;
      writes++;
      return count;
    }) as typeof fs.writeSync);
    expect(await compress(events)).toMatchObject({ kind: "compressed", originalBytes: bytes.length, compressedBytes: outputOffset });
    expect(reads).toBeGreaterThan(2);
    expect(writes).toBeGreaterThan(1);
    expect(fs.existsSync(events)).toBe(false);
    expect(zstdDecompressSync(fs.readFileSync(`${events}.zst`))).toEqual(bytes);
    expect(temporary()).toEqual([]);
  });

  it("rejects a zero-progress output write after partial output without retiring source", async context => {
    requireDurableCompression(context);
    const originalWrite = fs.writeSync;
    let calls = 0;
    vi.spyOn(fs, "writeSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
      if (calls++ === 0) return originalWrite(fd, buffer, offset, Math.min(length, 3), position);
      return 0;
    }) as typeof fs.writeSync);
    await preserves(compress(events), "short-write");
    expect(calls).toBe(2);
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(temporary()).toHaveLength(1);
    expect(fs.statSync(join(directory, temporary()[0]!)).size).toBe(3);
  });

  it("refuses a final collision at publication without replacing either history", async context => {
    requireDurableCompression(context);
    const originalLink = fs.linkSync;
    const foreign = frame(Buffer.from("foreign"));
    vi.spyOn(fs, "linkSync").mockImplementation((from, to) => {
      fs.writeFileSync(to, foreign, { mode: 0o600 });
      originalLink(from, to);
    });
    await preserves(compress(events), "publish-compressed");
    expect(fs.readFileSync(`${events}.zst`)).toEqual(foreign);
    expect(temporary()).toHaveLength(1);
  });

  it("refuses a changed source after encoding and preserves the staged frame", async context => {
    requireDurableCompression(context);
    const originalSync = fs.fsyncSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      originalSync(fd);
      if (temporary().length && fs.fstatSync(fd).ino === fs.statSync(join(directory, temporary()[0]!)).ino) {
        fs.appendFileSync(events, "late writer\n");
      }
    });
    await expect(compress(events)).rejects.toMatchObject({ code: "run-log-codec", committed: false });
    expect(fs.readFileSync(events).subarray(0, content.length)).toEqual(content);
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(temporary()).toHaveLength(1);
  });

  it("verifies decoded SHA-256 before publication, not merely a valid frame", async context => {
    requireDurableCompression(context);
    const originalRead = fs.readSync;
    const other = Buffer.alloc(content.length, "x");
    let injected = false;
    vi.spyOn(fs, "readSync").mockImplementation(((fd: number, buffer: Buffer, offset: number, length: number, position: number) => {
      if (!injected && temporary().length && fs.fstatSync(fd).ino === fs.statSync(events).ino) {
        injected = true;
        return other.copy(buffer, offset, position, position + length);
      }
      return originalRead(fd, buffer, offset, length, position);
    }) as typeof fs.readSync);
    await preserves(compress(events), "content-mismatch");
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
    expect(zstdDecompressSync(fs.readFileSync(join(directory, temporary()[0]!)))).toEqual(other);
  });

  it.for(["publish-sync", "cleanup", "retire", "post-retire-sync"])("preserves recoverable bytes after publication failure: %s", async (fault, context) => {
    requireDurableCompression(context);
    const originalSync = fs.fsyncSync;
    const originalUnlink = fs.unlinkSync;
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isDirectory() && fs.existsSync(`${events}.zst`) &&
        (fault === "publish-sync" || (fault === "post-retire-sync" && !fs.existsSync(events)))) throw new Error("sync failed");
      originalSync(fd);
    });
    vi.spyOn(fs, "unlinkSync").mockImplementation(path => {
      if ((fault === "cleanup" && RUN_LOG_COMPRESSION_TEMP.test(basename(String(path)))) ||
        (fault === "retire" && String(path) === events)) throw new Error("unlink failed");
      originalUnlink(path);
    });
    await expect(compress(events)).rejects.toMatchObject({ code: "run-log-codec", committed: true });
    expect(zstdDecompressSync(fs.readFileSync(`${events}.zst`))).toEqual(content);
    if (fault === "post-retire-sync") expect(fs.existsSync(events)).toBe(false);
    else expect(fs.readFileSync(events)).toEqual(content);
  });

  it.for(["source-link", "target-link", "symlink", "foreign-owner", "fifo", "public-directory"])("rejects unsafe custody: %s", async (scenario, context) => {
    if (scenario !== "public-directory") requireDurableCompression(context); // Windows skips before any filesystem custody.
    // The public-directory case forces POSIX below and rejects before opening a directory.
    if (scenario === "foreign-owner" && typeof process.getuid !== "function") context.skip("POSIX uid ownership check is unavailable on this platform; source type/link custody still runs");
    if (scenario === "source-link") fs.linkSync(events, join(directory, "alias"));
    if (scenario === "target-link") {
      fs.writeFileSync(`${events}.zst`, frame(content), { mode: 0o600 });
      fs.linkSync(`${events}.zst`, join(directory, "alias"));
    }
    if (scenario === "public-directory") Object.defineProperty(process, "platform", { value: "linux" });
    const original = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(original, fs, [path, options]);
      if (String(path) === directory && scenario === "public-directory") return Object.assign(Object.create(stat), { mode: stat.mode | 0o077n });
      if (String(path) !== events) return stat;
      if (scenario === "symlink") return Object.assign(Object.create(stat), { isSymbolicLink: () => true });
      if (scenario === "fifo") return Object.assign(Object.create(stat), { isFile: () => false });
      if (scenario === "foreign-owner") return Object.assign(Object.create(stat), { uid: stat.uid + 1n });
      return stat;
    }) as typeof fs.lstatSync);
    await preserves(compress(events));
  });

  it("rejects encoded oversize before allocating it", async context => {
    requireDurableCompression(context);
    fs.writeFileSync(`${events}.zst`, frame(content), { mode: 0o600 });
    fs.truncateSync(`${events}.zst`, 128 * 1024 * 1024 + 1);
    const allocate = vi.spyOn(Buffer, "allocUnsafe");
    await preserves(compress(events), "encoded-too-large");
    expect(allocate.mock.calls.every(([size]) => size <= 128 * 1024 * 1024)).toBe(true);
  });

  it("warns once per module lifetime for disabled win32 compression across repeated calls and distinct logs", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    // Load a fresh real codec instance so this assertion does not depend on test order.
    // Its flag is NOT reset between the multiple calls under test.
    vi.resetModules();
    try {
      const { compressTerminalRunLog } = await import("../src/storage/run-log-compression.js");
      const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
      const nextDirectory = join(directory, "next");
      fs.mkdirSync(nextDirectory, { mode: 0o700 });
      const nextEvents = join(nextDirectory, "events.jsonl");
      fs.writeFileSync(nextEvents, content, { mode: 0o600 });
      for (const file of [events, nextEvents, events]) {
        expect(await compressTerminalRunLog(file)).toEqual({ kind: "skipped", reason: "platform" });
        expect(fs.readFileSync(file)).toEqual(content);
        expect(fs.existsSync(`${file}.zst`)).toBe(false);
      }
      expect(warn).toHaveBeenCalledExactlyOnceWith("[pi-fabric] Run log compression is disabled on this platform (win32): no durable directory sync; plaintext retained.");
      expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "next"]);
      expect(fs.readdirSync(nextDirectory)).toEqual(["events.jsonl"]);
    } finally {
      vi.resetModules();
    }
  });

  it.each(["absent", "matching", "mismatched", "corrupt", "read-only", "write-denied", "read-denied"] as const)("forced win32 retains exact plaintext with no filesystem activity or FD debt (%s)", async scenario => {
    const encoded = scenario === "matching" ? frame(content)
      : scenario === "mismatched" ? frame(Buffer.from("other history")) : Buffer.from("corrupt");
    const hasTarget = scenario === "matching" || scenario === "mismatched" || scenario === "corrupt";
    if (hasTarget) fs.writeFileSync(`${events}.zst`, encoded, { mode: 0o600 });
    if (scenario === "read-only") fs.chmodSync(events, 0o400);
    if (scenario === "read-denied") fs.chmodSync(events, 0o000);
    if (scenario !== "read-denied") expect(fs.readFileSync(events)).toEqual(content);
    const names = fs.readdirSync(directory).sort();
    const sourceStat = fs.statSync(events, { bigint: true });
    const targetStat = hasTarget ? fs.statSync(`${events}.zst`, { bigint: true }) : undefined;
    Object.defineProperty(process, "platform", { value: "win32" });
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const denied = Object.assign(new Error("source access denied"), { code: "EACCES" });
    // Deny every attempted open, including O_RDONLY: a platform skip must not
    // depend on source permissions or acquire any FD that would need closing.
    const operations = [
      vi.spyOn(fs, "openSync").mockImplementation(() => { throw denied; }),
      vi.spyOn(fs, "open"), vi.spyOn(fs, "closeSync"), vi.spyOn(fs, "close"),
      vi.spyOn(fs, "lstatSync"), vi.spyOn(fs, "statSync"), vi.spyOn(fs, "fstatSync"),
      vi.spyOn(fs, "realpathSync"), vi.spyOn(fs, "readdirSync"),
      vi.spyOn(fs, "readSync"), vi.spyOn(fs, "readFileSync"), vi.spyOn(fs, "fsyncSync"),
      vi.spyOn(fs, "writeSync"), vi.spyOn(fs, "write"), vi.spyOn(fs, "writeFileSync"),
      vi.spyOn(fs, "linkSync"), vi.spyOn(fs, "unlinkSync"), vi.spyOn(fs, "renameSync"),
    ];
    try {
      expect(await compress(events)).toEqual({ kind: "skipped", reason: "platform" });
      for (const operation of operations) expect(operation).not.toHaveBeenCalled();
      vi.restoreAllMocks(); // Fixture inspection is outside the compressor's zero-I/O window.
      expect(fs.readdirSync(directory).sort()).toEqual(names);
      expect(temporary()).toEqual([]);
      expect(fs.statSync(events, { bigint: true })).toEqual(sourceStat);
      if (targetStat) expect(fs.statSync(`${events}.zst`, { bigint: true })).toEqual(targetStat);
    } finally {
      vi.restoreAllMocks();
      if (scenario === "read-only" || scenario === "read-denied") fs.chmodSync(events, 0o600);
    }
    expect(fs.readFileSync(events)).toEqual(content);
    if (hasTarget) expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
    else expect(fs.existsSync(`${events}.zst`)).toBe(false);
  });

  describe.each(["native", "win32"] as const)("compressed-only compressor contract (%s)", selectedPlatform => {
    it.each(["valid", "checksum", "trailing", "truncated", "second-frame", "no-checksum", "encoded-too-large"] as const)("validates on POSIX but skips discovery on win32 (%s)", async scenario => {
      if (selectedPlatform === "win32") Object.defineProperty(process, "platform", { value: "win32" });
      const encoded = frame(content);
      const bytes = scenario === "trailing" ? Buffer.concat([encoded, Buffer.from("x")])
        : scenario === "truncated" ? encoded.subarray(0, -1)
        : scenario === "second-frame" ? Buffer.concat([encoded, encoded])
        : scenario === "no-checksum" ? zstdCompressSync(content) : Buffer.from(encoded);
      if (scenario === "checksum") bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 1;
      fs.writeFileSync(`${events}.zst`, bytes, { mode: 0o600 });
      fs.unlinkSync(events);
      if (scenario === "encoded-too-large") fs.truncateSync(`${events}.zst`, 128 * 1024 * 1024 + 1);
      const stat = fs.statSync(`${events}.zst`, { bigint: true });
      const allocate = vi.spyOn(Buffer, "allocUnsafe");
      const link = vi.spyOn(fs, "linkSync");
      const unlink = vi.spyOn(fs, "unlinkSync");
      if (process.platform === "win32") {
        const open = vi.spyOn(fs, "openSync");
        const discover = vi.spyOn(fs, "lstatSync");
        expect(await compress(events)).toEqual({ kind: "skipped", reason: "platform" });
        expect(open).not.toHaveBeenCalled();
        expect(discover).not.toHaveBeenCalled();
        expect(allocate).not.toHaveBeenCalled();
      } else if (scenario === "valid") {
        expect(await compress(events)).toEqual({ kind: "already-compressed", originalBytes: content.length, compressedBytes: bytes.length });
      } else {
        await expect(compress(events)).rejects.toMatchObject({ code: "run-log-codec", committed: false,
          reason: scenario === "encoded-too-large" ? "encoded-too-large" : "invalid-frame" });
      }
      expect(allocate.mock.calls.every(([size]) => size <= 128 * 1024 * 1024)).toBe(true);
      expect(link).not.toHaveBeenCalled();
      expect(unlink).not.toHaveBeenCalled();
      if (scenario === "valid") expect(readRunLogBuffer(events)).toEqual(content);
      expect(fs.existsSync(events)).toBe(false);
      expect(fs.readdirSync(directory)).toEqual(["events.jsonl.zst"]);
      const after = fs.statSync(`${events}.zst`, { bigint: true });
      expect([after.dev, after.ino, after.size, after.mtimeNs, after.ctimeNs]).toEqual([stat.dev, stat.ino, stat.size, stat.mtimeNs, stat.ctimeNs]);
      if (scenario !== "encoded-too-large") expect(fs.readFileSync(`${events}.zst`)).toEqual(bytes);
    });
  });

  it("forced win32 additively restores a real imported frame without authorizing subsequent compression", async () => {
    Object.defineProperty(process, "platform", { value: "win32" });
    const encoded = frame(content);
    fs.writeFileSync(`${events}.zst`, encoded, { mode: 0o600 });
    fs.unlinkSync(events); // Private fixture represents a compressed-only log produced elsewhere.
    expect(readRunLogBuffer(events)).toEqual(content);
    expect(await restoreRunLog(`${events}.zst`)).toMatchObject({ kind: "dry-run", originalBytes: content.length });
    expect(fs.existsSync(events)).toBe(false);
    expect(await restoreRunLog(`${events}.zst`, { apply: true })).toMatchObject({ kind: "restored", originalBytes: content.length });
    expect(await compress(events)).toEqual({ kind: "skipped", reason: "platform" });
    expect(fs.readFileSync(events)).toEqual(content);
    expect(fs.readFileSync(`${events}.zst`)).toEqual(encoded);
    expect(fs.readdirSync(directory).sort()).toEqual(["events.jsonl", "events.jsonl.zst"]);
  });

  it("refuses a private directory identity change before publication", async context => {
    requireDurableCompression(context);
    const originalStat = fs.lstatSync;
    const originalClose = fs.closeSync;
    let closed = false;
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { closed = true; originalClose(fd); });
    vi.spyOn(fs, "lstatSync").mockImplementation(((path: fs.PathLike, options?: unknown) => {
      const stat = Reflect.apply(originalStat, fs, [path, options]);
      return String(path) === directory && closed ? Object.assign(Object.create(stat), { ino: stat.ino + 1n }) : stat;
    }) as typeof fs.lstatSync);
    await preserves(compress(events), "directory-changed");
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
  });

  it("closes an exclusively-created temporary when its initial fstat fails", async context => {
    requireDurableCompression(context);
    const originalOpen = fs.openSync;
    const originalStat = fs.fstatSync;
    const originalClose = fs.closeSync;
    let created: number | undefined;
    const closed: number[] = [];
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      const fd = originalOpen(path, flags, mode);
      if (RUN_LOG_COMPRESSION_TEMP.test(basename(String(path)))) created = fd;
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, options?: unknown) => {
      if (fd === created) throw new Error("initial temporary stat failed");
      return Reflect.apply(originalStat, fs, [fd, options]);
    }) as typeof fs.fstatSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { closed.push(fd); originalClose(fd); });
    await preserves(compress(events), "exclusive-create");
    expect(created).toBeTypeOf("number");
    expect(closed.filter(fd => fd === created)).toHaveLength(1);
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
  });

  it("does not retry a close that already released its descriptor", async context => {
    requireDurableCompression(context);
    const originalOpen = fs.openSync;
    const originalClose = fs.closeSync;
    let source: number | undefined;
    let attempts = 0;
    vi.spyOn(fs, "openSync").mockImplementation(((path: fs.PathLike, flags: string | number, mode?: number) => {
      const fd = originalOpen(path, flags, mode);
      if (String(path) === events) source = fd;
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => {
      originalClose(fd);
      if (fd === source) { attempts++; throw new Error("close released with error"); }
    });
    await expect(compress(events)).rejects.toBeInstanceOf(RunLogCodecError);
    expect(attempts).toBe(1);
    vi.restoreAllMocks();
    expect(fs.readFileSync(events)).toEqual(content);
    expect(fs.existsSync(`${events}.zst`)).toBe(false);
  });
});
