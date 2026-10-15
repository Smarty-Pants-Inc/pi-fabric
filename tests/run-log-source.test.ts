import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { constants, createZstdCompress, zstdCompressSync } from "node:zlib";
import { describe, expect, it, vi } from "vitest";
import { readRunLogPage as readJsonlPage, readJsonlPage as readPlainPage } from "../src/log-tail.js";

const frame = (data: Buffer | string) => zstdCompressSync(data, {
  params: { [constants.ZSTD_c_checksumFlag]: 1 },
});
const streamFrame = (data: Buffer): Promise<Buffer> => new Promise((resolve, reject) => {
  const stream = createZstdCompress({ params: { [constants.ZSTD_c_checksumFlag]: 1 } });
  const chunks: Buffer[] = [];
  stream.on("data", (chunk: Buffer) => chunks.push(chunk));
  stream.once("error", reject);
  stream.once("end", () => resolve(Buffer.concat(chunks)));
  stream.end(data);
});
const workspace = () => fs.mkdtempSync(path.join(os.tmpdir(), "run-source-"));
const logical = () => path.join(workspace(), "events.jsonl");
const helper = () => import("../src/run-log-source.js");
console.info("3252-write-root", JSON.stringify({ cwd: process.cwd(), testTmpdir: os.tmpdir(),
  vitest: fs.realpathSync("node_modules/vitest/vitest.mjs") }));

describe("run log source — physical zstd, logical bytes", () => {
  it("pages exact raw Unicode CRLF bytes and rejects the retired plain generation", () => {
    const file = logical();
    const bytes = Buffer.from('{"v":"α😀"}\r\n{"v":2}\r\n{"partial":');
    fs.writeFileSync(file, bytes);
    const plain = readJsonlPage(file, 2);
    // Fixture-only representation transition: retain the plain bytes under a new name.
    fs.renameSync(file, `${file}.fixture-plain`);
    fs.writeFileSync(`${file}.zst`, frame(bytes));
    const compressed = readJsonlPage(file, 2);
    expect(compressed.lines).toEqual(plain.lines);
    expect(compressed.hasMore).toBe(plain.hasMore);
    expect(compressed.before).toBe(plain.before);
    expect(compressed.generation).not.toBe(plain.generation);
    expect(() => readJsonlPage(file, 1, plain.before, undefined, plain.generation)).toThrow(/cursor-stale/);
  });

  it("keeps generic mesh/session pages plain and preserves all run byte offsets across multiple chunks", () => {
    const file = logical();
    const lines = Array.from({ length: 180 }, (_, index) => JSON.stringify({ index, text: `α😀${"x".repeat(1200)}` }));
    const bytes = Buffer.from(lines.join("\r\n") + "\r\n");
    fs.writeFileSync(`${file}.zst`, frame(bytes));
    expect(readPlainPage(file, 10)).toEqual({ lines: [], hasMore: false });
    const pages = [];
    let before: number | undefined;
    do {
      const page = readJsonlPage(file, 17, before);
      pages.unshift(...page.lines);
      before = page.before;
    } while (before !== undefined);
    expect(pages.map((line) => line.raw)).toEqual(lines);
    let offset = 0;
    for (const line of pages) {
      expect(line.offset).toBe(offset);
      offset += Buffer.byteLength(line.raw) + 2;
    }
  });

  it("rejects a checksum-truncated frame through raw agents.log paging", () => {
    const file = logical();
    const encoded = frame('{"type":"agent_end"}\n');
    fs.writeFileSync(`${file}.zst`, encoded.subarray(0, encoded.length - 1));
    expect(() => readJsonlPage(file, 10)).toThrow(/run-log-codec.*truncated|run-log-codec.*checksum/);
  });

  it("rejects a checksum-truncated frame through the shared restore decoder", async () => {
    const { decodeRunLogFrame } = await helper();
    const encoded = frame('{"type":"agent_end"}\n');
    expect(() => decodeRunLogFrame(encoded.subarray(0, encoded.length - 1), "restore/events.jsonl.zst"))
      .toThrow(/run-log-codec.*truncated|run-log-codec.*checksum/);
  });

  it("opens plain first, stats without decoding, preserves descriptor identity and closes", async () => {
    const { openRunLogSource, statRunLogSource, readRunLogBuffer, readRunLogText } = await helper();
    const file = logical();
    fs.writeFileSync(file, "plain😀\r\n");
    fs.writeFileSync(`${file}.zst`, "incomplete");
    expect(readRunLogText(file)).toBe("plain😀\r\n");
    const opened = openRunLogSource(file);
    const stat = fs.statSync(file, { bigint: true });
    expect(opened.generation).toBe(`${stat.dev}:${stat.ino}`);
    fs.renameSync(file, `${file}.fixture-plain`);
    const buffer = Buffer.alloc(opened.size);
    expect(opened.read(buffer, 0, buffer.length, 0)).toBe(buffer.length);
    expect(buffer.toString()).toBe("plain😀\r\n");
    opened.close();
    expect(() => opened.read(buffer, 0, 1, 0)).toThrow();
    expect(statRunLogSource(file).size).toBe(10n);
    expect(() => readRunLogBuffer(file)).toThrow(/run-log-codec/);
  });

  it("separates missing, denied and unsafe plain sources without compressed fallback", async () => {
    const { openRunLogSource, RunLogReadError } = await helper();
    const file = logical();
    expect(() => openRunLogSource(file)).toThrow(expect.objectContaining({ code: "ENOENT" }));
    fs.writeFileSync(`${file}.zst`, frame("valid"));
    const original = fs.openSync;
    const spy = vi.spyOn(fs, "openSync").mockImplementation(((name: fs.PathLike, ...args: unknown[]) => {
      if (name === file) throw Object.assign(new Error("denied fixture"), { code: "EACCES" });
      return Reflect.apply(original, fs, [name, ...args]);
    }) as typeof fs.openSync);
    expect(() => openRunLogSource(file)).toThrow(RunLogReadError);
    expect(spy.mock.calls.map(([name]) => name)).not.toContain(`${file}.zst`);
    spy.mockRestore();
    fs.mkdirSync(file);
    expect(() => openRunLogSource(file)).toThrow(RunLogReadError);
  });

  it("preserves generic plain I/O failures while run failures identify the selected physical source", async () => {
    const { RunLogReadError, statRunLogSource } = await helper();
    const file = logical();
    fs.writeFileSync(file, '{"ok":true}\n');
    fs.writeFileSync(`${file}.zst`, frame('{"compressed":true}\n'));
    const originalRead = fs.readSync;
    const reads = vi.spyOn(fs, "readSync").mockImplementation((() => {
      throw Object.assign(new Error("fixture read EIO"), { code: "EIO" });
    }) as typeof fs.readSync);
    expect(readPlainPage(file, 10)).toEqual({ lines: [], hasMore: false });
    expect(() => readJsonlPage(file, 10)).toThrow(expect.objectContaining({ code: "run-log-codec", path: file }));
    reads.mockRestore();
    fs.renameSync(file, `${file}.fixture-plain`);
    const originalStat = fs.fstatSync;
    const stats = vi.spyOn(fs, "fstatSync").mockImplementation((() => {
      throw Object.assign(new Error("fixture stat EIO"), { code: "EIO" });
    }) as typeof fs.fstatSync);
    expect(() => readJsonlPage(file, 10)).toThrow(expect.objectContaining({ code: "run-log-codec", path: `${file}.zst` }));
    const descriptor = stats.mock.calls[0]![0];
    stats.mockRestore();
    expect(() => Reflect.apply(originalStat, fs, [descriptor])).toThrow();
    const encodedReads = vi.spyOn(fs, "readSync").mockImplementation((() => {
      throw Object.assign(new Error("fixture encoded EIO"), { code: "EIO" });
    }) as typeof originalRead);
    expect(() => readJsonlPage(file, 10)).toThrow(expect.objectContaining({ code: "run-log-codec", path: `${file}.zst` }));
    expect(() => readJsonlPage(file, 10)).toThrow(RunLogReadError);
    encodedReads.mockRestore();
    const originalClose = fs.closeSync;
    vi.spyOn(fs, "closeSync").mockImplementation((descriptor) => {
      originalClose(descriptor);
      throw Object.assign(new Error("fixture close EIO"), { code: "EIO" });
    });
    expect(() => statRunLogSource(file)).toThrow(expect.objectContaining({ code: "run-log-codec", path: `${file}.zst` }));
  });

  it("refuses symlink and swapped physical candidates when the platform has no O_NOFOLLOW", async () => {
    const file = logical();
    fs.writeFileSync(file, "plain");
    fs.writeFileSync(`${file}.zst`, frame("compressed"));
    // Recorded Windows boundary: ordinary real fixture I/O, no no-follow flag.
    vi.resetModules();
    vi.doMock("node:fs", () => ({ default: new Proxy({}, { get(_target, key) {
      return key === "constants" ? { ...fs.constants, O_NOFOLLOW: undefined } : Reflect.get(fs, key);
    } }) }));
    try {
      const { openRunLogSource } = await helper();
      const opened = openRunLogSource(file);
      fs.renameSync(file, `${file}.fixture-plain`);
      const bytes = Buffer.alloc(opened.size);
      opened.read(bytes, 0, bytes.length, 0);
      expect(bytes.toString()).toBe("plain");
      opened.close();
      fs.renameSync(`${file}.fixture-plain`, file);
      let candidate = file;
      let swapped = false;
      const original = fs.lstatSync;
      vi.spyOn(fs, "lstatSync").mockImplementation(((name: fs.PathLike, ...args: unknown[]) => {
        const stat = Reflect.apply(original, fs, [name, ...args]);
        return name !== candidate ? stat : new Proxy(stat, { get(target, key) {
          if (key === "isSymbolicLink") return () => !swapped;
          if (key === "isFile" && !swapped) return () => false;
          if (key === "ino" && swapped) return target.ino + 1n;
          return Reflect.get(target, key);
        } });
      }) as typeof fs.lstatSync);
      const opens = vi.spyOn(fs, "openSync");
      expect(() => openRunLogSource(file)).toThrow(expect.objectContaining({ code: "run-log-codec", path: file }));
      expect(opens).not.toHaveBeenCalled();
      fs.renameSync(file, `${file}.fixture-plain`);
      candidate = `${file}.zst`;
      expect(() => openRunLogSource(file)).toThrow(expect.objectContaining({ code: "run-log-codec", path: candidate }));
      expect(opens).not.toHaveBeenCalled();
      swapped = true;
      const closes = vi.spyOn(fs, "closeSync");
      expect(() => openRunLogSource(file)).toThrow(expect.objectContaining({ code: "run-log-codec", path: candidate }));
      expect(opens).toHaveBeenCalledTimes(1);
      expect(closes).toHaveBeenCalledTimes(1);
    } finally {
      vi.doUnmock("node:fs");
      vi.resetModules();
    }
  });

  it("rejects corruption, absent checksums, trailing/second/skippable frames and every partial prefix", async () => {
    const { decodeRunLogFrame, validateRunLogZstdFrame, RunLogReadError } = await helper();
    const encoded = frame("the whole original log😀\n".repeat(100));
    expect(decodeRunLogFrame(encoded, "events.jsonl.zst").toString()).toBe("the whole original log😀\n".repeat(100));
    expect(() => validateRunLogZstdFrame(encoded)).not.toThrow();
    for (let length = 0; length < encoded.length; length++) {
      expect(() => decodeRunLogFrame(encoded.subarray(0, length), "events.jsonl.zst"), `prefix ${length}`).toThrow(RunLogReadError);
    }
    const corrupt = Buffer.from(encoded);
    corrupt[corrupt.length - 1] = corrupt[corrupt.length - 1]! ^ 1;
    for (const invalid of [corrupt, zstdCompressSync("unchecked"), Buffer.concat([encoded, Buffer.from([0])]),
      Buffer.concat([encoded, encoded]), Buffer.from("502a4d1800000000", "hex")]) {
      expect(() => decodeRunLogFrame(invalid, "events.jsonl.zst")).toThrow(RunLogReadError);
    }
  });

  it("validates the writer's streaming frame with unknown content size and multiple blocks", async () => {
    const { decodeRunLogFrame } = await helper();
    const bytes = Buffer.from(Array.from({ length: 8000 }, (_, index) => `${index}: α😀${"block".repeat(20)}\n`).join(""));
    const encoded = await streamFrame(bytes);
    expect(encoded[4]! & 0x20).toBe(0);
    expect(decodeRunLogFrame(encoded, "events.jsonl.zst")).toEqual(bytes);
  });

  it("keeps codec loading out of plain reads and physical-only compressed stats", async () => {
    const { readRunLogText, statRunLogSource } = await helper();
    const file = logical();
    fs.writeFileSync(file, "plain");
    const builtin = vi.spyOn(process, "getBuiltinModule");
    expect(readRunLogText(file)).toBe("plain");
    fs.renameSync(file, `${file}.fixture-plain`);
    fs.writeFileSync(`${file}.zst`, frame("compressed"));
    statRunLogSource(file);
    expect(builtin).not.toHaveBeenCalled();
    expect(readRunLogText(file)).toBe("compressed");
    expect(builtin).toHaveBeenCalledWith("node:zlib");
  });

  it("caps encoded allocation before read, and caps decoded bombs", async () => {
    const { readRunLogBuffer, decodeRunLogFrame } = await helper();
    const file = logical();
    fs.writeFileSync(`${file}.zst`, frame("ok"));
    const original = fs.fstatSync;
    vi.spyOn(fs, "fstatSync").mockImplementation(((...args: unknown[]) => {
      const stat = Reflect.apply(original, fs, args);
      return Object.assign(stat, { size: 128n * 1024n * 1024n + 1n });
    }) as typeof fs.fstatSync);
    const read = vi.spyOn(fs, "readSync");
    expect(() => readRunLogBuffer(file)).toThrow(/encoded.*cap/);
    expect(read).not.toHaveBeenCalled();
    fs.writeFileSync(file, "plain size fixture");
    expect(() => readRunLogBuffer(file)).toThrow(/decoded.*cap/);
    expect(read).not.toHaveBeenCalled();
    vi.restoreAllMocks();
    const payload = Buffer.alloc(128 * 1024 * 1024 + 1);
    expect(() => decodeRunLogFrame(frame(payload), file)).toThrow(/decoded.*cap/);
    const unknownSizeBomb = await streamFrame(payload);
    expect(() => decodeRunLogFrame(unknownSizeBomb, file)).toThrow(/run-log-codec/);
  });

  it("bounds the LRU by 32 entries and 16 MiB, invalidating physical metadata changes", async () => {
    const { readRunLogText, readRunLogBuffer } = await helper();
    const directory = workspace();
    const files = Array.from({ length: 34 }, (_, index) => {
      const run = path.join(directory, `${index}`);
      fs.mkdirSync(run);
      const file = path.join(run, "events.jsonl");
      fs.writeFileSync(`${file}.zst`, frame(""));
      return file;
    });
    readRunLogText(files[0]!);
    const read = vi.spyOn(fs, "readSync");
    readRunLogText(files[0]!);
    expect(read).not.toHaveBeenCalled();
    for (const file of files.slice(1)) readRunLogText(file);
    read.mockClear();
    readRunLogText(files[0]!);
    expect(read).toHaveBeenCalled();
    fs.writeFileSync(`${files[0]}.zst`, frame("changed"));
    expect(readRunLogText(files[0]!)).toBe("changed");
    const firstFile = `${files[0]}.zst`;
    fs.writeFileSync(firstFile, frame("A"));
    fs.utimesSync(firstFile, 1_700_000_000, 1_700_000_000);
    expect(readRunLogText(files[0]!)).toBe("A");
    const priorSize = fs.statSync(firstFile).size;
    fs.writeFileSync(firstFile, frame("B"));
    fs.utimesSync(firstFile, 1_700_000_001, 1_700_000_001);
    expect(fs.statSync(firstFile).size).toBe(priorSize);
    expect(readRunLogText(files[0]!)).toBe("B");
    readRunLogBuffer(files[0]!).fill(0);
    expect(readRunLogText(files[0]!)).toBe("B");
    const big = path.join(directory, "big", "events.jsonl");
    fs.mkdirSync(path.dirname(big));
    fs.writeFileSync(`${big}.zst`, frame(Buffer.alloc(16 * 1024 * 1024 + 1, 97)));
    readRunLogText(big);
    read.mockClear();
    readRunLogText(big);
    expect(read).toHaveBeenCalled();
    // Two individually cacheable 9 MiB sources cannot coexist in the byte budget.
    for (const index of [0, 1]) fs.writeFileSync(`${files[index]}.zst`, frame(Buffer.alloc(9 * 1024 * 1024, 97 + index)));
    readRunLogText(files[0]!);
    readRunLogText(files[1]!);
    read.mockClear();
    readRunLogText(files[0]!);
    expect(read).toHaveBeenCalled();
  });
});
