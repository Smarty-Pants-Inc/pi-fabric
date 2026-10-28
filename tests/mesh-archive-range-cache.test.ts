import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { archiveFileName, MeshArchive } from "../src/mesh/archive.js";
import { archiveRangeCache, ARCHIVE_RANGE_CACHE_BYTES, ARCHIVE_RANGE_CACHE_ENTRIES, type ArchiveRangeIndex } from "../src/mesh/archive-range-cache.js";
import type { MeshEvent } from "../src/mesh/store.js";

// Keep: data safety. Exercise real archive reads and sealed bytes, not metadata text.
const MAX = Number.MAX_SAFE_INTEGER;
const all = () => true;
const event = (sequence: number, day = 0): MeshEvent => ({
  id: `range-${sequence}`, sequence, topic: "test.range", kind: "message",
  from: { id: "session:range", name: "range", kind: "main" },
  createdAt: Date.UTC(2024, 0, 1) + day * 86400000, text: `event ${sequence}`,
});
const setup = () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "range-memory-"));
  const dir = path.join(base, "archive"), root = path.join(base, "mesh");
  fs.mkdirSync(dir); fs.mkdirSync(root);
  return { dir, root, archive: new MeshArchive(dir, root) };
};
const snapshot = (dir: string): unknown[] => fs.readdirSync(dir).sort().flatMap(name => {
  const file = path.join(dir, name), stat = fs.lstatSync(file, { bigint: true });
  return [[name, String(stat.mtimeNs), stat.isDirectory() ? snapshot(file) : fs.readFileSync(file).toString("hex")]];
});
const bounded = () => {
  expect(archiveRangeCache.stats.entries).toBeLessThanOrEqual(ARCHIVE_RANGE_CACHE_ENTRIES);
  expect(archiveRangeCache.stats.bytes).toBeLessThanOrEqual(ARCHIVE_RANGE_CACHE_BYTES);
};
afterEach(() => vi.restoreAllMocks());

it("keeps all archive names, bytes and mtimes identical through 120 sealed range reads", async () => {
  const f = setup();
  const values = Array.from({ length: 120 }, (_, i) => event(i + 1, Math.floor(i / 40)));
  f.archive.catchUp(values.map(value => ({ event: value, line: JSON.stringify(value) })));
  const directory = path.join(f.dir, "2024/01/01");
  expect(fs.existsSync(path.join(directory, "SEAL.json"))).toBe(true);
  const suffix = createHash("sha256").update(archiveFileName("test.range")).digest("hex");
  fs.writeFileSync(path.join(directory, `.range-${suffix}.json`), "{obsolete");
  fs.writeFileSync(path.join(directory, ".ranges.json"), "{obsolete");
  const before = snapshot(f.dir), open = fs.openSync;
  const write = vi.spyOn(fs, "writeFileSync"), rename = vi.spyOn(fs, "renameSync");
  const noSidecars = vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
    expect(args[1]).toBe("r");
    expect(path.basename(String(args[0]))).not.toMatch(/^\.range/);
    return open(...args);
  }) as typeof fs.openSync);
  for (let pass = 0; pass < 120; pass++) {
    const after = pass % 113, through = after + 6;
    const archive = pass % 3 === 0 ? f.archive : new MeshArchive(f.dir, f.root);
    const actual = pass % 2 === 0 ? archive.readAfter(after, through, all, 4) : await archive.readAfterAsync(after, through, all, 4);
    expect(actual).toEqual(values.filter(value => value.sequence > after && value.sequence <= through).slice(0, 4));
    bounded();
  }
  expect(write).not.toHaveBeenCalled(); expect(rename).not.toHaveBeenCalled();
  noSidecars.mockRestore();
  expect(snapshot(f.dir)).toEqual(before);
});

it("invalidates memory on same-size seal rewrite, seal removal and resealing", () => {
  const f = setup(), directory = path.join(f.dir, "2024/01/01");
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, archiveFileName("test.range")), Array.from({ length: 100 }, (_, i) => JSON.stringify(event(i + 1)) + "\n").join(""));
  const seal = path.join(directory, "SEAL.json");
  fs.writeFileSync(seal, "seal-a");
  const read = () => new MeshArchive(f.dir, f.root).readAfter(500, MAX, all, 10);
  expect(read()).toEqual([]);
  const parse = JSON.parse; let parsed = 0;
  vi.spyOn(JSON, "parse").mockImplementation((...args: Parameters<typeof JSON.parse>) => {
    const value = parse(...args); if (value?.sequence) parsed++; return value;
  });
  expect(read()).toEqual([]); expect(parsed).toBe(0);
  const before = fs.statSync(seal);
  fs.writeFileSync(seal, "seal-b"); fs.utimesSync(seal, before.atime, before.mtime);
  expect(read()).toEqual([]); expect(parsed).toBe(100);
  fs.unlinkSync(seal);
  expect(read()).toEqual([]); expect(parsed).toBe(200);
  fs.writeFileSync(seal, "seal-c");
  expect(read()).toEqual([]); expect(parsed).toBe(300);
});

it("bounds the process cache across 384 real days and fresh archive objects", async () => {
  const f = setup(), values = Array.from({ length: 384 }, (_, i) => event(i + 1, i));
  for (const value of values) {
    const date = new Date(value.createdAt).toISOString().slice(0, 10).replaceAll("-", "/");
    const directory = path.join(f.dir, date); fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, archiveFileName(value.topic)), JSON.stringify(value) + "\n");
    fs.writeFileSync(path.join(directory, "SEAL.json"), "{}");
  }
  for (let pass = 0; pass < 4; pass++) {
    const archive = new MeshArchive(f.dir, f.root);
    const after = pass * 73;
    const actual = pass % 2 ? await archive.readAfterAsync(after, MAX, all, 400) : archive.readAfter(after, MAX, all, 400);
    expect(actual).toEqual(values.filter(value => value.sequence > after));
    bounded();
  }
  expect(archiveRangeCache.stats.entries).toBe(128);
});

it("uses LRU eviction and a byte cap, including oversized-day refusal", () => {
  const prefix = "memory-only-lru-";
  const small = new Map<string, ArchiveRangeIndex>([["a.jsonl", { identity: "a", min: 1, max: 1, blocks: [[0, 1, 1, 1]] }]]);
  for (let i = 0; i < 128; i++) archiveRangeCache.set(prefix + i, "seal", small);
  expect(archiveRangeCache.get(prefix + 0, "seal").size).toBe(1);
  archiveRangeCache.set(prefix + 128, "seal", small);
  expect(archiveRangeCache.get(prefix + 1, "seal").size).toBe(0);
  expect(archiveRangeCache.get(prefix + 0, "seal").size).toBe(1);
  const large = new Map<string, ArchiveRangeIndex>();
  for (let i = 0; i < 8; i++) large.set(`${i}.jsonl`, { identity: String(i), min: 1, max: 1, blocks: Array.from({ length: 1024 }, (_, n) => [n, n + 1, 1, 1]) });
  for (let i = 0; i < 100; i++) { archiveRangeCache.set(`byte-day-${i}`, "seal", large); bounded(); }
  expect(archiveRangeCache.stats.entries).toBeLessThan(128);
  expect(archiveRangeCache.get("byte-day-0", "seal").size).toBe(0);
  const oversized = new Map<string, ArchiveRangeIndex>();
  for (let i = 0; i < 257; i++) oversized.set(`${i}.jsonl`, large.get("0.jsonl")!);
  archiveRangeCache.set("oversized", "seal", oversized);
  expect(archiveRangeCache.get("oversized", "seal").size).toBe(0); bounded();
  archiveRangeCache.set("generation", "seal-a", small);
  expect(archiveRangeCache.get("generation", "seal-b").size).toBe(0);
});
