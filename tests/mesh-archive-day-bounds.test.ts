import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { archiveFileName, MeshArchive } from "../src/mesh/archive.js";
import { archiveDayBounds, ARCHIVE_DAY_BOUNDS_BYTES, ARCHIVE_DAY_BOUNDS_ENTRIES } from "../src/mesh/archive-day-bounds.js";
import { archiveRangeCache } from "../src/mesh/archive-range-cache.js";

// Keep: the real cache-thrashing regression, and data safety at the stat boundary.
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "day-bounds-")); roots.push(root);
  const files: string[] = [];
  for (let day = 0; day < 384; day++) {
    const directory = path.join(root, new Date(Date.UTC(2024, 0, day + 1)).toISOString().slice(0, 10).replaceAll("-", "/"));
    fs.mkdirSync(directory, { recursive: true });
    const file = path.join(directory, archiveFileName("bounds")); files.push(file);
    fs.writeFileSync(file, JSON.stringify({ sequence: day + 1, id: String(day), topic: "bounds" }) + "\n");
    fs.writeFileSync(path.join(directory, "SEAL.json"), "{}");
  }
  return { root, files, archive: new MeshArchive(root, root) };
};
it("retains cheap bounds beyond the fine-cache day cap and skips without any data opens", async () => {
  const f = fixture(), all = () => true;
  expect(f.archive.readAfter(1000, 2000, all, 1)).toEqual([]);
  expect(archiveRangeCache.stats.entries).toBe(128);
  const open = fs.openSync, dataOpens: string[] = [];
  vi.spyOn(fs, "openSync").mockImplementation(((...args: Parameters<typeof fs.openSync>) => {
    if (String(args[0]).endsWith(".jsonl")) dataOpens.push(String(args[0]));
    return open(...args);
  }) as typeof fs.openSync);
  for (let pass = 0; pass < 3; pass++) {
    expect(await new MeshArchive(f.root, f.root).readAfterAsync(1000, 2000, all, 1)).toEqual([]);
    expect(f.archive.readAfter(-1, 0, all, 1, "bounds")).toEqual([]);
  }
  expect(dataOpens).toEqual([]);
  expect(archiveDayBounds.stats.entries).toBeGreaterThanOrEqual(384);
  expect(archiveDayBounds.stats.entries).toBeLessThanOrEqual(ARCHIVE_DAY_BOUNDS_ENTRIES);
  expect(archiveDayBounds.stats.bytes).toBeLessThanOrEqual(ARCHIVE_DAY_BOUNDS_BYTES);
});
it.each(["sync", "async"] as const)("revalidates skipped-day segments, including in-place rewrite after the first stat (%s)", async mode => {
  const f = fixture(), file = f.files[0]!, all = () => true;
  f.archive.readAfter(1000, 2000, all, 1);
  const original = fs.statSync; let changed = false;
  vi.spyOn(fs, "statSync").mockImplementation(((...args: Parameters<typeof fs.statSync>) => {
    const value = original(...args);
    if (!changed && String(args[0]) === file) {
      changed = true; fs.writeFileSync(file, '{"sequence":1500,"id":"new","topic":"bounds"}\n');
    }
    return value;
  }) as typeof fs.statSync);
  const result = mode === "sync" ? f.archive.readAfter(1000, 2000, all, 1) : await f.archive.readAfterAsync(1000, 2000, all, 1);
  expect(result.map(event => event.sequence)).toEqual([1500]);
});
it("bounds the independent summary tier and invalidates generations", () => {
  for (let day = 0; day < 6000; day++) archiveDayBounds.set("summary-" + day, { generation: "a", fingerprint: "f".repeat(64), min: 1, max: 2 });
  expect(archiveDayBounds.stats.entries).toBeLessThanOrEqual(ARCHIVE_DAY_BOUNDS_ENTRIES);
  expect(archiveDayBounds.stats.bytes).toBeLessThanOrEqual(ARCHIVE_DAY_BOUNDS_BYTES);
  expect(archiveDayBounds.get("summary-0", "a")).toBeUndefined();
  expect(archiveDayBounds.get("summary-5999", "b")).toBeUndefined();
});
