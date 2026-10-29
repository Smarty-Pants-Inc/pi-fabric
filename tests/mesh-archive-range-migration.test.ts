import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, expect, it, vi } from "vitest";
import { archiveFileName, MeshArchive } from "../src/mesh/archive.js";
import { retireArchiveRangeSidecars, RETIRED_ARCHIVE_RANGES } from "../src/mesh/archive-range-migration.js";

// Keep: data safety across rename, interruption and live range snapshots.
const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const range = (letter: string) => `.range-${letter.repeat(64)}.json`;
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "range-retirement-")); roots.push(root);
  const day = path.join(root, "2024/01/01"); fs.mkdirSync(day, { recursive: true });
  const values = Array.from({ length: 100 }, (_, i) => ({ sequence: i + 1, id: String(i), topic: "migration" }));
  const data = path.join(day, archiveFileName("migration"));
  fs.writeFileSync(data, values.map(event => JSON.stringify(event) + "\n").join(""));
  fs.writeFileSync(path.join(day, "SEAL.json"), JSON.stringify({ version: 1, files: { [path.basename(data)]: {} } }));
  fs.writeFileSync(path.join(day, ".ranges.json"), "{torn-old-catalog");
  fs.writeFileSync(path.join(day, range("a")), "old-sidecar");
  return { root, day, data, values };
};
const snapshot = (file: string): unknown => {
  const s = fs.lstatSync(file, { bigint: true });
  return [String(s.dev), String(s.ino), String(s.mode), String(s.nlink), String(s.size), String(s.mtimeNs), String(s.ctimeNs),
    s.isDirectory() ? fs.readdirSync(file).sort().map(name => [name, snapshot(path.join(file, name))]) : s.isSymbolicLink() ? fs.readlinkSync(file) : fs.readFileSync(file).toString("hex")];
};
it("moves legacy sidecars out of the seal namespace, preserving bytes, identity and every other file", () => {
  const f = fixture(), unsealed = path.join(f.root, "2024/01/02"); fs.mkdirSync(unsealed);
  fs.writeFileSync(path.join(unsealed, ".ranges.json"), "unsealed");
  fs.writeFileSync(path.join(f.day, ".digest-legacy.json"), "digest");
  fs.writeFileSync(path.join(f.day, "ABORTED.json"), "{}");
  const untouched = [f.data, path.join(f.day, "SEAL.json"), path.join(f.day, ".digest-legacy.json"), path.join(f.day, "ABORTED.json"), unsealed];
  const before = untouched.map(snapshot);
  const originals = [".ranges.json", range("a")].map(name => {
    const source = path.join(f.day, name), s = fs.lstatSync(source, { bigint: true });
    return { source, ino: s.ino, dev: s.dev, size: s.size, mtime: s.mtimeNs, bytes: fs.readFileSync(source) };
  });
  const moved = retireArchiveRangeSidecars(f.root);
  expect(moved).toHaveLength(2);
  for (const item of originals) {
    const row = moved.find(row => row.source === item.source)!;
    expect(fs.existsSync(item.source)).toBe(false);
    expect(row.destination.startsWith(path.join(f.root, RETIRED_ARCHIVE_RANGES) + path.sep)).toBe(true);
    const s = fs.lstatSync(row.destination, { bigint: true });
    expect([s.ino, s.dev, s.size, s.mtimeNs]).toEqual([item.ino, item.dev, item.size, item.mtime]);
    expect(fs.readFileSync(row.destination)).toEqual(item.bytes);
  }
  expect(untouched.map(snapshot)).toEqual(before);
  // Same acceptance condition as strict seal backup: only sealed data and allowed metadata remain.
  expect(fs.readdirSync(f.day).sort()).toEqual([".digest-legacy.json", "ABORTED.json", "SEAL.json", path.basename(f.data)].sort());
  const after = snapshot(f.root);
  expect(retireArchiveRangeSidecars(f.root)).toEqual([]);
  expect(snapshot(f.root)).toEqual(after);
});
it("moves symlinks themselves and hard links without touching their targets", () => {
  const f = fixture(), target = path.join(f.root, "sentinel"); fs.writeFileSync(target, "sentinel");
  const targetBefore = snapshot(target), hard = path.join(f.day, range("b")), symbolic = path.join(f.day, range("c"));
  fs.linkSync(path.join(f.day, range("a")), hard); fs.symlinkSync(target, symbolic);
  const fd = fs.openSync(hard, "r"), before = fs.fstatSync(fd, { bigint: true });
  try {
    const moved = retireArchiveRangeSidecars(f.root);
    expect(moved).toHaveLength(4);
    expect(snapshot(target)).toEqual(targetBefore);
    const link = moved.find(row => row.source === symbolic)!;
    expect(fs.lstatSync(link.destination).isSymbolicLink()).toBe(true);
    expect(fs.readlinkSync(link.destination)).toBe(target);
    const renamedHard = moved.find(row => row.source === hard)!;
    const after = fs.lstatSync(renamedHard.destination, { bigint: true });
    expect([after.dev, after.ino, after.nlink]).toEqual([before.dev, before.ino, before.nlink]);
    expect(fs.readFileSync(fd, "utf8")).toBe("old-sidecar"); // Existing readers keep a valid handle.
  } finally { fs.closeSync(fd); }
});
it("is safe during an async range snapshot and range reads still write nothing", async () => {
  const f = fixture(), archive = new MeshArchive(f.root, f.root);
  const pending = archive.readAfterAsync(0, 100, () => true, 100);
  expect(retireArchiveRangeSidecars(f.root)).toHaveLength(2);
  expect(await pending).toEqual(f.values);
  const before = snapshot(f.root), rename = vi.spyOn(fs, "renameSync"), write = vi.spyOn(fs, "writeFileSync"), mkdir = vi.spyOn(fs, "mkdirSync");
  for (let pass = 0; pass < 10; pass++) {
    expect(await new MeshArchive(f.root, f.root).readAfterAsync(0, 100, () => true, 100)).toEqual(f.values);
    expect(archive.readAfter(0, 100, () => true, 100)).toEqual(f.values);
  }
  expect(rename).not.toHaveBeenCalled(); expect(write).not.toHaveBeenCalled(); expect(mkdir).not.toHaveBeenCalled();
  expect(snapshot(f.root)).toEqual(before);
});
it("rejects a source identity change before rename without moving it", () => {
  const f = fixture(), source = path.join(f.day, range("a")), original = fs.lstatSync; let calls = 0;
  vi.spyOn(fs, "lstatSync").mockImplementation(((...args: Parameters<typeof fs.lstatSync>) => {
    if (String(args[0]) === source && ++calls === 2) fs.writeFileSync(source, "replacement");
    return original(...args);
  }) as typeof fs.lstatSync);
  const rename = vi.spyOn(fs, "renameSync");
  expect(() => retireArchiveRangeSidecars(f.root)).toThrow(/changed before/);
  expect(rename).not.toHaveBeenCalled();
  expect(fs.readFileSync(source, "utf8")).toBe("replacement");
});
it("verifies after rename and preserves a raced destination for investigation", () => {
  const f = fixture(), original = fs.renameSync; let destination = "";
  vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
    original(source, target); destination = String(target); fs.writeFileSync(target, "raced");
  });
  expect(() => retireArchiveRangeSidecars(f.root)).toThrow(/verification failed/);
  expect(fs.readFileSync(destination, "utf8")).toBe("raced");
  expect(fs.existsSync(path.join(f.day, ".ranges.json"))).toBe(true); // Subsequent input was not touched.
});
it("resumes an interrupted run without replacing already-retired files", () => {
  const f = fixture(), original = fs.renameSync; let calls = 0, first = "";
  const rename = vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
    if (++calls === 2) throw new Error("injected interruption");
    original(source, target); first = String(target);
  });
  expect(() => retireArchiveRangeSidecars(f.root)).toThrow("injected interruption");
  const before = snapshot(first); rename.mockRestore();
  expect(retireArchiveRangeSidecars(f.root)).toHaveLength(1);
  expect(snapshot(first)).toEqual(before);
  expect(retireArchiveRangeSidecars(f.root)).toEqual([]);
});
it("does not follow a retired-directory symlink, symlinked day or sidecar lookalike", () => {
  const f = fixture(), outside = fs.mkdtempSync(path.join(os.tmpdir(), "not-an-archive-")); roots.push(outside);
  fs.symlinkSync(outside, path.join(f.root, RETIRED_ARCHIVE_RANGES));
  expect(() => retireArchiveRangeSidecars(f.root)).toThrow(/real directory/);
  expect(fs.readdirSync(outside)).toEqual([]);
  fs.unlinkSync(path.join(f.root, RETIRED_ARCHIVE_RANGES));
  fs.symlinkSync(f.day, path.join(f.root, "2024/01/03"));
  fs.writeFileSync(path.join(f.day, ".range-not-generated.json"), "not ours");
  expect(retireArchiveRangeSidecars(f.root)).toHaveLength(2);
  expect(fs.readFileSync(path.join(f.day, ".range-not-generated.json"), "utf8")).toBe("not ours");
});
