import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startRemovalTally, stopRemovalTally, verifiedRemove } from "../src/storage/verified-remove.js";

// Old audit findings 1 (writable original-parent namespace) and 3 (stale graveyard path). No product delete runs here:
// fs.unlinkSync and fs.rmdirSync are recorders that never execute, and fs.renameSync is recorded and suppressed when
// its destination exists (an overwriting rename); a rename to a free name (the helper's own move, a rollback) runs.
// Fixtures are this file's own mkdtemp roots, removed only after every stub is restored.
const DAY = 24 * 60 * 60 * 1_000;
const OLD = (Date.now() - 30 * DAY) / 1000;
const GRAVE = `.retention-graveyard-${process.pid}`;
const realRename = fs.renameSync;
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  stopRemovalTally();
  for (const root of roots.splice(0)) {
    for (const dir of [root, path.join(root, "parent")]) try { fs.chmodSync(dir, 0o700); } catch { /* gone */ }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};
/** parent/{target/{a.txt, sub/inner.txt}, file.log}, aged 30 days; parent mode as given. */
const fixture = (mode = 0o755) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-vrc-"));
  roots.push(root);
  const parent = path.join(root, "parent");
  const target = path.join(parent, "target");
  fs.mkdirSync(path.join(target, "sub"), { recursive: true, mode: 0o755 });
  fs.writeFileSync(path.join(target, "a.txt"), "aaaa");
  fs.writeFileSync(path.join(target, "sub", "inner.txt"), "inner");
  const file = path.join(parent, "file.log");
  fs.writeFileSync(file, "log-bytes");
  for (const entry of [path.join(target, "a.txt"), path.join(target, "sub", "inner.txt"), path.join(target, "sub"), target, file]) fs.utimesSync(entry, OLD, OLD);
  fs.chmodSync(target, 0o755); fs.chmodSync(path.join(target, "sub"), 0o755);
  fs.chmodSync(parent, mode);
  return { root, parent, target, file };
};
type Call = { op: string; file: string; to?: string; ino: number | undefined; suppressed?: boolean };
const record = () => {
  const calls: Call[] = [];
  const capture = (op: string) => ((file: fs.PathLike) => {
    calls.push({ op, file: String(file), ino: fs.lstatSync(file, { throwIfNoEntry: false })?.ino });
  }) as never;
  vi.spyOn(fs, "unlinkSync").mockImplementation(capture("unlink"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(capture("rmdir"));
  vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
    const taken = fs.lstatSync(to, { throwIfNoEntry: false }) !== undefined;
    calls.push({ op: "rename", file: String(from), to: String(to), ino: fs.lstatSync(from, { throwIfNoEntry: false })?.ino, suppressed: taken });
    if (!taken) realRename(from, to);
  }) as never);
  return calls;
};
const destructive = (calls: Call[]) => calls.filter(call => call.op !== "rename" || call.suppressed);
const base = (file: string) => path.basename(file).replace(/\.[0-9a-f-]{36}$/, "");

describe.skipIf(process.platform !== "linux")("finding 1: the original parent must be closed to other writers", () => {
  for (const mode of [0o777, 0o775]) {
    it(`a user-owned ${mode.toString(8)} parent refuses every mode completely: no move, no delete, bytes unchanged`, () => {
      const { parent, target, file } = fixture(mode);
      const before = shape(parent);
      const ids = [fs.lstatSync(target).ino, fs.lstatSync(file).ino];
      const calls = record();
      const tally = startRemovalTally();
      let proceeded = 0;
      const judgedTree = fs.lstatSync(target), judgedFile = fs.lstatSync(file);
      const results = [
        verifiedRemove(target, judgedTree, { deleter: "custody" }),
        verifiedRemove(target, judgedTree, { deleter: "custody", dryRun: true }),
        verifiedRemove(target, judgedTree, { deleter: "custody", proceed: () => (proceeded++, true) }),
        verifiedRemove(target, judgedTree, { deleter: "custody", proceed: () => (proceeded++, false) }),
        verifiedRemove(file, judgedFile, { deleter: "custody", proceed: () => (proceeded++, true) }),
        verifiedRemove(file, judgedFile, { deleter: "custody", proceed: () => (proceeded++, false) }),
      ];
      for (const result of results) expect(result).toEqual({ removed: false, reason: "parent directory is writable by group or others" });
      expect(proceeded).toBe(0);
      expect(calls).toEqual([]);
      expect(shape(parent)).toEqual(before);
      expect([fs.lstatSync(target).ino, fs.lstatSync(file).ino]).toEqual(ids);
      expect(fs.readdirSync(parent).some(name => name.startsWith(".retention-graveyard-"))).toBe(false);
      expect(fs.lstatSync(parent).mode & 0o777).toBe(mode); // never healed
      expect(tally.custody).toMatchObject({ removed: 0, bytes: 0 });
    });
  }

  for (const mode of [0o755, 0o700]) {
    it(`a closed ${mode.toString(8)} parent still allows recorded removal and real rollback`, () => {
      const { parent, target, file } = fixture(mode);
      const calls = record();
      expect(verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => false })).toEqual({ removed: false, reason: "kept by the final check" });
      expect(fs.lstatSync(target).isDirectory()).toBe(true);
      expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "custody", proceed: () => false })).toEqual({ removed: false, reason: "kept by the final check" });
      expect(fs.readFileSync(file, "utf8")).toBe("log-bytes");
      expect(verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => true })).toEqual({ removed: true, bytes: 9 });
      const ops = destructive(calls).map(call => `${call.op}:${base(call.file)}`);
      expect(ops.filter(op => op.startsWith("unlink:")).sort()).toEqual(["unlink:a.txt", "unlink:file.log", "unlink:inner.txt"]);
      expect(ops.filter(op => op.startsWith("rmdir:"))).toContain("rmdir:target");
      expect(calls.some(call => call.suppressed)).toBe(false);
      expect(fs.lstatSync(parent).mode & 0o777).toBe(mode);
    });
  }

  it("a parent opened to others during proceed(false): no rollback into it, no container cleanup, entry reported where it is", () => {
    const { parent, target } = fixture();
    const ino = fs.lstatSync(target).ino;
    const calls = record();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => (fs.chmodSync(parent, 0o777), false) });
    expect(result).toMatchObject({ removed: false, reason: expect.stringContaining("parent directory is writable by group or others") });
    const graveyard = (result as { graveyard?: string }).graveyard!;
    expect(fs.lstatSync(graveyard).ino).toBe(ino);
    expect(fs.lstatSync(target, { throwIfNoEntry: false })).toBeUndefined();
    expect(destructive(calls)).toEqual([]);
    expect(calls.filter(call => call.op === "rename").map(call => path.basename(call.to!))).toEqual([path.basename(graveyard)]);
  });

  it("a parent opened to others during a file proceed(false): no link back, no graveyard unlink", () => {
    const { parent, file } = fixture();
    const calls = record();
    const link = vi.spyOn(fs, "linkSync");
    const result = verifiedRemove(file, fs.lstatSync(file), { deleter: "custody", proceed: () => (fs.chmodSync(parent, 0o775), false) });
    expect(result).toMatchObject({ removed: false, reason: expect.stringContaining("parent directory is writable") });
    expect(fs.readFileSync((result as { graveyard: string }).graveyard, "utf8")).toBe("log-bytes");
    expect(link).not.toHaveBeenCalled();
    expect(destructive(calls)).toEqual([]);
  });

  it("a parent opened to others during proceed(true): graveyard-internal removal is recorded, the container is not rmdir'd", () => {
    const { parent, target } = fixture();
    const calls = record();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => (fs.chmodSync(parent, 0o777), true) });
    expect(result).toEqual({ removed: true, bytes: 9 });
    const ops = destructive(calls).map(call => `${call.op}:${base(call.file)}`);
    expect(ops.sort()).toEqual(["rmdir:sub", "rmdir:target", "unlink:a.txt", "unlink:inner.txt"]);
    expect(calls.some(call => path.basename(call.file) === GRAVE)).toBe(false);
  });
});

describe.skipIf(process.platform !== "linux")("finding 3: a refusal reports where the held entry actually is", () => {
  it("container renamed while pinned, then a child changed: the reported path exists at the new location, tally agrees", () => {
    const { parent, target } = fixture();
    const ino = fs.lstatSync(target).ino;
    const calls = record();
    const tally = startRemovalTally();
    let replacement: number | undefined;
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => {
      fs.renameSync(path.join(parent, GRAVE), path.join(parent, "renamed-grave"));
      fs.mkdirSync(path.join(parent, GRAVE), { mode: 0o700 }); // a replacement at the old name
      replacement = fs.lstatSync(path.join(parent, GRAVE)).ino;
      const moved = fs.readdirSync(path.join(parent, "renamed-grave"))[0]!;
      fs.utimesSync(path.join(parent, "renamed-grave", moved, "a.txt"), OLD + 60, OLD + 60);
      return true;
    } });
    expect(result).toMatchObject({ removed: false, reason: expect.stringContaining("changed during removal") });
    const graveyard = (result as { graveyard?: string }).graveyard!;
    expect(path.dirname(graveyard)).toBe(fs.realpathSync(path.join(parent, "renamed-grave")));
    expect(fs.lstatSync(graveyard).ino).toBe(ino);
    expect(fs.readFileSync(path.join(graveyard, "sub", "inner.txt"), "utf8")).toBe("inner");
    expect(tally.custody?.graveyards).toEqual([graveyard]);
    expect(destructive(calls)).toEqual([]);
    expect(fs.lstatSync(path.join(parent, GRAVE)).ino).toBe(replacement);
    expect(fs.readdirSync(path.join(parent, GRAVE))).toEqual([]);
  });

  it("an unprovable location is reported as unresolved with the held identity, never as the old path", () => {
    const { parent, target } = fixture();
    const calls = record();
    const tally = startRemovalTally();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => {
      const moved = fs.readdirSync(path.join(parent, GRAVE))[0]!;
      fs.utimesSync(path.join(parent, GRAVE, moved, "a.txt"), OLD + 60, OLD + 60);
      vi.spyOn(fs, "readlinkSync").mockImplementation((() => "/nonexistent-held-grave") as never);
      return true;
    } });
    expect(result).toEqual({ removed: false, reason: expect.stringMatching(/graveyard location unresolved \(held container dev \d+ ino \d+, entry target\./) });
    expect((result as { graveyard?: string }).graveyard).toBeUndefined();
    expect(tally.custody?.graveyards).toEqual([expect.stringMatching(/^unresolved: held graveyard container dev \d+ ino \d+, entry target\./)]);
    // Only the helper's own (non-empty, so a real rmdir refuses) container rmdir; no tree entry.
    expect(destructive(calls).map(call => `${call.op}:${path.basename(call.file)}`)).toEqual([`rmdir:${GRAVE}`]);
  });

  it("an unchanged container still reports its ordinary path", () => {
    const { parent, target } = fixture();
    record();
    const tally = startRemovalTally();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "custody", proceed: () => {
      const moved = fs.readdirSync(path.join(parent, GRAVE))[0]!;
      fs.utimesSync(path.join(parent, GRAVE, moved, "a.txt"), OLD + 60, OLD + 60);
      return true;
    } });
    const graveyard = (result as { graveyard: string }).graveyard;
    expect(path.dirname(graveyard)).toBe(fs.realpathSync(path.join(parent, GRAVE)));
    expect(fs.existsSync(graveyard)).toBe(true);
    expect(tally.custody?.graveyards).toEqual([graveyard]);
  });
});
