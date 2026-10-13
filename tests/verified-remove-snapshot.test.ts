import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startRemovalTally, stopRemovalTally, verifiedRemove, type VerifiedRemoveResult } from "../src/storage/verified-remove.js";

// smarty-dev#7916 helper refinement: the verified removal keeps the pre-delete judgment of every tree entry (identity,
// type, owner, mtime, the exact name set) and refuses any difference, also after the graveyard move and after proceed().
// No product delete ever runs here: fs.unlinkSync and fs.rmdirSync are replaced with recorders that capture each call
// (and the inode then at that name) without executing it. Refusal cases must record no deletion of a tree entry; the
// only call allowed is the helper's final rmdir of its own graveyard container (real rmdir refuses it: not empty).
const DAY = 24 * 60 * 60 * 1_000;
const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  stopRemovalTally();
  // Only this file's own temp roots, and only after every stub is restored.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const OLD = (Date.now() - 30 * DAY) / 1000;
const write = (file: string, data: string) => { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, data); };
const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isSymbolicLink()) return ["link", fs.readlinkSync(file)];
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};
/** target/{a.txt, old, sub/{inner.txt, empty/}}, every entry aged 30 days. */
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-vrs-"));
  roots.push(root);
  const parent = path.join(root, "parent");
  const outside = path.join(root, "outside");
  fs.mkdirSync(outside);
  const target = path.join(parent, "target");
  write(path.join(target, "a.txt"), "aaaa");
  write(path.join(target, "old"), "old-bytes");
  write(path.join(target, "sub", "inner.txt"), "inner");
  fs.mkdirSync(path.join(target, "sub", "empty"));
  for (const file of ["a.txt", "old", "sub/inner.txt", "sub/empty", "sub", ""]) fs.utimesSync(path.join(target, file), OLD, OLD);
  return { root, parent, outside, target, judged: fs.lstatSync(target), before: shape(target) };
};
/** Recorded, never executed: every unlink/rmdir the helper attempts, with the inode at that name at call time. */
const record = () => {
  const calls: Array<{ op: string; file: string; ino: number | undefined }> = [];
  const capture = (op: string) => ((file: fs.PathLike) => {
    calls.push({ op, file: String(file), ino: fs.lstatSync(file, { throwIfNoEntry: false })?.ino });
  }) as never;
  vi.spyOn(fs, "unlinkSync").mockImplementation(capture("unlink"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(capture("rmdir"));
  return calls;
};
const graveContainer = (file: string) => /^\/proc\/self\/fd\/\d+\/\.retention-graveyard-\d+$/.test(file);
const treeDeletes = (calls: Array<{ op: string; file: string }>) => calls.filter(call => !(call.op === "rmdir" && graveContainer(call.file)));
/** Refused, reported with a graveyard, nothing deleted, and the graveyard holds what the swap left there. */
const expectRefused = (result: VerifiedRemoveResult, calls: Array<{ op: string; file: string }>) => {
  expect(treeDeletes(calls)).toEqual([]);
  expect(result.removed).toBe(false);
  const graveyard = (result as { graveyard?: string }).graveyard;
  expect(graveyard, `reason: ${(result as { reason: string }).reason}`).toEqual(expect.stringContaining(".retention-graveyard-"));
  return graveyard!;
};

describe.skipIf(process.platform !== "linux")("verified removal keeps the judged tree snapshot (smarty-dev#7916)", () => {
  it("positive control: an unswapped tree is removed entry by entry (recorded), with its bytes", () => {
    const { target, judged } = fixture();
    const calls = record();
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => true });
    expect(result).toEqual({ removed: true, bytes: 4 + 9 + 5 });
    const tree = treeDeletes(calls);
    expect(tree.filter(call => call.op === "unlink").map(call => path.basename(call.file)).sort()).toEqual(["a.txt", "inner.txt", "old"]);
    expect(tree.filter(call => call.op === "rmdir").map(call => path.basename(call.file).replace(/\.[0-9a-f-]{36}$/, "")).sort()).toEqual(["empty", "sub", "target"]);
  });

  it("positive control: a dry run records no deletion", () => {
    const { target, judged, before } = fixture();
    const calls = record();
    expect(verifiedRemove(target, judged, { deleter: "snap", dryRun: true })).toEqual({ removed: true, bytes: 18 });
    expect(calls).toEqual([]);
    expect(shape(target)).toEqual(before);
  });

  it("refuses a refreshed file even when the final check asks to keep it; rollback unlinks nothing", () => {
    const { target } = fixture();
    const file = path.join(target, "a.txt");
    const judged = fs.lstatSync(file);
    const calls = record();
    const result = verifiedRemove(file, judged, { deleter: "snap", proceed: () => {
      const grave = path.join(target, `.retention-graveyard-${process.pid}`);
      const moved = path.join(grave, fs.readdirSync(grave)[0]!);
      const now = Date.now() / 1000;
      fs.utimesSync(moved, now, now);
      return false;
    } });
    const graveyard = expectRefused(result, calls);
    expect(fs.readFileSync(graveyard, "utf8")).toBe("aaaa");
    expect(fs.existsSync(file)).toBe(false);
  });

  it.each(["file", "directory"] as const)("positive keep control: an unchanged %s goes back to its name", kind => {
    const { target, before } = fixture();
    const file = kind === "file" ? path.join(target, "a.txt") : target;
    const judged = fs.lstatSync(file);
    const calls = record();
    const result = verifiedRemove(file, judged, { deleter: "snap", proceed: () => false });
    expect(result).toEqual({ removed: false, reason: "kept by the final check" });
    expect(fs.lstatSync(file).ino).toBe(judged.ino);
    if (kind === "file") {
      expect(fs.readFileSync(file, "utf8")).toBe("aaaa");
      // Rollback unlinks only the new graveyard hardlink to the verified file (recorded, not executed).
      expect(treeDeletes(calls).map(call => call.op)).toEqual(["unlink"]);
    } else {
      expect(shape(target)).toEqual(before);
      expect(treeDeletes(calls)).toEqual([]);
    }
  });

  it("refuses a child replaced by a new same-type inode during proceed (the recorded probe)", () => {
    const { target, outside, judged } = fixture();
    const calls = record();
    let replacement: number | undefined;
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => {
      const grave = path.join(path.dirname(target), fs.readdirSync(path.dirname(target)).find(name => name.startsWith(".retention-graveyard-"))!);
      const moved = path.join(grave, fs.readdirSync(grave)[0]!);
      fs.renameSync(path.join(moved, "old"), path.join(outside, "original-old"));
      write(path.join(moved, "old"), "new-bytes");
      fs.utimesSync(path.join(moved, "old"), OLD, OLD);
      replacement = fs.lstatSync(path.join(moved, "old")).ino;
      return true;
    } });
    expect(calls.some(call => call.ino === replacement)).toBe(false);
    const graveyard = expectRefused(result, calls);
    expect(fs.readFileSync(path.join(graveyard, "old"), "utf8")).toBe("new-bytes");
    expect(fs.readFileSync(path.join(outside, "original-old"), "utf8")).toBe("old-bytes");
  });

  it("refuses a child replaced after the tree walk, before the graveyard move", () => {
    const { target, outside, judged } = fixture();
    const calls = record();
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
      if (String(from).startsWith("/proc/self/fd/") && path.basename(String(from)) === "target") {
        rename(path.join(target, "old"), path.join(outside, "original-old"));
        write(path.join(target, "old"), "new-bytes");
        fs.utimesSync(path.join(target, "old"), OLD, OLD);
        fs.utimesSync(target, OLD, OLD); // even the root's own mtime is put back
      }
      return rename(from, to);
    }) as never);
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => true });
    const graveyard = expectRefused(result, calls);
    expect(fs.readFileSync(path.join(graveyard, "old"), "utf8")).toBe("new-bytes");
    expect(fs.readFileSync(path.join(outside, "original-old"), "utf8")).toBe("old-bytes");
  });

  it("refuses a child whose mtime was refreshed in place", () => {
    const { target, judged, before } = fixture();
    const calls = record();
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => {
      const grave = path.join(path.dirname(target), fs.readdirSync(path.dirname(target)).find(name => name.startsWith(".retention-graveyard-"))!);
      const now = Date.now() / 1000;
      fs.utimesSync(path.join(grave, fs.readdirSync(grave)[0]!, "sub", "inner.txt"), now, now);
      return true;
    } });
    const graveyard = expectRefused(result, calls);
    expect(shape(graveyard)).toEqual(before);
  });

  it("refuses an entry that was not seen at judgment (a new child), and never deletes it", () => {
    const { target, judged } = fixture();
    const calls = record();
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => {
      const grave = path.join(path.dirname(target), fs.readdirSync(path.dirname(target)).find(name => name.startsWith(".retention-graveyard-"))!);
      const moved = path.join(grave, fs.readdirSync(grave)[0]!);
      write(path.join(moved, "sub", "unseen"), "unseen");
      fs.utimesSync(path.join(moved, "sub", "unseen"), OLD, OLD);
      fs.utimesSync(path.join(moved, "sub"), OLD, OLD);
      return true;
    } });
    const graveyard = expectRefused(result, calls);
    expect(fs.readFileSync(path.join(graveyard, "sub", "unseen"), "utf8")).toBe("unseen");
    expect(fs.readFileSync(path.join(graveyard, "a.txt"), "utf8")).toBe("aaaa");
  });

  it("refuses a file swapped to a symbolic link", () => {
    const { target, outside, judged } = fixture();
    const calls = record();
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => {
      const grave = path.join(path.dirname(target), fs.readdirSync(path.dirname(target)).find(name => name.startsWith(".retention-graveyard-"))!);
      const moved = path.join(grave, fs.readdirSync(grave)[0]!);
      fs.renameSync(path.join(moved, "a.txt"), path.join(outside, "a.txt"));
      fs.symlinkSync(path.join(outside, "a.txt"), path.join(moved, "a.txt"));
      return true;
    } });
    const graveyard = expectRefused(result, calls);
    expect(fs.readlinkSync(path.join(graveyard, "a.txt"))).toBe(path.join(outside, "a.txt"));
    expect(fs.readFileSync(path.join(outside, "a.txt"), "utf8")).toBe("aaaa");
  });

  it("refuses a nested directory whose reopened fd reports a foreign owner (mocked fstat)", () => {
    const { target, judged, before } = fixture();
    const calls = record();
    const paths = new Map<number, string>();
    const openSync = fs.openSync, fstatSync = fs.fstatSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      const fd = (openSync as (...args: unknown[]) => number)(file, ...rest);
      paths.set(fd, String(file));
      return fd;
    }) as never);
    let armed = false;
    vi.spyOn(fs, "fstatSync").mockImplementation(((fd: number, ...rest: unknown[]) => {
      const stat = (fstatSync as (...args: unknown[]) => fs.Stats)(fd, ...rest);
      if (!armed || !String(paths.get(fd)).endsWith("/empty")) return stat;
      return Object.assign(Object.create(Object.getPrototypeOf(stat) as object) as fs.Stats, stat, { uid: stat.uid + 1 });
    }) as never);
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => (armed = true) });
    const graveyard = expectRefused(result, calls);
    expect(shape(graveyard)).toEqual(before);
  });

  it("refuses a root whose mtime was refreshed between the first verify and the graveyard move", () => {
    const { target, judged, before } = fixture();
    const calls = record();
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation(((from: fs.PathLike, to: fs.PathLike) => {
      if (String(from).startsWith("/proc/self/fd/") && path.basename(String(from)) === "target") {
        const now = Date.now() / 1000;
        fs.utimesSync(target, now, now);
      }
      return rename(from, to);
    }) as never);
    let proceeded = false;
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => (proceeded = true) });
    expect(proceeded).toBe(false);
    const graveyard = expectRefused(result, calls);
    expect(shape(graveyard)).toEqual(before);
  });

  it("reports the graveyard of a refused entry in the tally, so a caller that keeps only .removed loses nothing", () => {
    const { target, judged } = fixture();
    const calls = record();
    const tally = startRemovalTally();
    const result = verifiedRemove(target, judged, { deleter: "snap", proceed: () => {
      const grave = path.join(path.dirname(target), fs.readdirSync(path.dirname(target)).find(name => name.startsWith(".retention-graveyard-"))!);
      const now = Date.now() / 1000;
      fs.utimesSync(path.join(grave, fs.readdirSync(grave)[0]!, "a.txt"), now, now);
      return true;
    } });
    const graveyard = expectRefused(result, calls);
    expect(tally.snap).toMatchObject({ removed: 0, graveyards: [graveyard] });
  });
});
