import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { platformRemovalSkips, startRemovalTally, stopRemovalTally, verifiedRemove, verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// pi-fabric#1014 round 1 Astra P1: a group/other-writable non-directory entry (a 0664 regular file) in an otherwise
// closed tree can be opened for write by another uid before the move; that descriptor can write after the final
// mtime check and the helper would then delete the new data. Every entry must be closed (mode & 022 == 0) at judgment
// and again at each recheck and right before its unlink; a refusal is counted, nothing is chmod-healed. A symbolic link
// in the tree is exempt (owner decision: its 0777 mode means nothing on Linux and only the link is unlinked, never
// followed); a symbolic link as the target is still refused. Writable-entry refusals at any step are visible without a
// tally (platformRemovalSkips + one coalesced warning).
// (This refuses an entry writable NOW or made writable later; it does not prove no descriptor was opened while a past
// mode allowed it: chmod does not revoke an open descriptor. That last-check window is #10678's design cut.)
// Recorder-only: every unlink/rmdir/rm is captured and never executed; the helper's graveyard rename is real but stays
// inside this file's own temp root, which is removed only after every spy is restored.
const roots: string[] = [];
afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  stopRemovalTally();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const sandbox = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "verified-remove-modes-"));
  fs.chmodSync(root, 0o700);
  roots.push(root);
  return root;
};
const record = () => {
  const ops: string[] = [];
  const realRename = fs.renameSync;
  vi.spyOn(fs, "unlinkSync").mockImplementation(file => { ops.push(`unlink ${path.basename(String(file))}`); });
  vi.spyOn(fs, "rmdirSync").mockImplementation(file => { ops.push(`rmdir ${path.basename(String(file))}`); });
  vi.spyOn(fs, "rmSync").mockImplementation(file => { ops.push(`rm ${String(file)}`); });
  const renames: string[] = [];
  vi.spyOn(fs, "renameSync").mockImplementation((from, to) => { renames.push(String(to)); realRename(from, to); });
  return { ops, renames };
};
/** target/{a.txt, sub/b.txt}; directories 0700, files 0644 unless overridden. */
const tree = (root: string, aMode = 0o644): { target: string; a: string; b: string } => {
  const target = path.join(root, "target");
  fs.mkdirSync(path.join(target, "sub"), { recursive: true });
  fs.chmodSync(target, 0o700); fs.chmodSync(path.join(target, "sub"), 0o700);
  const a = path.join(target, "a.txt"), b = path.join(target, "sub", "b.txt");
  fs.writeFileSync(a, "a"); fs.writeFileSync(b, "b");
  fs.chmodSync(a, aMode); fs.chmodSync(b, 0o644);
  return { target, a, b };
};
const FILE = "a regular file in the tree is writable by group or others";
const GRAVE = `.retention-graveyard-${process.pid}`;
/** The moved entry's real path below this root's graveyard (the helper itself renames through /proc/self/fd). */
const movedEntry = (root: string): string => {
  const [name] = fs.readdirSync(path.join(root, GRAVE));
  return path.join(root, GRAVE, name!);
};
/** The only recorded op a refused move may leave: the attempted rmdir of the (non-empty) graveyard container. */
const noDeletes = (ops: string[]) => expect(ops.map(op => op.replace(/^(\w+) .*\//, "$1 "))
  .filter(op => op !== `rmdir ${GRAVE}`)).toEqual([]);

describe.skipIf(process.platform !== "linux" || typeof process.getuid !== "function")("verifiedRemove refuses group/other-writable files", () => {
  for (const mode of [0o664, 0o666, 0o620, 0o602]) {
    it(`a ${mode.toString(8)} child file: refused at judgment, counted, nothing moved or deleted`, () => {
      const root = sandbox();
      const { target, a } = tree(root, mode);
      const { ops, renames } = record();
      const tally = startRemovalTally();
      const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "modes" });
      expect(result).toEqual({ removed: false, reason: FILE });
      expect(tally.modes!.refused).toEqual({ [FILE]: 1 });
      expect(tally.modes!.removed).toBe(0);
      expect(ops).toEqual([]);
      expect(renames).toEqual([]);
      expect(fs.readFileSync(a, "utf8")).toBe("a");
      expect(fs.lstatSync(a).mode & 0o777).toBe(mode); // never healed
    });
  }

  it("a 0664 root target file is refused, nothing moved or deleted", () => {
    const root = sandbox();
    const file = path.join(root, "run.jsonl");
    fs.writeFileSync(file, "data"); fs.chmodSync(file, 0o664);
    const { ops, renames } = record();
    const tally = startRemovalTally();
    expect(verifiedRemove(file, fs.lstatSync(file), { deleter: "modes" })).toEqual({ removed: false, reason: FILE });
    expect(tally.modes!.refused).toEqual({ [FILE]: 1 });
    expect(ops).toEqual([]);
    expect(renames).toEqual([]);
    expect(fs.readFileSync(file, "utf8")).toBe("data");
  });

  it("a root judged 0664, chmod 0600 before the open: the judged mode refuses it, nothing moved or deleted", () => {
    const root = sandbox();
    const file = path.join(root, "run.jsonl");
    fs.writeFileSync(file, "data"); fs.chmodSync(file, 0o664);
    const judged = fs.lstatSync(file);
    verifiedRemoveTestHooks.beforeOpen = () => fs.chmodSync(file, 0o600);
    const { ops, renames } = record();
    const tally = startRemovalTally();
    expect(verifiedRemove(file, judged, { deleter: "modes" })).toEqual({ removed: false, reason: FILE });
    expect(tally.modes!.refused).toEqual({ [FILE]: 1 });
    expect(ops).toEqual([]);
    expect(renames).toEqual([]);
    expect(fs.readFileSync(file, "utf8")).toBe("data");
  });

  it("a child chmod 0644 -> 0664 during proceed() is refused before any unlink; left in the graveyard, reported", () => {
    const root = sandbox();
    const { target, b } = tree(root);
    const { ops, renames } = record();
    const tally = startRemovalTally();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "modes", proceed: () => {
      fs.chmodSync(path.join(movedEntry(root), "sub", "b.txt"), 0o664);
      return true;
    } });
    expect(result).toMatchObject({ removed: false, reason: `${FILE} (changed during removal; left in the graveyard)` });
    const grave = (result as { graveyard?: string }).graveyard!;
    expect(renames).toHaveLength(1);
    expect(grave).toBe(movedEntry(root));
    expect(fs.readFileSync(path.join(grave, "sub", "b.txt"), "utf8")).toBe("b");
    expect(fs.existsSync(b)).toBe(false); // moved, not deleted
    noDeletes(ops);
    expect(tally.modes!.refused).toEqual({ [`${FILE} (changed during removal; left in the graveyard)`]: 1 });
    expect(tally.modes!.graveyards).toEqual([grave]);
  });

  it("a child chmod 0644 -> 0664 right after the graveyard move is refused before any unlink", () => {
    const root = sandbox();
    const { target } = tree(root);
    const ops: string[] = [];
    vi.spyOn(fs, "unlinkSync").mockImplementation(file => { ops.push(`unlink ${String(file)}`); });
    vi.spyOn(fs, "rmdirSync").mockImplementation(file => { ops.push(`rmdir ${String(file)}`); });
    const realRename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      realRename(from, to);
      fs.chmodSync(path.join(movedEntry(root), "a.txt"), 0o664);
    });
    const tally = startRemovalTally();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "modes" });
    const reason = `${FILE} (swapped during removal; left in the graveyard)`;
    expect(result).toMatchObject({ removed: false, reason, graveyard: movedEntry(root) });
    expect(tally.modes!.refused).toEqual({ [reason]: 1 });
    noDeletes(ops);
    expect(fs.readFileSync(path.join(movedEntry(root), "a.txt"), "utf8")).toBe("a");
  });

  it("positive control: 0600 and 0644 files only are still removed (recorded)", () => {
    const root = sandbox();
    const { target } = tree(root, 0o600);
    const { ops } = record();
    const tally = startRemovalTally();
    expect(verifiedRemove(target, fs.lstatSync(target), { deleter: "modes" })).toMatchObject({ removed: true });
    expect(tally.modes!.removed).toBe(1);
    expect(ops.filter(op => op.startsWith("unlink")).sort()).toEqual(["unlink a.txt", "unlink b.txt"]);
    expect(ops.filter(op => op.startsWith("rm "))).toEqual([]);
  });

  it("positive no-follow control: 0600/0644 files plus a child symlink pointing outside are removed; only the link is unlinked, the outside file is untouched", () => {
    const root = sandbox();
    const { target } = tree(root, 0o600);
    const outside = path.join(root, "outside.txt");
    fs.writeFileSync(outside, "outside"); fs.chmodSync(outside, 0o600);
    fs.symlinkSync(outside, path.join(target, "link"));
    const before = fs.lstatSync(outside);
    const { ops } = record();
    const tally = startRemovalTally();
    expect(verifiedRemove(target, fs.lstatSync(target), { deleter: "modes" })).toMatchObject({ removed: true });
    expect(tally.modes!.removed).toBe(1);
    expect(tally.modes!.refused).toEqual({});
    expect(ops.filter(op => op.startsWith("unlink")).sort()).toEqual(["unlink a.txt", "unlink b.txt", "unlink link"]);
    expect(ops.filter(op => op.startsWith("rm "))).toEqual([]);
    expect(fs.readFileSync(outside, "utf8")).toBe("outside");
    const after = fs.lstatSync(outside);
    expect({ dev: after.dev, ino: after.ino, size: after.size, mode: after.mode, mtimeMs: after.mtimeMs })
      .toEqual({ dev: before.dev, ino: before.ino, size: before.size, mode: before.mode, mtimeMs: before.mtimeMs });
  });

  it("a root symbolic link target is still refused, not followed, nothing moved or deleted", () => {
    const root = sandbox();
    const outside = path.join(root, "outside.txt");
    fs.writeFileSync(outside, "outside"); fs.chmodSync(outside, 0o600);
    const link = path.join(root, "link");
    fs.symlinkSync(outside, link);
    const { ops, renames } = record();
    const tally = startRemovalTally();
    expect(verifiedRemove(link, fs.lstatSync(link), { deleter: "modes" })).toEqual({ removed: false, reason: "target is a symbolic link" });
    expect(tally.modes!.refused).toEqual({ "target is a symbolic link": 1 });
    expect(ops).toEqual([]);
    expect(renames).toEqual([]);
    expect(fs.readlinkSync(link)).toBe(outside);
    expect(fs.readFileSync(outside, "utf8")).toBe("outside");
  });

  it("no tally: a child chmod 0644 -> 0664 during proceed() is refused after the move and visible (platformRemovalSkips +1, one warning x1); nothing in the tree deleted", async () => {
    const root = sandbox();
    const { target } = tree(root);
    const deleter = `modes-late-${process.pid}-${Date.now()}`;
    const reason = `${FILE} (changed during removal; left in the graveyard)`;
    const key = `${deleter}: ${reason}`;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { ops, renames } = record();
    const skipsBefore = platformRemovalSkips()[key] ?? 0;
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter, proceed: () => {
      fs.chmodSync(path.join(movedEntry(root), "sub", "b.txt"), 0o664);
      return true;
    } });
    expect(result).toMatchObject({ removed: false, reason, graveyard: movedEntry(root) });
    expect(renames).toHaveLength(1);
    noDeletes(ops); // only the graveyard container cleanup attempt, recorded
    const grave = (result as { graveyard: string }).graveyard;
    expect(fs.readFileSync(path.join(grave, "a.txt"), "utf8")).toBe("a");
    expect(fs.readFileSync(path.join(grave, "sub", "b.txt"), "utf8")).toBe("b");
    expect(platformRemovalSkips()[key]).toBe(skipsBefore + 1);
    await new Promise<void>(resolve => setTimeout(resolve, 1_100));
    const mine = warn.mock.calls.filter(args => String(args[0]).includes(deleter));
    expect(mine).toEqual([[expect.stringContaining(`${key} x1 (e.g. ${target})`)]]);
  });

  /** Late child chmod 0664 during proceed() with the held graveyard's /proc/self/fd/N link unreadable (location unresolved). */
  const lateUnresolved = (root: string, deleter: string) => {
    const { target } = tree(root);
    const realReadlink = fs.readlinkSync;
    vi.spyOn(fs, "readlinkSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      if (/^\/proc\/self\/fd\/\d+$/.test(String(file))) throw Object.assign(new Error("stubbed"), { code: "EACCES" });
      return (realReadlink as (f: fs.PathLike, o?: unknown) => string)(file, options);
    }) as typeof fs.readlinkSync);
    const recorded = record();
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter, proceed: () => {
      fs.chmodSync(path.join(movedEntry(root), "sub", "b.txt"), 0o664);
      return true;
    } });
    return { target, result, ...recorded };
  };
  const UNRESOLVED = `${FILE} (changed during removal; left in the graveyard); graveyard location unresolved`;

  it("no tally: late child chmod 0664 with an unresolvable graveyard location is visible (platformRemovalSkips +1, one warning x1); nothing in the tree deleted", async () => {
    const root = sandbox();
    const deleter = `modes-unresolved-${process.pid}-${Date.now()}`;
    const key = `${deleter}: ${UNRESOLVED}`;
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const skipsBefore = platformRemovalSkips()[key] ?? 0;
    const { target, result, ops, renames } = lateUnresolved(root, deleter);
    expect(result.removed).toBe(false);
    expect((result as { reason: string }).reason).toMatch(new RegExp(`^${UNRESOLVED.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")} \\(held container dev \\d+ ino \\d+, entry target\\.[0-9a-f-]+\\)$`));
    expect(result).not.toHaveProperty("graveyard");
    expect(renames).toHaveLength(1);
    noDeletes(ops); // only the graveyard container cleanup attempt, recorded
    const moved = movedEntry(root);
    expect(fs.readFileSync(path.join(moved, "a.txt"), "utf8")).toBe("a");
    expect(fs.readFileSync(path.join(moved, "sub", "b.txt"), "utf8")).toBe("b");
    expect(platformRemovalSkips()[key]).toBe(skipsBefore + 1);
    await new Promise<void>(resolve => setTimeout(resolve, 1_100));
    const mine = warn.mock.calls.filter(args => String(args[0]).includes(deleter));
    expect(mine).toEqual([[expect.stringContaining(`${key} x1 (e.g. ${target})`)]]);
  });

  it("tally: late child chmod 0664 with an unresolvable graveyard location counts once, with one unresolved graveyard entry, no skip log", () => {
    const root = sandbox();
    const deleter = `modes-unresolved-tally-${process.pid}-${Date.now()}`;
    const tally = startRemovalTally();
    const { result, ops } = lateUnresolved(root, deleter);
    expect(result).not.toHaveProperty("graveyard");
    expect(tally[deleter]!.refused).toEqual({ [UNRESOLVED]: 1 });
    expect(tally[deleter]!.graveyards).toEqual([expect.stringMatching(/^unresolved: held graveyard container dev \d+ ino \d+, entry target\./)]);
    expect(Object.keys(platformRemovalSkips()).filter(name => name.startsWith(deleter))).toEqual([]);
    noDeletes(ops);
  });

  it("no tally: a 0664 file is refused and visible (platformRemovalSkips +1, one coalesced warning x1); a 0644 file is still removed", async () => {
    const root = sandbox();
    const deleter = `modes-${process.pid}-${Date.now()}`;
    const key = `${deleter}: ${FILE}`;
    const writable = path.join(root, "writable.jsonl"), owner = path.join(root, "owner.jsonl");
    fs.writeFileSync(writable, "data"); fs.chmodSync(writable, 0o664);
    fs.writeFileSync(owner, "keep"); fs.chmodSync(owner, 0o644);
    const before = fs.lstatSync(writable);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const { ops, renames } = record();
    const skipsBefore = platformRemovalSkips()[key] ?? 0;
    expect(verifiedRemove(writable, fs.lstatSync(writable), { deleter })).toEqual({ removed: false, reason: FILE });
    expect(platformRemovalSkips()[key]).toBe(skipsBefore + 1);
    expect(ops).toEqual([]);
    expect(renames).toEqual([]);
    const after = fs.lstatSync(writable);
    expect(fs.readFileSync(writable, "utf8")).toBe("data");
    expect({ ino: after.ino, size: after.size, mode: after.mode & 0o777 }).toEqual({ ino: before.ino, size: 4, mode: 0o664 });
    // The owner-readable counterpart is still removed (its unlink recorded, never executed); no further skip.
    expect(verifiedRemove(owner, fs.lstatSync(owner), { deleter })).toEqual({ removed: true, bytes: 4 });
    expect(ops.filter(op => op.startsWith("unlink"))).toEqual([expect.stringMatching(/^unlink owner\.jsonl\./)]);
    expect(ops.filter(op => !op.startsWith("unlink"))).toEqual([`rmdir ${GRAVE}`]); // the container cleanup, recorded
    expect(renames).toHaveLength(1); // the controlled graveyard move inside this root
    expect(platformRemovalSkips()[key]).toBe(skipsBefore + 1);
    await new Promise<void>(resolve => setTimeout(resolve, 1_100));
    const mine = warn.mock.calls.filter(args => String(args[0]).includes(deleter));
    expect(mine).toEqual([[expect.stringContaining(`${key} x1 (e.g. ${writable})`)]]);
  });
});
