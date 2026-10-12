import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { setWindowsVerifiedRemoval, startRemovalTally, stopRemovalTally, verifiedRemove } from "../src/storage/verified-remove.js";

// smarty-dev#7916 platform decision: without getuid OR geteuid nothing is deleted on any platform (Windows helper path
// included), and off Linux the path-based `fallback` is never invoked, even where getuid exists. No product delete runs
// here: fs.unlinkSync and fs.rmdirSync are recorders that capture each call without executing it.
const DAY = 24 * 60 * 60 * 1_000;
const OLD = (Date.now() - 30 * DAY) / 1000;
const roots: string[] = [];
const restores: Array<() => void> = [];
afterEach(() => {
  for (const restore of restores.splice(0).reverse()) restore();
  vi.restoreAllMocks();
  stopRemovalTally();
  setWindowsVerifiedRemoval(false);
  // Only this file's own temp roots, and only after every stub is restored.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const override = (key: "platform" | "getuid" | "geteuid", value: unknown) => {
  const descriptor = Object.getOwnPropertyDescriptor(process, key);
  Object.defineProperty(process, key, { value, configurable: true, writable: true });
  restores.push(() => descriptor ? Object.defineProperty(process, key, descriptor) : delete (process as unknown as Record<string, unknown>)[key]);
};
const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};
/** parent/{target/{a.txt, sub/inner.txt}, file.log}, aged 30 days. */
const fixture = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-vrp-"));
  roots.push(root);
  const parent = path.join(root, "parent");
  const target = path.join(parent, "target");
  fs.mkdirSync(path.join(target, "sub"), { recursive: true });
  fs.writeFileSync(path.join(target, "a.txt"), "aaaa");
  fs.writeFileSync(path.join(target, "sub", "inner.txt"), "inner");
  const file = path.join(parent, "file.log");
  fs.writeFileSync(file, "log-bytes");
  for (const entry of [path.join(target, "a.txt"), path.join(target, "sub", "inner.txt"), path.join(target, "sub"), target, file]) fs.utimesSync(entry, OLD, OLD);
  return { root, parent, target, file, before: shape(parent) };
};
const record = () => {
  const calls: Array<{ op: string; file: string; ino: number | undefined }> = [];
  const capture = (op: string) => ((file: fs.PathLike) => {
    calls.push({ op, file: String(file), ino: fs.lstatSync(file, { throwIfNoEntry: false })?.ino });
  }) as never;
  vi.spyOn(fs, "unlinkSync").mockImplementation(capture("unlink"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(capture("rmdir"));
  return calls;
};

describe("verified removal deletes nothing without ownership proof, on every platform", () => {
  for (const platform of ["linux", "darwin", "win32"] as const) {
    for (const missing of ["getuid", "geteuid"] as const) {
      it(`${platform}: no ${missing} refuses apply and dry run; fallback never runs`, () => {
        const { parent, target, file, before } = fixture();
        const calls = record();
        const judgedTree = fs.lstatSync(target), judgedFile = fs.lstatSync(file);
        override(missing, undefined);
        override("platform", platform);
        setWindowsVerifiedRemoval(true);
        const tally = startRemovalTally();
        let fallbackRan = 0;
        const fallback = () => { fallbackRan++; return true; };
        const results = [
          verifiedRemove(target, judgedTree, { deleter: "plat", fallback }),
          verifiedRemove(target, judgedTree, { deleter: "plat", fallback, dryRun: true }),
          verifiedRemove(file, judgedFile, { deleter: "plat", fallback, proceed: () => true }),
        ];
        for (const result of results) expect(result).toEqual({ removed: false, reason: expect.stringContaining("no getuid") });
        expect(fallbackRan).toBe(0);
        expect(calls).toEqual([]);
        expect(tally.plat).toMatchObject({ removed: 0, bytes: 0 });
        expect(shape(parent)).toEqual(before);
      });
    }
  }

  for (const platform of ["darwin", "win32"] as const) {
    it(`${platform} with getuid: the path-based fallback is a refusal, never invoked`, () => {
      const { parent, target, file, before } = fixture();
      const calls = record();
      const judgedTree = fs.lstatSync(target), judgedFile = fs.lstatSync(file);
      if (typeof process.getuid !== "function") override("getuid", () => 0);
      if (typeof process.geteuid !== "function") override("geteuid", () => 0);
      override("platform", platform);
      let fallbackRan = 0;
      const fallback = () => { fallbackRan++; return true; };
      for (const result of [
        verifiedRemove(target, judgedTree, { deleter: "plat", fallback }),
        verifiedRemove(target, judgedTree, { deleter: "plat", fallback, dryRun: true }),
        verifiedRemove(file, judgedFile, { deleter: "plat", fallback }),
      ]) expect(result).toMatchObject({ removed: false, reason: expect.stringContaining("nothing is deleted") });
      expect(fallbackRan).toBe(0);
      expect(calls).toEqual([]);
      expect(shape(parent)).toEqual(before);
    });
  }
});

describe.skipIf(process.platform !== "linux")("Linux positive controls (deletion recorded, never executed)", () => {
  it("a judged file is unlinked once, from the graveyard, and counted", () => {
    const { file } = fixture();
    const calls = record();
    const tally = startRemovalTally();
    let fallbackRan = false;
    const result = verifiedRemove(file, fs.lstatSync(file), { deleter: "plat", proceed: () => true, fallback: () => (fallbackRan = true) });
    expect(result).toEqual({ removed: true, bytes: 9 });
    expect(fallbackRan).toBe(false);
    expect(calls.filter(call => call.op === "unlink").map(call => path.basename(call.file).replace(/\.[0-9a-f-]{36}$/, ""))).toEqual(["file.log"]);
    expect(tally.plat).toMatchObject({ removed: 1, bytes: 9 });
  });

  it("a judged tree is removed entry by entry; the graveyard container rmdir is its own opened inode", () => {
    const { target, parent } = fixture();
    const calls = record();
    let container: number | undefined;
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "plat", proceed: () => {
      container = fs.lstatSync(path.join(parent, `.retention-graveyard-${process.pid}`)).ino;
      return true;
    } });
    expect(result).toEqual({ removed: true, bytes: 9 });
    const names = calls.map(call => `${call.op}:${path.basename(call.file).replace(/\.[0-9a-f-]{36}$/, "")}`);
    expect(names.filter(name => name.startsWith("unlink:")).sort()).toEqual(["unlink:a.txt", "unlink:inner.txt"]);
    expect(names.filter(name => name.startsWith("rmdir:"))).toEqual(["rmdir:sub", "rmdir:target", `rmdir:.retention-graveyard-${process.pid}`]);
    expect(calls.at(-1)!.ino).toBe(container);
  });

  it("a graveyard container replaced by name while pinned is never removed by name", () => {
    const { target, parent, root } = fixture();
    const calls = record();
    const graveName = `.retention-graveyard-${process.pid}`;
    let replacement: number | undefined;
    const result = verifiedRemove(target, fs.lstatSync(target), { deleter: "plat", proceed: () => {
      fs.renameSync(path.join(parent, graveName), path.join(root, "original-grave"));
      fs.mkdirSync(path.join(parent, graveName), { mode: 0o700 });
      replacement = fs.lstatSync(path.join(parent, graveName)).ino;
      return true;
    } });
    expect(result).toMatchObject({ removed: true });
    expect(replacement).toBeDefined();
    expect(calls.some(call => call.ino === replacement)).toBe(false);
    expect(calls.filter(call => path.basename(call.file) === graveName)).toEqual([]);
    expect(fs.lstatSync(path.join(parent, graveName)).ino).toBe(replacement);
  });
});
