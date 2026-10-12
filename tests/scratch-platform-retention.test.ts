import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SCRATCH_OWNER_FILE, sweepScratch } from "../src/storage/scratch.js";
import { platformRemovalSkips, startRemovalTally, stopRemovalTally, verifiedRemove } from "../src/storage/verified-remove.js";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

// pi-fabric#1014 SEC P2-2: off Linux, managed scratch keeps main's (c0b67ef) path-based removal after every candidate
// recheck; the verified helper's platform/no-uid refusal stays for every other deleter, now counted and logged in one
// coalesced warning per batch.
// Recorder-only: fs.rmSync, fs.unlinkSync and fs.rmdirSync (and fs.renameSync off Linux) capture each call and never
// execute it, so no product deletion runs here. A forced process.platform is not native Darwin or Windows evidence:
// it proves which removal sweepScratch dispatches, with which target and options, and what it reports.
const HOUR = 3_600_000;
// The fixture's own concurrent replacement moves its directory aside with the real rename, not the recorder.
const realRename = fs.renameSync;
const roots: string[] = [];
const restores: Array<() => void> = [];
afterEach(() => {
  for (const restore of restores.splice(0).reverse()) restore();
  vi.restoreAllMocks();
  // Only this file's own temp roots, and only after every stub is restored.
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const override = (key: "platform" | "getuid" | "geteuid", value: unknown) => {
  const descriptor = Object.getOwnPropertyDescriptor(process, key);
  Object.defineProperty(process, key, { value, configurable: true, writable: true });
  restores.push(() => descriptor ? Object.defineProperty(process, key, descriptor) : delete (process as unknown as Record<string, unknown>)[key]);
};
const sandbox = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "scratch-platform-"));
  roots.push(root);
  return root;
};
const fixture = (tempRoot: string, extra: Record<string, unknown> = {}): string => {
  const directory = fs.mkdtempSync(path.join(tempRoot, "pi-fabric-output-"));
  fs.writeFileSync(path.join(directory, SCRATCH_OWNER_FILE), JSON.stringify({
    app: "pi-fabric-scratch", version: 1, kind: "output", pid: process.pid, createdAt: 1, ...extra,
  }));
  fs.writeFileSync(path.join(directory, "output.txt"), "data");
  return directory;
};
const shape = (file: string): unknown => {
  const stat = fs.lstatSync(file, { throwIfNoEntry: false });
  if (!stat) return null;
  if (stat.isDirectory()) return Object.fromEntries(fs.readdirSync(file).sort().map(name => [name, shape(path.join(file, name))]));
  return fs.readFileSync(file, "utf8");
};
/** Records every product delete without executing it; `rename` too unless the Linux helper needs its graveyard move. */
const record = (rename = true) => {
  const calls: Array<{ op: string; file: string; options?: unknown }> = [];
  const capture = (op: string) => ((file: fs.PathLike, options?: unknown) => {
    calls.push(options === undefined ? { op, file: String(file) } : { op, file: String(file), options });
  }) as never;
  vi.spyOn(fs, "rmSync").mockImplementation(capture("rm"));
  vi.spyOn(fs, "unlinkSync").mockImplementation(capture("unlink"));
  vi.spyOn(fs, "rmdirSync").mockImplementation(capture("rmdir"));
  if (rename) vi.spyOn(fs, "renameSync").mockImplementation(capture("rename"));
  return calls;
};
/** Rewrites a candidate between its judgment and its final recheck (the second lstat of the root). */
const changeAfterJudgment = (directory: string, change: () => void) => {
  const original = fs.lstatSync;
  let calls = 0;
  vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike, options?: never) => {
    if (file === directory && ++calls === 2) change();
    return original(file, options);
  }) as typeof fs.lstatSync);
};

describe("off Linux, scratch keeps main's removal after every candidate recheck (recorded, never executed)", () => {
  for (const platform of ["darwin", "win32"] as const) {
    it(`${platform}: the expired closed root reaches exactly main's recursive removal; others remain`, async () => {
      const tempRoot = sandbox();
      const old = fixture(tempRoot, { closedAt: 1 });
      const young = fixture(tempRoot, { closedAt: 30 * HOUR });
      const active = fixture(tempRoot);
      const unknown = fixture(tempRoot, { closedAt: 1 });
      fs.writeFileSync(path.join(unknown, "mine.txt"), "private");
      const before = shape(tempRoot);
      const calls = record();
      override("platform", platform);
      const dry = await sweepScratch({ tempRoot, now: 30 * HOUR + 1, dryRun: true });
      expect(dry).toMatchObject({ eligible: [old], removed: [] });
      expect(calls).toEqual([]);
      const result = await sweepScratch({ tempRoot, now: 30 * HOUR + 1 });
      expect(result).toMatchObject({ eligible: [old], removed: [old] });
      expect(calls).toEqual([{ op: "rm", file: old, options: { recursive: true, force: true } }]);
      // The recorder executed nothing: every fixture, the removed one included, is byte-for-byte intact.
      expect(shape(tempRoot)).toEqual(before);
      for (const kept of [young, active, unknown]) expect(result.removed).not.toContain(kept);
    });

    for (const change of ["replaced", "reactivated", "refreshed"] as const) {
      it(`${platform}: a ${change} candidate is not dispatched`, async () => {
        const tempRoot = sandbox();
        const directory = fixture(tempRoot, { closedAt: 1 });
        changeAfterJudgment(directory, () => {
          if (change === "replaced") {
            realRename(directory, `${directory}.saved`);
            fs.mkdirSync(directory);
            fs.writeFileSync(path.join(directory, "mine.txt"), "new active directory");
          } else if (change === "reactivated") {
            const marker = path.join(directory, SCRATCH_OWNER_FILE);
            const owner = JSON.parse(fs.readFileSync(marker, "utf8"));
            delete owner.closedAt;
            fs.writeFileSync(marker, JSON.stringify(owner));
          } else fs.appendFileSync(path.join(directory, "output.txt"), " and more");
        });
        const calls = record();
        override("platform", platform);
        expect(await sweepScratch({ tempRoot, now: 100 * HOUR })).toMatchObject({ eligible: [directory], removed: [] });
        expect(calls).toEqual([]);
      });
    }
  }

  it("darwin without getuid is main's removal too (main ran the fallback before its uid check)", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, { closedAt: 1 });
    const calls = record();
    override("getuid", undefined);
    override("geteuid", undefined);
    override("platform", "darwin");
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR })).removed).toEqual([old]);
    expect(calls).toEqual([{ op: "rm", file: old, options: { recursive: true, force: true } }]);
  });
});

/** The coalesced skip warning flushes after its 1 s batch window. */
const flushed = () => new Promise<void>(resolve => setTimeout(resolve, 1_100));
const skipCount = (prefix: string): number => Object.entries(platformRemovalSkips())
  .filter(([name]) => name.startsWith(prefix)).reduce((sum, [, count]) => sum + count, 0);

describe("remaining platform refusals and failed removals are counted and logged as one batch warning, not silent", () => {
  it("darwin: a failed main removal is not reported removed; counted and warned", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, { closedAt: 1 });
    const calls: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation(((file: fs.PathLike) => {
      calls.push(String(file));
      throw Object.assign(new Error("denied"), { code: "EACCES" });
    }) as never);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    override("platform", "darwin");
    const prefix = "sweepScratch: removal failed (main's path on this platform, EACCES)";
    const start = skipCount(prefix);
    expect(await sweepScratch({ tempRoot, now: 100 * HOUR })).toMatchObject({ eligible: [old], removed: [] });
    expect(calls).toEqual([old]);
    expect(skipCount(prefix) - start).toBe(1);
    await flushed();
    expect(warn.mock.calls.map(args => String(args[0])).filter(line => line.includes(prefix))).toEqual([
      expect.stringContaining(`${prefix} x1 (e.g. ${old})`)]);
  });

  it("darwin under an active report tally: a failed main removal is a refusal in the tally, no warning, no double count", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, { closedAt: 1 });
    const calls: string[] = [];
    vi.spyOn(fs, "rmSync").mockImplementation(((file: fs.PathLike) => {
      calls.push(String(file));
      throw Object.assign(new Error("denied"), { code: "EACCES" });
    }) as never);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    override("platform", "darwin");
    const reason = "removal failed (main's path on this platform, EACCES)";
    const start = skipCount(`sweepScratch: ${reason}`);
    const tally = startRemovalTally();
    restores.push(stopRemovalTally);
    expect(await sweepScratch({ tempRoot, now: 100 * HOUR })).toMatchObject({ eligible: [old], removed: [] });
    stopRemovalTally();
    expect(calls).toEqual([old]);
    expect(tally).toEqual({ sweepScratch: { removed: 0, bytes: 0, refused: { [reason]: 1 }, refusedBytes: {}, examples: { [reason]: [old] } } });
    expect(skipCount(`sweepScratch: ${reason}`) - start).toBe(0);
    await flushed();
    expect(warn.mock.calls.filter(args => String(args[0]).includes(reason))).toEqual([]);
  });

  it("a short process that exits naturally right after a platform skip still emits the counted warning", () => {
    // Child: forced darwin makes verifiedRemove refuse before any filesystem operation; no product operation runs.
    const helper = pathToFileURL(path.resolve("src/storage/verified-remove.ts")).href;
    const child = spawnSync(process.execPath.includes("bun") ? "node" : process.execPath, ["--input-type=module", "-e", [
      `const m = await import(${JSON.stringify(helper)});`,
      `Object.defineProperty(process, "platform", { value: "darwin" });`,
      `const r = m.verifiedRemove("/nonexistent/scratch-probe", undefined, { deleter: "probe" });`,
      `if (r.removed) process.exitCode = 3;`,
    ].join("\n")], { encoding: "utf8", timeout: 20_000 });
    expect(child.status).toBe(0);
    expect(child.stderr).toContain("probe: directory-relative removal needs Linux /proc/self/fd; nothing is deleted x1 (e.g. /nonexistent/scratch-probe)");
  });

  it("Linux without getuid: scratch is not removed by any path; the skips are counted, one batch warning", async () => {
    const tempRoot = sandbox();
    const first = fixture(tempRoot, { closedAt: 1 });
    const second = fixture(tempRoot, { closedAt: 2 });
    const before = shape(tempRoot);
    const calls = record();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    override("getuid", undefined);
    override("geteuid", undefined);
    override("platform", "linux");
    const prefix = "sweepScratch: file ownership cannot be proven";
    const start = skipCount(prefix);
    const result = await sweepScratch({ tempRoot, now: 100 * HOUR });
    expect(result).toMatchObject({ removed: [] });
    expect(result.eligible.sort()).toEqual([first, second].sort());
    expect(calls).toEqual([]);
    expect(shape(tempRoot)).toEqual(before);
    expect(skipCount(prefix) - start).toBe(2);
    await flushed();
    // One warning for the whole sweep (not one per root), with its count.
    const warned = warn.mock.calls.map(args => String(args[0])).filter(line => line.includes(prefix));
    expect(warned).toEqual([expect.stringMatching(/sweepScratch: file ownership cannot be proven .* x2 \(e\.g\. /)]);
  });

  it("another deleter off Linux still refuses (fallback never invoked); counted, one batch warning", async () => {
    const parent = sandbox();
    const target = path.join(parent, "target");
    fs.mkdirSync(target);
    fs.writeFileSync(path.join(target, "a.txt"), "aaaa");
    const before = shape(parent);
    const calls = record();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    if (typeof process.getuid !== "function") override("getuid", () => 0);
    if (typeof process.geteuid !== "function") override("geteuid", () => 0);
    override("platform", "darwin");
    let fallbackRan = 0;
    const deleter = `other-${process.pid}-${Date.now()}`;
    for (let i = 0; i < 3; i++) {
      expect(verifiedRemove(target, fs.lstatSync(target), { deleter, fallback: () => (++fallbackRan, true) }))
        .toMatchObject({ removed: false, reason: expect.stringContaining("nothing is deleted") });
    }
    expect(fallbackRan).toBe(0);
    expect(calls).toEqual([]);
    expect(shape(parent)).toEqual(before);
    const skips = Object.entries(platformRemovalSkips()).filter(([name]) => name.startsWith(`${deleter}:`));
    expect(skips.map(([, count]) => count)).toEqual([3]);
    await flushed();
    expect(warn.mock.calls.filter(args => String(args[0]).includes(deleter))).toEqual([[expect.stringContaining(`${deleter}: directory-relative removal needs Linux /proc/self/fd; nothing is deleted x3`)]]);
  });
});

describe.skipIf(process.platform !== "linux" || typeof process.getuid !== "function")("Linux positive control: the verified helper, not the path removal", () => {
  it("an expired closed root is unlinked entry by entry from the graveyard (recorded, never executed)", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, { closedAt: 1 });
    const calls = record(false);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR })).removed).toEqual([old]);
    expect(calls.filter(call => call.op === "rm")).toEqual([]);
    const names = calls.map(call => `${call.op}:${path.basename(call.file).replace(/\.[0-9a-f-]{36}$/, "")}`);
    expect(names.filter(name => name.startsWith("unlink:")).sort()).toEqual([`unlink:${SCRATCH_OWNER_FILE}`, "unlink:output.txt"]);
    expect(names.filter(name => name.startsWith("rmdir:"))).toEqual([`rmdir:${path.basename(old)}`, `rmdir:.retention-graveyard-${process.pid}`]);
    await flushed();
    expect(warn).not.toHaveBeenCalled();
  });
});

// Real deletion, hosted CI only (GitHub Actions sets GITHUB_ACTIONS): never run on a developer or agent host. With
// process.platform forced to darwin on a hosted runner, the restored path removes the expired root's actual inode and
// leaves the active root; it is still not native Darwin evidence (the planned native macOS E2E is separate).
describe.runIf(process.env.GITHUB_ACTIONS === "true")("hosted CI: actual off-Linux scratch reclamation", () => {
  it("darwin (forced): the expired closed root is gone, the active root and its identity remain", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, { closedAt: 1 });
    const active = fixture(tempRoot);
    const activeIno = fs.lstatSync(active).ino;
    override("platform", "darwin");
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR })).removed).toEqual([old]);
    expect(fs.existsSync(old)).toBe(false);
    expect(fs.lstatSync(active).ino).toBe(activeIno);
  });
});
