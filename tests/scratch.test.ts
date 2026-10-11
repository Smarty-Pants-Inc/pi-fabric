import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { closeScratch, createScratch, SCRATCH_OWNER_FILE, sweepScratch } from "../src/storage/scratch.js";
import { NativeReaderCheckpoint } from "../src/ui/conversation-native-reader-checkpoint.js";

const HOUR = 3_600_000;
const roots: string[] = [];
const sandbox = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "scratch-test-"));
  roots.push(root);
  return root;
};
const fixture = (tempRoot: string, kind: "output" | "shell" | "checkpoint" = "output", extra: Record<string, unknown> = {}): string => {
  const directory = fs.mkdtempSync(path.join(tempRoot, kind === "checkpoint" ? "pi-native-reader-" : `pi-fabric-${kind}-`));
  fs.writeFileSync(path.join(directory, SCRATCH_OWNER_FILE), JSON.stringify({
    app: "pi-fabric-scratch", version: 1, kind, pid: process.pid, createdAt: 1, ...extra,
  }));
  fs.writeFileSync(path.join(directory, kind === "output" ? "output.txt" : kind === "shell" ? "output.log" : "checkpoint"), "data");
  return directory;
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("managed scratch retention", () => {
  it.each(["replacement", "reactivation"])("rechecks a candidate after concurrent %s", async change => {
    const tempRoot = sandbox();
    const directory = fixture(tempRoot, "output", { closedAt: 1 });
    const original = fs.lstatSync;
    let calls = 0;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      if (file === directory && ++calls === 2) {
        if (change === "replacement") {
          fs.renameSync(directory, `${directory}.saved`);
          fs.mkdirSync(directory);
          fs.writeFileSync(path.join(directory, "mine.txt"), "new active directory");
        } else {
          const marker = path.join(directory, SCRATCH_OWNER_FILE);
          const owner = JSON.parse(fs.readFileSync(marker, "utf8"));
          delete owner.closedAt;
          fs.writeFileSync(marker, JSON.stringify(owner));
        }
      }
      return original(file);
    }) as typeof fs.lstatSync);
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR })).removed).toEqual([]);
    expect(fs.existsSync(directory)).toBe(true);
  });
  it("expires only marked closed caches; dry-run reports without mutating", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, "output", { closedAt: 1 });
    const young = fixture(tempRoot, "output", { closedAt: 30 * HOUR });
    const active = fixture(tempRoot);
    const unrelated = path.join(tempRoot, "pi-fabric-output-unrelated");
    fs.mkdirSync(unrelated);
    const options = { tempRoot, now: 30 * HOUR + 1 };
    expect((await sweepScratch({ ...options, dryRun: true })).eligible).toEqual([old]);
    expect(fs.existsSync(old)).toBe(true);
    expect((await sweepScratch(options)).removed).toEqual([old]);
    for (const file of [young, active, unrelated]) expect(fs.existsSync(file)).toBe(true);
  });

  it("evicts oldest eligible caches by count/bytes but protects active and recent data", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, "output", { closedAt: 1 });
    const newer = fixture(tempRoot, "shell", { closedAt: HOUR });
    const recent = fixture(tempRoot, "output", { closedAt: 2 * HOUR });
    const active = fixture(tempRoot, "output");
    const checkpoint = fixture(tempRoot, "checkpoint");
    expect((await sweepScratch({ tempRoot, now: 2 * HOUR + 1, maxItems: 3 })).removed).toEqual([old]);
    expect((await sweepScratch({ tempRoot, now: 2 * HOUR + 1, maxBytes: 0 })).removed).toEqual([newer]);
    for (const file of [recent, active, checkpoint]) expect(fs.existsSync(file)).toBe(true);
  });

  it("recovers a dead host checkpoint only after first-observed-dead grace", async () => {
    const tempRoot = sandbox();
    const child = spawnSync(process.execPath, ["-e", "process.stdout.write(String(process.pid))"], { encoding: "utf8" });
    expect(child.status).toBe(0);
    const orphan = fixture(tempRoot, "checkpoint", { pid: Number(child.stdout) });
    const before = fs.readFileSync(path.join(orphan, SCRATCH_OWNER_FILE), "utf8");
    expect((await sweepScratch({ tempRoot, now: 40 * HOUR, dryRun: true })).orphaned).toEqual([orphan]);
    expect(fs.readFileSync(path.join(orphan, SCRATCH_OWNER_FILE), "utf8")).toBe(before);
    expect((await sweepScratch({ tempRoot, now: 40 * HOUR })).removed).toEqual([]);
    expect((await sweepScratch({ tempRoot, now: 45 * HOUR })).removed).toEqual([]);
    expect((await sweepScratch({ tempRoot, now: 46 * HOUR })).removed).toEqual([orphan]);
  });

  it("protects shell children even after their host dies", async () => {
    const tempRoot = sandbox();
    const shell = fixture(tempRoot, "shell", { pid: 2147483647, orphanedAt: 1 });
    fs.writeFileSync(path.join(shell, "child.pid"), String(process.pid));
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR, maxBytes: 0 })).removed).toEqual([]);
  });

  it("does not reclaim malformed metadata, unknown contents, unmarked legacy roots or symlinks", async () => {
    const tempRoot = sandbox();
    const malformed = fixture(tempRoot, "output", { pid: "dead", closedAt: 1 });
    const unknown = fixture(tempRoot, "output", { closedAt: 1 });
    fs.writeFileSync(path.join(unknown, "mine.txt"), "private");
    const legacy = fixture(tempRoot, "output", { closedAt: 1 });
    fs.unlinkSync(path.join(legacy, SCRATCH_OWNER_FILE));
    const target = sandbox();
    fs.writeFileSync(path.join(target, "keep"), "important");
    const linked = fixture(tempRoot, "output", { closedAt: 1 });
    fs.unlinkSync(path.join(linked, "output.txt"));
    fs.symlinkSync(path.join(target, "keep"), path.join(linked, "output.txt"));
    fs.symlinkSync(target, path.join(tempRoot, "pi-fabric-output-abcdef"), "junction");
    const ownerLink = fixture(tempRoot, "output", { closedAt: 1 });
    fs.renameSync(path.join(ownerLink, SCRATCH_OWNER_FILE), path.join(target, "owner"));
    fs.symlinkSync(path.join(target, "owner"), path.join(ownerLink, SCRATCH_OWNER_FILE));
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR, maxBytes: 0 })).eligible).toEqual([]);
    for (const root of [malformed, unknown, legacy, linked, ownerLink, target]) expect(fs.existsSync(root)).toBe(true);
  });

  it.skipIf(!process.getuid)("does not reclaim another uid's marked root", async () => {
    const tempRoot = sandbox();
    const root = fixture(tempRoot, "output", { closedAt: 1 });
    const original = fs.lstatSync;
    vi.spyOn(fs, "lstatSync").mockImplementation(((file: fs.PathLike) => {
      const stat = original(file);
      if (file === root) Object.defineProperty(stat, "uid", { value: process.getuid!() + 1 });
      return stat;
    }) as typeof fs.lstatSync);
    expect((await sweepScratch({ tempRoot, now: 100 * HOUR })).removed).toEqual([]);
  });

  it("cleans failed allocation and preserves live lossless checkpoints under pressure", async () => {
    const tempRoot = sandbox();
    const write = vi.spyOn(fs, "writeFileSync").mockImplementation(() => { throw new Error("disk full"); });
    expect(() => createScratch("output", tempRoot)).toThrow("disk full");
    write.mockRestore();
    expect(fs.readdirSync(tempRoot)).toEqual([]);
    const tmp = vi.spyOn(os, "tmpdir").mockReturnValue(tempRoot);
    const checkpoint = new NativeReaderCheckpoint({ important: "exact state" });
    const dir = fs.readdirSync(tempRoot)[0]!;
    expect((await sweepScratch({ tempRoot, now: Date.now() + 100 * HOUR, maxBytes: 0, maxItems: 0 })).removed).toEqual([]);
    expect(checkpoint.restore()).toEqual({ important: "exact state" });
    checkpoint.dispose();
    checkpoint.dispose();
    expect(fs.existsSync(path.join(tempRoot, dir))).toBe(false);
    tmp.mockRestore();
    const root = createScratch("output", tempRoot);
    closeScratch(root);
    expect(JSON.parse(fs.readFileSync(path.join(root, SCRATCH_OWNER_FILE), "utf8")).closedAt).toEqual(expect.any(Number));
  });
});
