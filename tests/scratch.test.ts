import fs from "node:fs";
import { hostedOnly } from "../scripts/hosted-only.mjs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { closeScratch, createScratch, ScratchScope, SCRATCH_OWNER_FILE, sweepScratch } from "../src/storage/scratch.js";
import { createHash } from "node:crypto";
import { fabricDataRoot } from "../src/storage/temp-root.js";
import { NativeReaderCheckpoint } from "../src/ui/conversation-native-reader-checkpoint.js";

const HOUR = 3_600_000;
// pi-fabric#1014: on Linux the verified helper removes (only with getuid proof); off Linux scratch keeps main's path
// removal after its rechecks (tests/scratch-platform-retention.test.ts). Planning and dry runs are unchanged everywhere.
// The legacy block below stays hosted-only. Archive regressions additionally run against private TMPDIR fixtures.
const verifiedDeletes = process.platform !== "linux" || typeof process.getuid === "function";
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
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Data-safety regression: only these private TMPDIR fixtures are deleted; no shared scratch is swept.
const scratchArchive = (tempRoot: string): string => path.join(tempRoot, ".fabric-scratch-archive");
const archivedFiles = (directory: string): Buffer[] => {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = path.join(directory, entry.name);
    return entry.isDirectory() ? archivedFiles(file) : [fs.readFileSync(file)];
  });
};
const sha256 = (bytes: Buffer): string => createHash("sha256").update(bytes).digest("hex");

describe("scratch archival before reclamation (owned temporary fixtures only)", () => {
  it.skipIf(!verifiedDeletes).each(["output", "shell", "checkpoint"] as const)("expiry preserves the complete %s bytes before removing the original", async kind => {
    const tempRoot = sandbox();
    const directory = fixture(tempRoot, kind, { closedAt: 1 });
    const name = kind === "output" ? "output.txt" : kind === "shell" ? "output.log" : "checkpoint";
    const payload = Buffer.concat([Buffer.from("full output, not a preview\n"), Buffer.alloc(192 * 1024, 0xa5), Buffer.from([0, 255, 10])]);
    fs.writeFileSync(path.join(directory, name), payload);
    const marker = fs.readFileSync(path.join(directory, SCRATCH_OWNER_FILE));
    const beforeRemoval: string[] = [];
    const capture = () => {
      const archive = archivedFiles(scratchArchive(tempRoot));
      beforeRemoval.push(...archive.map(sha256));
    };
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (path.basename(String(from)) === path.basename(directory)) capture();
      rename(from, to);
    });
    const remove = fs.rmSync;
    vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (String(file) === directory) capture();
      remove(file, options);
    });

    expect((await sweepScratch({ tempRoot, now: 100 * HOUR })).removed).toEqual([directory]);
    expect(fs.existsSync(directory)).toBe(false);
    const copies = archivedFiles(scratchArchive(tempRoot));
    expect(copies.some(bytes => bytes.equals(payload))).toBe(true);
    expect(copies.some(bytes => bytes.equals(marker))).toBe(true);
    expect(beforeRemoval).toContain(sha256(payload));
    const receipts = fs.readFileSync(path.join(scratchArchive(tempRoot), "MANIFEST.jsonl"), "utf8")
      .trim().split("\n").map(line => JSON.parse(line));
    expect(receipts).toContainEqual(expect.objectContaining({ kind: "tree", from: directory,
      files: expect.arrayContaining([{ path: name, sha256: sha256(payload), bytes: payload.length }]) }));
    const before = copies.map(sha256).sort();
    expect((await sweepScratch({ tempRoot, now: 200 * HOUR, maxBytes: 0, maxItems: 0 })).removed).toEqual([]);
    expect(archivedFiles(scratchArchive(tempRoot)).map(sha256).sort()).toEqual(before);
  });

  it.each(["write failure", "readback failure", "readback corruption"] as const)("%s keeps every original byte and inode", async failure => {
    const tempRoot = sandbox();
    const directory = fixture(tempRoot, "shell", { closedAt: 1 });
    fs.writeFileSync(path.join(directory, "output.log"), "full untruncated shell result\u0000\n");
    const before = fs.readdirSync(directory).sort().map(name => ({
      name, bytes: fs.readFileSync(path.join(directory, name)), stat: fs.lstatSync(path.join(directory, name)),
    }));
    const archiveFds = new Map<number, boolean>();
    const open = fs.openSync, close = fs.closeSync, read = fs.readSync, write = fs.writeSync;
    vi.spyOn(fs, "openSync").mockImplementation(((file, flags, mode) => {
      const fd = open(file, flags, mode);
      if (typeof file === "string" && file.startsWith(scratchArchive(tempRoot) + path.sep))
        archiveFds.set(fd, typeof flags === "number" && (flags & (fs.constants.O_WRONLY | fs.constants.O_RDWR)) === 0);
      return fd;
    }) as typeof fs.openSync);
    vi.spyOn(fs, "closeSync").mockImplementation(fd => { archiveFds.delete(fd); close(fd); });
    let injected = 0;
    vi.spyOn(fs, "writeSync").mockImplementation(((...args: Parameters<typeof fs.writeSync>) => {
      if (failure === "write failure" && archiveFds.has(args[0])) { injected++; throw new Error("archive disk full"); }
      return Reflect.apply(write, fs, args);
    }) as typeof fs.writeSync);
    vi.spyOn(fs, "readSync").mockImplementation(((...args: Parameters<typeof fs.readSync>) => {
      if (archiveFds.get(args[0]) === true) {
        if (failure === "readback failure") { injected++; throw new Error("archive readback failed"); }
        const count = Reflect.apply(read, fs, args) as number;
        if (failure === "readback corruption" && count > 0) {
          injected++;
          const buffer = args[1] as Buffer;
          buffer[0] = buffer[0]! ^ 0xff;
        }
        return count;
      }
      return Reflect.apply(read, fs, args);
    }) as typeof fs.readSync);
    expect(await sweepScratch({ tempRoot, now: 100 * HOUR })).toMatchObject({ eligible: [directory], removed: [] });
    expect(injected).toBeGreaterThan(0);
    expect(fs.readdirSync(directory).sort()).toEqual(before.map(entry => entry.name));
    for (const entry of before) {
      const file = path.join(directory, entry.name);
      expect(fs.readFileSync(file)).toEqual(entry.bytes);
      expect(fs.lstatSync(file)).toMatchObject({ dev: entry.stat.dev, ino: entry.stat.ino, mtimeMs: entry.stat.mtimeMs });
    }
  });

  it.skipIf(!verifiedDeletes)("pressure still reclaims ordinary closed caches while retaining their full output.log", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, "shell", { closedAt: 1 });
    const payload = Buffer.from("ordinary nontrace shell output\ncomplete trailing bytes\n");
    fs.writeFileSync(path.join(old, "output.log"), payload);
    const recent = fixture(tempRoot, "output", { closedAt: 2 * HOUR });
    const active = fixture(tempRoot);
    const checkpoint = fixture(tempRoot, "checkpoint");
    expect((await sweepScratch({ tempRoot, now: 2.5 * HOUR, maxBytes: 0, maxItems: 0 })).removed).toEqual([old]);
    expect(fs.existsSync(old)).toBe(false);
    expect(archivedFiles(scratchArchive(tempRoot)).some(bytes => bytes.equals(payload))).toBe(true);
    for (const kept of [recent, active, checkpoint]) expect(fs.existsSync(kept)).toBe(true);
  });

  it.skipIf(!verifiedDeletes)("scope release archives only its own closed allocations, including checkpoint and pending bytes", async () => {
    const tempRoot = sandbox();
    const own = new ScratchScope(), other = new ScratchScope();
    const checkpoint = own.create("checkpoint", tempRoot);
    const output = own.create("output", tempRoot);
    const otherOutput = other.create("output", tempRoot);
    const active = own.create("output", tempRoot);
    fs.writeFileSync(path.join(checkpoint, "checkpoint"), "complete checkpoint\u0000\u00ff");
    fs.writeFileSync(path.join(checkpoint, "pending"), "pending lossless state\n");
    fs.writeFileSync(path.join(output, "output.txt"), "own complete result\n");
    fs.writeFileSync(path.join(otherOutput, "output.txt"), "other session's result\n");
    fs.writeFileSync(path.join(active, "output.txt"), "still running\n");
    for (const directory of [checkpoint, output, otherOutput]) closeScratch(directory);
    await own.close();
    await own.close();
    for (const removed of [checkpoint, output]) expect(fs.existsSync(removed)).toBe(false);
    for (const kept of [otherOutput, active]) expect(fs.existsSync(kept)).toBe(true);
    const copies = archivedFiles(scratchArchive(tempRoot));
    for (const value of ["complete checkpoint\u0000\u00ff", "pending lossless state\n", "own complete result\n"])
      expect(copies.some(bytes => bytes.equals(Buffer.from(value)))).toBe(true);
    expect(copies.some(bytes => bytes.equals(Buffer.from("other session's result\n")))).toBe(false);
  });

  it.skipIf(!verifiedDeletes)("dead-owner checkpoints keep their first-observed grace, then archive before reclaiming", async () => {
    const tempRoot = sandbox();
    const directory = fixture(tempRoot, "checkpoint", { pid: 2147483647 });
    const marker = fs.readFileSync(path.join(directory, SCRATCH_OWNER_FILE));
    expect((await sweepScratch({ tempRoot, now: 40 * HOUR, dryRun: true })).orphaned).toEqual([directory]);
    expect(fs.readFileSync(path.join(directory, SCRATCH_OWNER_FILE))).toEqual(marker);
    expect((await sweepScratch({ tempRoot, now: 40 * HOUR })).removed).toEqual([]);
    expect((await sweepScratch({ tempRoot, now: 45 * HOUR })).removed).toEqual([]);
    expect(fs.existsSync(scratchArchive(tempRoot))).toBe(false);
    expect((await sweepScratch({ tempRoot, now: 46 * HOUR })).removed).toEqual([directory]);
    expect(archivedFiles(scratchArchive(tempRoot)).some(bytes => bytes.equals(Buffer.from("data")))).toBe(true);
  });

  it.each(["source subtree", "other directory"] as const)("refuses a symlink archive pointing at the %s, preserving both trees", async target => {
    const tempRoot = sandbox();
    const directory = fixture(tempRoot, "output", { closedAt: 1 });
    const other = sandbox();
    fs.writeFileSync(path.join(other, "keep"), "unrelated original bytes");
    const before = fs.readdirSync(directory).sort().map(name => [name, fs.readFileSync(path.join(directory, name))]);
    fs.symlinkSync(target === "source subtree" ? directory : other, scratchArchive(tempRoot), "junction");
    expect(await sweepScratch({ tempRoot, now: 100 * HOUR })).toMatchObject({ eligible: [directory], removed: [] });
    expect(fs.readdirSync(directory).sort().map(name => [name, fs.readFileSync(path.join(directory, name))])).toEqual(before);
    expect(fs.readdirSync(other)).toEqual(["keep"]);
    expect(fs.readFileSync(path.join(other, "keep"), "utf8")).toBe("unrelated original bytes");
  });

  it("keeps live, recent, malformed, linked and unknown JSONL/NDJSON trees untouched", async () => {
    const tempRoot = sandbox();
    const live = fixture(tempRoot);
    const recent = fixture(tempRoot, "output", { closedAt: 100 * HOUR });
    const malformed = fixture(tempRoot, "output", { pid: "dead", closedAt: 1 });
    const child = fixture(tempRoot, "shell", { pid: 2147483647, orphanedAt: 1 });
    fs.writeFileSync(path.join(child, "child.pid"), String(process.pid));
    const unknown = ["events.jsonl", "custom.ndjson", "mine.txt"].map(name => {
      const directory = fixture(tempRoot, "output", { closedAt: 1 });
      fs.writeFileSync(path.join(directory, name), "unknown original bytes\n");
      return directory;
    });
    const linked = fixture(tempRoot, "output", { closedAt: 1 });
    fs.renameSync(path.join(linked, "output.txt"), path.join(tempRoot, "target"));
    fs.symlinkSync(path.join(tempRoot, "target"), path.join(linked, "output.txt"));
    const preserved = [live, recent, malformed, child, ...unknown, linked];
    const before = preserved.map(directory => fs.readdirSync(directory).sort().map(name => fs.readFileSync(path.join(directory, name))));
    expect(await sweepScratch({ tempRoot, now: 100 * HOUR + 1, maxBytes: 0, maxItems: 0 }))
      .toMatchObject({ eligible: [], removed: [] });
    expect(preserved.map(directory => fs.readdirSync(directory).sort().map(name => fs.readFileSync(path.join(directory, name))))).toEqual(before);
    expect(fs.existsSync(scratchArchive(tempRoot))).toBe(false);
  });
});

describe.runIf(hostedOnly("managed scratch retention"))("managed scratch retention", () => {
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
    expect((await sweepScratch(options)).removed).toEqual(verifiedDeletes ? [old] : []);
    expect(fs.existsSync(old)).toBe(!verifiedDeletes);
    for (const file of [young, active, unrelated]) expect(fs.existsSync(file)).toBe(true);
  });

  it("evicts oldest eligible caches by count/bytes but protects active and recent data", async () => {
    const tempRoot = sandbox();
    const old = fixture(tempRoot, "output", { closedAt: 1 });
    const newer = fixture(tempRoot, "shell", { closedAt: HOUR });
    const recent = fixture(tempRoot, "output", { closedAt: 2 * HOUR });
    const active = fixture(tempRoot, "output");
    const checkpoint = fixture(tempRoot, "checkpoint");
    expect((await sweepScratch({ tempRoot, now: 2 * HOUR + 1, maxItems: 3 })).removed).toEqual(verifiedDeletes ? [old] : []);
    expect((await sweepScratch({ tempRoot, now: 2 * HOUR + 1, maxBytes: 0 })).removed).toEqual(verifiedDeletes ? [newer] : []);
    if (!verifiedDeletes) for (const file of [old, newer]) expect(fs.existsSync(file)).toBe(true);
    for (const file of [recent, active, checkpoint]) expect(fs.existsSync(file)).toBe(true);
  });

  it.runIf(process.platform === "linux")("shared-root pressure evicts session B's old closed output cache but preserves live caches and unknown files", async () => {
    const base = sandbox(), privateA = path.join(base, "session-a"), privateB = path.join(base, "session-b"), shared = path.join(base, "file-data");
    for (const root of [privateA, privateB, shared]) fs.mkdirSync(root, { mode: 0o700 });
    const tmp = vi.spyOn(os, "tmpdir").mockReturnValue(privateB);
    vi.stubEnv("TMPDIR", privateB); vi.stubEnv("PI_FABRIC_TMPDIR", shared);
    // B's cache is older than the one-hour pressure fence, but younger than normal 24-hour expiry.
    const closedB = fixture(fabricDataRoot(), "output", { pid: 2147483647, closedAt: HOUR });
    const liveB = fixture(fabricDataRoot(), "output");
    const unknownB = fixture(fabricDataRoot(), "output", { pid: 2147483647, closedAt: 1 });
    fs.writeFileSync(path.join(unknownB, "mine.txt"), "not Fabric's data");
    tmp.mockReturnValue(privateA); vi.stubEnv("TMPDIR", privateA);
    const activeA = fixture(fabricDataRoot(), "output");
    const protectedRoots = [liveB, activeA, unknownB];
    const preserved = () => protectedRoots.map(root => fs.readdirSync(root).sort().map(name => [name, fs.readFileSync(path.join(root, name), "utf8")]));
    const before = preserved();
    const options = { tempRoot: fabricDataRoot(), now: 3 * HOUR };
    expect(options.tempRoot).toBe(shared);
    expect((await sweepScratch({ ...options, dryRun: true })).eligible).toEqual([]);
    expect((await sweepScratch({ ...options, maxItems: 0, dryRun: true })).eligible).toEqual([closedB]);
    expect(fs.existsSync(closedB)).toBe(true);
    expect((await sweepScratch({ ...options, maxItems: 0 })).removed).toEqual(verifiedDeletes ? [closedB] : []);
    expect(fs.existsSync(closedB)).toBe(!verifiedDeletes);
    expect(preserved()).toEqual(before);
    expect(fs.readdirSync(privateA)).toEqual([]); expect(fs.readdirSync(privateB)).toEqual([]);
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
    expect((await sweepScratch({ tempRoot, now: 46 * HOUR })).removed).toEqual(verifiedDeletes ? [orphan] : []);
    expect(fs.existsSync(orphan)).toBe(!verifiedDeletes);
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
