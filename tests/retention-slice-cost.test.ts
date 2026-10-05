import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { describe, expect, it, vi } from "vitest";
import { canRemoveTerminalRun, pruneActorRunArchiveSlices, runTreeExitVeto } from "../src/storage/retention.js";
import { JOINED_SCRATCH_FILE, NEVER_STARTED_FILE, RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE } from "../src/storage/run-scratch.js";

// Count synchronous fs API crossings rather than wall-clock thresholds: NTFS
// metadata costs dominate this path. readFileSync/rmSync can issue multiple
// native syscalls internally, so these counts are not a native Windows trace.
describe("actor archive slice fs cost", () => {
  it.each([
    ["win32", false], ["linux", false], ["win32", true],
  ] as const)("bounds per-run custody work (%s, JS removal traversal=%s)", async (platform, jsRemovalTraversal) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-cost-"));
    const count = 2_000;
    const old = Date.now() - 10 * 24 * 60 * 60_000;
    for (let i = 0; i < count; i++) {
      const run = path.join(root, `run-${i}`);
      fs.mkdirSync(run);
      fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({
        status: "completed", transport: "process", sessionId: "2147483646", finishedAt: old,
      }));
      fs.writeFileSync(path.join(run, "events.jsonl"), '{"type":"complete"}\n');
      fs.writeFileSync(path.join(run, "task.txt"), "fixture task");
    }
    const calls: Record<string, number> = {};
    let scratchCalls = 0, removalDepth = 0;
    const keys = ["lstatSync", "statSync", "existsSync", "readdirSync", "readFileSync", "rmSync"] as const;
    const platformMock = vi.spyOn(process, "platform", "get").mockReturnValue(platform);
    for (const key of keys) {
      const original = fs[key];
      // Overloaded fs signatures cannot be expressed as one implementation.
      vi.spyOn(fs, key).mockImplementation(((...args: unknown[]) => {
        // Count the policy's public crossings, not rmSync's implementation.
        // Older Node versions expose recursive removal's lstat/readdir calls
        // through fs when win32 is mocked on POSIX; newer versions use native IO.
        if (removalDepth === 0) {
          calls[key] = (calls[key] ?? 0) + 1;
          if (["tmp", "unresolved-scratch.json"].includes(path.basename(String(args[0])))) scratchCalls++;
        }
        if (key === "rmSync") removalDepth++;
        try {
          if (key === "rmSync" && jsRemovalTraversal) {
            // Deterministically exercise the five nested crossings seen in CI,
            // even on a runtime whose actual recursive deletion is native.
            fs.lstatSync(args[0] as fs.PathLike);
            for (const name of fs.readdirSync(args[0] as fs.PathLike)) {
              fs.lstatSync(path.join(String(args[0]), name));
            }
          }
          return Reflect.apply(original, fs, args);
        } finally { if (key === "rmSync") removalDepth--; }
      }) as never);
    }
    let longest = 0, largestCalls = 0, largestDeletes = 0;
    const total = () => Object.values(calls).reduce((sum, n) => sum + n, 0);
    let removed: string[] = [];
    try {
      process.stdout.write(`retention-trace-start ${platform}\n`);
      const slices = pruneActorRunArchiveSlices({ runsDirectory: root, retentionMs: 24 * 60 * 60_000 });
      for (;;) {
        const before = total(), deletes = calls.rmSync ?? 0, start = performance.now();
        const step = slices.next();
        longest = Math.max(longest, performance.now() - start);
        largestCalls = Math.max(largestCalls, total() - before);
        largestDeletes = Math.max(largestDeletes, (calls.rmSync ?? 0) - deletes);
        if (step.done) { removed = step.value; break; }
        await new Promise<void>(resolve => setImmediate(resolve));
      }
      process.stdout.write(`retention-trace-end ${platform}\n`);
      process.stdout.write(JSON.stringify({ platform, jsRemovalTraversal, runs: count, calls, perRun: (total() - 2) / count,
        scratchCallsPerRun: scratchCalls / count, largestCalls, largestDeletes, longestSliceMs: longest }) + "\n");
      expect(removed).toHaveLength(count);
      expect(largestDeletes).toBe(1);
      expect(longest).toBeLessThan(250);
      // Flat three-file fixture: root lstat (1); archive/actor-archive/worker
      // marker exists checks (3); status lstat/read (2); nested absence lstat
      // (1); root readdir (1); task/events lstat (2); rm (1) = 11. The checked
      // status/root snapshot is reused by allowlist/TTL, never across a run.
      // POSIX adds fence lstat for disposal plus fence/tmp veto lstat (3) = 14.
      const exact = platform === "win32" ? 11 : 14;
      expect(largestCalls).toBe(exact);
      expect((total() - 2) / count).toBe(exact); // Only root lstat/census are outside run slices.
      expect(calls).toEqual({ lstatSync: (platform === "win32" ? 5 : 8) * count + 1,
        readdirSync: count + 1, existsSync: 3 * count, readFileSync: count, rmSync: count });
      expect(scratchCalls).toBe(platform === "win32" ? 0 : 3 * count);
    } finally {
      platformMock.mockRestore(); vi.restoreAllMocks();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  const terminal = (directory: string) => {
    fs.mkdirSync(directory, { recursive: true });
    fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify({
      status: "completed", transport: "process", sessionId: "2147483646", finishedAt: 1,
    }));
  };

  it.each([RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE, JOINED_SCRATCH_FILE, NEVER_STARTED_FILE])(
    "Windows collection still vetoes root and descendant scratch artifacts (%s)", (name) => {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-custody-"));
      vi.spyOn(process, "platform", "get").mockReturnValue("win32");
      try {
        for (const nested of [false, true]) {
          const run = path.join(root, String(nested)); terminal(run);
          const holder = nested ? path.join(run, "nested", "child") : run;
          if (nested) terminal(holder);
          const artifact = path.join(holder, name);
          if (name === RUN_TMP_DIRECTORY) fs.mkdirSync(artifact);
          else fs.writeFileSync(artifact, "{}");
          expect(canRemoveTerminalRun(run)).toBe(false);
          if (name === RUN_TMP_DIRECTORY || name === UNRESOLVED_SCRATCH_FILE) {
            expect(runTreeExitVeto(run, 0, undefined, true)).toMatch(/scratch writer exit is unconfirmed/);
          }
          const slices = pruneActorRunArchiveSlices({ runsDirectory: root, retentionMs: 0 });
          while (!slices.next().done) { /* Every candidate must remain. */ }
          expect(fs.existsSync(artifact)).toBe(true);
          expect(fs.existsSync(run)).toBe(true);
        }
      } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
    },
  );

  it("Windows collection still fails closed for unreadable directory and file metadata", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-unreadable-"));
    const run = path.join(root, "run"); terminal(run);
    const file = path.join(run, "task.txt"); fs.writeFileSync(file, "task");
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const lstat = fs.lstatSync, readdir = fs.readdirSync;
    try {
      const metadata = vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
        if (String(target) === file) throw Object.assign(new Error("access denied"), { code: "EACCES" });
        return Reflect.apply(lstat, fs, [target, ...args]);
      }) as never);
      expect(canRemoveTerminalRun(run)).toBe(false);
      metadata.mockRestore();
      vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
        if (String(target) === run) throw Object.assign(new Error("access denied"), { code: "EACCES" });
        return Reflect.apply(readdir, fs, [target, ...args]);
      }) as never);
      expect(canRemoveTerminalRun(run)).toBe(false);
      expect(fs.existsSync(run)).toBe(true);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("does not put slow negative Windows scratch stats on the archive slice", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-slow-stat-"));
    const run = path.join(root, "run"); terminal(run);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const lstat = fs.lstatSync;
    let scratchProbes = 0, longest = 0;
    try {
      vi.spyOn(fs, "lstatSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
        if ([RUN_TMP_DIRECTORY, UNRESOLVED_SCRATCH_FILE].includes(path.basename(String(target)))) {
          scratchProbes++;
          const until = performance.now() + 150;
          while (performance.now() < until) { /* Controlled missing-name NTFS cost. */ }
        }
        return Reflect.apply(lstat, fs, [target, ...args]);
      }) as never);
      const slices = pruneActorRunArchiveSlices({ runsDirectory: root, retentionMs: 0 });
      for (;;) {
        const start = performance.now(), step = slices.next();
        longest = Math.max(longest, performance.now() - start);
        if (step.done) { expect(step.value).toEqual([run]); break; }
      }
      process.stdout.write(JSON.stringify({ probe: "slow-negative-Windows-stats", scratchProbes, longestSliceMs: longest }) + "\n");
      expect(scratchProbes).toBe(0);
      expect(longest).toBeLessThan(250);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });
});
