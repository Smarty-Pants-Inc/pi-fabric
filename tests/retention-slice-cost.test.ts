import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { describe, expect, it, vi } from "vitest";
import { canRemoveTerminalRun, pruneActorRunArchives, pruneActorRunArchiveSlices, runTreeExitVeto } from "../src/storage/retention.js";
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
    const nativePlatform = process.platform;
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
      // Windows reuses only this uninterrupted slice's census/status/metadata:
      // 4 lstat + 1 read + 1 readdir + 1 rm = 7, with no negative-name probes.
      // POSIX retains 8 lstat + 3 exists + 1 read + 1 readdir + 1 rm = 14.
      // Main used 20 Windows crossings; the previous lane used 11. No custody
      // evidence survives a yield. Recursive rmSync traversal is excluded.
      const exact = platform === "win32" ? 7 : 14;
      expect(largestCalls).toBe(exact);
      expect((total() - 2) / count).toBe(exact); // Only root lstat/census are outside run slices.
      expect(calls).toEqual({ lstatSync: (platform === "win32" ? 4 : 8) * count + 1,
        readdirSync: count + 1, ...(platform === "win32" ? {} : { existsSync: 3 * count }),
        readFileSync: count, rmSync: count });
      expect(scratchCalls).toBe(platform === "win32" ? 0 : 3 * count);
      // A forced win32 branch on Linux proves work counts, not native NTFS
      // latency: Ubuntu CI already had exactly 20 crossings but a 290 ms
      // scheduler/IO outlier. Keep the unchanged native Windows and POSIX bound.
      if (platform !== "win32" || nativePlatform === "win32") expect(longest).toBeLessThan(250);
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
            expect(runTreeExitVeto(run, 0, undefined, true)).toBeUndefined(); // scratch sweep is a no-op
          }
          const slices = pruneActorRunArchiveSlices({ runsDirectory: root, retentionMs: 0 });
          while (!slices.next().done) { /* Every candidate must remain. */ }
          expect(fs.existsSync(artifact)).toBe(true);
          expect(fs.existsSync(run)).toBe(true);
        }
      } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
    },
  );

  it.each(["status", "scratch", "latest", "archive-pending.json", "actor-run-archive-pending.json", "unresolved-worker.json", "nested"] as const)("does not reuse Windows collection evidence across a %s change between slices", fence => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-fresh-slice-"));
    const run = path.join(root, "run"); terminal(run);
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    let latest = false;
    try {
      const slices = pruneActorRunArchiveSlices({ runsDirectory: root, retentionMs: 0, retainRun: () => latest });
      expect(slices.next().done).toBe(false); // Enumeration has yielded; no run proof is cached.
      if (fence === "status") fs.writeFileSync(path.join(run, "status.json"), JSON.stringify({
        status: "running", transport: "process", sessionId: "2147483646", finishedAt: 1,
      }));
      if (fence === "scratch") fs.mkdirSync(path.join(run, RUN_TMP_DIRECTORY));
      if (fence === "latest") latest = true;
      if (fence.endsWith(".json")) fs.writeFileSync(path.join(run, fence), "{}");
      if (fence === "nested") {
        const child = path.join(run, "nested", "child"); terminal(child);
        fs.mkdirSync(path.join(child, RUN_TMP_DIRECTORY));
      }
      const step = slices.next();
      expect(step.done).toBe(true); expect(step.value).toEqual([]);
      expect(fs.existsSync(run)).toBe(true);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

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
      expect(pruneActorRunArchives({ runsDirectory: root, retentionMs: 0 })).toEqual([]);
      metadata.mockRestore();
      vi.spyOn(fs, "readdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
        if (String(target) === run) throw Object.assign(new Error("access denied"), { code: "EACCES" });
        return Reflect.apply(readdir, fs, [target, ...args]);
      }) as never);
      expect(canRemoveTerminalRun(run)).toBe(false);
      expect(pruneActorRunArchives({ runsDirectory: root, retentionMs: 0 })).toEqual([]);
      expect(fs.existsSync(run)).toBe(true);
    } finally { vi.restoreAllMocks(); fs.rmSync(root, { recursive: true, force: true }); }
  });

  it("stops a slow Windows metadata walk at its deadline and retries with fresh evidence", () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-retention-deadline-"));
    const run = path.join(root, "run"); terminal(run);
    for (let i = 0; i < 100; i++) fs.writeFileSync(path.join(run, `oversized-event-prefix-${i}.txt`), "event");
    vi.spyOn(process, "platform", "get").mockReturnValue("win32");
    const lstat = fs.lstatSync;
    let clock = 0, stats = 0;
    try {
      // Virtual metadata latency avoids scheduler-sensitive work assertions.
      vi.spyOn(performance, "now").mockImplementation(() => clock);
      const metadata = vi.spyOn(fs, "lstatSync").mockImplementation(((...args: unknown[]) => {
        stats++; clock += 10;
        return Reflect.apply(lstat, fs, args);
      }) as never);
      const drain = () => {
        const slices = pruneActorRunArchiveSlices({ runsDirectory: root, retentionMs: 0 });
        for (;;) { const step = slices.next(); if (step.done) return step.value; }
      };
      const remove = vi.spyOn(fs, "rmSync");
      expect(drain()).toEqual([]);
      expect(stats).toBeLessThanOrEqual(4); // Root census + at most three candidate stats.
      expect(remove).not.toHaveBeenCalled();
      expect(fs.existsSync(run)).toBe(true);
      metadata.mockRestore();
      fs.mkdirSync(path.join(run, RUN_TMP_DIRECTORY));
      expect(drain()).toEqual([]); // No partial proof retained after the deadline.
      expect(remove).not.toHaveBeenCalled();
      fs.rmdirSync(path.join(run, RUN_TMP_DIRECTORY));
      expect(drain()).toEqual([run]); // A later fresh sweep can finish.
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
