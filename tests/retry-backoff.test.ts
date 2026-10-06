import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { retryDelayMs } from "../src/core/retry-backoff.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshStore } from "../src/mesh/store.js";

const roots: string[] = [];
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
const seededRandom = () => { let seed = 4383; return () => { seed = (1664525 * seed + 1013904223) >>> 0; return seed / 2 ** 32; }; };

describe("capped exponential full jitter (#4383)", () => {
  it("draws across the full exponential range, caps, and clamps to the remaining deadline", () => {
    for (const [attempt, ceiling] of [[0, 20], [1, 40], [2, 80], [3, 160], [4, 250], [100, 250]]) {
      expect(retryDelayMs(attempt!, 20, 250, Infinity, () => 0)).toBe(0);
      expect(retryDelayMs(attempt!, 20, 250, Infinity, () => 0.5)).toBe(ceiling! / 2);
      expect(retryDelayMs(attempt!, 20, 250, Infinity, () => 0.999999)).toBe(ceiling! - 1);
      expect(retryDelayMs(attempt!, 20, 250, 3, () => 0.999999)).toBe(3);
      expect(retryDelayMs(attempt!, 20, 250, -1)).toBe(0);
    }
  });

  it.each(["FIFO", "plain fallback"] as const)("bounds 128 mesh contenders by their original deadlines with %s admission", async admission => {
    // Keep ticket ordering on the real monotonic clock, while acquisition deadlines are fake.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] }); vi.setSystemTime(100_000);
    vi.spyOn(Math, "random").mockImplementation(seededRandom());
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-jitter-burst-")); roots.push(root);
    const queue = meshLockQueueDirectory(root);
    const lock = path.join(root, ".lock"); fs.mkdirSync(lock);
    const owner = `held\n${process.pid}\n${Date.now()}\n`;
    fs.writeFileSync(path.join(lock, "owner"), owner);
    const timestamps: number[] = [];
    let admissionAttempts = 0;
    const mkdir = fs.mkdirSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options: fs.MakeDirectoryOptions) => {
      if (String(file) === queue) {
        admissionAttempts++;
        // A portable read-only-sidecar failure exercises the real plain-contest fallback
        // from the start, without changing or randomizing FIFO polling in production.
        if (admission === "plain fallback") throw Object.assign(new Error("advisory queue unavailable"), { code: "EACCES" });
      }
      if (String(file) === lock) timestamps.push(Date.now());
      return mkdir(file, options);
    }) as typeof fs.mkdirSync);
    const jobs = Array.from({ length: 128 }, (_, i) => new MeshStore(root, 64 * 1024, 100, { lockProtocol: 1, lockTimeoutMs: 400 })
      .publish({ topic: "jitter.test", from: { id: `sender:${i}`, name: "test", kind: "main" }, text: "blocked" })
      .then(() => { throw new Error("holder must not be reclaimed"); }, error => ({ error, at: Date.now() })));
    expect(admissionAttempts).toBe(128);
    if (admission === "FIFO") {
      expect(fs.readdirSync(queue)).toHaveLength(128);
      await vi.advanceTimersByTimeAsync(319);
      // Only the head contests .lock on its fixed 10ms cadence; non-heads wait.
      expect(timestamps).toEqual(Array.from({ length: 32 }, (_, i) => 100_000 + i * 10));
      expect(fs.readdirSync(queue)).toHaveLength(128);
      await vi.advanceTimersByTimeAsync(1);
      // The 80%-budget transition releases every receipt and enters plain contention.
      expect(fs.readdirSync(queue)).toEqual([]);
      expect(timestamps.filter(at => at === 100_320).length).toBeGreaterThanOrEqual(128);
    } else {
      expect(fs.existsSync(queue)).toBe(false);
      await vi.advanceTimersByTimeAsync(320);
    }
    await vi.advanceTimersByTimeAsync(80);
    const results = await Promise.all(jobs);
    expect(results.every(({ error, at }) => error.code === "FABRIC_MESH_LOCK_TIMEOUT" && at === 100_400)).toBe(true);
    if (admission === "plain fallback") {
      const retryTimes = timestamps.filter(at => at > 100_000 && at < 100_400).map(at => at - 100_000);
      const mean = retryTimes.reduce((a, b) => a + b, 0) / retryTimes.length;
      const variance = retryTimes.reduce((a, b) => a + (b - mean) ** 2, 0) / retryTimes.length;
      // Full jitter owns this entire window; retain the original spread requirements.
      expect(new Set(retryTimes).size).toBeGreaterThan(200);
      expect(variance).toBeGreaterThan(5_000);
      console.log(`128 plain contenders: ${retryTimes.length} retries, ${new Set(retryTimes).size} timestamps, variance ${variance.toFixed(2)} ms²; all 400ms deadlines held`);
      expect(fs.existsSync(queue)).toBe(false);
    } else {
      expect(fs.readdirSync(queue)).toEqual([]);
    }
    expect(fs.readFileSync(path.join(lock, "owner"), "utf8")).toBe(owner);
    expect(vi.getTimerCount()).toBe(0);
  });
});
