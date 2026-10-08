import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { MeshLockTimeoutError } from "../src/mesh.js";

// smarty-dev#6477: idle hosts burned CPU because background refreshes retried a
// FABRIC_MESH_LOCK_TIMEOUT 12-194 ms later, adding to the contention they waited on.
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

const seeded = (seed: number) => () => {
  seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648;
  return seed / 2_147_483_648;
};

describe("background mesh retry under a lock that always times out", () => {
  it.each([
    ["participant heartbeat/change refresh", 100, 10],
    ["actor presence a1", 50, 20],
  ])("%s: backs off from the base interval, caps the gap and never overlaps attempts", async (label, baseMs, tickMs) => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(Math, "random").mockImplementation(seeded(6477));
    const lockWaitMs = 30;                                  // each acquisition waits, then times out
    const retry = new MeshBackgroundRetry(label, baseMs);
    const starts: number[] = [];
    const ends: number[] = [];
    let inFlight = 0;
    let maxInFlight = 0;
    const acquire = async () => {
      starts.push(Date.now());
      maxInFlight = Math.max(maxInFlight, ++inFlight);
      try {
        await new Promise(resolve => setTimeout(resolve, lockWaitMs));
        throw new MeshLockTimeoutError(" held by pid 1 (alive, running)", 6, lockWaitMs);
      } finally { inFlight--; ends.push(Date.now()); }
    };
    // The caller polls far faster than any backoff, as change refreshes and polls do.
    const poller = setInterval(() => { void retry.run(acquire); }, tickMs);
    await vi.advanceTimersByTimeAsync(60_000);
    clearInterval(poller);
    await vi.advanceTimersByTimeAsync(lockWaitMs);

    expect(maxInFlight).toBe(1);
    const gaps = starts.slice(1).map((start, index) => start - ends[index]!);
    // Never sooner than the path's normal interval after a timeout, never past the cap (+ one poll tick).
    expect(Math.min(...gaps)).toBeGreaterThanOrEqual(baseMs);
    expect(Math.max(...gaps)).toBeLessThanOrEqual(7_000 + tickMs);
    // Gaps grow: the second half of a continuous outage is idle, not a sub-second loop.
    const late = gaps.slice(Math.floor(gaps.length / 2));
    expect(late.reduce((sum, gap) => sum + gap, 0) / late.length).toBeGreaterThan(3_000);
    expect(starts.length).toBeLessThan(25);
  });

  it("resets to the base interval after a success", async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(Math, "random").mockReturnValue(0);
    const retry = new MeshBackgroundRetry("poll", 100);
    const busy = () => { throw new MeshLockTimeoutError("", 1, 1); };
    for (let index = 0; index < 8; index++) {
      expect(await retry.run(busy)).toBe("retry");
      expect(retry.waitMs).toBe(100);                       // a zero draw still waits the base
      await vi.advanceTimersByTimeAsync(retry.waitMs);
    }
    vi.spyOn(Math, "random").mockReturnValue(0.999);
    expect(await retry.run(busy)).toBe("retry");
    expect(retry.waitMs).toBe(6_993);                         // capped: 100 + 0.999 * 6_900
    await vi.advanceTimersByTimeAsync(retry.waitMs);
    expect(await retry.run(() => undefined)).toBe("done");
    expect(await retry.run(busy)).toBe("retry");
    expect(retry.waitMs).toBe(199);                           // first window again: [100, 200)
  });
});
