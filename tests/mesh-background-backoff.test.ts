import { afterEach, describe, expect, it, vi } from "vitest";
import { MESH_BACKGROUND_RETRY_BASE_MS, MESH_BACKGROUND_RETRY_CAP_MS, MeshBackgroundRetry, MeshLockTimeoutError } from "../src/core/atomic-write.js";

// smarty-dev#7176/#6477: the nominal cap is not a remaining-lease guarantee.
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
const busy = () => new MeshLockTimeoutError(" held by pid 1 (alive, running)", 6, 30);
const seeded = (seed: number) => () => {
  seed = (Math.imul(seed, 1_664_525) + 1_013_904_223) >>> 0;
  return seed / 4_294_967_296;
};
const clock = () => {
  vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
  return vi.spyOn(Math, "random").mockReturnValue(0.999);
};

describe("lease-budgeted background mesh backoff", () => {
  it("starts at a 250 ms nominal window, doubles to <=10 s and resets only on confirmed success", async () => {
    clock(); const retry = new MeshBackgroundRetry("heartbeat");
    expect(MESH_BACKGROUND_RETRY_BASE_MS).toBe(250);
    expect(MESH_BACKGROUND_RETRY_CAP_MS).toBeLessThanOrEqual(10_000);
    for (const window of [250, 500, 1_000, 2_000, 4_000, 7_000, 7_000]) {
      expect(await retry.run(() => { throw busy(); })).toBe("retry");
      expect(retry.waitMs).toBe(Math.floor(0.999 * window));
      expect(await retry.run(() => undefined)).toBe("skipped");
      await vi.advanceTimersByTimeAsync(retry.waitMs);
    }
    expect(await retry.run(() => undefined, false)).toBe("done");
    retry.failure(busy()); expect(retry.waitMs).toBe(6_993);
    await vi.advanceTimersByTimeAsync(retry.waitMs);
    expect(await retry.run(() => undefined)).toBe("done");
    retry.failure(busy()); expect(retry.waitMs).toBe(249);
  });

  it("uses full rather than shifted jitter, including zero and draws below the base", async () => {
    const random = clock(), retry = new MeshBackgroundRetry("poll");
    for (const [draw, expected] of [[0, 1], [0.25, 125], [0.5, 500], [0.999, 1_998]]) {
      random.mockReturnValue(draw!); retry.failure(busy()); expect(retry.waitMs).toBe(expected);
      await vi.advanceTimersByTimeAsync(retry.waitMs);
    }
    const oversized = new MeshBackgroundRetry("bad cap", 50_000, 100_000);
    random.mockReturnValue(0.999); oversized.failure(busy()); expect(oversized.waitMs).toBe(6_993);
  });

  it("fences concurrent/reentrant attempts per key even if success is called while running", async () => {
    clock(); const retry = new MeshBackgroundRetry("key:a"), other = new MeshBackgroundRetry("key:b");
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    const operation = vi.fn(async () => { expect(await retry.run(() => undefined)).toBe("skipped"); await gate; });
    const work = retry.run(operation);
    try {
      const burst = await Promise.all(Array.from({ length: 50 }, () => retry.run(operation)));
      expect(new Set(burst)).toEqual(new Set(["skipped"]));
      retry.success(); expect(await retry.run(operation)).toBe("skipped");
      expect(await other.run(() => undefined)).toBe("done"); expect(operation).toHaveBeenCalledOnce();
    } finally { release(); await work; }
    expect(await retry.run(() => { throw busy(); })).toBe("retry");
    await vi.advanceTimersByTimeAsync(retry.waitMs);
    expect(await retry.run(() => undefined)).toBe("done");
  });

  it("clamps min(backoff, remaining lease minus margin) using actual expiry", async () => {
    clock(); let expiresAt: number | undefined;
    const retry = new MeshBackgroundRetry("renewal", 250, 7_000, { expiresAt: () => expiresAt, marginMs: 250 });
    for (let failure = 0; failure < 6; failure++) {
      retry.failure(busy()); await vi.advanceTimersByTimeAsync(retry.waitMs);
    }
    expiresAt = Date.now() + 1_000;
    retry.failure(busy()); expect(retry.waitMs).toBe(750);
    await vi.advanceTimersByTimeAsync(749); expect(await retry.run(() => undefined)).toBe("skipped");
    await vi.advanceTimersByTimeAsync(1);
    let attemptedAt = 0;
    expect(await retry.run(() => { attemptedAt = Date.now(); })).toBe("done");
    expect(attemptedAt).toBe(expiresAt - 250);
    expiresAt = Date.now() + 15_000;
    retry.failure(busy()); expect(retry.waitMs).toBe(249);
  });

  it("spends an unchanged lease's last retry budget once instead of spinning in its margin", async () => {
    clock(); const expiresAt = Date.now() + 1_000;
    const retry = new MeshBackgroundRetry("renewal", 7_000, 7_000, { expiresAt: () => expiresAt });
    retry.failure(busy()); expect(retry.waitMs).toBe(750);
    await vi.advanceTimersByTimeAsync(750);
    retry.failure(busy()); expect(retry.waitMs).toBe(6_993);
    retry.success(); retry.failure(busy()); expect(retry.waitMs).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    retry.failure(busy()); expect(retry.waitMs).toBe(6_993);
  });

  it("does not manufacture expiry before admission or hot-spin against a lapsed lease", () => {
    clock(); let expiresAt: number | undefined;
    const retry = new MeshBackgroundRetry("renewal", 250, 7_000, { expiresAt: () => expiresAt });
    retry.failure(busy()); expect(retry.waitMs).toBe(249);
    expiresAt = Date.now() - 1; retry.failure(busy()); expect(retry.waitMs).toBe(499);
    expiresAt = Number.NaN; retry.failure(busy()); expect(retry.waitMs).toBe(999);
  });

  it.each(["participant heartbeat/change refresh", "actor presence a1"])("%s stays single-flight for a minute of failures", async label => {
    clock(); vi.spyOn(Math, "random").mockImplementation(seeded(7176));
    const retry = new MeshBackgroundRetry(label);
    const starts: number[] = [], ends: number[] = [];
    let inFlight = 0, maxInFlight = 0;
    const acquire = async () => {
      starts.push(Date.now()); maxInFlight = Math.max(maxInFlight, ++inFlight);
      try { await new Promise(resolve => setTimeout(resolve, 30)); throw busy(); }
      finally { inFlight--; ends.push(Date.now()); }
    };
    const poller = setInterval(() => { void retry.run(acquire); }, 10);
    try { await vi.advanceTimersByTimeAsync(60_000); }
    finally { clearInterval(poller); await vi.advanceTimersByTimeAsync(30); }
    expect(maxInFlight).toBe(1);
    const gaps = starts.slice(1).map((start, index) => start - ends[index]!);
    expect(Math.min(...gaps)).toBeGreaterThanOrEqual(1);
    expect(Math.max(...gaps)).toBeLessThanOrEqual(7_000 + 10);
    expect(starts.length).toBeLessThan(35);
    const late = gaps.slice(Math.floor(gaps.length / 2));
    expect(late.reduce((sum, gap) => sum + gap, 0) / late.length).toBeGreaterThan(2_000);
  });
});
