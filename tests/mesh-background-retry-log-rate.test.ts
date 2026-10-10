import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundRetry, MeshLockTimeoutError } from "../src/core/atomic-write.js";

const busy = () => new MeshLockTimeoutError(" held by pid 123 (alive, running)", 6, 8020);
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

describe("background mesh retry warning rate limit", () => {
  it("logs 100 short outages within 60 seconds once, then reports 99 suppressed retries at 60 seconds", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("participant heartbeat/change refresh");
    const operation = () => { throw busy(); };
    for (let outage = 0; outage < 100; outage++) {
      expect(await retry.run(operation)).toBe("retry");
      expect(retry.waitMs).toBe(50);
      if (outage === 0) {
        expect(warn).toHaveBeenCalledOnce();
        expect(warn.mock.calls[0]![0]).toContain("mesh lock timeout; retrying in 50 ms");
        expect(warn.mock.calls[0]![0]).toContain("held by pid 123");
        expect(warn.mock.calls[0]![0]).not.toContain("suppressed");
      }
      await vi.advanceTimersByTimeAsync(50);
      expect(await retry.run(() => undefined)).toBe("done");
      expect(retry.waitMs).toBe(0);
      await vi.advanceTimersByTimeAsync(450);
    }
    expect(warn).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(await retry.run(operation)).toBe("retry");
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toMatch(/\(99 similar retries suppressed in the last 60s\)$/);

    await vi.advanceTimersByTimeAsync(50);
    expect(await retry.run(() => undefined)).toBe("done");
    await vi.advanceTimersByTimeAsync(59_950);
    expect(await retry.run(operation)).toBe("retry");
    expect(warn).toHaveBeenCalledTimes(3);
    expect(warn.mock.calls[2]![0]).not.toContain("suppressed");
  });

  it("suppresses through 59999 ms without recovery resetting the warning clock", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("poll");
    retry.failure(busy());
    retry.success();
    vi.setSystemTime(59_999);
    retry.failure(busy());
    retry.success();
    expect(warn).toHaveBeenCalledOnce();
    vi.setSystemTime(60_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toMatch(/\(1 similar retries suppressed in the last 60s\)$/);
  });

  it("summarizes continuous retries using elapsed seconds and resets the count after reporting", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("poll");
    retry.failure(busy());
    retry.failure(busy());
    retry.failure(busy());
    expect(warn).toHaveBeenCalledOnce();
    vi.setSystemTime(61_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[1]![0]).toMatch(/\(2 similar retries suppressed in the last 61s\)$/);
    retry.failure(busy());
    vi.setSystemTime(121_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(3);
    expect(warn.mock.calls[2]![0]).toMatch(/\(1 similar retries suppressed in the last 60s\)$/);
  });

  it("logs every non-transient error without clearing transient suppression", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("handler");
    retry.failure(busy());
    retry.success();
    for (const error of [new Error("bug"), "plain failure", new Error("Timed out waiting for the Fabric mesh lock")]) {
      expect(await retry.run(() => { throw error; })).toBe("failed");
      expect(retry.waitMs).toBe(0);
      retry.failure(busy());
      retry.success();
    }
    expect(warn).toHaveBeenCalledTimes(4);
    expect(warn.mock.calls.slice(1).map(call => call[0])).toEqual([
      "[pi-fabric] handler: background operation failed: bug",
      "[pi-fabric] handler: background operation failed: plain failure",
      "[pi-fabric] handler: background operation failed: Timed out waiting for the Fabric mesh lock",
    ]);
    vi.setSystemTime(60_000);
    retry.failure(busy());
    expect(warn).toHaveBeenCalledTimes(5);
    expect(warn.mock.calls[4]![0]).toMatch(/\(3 similar retries suppressed in the last 60s\)$/);
  });

  it("keeps warning budgets independent for separate background paths", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const first = new MeshBackgroundRetry("first");
    const second = new MeshBackgroundRetry("second");
    first.failure(busy());
    second.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
    expect(warn.mock.calls[0]![0]).toContain("first:");
    expect(warn.mock.calls[1]![0]).toContain("second:");
    first.success();
    second.success();
    first.failure(busy());
    second.failure(busy());
    expect(warn).toHaveBeenCalledTimes(2);
  });
});
