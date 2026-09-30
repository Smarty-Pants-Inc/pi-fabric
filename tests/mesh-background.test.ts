import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../src/core/atomic-write.js";
import { MeshLockTimeoutError } from "../src/mesh.js";

const busy = () => new MeshLockTimeoutError(" held by pid 123 (alive, running)", 6, 8020);
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

describe("background mesh retry boundary", () => {
  it("backs off only typed timeouts, caps delay, logs holder once and resets after recovery", async () => {
    vi.useFakeTimers();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("poll", 100, 400);
    const operation = vi.fn(async () => { throw busy(); });
    expect(await retry.run(operation)).toBe("retry");
    expect(retry.waitMs).toBe(100);
    expect(await retry.run(operation)).toBe("skipped");
    expect(operation).toHaveBeenCalledTimes(1);
    for (const wait of [100, 200, 400, 400]) {
      await vi.advanceTimersByTimeAsync(wait);
      expect(await retry.run(operation)).toBe("retry");
    }
    expect(retry.waitMs).toBe(400);
    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]![0]).toContain("held by pid 123");
    await vi.advanceTimersByTimeAsync(400);
    expect(await retry.run(() => undefined)).toBe("done");
    expect(retry.waitMs).toBe(0);
    expect(await retry.run(operation)).toBe("retry");
    expect(retry.waitMs).toBe(100);
    expect(warn).toHaveBeenCalledTimes(2);
  });

  it("contains sync/async non-lock bugs visibly without imposing lock backoff", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const retry = new MeshBackgroundRetry("handler");
    expect(await retry.run(() => { throw new Error("bug"); })).toBe("failed");
    expect(await retry.run(async () => { throw new Error("another bug"); })).toBe("failed");
    expect(retry.waitMs).toBe(0);
    expect(warn).toHaveBeenCalledTimes(2);
    expect(await retry.run(() => undefined)).toBe("done");
  });

  it("serializes one-shot events and retains them across timeouts without blocking later hook admission", async () => {
    vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("events");
    let locked = true;
    const written: number[] = [];
    const first = vi.fn(() => { if (locked) throw busy(); written.push(1); });
    await queue.enqueue(first); // first failed attempt, not an infinite lock wait
    await queue.enqueue(() => written.push(2)); // admitted during outage without waiting
    expect(written).toEqual([]);
    await vi.advanceTimersByTimeAsync(100);
    expect(first).toHaveBeenCalledTimes(2);
    locked = false;
    await vi.advanceTimersByTimeAsync(200);
    expect(written).toEqual([1, 2]);
    await queue.close();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("never retries an untyped/permanent failure and continues the notification queue", async () => {
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("events");
    const fail = vi.fn(() => { throw new Error("Timed out waiting for the Fabric mesh lock"); });
    await queue.enqueue(fail);
    const later = vi.fn();
    await queue.enqueue(later);
    await queue.close();
    expect(fail).toHaveBeenCalledOnce(); expect(later).toHaveBeenCalledOnce();
  });

  it("close cancels pending retries, settles hook admission and fences the in-flight operation", async () => {
    vi.useFakeTimers(); vi.spyOn(console, "warn").mockImplementation(() => {});
    const queue = new MeshBackgroundQueue("events");
    const fail = vi.fn(() => { throw busy(); });
    await queue.enqueue(fail);
    await queue.close();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(fail).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
    const running = new MeshBackgroundQueue("running");
    let release!: () => void;
    const admitted = running.enqueue(() => new Promise<void>(resolve => { release = resolve; }));
    await Promise.resolve(); await Promise.resolve();
    let closed = false;
    const closing = running.close().then(() => { closed = true; });
    await Promise.resolve(); expect(closed).toBe(false);
    release(); await admitted; await closing;
    expect(closed).toBe(true);
  });
});
