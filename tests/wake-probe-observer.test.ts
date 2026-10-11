import { describe, expect, it, vi } from "vitest";

const observerModule = () => import(`../scripts/${"wake-probe-observer"}.mjs`);

describe("native wake probe event observation", () => {
  it("checks only initial state and received events, not a 50ms sampling timer", async () => {
    const { createWakeProbeObserver } = await observerModule();
    vi.useFakeTimers();
    try {
      const observer = createWakeProbeObserver();
      let ready = false;
      const predicate = vi.fn(() => ready);
      const waiting = observer.waitFor(predicate, "expected event", 15000);
      expect(predicate).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1000);
      expect(predicate).toHaveBeenCalledTimes(1);
      ready = true; observer.notify(); await waiting;
      expect(predicate).toHaveBeenCalledTimes(2);
      expect(vi.getTimerCount()).toBe(0);
      observer.close();
    } finally { vi.useRealTimers(); }
  });

  it("accepts already-arrived events and rejects bounded missing events", async () => {
    const { createWakeProbeObserver } = await observerModule();
    const observer = createWakeProbeObserver();
    await observer.waitFor(() => true, "already present", 100);
    await expect(observer.waitFor(() => false, "missing receipt", 5)).rejects.toThrow("Timed out: missing receipt");
    observer.close();
  });

  it("observes quiet windows with one deadline and fails immediately on an unexpected event", async () => {
    const { createWakeProbeObserver } = await observerModule();
    vi.useFakeTimers();
    try {
      const observer = createWakeProbeObserver();
      const check = vi.fn();
      const quiet = observer.quiet(check, "test-only absence", 10000);
      await vi.advanceTimersByTimeAsync(1000);
      expect(check).toHaveBeenCalledTimes(1);
      expect(vi.getTimerCount()).toBe(1);
      await vi.advanceTimersByTimeAsync(9000); await quiet;
      expect(check).toHaveBeenCalledTimes(2);
      const failed = observer.quiet(() => { throw new Error("unexpected inference"); }, "idle", 10000);
      await expect(failed).rejects.toThrow("unexpected inference");
      expect(vi.getTimerCount()).toBe(0);
      observer.close();
    } finally { vi.useRealTimers(); }
  });

  it("failure and close reject pending observations and release their deadlines", async () => {
    const { createWakeProbeObserver } = await observerModule();
    vi.useFakeTimers();
    try {
      const observer = createWakeProbeObserver();
      const waiting = observer.waitFor(() => false, "event", 10000);
      observer.fail(new Error("native CLI exited"));
      await expect(waiting).rejects.toThrow("native CLI exited");
      expect(vi.getTimerCount()).toBe(0);
      const closed = createWakeProbeObserver();
      const abandoned = closed.waitFor(() => false, "event", 10000);
      closed.close(); await expect(abandoned).rejects.toThrow("Probe observer closed");
      expect(vi.getTimerCount()).toBe(0);
    } finally { vi.useRealTimers(); }
  });
});
