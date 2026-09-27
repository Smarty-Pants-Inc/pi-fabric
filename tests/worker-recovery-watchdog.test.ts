import { afterEach, describe, expect, it, vi } from "vitest";
import { PI_RECOVERY_TIMEOUT_MS, PiRecoveryWatchdog } from "../src/worker/recovery-watchdog.js";

afterEach(() => vi.useRealTimers());

const setup = () => {
  vi.useFakeTimers();
  const fail = vi.fn();
  return { fail, watchdog: new PiRecoveryWatchdog(fail) };
};

describe("PiRecoveryWatchdog", () => {
  it("bounds a terminated response and reports the original cause exactly once", () => {
    const { fail, watchdog } = setup();
    expect(PI_RECOVERY_TIMEOUT_MS).toBe(60_000);
    watchdog.arm("Error: Terminated");
    vi.advanceTimersByTime(59_999);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(fail).toHaveBeenCalledExactlyOnceWith(
      "Error: Terminated; Pi made no recovery progress for 60000ms; terminating child",
    );
    watchdog.arm("another retry");
    watchdog.progress();
    vi.advanceTimersByTime(120_000);
    expect(fail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not extend the deadline for repeated errors or retry announcements", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    for (let i = 0; i < 5; i++) {
      vi.advanceTimersByTime(10_000);
      watchdog.arm("Error: Terminated");
    }
    vi.advanceTimersByTime(10_000);
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it("refreshes on real streaming progress but still bounds a retry that stalls again", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    vi.advanceTimersByTime(50_000);
    watchdog.progress();
    vi.advanceTimersByTime(50_000);
    expect(fail).not.toHaveBeenCalled();
    vi.advanceTimersByTime(10_000);
    expect(fail).toHaveBeenCalledTimes(1);
  });

  it("does not time out healthy work and permits a later independent failure", () => {
    const { fail, watchdog } = setup();
    watchdog.progress();
    vi.advanceTimersByTime(120_000);
    watchdog.arm("first error");
    vi.advanceTimersByTime(30_000);
    watchdog.clear();
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    watchdog.arm("later error");
    vi.advanceTimersByTime(60_000);
    expect(fail.mock.calls[0]?.[0]).toContain("later error");
  });

  it("cancels pending recovery on stop, timeout or process exit", () => {
    const { fail, watchdog } = setup();
    watchdog.arm("Error: Terminated");
    watchdog.dispose();
    watchdog.progress();
    watchdog.arm("late event");
    vi.advanceTimersByTime(120_000);
    expect(fail).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});
