import { expect, vi } from "vitest";

/** Capture the real backend watchdog, not the outer Main watchdog or guest timers.
 * Invoke it with the wall clock one millisecond short of its recorded deadline.
 * Real workers and all other clocks/timers continue to run normally. */
export function captureRuntimeDeadline(backend: string) {
  const actual = globalThis.setTimeout;
  let scheduled: { callback: () => void; timer: ReturnType<typeof setTimeout>; at: number } | undefined;
  const spy = vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: unknown[]) => void, delay?: number, ...args: unknown[]) => {
    const stack = new Error().stack ?? "";
    const timer = actual(callback, delay, ...args);
    if (stack.includes("scheduleDeadline") && stack.includes(`${backend}-runtime`)) {
      scheduled = { callback: () => callback(...args), timer, at: Date.now() + (delay ?? 0) };
    }
    return timer;
  }) as typeof setTimeout);
  const fire = (at: number): void => {
    expect(scheduled, "runtime deadline was armed").toBeDefined();
    const current = scheduled!;
    clearTimeout(current.timer);
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    try { current.callback(); } finally { clock.mockRestore(); }
  };
  return {
    ready: () => scheduled !== undefined,
    fireEarly(deadlineAt?: number) {
      expect(scheduled, "runtime deadline was armed").toBeDefined();
      fire((deadlineAt ?? scheduled!.at) - 1);
    },
    fireAt: (deadlineAt: number) => fire(deadlineAt),
    restore: () => spy.mockRestore(),
  };
}
