import { expect, vi } from "vitest";

// Drive only the host's one-shot deadline, with a controllable wall clock. Native
// watcher/actor/retention timers remain real: advancing all timers hides missed signals.
export const idleDeadlineDriver = () => {
  const timeout = globalThis.setTimeout;
  const clear = globalThis.clearTimeout;
  const live = new Map<ReturnType<typeof setTimeout>, () => void>();
  let remainingMs = 0;
  vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: (...args: unknown[]) => void, ms?: number, ...args: unknown[]) => {
    const idle = (new Error().stack ?? "").includes("armIdleDeadline");
    const handle = timeout(() => { live.delete(handle); (callback as (...args: unknown[]) => void)(...args); }, ms);
    if (idle) { remainingMs = Number(ms); live.set(handle, () => (callback as () => void)()); }
    return handle;
  }) as typeof setTimeout);
  vi.spyOn(globalThis, "clearTimeout").mockImplementation(handle => {
    if (handle) live.delete(handle as ReturnType<typeof setTimeout>);
    return clear(handle);
  });
  return {
    count: () => live.size,
    remainingMs: () => remainingMs,
    fire: () => {
      expect(live.size).toBe(1);
      const [handle, callback] = [...live][0]!;
      clearTimeout(handle);
      callback();
    },
  };
};
