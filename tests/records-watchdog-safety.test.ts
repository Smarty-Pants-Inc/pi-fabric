import { afterEach, expect, it, vi } from "vitest";
import { RecordsWatchdog } from "../src/records/watchdog.js";
import type { RecordsOps } from "../src/records/store.js";
import type { PublicationRelay } from "../src/records/relay.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });

it("Main records safety observes lag at most once a minute but never flushes, alarms or wakes idle work", async () => {
  vi.useFakeTimers();
  const lag = { consumer: "session:main", oldestAt: 1 };
  const lagging = vi.fn().mockResolvedValue([lag]);
  const wake = vi.fn(), alarm = vi.fn(), check = vi.fn(), flush = vi.fn();
  const interval = vi.spyOn(globalThis, "setInterval");
  const watchdog = new RecordsWatchdog({
    store: { lagging } as unknown as RecordsOps,
    relay: { flush } as unknown as PublicationRelay,
    self: lag.consumer, wake, alarm, check,
    intervalMs: 1, periodicObservationOnly: true,
  });
  try {
    watchdog.start();
    expect(interval.mock.calls.map(([, ms]) => ms)).toEqual([60_000]);
    await vi.advanceTimersByTimeAsync(5 * 60_000);
    expect(lagging).toHaveBeenCalledTimes(5);
    expect(wake).not.toHaveBeenCalled(); expect(alarm).not.toHaveBeenCalled();
    expect(flush).not.toHaveBeenCalled(); expect(check).not.toHaveBeenCalled();
    expect(await watchdog.tick(true)).toEqual({ lagging: [lag], woke: false, alarmed: [] });
    // Explicit owner maintenance retains its old contract; it is not an idle safety tick.
    flush.mockResolvedValue(undefined);
    expect((await watchdog.tick()).woke).toBe(true);
    expect(wake).toHaveBeenCalledOnce(); expect(flush).toHaveBeenCalledOnce();
  } finally { watchdog.stop(); await watchdog.idle(); }
  expect(vi.getTimerCount()).toBe(0);
});
