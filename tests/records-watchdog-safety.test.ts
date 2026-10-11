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

// #792 review P1-1: without the relay credential no committed append publishes a nudge (F9),
// so the minute observation tick keeps the documented own-root wake (records/service.ts):
// it wakes this root, through its gate, for this root's own lag only; never a peer's, never an alarm.
it("without a relay credential, the observation tick still wakes this root for its own lag only", async () => {
  const own = { consumer: "session:main", after: 0, count: 1, oldestAt: 1 };
  const peer = { consumer: "session:peer", after: 0, count: 2, oldestAt: 1 };
  const lagging = vi.fn().mockResolvedValue([peer, own]);
  const claimAlarm = vi.fn(), alarm = vi.fn(), check = vi.fn();
  const wake = vi.fn().mockResolvedValue(undefined);
  const watchdog = new RecordsWatchdog({
    store: { lagging, claimAlarm } as unknown as RecordsOps,
    self: own.consumer, wake, alarm, check, periodicObservationOnly: true,
  });
  expect(await watchdog.tick(true)).toEqual({ lagging: [peer, own], woke: true, alarmed: [] });
  expect(wake).toHaveBeenCalledOnce();
  expect(alarm).not.toHaveBeenCalled(); expect(claimAlarm).not.toHaveBeenCalled(); expect(check).not.toHaveBeenCalled();
  // No lag of its own: observation stays read-only.
  lagging.mockResolvedValue([peer]);
  expect(await watchdog.tick(true)).toEqual({ lagging: [peer], woke: false, alarmed: [] });
  expect(wake).toHaveBeenCalledOnce();
});
