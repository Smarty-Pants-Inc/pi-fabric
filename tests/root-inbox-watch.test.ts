import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RootInboxWatch } from "../src/topology/root-inbox-wake.js";

// smarty-dev#4313 RootInboxWatch scheduling contract (NOT E2E, Decision C): recorded calls to the
// injected `wake` under fake timers. Real fs notification from a second process is covered by
// tests/root-inbox-idle-wake-cli.test.ts. Healthy watches observe os.tmpdir() (existing dir, no
// files created or deleted); `request()` stands in for a directory notification.
const healthyRoot = os.tmpdir();

beforeEach(() => { vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] }); });
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
};

describe("RootInboxWatch (#4313 contract)", () => {
  it("arms no timer when the inbox reports no deadline: no periodic interval", async () => {
    const wake = vi.fn(async () => undefined);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(wake).toHaveBeenCalledTimes(1);
    watch.close();
  });

  it("fires one one-shot read at a known deadline, then stays quiet", async () => {
    const due = Date.now() + 60_000;
    const wake = vi.fn<() => Promise<number | undefined>>().mockResolvedValueOnce(due).mockResolvedValue(undefined);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    await vi.advanceTimersByTimeAsync(59_999);
    expect(wake).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(wake).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(wake).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
    watch.close();
  });

  it("replays notifications that land during an in-flight read exactly once", async () => {
    const first = deferred<number | undefined>();
    const wake = vi.fn<() => Promise<number | undefined>>().mockReturnValueOnce(first.promise).mockResolvedValue(undefined);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    expect(wake).toHaveBeenCalledTimes(1);
    watch.request();
    watch.request();
    expect(wake).toHaveBeenCalledTimes(1);   // coalesced while the read awaits
    first.resolve(undefined);
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(wake).toHaveBeenCalledTimes(2);
    watch.close();
  });

  it("close cancels a pending deadline and ignores later requests", async () => {
    const wake = vi.fn(async () => Date.now() + 30_000);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(vi.getTimerCount()).toBe(1);
    watch.close();
    expect(vi.getTimerCount()).toBe(0);
    watch.request();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(wake).toHaveBeenCalledTimes(1);
  });

  it("survives a throwing wake: arms nothing, and the next request reads again", async () => {
    const wake = vi.fn<() => Promise<number | undefined>>()
      .mockRejectedValueOnce(new Error("mesh lock timeout")).mockResolvedValue(undefined);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(0);
    watch.request();
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(2);
    watch.close();
  });

  // Node clamps a setTimeout delay above 2^31-1 ms to 1 ms: a far deadline (a huge cooldown) must
  // arm bounded chunks that re-arm silently, never an immediate read loop.
  const MAX_TIMER_DELAY = 2 ** 31 - 1;

  it("chunks a far deadline under the 2^31-1 ms timer limit and reads only at the deadline", async () => {
    const timer = vi.spyOn(globalThis, "setTimeout");
    const due = Date.now() + 1e12;
    const wake = vi.fn<() => Promise<number | undefined>>().mockResolvedValueOnce(due).mockResolvedValue(undefined);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(1);
    const delays = () => timer.mock.calls.map((call) => Number(call[1] ?? 0));
    expect(delays().length).toBeGreaterThan(0);
    expect(Math.max(...delays())).toBeLessThanOrEqual(MAX_TIMER_DELAY);
    // One full chunk plus part of the remainder: chunk re-arms are not reads.
    await vi.advanceTimersByTimeAsync(MAX_TIMER_DELAY + 1_000);
    expect(wake).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(due - Date.now() - 1);
    expect(wake).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(wake).toHaveBeenCalledTimes(2);   // exactly the deadline read; it reports nothing more
    expect(vi.getTimerCount()).toBe(0);
    expect(Math.max(...delays())).toBeLessThanOrEqual(MAX_TIMER_DELAY);
    watch.close();
  });

  it("under a far deadline, an earlier request and deadline still read, and close cancels a re-armed chunk", async () => {
    const wake = vi.fn<() => Promise<number | undefined>>()
      .mockImplementationOnce(async () => Date.now() + 1e12)
      .mockImplementationOnce(async () => Date.now() + 60_000)
      .mockResolvedValueOnce(undefined)
      .mockImplementationOnce(async () => Date.now() + 1e12)
      .mockResolvedValue(undefined);
    const watch = new RootInboxWatch(healthyRoot, wake);
    watch.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(1);
    watch.request();   // a notification still reads at once
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(59_999);
    expect(wake).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(wake).toHaveBeenCalledTimes(3);   // the earlier deadline wins over the far one
    watch.request();   // reports a far deadline again
    await vi.advanceTimersByTimeAsync(MAX_TIMER_DELAY + 1_000);
    expect(wake).toHaveBeenCalledTimes(4);   // past one chunk: re-armed, not read
    expect(vi.getTimerCount()).toBe(1);
    watch.close();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(1e12);
    expect(wake).toHaveBeenCalledTimes(4);
  });

  it("reports a watch failure once and still serves requests and deadlines", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const missing = path.join(os.tmpdir(), "pi-fabric-root-inbox-watch-missing-never-created");
    const due = Date.now() + 1_000;
    const wake = vi.fn<() => Promise<number | undefined>>().mockResolvedValueOnce(due).mockResolvedValue(undefined);
    const watch = new RootInboxWatch(missing, wake);
    watch.start();
    expect(watch.failure).toMatch(/root inbox watch failed/);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(wake).toHaveBeenCalledTimes(2);   // the deadline still fires without notifications
    watch.request();
    await vi.advanceTimersByTimeAsync(0);
    expect(wake).toHaveBeenCalledTimes(3);
    expect(warn).toHaveBeenCalledTimes(1);   // one line per distinct failure, not per reopen
    watch.close();
  });
});
