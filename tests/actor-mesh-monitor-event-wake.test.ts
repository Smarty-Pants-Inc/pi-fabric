import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { resetWatchFallbackWarnings } from "../src/core/watch-fallback.js";
import type { MeshEvent, MeshTailResult } from "../src/mesh/store.js";

// #11172: no standing safety-net interval. Watch errors rescan and re-arm; failed
// drains and unsupported watches use bounded one-shot timers only.
const roots: string[] = [];
const monitors: ActorMeshMonitor[] = [];
beforeEach(() => {
  resetWatchFallbackWarnings();
  vi.spyOn(console, "warn").mockImplementation(() => {});
});
afterEach(() => {
  for (const monitor of monitors.splice(0)) monitor.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

type FakeWatcher = EventEmitter & { close: ReturnType<typeof vi.fn> };
function setup(extra: Record<string, unknown> = {}) {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-wake-"));
  roots.push(root);
  const watchers: FakeWatcher[] = [];
  const watch = vi.spyOn(fs, "watch").mockImplementation((() => {
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    watchers.push(watcher);
    return watcher as unknown as FSWatcher;
  }) as unknown as typeof fs.watch);
  const mesh = {
    root,
    latestOffset: vi.fn(() => 10),
    tail: vi.fn((_offset: number, _limit: number): MeshTailResult => ({ events: [], nextOffset: 10 })),
    ...extra,
  };
  const beforePoll = vi.fn(() => true);
  const onEvent = vi.fn();
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 },
    { cursorPath: path.join(root, "cursor.json"), beforePoll, onEvent });
  monitors.push(monitor);
  const interval = vi.spyOn(globalThis, "setInterval");
  const notify = (name: string | null = "events.jsonl") =>
    (watch.mock.calls.at(-1)!.at(-1) as fs.WatchListener<string>)("change", name);
  return { root, watch, watchers, mesh, beforePoll, onEvent, monitor, interval, notify };
}

const flush = async () => { for (let index = 0; index < 8; index++) await Promise.resolve(); };
const publish = (s: ReturnType<typeof setup>, topic: string, offset: number) => {
  const event = { topic } as MeshEvent;
  s.mesh.tail.mockReturnValue({ events: [event], nextOffset: offset });
  fs.appendFileSync(path.join(s.root, "events.jsonl"), `${topic}\n`);
  return event;
};

describe("ActorMeshMonitor event wakes (#11172)", () => {
  it("owns no interval and no timer while healthy and idle, yet picks up a publish promptly", async () => {
    const s = setup();
    s.monitor.start(); await flush();
    expect(s.interval).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(s.beforePoll).toHaveBeenCalledOnce();
    expect(s.mesh.tail).toHaveBeenCalledOnce();
    const event = publish(s, "work", 20);
    s.notify(); await flush();
    expect(s.onEvent).toHaveBeenCalledWith(event);
    expect(vi.getTimerCount()).toBe(0);
    expect(s.interval).not.toHaveBeenCalled();
  });

  it("rescans and re-arms on a watcher error; a repeat error backs off on a one-shot deadline", async () => {
    const s = setup();
    s.monitor.start(); await flush();
    const missed = publish(s, "missed", 20); // the failed watch never reported it
    s.watchers[0]!.emit("error", new Error("watch died"));
    expect(s.watchers[0]!.close).toHaveBeenCalledOnce();
    expect(s.watch).toHaveBeenCalledTimes(2); // re-armed at once
    await flush();
    expect(s.onEvent).toHaveBeenCalledWith(missed); // rescanned
    expect(vi.getTimerCount()).toBe(0);
    // A second error without an intervening notification does not re-arm in a hot loop.
    s.watchers[1]!.emit("error", new Error("watch died again"));
    await flush();
    expect(s.watch).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(1);
    const late = publish(s, "late", 30);
    await vi.advanceTimersByTimeAsync(99); // second consecutive failure: 2 x actorPollMs
    expect(s.watch).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(s.watch).toHaveBeenCalledTimes(3);
    expect(s.onEvent).toHaveBeenCalledWith(late); // the re-arm deadline rescans
    expect(vi.getTimerCount()).toBe(0);
    // A delivered notification proves health; the next error re-arms at once again.
    const next = publish(s, "next", 40);
    s.notify(); await flush();
    expect(s.onEvent).toHaveBeenCalledWith(next);
    s.watchers[2]!.emit("error", new Error("third"));
    expect(s.watch).toHaveBeenCalledTimes(4);
    expect(s.interval).not.toHaveBeenCalled();
  });

  it("an unsupported watch retries the re-arm on bounded one-shot deadlines, never an interval", async () => {
    const s = setup();
    let failures = 3;
    const realWatch = s.watch.getMockImplementation()!;
    s.watch.mockImplementation(((...args: Parameters<typeof fs.watch>) => {
      if (failures-- > 0) throw Object.assign(new Error("EPERM watch"), { code: "EPERM", syscall: "watch" });
      return (realWatch as unknown as typeof fs.watch)(...args);
    }) as unknown as typeof fs.watch);
    s.monitor.start(); await flush();
    expect(vi.getTimerCount()).toBe(1);
    const missed = publish(s, "missed", 20);
    await vi.advanceTimersByTimeAsync(50); // 1st deadline
    expect(s.watch).toHaveBeenCalledTimes(2);
    expect(s.onEvent).toHaveBeenCalledWith(missed);
    await vi.advanceTimersByTimeAsync(99);
    expect(s.watch).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1); // 2nd deadline doubled
    expect(s.watch).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(200); // 3rd deadline: watch now supported
    expect(s.watch).toHaveBeenCalledTimes(4);
    await flush();
    expect(vi.getTimerCount()).toBe(0);
    expect(s.interval).not.toHaveBeenCalled();
  });

  it("caps the unsupported-watch re-arm deadline", async () => {
    const s = setup();
    s.watch.mockImplementation((() => { throw new Error("unsupported"); }) as unknown as typeof fs.watch);
    s.monitor.start(); await flush();
    await vi.advanceTimersByTimeAsync(60_000);
    const calls = s.watch.mock.calls.length;
    await vi.advanceTimersByTimeAsync(5_000);
    expect(s.watch.mock.calls.length).toBe(calls + 1); // 5 s cap, one timer at a time
    expect(vi.getTimerCount()).toBe(1);
    expect(s.interval).not.toHaveBeenCalled();
  });

  it("retries a failed drain with a bounded one-shot backoff and stops once it succeeds", async () => {
    const s = setup();
    const event = { topic: "work" } as MeshEvent;
    let fail = 3;
    s.mesh.tail.mockImplementation(() => {
      if (fail-- > 0) throw new Error("boom");
      return { events: [event], nextOffset: 20 };
    });
    s.monitor.start(); await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(1);
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(99);
    expect(s.mesh.tail).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(200);
    expect(s.mesh.tail).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(400);
    expect(s.mesh.tail).toHaveBeenCalledTimes(4);
    expect(s.onEvent).toHaveBeenCalledWith(event);
    expect(vi.getTimerCount()).toBe(0);
    expect(s.interval).not.toHaveBeenCalled();
  });

  it("retries a lock-deferred drain once the backoff expires, without a standing timer", async () => {
    const s = setup();
    let fail = true;
    const event = { topic: "work" } as MeshEvent;
    s.mesh.tail.mockImplementation(() => {
      if (fail) { fail = false; throw new MeshLockTimeoutError(" held by pid 1", 6, 8020); }
      return { events: [event], nextOffset: 20 };
    });
    s.monitor.start(); await flush();
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(s.onEvent).toHaveBeenCalledWith(event);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("releases a scheduled event at its real due time with one one-shot timer", async () => {
    let dueAt: number | undefined;
    const releaseDueSchedules = vi.fn(async () => { dueAt = undefined; });
    const s = setup({ nextScheduleDueAt: () => dueAt, releaseDueSchedules });
    dueAt = Date.now() + 30_000;
    s.monitor.start(); await flush();
    expect(vi.getTimerCount()).toBe(1);
    await vi.advanceTimersByTimeAsync(29_999);
    expect(releaseDueSchedules).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(releaseDueSchedules).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
    expect(s.interval).not.toHaveBeenCalled();
  });

  it("close joins the owned watcher and clears every one-shot timer", async () => {
    const s = setup();
    s.mesh.tail.mockImplementation(() => { throw new Error("boom"); });
    s.monitor.start(); await flush();
    s.watchers[0]!.emit("error", new Error("x"));
    s.watchers[1]!.emit("error", new Error("y"));
    await flush();
    expect(vi.getTimerCount()).toBeGreaterThan(0);
    s.monitor.close();
    expect(s.watchers.every(watcher => watcher.close.mock.calls.length === 1)).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(s.watch).toHaveBeenCalledTimes(2);
  });
});
