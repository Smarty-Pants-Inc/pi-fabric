import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { MeshStore, type MeshEvent } from "../src/mesh/store.js";

const roots: string[] = [];
const monitors: ActorMeshMonitor[] = [];
afterEach(() => {
  for (const monitor of monitors.splice(0)) monitor.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function setup(cursor?: string) {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-"));
  roots.push(root);
  const cursorPath = path.join(root, "cursor.json");
  if (cursor !== undefined) fs.writeFileSync(cursorPath, cursor);
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
  vi.spyOn(fs, "watch").mockReturnValue(watcher as unknown as FSWatcher);
  const event = { topic: "test" } as MeshEvent;
  const mesh = { root, latestOffset: vi.fn(() => 10), tail: vi.fn(() => ({ events: [event], nextOffset: 20 })) };
  const beforePoll = vi.fn(() => true);
  const onEvent = vi.fn();
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 }, { cursorPath, beforePoll, onEvent });
  monitors.push(monitor);
  return { root, cursorPath, watcher, mesh, beforePoll, onEvent, monitor };
}

const flush = async () => { await Promise.resolve(); await Promise.resolve(); };

describe("ActorMeshMonitor", () => {
  it("coalesces notifications, preserves a halted cursor, and resumes in order", async () => {
    const s = setup('{"format":1,"cursor":3}');
    s.beforePoll.mockReturnValue(false);
    s.monitor.start();
    await flush();
    expect(s.mesh.tail).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(3);
    s.beforePoll.mockReturnValue(true);
    s.monitor.schedule();
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledExactlyOnceWith(3, 7);
    expect(s.onEvent).toHaveBeenCalledOnce();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8"))).toEqual({ format: 1, cursor: 20 });
  });

  it.skipIf(process.platform === "win32")("falls back after watcher errors and closes timers and queued work", async () => {
    const s = setup();
    s.monitor.start();
    await flush();
    s.watcher.emit("error", new Error("watch failed"));
    await flush();
    expect(s.watcher.close).toHaveBeenCalledOnce();
    const count = s.mesh.tail.mock.calls.length;
    await vi.advanceTimersByTimeAsync(50);
    expect(s.mesh.tail).toHaveBeenCalledTimes(count + 1);
    s.monitor.schedule();
    s.monitor.close();
    s.monitor.close();
    s.watcher.emit("error", new Error("late error"));
    await vi.advanceTimersByTimeAsync(5000);
    expect(s.mesh.tail).toHaveBeenCalledTimes(count + 1);
    expect(vi.getTimerCount()).toBe(0);
    expect(s.watcher.close).toHaveBeenCalledOnce();
  });

  it("resumes from a saved cursor but skips events older than the replay window", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-"));
    roots.push(root);
    const cursorPath = path.join(root, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: 5 }));
    vi.spyOn(fs, "watch").mockReturnValue(Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as FSWatcher);
    const old = { topic: "t", createdAt: 1_000_000 - 60_001 } as MeshEvent;
    const recent = { topic: "t", createdAt: 1_000_000 - 30_000 } as MeshEvent;
    const log = [old, recent];                               // offsets 5 and 6; 7 is the end
    const mesh = { root, latestOffset: vi.fn(() => 99), tail: vi.fn((offset: number, limit = 7) => {
      const events = log.slice(offset - 5, offset - 5 + limit);
      return { events, nextOffset: offset + events.length };
    }) };
    const onEvent = vi.fn();
    const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 },
      { cursorPath, maxReplayAgeMs: 60_000, beforePoll: () => true, onEvent });
    monitors.push(monitor);
    monitor.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(mesh.tail).toHaveBeenCalledWith(5, 7);            // resumed from the saved cursor, a page at a time
    expect(onEvent.mock.calls.map(([event]) => event)).toEqual([recent]);
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).cursor).toBe(7);
  });

  it("keeps a catch-up event that a full receiver rejected, and offers it again", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-"));
    roots.push(root);
    const cursorPath = path.join(root, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: 0 }));
    vi.spyOn(fs, "watch").mockReturnValue(Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as FSWatcher);
    const log = [{ topic: "t", createdAt: 998_000 }, { topic: "t", createdAt: 999_000 }, { topic: "t", createdAt: 999_500 }] as MeshEvent[];
    const mesh = { root, latestOffset: vi.fn(() => 99), tail: vi.fn((offset: number, limit = 7) => {
      const events = log.slice(offset, offset + limit);
      return { events, nextOffset: offset + events.length };
    }) };
    let full = true;
    // The first event is taken; the second is rejected mid-page while the queue is full.
    const onEvent = vi.fn((event: MeshEvent) => event === log[0] || !full);
    const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 },
      { cursorPath, maxReplayAgeMs: 60_000, beforePoll: () => true, onEvent });
    monitors.push(monitor);
    monitor.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(onEvent).toHaveBeenCalledTimes(2);                 // the second is rejected: the cursor stays on it
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).cursor).toBe(1);
    full = false;
    monitor.schedule();
    await vi.advanceTimersByTimeAsync(0);
    expect(onEvent.mock.calls.map(([event]) => event)).toEqual([log[0], log[1], log[1], log[2]]);
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).cursor).toBe(3);
  });

  // review/astra on #45: a large backlog is read in pages, with batched cursor writes, while
  // the event loop keeps running (the lease heartbeat).
  it("catches up a large backlog in pages and yields to the event loop", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-"));
    roots.push(root);
    const store = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const cursorPath = path.join(root, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: store.latestOffset() }));
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    for (let index = 0; index < 1_500; index++) await store.publish({ topic: index % 100 === 0 ? "wanted" : "other", from, text: `e${index}` });
    const tail = vi.spyOn(store, "tail");
    const writes = vi.spyOn(fs, "renameSync");
    let ticks = 0;
    const heartbeat = setInterval(() => { ticks++; }, 1);
    const seen: string[] = [];
    let ticksWhenDone = -1;
    const monitor = new ActorMeshMonitor(store, { enabled: true, actorPollMs: 60_000, maxReadEvents: 100 },
      { cursorPath, maxReplayAgeMs: 60_000, beforePoll: () => true, onEvent: (event) => {
        if (event.topic !== "wanted") return;
        seen.push(event.text ?? "");
        if (seen.length === 15) ticksWhenDone = ticks;
      } });
    monitors.push(monitor);
    try {
      monitor.start();
      await vi.waitFor(() => expect(seen).toHaveLength(15), { timeout: 10_000, interval: 5 });
      await new Promise((resolve) => setTimeout(resolve, 50));
    } finally {
      clearInterval(heartbeat);
    }
    expect(tail.mock.calls.length).toBeLessThanOrEqual(1_500 / 100 + 3);     // pages, not events
    expect(writes.mock.calls.filter(([, target]) => String(target) === cursorPath).length).toBeLessThanOrEqual(1_500 / 100 + 3);
    expect(ticksWhenDone).toBeGreaterThan(0);                                 // timers ran during catch-up
  }, 30_000);

  it("uses polling when watch creation fails and ignores malformed cursors", async () => {
    const s = setup('{"format":2,"cursor":3}');
    vi.mocked(fs.watch).mockImplementation(() => { throw new Error("unsupported"); });
    s.monitor.start();
    s.monitor.start();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledExactlyOnceWith(10, 7);
    await vi.advanceTimersByTimeAsync(50);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(1);
  });

  it("does not commit a cursor after dispatch failure and tolerates cursor write failure", async () => {
    const s = setup('{"format":1,"cursor":3}');
    s.onEvent.mockImplementationOnce(() => { throw new Error("dispatch"); });
    s.monitor.start();
    await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(3);
    fs.rmSync(s.cursorPath);
    fs.mkdirSync(s.cursorPath);
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenLastCalledWith(20, 7);
    expect(s.onEvent).toHaveBeenCalledTimes(2);
  });
});
