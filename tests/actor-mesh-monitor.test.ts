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
      return { events, nextOffset: offset + events.length, cursors: events.map((_, index) => offset + index + 1) };
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

  // review/astra on #45, F3: the retry boundary comes from the same read; a compaction
  // between reads cannot move the saved cursor past the rejected event.
  it("keeps the rejected event's position from the page it was read in", async () => {
    vi.useFakeTimers({ now: 1_000_000 });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-"));
    roots.push(root);
    const cursorPath = path.join(root, "cursor.json");
    fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: 0 }));
    vi.spyOn(fs, "watch").mockReturnValue(Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as FSWatcher);
    const events = [0, 1, 2].map((index) => ({ topic: "t", createdAt: 999_000 + index })) as MeshEvent[];
    let reads = 0;
    const mesh = { root, latestOffset: vi.fn(() => 99), tail: vi.fn(() => reads++ === 0
      ? { events, nextOffset: 30, cursors: [10, 20, 30] }
      : { events: [], nextOffset: 999 }) };                // the log was compacted since
    const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 },
      { cursorPath, maxReplayAgeMs: 60_000, beforePoll: () => true, onEvent: (event) => event !== events[1] });
    monitors.push(monitor);
    monitor.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).cursor).toBe(10);   // just past the first event
    expect(reads).toBe(1);
  });

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

// smarty-dev#754 §3.2 step 3 (actors): work events are durable work. They skip the replay window,
// and ones a live-log rewrite dropped while the host was away come back from the archive.
describe("ActorMeshMonitor work-topic reconciliation", () => {
  const from = { id: "session:peer", name: "main", kind: "main" as const, sessionId: "peer" };
  const store = (options: { archive?: boolean; maxEventLogBytes?: number; retainedEventLogBytes?: number } = {}) => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-work-"));
    roots.push(base);
    const root = path.join(base, "mesh");
    fs.mkdirSync(root, { recursive: true });
    if (options.archive) {
      const dir = path.join(base, "archive");
      fs.mkdirSync(dir);
      fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir }));
    }
    const mesh = new MeshStore(root, 4_096, 500, {
      ...(options.maxEventLogBytes ? { maxEventLogBytes: options.maxEventLogBytes, retainedEventLogBytes: options.retainedEventLogBytes! } : {}),
    });
    return { mesh, cursorPath: path.join(base, "cursor.json") };
  };
  const monitor = (mesh: MeshStore, cursorPath: string, seen: MeshEvent[], maxReplayAgeMs?: number) => {
    const value = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 50 }, {
      cursorPath, beforePoll: () => true, onEvent: (event) => { seen.push(event); },
      ...(maxReplayAgeMs !== undefined ? { maxReplayAgeMs } : {}),
    });
    monitors.push(value);
    return value;
  };
  const drain = async (value: ActorMeshMonitor) => {
    for (let index = 0; index < 20; index++) { value.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); }
  };

  it("delivers a work event older than the replay window, and still skips an old ordinary one", async () => {
    const { mesh, cursorPath } = store();
    const first: MeshEvent[] = [];
    const before = monitor(mesh, cursorPath, first);
    await mesh.publish({ topic: "team.x", from, text: "seen" });
    await drain(before);
    before.close();
    await mesh.publish({ topic: "team.x", from, text: "old ordinary" });
    await mesh.publish({ topic: "fleet.work.pi-fabric.1", to: "actor:a", from, text: "old work" });
    const seen: MeshEvent[] = [];
    await drain(monitor(mesh, cursorPath, seen, -60_000));   // every event is older than the window
    expect(seen.map((event) => event.text)).toEqual(["old work"]);
  });

  it("delivers nothing twice when a rewrite restarts the stream on events already handed on", async () => {
    const { mesh, cursorPath } = store({ archive: true, maxEventLogBytes: 6_000, retainedEventLogBytes: 1_500 });
    const first: MeshEvent[] = [];
    const before = monitor(mesh, cursorPath, first);
    for (let index = 1; index <= 24; index++) await mesh.publish({ topic: "fleet.work.pi-fabric.1", to: "actor:a", from, text: `a${index}` });
    await drain(before);
    expect(first).toHaveLength(24);
    before.close();
    const generation = () => { try { return fs.readFileSync(path.join(mesh.root, "generation"), "utf8"); } catch { return ""; } };
    const was = generation();
    for (let index = 1; generation() === was && index <= 20; index++) await mesh.publish({ topic: "fleet.work.pi-fabric.1", to: "actor:a", from, text: `b${index}` });
    const retained = mesh.read({ limit: 50 }).map((event) => event.text);
    expect(retained.some((text) => text?.startsWith("a"))).toBe(true);          // the rewrite kept some delivered ones
    const seen: MeshEvent[] = [];
    await drain(monitor(mesh, cursorPath, seen, 10 * 60_000));
    expect(seen.length).toBeGreaterThan(0);
    expect(seen.every((event) => event.text?.startsWith("b"))).toBe(true);
  });

  it("reads work events a live-log rewrite dropped from the archive, each once, with nothing repeated", async () => {
    const { mesh, cursorPath } = store({ archive: true, maxEventLogBytes: 6_000, retainedEventLogBytes: 1_500 });
    const first: MeshEvent[] = [];
    const before = monitor(mesh, cursorPath, first);
    await mesh.publish({ topic: "fleet.work.pi-fabric.1", to: "actor:a", from, text: "work 0" });
    await drain(before);
    expect(first.map((event) => event.text)).toEqual(["work 0"]);
    before.close();
    // The host is away while the live log is rewritten several times.
    for (let index = 1; index <= 40; index++) {
      await mesh.publish({ topic: index % 5 === 0 ? "fleet.work.pi-fabric.1" : "team.noise", to: "actor:a", from, text: `${index % 5 === 0 ? "work" : "noise"} ${index}` });
    }
    expect(mesh.oldestSequence()).toBeGreaterThan(10);
    const seen: MeshEvent[] = [];
    await drain(monitor(mesh, cursorPath, seen, 10 * 60_000));
    const work = seen.filter((event) => event.topic.startsWith("fleet.")).map((event) => event.text);
    expect(work).toEqual(["work 5", "work 10", "work 15", "work 20", "work 25", "work 30", "work 35", "work 40"]);
    expect(new Set(seen.map((event) => event.id)).size).toBe(seen.length);
    expect(seen.some((event) => event.text === "work 0")).toBe(false);
  });
});
