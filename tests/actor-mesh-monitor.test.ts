import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshEvent, type MeshTailResult } from "../src/mesh/store.js";

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
  const mesh = { root, latestOffset: vi.fn(() => 10), tail: vi.fn((_offset: number, _limit: number): MeshTailResult => ({ events: [event], nextOffset: 20 })) };
  const beforePoll = vi.fn(() => true);
  const onEvent = vi.fn();
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 }, { cursorPath, beforePoll, onEvent });
  monitors.push(monitor);
  return { root, cursorPath, watcher, mesh, beforePoll, onEvent, monitor };
}

// Include the background retry fence and the retained-wake microtask.
const flush = async () => { for (let index = 0; index < 8; index++) await Promise.resolve(); };

describe("ActorMeshMonitor", () => {
  it.skipIf(process.platform === "win32").each([50, 1_600])("delivers every changed watch burst within 100 ms without an actorPollMs=%i trailing timer", async actorPollMs => {
    const s = setup();
    s.monitor.config.actorPollMs = actorPollMs;
    s.mesh.tail.mockReturnValue({ events: [], nextOffset: 10 });
    s.monitor.start(); await flush();
    const notify = vi.mocked(fs.watch).mock.calls[0]!.at(-1) as fs.WatchListener<string>;
    for (let index = 0; index < 12; index++) {
      const event = { topic: `burst-${index}` } as MeshEvent;
      s.mesh.tail.mockReturnValue({ events: [event], nextOffset: 20 + index });
      fs.appendFileSync(path.join(s.root, "events.jsonl"), `${index}\n`);
      const startedAt = Date.now();
      notify("change", index % 2 ? "events.jsonl" : null);
      notify("change", "events.jsonl"); // same-turn notification coalescing
      notify("rename", "state.read-signal.json");
      await flush();
      expect(s.onEvent).toHaveBeenLastCalledWith(event);
      expect(s.mesh.tail).toHaveBeenCalledTimes(index + 2);
      expect(Date.now() - startedAt).toBeLessThan(100);
      await vi.advanceTimersByTimeAsync(10); // no quiet window between events
    }
    expect(vi.getTimerCount()).toBe(1); // only the named safety net
    const count = s.mesh.tail.mock.calls.length;
    notify("change", "events.jsonl"); await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(count); // duplicate edge, unchanged stamp
    s.monitor.close();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("uses only a >=5 s safety net, with no idle log rereads over 60 s", async () => {
    const s = setup();
    s.mesh.tail.mockReturnValue({ events: [], nextOffset: 10 });
    const interval = vi.spyOn(globalThis, "setInterval");
    s.monitor.start(); await flush();
    expect(interval).toHaveBeenCalledTimes(1);
    expect(interval.mock.calls[0]![1]).toBe(5_000);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(s.beforePoll).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(55_001);
    expect(s.beforePoll).toHaveBeenCalledTimes(13); // startup + 12 recovery sweeps
    expect(s.mesh.tail).toHaveBeenCalledOnce();
    expect(s.mesh.latestOffset).toHaveBeenCalledOnce(); // no log-derived stamp
  });

  it.skipIf(process.platform === "win32")("checks stamps before noisy notifications and detects same-size rewrites and generation changes", async () => {
    const s = setup();
    fs.writeFileSync(path.join(s.root, "events.jsonl"), "old");
    s.monitor.start(); await flush();
    const notify = vi.mocked(fs.watch).mock.calls[0]!.at(-1) as fs.WatchListener<string>;
    s.mesh.tail.mockClear(); s.beforePoll.mockClear();
    for (let index = 0; index < 20; index++) { notify("change", null); await flush(); }
    expect(s.mesh.tail).not.toHaveBeenCalled();
    expect(s.beforePoll).not.toHaveBeenCalled();
    fs.writeFileSync(path.join(s.root, "replacement"), "new");
    fs.renameSync(path.join(s.root, "replacement"), path.join(s.root, "events.jsonl"));
    notify("rename", "events.jsonl"); await flush();
    expect(s.mesh.tail).toHaveBeenCalledOnce();
    fs.writeFileSync(path.join(s.root, "generation"), "1");
    notify("change", "generation"); await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    notify("change", "registry.json"); await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
  });

  it("uses the same slow safety net on Windows while explicit delivery wakes remain immediate", async () => {
    const platform = Object.getOwnPropertyDescriptor(process, "platform")!;
    Object.defineProperty(process, "platform", { ...platform, value: "win32" });
    try {
      const s = setup(); s.monitor.start(); await flush();
      expect(fs.watch).not.toHaveBeenCalled();
      fs.appendFileSync(path.join(s.root, "events.jsonl"), "changed\n");
      await vi.advanceTimersByTimeAsync(4_999);
      expect(s.mesh.tail).toHaveBeenCalledOnce();
      await vi.advanceTimersByTimeAsync(1);
      expect(s.mesh.tail).toHaveBeenCalledTimes(2);
      s.monitor.schedule(); await flush();
      expect(s.mesh.tail).toHaveBeenCalledTimes(3);
      await vi.advanceTimersByTimeAsync(5_000);
      expect(s.mesh.tail).toHaveBeenCalledTimes(3);
    } finally { Object.defineProperty(process, "platform", platform); }
  });

  it("does not treat unreadable metadata as an unchanged mesh", async () => {
    const s = setup(); s.monitor.start(); await flush();
    const stat = vi.spyOn(fs, "statSync").mockImplementation(() => { throw new Error("metadata unavailable"); });
    try {
      await vi.advanceTimersByTimeAsync(10_000);
      expect(s.mesh.tail).toHaveBeenCalledTimes(3);
    } finally { stat.mockRestore(); }
  });

  it("retains unread work for safety-net recovery even when the log stamp is unchanged", async () => {
    const s = setup();
    s.mesh.tail.mockReturnValue({ events: [{ topic: "fleet.wait" } as MeshEvent], nextOffset: 20 });
    s.onEvent.mockReturnValue(false);
    s.monitor.start(); await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(10);
    s.onEvent.mockReturnValue(true);
    await vi.advanceTimersByTimeAsync(4_999);
    expect(s.onEvent).toHaveBeenCalledOnce();
    await vi.advanceTimersByTimeAsync(1);
    expect(s.onEvent).toHaveBeenCalledTimes(2);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(20);
  });

  it("retains explicit queue-capacity wakes received inside dispatch", async () => {
    const s = setup();
    s.onEvent.mockImplementationOnce(() => { s.monitor.schedule(); });
    s.monitor.start(); await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
  });

  it("drains full live pages without another watch edge, yielding between pages", async () => {
    const s = setup();
    const events = Array.from({ length: 17 }, (_, index) => ({ topic: `page-${index}` } as MeshEvent));
    s.mesh.tail.mockImplementation((offset, limit) => {
      const page = events.slice(offset - 10, offset - 10 + limit);
      return { events: page, nextOffset: offset + page.length };
    });
    s.monitor.start();
    await vi.advanceTimersByTimeAsync(1);
    expect(s.onEvent.mock.calls.map(([event]) => event)).toEqual(events);
    expect(s.mesh.tail).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(s.mesh.tail).toHaveBeenCalledTimes(3);
  });

  it("flushes an ignored-only checkpoint at the safety net without rereading an unchanged log", async () => {
    const s = setup();
    s.mesh.tail.mockReturnValue({ events: [], nextOffset: 10 });
    s.monitor.start(); await flush();
    const writes = vi.spyOn(fs, "renameSync");
    s.onEvent.mockReturnValue("ignored");
    s.mesh.tail.mockReturnValue({ events: [{ topic: "noise" } as MeshEvent], nextOffset: 20 });
    s.monitor.schedule(); await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(10);
    await vi.advanceTimersByTimeAsync(10_000);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(20);
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(1);
  });

  it("3864 holds the event boundary when the lease is lost during dispatch", async () => {
    const s = setup('{"format":1,"cursor":3}');
    let leased = true;
    Object.assign(s.monitor.callbacks, { canConsumeMesh: () => leased });
    s.onEvent.mockImplementation(() => { leased = false; });
    s.monitor.start(); await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(3);
    leased = true;
    s.onEvent.mockImplementation(() => true);
    s.monitor.schedule(); await flush();
    expect(s.onEvent).toHaveBeenCalledTimes(2); // owner deduplication handles replay
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(20);
  });
  it("does not persist a cursor when disabled or never started", () => {
    const disabled = setup();
    disabled.monitor.config.enabled = false;
    disabled.monitor.start();
    disabled.monitor.close();
    expect(fs.existsSync(disabled.cursorPath)).toBe(false);
    const unstarted = setup();
    unstarted.monitor.close();
    expect(fs.existsSync(unstarted.cursorPath)).toBe(false);
  });

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
    fs.appendFileSync(path.join(s.root, "events.jsonl"), "changed\n");
    await vi.advanceTimersByTimeAsync(4_999);
    expect(s.mesh.tail).toHaveBeenCalledTimes(count);
    await vi.advanceTimersByTimeAsync(1);
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

  it("uses the slow safety net when watch creation fails and ignores malformed cursors", async () => {
    const s = setup('{"format":2,"cursor":3}');
    vi.mocked(fs.watch).mockImplementation(() => { throw new Error("unsupported"); });
    s.monitor.start();
    s.monitor.start();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledExactlyOnceWith(10, 7);
    fs.appendFileSync(path.join(s.root, "events.jsonl"), "changed\n");
    await vi.advanceTimersByTimeAsync(4_999);
    expect(s.mesh.tail).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(5_000);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(1);
  });

  it("idle checkpoints seed once and never rewrite an unchanged cursor", async () => {
    const s = setup();
    s.mesh.tail.mockReturnValue({ events: [], nextOffset: 10 });
    const writes = vi.spyOn(fs, "renameSync");
    s.monitor.schedule();
    await flush();
    for (let index = 0; index < 100; index++) {
      s.monitor.schedule();
      await flush();
    }
    await vi.advanceTimersByTimeAsync(30_000);
    s.monitor.schedule();
    await flush();
    s.monitor.close();
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(10);
  });

  it("idle checkpoints bound unrelated append writes to ten seconds and flush on close", async () => {
    const s = setup('{"format":1,"cursor":3}');
    const start = Date.now();
    s.onEvent.mockReturnValue("ignored");
    s.mesh.tail.mockImplementation((offset: number) => ({ events: [{ topic: "other" } as MeshEvent], nextOffset: offset + 1 }));
    const writes = vi.spyOn(fs, "renameSync");
    s.monitor.start();
    for (let index = 0; index < 100; index++) {
      s.monitor.schedule();
      await flush();
    }
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(0);
    vi.setSystemTime(start + 9_999);
    s.monitor.schedule();
    await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(3);
    vi.setSystemTime(start + 10_000);
    s.monitor.schedule();
    await flush();
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(105);
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(103);
    s.monitor.close();
    s.monitor.close();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(106);
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(2);
  });

  it("idle checkpoints persist relevant delivery immediately and leave rejected work for restart", async () => {
    const s = setup('{"format":1,"cursor":3}');
    const events = [
      { id: "noise", sequence: 1, topic: "other", createdAt: Date.now() },
      { id: "work", sequence: 2, topic: "fleet.work.a", to: "actor:a", createdAt: Date.now() },
      { id: "held", sequence: 3, topic: "fleet.work.a", to: "actor:a", createdAt: Date.now() },
    ] as MeshEvent[];
    s.mesh.tail.mockReturnValue({ events, nextOffset: 30, cursors: [10, 20, 30] });
    s.onEvent.mockImplementation((event: MeshEvent) => event.id === "noise" ? "ignored" : event.id !== "held");
    s.monitor.schedule();
    await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8"))).toEqual({ format: 1, cursor: 20, last: { sequence: 2, id: "work" } });
    s.monitor.close();
    const seen: MeshEvent[] = [];
    const tail = vi.fn(() => ({ events: [events[2]!], nextOffset: 30, cursors: [30] }));
    const resumed = new ActorMeshMonitor({ root: s.root, latestOffset: () => 30, tail },
      { enabled: true, actorPollMs: 50, maxReadEvents: 7 }, {
        cursorPath: s.cursorPath, maxReplayAgeMs: -60_000, beforePoll: () => true, onEvent: (event) => { seen.push(event); return true; },
      });
    monitors.push(resumed);
    resumed.schedule();
    await flush();
    expect(tail).toHaveBeenCalledWith(20, 7);
    expect(seen).toEqual([events[2]]);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).last).toEqual({ sequence: 3, id: "held" });
  });

  it("idle checkpoints reconcile archived work across rewrite and full-receiver retry", async () => {
    const s = setup('{"format":1,"cursor":0,"last":{"sequence":1,"id":"seen"}}');
    s.monitor.close();
    const events = [2, 3, 4, 5].map((sequence) => ({
      id: `e${sequence}`, sequence, topic: sequence % 2 ? "fleet.work.a" : "team.noise", createdAt: 0,
    })) as MeshEvent[];
    const generationStart = 2 ** 32;
    const tail = vi.fn((cursor: number) => cursor === generationStart + 20
      ? { events: [], nextOffset: cursor }
      : { events: events.slice(2), nextOffset: generationStart + 20, cursors: [generationStart + 10, generationStart + 20] });
    const read = vi.fn((input: { after?: number }) => events.filter((event) => event.sequence > (input.after ?? 0)));
    let full = true;
    const seen: string[] = [];
    const onEvent = vi.fn((event: MeshEvent) => {
      if (full) return false;
      seen.push(event.id);
      return true;
    });
    const monitor = new ActorMeshMonitor({ root: s.root, latestOffset: () => generationStart + 20,
      oldestSequence: () => 4, read, tail }, { enabled: true, actorPollMs: 50, maxReadEvents: 7 }, {
      cursorPath: s.cursorPath, maxReplayAgeMs: 60_000, beforePoll: () => true, onEvent,
    });
    monitors.push(monitor);
    const writes = vi.spyOn(fs, "renameSync");
    monitor.schedule();
    await flush();
    expect(onEvent.mock.calls.map(([event]) => event.id)).toEqual(["e3"]);
    expect(tail).not.toHaveBeenCalled();
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(0);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).last.sequence).toBe(1);
    full = false;
    monitor.schedule();
    await vi.advanceTimersByTimeAsync(0);
    expect(seen).toEqual(["e3", "e5"]); // old noise skipped, old work retained across both sources
    expect(onEvent.mock.calls.map(([event]) => event.id)).toEqual(["e3", "e3", "e5"]);
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8"))).toEqual({
      format: 1, cursor: generationStart + 20, last: { sequence: 5, id: "e5" },
    });
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(2);
    monitor.close();
    expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(2);
  });

  it("idle checkpoints flush only safe progress when a later live dispatch throws", async () => {
    const s = setup('{"format":1,"cursor":3}');
    s.onEvent.mockReturnValue("ignored");
    s.monitor.start();
    await flush();
    const failed = { id: "failed", sequence: 1, topic: "fleet.work.a", createdAt: Date.now() } as MeshEvent;
    s.mesh.tail.mockReturnValue({ events: [failed], nextOffset: 30, cursors: [30] });
    s.onEvent.mockImplementationOnce(() => { throw new Error("dispatch"); });
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    expect(s.onEvent).toHaveBeenCalledTimes(2);
    // Model a fallback poll that would be empty at the wrongly advanced offset.
    s.mesh.tail.mockImplementation((offset: number) => offset >= 30
      ? { events: [], nextOffset: 30 }
      : { events: [failed], nextOffset: 30, cursors: [30] });
    s.onEvent.mockImplementation(() => { throw new Error("dispatch still fails"); });
    s.monitor.schedule();
    await flush();
    s.monitor.close();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(20);
    const offered: string[] = [];
    const resumed = new ActorMeshMonitor(s.mesh, s.monitor.config, {
      cursorPath: s.cursorPath, beforePoll: () => true, onEvent: (event) => { offered.push(event.id); },
    });
    monitors.push(resumed);
    resumed.schedule();
    await flush();
    expect(offered).toEqual(["failed"]);
  });

  it("retains only the same-page consumed prefix when a later dispatch throws", async () => {
    const s = setup('{"format":1,"cursor":3,"last":{"sequence":0,"id":""}}');
    const events = [1, 2, 3].map((sequence) => ({
      id: `e${sequence}`, sequence, topic: "fleet.work.a", createdAt: Date.now(),
    })) as MeshEvent[];
    s.mesh.tail.mockImplementation((offset: number) => {
      const index = offset === 3 ? 0 : offset / 10;
      return { events: events.slice(index), nextOffset: 30, cursors: [10, 20, 30].slice(index) };
    });
    s.onEvent.mockImplementation((event: MeshEvent) => {
      if (event.sequence === 2) throw new Error("dispatch");
      return true;
    });
    s.monitor.start();
    await flush();
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenLastCalledWith(10, 7);
    expect(s.onEvent.mock.calls.map(([event]) => event.id)).toEqual(["e1", "e2", "e2"]);
    s.monitor.close();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8"))).toEqual({ format: 1, cursor: 10, last: { sequence: 1, id: "e1" } });
    const offered: string[] = [];
    const resumed = new ActorMeshMonitor(s.mesh, s.monitor.config, {
      cursorPath: s.cursorPath, beforePoll: () => true, onEvent: (event) => { offered.push(event.id); },
    });
    monitors.push(resumed);
    resumed.schedule();
    await flush();
    expect(offered).toEqual(["e2", "e3"]);
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
    expect(s.mesh.tail).toHaveBeenLastCalledWith(3, 7);
    expect(s.onEvent).toHaveBeenCalledTimes(2);
  });
});

describe("ActorMeshMonitor unanchored crash recovery", () => {
  for (const mode of ["empty seed", "existing tail seed", "first ignored progress", "legacy lastless"] as const) {
    it(`recovers archived targeted work after ${mode}, without a later-sequence seed`, async () => {
      vi.useFakeTimers({ now: 1_000_000 });
      const base = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-anchor-"));
      roots.push(base);
      const root = path.join(base, "mesh");
      const dir = path.join(base, "archive");
      fs.mkdirSync(root);
      fs.mkdirSync(dir);
      fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir }));
      vi.spyOn(fs, "watch").mockReturnValue(Object.assign(new EventEmitter(), { close: vi.fn() }) as unknown as FSWatcher);
      const mesh = new MeshStore(root, 4_096, 50);
      const from = { id: "peer", name: "peer", kind: "actor" as const };
      const cursorPath = path.join(base, "cursor.json");
      const historical = mode === "existing tail seed" || mode === "legacy lastless"
        ? await mesh.publish({ topic: mode === "existing tail seed" ? "fleet.work.a" : "team.noise", from, text: "before first start" })
        : undefined;
      if (mode === "legacy lastless") fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: mesh.latestOffset() }));
      const before = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 60_000, maxReadEvents: 50 }, {
        cursorPath, beforePoll: () => true, onEvent: () => "ignored",
      });
      monitors.push(before);
      // Leave a legacy lastless cursor untouched until the restart after compaction.
      if (mode !== "legacy lastless") {
        before.start();
        await flush();
      }
      if (mode === "first ignored progress") {
        const ignored = await mesh.publish({ topic: "team.noise", from });
        before.schedule();
        await flush();
        // The first actual event anchor must not wait for the ten-second checkpoint.
        expect(JSON.parse(fs.readFileSync(cursorPath, "utf8")).last).toEqual({ sequence: ignored.sequence, id: ignored.id });
      }
      const crashCursor = fs.readFileSync(cursorPath, "utf8");
      before.close();
      fs.writeFileSync(cursorPath, crashCursor); // Restore the bytes a crash (no close flush) leaves.
      const noise = await mesh.publish({ topic: "team.noise", from, text: "old ordinary" });
      const target = await mesh.publish({ topic: "fleet.work.a", to: "actor:a", from, text: "archived target" });
      const retained = await mesh.publish({ topic: "team.noise", from, text: "retained old ordinary" });
      const live = path.join(root, "events.jsonl");
      const suffix = fs.readFileSync(live, "utf8").split("\n").filter((line) => line && JSON.parse(line).sequence >= retained.sequence);
      fs.writeFileSync(`${live}.tmp`, suffix.join("\n") + "\n");
      fs.renameSync(`${live}.tmp`, live);
      fs.writeFileSync(path.join(root, "generation"), "1");
      const seen: MeshEvent[] = [];
      const resumed = new ActorMeshMonitor(mesh, before.config, {
        cursorPath, maxReplayAgeMs: -60_000, beforePoll: () => true, onEvent: (event) => { seen.push(event); },
      });
      monitors.push(resumed);
      resumed.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(seen.map((event) => event.id)).toEqual([target.id]);
      expect(seen.some((event) => event.id === noise.id || event.id === historical?.id)).toBe(false);
    });
  }
});

describe("ActorManager idle checkpoints", () => {
  const fixture = async (filtered: boolean, actorCount = 1) => {
    vi.useFakeTimers();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-manager-cursor-"));
    roots.push(root);
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    // fs.watch attaches its callback as a change listener; returning an emitter alone
    // silently drops every notification in this fixture.
    vi.spyOn(fs, "watch").mockImplementation((_root: fs.PathLike, ...args: unknown[]) => {
      const listener = args.at(-1);
      if (typeof listener === "function") watcher.on("change", listener as fs.WatchListener<string>);
      return watcher as unknown as FSWatcher;
    });
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const reads = vi.spyOn(mesh, "tail");
    const agents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, "runs") });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let firstRunStarted!: () => void;
    let allRunsStarted!: () => void;
    const firstRun = new Promise<void>((resolve) => { firstRunStarted = resolve; });
    const allRuns = new Promise<void>((resolve) => { allRunsStarted = resolve; });
    let runs = 0;
    const run = vi.spyOn(agents, "run").mockImplementation(async () => {
      if (++runs === 1) firstRunStarted();
      if (runs === 11) allRunsStarted();
      await gate;
      throw new Error("test activation ended");
    });
    const identity = { id: "session:test", name: "main", kind: "main" as const, sessionId: "test" };
    const cursorPath = path.join(root, "cursor.json");
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 50, actorQueueLimit: 1 },
      agents, () => {}, { actorRoot: path.join(root, "actors"), meshCursorPath: cursorPath });
    const actor = await actors.create({ name: "receiver", instructions: "Watch", topics: ["fleet.work.wanted"],
      responseMode: "text", coalesce: false,
      ...(filtered ? { activationFilter: [{ id: "skip", topic: ["fleet.work.wanted"], kind: ["skip"] }] } : {}),
    });
    for (let index = 1; index < actorCount; index++) {
      await actors.create({ name: `idle-${index}`, instructions: "Observe", topics: [], responseMode: "text" });
    }
    await vi.advanceTimersByTimeAsync(0);
    const poll = async () => {
      watcher.emit("change", "change", "events.jsonl");
      // Unchanged notifications intentionally do not call tail. Await the owned
      // microtasks, not a promise that only a needless log reread could resolve.
      await vi.advanceTimersByTimeAsync(0);
    };
    const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as { cursor: number; last?: { sequence: number; id: string } };
    const from = { id: "session:peer", name: "main", kind: "main" as const };
    const notify = () => watcher.emit("change", "change", "events.jsonl");
    return { mesh, actors, agents, actor, run, reads, firstRun, allRuns, release, cursorPath, cursor, from, poll, notify };
  };

  it("keeps 20 idle actors free of log rescans for 60 s and wakes a delivered actor within 100 ms", async () => {
    let monitor!: ActorMeshMonitor;
    const start = ActorMeshMonitor.prototype.start;
    vi.spyOn(ActorMeshMonitor.prototype, "start").mockImplementation(function (this: ActorMeshMonitor) {
      monitor = this;
      return start.call(this);
    });
    const s = await fixture(false, 20);
    try {
      await s.poll();
      expect(s.actors.list()).toHaveLength(20);
      expect(s.actors.list().every(actor => actor.status === "idle")).toBe(true);
      const maintenance = vi.spyOn(monitor.callbacks, "beforePoll");
      s.reads.mockClear();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(s.reads).not.toHaveBeenCalled();
      expect(maintenance).toHaveBeenCalledTimes(12);
      expect(s.run).not.toHaveBeenCalled();
      await s.mesh.publish({ topic: "fleet.work.wanted", to: s.actor.id, from: s.from });
      const startedAt = Date.now();
      await s.poll();
      // Queue acceptance and preparation entry are the actor wake, not the
      // downstream async model-admission/presence commit.
      expect(s.actors.status(s.actor.id).status).toBe("preparing");
      expect(Date.now() - startedAt).toBeLessThan(100);
      expect(s.reads).toHaveBeenCalled();
    } finally {
      s.release();
      const closing = s.actors.close();
      await vi.advanceTimersByTimeAsync(1);
      await closing;
      await s.agents.close();
    }
  });

  it("real manager ignores unrelated appends but immediately checkpoints a relevant skip and direct event", async () => {
    const s = await fixture(true);
    const writes = vi.spyOn(fs, "renameSync");
    const initial = s.cursor();
    try {
      let firstIgnored!: MeshEvent;
      for (let index = 0; index < 40; index++) {
        const event = await s.mesh.publish({ topic: index % 2 ? "fabric.control.noise" : "fleet.work.other", to: "actor:elsewhere", from: s.from });
        if (index === 0) firstIgnored = event;
        if (index === 0) await s.poll();
        else s.notify(); // event-driven burst; no fixed watch window
      }
      await s.poll();
      // An empty seed is safe at sequence zero; the first real ignored event anchors
      // immediately, then the remaining unrelated burst stays batched.
      expect(initial.last).toEqual({ sequence: 0, id: "" });
      expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(1);
      // Actor creation may have reserved an earlier sequence; assert the actual
      // handed-on anchor rather than assuming this event is sequence one.
      expect(s.cursor().last).toEqual({ sequence: firstIgnored.sequence, id: firstIgnored.id });
      const skipped = await s.mesh.publish({ topic: "fleet.work.wanted", kind: "skip", from: s.from });
      await s.poll();
      expect(s.actors.status(s.actor.id).filteredCount).toBe(1);
      expect(s.cursor().last).toEqual({ sequence: skipped.sequence, id: skipped.id });
      expect(s.run).not.toHaveBeenCalled();
      const directed = await s.mesh.publish({ topic: "team.direct", to: s.actor.id, from: s.from });
      await s.poll();
      await s.firstRun;
      expect(s.run).toHaveBeenCalledOnce();
      expect(s.cursor().last).toEqual({ sequence: directed.sequence, id: directed.id });
      expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(3);
    } finally {
      s.release();
      const closing = s.actors.close();
      await vi.advanceTimersByTimeAsync(1); // Join the deferred setImmediate retention slice.
      await closing;
      await s.agents.close();
    }
  });

  it("real manager retries work rejected by a full receiver without rewriting an unchanged cursor", async () => {
    const s = await fixture(false);
    const writes = vi.spyOn(fs, "renameSync");
    try {
      // One running activation, one queued, eight overflow: the eleventh waits in the mesh.
      const events: MeshEvent[] = [];
      for (let index = 0; index < 11; index++) {
        events.push(await s.mesh.publish({ topic: "fleet.work.wanted", from: s.from, text: `${index}` }));
        await s.poll();
        if (index === 0) await s.firstRun;
      }
      expect(s.cursor().last).toEqual({ sequence: events[9]!.sequence, id: events[9]!.id });
      const count = writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath).length;
      await s.poll();
      await s.poll();
      expect(writes.mock.calls.filter(([, target]) => String(target) === s.cursorPath)).toHaveLength(count);
      expect(s.run).toHaveBeenCalledOnce();
      s.release();
      await s.allRuns;
      await s.poll();
      expect(s.cursor().last).toEqual({ sequence: events[10]!.sequence, id: events[10]!.id });
      expect(s.run).toHaveBeenCalledTimes(11);
      expect(s.run.mock.calls.map(([request]) =>
        JSON.parse(request.task.slice(request.task.indexOf("\n\n") + 2)).payload.id,
      )).toEqual(events.map((event) => event.id));
    } finally {
      s.release();
      const closing = s.actors.close();
      await vi.advanceTimersByTimeAsync(1); // Join the deferred setImmediate retention slice.
      await closing;
      await s.agents.close();
    }
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

// review/astra round 2 on pi-fabric#97.
describe("ActorMeshMonitor work-topic reconciliation, round 2", () => {
  const from = { id: "session:peer", name: "main", kind: "main" as const, sessionId: "peer" };
  const archivedStore = (maxEventLogBytes = 6_000, retainedEventLogBytes = 1_500) => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-r2-"));
    roots.push(base);
    const root = path.join(base, "mesh");
    const dir = path.join(base, "archive");
    fs.mkdirSync(root, { recursive: true });
    fs.mkdirSync(dir);
    fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir }));
    return { mesh: new MeshStore(root, 4_096, 500, { maxEventLogBytes, retainedEventLogBytes }), base, cursorPath: path.join(base, "cursor.json") };
  };

  // F2: a rewrite between the archive read and the tail read left a gap.
  it("closes a gap that a rewrite opens while it reads the archive", async () => {
    const { mesh, cursorPath } = archivedStore();
    const seen: MeshEvent[] = [];
    const first = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 50 }, { cursorPath, beforePoll: () => true, onEvent: (event) => { seen.push(event); } });
    monitors.push(first);
    await mesh.publish({ topic: "fleet.work.a", from, text: "w0" });
    for (let index = 0; index < 5; index++) { first.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); }
    first.close();
    let n = 1;
    const burst = async (count: number) => { for (let index = 0; index < count; index++, n++) await mesh.publish({ topic: n % 3 === 0 ? "fleet.work.a" : "team.noise", from, text: `${n % 3 === 0 ? "w" : "x"}${n}` }); };
    await burst(40);                                             // the first rewrite, while away
    const read = mesh.read.bind(mesh);
    let rewrites = 0;
    vi.spyOn(mesh, "read").mockImplementation((input) => {
      const page = read(input);
      if (rewrites++ === 0) void burst(40);                     // another rewrite during the archive read
      return page;
    });
    const later: MeshEvent[] = [];
    const again = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 50 }, { cursorPath, maxReplayAgeMs: 600_000, beforePoll: () => true, onEvent: (event) => { later.push(event); } });
    monitors.push(again);
    for (let index = 0; index < 60; index++) { again.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); }
    const expected = Array.from({ length: n - 1 }, (_, index) => index + 1).filter((value) => value % 3 === 0).map((value) => `w${value}`);
    expect(later.filter((event) => event.topic.startsWith("fleet.")).map((event) => event.text)).toEqual(expected);
    expect(new Set(later.map((event) => event.id)).size).toBe(later.length);
  });

  // F3: archive catch-up yields between pages.
  it("yields to the event loop between archive pages", async () => {
    const { mesh, cursorPath } = archivedStore(40_000, 4_000);
    const seen: MeshEvent[] = [];
    const first = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 20 }, { cursorPath, beforePoll: () => true, onEvent: (event) => { seen.push(event); } });
    monitors.push(first);
    await mesh.publish({ topic: "fleet.work.a", from, text: "w0" });
    for (let index = 0; index < 5; index++) { first.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); }
    first.close();
    for (let index = 1; index <= 400; index++) await mesh.publish({ topic: index % 50 === 0 ? "fleet.work.a" : "team.noise", from, text: `e${index}` });
    expect(mesh.oldestSequence()).toBeGreaterThan(100);
    let ticks = 0;
    const heartbeat = setInterval(() => { ticks++; }, 1);
    const ticksAtRead: number[] = [];
    const read = mesh.read.bind(mesh);
    vi.spyOn(mesh, "read").mockImplementation((input) => { ticksAtRead.push(ticks); return read(input); });
    const later: MeshEvent[] = [];
    // smarty-dev#883: wait for the eighth work event itself, not a wall-time poll, so a slow
    // Windows runner only takes longer.
    let eighth!: () => void;
    const delivered = new Promise<void>((resolve) => { eighth = resolve; });
    const again = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 60_000, maxReadEvents: 20 }, { cursorPath, maxReplayAgeMs: 600_000, beforePoll: () => true, onEvent: (event) => {
      later.push(event);
      if (later.filter((seen) => seen.topic.startsWith("fleet.")).length === 8) eighth();
    } });
    monitors.push(again);
    try {
      again.schedule();
      await delivered;
      expect(later.filter((event) => event.topic.startsWith("fleet.")).length).toBe(8);
    } finally {
      clearInterval(heartbeat);
    }
    expect(ticksAtRead.length).toBeGreaterThan(3);
    expect(ticksAtRead.at(-1)!).toBeGreaterThan(ticksAtRead[0]!);          // timers ran between pages
  }, 60_000);
});

// review/astra round 3 on pi-fabric#97: the generation file and the events file are separate
// reads, and a rewrite renames the file before it bumps the generation.
describe("ActorMeshMonitor archive/live handoff across a rewrite's two writes", () => {
  const from = { id: "session:peer", name: "main", kind: "main" as const, sessionId: "peer" };
  const setup = async (archived = true) => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-r3-"));
    roots.push(base);
    const root = path.join(base, "mesh");
    fs.mkdirSync(root, { recursive: true });
    fs.mkdirSync(path.join(base, "archive"));
    if (archived) fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir: path.join(base, "archive") }));
    const mesh = new MeshStore(root, 4_096, 500, { maxEventLogBytes: 1_000_000, retainedEventLogBytes: 500_000 });
    const seen: MeshEvent[] = [];
    const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 60_000, maxReadEvents: 3 },
      { cursorPath: path.join(base, "cursor.json"), beforePoll: () => true, onEvent: (event) => { seen.push(event); } });
    monitors.push(monitor);
    const poll = async (times: number) => { for (let index = 0; index < times; index++) { monitor.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); } };
    // Work on odd numbers, noise on even ones, so the monitor's last event may be either. Every
    // line has the same length, so an old file's byte offset lands on a line of the new one.
    const publish = async (low: number, high: number) => {
      for (let n = low; n <= high; n++) await mesh.publish({ topic: n % 2 ? "fleet.work.a" : "team.noise.x", from, text: label(n) });
    };
    const events = path.join(root, "events.jsonl");
    const generation = path.join(root, "generation");
    // What a rewrite leaves: the events file holds only the suffix from `keep`.
    const renameLog = (keep: number) => {
      const lines = fs.readFileSync(events, "utf8").split("\n").filter((line) => line && (JSON.parse(line) as MeshEvent).sequence >= keep);
      fs.writeFileSync(`${events}.tmp`, lines.map((line) => `${line}\n`).join(""));
      fs.renameSync(`${events}.tmp`, events);
    };
    const bumpGeneration = () => {
      const current = fs.existsSync(generation) ? Number(JSON.parse(fs.readFileSync(generation, "utf8"))) : 0;
      fs.writeFileSync(generation, JSON.stringify(current + 1));
    };
    const workTexts = () => seen.filter((event) => event.topic.startsWith("fleet.")).map((event) => event.text);
    return { mesh, seen, poll, publish, renameLog, bumpGeneration, workTexts, events };
  };
  const label = (n: number) => `e${n}`.padEnd(2 * (2 - String(n).length) + String(n).length + 1, "_");
  const odd = (low: number, high: number) => Array.from({ length: high - low + 1 }, (_, index) => low + index).filter((n) => n % 2).map(label);

  it("fills the gap when the file is renamed and the generation not yet bumped", async () => {
    const { mesh, poll, publish, renameLog, bumpGeneration, workTexts, events } = await setup();
    await publish(1, 4);
    await poll(5);
    expect(workTexts()).toEqual(odd(1, 4));
    await publish(5, 14);
    expect(mesh.oldestSequence()).toBe(1);
    const lines = fs.readFileSync(events, "utf8").split("\n").filter(Boolean);
    expect(new Set(lines.map((line) => line.length)).size).toBe(1);
    renameLog(9);                                            // events 5–8 leave the live log
    // The old cursor's byte offset (after event 4) is now the start of event 13's line: a read
    // there skips 9–12, which are in the live log, not the archive.
    await poll(10);                                          // the generation is still the old one
    expect(workTexts()).toEqual(odd(1, 14));
    bumpGeneration();                                        // the rewrite's second write lands
    await publish(15, 16);
    await poll(10);
    expect(workTexts()).toEqual(odd(1, 16));                 // each once, in order
  });

  it("fills the gap when the rewrite lands between the generation read and the file open", async () => {
    const { poll, publish, renameLog, bumpGeneration, workTexts, events } = await setup();
    await publish(1, 4);
    await poll(5);
    await publish(5, 14);
    const open = fs.openSync.bind(fs);
    let armed = true;
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (armed && String(file) === events) {
        armed = false;                                       // tail has read the old generation
        renameLog(9);
        bumpGeneration();
      }
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    await poll(10);
    expect(armed).toBe(false);
    vi.mocked(fs.openSync).mockRestore();
    await publish(15, 16);
    await poll(10);
    expect(workTexts()).toEqual(odd(1, 16));
  });

  // review/astra round 4: a second rewrite lands during the retry read, after the first gap was
  // seen but before the archive covered it. The generation file stays behind throughout.
  it("fills a gap that a second rewrite moves during the retry read", async () => {
    const { mesh, poll, publish, bumpGeneration, workTexts, events } = await setup();
    await publish(1, 4);
    await poll(5);
    await publish(5, 13);
    const all = fs.readFileSync(events, "utf8").split("\n").filter(Boolean);
    // A rewrite renames a new file into place, as the store's compaction does.
    const write = (low: number, high: number) => {
      fs.writeFileSync(`${events}.tmp`, all
        .filter((line) => { const sequence = (JSON.parse(line) as MeshEvent).sequence; return sequence >= low && sequence <= high; })
        .map((line) => `${line}\n`).join(""));
      fs.renameSync(`${events}.tmp`, events);
    };
    const tail = mesh.tail.bind(mesh);
    let call = 0;
    vi.spyOn(mesh, "tail").mockImplementation((cursor, limit) => {
      call++;
      if (call === 1) write(5, 9);       // the old offset (four lines) now points at event 9
      if (call === 2) write(9, 13);      // the second rewrite, during the retry read
      return tail(cursor, limit);
    });
    await poll(12);
    expect(call).toBeGreaterThan(2);
    expect(workTexts()).toEqual(odd(1, 13));                 // 5 and 7 before 9–13
    vi.mocked(mesh.tail).mockRestore();
    bumpGeneration();
    await publish(14, 16);
    await poll(10);
    expect(workTexts()).toEqual(odd(1, 16));
  });

  // review/astra round 6 on pi-fabric#97: the gap lookup itself must not be fooled by a rewrite
  // that lands after it chose where to look and before it opened the live log.
  it("fills the gap when a rewrite lands inside the gap lookup", async () => {
    const { mesh, poll, publish, workTexts, events } = await setup();
    await publish(1, 4);
    await poll(5);
    await publish(5, 13);
    const all = fs.readFileSync(events, "utf8").split("\n").filter(Boolean);
    const write = (low: number, high: number) => {
      fs.writeFileSync(`${events}.tmp`, all
        .filter((line) => { const sequence = (JSON.parse(line) as MeshEvent).sequence; return sequence >= low && sequence <= high; })
        .map((line) => `${line}\n`).join(""));
      fs.renameSync(`${events}.tmp`, events);
    };
    const open = fs.openSync.bind(fs);
    let opensAfterTail = -1;                                   // armed once the stale page is read
    vi.spyOn(fs, "openSync").mockImplementation(((file: fs.PathLike, ...rest: unknown[]) => {
      if (opensAfterTail >= 0 && String(file) === events && ++opensAfterTail === 2) write(9, 13);
      return (open as (...args: unknown[]) => number)(file, ...rest);
    }) as typeof fs.openSync);
    const tail = mesh.tail.bind(mesh);
    let calls = 0;
    vi.spyOn(mesh, "tail").mockImplementation((cursor, limit) => {
      if (++calls === 1) write(5, 9);                          // the old offset now points at event 9
      const page = tail(cursor, limit);
      if (calls === 1) opensAfterTail = 0;
      return page;
    });
    await poll(12);
    vi.mocked(fs.openSync).mockRestore();
    vi.mocked(mesh.tail).mockRestore();
    expect(opensAfterTail).toBeGreaterThanOrEqual(2);
    expect(workTexts()).toEqual(odd(1, 13));                   // 5 and 7 before 9
  });

  // review/astra round 7 on pi-fabric#97: the archive was enabled after a retention cut, so it
  // starts after the monitor's last event. Its events must still count in the gap lookup.
  it("fills the archived part of a gap that starts before the archive", async () => {
    const { mesh, poll, publish, bumpGeneration, workTexts, events } = await setup(false);
    await publish(1, 4);
    await poll(5);
    await publish(5, 12);
    const renameKeeping = (low: number) => {
      const lines = fs.readFileSync(events, "utf8").split("\n").filter((line) => line && (JSON.parse(line) as MeshEvent).sequence >= low);
      fs.writeFileSync(`${events}.tmp`, lines.map((line) => `${line}\n`).join(""));
      fs.renameSync(`${events}.tmp`, events);
    };
    renameKeeping(9);                                         // a retention cut while no archive was set:
    bumpGeneration();                                         // 5–8 are gone for good
    const archiveDir = path.join(path.dirname(events), "..", "archive");
    fs.writeFileSync(path.join(path.dirname(events), "event-archive.json"), JSON.stringify({ version: 1, dir: path.resolve(archiveDir) }));
    await publish(13, 17);                                    // the archive backfills from 9
    expect(mesh.nextEventAfter(4)?.sequence).toBe(9);
    const tail = mesh.tail.bind(mesh);
    let calls = 0;
    vi.spyOn(mesh, "tail").mockImplementation((cursor, limit) => {
      // Call 1 sees the generation change; the archive catch-up then finds nothing below 9.
      // Call 2 reads a log renamed to keep 13–17, before its generation bump.
      if (++calls === 2) renameKeeping(13);
      return tail(cursor, limit);
    });
    await poll(15);
    vi.mocked(mesh.tail).mockRestore();
    expect(calls).toBeGreaterThan(2);
    expect(workTexts()).toEqual([...odd(1, 4), ...odd(9, 17)]);   // 9 and 11 once, before 13
  });
});

describe("ActorMeshMonitor without an archive", () => {
  it("passes a gap it cannot fill once, and does not stall on it", async () => {
    const base = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-r3-noarchive-"));
    roots.push(base);
    const root = path.join(base, "mesh");
    const mesh = new MeshStore(root, 4_096, 500);
    const from = { id: "session:peer", name: "main", kind: "main" as const, sessionId: "peer" };
    const seen: string[] = [];
    const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 60_000, maxReadEvents: 3 },
      { cursorPath: path.join(base, "cursor.json"), beforePoll: () => true, onEvent: (event) => { seen.push(event.text ?? ""); } });
    monitors.push(monitor);
    const poll = async (times: number) => { for (let index = 0; index < times; index++) { monitor.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); } };
    for (let n = 1; n <= 4; n++) await mesh.publish({ topic: "fleet.work.a", from, text: `e${n}` });
    await poll(5);
    for (let n = 5; n <= 14; n++) await mesh.publish({ topic: "fleet.work.a", from, text: `e${n}` });
    const events = path.join(root, "events.jsonl");
    const kept = fs.readFileSync(events, "utf8").split("\n").filter((line) => line && (JSON.parse(line) as MeshEvent).sequence >= 9);
    fs.writeFileSync(`${events}.tmp`, kept.map((line) => `${line}\n`).join(""));
    fs.renameSync(`${events}.tmp`, events);                                  // 5–8 are gone for good
    await poll(12);
    expect(seen).toEqual(["e1", "e2", "e3", "e4", "e9", "e10", "e11", "e12", "e13", "e14"]);
  });
});

// review/astra round 5 (F4) on pi-fabric#97: a failed publish leaves its reserved sequence as a
// hole inside the live log. The stream passes it, with no rewrite, publish or restart after it.
describe("ActorMeshMonitor and a failed publish's sequence hole", () => {
  for (const archived of [true, false]) {
    it(`delivers the events after a hole at a page boundary (${archived ? "with" : "without"} the archive)`, async () => {
      const base = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-f4-"));
      roots.push(base);
      const root = path.join(base, "mesh");
      fs.mkdirSync(root, { recursive: true });
      if (archived) {
        fs.mkdirSync(path.join(base, "archive"));
        fs.writeFileSync(path.join(root, "event-archive.json"), JSON.stringify({ version: 1, dir: path.join(base, "archive") }));
      }
      const mesh = new MeshStore(root, 4_096, 500);
      const from = { id: "session:peer", name: "main", kind: "main" as const, sessionId: "peer" };
      const seen: string[] = [];
      const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 60_000, maxReadEvents: 3 },
        { cursorPath: path.join(base, "cursor.json"), beforePoll: () => true, onEvent: (event) => { seen.push(event.text ?? ""); } });
      monitors.push(monitor);
      const poll = async (times: number) => { for (let index = 0; index < times; index++) { monitor.schedule(); await new Promise((resolve) => setTimeout(resolve, 5)); } };
      for (const text of ["e1", "e2", "e3"]) await mesh.publish({ topic: "fleet.work.a", from, text });
      await poll(5);
      expect(seen).toEqual(["e1", "e2", "e3"]);
      const events = path.join(root, "events.jsonl");
      const append = fs.appendFileSync.bind(fs);
      let fail = true;
      const spy = vi.spyOn(fs, "appendFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, ...rest: unknown[]) => {
        if (fail && String(file) === events) { fail = false; throw new Error("disk full"); }
        return (append as (...args: unknown[]) => void)(file, ...rest);
      }) as typeof fs.appendFileSync);
      await expect(mesh.publish({ topic: "fleet.work.a", from, text: "e4" })).rejects.toThrow("disk full");
      spy.mockRestore();
      await mesh.publish({ topic: "fleet.work.a", from, text: "e5" });
      await mesh.publish({ topic: "fleet.work.a", from, text: "e6" });
      expect(mesh.read({ after: 0 }).map((event) => event.sequence)).toEqual([1, 2, 3, 5, 6]);
      await poll(10);
      expect(seen).toEqual(["e1", "e2", "e3", "e5", "e6"]);
    });
  }
});
