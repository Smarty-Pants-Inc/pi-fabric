import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import { isWatchLimitError, resetWatchFallbackWarnings, watchFallbackMessage } from "../src/core/watch-fallback.js";
import { MeshStore, type MeshEvent, type MeshTailResult } from "../src/mesh/store.js";

// smarty-dev#5247: fs.inotify.max_user_watches cannot be lowered without root, so inject the
// exact error Node raises when inotify_add_watch(2) hits the per-user watch limit.
const watchLimit = (target: string): NodeJS.ErrnoException => Object.assign(
  new Error(`ENOSPC: System limit for number of file watchers reached, watch '${target}'`),
  { code: "ENOSPC", errno: -28, syscall: "watch", path: target },
);
const diskFull = (): NodeJS.ErrnoException => Object.assign(
  new Error("ENOSPC: no space left on device, write"), { code: "ENOSPC", errno: -28, syscall: "write" },
);
const CAUSE = "inotify watch limit, not disk space";

const roots: string[] = [];
const monitors: ActorMeshMonitor[] = [];
let warn: ReturnType<typeof vi.spyOn>;
beforeEach(() => {
  resetWatchFallbackWarnings();
  warn = vi.spyOn(console, "warn").mockImplementation(() => {});
});
afterEach(() => {
  for (const monitor of monitors.splice(0)) monitor.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function tempRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "watch-enospc-"));
  roots.push(root);
  return root;
}

function fakeMonitor(root: string) {
  const event = { topic: "test" } as MeshEvent;
  const mesh = { root, latestOffset: vi.fn(() => 10), tail: vi.fn((_offset: number, _limit: number): MeshTailResult => ({ events: [event], nextOffset: 20 })) };
  const onEvent = vi.fn();
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 }, { cursorPath: path.join(root, "cursor.json"), beforePoll: () => true, onEvent });
  monitors.push(monitor);
  return { mesh, monitor, onEvent };
}

const flush = async () => { await Promise.resolve(); await Promise.resolve(); };
const warnings = () => warn.mock.calls.map((call: unknown[]) => String(call[0]));

describe("watch ENOSPC (smarty-dev#5247)", () => {
  it("classifies only the watch ENOSPC as the inotify limit, never a write ENOSPC", () => {
    expect(isWatchLimitError(watchLimit("/x"))).toBe(true);
    expect(isWatchLimitError(Object.assign(new Error("ENOSPC"), { code: "ENOSPC" }))).toBe(true);
    expect(isWatchLimitError(diskFull())).toBe(false);
    expect(isWatchLimitError(Object.assign(new Error("EACCES"), { code: "EACCES", syscall: "watch" }))).toBe(false);
    expect(isWatchLimitError(undefined)).toBe(false);
    expect(watchFallbackMessage("m", "/x", watchLimit("/x"), 50)).toContain(CAUSE);
    expect(watchFallbackMessage("m", "/x", watchLimit("/x"), 50)).toContain("fs.inotify.max_user_watches");
    expect(watchFallbackMessage("m", "/x", diskFull(), 50)).not.toContain("inotify");
  });

  it.skipIf(process.platform === "win32")("polls after a synchronous fs.watch ENOSPC, warns once with the real cause and never throws", async () => {
    vi.useFakeTimers();
    const root = tempRoot();
    vi.spyOn(fs, "watch").mockImplementation(() => { throw watchLimit(root); });
    const s = fakeMonitor(root);
    expect(() => s.monitor.start()).not.toThrow();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(50);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    expect(s.onEvent).toHaveBeenCalled();
    expect(warnings()).toHaveLength(1);
    expect(warnings()[0]).toContain(CAUSE);
    expect(warnings()[0]).toContain(root);
    expect(warnings()[0]).toContain("polling every 50 ms");
    // A restarted monitor (actor manager reload) does not repeat the warning.
    const again = fakeMonitor(tempRoot());
    again.monitor.start();
    await flush();
    expect(again.mesh.tail).toHaveBeenCalledTimes(1);
    expect(warnings()).toHaveLength(1);
  });

  it.skipIf(process.platform === "win32")("falls back to polling on an asynchronous watcher ENOSPC with one precise warning", async () => {
    vi.useFakeTimers();
    const root = tempRoot();
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    vi.spyOn(fs, "watch").mockReturnValue(watcher as unknown as FSWatcher);
    const s = fakeMonitor(root);
    s.monitor.start();
    await flush();
    expect(warnings()).toHaveLength(0);
    expect(() => watcher.emit("error", watchLimit(root))).not.toThrow();
    watcher.emit("error", watchLimit(root));
    expect(watcher.close).toHaveBeenCalledOnce();
    const count = s.mesh.tail.mock.calls.length;
    await vi.advanceTimersByTimeAsync(50);
    expect(s.mesh.tail.mock.calls.length).toBeGreaterThan(count);
    expect(warnings()).toEqual([expect.stringContaining(CAUSE)]);
  });

  it.skipIf(process.platform === "win32")("keeps every mesh write and still delivers it by polling when the watch limit is hit", async () => {
    const base = tempRoot();
    const root = path.join(base, "mesh");
    fs.mkdirSync(root);
    vi.spyOn(fs, "watch").mockImplementation(() => { throw watchLimit(root); });
    const mesh = new MeshStore(root, 4_096, 50);
    const delivered: string[] = [];
    const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 50 }, {
      cursorPath: path.join(base, "cursor.json"), beforePoll: () => true, onEvent: (event) => { delivered.push(event.text ?? ""); },
    });
    monitors.push(monitor);
    monitor.start();
    const from = { id: "peer", name: "peer", kind: "actor" as const };
    const published = await mesh.publish({ topic: "fleet.work.a", from, text: "survives ENOSPC" });
    expect(fs.readFileSync(path.join(root, "events.jsonl"), "utf8")).toContain(published.id);
    await vi.waitFor(() => expect(delivered).toContain("survives ENOSPC"), { timeout: 2_000, interval: 10 });
    expect(warnings()).toEqual([expect.stringContaining(CAUSE)]);
  });
});
