import fs from "node:fs";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { waitForScopeAdmission } from "../src/scope-admission.js";

afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); });
describe("event-driven scope admission", () => {
  const marker = "/fixture/scope/admitted";
  const setup = () => {
    vi.useFakeTimers();
    let present = false, event!: (name: string | null) => void, close!: () => void;
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    const watch = vi.spyOn(fs, "watch").mockImplementation(((_path: fs.PathLike, _options: unknown, listener: unknown) => {
      event = name => (listener as (...args: unknown[]) => void)("rename", name);
      return watcher as unknown as fs.FSWatcher;
    }) as typeof fs.watch);
    const read = vi.spyOn(fs, "existsSync").mockImplementation(() => present);
    const closed = new Promise<void>(resolve => { close = resolve; });
    return { watch, watcher, read, closed, close, event: (name: string | null = "admitted") => event(name), present: () => { present = true; } };
  };
  it("subscribes to the marker directory before its first read, including creation during subscribe", async () => {
    const f = setup(); const watch = f.watch.getMockImplementation()!;
    f.watch.mockImplementation(((...args: Parameters<typeof fs.watch>) => { f.present(); return watch(...args); }) as typeof fs.watch);
    f.read.mockImplementation(() => { expect(f.watch).toHaveBeenCalledOnce(); return true; });
    expect(await waitForScopeAdmission(marker, f.closed)).toBe(true);
    expect(f.watch).toHaveBeenCalledWith("/fixture/scope", { persistent: false }, expect.any(Function));
    expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("wakes on atomic marker rename and does not poll every 10ms", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    await vi.advanceTimersByTimeAsync(4_999); expect(f.read).toHaveBeenCalledOnce();
    f.present(); f.event(); expect(await wait).toBe(true);
    expect(f.read).toHaveBeenCalledTimes(2); expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("ignores temp-file events, but checks an event with no filename", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    f.event("admitted.tmp"); expect(f.read).toHaveBeenCalledOnce();
    f.present(); f.event(null); expect(await wait).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it("has one deadline and at most one final safety read for a missed marker event", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    await vi.advanceTimersByTimeAsync(4_999); expect(f.read).toHaveBeenCalledOnce(); f.present();
    await vi.advanceTimersByTimeAsync(1); expect(await wait).toBe(true);
    expect(f.read).toHaveBeenCalledTimes(2); expect(vi.getTimerCount()).toBe(0);
  });
  it("times out without admission, closing its watcher and timer", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    await vi.advanceTimersByTimeAsync(5_000); expect(await wait).toBe(false);
    expect(f.read).toHaveBeenCalledTimes(2); expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("fails closed when watch creation throws, without reading or polling the marker", async () => {
    const f = setup(); f.watch.mockImplementation(() => { throw new Error("no watch"); }); f.present();
    expect(await waitForScopeAdmission(marker, f.closed)).toBe(false);
    expect(f.read).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });
  it("fails closed on a watcher error and does not recover by polling a later marker", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    f.watcher.emit("error", new Error("watch lost")); expect(await wait).toBe(false);
    f.present(); f.event(); await vi.advanceTimersByTimeAsync(5_000);
    expect(f.read).toHaveBeenCalledOnce(); expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("wakes on native close, preserving a marker that raced with close", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    f.present(); f.close(); expect(await wait).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it("native close without a marker is non-admission", async () => {
    const f = setup(), wait = waitForScopeAdmission(marker, f.closed);
    f.close(); expect(await wait).toBe(false); expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("wakes on cancellation and removes its abort listener", async () => {
    const f = setup(), controller = new AbortController();
    const remove = vi.spyOn(controller.signal, "removeEventListener");
    const wait = waitForScopeAdmission(marker, f.closed, controller.signal);
    controller.abort(); expect(await wait).toBe(false);
    expect(remove).toHaveBeenCalledWith("abort", expect.any(Function)); expect(f.watcher.close).toHaveBeenCalledOnce(); expect(vi.getTimerCount()).toBe(0);
  });
  it("does not watch or read for an already-cancelled admission", async () => {
    const f = setup(), controller = new AbortController(); controller.abort();
    expect(await waitForScopeAdmission(marker, f.closed, controller.signal)).toBe(false);
    expect(f.watch).not.toHaveBeenCalled(); expect(f.read).not.toHaveBeenCalled(); expect(vi.getTimerCount()).toBe(0);
  });
});
