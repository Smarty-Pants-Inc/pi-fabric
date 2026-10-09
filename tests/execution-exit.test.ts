import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { waitForExecutionExit } from "../src/worker/execution-exit.js";

afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
const child = (): ChildProcess => new EventEmitter() as ChildProcess;

describe("event-driven finishing exit observation", () => {
  it.each(["exit", "close"])("wakes on native %s, with one deadline and no 20 ms or interval timers", async event => {
    vi.useFakeTimers();
    const timeout = vi.spyOn(globalThis, "setTimeout");
    const interval = vi.spyOn(globalThis, "setInterval");
    const execution = child();
    let gone = false;
    const probe = vi.fn(() => gone);
    const wait = waitForExecutionExit(execution, probe, Date.now() + 5000);
    expect(vi.getTimerCount()).toBe(1);
    expect(timeout.mock.calls.map(call => call[1])).toEqual([5000]);
    expect(interval).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(4000);
    expect(probe).toHaveBeenCalledTimes(1); // no periodic process membership census
    gone = true;
    execution.emit(event, 0);
    await expect(wait).resolves.toBe(true);
    expect(vi.getTimerCount()).toBe(0);
    expect(execution.listenerCount("exit")).toBe(0);
    expect(execution.listenerCount("close")).toBe(0);
  });

  it("wakes on cgroup.events populated 0 after the native child closed", async () => {
    vi.useFakeTimers();
    const execution = child();
    let notify!: () => void;
    let populated = true;
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    vi.spyOn(fs, "watch").mockImplementation((_file, listener) => {
      notify = listener as () => void;
      return watcher as unknown as fs.FSWatcher;
    });
    vi.spyOn(fs, "readFileSync").mockImplementation(() => `populated ${populated ? 1 : 0}\nfrozen 0\n`);
    const wait = waitForExecutionExit(execution, () => !populated, Date.now() + 5000, "/scope/cgroup.events");
    execution.emit("close", 0); // close is not tree exit
    notify();
    expect(watcher.close).not.toHaveBeenCalled();
    populated = false;
    notify();
    await expect(wait).resolves.toBe(true);
    expect(watcher.close).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("falls back to a single deadline snapshot when descendants have no notification", async () => {
    vi.useFakeTimers();
    const probe = vi.fn(() => false);
    const wait = waitForExecutionExit(child(), probe, Date.now() + 5000);
    await vi.advanceTimersByTimeAsync(4999);
    expect(probe).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    await expect(wait).resolves.toBe(false);
    expect(probe).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("uses the original absolute deadline after late native close, and wakes on one-shot cleanup", async () => {
    vi.useFakeTimers();
    const deadline = Date.now() + 5000;
    await vi.advanceTimersByTimeAsync(4500);
    const timeout = vi.spyOn(globalThis, "setTimeout");
    let notify!: () => void;
    let gone = false;
    const unsubscribe = vi.fn();
    const wait = waitForExecutionExit(child(), () => gone, deadline, undefined, changed => {
      notify = changed;
      return unsubscribe;
    });
    expect(timeout.mock.calls.map(call => call[1])).toEqual([500]);
    gone = true;
    notify();
    await expect(wait).resolves.toBe(true);
    expect(unsubscribe).toHaveBeenCalledOnce();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("never turns unreadable membership into a successful exit receipt", async () => {
    vi.useFakeTimers();
    const execution = child();
    let unknown = false;
    const wait = waitForExecutionExit(execution, () => {
      if (unknown) throw new Error("custody unreadable");
      return false;
    }, Date.now() + 5000);
    const result = expect(wait).rejects.toThrow("custody unreadable");
    unknown = true;
    execution.emit("close", 0);
    await result;
    expect(vi.getTimerCount()).toBe(0);
  });
});
