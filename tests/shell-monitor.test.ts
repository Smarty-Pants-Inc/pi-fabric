import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ShellMonitor, parseShellMonitor } from "../src/core/shell-monitor.js";

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());
const options = () => parseShellMonitor({ delivery: "wake", intervalMs: 1000 })!;

describe("shell monitor", () => {
  it("requires explicit delivery and enforces a finite bounded lifetime", () => {
    expect(parseShellMonitor(undefined)).toBeUndefined();
    expect(parseShellMonitor({ delivery: "ui" })).toEqual({ delivery: "ui", timeoutMs: 300000, intervalMs: 5000 });
    for (const bad of [null, {}, { delivery: "steer" }, { delivery: "ui", timeoutMs: 0 }, { delivery: "ui", timeoutMs: 1800001 }, { delivery: "wake", intervalMs: 999 }, { delivery: "wake", match: "" }, { delivery: "ui", unknown: true }]) expect(() => parseShellMonitor(bad)).toThrow();
  });

  it("frames split UTF-8 and lines, filters literally, and suppresses adjacent duplicates", async () => {
    const emit = vi.fn();
    const monitor = new ShellMonitor({ ...options(), match: "CI:" }, emit);
    const data = Buffer.from("CI: 🚀 ready\n");
    monitor.append(data.subarray(0, 6));
    monitor.append(data.subarray(6));
    monitor.append(Buffer.from("ignored\nCI: 🚀 ready\nCI: passed"));
    expect(emit).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1000);
    expect(emit).toHaveBeenCalledExactlyOnceWith({ lines: ["CI: 🚀 ready"], omitted: 0 });
    monitor.close();
    expect(emit.mock.calls[1]![0].lines).toEqual(["CI: passed"]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("bounds unterminated lines and burst batches with overflow disclosure", async () => {
    const emit = vi.fn();
    const monitor = new ShellMonitor(options(), emit);
    monitor.append(Buffer.alloc(1024 * 1024, 97));
    monitor.append(Buffer.from("\n"));
    for (let i = 0; i < 100; i++) monitor.append(Buffer.from(`event ${i}\n`));
    await vi.advanceTimersByTimeAsync(1000);
    expect(emit).toHaveBeenCalledOnce();
    const batch = emit.mock.calls[0]![0];
    expect(batch.lines).toHaveLength(8);
    expect(batch.lines.at(-1)).toBe("event 99");
    expect(batch.omitted).toBe(93);
    monitor.close();
  });

  it("does not schedule inference/timers for silent or filtered polls, and cancellation drops pending output", async () => {
    const emit = vi.fn();
    const monitor = new ShellMonitor({ ...options(), match: "interesting" }, emit);
    for (let i = 0; i < 20; i++) monitor.append(Buffer.from("no change\n"));
    expect(vi.getTimerCount()).toBe(0);
    monitor.append(Buffer.from("interesting\n"));
    monitor.close(false);
    await vi.advanceTimersByTimeAsync(10000);
    expect(emit).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });
});
