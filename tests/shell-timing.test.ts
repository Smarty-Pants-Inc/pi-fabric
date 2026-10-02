import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FabricShellJobStore, raceShellHang } from "../src/core/shell-jobs.js";
import { FabricShellTimingBridge } from "../src/core/shell-timing.js";
import { FABRIC_SHELL_TIMING_EVENT } from "../src/protocol.js";

const stores: FabricShellJobStore[] = [];
const bridges: FabricShellTimingBridge[] = [];
function harness() {
  const jobs = new FabricShellJobStore(); stores.push(jobs);
  const emit = vi.fn();
  const bridge = new FabricShellTimingBridge({ emit } as unknown as ExtensionAPI["events"], "session-a", jobs);
  bridges.push(bridge);
  return { jobs, emit, bridge };
}
afterEach(async () => {
  for (const bridge of bridges.splice(0)) bridge.close();
  for (const jobs of stores.splice(0)) await jobs.close();
  vi.useRealTimers();
});

describe("background shell timing bridge", () => {
  it("publishes a minimal versioned lifecycle only after handoff", async () => {
    const { jobs, emit } = harness();
    const foreground = jobs.begin("bash", "private command");
    await foreground.finish(0);
    expect(emit).not.toHaveBeenCalled();
    const job = jobs.begin("powershell", "secret command", { cwd: "/private", description: "private label" });
    job.append(Buffer.from("private output"));
    job.spill(); job.spill();
    const payload = { version: 1, sessionId: "session-a", taskId: job.id, tool: "powershell", phase: "started", timestamp: expect.any(Number) };
    expect(emit).toHaveBeenCalledExactlyOnceWith(FABRIC_SHELL_TIMING_EVENT, payload);
    jobs.acknowledge(job.id);
    await job.finish(1); await job.finish(1);
    expect(emit).toHaveBeenCalledTimes(2);
    expect(emit).toHaveBeenLastCalledWith(FABRIC_SHELL_TIMING_EVENT, { ...payload, phase: "finished" });
  });

  it.each(["ui", "wake"] as const)("times %s monitors even when stopped or acknowledged", async delivery => {
    const { jobs, emit } = harness();
    const job = jobs.begin("bash", "monitor", { monitor: { delivery, timeoutMs: 1000, intervalMs: 1000 } });
    job.spill(); job.append(Buffer.from("progress\n"));
    jobs.acknowledge(job.id); jobs.stop(job.id);
    expect(emit).toHaveBeenCalledTimes(1);
    await job.finish(null);
    expect(emit).toHaveBeenCalledTimes(2);
    expect(job.status).toBe("killed");
  });

  it("publishes terminal timing when a monitor deadline expires", async () => {
    // ScratchScope release yields with setImmediate; only fake the timers under test.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });
    const { jobs, emit } = harness();
    const job = jobs.begin("bash", "monitor", { monitor: { delivery: "ui", timeoutMs: 1000, intervalMs: 1000 } });
    job.spill();
    await vi.advanceTimersByTimeAsync(1000);
    await job.finish(null);
    expect(job.status).toBe("timed_out");
    expect(emit).toHaveBeenCalledTimes(2);
  });

  it("observes explicit immediate background handoff", async () => {
    const { jobs, emit } = harness();
    const job = jobs.begin("bash", "immediate command");
    vi.spyOn(job, "readPid").mockResolvedValue(42);
    let complete!: (value: number) => void;
    const pending = new Promise<number>(resolve => { complete = resolve; });
    expect(await raceShellHang({ execute: () => pending, parentSignal: undefined, hangMs: 0, immediate: true, job })).toEqual({ status: "spilled", auto: false });
    expect(emit).toHaveBeenCalledTimes(1);
    await job.finish(0);
    complete(0);
    expect(emit).toHaveBeenCalledTimes(2);
  });

  it("observes automatic hang handoff and manual spill through the same lifecycle", async () => {
    // ScratchScope release yields with setImmediate; only fake the timers under test.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval", "Date"] });
    const { jobs, emit } = harness();
    const job = jobs.begin("bash", "long command");
    let complete!: (value: number) => void;
    const pending = new Promise<number>(resolve => { complete = resolve; });
    const result = raceShellHang({ execute: () => pending, parentSignal: undefined, hangMs: 100, job });
    await vi.advanceTimersByTimeAsync(100);
    expect(await result).toEqual({ status: "spilled", auto: true });
    expect(emit).toHaveBeenCalledTimes(1);
    complete(0);
    await job.finish(0);
    const manual = jobs.begin("bash", "manual");
    expect(jobs.spillWaiting()).toBe(1);
    await manual.finish(0);
    expect(emit.mock.calls.map(call => call[1].phase)).toEqual(["started", "finished", "started", "finished"]);
  });

  it("closes live spans exactly once before silent store teardown", async () => {
    const { jobs, emit, bridge } = harness();
    const a = jobs.begin("bash", "a"); a.spill();
    const b = jobs.begin("bash", "b"); b.spill();
    bridge.close(); bridge.close();
    await jobs.close();
    expect(emit.mock.calls.map(call => call[1].phase)).toEqual(["started", "started", "finished", "finished"]);
  });

  it("isolates timing listener failures from execution and cleanup", async () => {
    const { jobs, emit, bridge } = harness();
    emit.mockImplementation(() => { throw new Error("observer failed"); });
    const job = jobs.begin("bash", "a");
    expect(() => job.spill()).not.toThrow();
    expect(() => bridge.close()).not.toThrow();
    await expect(jobs.close()).resolves.toBeUndefined();
  });
});
