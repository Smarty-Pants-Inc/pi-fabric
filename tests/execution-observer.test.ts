import fs from "node:fs";
import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { executionGroup } from "../src/worker/execution-group.js";
import { executionObserver } from "../src/worker/execution-observer.js";

beforeEach(() => vi.useFakeTimers());
afterEach(() => { vi.clearAllTimers(); vi.useRealTimers(); vi.restoreAllMocks(); });

describe("native-child legacy observation", () => {
  it("does not infer a Windows execution tree or sole-leader receipt from native exit", () => {
    const child = new EventEmitter() as ChildProcess;
    let closed = false;
    child.once("close", () => { closed = true; });
    const observer = executionObserver(child, {
      exited: () => closed,
      inspectIdle: () => "active",
    });
    observer.idle();
    child.emit("exit", 0);
    expect(observer.exited()).toBe(false);
    expect(vi.getTimerCount()).toBe(1);
    child.emit("close", 0);
    expect(observer.exited()).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe.skipIf(process.platform !== "linux")("owned execution observation lifetime", () => {
  const setup = () => {
    const processes = new Map<number, { started: string; group: number }>([[100, { started: "leader-birth", group: 100 }]]);
    let unreadable = false;
    const census = vi.spyOn(fs, "readdirSync").mockImplementation(() => [...processes.keys()].map(String) as never);
    const reads = vi.spyOn(fs, "readFileSync").mockImplementation(file => {
      if (unreadable) throw Object.assign(new Error("identity unreadable"), { code: "EIO" });
      const pid = Number(String(file).split("/")[2]);
      const value = processes.get(pid);
      if (!value) throw Object.assign(new Error("gone"), { code: "ENOENT" });
      const fields = Array<string>(20).fill("0");
      fields[0] = "S"; fields[2] = String(value.group); fields[19] = value.started;
      return `${pid} (fixture) ${fields.join(" ")}`;
    });
    const child = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    const kill = vi.spyOn(process, "kill").mockImplementation(() => true);
    const group = executionGroup(child);
    const observer = executionObserver(child, group);
    const leaderExit = () => { processes.delete(100); Object.assign(child, { exitCode: 0 }); child.emit("exit", 0); };
    return { processes, census, reads, child, group, observer, kill, leaderExit, unknown: () => { unreadable = true; } };
  };

  it("creates no observer or timer merely by importing the module", () => {
    expect(vi.getTimerCount()).toBe(0);
  });

  it("stops immediately at native leader exit + checked emptiness while pipe close is outstanding", () => {
    const { observer, reads, census, child, leaderExit } = setup();
    leaderExit();
    expect(census).toHaveBeenCalledTimes(1);
    expect(child.listenerCount("close")).toBe(1); // Group close tracking is independent.
    expect(vi.getTimerCount()).toBe(0);
    reads.mockClear(); census.mockClear();
    // Crash/cleanup/close callers can join the same latched group receipt.
    expect(observer.exited()).toBe(true);
    observer.arm();
    vi.advanceTimersByTime(30_000);
    child.emit("close", 0);
    expect(observer.exited()).toBe(true);
    expect(reads).not.toHaveBeenCalled();
    expect(census).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("stops on a cleanup query before pipe close, without leaving the 100ms census", () => {
    const { processes, observer, reads, census } = setup();
    processes.clear();
    expect(observer.exited()).toBe(true);
    reads.mockClear(); census.mockClear();
    vi.advanceTimersByTime(30_000);
    expect(reads).not.toHaveBeenCalled();
    expect(census).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("performs one checked census at positive native idle, then no reads for a LIVE sole Pi leader", () => {
    const { observer, census, reads, child } = setup();
    observer.idle();
    expect(census).toHaveBeenCalledTimes(1);
    expect(child.exitCode).toBeNull();
    expect(vi.getTimerCount()).toBe(0);
    reads.mockClear(); census.mockClear();
    vi.advanceTimersByTime(30_000);
    expect(reads).not.toHaveBeenCalled();
    expect(census).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("re-arms before work and discovers a new descendant before leader exit", () => {
    const { observer, processes, leaderExit, group, kill, census } = setup();
    observer.idle();
    observer.arm(); observer.arm();
    expect(vi.getTimerCount()).toBe(1);
    processes.set(101, { started: "refusing-birth", group: 100 });
    vi.advanceTimersByTime(100);
    leaderExit();
    expect(observer.exited()).toBe(false);
    census.mockClear();
    vi.advanceTimersByTime(30_000);
    expect(census).toHaveBeenCalledTimes(300);
    expect(vi.getTimerCount()).toBe(1);
    group.signal("SIGKILL");
    expect(kill).toHaveBeenCalledWith(-100, "SIGKILL");
    processes.clear();
    expect(observer.exited()).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("keeps active custody at native idle until a recorded descendant drains", () => {
    const { processes, observer, census, reads } = setup();
    processes.set(101, { started: "descendant-birth", group: 100 });
    observer.idle();
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(100);
    processes.delete(101);
    vi.advanceTimersByTime(100);
    expect(vi.getTimerCount()).toBe(0);
    census.mockClear(); reads.mockClear();
    vi.advanceTimersByTime(30_000);
    expect(census).not.toHaveBeenCalled();
    expect(reads).not.toHaveBeenCalled();
  });

  it("does not disarm while a recorded same-birth descendant survives in a foreign PGID", () => {
    const { processes, observer, group } = setup();
    processes.set(101, { started: "descendant-birth", group: 100 });
    group.observe();
    processes.set(101, { started: "descendant-birth", group: 200 });
    observer.idle();
    vi.advanceTimersByTime(30_000);
    expect(vi.getTimerCount()).toBe(1);
    expect(observer.exited()).toBe(false);
  });

  it("retains the active fallback for silent external CLI / unproved Pi handshake", () => {
    const { census, observer } = setup();
    // Without an admitted work -> positive native idle boundary, do not idle().
    vi.advanceTimersByTime(30_000);
    expect(census).toHaveBeenCalledTimes(300);
    expect(observer.exited()).toBe(false);
    expect(vi.getTimerCount()).toBe(1);
  });

  it.each(["EIO", "reused leader", "foreign group"])("keeps %s fail closed across idle and native exit wakes", mode => {
    const { observer, group, processes, unknown, child, kill } = setup();
    if (mode === "EIO") unknown();
    else if (mode === "reused leader") processes.set(100, { started: "foreign-birth", group: 100 });
    else {
      processes.set(100, { started: "leader-birth", group: 200 });
      processes.set(101, { started: "foreign-birth", group: 100 });
    }
    expect(() => observer.idle()).not.toThrow();
    child.emit("exit", 0);
    vi.advanceTimersByTime(100);
    expect(vi.getTimerCount()).toBe(1);
    expect(() => observer.exited()).toThrow();
    expect(() => group.signal("SIGKILL")).toThrow();
    expect(kill).not.toHaveBeenCalled();
  });

  it("retains unresolved debt when the leader exits before any descendant birth was captured", () => {
    const { processes, leaderExit, observer, group, kill } = setup();
    processes.set(101, { started: "unobserved-birth", group: 100 });
    leaderExit();
    expect(() => observer.exited()).toThrow(/no surviving owned birth/);
    expect(() => group.signal("SIGKILL")).toThrow(/no surviving owned birth/);
    expect(kill).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
  });

  it("fails closed if a background extension forks after positive idle then the leader exits before work resumes", () => {
    const { observer, processes, leaderExit, group, kill } = setup();
    observer.idle();
    expect(vi.getTimerCount()).toBe(0);
    processes.set(101, { started: "late-background-birth", group: 100 });
    leaderExit();
    // Native exit re-arms the census, but cannot adopt an unanchored orphan.
    expect(() => observer.exited()).toThrow(/no surviving owned birth/);
    expect(() => group.signal("SIGTERM")).toThrow(/no surviving owned birth/);
    expect(kill).not.toHaveBeenCalled();
    vi.advanceTimersByTime(30_000);
    expect(vi.getTimerCount()).toBe(1);
  });

  it("keeps attempts independent when a provider resume creates another observer", () => {
    const { processes, observer, child, group, reads, census } = setup();
    processes.clear();
    expect(observer.exited()).toBe(true);
    // A resumed attempt must not re-use the previous attempt's empty latch.
    processes.set(100, { started: "resumed-birth", group: 100 });
    const resumedChild = Object.assign(new EventEmitter(), { pid: 100, exitCode: null, signalCode: null }) as unknown as ChildProcess;
    const resumed = executionObserver(resumedChild, executionGroup(resumedChild));
    child.emit("close", 0);
    reads.mockClear(); census.mockClear();
    expect(observer.exited()).toBe(true);
    expect(group.exited()).toBe(true);
    expect(reads).not.toHaveBeenCalled();
    expect(census).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
    expect(resumed.exited()).toBe(false);
    processes.clear();
    expect(resumed.exited()).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
});
