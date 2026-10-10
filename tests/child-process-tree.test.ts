import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { terminatePosixGroup, terminateWindowsTree } from "../src/child-process-tree.js";
const { spawn } = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn }));
const fakeChild = () => Object.assign(new EventEmitter(), { pid: 1234, kill: vi.fn() });
const spyAlarm = () => vi.spyOn(process, "emitWarning").mockImplementation(() => {});
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); vi.clearAllMocks(); vi.unstubAllEnvs(); });

describe("owned Windows taskkill close obligation", () => {
  it("targets only the owned PID/tree and joins successful helper close", async () => {
    vi.stubEnv("PI_FABRIC_LANDLOCK_ESCAPE", "1");
    // #738: helpers inherit ordinary values, never host-owned Landlock controls.
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith("PI_FABRIC_LANDLOCK_")));
    const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    const pending = terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    expect(spawn).toHaveBeenCalledWith("taskkill", ["/pid", "1234", "/T", "/F"], { env, windowsHide: true, stdio: "ignore" });
    await Promise.resolve(); expect(joined).toBe(false);
    killer.emit("close", 0); await pending;
    expect(joined).toBe(true); expect(child.kill).not.toHaveBeenCalled();
  });
  it.each([-1, 1, null])("unsuccessful helper close %s retains an alarmed uncertain-tree fence", async code => {
    const alarm = spyAlarm(); const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    void terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    // Parent close is not evidence that a pipe-independent descendant exited.
    child.emit("close", null);
    killer.emit("close", code);
    await Promise.resolve(); expect(joined).toBe(false);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    expect(alarm).toHaveBeenCalledOnce();
    expect(alarm).toHaveBeenCalledWith(expect.stringContaining("retirement remains pending"), { code: "FABRIC_PROCESS_TREE_UNCONFIRMED" });
  });
  it("failed attempt closure notifies its owner without discharging tree custody", async () => {
    spyAlarm(); const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    const debt = vi.fn();
    const closed = vi.fn();
    let joined = false;
    void terminateWindowsTree(child as unknown as ChildProcess, debt, closed).then(() => { joined = true; });
    killer.emit("error", new Error("failed attempt"));
    child.emit("close", null);
    expect(closed).not.toHaveBeenCalled(); // Parent exit/error do not join the helper.
    killer.emit("close", 1);
    await Promise.resolve();
    expect(closed).toHaveBeenCalledOnce();
    expect(debt).toHaveBeenCalledOnce();
    expect(debt.mock.invocationCallOrder[0]).toBeLessThan(closed.mock.invocationCallOrder[0]!);
    expect(joined).toBe(false); // Closed failure is still not tree-exit authority.
  });
  it("an error keeps retirement pending even after helper and parent close", async () => {
    const alarm = spyAlarm(); const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    void terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    killer.emit("error", new Error("FAKE_SECRET"));
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    child.emit("close", null); killer.emit("close", 0);
    await Promise.resolve(); expect(joined).toBe(false);
    expect(alarm).toHaveBeenCalledOnce(); expect(alarm.mock.calls[0]![0]).not.toContain("FAKE_SECRET");
  });
  it("bounded helper timeout kills only owned processes and retains the fence even on late success", async () => {
    vi.useFakeTimers(); const alarm = spyAlarm();
    const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    void terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    await vi.advanceTimersByTimeAsync(1000);
    expect(killer.kill).toHaveBeenCalledWith("SIGKILL"); expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    child.emit("close", null); killer.emit("close", 0);
    await Promise.resolve(); expect(joined).toBe(false); expect(vi.getTimerCount()).toBe(0);
    expect(alarm).toHaveBeenCalledOnce();
  });
  it("synchronous helper spawn failure retains an alarmed fence without a helper", async () => {
    const alarm = spyAlarm(); const child = fakeChild(); spawn.mockImplementationOnce(() => { throw new Error("FAKE_SECRET"); });
    let joined = false;
    void terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    child.emit("close", null); await Promise.resolve(); expect(joined).toBe(false);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL"); expect(alarm).toHaveBeenCalledOnce();
    expect(alarm.mock.calls[0]![0]).not.toContain("FAKE_SECRET");
  });
  it("needs no helper for a child that never spawned", async () => {
    await terminateWindowsTree({ pid: undefined } as ChildProcess);
    expect(spawn).not.toHaveBeenCalled();
  });
});

describe("owned POSIX group confirmation", () => {
  it("TERM and bounded KILL do not join until the group probe reports ESRCH", async () => {
    vi.useFakeTimers(); const alarm = spyAlarm(); let gone = false;
    const kill = vi.spyOn(process, "kill").mockImplementation((_pid, signal) => {
      if (signal === 0 && gone) throw Object.assign(new Error("gone"), { code: "ESRCH" });
      return true;
    });
    let joined = false;
    const pending = terminatePosixGroup(1234).then(() => { joined = true; });
    expect(kill).toHaveBeenCalledWith(-1234, "SIGTERM");
    vi.setSystemTime(new Date(0)); // Moving the wall clock must not extend grace.
    await vi.advanceTimersByTimeAsync(499); expect(kill).not.toHaveBeenCalledWith(-1234, "SIGKILL"); expect(joined).toBe(false);
    await vi.advanceTimersByTimeAsync(1); expect(kill).toHaveBeenCalledWith(-1234, "SIGKILL"); expect(joined).toBe(false);
    await vi.advanceTimersByTimeAsync(1000); expect(joined).toBe(false); expect(alarm).toHaveBeenCalledOnce();
    gone = true; await vi.advanceTimersByTimeAsync(1000); await pending;
    expect(joined).toBe(true); expect(vi.getTimerCount()).toBe(0);
    expect(kill.mock.calls.every(([pid]) => pid === -1234)).toBe(true);
  });
  it("EPERM is not an empty-group proof and keeps the debt alarmed", async () => {
    vi.useFakeTimers(); const alarm = spyAlarm(); let gone = false;
    vi.spyOn(process, "kill").mockImplementation(() => {
      throw Object.assign(new Error("FAKE_SECRET"), { code: gone ? "ESRCH" : "EPERM" });
    });
    let joined = false;
    const pending = terminatePosixGroup(1234).then(() => { joined = true; });
    await vi.advanceTimersByTimeAsync(600); expect(joined).toBe(false);
    expect(alarm).toHaveBeenCalledOnce(); expect(alarm.mock.calls[0]![0]).not.toContain("FAKE_SECRET");
    gone = true; await vi.advanceTimersByTimeAsync(25); await pending;
    expect(joined).toBe(true); expect(vi.getTimerCount()).toBe(0);
  });
  it("a TERM-cooperative group clears escalation/poll/alarm timers before they can signal a reused ID", async () => {
    vi.useFakeTimers(); const alarm = spyAlarm(); let gone = false;
    const kill = vi.spyOn(process, "kill").mockImplementation((_pid, signal) => {
      if (signal === 0 && gone) throw Object.assign(new Error("gone"), { code: "ESRCH" });
      if (signal === "SIGTERM") gone = true;
      return true;
    });
    await terminatePosixGroup(1234);
    expect(vi.getTimerCount()).toBe(0);
    await vi.advanceTimersByTimeAsync(2000);
    expect(kill).not.toHaveBeenCalledWith(-1234, "SIGKILL"); expect(alarm).not.toHaveBeenCalled();
  });

  it("an already empty group or failed spawn needs no termination", async () => {
    const kill = vi.spyOn(process, "kill").mockImplementation(() => { throw Object.assign(new Error("gone"), { code: "ESRCH" }); });
    await terminatePosixGroup(undefined); expect(kill).not.toHaveBeenCalled();
    await terminatePosixGroup(1234); expect(kill).toHaveBeenCalledExactlyOnceWith(-1234, 0);
  });
});
