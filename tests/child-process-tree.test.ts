import { EventEmitter } from "node:events";
import type { ChildProcess } from "node:child_process";
import { afterEach, describe, expect, it, vi } from "vitest";
import { terminateWindowsTree } from "../src/child-process-tree.js";
const { spawn } = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", () => ({ spawn }));
const fakeChild = () => Object.assign(new EventEmitter(), { pid: 1234, kill: vi.fn() });
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks(); });

describe("owned Windows taskkill close obligation", () => {
  it("targets only the owned PID/tree and joins helper close", async () => {
    const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    const pending = terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    expect(spawn).toHaveBeenCalledWith("taskkill", ["/pid", "1234", "/T", "/F"], { windowsHide: true, stdio: "ignore" });
    await Promise.resolve(); expect(joined).toBe(false);
    killer.emit("close", 0); await pending;
    expect(joined).toBe(true); expect(child.kill).not.toHaveBeenCalled();
  });
  it("an error fallback still joins helper close", async () => {
    const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    const pending = terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    killer.emit("error", new Error("sanitized test failure"));
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    await Promise.resolve(); expect(joined).toBe(false);
    killer.emit("close", -1); await pending;
  });
  it("bounded helper timeout kills only owned processes but does not fake close", async () => {
    vi.useFakeTimers();
    const child = fakeChild(); const killer = fakeChild(); spawn.mockReturnValue(killer);
    let joined = false;
    const pending = terminateWindowsTree(child as unknown as ChildProcess).then(() => { joined = true; });
    await vi.advanceTimersByTimeAsync(1000);
    expect(killer.kill).toHaveBeenCalledWith("SIGKILL"); expect(child.kill).toHaveBeenCalledWith("SIGKILL");
    expect(joined).toBe(false);
    killer.emit("close", null); await pending; expect(vi.getTimerCount()).toBe(0);
  });
  it("a synchronous helper spawn failure falls back without a nonexistent helper obligation", async () => {
    const child = fakeChild(); spawn.mockImplementationOnce(() => { throw new Error("spawn unavailable"); });
    await terminateWindowsTree(child as unknown as ChildProcess);
    expect(child.kill).toHaveBeenCalledWith("SIGKILL");
  });
  it("needs no helper for a child that never spawned", async () => {
    await terminateWindowsTree({ pid: undefined } as ChildProcess);
    expect(spawn).not.toHaveBeenCalled();
  });
});
