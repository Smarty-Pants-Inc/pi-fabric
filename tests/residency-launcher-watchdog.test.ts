import { describe, expect, it } from "vitest";
import { ResidentLauncherWatchdog } from "../src/residency/launcher-watchdog.js";
import type { FabricHostLease } from "../src/topology/host-leases.js";
const lease = (updatedAt: number): FabricHostLease => ({ id: "host", rootId: "root", identityId: "host", updatedAt, expiresAt: updatedAt + 1000 });

describe("resident launcher watchdog (#3864)", () => {
  it("alarms once after N stale renewal intervals and resets only for a new attempt", () => {
    const watcher = new ResidentLauncherWatchdog(100, 3);
    expect(watcher.observe(10, lease(10), [], 309)).toBeUndefined();
    expect(watcher.observe(10, lease(10), [], 310)).toBe("stale-lease");
    expect(watcher.observe(10, lease(10), [], 1000)).toBeUndefined();
    expect(new ResidentLauncherWatchdog(100, 3).observe(10, lease(10), [], 1000)).toBe("stale-lease");
  });
  it("does not confuse lease TTL with renewal freshness; missing renewal fails closed", () => {
    const watcher = new ResidentLauncherWatchdog(100, 3);
    expect(watcher.observe(10, { ...lease(10), expiresAt: 100000 }, [], 310)).toBe("stale-lease");
    expect(new ResidentLauncherWatchdog(100, 3).observe(10, undefined, [], 310)).toBe("stale-lease");
  });
  it("allows transient zombies but alarms for an unreaped birth even with a renewing lease", () => {
    const watcher = new ResidentLauncherWatchdog(100, 3);
    const zombie = { pid: 20, processStartTime: "birth" };
    expect(watcher.observe(10, lease(10), [zombie], 10)).toBeUndefined();
    expect(watcher.observe(10, lease(309), [zombie], 309)).toBeUndefined();
    expect(watcher.observe(10, lease(310), [zombie], 310)).toBe("unreaped-child");
    expect(watcher.observe(10, lease(400), [zombie], 400)).toBeUndefined();
  });
  it("drops reaped children and never transfers zombie age across PID reuse", () => {
    const watcher = new ResidentLauncherWatchdog(100, 3);
    expect(watcher.observe(10, lease(10), [{ pid: 20, processStartTime: "a" }], 10)).toBeUndefined();
    expect(watcher.observe(10, lease(250), [], 250)).toBeUndefined();
    expect(watcher.observe(10, lease(310), [{ pid: 20, processStartTime: "b" }], 310)).toBeUndefined();
    expect(watcher.observe(10, lease(600), [{ pid: 20, processStartTime: "b" }], 600)).toBeUndefined();
  });
});
