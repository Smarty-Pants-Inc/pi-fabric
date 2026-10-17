import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Recorded catch: pi-fabric#1249's hosted deadline failure. Timer progress must not depend on
// performance's diagnostic clock, even when a wall-clock correction also hides the elapsed budget.
describe.each([1, 2, 3] as const)("protocol %s: timer-owned wait budget", protocol => {
  it("refuses a late handoff with frozen performance and backward Date", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-timer-budget-"));
    roots.push(root);
    const lock = new MeshLock(root, { lockProtocol: protocol }, () => undefined);
    await lock.ownIncarnation;
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    vi.spyOn(performance, "now").mockReturnValue(0);
    const wall = Date.now();
    const canonical = path.join(root, ".lock");
    fs.mkdirSync(canonical);
    fs.writeFileSync(path.join(canonical, "owner"), `live-holder\n${process.pid}\n${wall}\n`);
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const operation = vi.fn(() => "too late");
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    // Ordinary retries must not restart the acquisition's fixed timer budget.
    await vi.advanceTimersByTimeAsync(900);
    // Fire timers without resuming their promise continuations, then hide elapsed time from Date.
    vi.advanceTimersByTime(1_101);
    vi.setSystemTime(wall - 60_000);
    fs.renameSync(canonical, path.join(root, "released-fixture"));
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    expect(result).toBeInstanceOf(MeshLockTimeoutError);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(canonical)).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });
});
