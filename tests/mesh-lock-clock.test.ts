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

// pi-fabric#1249: wall-clock lease time can move independently of a relative acquisition budget.
// Exercise actual plain acquisition, retry waits and cleanup, without notification delivery.
describe.each([1, 2, 3] as const)("protocol %s: relative lock wait clock", protocol => {
  const fixture = async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-clock-"));
    roots.push(root);
    // Prepare native identity before installing a deterministic timer clock.
    const lock = new MeshLock(root, { lockProtocol: protocol }, () => undefined);
    await lock.ownIncarnation;
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    const canonical = path.join(root, ".lock");
    fs.mkdirSync(canonical);
    fs.writeFileSync(path.join(canonical, "owner"), `live-holder\n${process.pid}\n${Date.now()}\n`);
    vi.spyOn(Math, "random").mockReturnValue(0.5);
    const operation = vi.fn(() => "acquired within budget");
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    expect(operation).not.toHaveBeenCalled();
    const release = () => {
      fs.renameSync(canonical, path.join(root, "released-fixture"));
    };
    const clean = () => {
      expect(fs.existsSync(canonical)).toBe(false);
      expect(vi.getTimerCount()).toBe(0);
    };
    return { operation, pending, release, clean };
  };

  it("wall-clock jump during asynchronous wait does not spend the budget", async () => {
    const { operation, pending, release, clean } = await fixture();
    await vi.advanceTimersByTimeAsync(30);
    vi.setSystemTime(Date.now() + 60_001); // lease time travel / wall-clock correction, not elapsed wait time
    release();
    await vi.advanceTimersByTimeAsync(250); // also wakes the jittered plain retry, bounded by 250 ms
    expect(await pending).toBe("acquired within budget");
    expect(operation).toHaveBeenCalledOnce();
    clean();
  });

  it("a late wake cannot acquire after real elapsed budget even with a backward wall clock", async () => {
    const { operation, pending, release, clean } = await fixture();
    const wall = Date.now();
    // Fire the wait timer, but leave its promise continuation pending as if the event loop stalled.
    vi.advanceTimersByTime(2_001);
    vi.setSystemTime(wall - 60_000);
    release();
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    expect(result).toBeInstanceOf(MeshLockTimeoutError);
    expect(result).toMatchObject({ attempts: 1 });
    expect(operation).not.toHaveBeenCalled();
    clean();
  });
});
