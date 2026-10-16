import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";

const roots: string[] = [];
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// pi-fabric#1249: wall-clock lease time can move independently of a relative acquisition budget.
// Exercise actual acquisition, notification/poll waits and cleanup, not mocked admission decisions.
describe.each([1, 2, 3] as const)("protocol %s: relative lock wait clock", protocol => {
  const fixture = async (queued: boolean) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-clock-"));
    roots.push(root);
    // Prepare native identity before installing a deterministic timer clock.
    const lock = new MeshLock(root, { lockProtocol: protocol }, () => undefined);
    await lock.ownIncarnation;
    vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
    const canonical = path.join(root, ".lock");
    fs.mkdirSync(canonical);
    fs.writeFileSync(path.join(canonical, "owner"), `live-holder\n${process.pid}\n${Date.now()}\n`);
    const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
    vi.spyOn(fs, "watch").mockImplementation(((_target: fs.PathLike, _options: fs.WatchOptions,
      listener: (event: string, filename: string | null) => void) => {
      watcher.on("change", listener);
      return watcher;
    }) as unknown as typeof fs.watch);
    if (!queued) vi.spyOn(MeshLockTicket.prototype, "queued", "get").mockReturnValue(false);
    const operation = vi.fn(() => "acquired within budget");
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    expect(operation).not.toHaveBeenCalled();
    const release = () => {
      fs.renameSync(canonical, path.join(root, "released-fixture"));
      watcher.emit("change", "rename", ".lock");
    };
    const clean = () => {
      expect(fs.existsSync(canonical)).toBe(false);
      expect(fs.readdirSync(meshLockQueueDirectory(root))).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      if (queued) expect(watcher.close).toHaveBeenCalled();
    };
    return { operation, pending, release, clean };
  };

  it.each([true, false])("wall-clock jump during asynchronous wait does not spend the budget (queued=%s)", async queued => {
    const { operation, pending, release, clean } = await fixture(queued);
    await vi.advanceTimersByTimeAsync(30);
    vi.setSystemTime(Date.now() + 60_001); // lease time travel / wall-clock correction, not elapsed wait time
    release();
    await vi.advanceTimersByTimeAsync(250); // also wakes the jittered plain retry, bounded by 250 ms
    expect(await pending).toBe("acquired within budget");
    expect(operation).toHaveBeenCalledOnce();
    clean();
  });

  it.each([true, false])("a late wake cannot acquire after real elapsed budget even with a backward wall clock (queued=%s)", async queued => {
    const { operation, pending, release, clean } = await fixture(queued);
    const wall = Date.now();
    // Fire the wait timer, but leave its promise continuation pending as if the event loop stalled.
    vi.advanceTimersByTime(2_001);
    vi.setSystemTime(wall - 60_000);
    release();
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    expect(result).toBeInstanceOf(MeshLockTimeoutError);
    expect(result).not.toBeInstanceOf(MeshLockQueueExpiredError);
    expect(result).toMatchObject({ attempts: 1 });
    expect(operation).not.toHaveBeenCalled();
    clean();
  });
});
