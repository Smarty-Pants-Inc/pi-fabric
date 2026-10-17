import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshIncarnationPendingError, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";

// Relative waits start after setup. Plain contention has real acquisition attempts;
// cold identity remains a distinct pre-lock refusal, never a fabricated contention timeout.
const roots: string[] = [];
const scratch = (): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-budget-"));
  roots.push(root);
  return root;
};
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

/** The fake clock jumps by `ms` inside native pre-lock root setup, before the wait budget starts. */
const slowSetup = (root: string, ms: number): void => {
  const mkdir = fs.mkdirSync;
  let armed = true;
  vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (armed && String(file) === root) { armed = false; vi.setSystemTime(Date.now() + ms); }
    return mkdir(file, options as undefined);
  }) as typeof fs.mkdirSync);
};
/** A live holder of `.lock` whose record no reclaimer may recover (live pid, no incarnation). */
const holdLock = (root: string): void => {
  fs.mkdirSync(path.join(root, ".lock"));
  fs.writeFileSync(path.join(root, ".lock", "owner"), `legacy\n${process.pid}\n${Date.now()}\n`);
};
const ready = async (protocol: 1 | 2 | 3, root: string, options: { lockTimeoutMs?: number } = {}): Promise<MeshLock> => {
  const lock = new MeshLock(root, { lockProtocol: protocol, ...options }, () => undefined);
  await lock.ownIncarnation;
  return lock;
};

describe("mesh lock wait budget starts after pre-lock setup", () => {
  it.each([1, 2, 3] as const)("protocol %s: setup that consumes the whole budget still acquires an uncontended lock", async (protocol) => {
    const root = scratch();
    const lock = await ready(protocol, root);
    vi.useFakeTimers({ toFake: ["Date"] });
    for (const budget of [0, 50, 5_000]) {
      slowSetup(root, 3 * budget + 1_000);
      await expect(lock.withLock(() => "acquired", budget)).resolves.toBe("acquired");
      await expect(lock.withTryLock(() => lock.withLock(() => "tried"), budget)).resolves.toBe("tried");
      vi.restoreAllMocks();
    }
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it.each([1, 2, 3] as const)("protocol %s: setup leaves the full native contention budget", async (protocol) => {
    const root = scratch();
    const lock = await ready(protocol, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const owner = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8");
    slowSetup(root, 5_000);
    const operation = vi.fn();
    let settled = false;
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error).finally(() => { settled = true; });
    await vi.advanceTimersByTimeAsync(1_000);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1_500);
    const error = await pending;
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect((error as MeshLockTimeoutError).attempts).toBeGreaterThan(1);
    expect(atomic.isMeshLockTimeout(error)).toBe(true);
    expect(atomic.isMeshWriteDeferred(error)).toBe(true);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8")).toBe(owner);
  });

  it("fresh-budget plain contenders all complete once after release", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const order: string[] = [];
    const first = lock.withLock(() => order.push("first"), 10_000);
    const second = lock.withLock(() => order.push("second"), 10_000);
    await vi.advanceTimersByTimeAsync(3_000);
    expect(order).toEqual([]);
    fs.renameSync(path.join(root, ".lock"), path.join(root, "fixture-held-lock"));
    await vi.advanceTimersByTimeAsync(250); // maximum native jitter delay after release
    await Promise.all([first, second]);
    expect([...order].sort()).toEqual(["first", "second"]);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it.each([1, 2, 3] as const)("protocol %s: a zero budget still gets one real native acquisition attempt", async (protocol) => {
    const root = scratch();
    const lock = await ready(protocol, root);
    await expect(lock.withLock(() => "acquired", 0)).resolves.toBe("acquired");
    holdLock(root);
    const operation = vi.fn();
    const error = await lock.withLock(operation, 0).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect(error).toMatchObject({ attempts: 1 });
    expect((error as Error).message).toContain("Timed out waiting");
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(true);
  });

  // Record actual native acquisition probes to enforce fixed per-caller deadlines under contention.
  const lockMkdirs = (root: string) => {
    const mkdir = fs.mkdirSync;
    const at: number[] = [];
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      if (String(file) === path.join(root, ".lock")) at.push(Date.now());
      return mkdir(file, options as undefined);
    }) as typeof fs.mkdirSync);
    return at;
  };

  it("one plain contender timing out does not extend another contender's native deadline", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root); // never released
    const t0 = Date.now();
    const attempts = lockMkdirs(root);
    let headSettledAt = -1;
    const head = lock.withLock(() => "never", 3_000).catch((error: unknown) => error)
      .finally(() => { headSettledAt = Date.now(); });
    let waiterSettledAt = -1;
    const waiter = lock.withLock(() => "never", 2_005).catch((error: unknown) => error)
      .finally(() => { waiterSettledAt = Date.now(); });
    await vi.advanceTimersByTimeAsync(3_100);
    const [headError, waiterError] = await Promise.all([head, waiter]);
    expect(waiterError).toBeInstanceOf(MeshLockTimeoutError);
    expect((waiterError as MeshLockTimeoutError).attempts).toBeGreaterThan(1);
    expect(waiterSettledAt).toBe(t0 + 2_005);
    expect(headError).toBeInstanceOf(MeshLockTimeoutError);
    expect((headError as MeshLockTimeoutError).attempts).toBeGreaterThan(1);
    expect((headError as MeshLockTimeoutError).attempts + (waiterError as MeshLockTimeoutError).attempts).toBe(attempts.length);
    // Both original deadlines stay fixed; neither timeout restarts the other's budget.
    expect(headSettledAt).toBe(t0 + 3_000);
    expect(Math.max(...attempts)).toBeLessThanOrEqual(t0 + 3_000);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(true);
  });

  it("custody: setup consuming the budget leaves the whole budget for contention", async () => {
    const root = scratch();
    fs.mkdirSync(path.join(root, MESH_CUSTODY_LOCK_NAME));
    fs.writeFileSync(path.join(root, MESH_CUSTODY_LOCK_NAME, "owner"), `legacy\n${process.pid}\n${Date.now()}\n`);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    // Host-qualified setup reads the machine identity between the call and the first attempt.
    const read = fs.readFileSync;
    let armed = true;
    vi.spyOn(fs, "readFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, options?: unknown) => {
      if (armed && String(file) === "/etc/machine-id") { armed = false; vi.setSystemTime(Date.now() + 5_000); }
      return read(file, options as undefined);
    }) as typeof fs.readFileSync);
    let settled = false;
    const pending = acquireMeshCustodyLock(root, 1_000, { hostQualified: true, ownIncarnation: undefined })
      .catch((error: unknown) => error).finally(() => { settled = true; });
    await vi.advanceTimersByTimeAsync(500);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1_000);
    const error = await pending;
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect((error as MeshLockTimeoutError).attempts).toBeGreaterThan(1);
  });
});

describe("cold lock identity is a distinct pre-lock refusal", () => {
  it.each([2, 3] as const)("protocol %s: a bounded try before identity is ready fails closed without touching .lock", async (protocol) => {
    const root = scratch();
    // Hold identity preparation open deterministically (the native reader may otherwise finish between awaits).
    let finish!: (value: string | undefined) => void;
    const preparing = new Promise<string | undefined>(done => { finish = done; });
    vi.spyOn(atomic, "ownProcessIncarnation").mockReturnValue(preparing);
    const lock = new MeshLock(root, { lockProtocol: protocol }, () => undefined);
    expect(lock.ownIncarnationReady).toBe(false);
    const operation = vi.fn();
    const tried = await lock.withTryLock(() => lock.withLock(operation), 50).catch((error: unknown) => error);
    const reduced = await lock.withLock(operation, 50).catch((error: unknown) => error);
    for (const error of [tried, reduced]) {
      expect(error).toBeInstanceOf(MeshIncarnationPendingError);
      expect(error).not.toBeInstanceOf(MeshLockTimeoutError);
      expect(error).toMatchObject({ code: "FABRIC_MESH_INCARNATION_PENDING" });
      // Identity preparation is retryable, but never attributed to lock contention.
      expect((error as Error).message.startsWith("FABRIC_MESH_INCARNATION_PENDING: ")).toBe(true);
      expect((error as Error).message).not.toMatch(/Timed out|0 attempts/);
    }
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    // An ordinary full-budget caller waits for preparation instead, then acquires.
    const ordinary = lock.withLock(() => "ordinary");
    finish(undefined);
    await expect(ordinary).resolves.toBe("ordinary");
    await lock.ownIncarnation;
    await expect(lock.withTryLock(() => lock.withLock(() => "warm"), 50)).resolves.toBe("warm");
  });

  it("protocol 1 has no identity preparation: a bounded try acquires at once", async () => {
    const root = scratch();
    const lock = new MeshLock(root, { lockProtocol: 1 }, () => undefined);
    expect(lock.ownIncarnation).toBeUndefined();
    await expect(lock.withTryLock(() => lock.withLock(() => "tried"), 0)).resolves.toBe("tried");
  });
});
