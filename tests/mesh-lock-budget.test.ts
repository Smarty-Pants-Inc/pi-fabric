import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshIncarnationPendingError, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { meshLockQueueDirectory, MeshLockTicket } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";

// Relative waits start after setup. Only eligible tickets attempt acquisition; FIFO expiry and cold identity
// are distinct pre-lock refusals, never contention timeouts with fabricated attempts.
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

/** The fake clock jumps by `ms` inside pre-lock setup: the admission receipt's queue lookup (realpath of the root). */
const slowSetup = (root: string, ms: number): void => {
  const realpath = fs.realpathSync;
  let armed = true;
  vi.spyOn(fs, "realpathSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
    if (armed && String(file) === root) { armed = false; vi.setSystemTime(Date.now() + ms); }
    return realpath(file, options as undefined);
  }) as typeof fs.realpathSync);
};
/** A live holder of `.lock` whose record no reclaimer may recover (live pid, no incarnation). */
const holdLock = (root: string): void => {
  fs.mkdirSync(path.join(root, ".lock"));
  fs.writeFileSync(path.join(root, ".lock", "owner"), `legacy\n${process.pid}\n${Date.now()}\n`);
};
/** A live FIFO predecessor receipt, sorted ahead of every new waiter. */
const predecessor = (root: string): string => {
  fs.mkdirSync(meshLockQueueDirectory(root), { recursive: true, mode: 0o700 });
  const file = path.join(meshLockQueueDirectory(root), `${"0".repeat(24)}-${process.pid}-${randomUUID()}`);
  fs.writeFileSync(file, "");
  return file;
};
const advanceWithHeartbeat = async (head: string, ms: number): Promise<void> => {
  for (let elapsed = 0; elapsed < ms; elapsed += 50) {
    const now = Date.now() / 1000;
    fs.utimesSync(head, now, now);
    await vi.advanceTimersByTimeAsync(Math.min(50, ms - elapsed));
  }
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

  it.each([1, 2, 3] as const)("protocol %s: setup leaves the full FIFO wait budget, then reports queue expiry without acquisition", async (protocol) => {
    const root = scratch();
    const lock = await ready(protocol, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const head = predecessor(root);
    slowSetup(root, 5_000);
    const operation = vi.fn();
    let settled = false;
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error).finally(() => { settled = true; });
    // Before the fix the deadline had already passed at the first loop: an instant 0-attempt timeout.
    await advanceWithHeartbeat(head, 1_000);
    expect(settled).toBe(false);
    await advanceWithHeartbeat(head, 1_500);
    const error = await pending;
    expect(error).toBeInstanceOf(MeshLockQueueExpiredError);
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
      attempts: 0, waitersAhead: 1, waitedMs: 2_000 });
    expect(operation).not.toHaveBeenCalled();
  });

  it("a waiter behind a live, heartbeating head never probes .lock, including at expiry", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const head = predecessor(root);
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const lockAttempts = (): number => mkdir.mock.calls.filter(([file]) => String(file) === path.join(root, ".lock")).length;
    const pending = lock.withLock(() => "never", 2_000).catch((error: unknown) => error);
    await advanceWithHeartbeat(head, 1_900);
    expect(lockAttempts()).toBe(0);
    await advanceWithHeartbeat(head, 300);
    const error = await pending;
    expect(error).toBeInstanceOf(MeshLockQueueExpiredError);
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
      attempts: 0, waitersAhead: 1, waitedMs: 2_000, kind: "pre-lock", retryable: true });
    expect((error as Error).message).toContain("expired in the FIFO queue before its turn, 1 waiters ahead, waited 2000 ms");
    expect((error as Error).message.startsWith("FABRIC_MESH_LOCK_QUEUE_EXPIRED (code FABRIC_MESH_LOCK_TIMEOUT): ")).toBe(true);
    expect((error as Error).message).not.toMatch(/Timed out|0 attempts/);
    expect(atomic.isMeshLockTimeout(error)).toBe(true);
    expect(atomic.isMeshWriteDeferred(error)).toBe(true);
    expect(lockAttempts()).toBe(0);
    expect(fs.existsSync(head)).toBe(true);
  });

  it("fresh-budget waiters keep FIFO order", async () => {
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
    await vi.advanceTimersByTimeAsync(200);
    await Promise.all([first, second]);
    expect(order).toEqual(["first", "second"]);
    expect(fs.readdirSync(meshLockQueueDirectory(root))).toEqual([]);
  });

  it("a free lock does not let an ineligible expired waiter jump a live head", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const head = predecessor(root);
    const operation = vi.fn();
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error);
    await advanceWithHeartbeat(head, 2_200);
    expect(await pending).toMatchObject({ preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED", attempts: 0, waitersAhead: 1 });
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it.each([1, 2, 3] as const)("protocol %s: an eligible head still gets a real acquisition attempt at expiry", async (protocol) => {
    const root = scratch();
    const lock = await ready(protocol, root);
    vi.useFakeTimers({ toFake: ["Date"] });
    const mayContend = MeshLockTicket.prototype.mayContend;
    vi.spyOn(MeshLockTicket.prototype, "mayContend").mockImplementation(function (this: MeshLockTicket) {
      const eligible = mayContend.call(this);
      vi.setSystemTime(Date.now() + 2_000); // admission/setup used the budget, but this ticket really is eligible
      return eligible;
    });
    await expect(lock.withLock(() => "head acquired", 2_000)).resolves.toBe("head acquired");
    holdLock(root);
    const error = await lock.withLock(() => "never", 2_000).catch((caught: unknown) => caught);
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect(error).not.toBeInstanceOf(MeshLockQueueExpiredError);
    expect(error).toMatchObject({ attempts: 1 });
    expect((error as Error).message).toContain("Timed out waiting");
  });

  // The reported interleaving: younger budgets expire between the live head's 10 ms probes. Expiry must
  // refuse admission instead of turning a free canonical lock into permission to barge.
  const lockMkdirs = (root: string, onAttempt?: () => void) => {
    const mkdir = fs.mkdirSync;
    const at: number[] = [];
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      if (String(file) === path.join(root, ".lock")) { at.push(Date.now()); onAttempt?.(); }
      return mkdir(file, options as undefined);
    }) as typeof fs.mkdirSync);
    return at;
  };

  it.each([1, 2])("%s ineligible expiry refusal(s) leave the live head ahead of every younger acquisition", async (waiters) => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const t0 = Date.now();
    let released = false;
    const attempts = lockMkdirs(root, () => {
      if (!released && Date.now() >= t0 + 2_005) {
        released = true;
        fs.renameSync(path.join(root, ".lock"), path.join(root, "fixture-held-lock"));
      }
    });
    const order: string[] = [];
    const head = lock.withLock(() => order.push("head"), 10_000);
    const younger = lock.withLock(() => order.push("younger"), 6_000);
    const budgets = [2_005, 2_007].slice(0, waiters);
    const expired = budgets.map((budget, index) =>
      lock.withLock(() => order.push(`expired${index}`), budget).catch((error: unknown) => error));
    await vi.advanceTimersByTimeAsync(2_100);
    await Promise.all([head, younger]);
    const refusals = await Promise.all(expired);
    expect(order).toEqual(["head", "younger"]);
    for (const [index, error] of refusals.entries()) {
      expect(error).toBeInstanceOf(MeshLockQueueExpiredError);
      expect(error).toMatchObject({ preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED", attempts: 0,
        waitersAhead: 2, waitedMs: budgets[index] });
      expect((error as Error).message.startsWith("FABRIC_MESH_LOCK_QUEUE_EXPIRED (code FABRIC_MESH_LOCK_TIMEOUT): ")).toBe(true);
      expect(attempts.filter(time => time === t0 + budgets[index]!)).toEqual([]);
    }
    expect(fs.readdirSync(meshLockQueueDirectory(root))).toEqual([]);
  });

  it("an ineligible queue expiry does not extend the live head's contention deadline", async () => {
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
    const [headError, queueError] = await Promise.all([head, waiter]);
    expect(queueError).toBeInstanceOf(MeshLockQueueExpiredError);
    expect(queueError).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 0, waitersAhead: 1, waitedMs: 2_005 });
    expect(waiterSettledAt).toBe(t0 + 2_005);
    expect(attempts.filter(time => time === t0 + 2_005)).toEqual([]);
    expect(headError).toBeInstanceOf(MeshLockTimeoutError);
    expect(headError).not.toBeInstanceOf(MeshLockQueueExpiredError);
    expect((headError as MeshLockTimeoutError).attempts).toBe(attempts.length);
    expect((headError as MeshLockTimeoutError).attempts).toBeGreaterThan(100);
    // H's deadline is fixed: queue expiry neither probes nor delays its settlement at t0 + 3000.
    expect(headSettledAt).toBe(t0 + 3_000);
    expect(Math.max(...attempts)).toBeLessThanOrEqual(t0 + 3_000);
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
