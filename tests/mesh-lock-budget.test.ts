import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { acquireMeshCustodyLock, MESH_CUSTODY_LOCK_NAME } from "../src/mesh/custody-lock.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockIdentityNotReadyError } from "../src/mesh/mesh-lock.js";

// A mesh lock's relative wait budget starts at its first admission/acquisition attempt, after setup; a contention
// timeout is reported only after at least one acquisition attempt; a cold identity refusal is a distinct pre-lock error.
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

  it.each([1, 2, 3] as const)("protocol %s: a queued waiter whose setup consumed its budget still waits it, then reports attempts > 0", async (protocol) => {
    const root = scratch();
    const lock = await ready(protocol, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    predecessor(root);
    slowSetup(root, 5_000);
    const operation = vi.fn();
    let settled = false;
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error).finally(() => { settled = true; });
    // Before the fix the deadline had already passed at the first loop: an instant 0-attempt timeout.
    await vi.advanceTimersByTimeAsync(1_000);
    expect(settled).toBe(false);
    await vi.advanceTimersByTimeAsync(1_500);
    const error = await pending;
    expect(error).toBeInstanceOf(MeshLockTimeoutError);
    expect(error).not.toBeInstanceOf(MeshLockIdentityNotReadyError);
    expect((error as MeshLockTimeoutError).attempts).toBeGreaterThan(0);
    expect(operation).not.toHaveBeenCalled();
  });

  it("a waiter queued behind a live, heartbeating head never probes .lock early, and tries exactly once at expiry", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const head = predecessor(root);
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const lockAttempts = (): number => mkdir.mock.calls.filter(([file]) => String(file) === path.join(root, ".lock")).length;
    const pending = lock.withLock(() => "never", 2_000).catch((error: unknown) => error);
    for (let elapsed = 0; elapsed < 1_900; elapsed += 50) {
      const now = Date.now() / 1000;
      fs.utimesSync(head, now, now); // the head keeps proving progress: FIFO holds this waiter back
      await vi.advanceTimersByTimeAsync(50);
    }
    expect(lockAttempts()).toBe(0); // normal FIFO fairness is unchanged within the budget
    for (let elapsed = 0; elapsed < 300; elapsed += 50) {
      const now = Date.now() / 1000;
      fs.utimesSync(head, now, now);
      await vi.advanceTimersByTimeAsync(50);
    }
    const error = await pending;
    expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 1 });
    expect(lockAttempts()).toBe(1);
    expect(fs.existsSync(head)).toBe(true);
  });

  it("fresh-budget waiters keep FIFO order: no final-attempt barging while their budgets last", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const order: string[] = [];
    const first = lock.withLock(() => order.push("first"), 10_000);
    const second = lock.withLock(() => order.push("second"), 10_000);
    await vi.advanceTimersByTimeAsync(3_000);
    expect(order).toEqual([]);
    fs.rmSync(path.join(root, ".lock"), { recursive: true });
    await vi.advanceTimersByTimeAsync(200);
    await Promise.all([first, second]);
    expect(order).toEqual(["first", "second"]);
    expect(fs.readdirSync(meshLockQueueDirectory(root))).toEqual([]);
  });

  it("the expiring queued waiter's one attempt takes a lock that is free by then", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    const head = predecessor(root);
    const pending = lock.withLock(() => "acquired", 2_000);
    for (let elapsed = 0; elapsed < 2_200; elapsed += 50) {
      const now = Date.now() / 1000;
      fs.utimesSync(head, now, now);
      await vi.advanceTimersByTimeAsync(50);
    }
    await expect(pending).resolves.toBe("acquired");
  });

  // Fairness envelope of the expiry attempt, against a live FIFO head H that is already contending (fake clock, no
  // sleeps). H's budget is 10 s, so it is head and probes .lock every 10 ms; bargers B are gated (>= 2000 ms) and
  // queued behind H, so they only reach .lock through the one expiry attempt. B budgets of 2005/2007 ms make their
  // expiry attempts fall strictly between H's 10 ms probes (H at 2000 and 2010): a real supported interleaving.
  const lockMkdirs = (root: string, onAttempt?: () => void) => {
    const mkdir = fs.mkdirSync;
    const at: number[] = [];
    vi.spyOn(fs, "mkdirSync").mockImplementation(((file: fs.PathLike, options?: unknown) => {
      if (String(file) === path.join(root, ".lock")) { at.push(Date.now()); onAttempt?.(); }
      return mkdir(file, options as undefined);
    }) as typeof fs.mkdirSync);
    return at;
  };

  it.each([1, 2])("%s expired barger(s) take at most one attempt each and never push a live head past its own budget", async (bargers) => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root);
    const t0 = Date.now();
    // The holder releases at the first probe on or after t0 + 2005: the earliest barger's expiry attempt, not H's.
    let released = false;
    const attempts = lockMkdirs(root, () => {
      if (!released && Date.now() >= t0 + 2_005) { released = true; fs.rmSync(path.join(root, ".lock"), { recursive: true }); }
    });
    const order: string[] = [];
    let headAt = -1;
    const head = lock.withLock(() => { order.push("head"); headAt = Date.now(); }, 10_000);
    const budgets = [2_005, 2_007].slice(0, bargers);
    const acquiredAt: number[] = [];
    const barged = budgets.map((budget, index) =>
      lock.withLock(() => { order.push(`barger${index}`); acquiredAt[index] = Date.now(); }, budget));
    await vi.advanceTimersByTimeAsync(1_990);
    // FIFO within the budgets: only H has probed .lock so far, the bargers are held back.
    expect(order).toEqual([]);
    const headProbes = attempts.length;
    expect(headProbes).toBeGreaterThan(0);
    await vi.advanceTimersByTimeAsync(100);
    await Promise.all([head, ...barged]);
    // The barge is real: the first barger acquires with its expiry attempt, exactly at its own deadline. A later
    // barger's one attempt may land before or after H (H is also woken by the real release watcher), never more.
    expect(order[0]).toBe("barger0");
    expect(order.slice().sort()).toEqual([...budgets.map((_, index) => `barger${index}`), "head"]);
    expect(acquiredAt[0]).toBe(t0 + 2_005);
    // A second barger acquires no later than its own expiry attempt; if H was woken by the release first, it may
    // instead have become the FIFO head behind H and acquired as such (ordinary admission, not a barge).
    if (bargers === 2) expect(acquiredAt[1]).toBeGreaterThanOrEqual(t0 + 2_005);
    if (bargers === 2) expect(acquiredAt[1]).toBeLessThanOrEqual(t0 + 2_007);
    if (bargers === 2 && order.indexOf("barger1") < order.indexOf("head")) expect(acquiredAt[1]).toBe(t0 + 2_007);
    // H is delayed at most until its own next probe after the barge (2010), far inside its own 10 s budget: the
    // barge reset nothing and cost H only the bargers' synchronous critical sections.
    expect(headAt).toBeGreaterThanOrEqual(t0 + 2_005);
    expect(headAt).toBeLessThanOrEqual(t0 + 2_010);
    expect(fs.readdirSync(meshLockQueueDirectory(root))).toEqual([]);
  });

  it("an expired barger that loses changes nothing for the head: it still settles at its own deadline with real attempts", async () => {
    const root = scratch();
    const lock = await ready(1, root);
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    holdLock(root); // never released
    const t0 = Date.now();
    const attempts = lockMkdirs(root);
    let headSettledAt = -1;
    const head = lock.withLock(() => "never", 3_000).catch((error: unknown) => error)
      .finally(() => { headSettledAt = Date.now(); });
    let bargerSettledAt = -1;
    const barger = lock.withLock(() => "never", 2_005).catch((error: unknown) => error)
      .finally(() => { bargerSettledAt = Date.now(); });
    await vi.advanceTimersByTimeAsync(3_100);
    const [headError, bargerError] = await Promise.all([head, barger]);
    expect(bargerError).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 1 });
    expect(bargerSettledAt).toBe(t0 + 2_005);
    expect(attempts.filter(time => time === t0 + 2_005)).toHaveLength(1); // the barger's one attempt, nothing more
    expect(headError).toBeInstanceOf(MeshLockTimeoutError);
    expect((headError as MeshLockTimeoutError).attempts).toBe(attempts.length - 1);
    expect((headError as MeshLockTimeoutError).attempts).toBeGreaterThan(100);
    // H's deadline was never reset or extended by the barge: it settles at exactly t0 + 3000.
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
      expect(error).toBeInstanceOf(MeshLockIdentityNotReadyError);
      // Safe callers keep their release-and-retry handling: still the typed timeout family and code.
      expect(error).toBeInstanceOf(MeshLockTimeoutError);
      expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", preLockCode: "FABRIC_MESH_LOCK_IDENTITY_NOT_READY",
        kind: "pre-lock", attempts: 0, lockProtocol: protocol });
      // It never claims a contention timeout.
      expect((error as Error).message.startsWith("FABRIC_MESH_LOCK_IDENTITY_NOT_READY (code FABRIC_MESH_LOCK_TIMEOUT): ")).toBe(true);
      expect((error as Error).message).not.toMatch(/Timed out|0 attempts/);
      expect((error as Error).message).toContain("not attempted");
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
