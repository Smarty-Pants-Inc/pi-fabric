import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { LockStats, LockStatsFile } from "../src/mesh/commit-stats.js";

const lockKey = Symbol.for("pi-fabric.mesh.lock-stats");
const eventsKey = Symbol.for("pi-fabric.mesh.lock-events");
const registry = globalThis as typeof globalThis & {
  [lockKey]?: { stats: LockStats | undefined; flush(): void; dispose(): void };
  [eventsKey]?: { dispose(): void };
};
let root: string;
let locks: typeof import("../src/mesh/mesh-lock.js");
let queue: typeof import("../src/mesh/lock-queue.js");

beforeEach(async () => {
  registry[lockKey]?.dispose();
  registry[eventsKey]?.dispose();
  delete registry[lockKey];
  delete registry[eventsKey];
  vi.stubEnv("PI_FABRIC_LOCK_STATS", "1");
  vi.resetModules();
  locks = await import("../src/mesh/mesh-lock.js");
  queue = await import("../src/mesh/lock-queue.js");
  root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-lock-wait-phases-"));
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
});
afterEach(() => {
  registry[lockKey]?.dispose();
  registry[eventsKey]?.dispose();
  delete registry[lockKey];
  delete registry[eventsKey];
  vi.restoreAllMocks();
  vi.useRealTimers();
  vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});

const recorded = () => {
  registry[lockKey]!.flush();
  const directory = path.join(root, "lock-stats");
  const file = fs.readdirSync(directory).find(name => name.endsWith(".json"))!;
  const contents = JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")) as LockStatsFile;
  expect(contents.version).toBe(1);
  expect(contents.minutes).toHaveLength(1);
  return contents.minutes[0]!;
};

// Keep reason: recorded incident #19677. Setup can outlive the relative timeout, while
// FIFO refusals must remain distinguishable from canonical contention. Use real lock,
// receipt and recorder paths, advancing only diagnostic clocks and the admission seam.
const instrument = () => {
  let now = 0;
  let wall = Date.now();
  vi.spyOn(performance, "now").mockImplementation(() => now);
  vi.spyOn(Date, "now").mockImplementation(() => wall);
  const mkdir = fs.mkdirSync;
  vi.spyOn(fs, "mkdirSync").mockImplementation(((...args: Parameters<typeof fs.mkdirSync>) => {
    if (String(args[0]) === root) now += 10;
    if (String(args[0]) === path.join(root, ".lock")) now += 17;
    return mkdir(...args);
  }) as typeof fs.mkdirSync);
  const write = fs.writeFileSync;
  vi.spyOn(fs, "writeFileSync").mockImplementation((...args: Parameters<typeof fs.writeFileSync>) => {
    if (String(args[0]) === path.join(root, ".lock", "owner")) now += 19;
    return write(...args);
  });
  const close = queue.MeshLockTicket.prototype.close;
  const closed = vi.spyOn(queue.MeshLockTicket.prototype, "close").mockImplementation(function (this: InstanceType<typeof queue.MeshLockTicket>) {
    const result = close.call(this);
    now += 100; // Advisory cleanup is deliberately expensive, outside the wait phases.
    return result;
  });
  const phase = vi.spyOn(locks.lockStats!, "phase");
  const acquired = vi.spyOn(locks.lockStats!, "acquired");
  const failed = vi.spyOn(locks.lockStats!, "failed");
  return {
    advance: (ms: number) => { now += ms; },
    expire: () => { wall += 101; },
    closed, phase, acquired, failed,
    assertClean: () => {
      expect(closed).toHaveBeenCalledTimes(1);
      expect(fs.readdirSync(queue.meshLockQueueDirectory(root))).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      for (const call of phase.mock.invocationCallOrder) {
        expect(call).toBeGreaterThan(closed.mock.invocationCallOrder[0]!);
      }
    },
  };
};

const admitAfterWait = (clock: ReturnType<typeof instrument>) => {
  let probes = 0;
  vi.spyOn(queue.MeshLockTicket.prototype, "mayContend").mockImplementation(() => {
    clock.advance(++probes === 1 ? 11 : 13);
    return probes > 1;
  });
  vi.spyOn(queue.MeshLockTicket.prototype, "wait").mockImplementation(async () => { clock.advance(5); });
};

const expectPhase = (minute: LockStatsFile["minutes"][number], operation: string, phase: string, ms: number) => {
  expect(minute.phases?.[operation]?.[phase]).toMatchObject({ n: 1, holdMs: ms, holdMaxMs: ms });
};

describe("mesh lock wait timing phases", () => {
  it.each([false, true])("splits successful admission, including a throwing operation (%s), without changing aggregates", async (throws) => {
    const clock = instrument();
    admitAfterWait(clock);
    const lock = new locks.MeshLock(root, { lockTimeoutMs: 100, lockHeldEventMs: 0 }, () => undefined);
    const operation = () => {
      clock.advance(23);
      if (throws) throw new Error("operation refused");
      return 7;
    };
    // Untagged custody must remain comparable with publish, not disappear from the split.
    const pending = lock.withLock(operation, undefined, "custody");
    if (throws) await expect(pending).rejects.toThrow("operation refused");
    else expect(await pending).toBe(7);
    clock.assertClean();
    expect(fs.existsSync(lock.lockPath)).toBe(false);
    const minute = recorded();
    expect(minute.classes.custody).toMatchObject({ n: 1, waitMs: 75, holdMs: 23, timeouts: 0, tries: 0 });
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-setup", 10);
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-admission", 29);
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-acquire", 36);
    expect(clock.failed).not.toHaveBeenCalled();
  });

  it("keeps caller-supplied operation and hold phase timing alongside the wait phases", async () => {
    const clock = instrument();
    admitAfterWait(clock);
    const lock = new locks.MeshLock(root, { lockTimeoutMs: 100 }, () => undefined);
    await lock.withLock(() => { clock.advance(23); }, undefined, "bridge", false,
      { operation: "bridge-publication", phase: "event-lock" });
    clock.assertClean();
    const minute = recorded();
    expect(minute.operations?.["bridge-publication"]).toMatchObject({ n: 1, holdMs: 23 });
    expectPhase(minute, "bridge-publication", "event-lock", 23);
    expectPhase(minute, "bridge-publication", "mesh-lock-setup", 10);
    expectPhase(minute, "bridge-publication", "mesh-lock-admission", 29);
    expectPhase(minute, "bridge-publication", "mesh-lock-acquire", 36);
  });

  it("records queue expiry with no canonical phase or contention count, excluding refusal cleanup", async () => {
    const clock = instrument();
    vi.spyOn(queue.MeshLockTicket.prototype, "mayContend").mockImplementation(() => { clock.advance(11); return false; });
    vi.spyOn(queue.MeshLockTicket.prototype, "wait").mockImplementation(async () => {
      clock.advance(5);
      clock.expire();
    });
    const event = vi.spyOn(locks.lockEvents!, "queueExpired").mockImplementation(() => {
      expect(clock.closed).toHaveBeenCalledTimes(1);
      clock.advance(100); // Event I/O is not FIFO admission either.
    });
    const timeout = vi.spyOn(locks.lockEvents!, "waitTimeout");
    const lock = new locks.MeshLock(root, { lockTimeoutMs: 100 }, () => undefined);
    const operation = vi.fn();
    await expect(lock.withLock(operation, undefined, "publish")).rejects.toBeInstanceOf(locks.MeshLockQueueExpiredError);
    // expire() closes admission before its event; finally's second close is intentionally idempotent.
    expect(clock.closed).toHaveBeenCalledTimes(2);
    expect(fs.readdirSync(queue.meshLockQueueDirectory(root))).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lock.lockPath)).toBe(false);
    expect(event).toHaveBeenCalledTimes(1);
    expect(timeout).not.toHaveBeenCalled();
    expect(clock.acquired).not.toHaveBeenCalled();
    expect(clock.failed).not.toHaveBeenCalled();
    const minute = recorded();
    expect(minute.classes).toEqual({});
    expectPhase(minute, "mesh-lock-publish", "mesh-lock-setup", 10);
    expectPhase(minute, "mesh-lock-publish", "mesh-lock-admission", 27);
    expect(minute.phases?.["mesh-lock-publish"]?.["mesh-lock-acquire"]).toBeUndefined();
    for (const call of clock.phase.mock.invocationCallOrder) {
      expect(call).toBeGreaterThan(clock.closed.mock.invocationCallOrder.at(-1)!);
    }
  });

  it("closes a cancelled queue wait before recording phases without inventing contention", async () => {
    const clock = instrument();
    const controller = new AbortController();
    const refusal = new Error("publication cancelled");
    vi.spyOn(queue.MeshLockTicket.prototype, "mayContend").mockImplementation(() => { clock.advance(11); return false; });
    vi.spyOn(queue.MeshLockTicket.prototype, "wait").mockImplementation(async (_ms, signal) => {
      clock.advance(5);
      controller.abort(refusal);
      signal!.throwIfAborted();
    });
    const lock = new locks.MeshLock(root, { lockTimeoutMs: 100, writeSignal: controller.signal }, () => undefined);
    await expect(lock.withLock(() => undefined, undefined, "custody")).rejects.toBe(refusal);
    clock.assertClean();
    expect(clock.acquired).not.toHaveBeenCalled();
    expect(clock.failed).not.toHaveBeenCalled();
    const minute = recorded();
    expect(minute.classes).toEqual({});
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-setup", 10);
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-admission", 16);
    expect(minute.phases?.["mesh-lock-custody"]?.["mesh-lock-acquire"]).toBeUndefined();
  });

  it.each([
    { lockClass: "put/delete", operationName: "mesh-lock-put-delete" },
    { lockClass: "heartbeat/confirm", operationName: "mesh-lock-heartbeat-confirm" },
    { lockClass: "writeBatch", operationName: "mesh-lock-writebatch" },
  ] as const)("records a canonical timeout separately and sanitizes $lockClass", async ({ lockClass, operationName }) => {
    fs.mkdirSync(path.join(root, ".lock"));
    fs.writeFileSync(path.join(root, ".lock", "owner"), `held\n${process.pid}\n${Date.now()}\n`);
    const clock = instrument();
    vi.spyOn(queue.MeshLockTicket.prototype, "mayContend").mockImplementation(() => { clock.advance(11); return true; });
    const lock = new locks.MeshLock(root, { lockTimeoutMs: 100 }, () => undefined);
    vi.spyOn(lock, "clearStaleLock").mockImplementation(async () => {
      clock.advance(19);
      clock.expire();
      return false;
    });
    const event = vi.spyOn(locks.lockEvents!, "waitTimeout").mockImplementation(() => undefined);
    const operation = vi.fn();
    await expect(lock.withLock(operation, undefined, lockClass)).rejects.toMatchObject({ attempts: 1 });
    clock.assertClean();
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(lock.lockPath)).toBe(true); // The live holder is untouched.
    expect(event).toHaveBeenCalledTimes(1);
    const minute = recorded();
    expect(minute.classes[lockClass]).toMatchObject({ n: 0, timeouts: 1, tries: 0, failedWaitMs: 57 });
    expectPhase(minute, operationName, "mesh-lock-setup", 10);
    expectPhase(minute, operationName, "mesh-lock-admission", 11);
    expectPhase(minute, operationName, "mesh-lock-acquire", 36);
  });

  it("makes awaited identity setup outside the relative acquisition budget visible", async () => {
    const atomic = await import("../src/core/atomic-write.js");
    let resolve!: (value: string | undefined) => void;
    vi.spyOn(atomic, "ownProcessIncarnation").mockReturnValue(new Promise(done => { resolve = done; }));
    let now = 0;
    let wall = Date.now();
    vi.spyOn(performance, "now").mockImplementation(() => now);
    vi.spyOn(Date, "now").mockImplementation(() => wall);
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((...args: Parameters<typeof fs.renameSync>) => {
      if (String(args[1]) === path.join(root, ".lock")) now += 7;
      return rename(...args);
    });
    const lock = new locks.MeshLock(root, { lockProtocol: 2, lockTimeoutMs: 100 }, () => undefined);
    const pending = lock.withLock(() => { now += 3; return 7; }, undefined, "custody");
    now += 30_000;
    wall += 30_000;
    resolve(undefined);
    expect(await pending).toBe(7);
    const minute = recorded();
    expect(minute.classes.custody).toMatchObject({ n: 1, waitMs: 30_007, holdMs: 3, timeouts: 0 });
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-setup", 30_000);
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-admission", 0);
    expectPhase(minute, "mesh-lock-custody", "mesh-lock-acquire", 7);
    expect(fs.existsSync(lock.lockPath)).toBe(false);
    expect(fs.readdirSync(queue.meshLockQueueDirectory(root))).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not read the diagnostic clock or write stats when disabled", async () => {
    registry[lockKey]!.dispose();
    registry[eventsKey]!.dispose();
    delete registry[lockKey];
    delete registry[eventsKey];
    vi.stubEnv("PI_FABRIC_LOCK_STATS", "0");
    vi.resetModules();
    const disabled = await import("../src/mesh/mesh-lock.js");
    const clock = vi.spyOn(performance, "now");
    const lock = new disabled.MeshLock(root, { lockTimeoutMs: 100 }, () => undefined);
    expect(await lock.withLock(() => 7, undefined, "custody", false, { operation: "custody-check" })).toBe(7);
    expect(clock).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, "lock-stats"))).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });
});
