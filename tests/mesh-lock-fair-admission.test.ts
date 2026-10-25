import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";

// Fair mesh lock admission (smarty-dev#6477 L0b): wake-on-handoff, predecessor-only waiting,
// and no fallback barging while the queue head is alive and making progress.
const roots: string[] = [];
const tickets: MeshLockTicket[] = [];
const scratch = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-fair-"));
  roots.push(root);
  return root;
};
const ticket = (root: string, budgetMs: number) => {
  const created = new MeshLockTicket(root, randomUUID(), budgetMs);
  tickets.push(created);
  return created;
};
const names = (root: string) => fs.readdirSync(meshLockQueueDirectory(root)).sort();
const mtime = (root: string, name: string) => fs.statSync(path.join(meshLockQueueDirectory(root), name)).mtimeMs;

// Real admission and filesystem receipts; only notification delivery/setup and the clock are controlled.
class WatchHandle extends EventEmitter {
  readonly close = vi.fn();
  ref(): this { return this; }
  unref(): this { return this; }
}
const watchNotifications = (onOpen?: () => void) => {
  const handles: WatchHandle[] = [];
  const attempts: string[] = [];
  let failNext = false;
  vi.spyOn(fs, "watch").mockImplementation(((target: fs.PathLike, _options: fs.WatchOptions,
    listener: (event: string, filename: string | null) => void) => {
    attempts.push(String(target));
    if (failNext) {
      failNext = false;
      throw Object.assign(new Error("watch unavailable"), { code: "ENOSPC" });
    }
    const handle = new WatchHandle();
    handle.on("change", listener);
    handles.push(handle);
    onOpen?.();
    return handle;
  }) as unknown as typeof fs.watch);
  return { handles, attempts, failNextOpen: () => { failNext = true; } };
};
afterEach(() => {
  for (const created of tickets.splice(0)) created.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("fair mesh lock admission", () => {
  it("wakes a waiter as soon as its predecessor's receipt goes, not at its next poll", async () => {
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    expect(waiter.mayContend()).toBe(false);
    const started = performance.now();
    const woken = waiter.wait(5000);
    setTimeout(() => head.close(), 20);
    await woken;
    expect(performance.now() - started).toBeLessThan(2500);
    expect(waiter.mayContend()).toBe(true);
  });

  it("wakes the head when .lock is released", async () => {
    const root = scratch();
    const lock = path.join(root, ".lock");
    fs.mkdirSync(lock);
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const started = performance.now();
    const woken = head.wait(5000);
    setTimeout(() => fs.renameSync(lock, `${lock}.released.x`), 20);
    await woken;
    expect(performance.now() - started).toBeLessThan(2500);
  });

  it("non-heads stat only their predecessor and list the queue only when it goes", () => {
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    expect(waiter.mayContend()).toBe(false);
    const readdir = vi.spyOn(fs, "readdirSync");
    for (let i = 0; i < 10; i++) expect(waiter.mayContend()).toBe(false);
    expect(readdir).not.toHaveBeenCalled();
    head.close();
    expect(waiter.mayContend()).toBe(true);
    expect(readdir).toHaveBeenCalledOnce();
    readdir.mockRestore();
  });

  it("admits strictly in receipt order and reaps a dead receipt in the middle", () => {
    const root = scratch();
    const first = ticket(root, 10_000);
    const dead = path.join(meshLockQueueDirectory(root), `${process.hrtime.bigint().toString().padStart(24, "0")}-999999999-${randomUUID()}`);
    fs.writeFileSync(dead, "");
    const second = ticket(root, 10_000);
    const third = ticket(root, 10_000);
    expect([first.mayContend(), second.mayContend(), third.mayContend()]).toEqual([true, false, false]);
    expect(fs.existsSync(dead)).toBe(false);
    first.close();
    expect([third.mayContend(), second.mayContend()]).toEqual([false, true]);
    second.close();
    expect(third.mayContend()).toBe(true);
  });

  it("does not fall back while the head is alive and making progress, even past 80% of the budget", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 2000); // fallback point 1600 ms, stall bound 200 ms
    expect(head.mayContend()).toBe(true);
    for (let elapsed = 0; elapsed < 2000; elapsed += 20) {
      vi.advanceTimersByTime(20);
      expect(head.mayContend()).toBe(true); // the head probes, and its receipt heartbeat stays fresh
      expect(waiter.mayContend()).toBe(false);
    }
    expect(waiter.queued).toBe(true);
  });

  it("falls back past 80% of the budget when the live head stops making progress", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const waiter = ticket(root, 2000);
    expect(head.mayContend()).toBe(true);
    vi.advanceTimersByTime(1500);
    expect(waiter.mayContend()).toBe(false); // before the fallback point: never
    vi.advanceTimersByTime(200);
    // Past 1600 ms: the head's heartbeat is 1700 ms old, but this is our first sight of it.
    expect(waiter.mayContend()).toBe(false);
    vi.advanceTimersByTime(250);
    // Still the same head and no heartbeat beyond the 200 ms stall bound: plain contest.
    expect(waiter.mayContend()).toBe(true);
    expect(waiter.queued).toBe(false);
    expect(names(root)).toHaveLength(1); // the live stalled head's receipt is never reaped early
  });

  it("short try budgets keep the unconditional 80% fallback", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const tryer = ticket(root, 50);
    expect(head.mayContend()).toBe(true);
    expect(tryer.mayContend()).toBe(false);
    vi.advanceTimersByTime(40);
    expect(head.mayContend()).toBe(true);
    expect(tryer.mayContend()).toBe(true);
  });

  it("judges a head that has just taken over by first sight, not by its old creation time", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const next = ticket(root, 10_000);
    const waiter = ticket(root, 2000);
    const successor = names(root)[1]!;
    expect(head.mayContend()).toBe(true);
    for (let elapsed = 0; elapsed < 1700; elapsed += 20) {
      vi.advanceTimersByTime(20);
      expect(head.mayContend()).toBe(true);
    }
    head.close(); // handoff: the successor's receipt is 1700 ms old and it has not probed yet
    expect(Date.now() - mtime(root, successor)).toBeGreaterThan(1600);
    expect(waiter.mayContend()).toBe(false);
    vi.advanceTimersByTime(250); // the new head never makes progress: now it is stalled
    expect(waiter.mayContend()).toBe(true);
    expect(next.queued).toBe(true);
  });

  it("an aborted wait rejects and leaves no timer", async () => {
    const root = scratch();
    ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    expect(waiter.mayContend()).toBe(false);
    const abort = new AbortController();
    const pending = waiter.wait(60_000, abort.signal).catch(error => error);
    abort.abort(new Error("cancel"));
    expect(await pending).toMatchObject({ message: "cancel" });
  });

  it("an async watch error wakes admission, failed reopen uses 20 ms, and a fresh watch restores notifications", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const canonical = path.join(root, ".lock");
    const owner = `live-holder\n${process.pid}\n${Date.now()}\n`;
    fs.mkdirSync(canonical);
    fs.writeFileSync(path.join(canonical, "owner"), owner);
    const watches = watchNotifications();
    const eligibility = vi.spyOn(MeshLockTicket.prototype, "mayContend");
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const attempts = () => mkdir.mock.calls.filter(([file]) => String(file) === canonical).length;
    const abort = new AbortController();
    const lock = new MeshLock(root, { writeSignal: abort.signal }, () => undefined);
    const operation = vi.fn(() => "admitted");
    const pending = lock.withLock(operation).catch((error: unknown) => error);
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(watches.handles).toHaveLength(1);
      expect(watches.attempts).toEqual([path.join(meshLockQueueDirectory(root), names(root)[0]!)]);
      watches.failNextOpen();
      watches.handles[0]!.emit("error", new Error("notification lost"));
      await vi.advanceTimersByTimeAsync(0);
      // Error is a wake, not permission: a failed reopen must not strand us on a cached healthy identity.
      expect(watches.attempts).toHaveLength(2);
      expect(operation).not.toHaveBeenCalled();
      expect(attempts()).toBe(0);
      expect(names(root)).toHaveLength(2);
      expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
      expect(watches.handles[0]!.close).toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(19);
      expect(watches.attempts).toHaveLength(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(watches.handles).toHaveLength(2); // failed-watch fallback was exactly 20 ms
      const checks = eligibility.mock.calls.length;
      await vi.advanceTimersByTimeAsync(19);
      expect(eligibility).toHaveBeenCalledTimes(checks); // retain the existing follower safety-net cadence
      await vi.advanceTimersByTimeAsync(1);
      expect(eligibility).toHaveBeenCalledTimes(checks + 1); // a dropped event retains the existing 20 ms poll
      expect(attempts()).toBe(0);
      head.close();
      fs.renameSync(canonical, path.join(root, "released-fixture"));
      watches.handles[1]!.emit("change", "rename", null);
      await vi.advanceTimersByTimeAsync(0);
      expect(await pending).toBe("admitted"); // actual canonical acquisition, not a mocked permission decision
      expect(operation).toHaveBeenCalledOnce();
      expect(attempts()).toBe(1);
      expect(fs.existsSync(canonical)).toBe(false);
      expect(names(root)).toEqual([]);
      expect(watches.handles.every(handle => handle.close.mock.calls.length > 0)).toBe(true);
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      abort.abort(new Error("test cleanup"));
      await vi.advanceTimersByTimeAsync(100);
      await pending;
    }
  });

  it("a stale old watch error cannot close, invalidate, or wake its healthy replacement", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    const predecessor = ticket(root, 10_000);
    const waiter = ticket(root, 10_000);
    const watches = watchNotifications();
    const abort = new AbortController();
    const waits: Promise<unknown>[] = [];
    try {
      expect(waiter.mayContend()).toBe(false);
      const first = waiter.wait(100, abort.signal).catch((error: unknown) => error);
      waits.push(first);
      await vi.advanceTimersByTimeAsync(0);
      const old = watches.handles[0]!;
      predecessor.close();
      old.emit("change", "rename", null);
      await first;
      expect(waiter.mayContend()).toBe(false); // promotion changes the immediate predecessor to head
      let woken = false;
      const replacement = waiter.wait(100, abort.signal).then(() => { woken = true; }).catch((error: unknown) => error);
      waits.push(replacement);
      await vi.advanceTimersByTimeAsync(0);
      const healthy = watches.handles[1]!;
      expect(old.close).toHaveBeenCalled();
      old.emit("error", new Error("late error from replaced handle"));
      await vi.advanceTimersByTimeAsync(0);
      expect(healthy.close).not.toHaveBeenCalled();
      expect(woken).toBe(false);
      await vi.advanceTimersByTimeAsync(99);
      expect(woken).toBe(false);
      healthy.emit("change", "change", null); // spurious heartbeat is a wake, never permission
      await replacement;
      expect(waiter.mayContend()).toBe(false);
      let handedOff = false;
      const resumed = waiter.wait(100, abort.signal).then(() => { handedOff = true; }).catch((error: unknown) => error);
      waits.push(resumed);
      await vi.advanceTimersByTimeAsync(0);
      expect(watches.attempts).toHaveLength(2); // late error did not invalidate the cached replacement identity
      expect(healthy.close).not.toHaveBeenCalled();
      head.close();
      healthy.emit("change", "rename", null);
      await vi.advanceTimersByTimeAsync(0);
      expect(handedOff).toBe(true); // healthy replacement still delivers immediate handoff
      expect(waiter.mayContend()).toBe(true);
      waiter.close();
      expect(names(root)).toEqual([]);
      expect(healthy.close).toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      abort.abort(new Error("test cleanup"));
      await vi.advanceTimersByTimeAsync(100);
      await Promise.all(waits);
    }
  });

  it("watch setup exhausting the fixed 2 s budget expires immediately with zero canonical attempts", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const headName = names(root)[0]!;
    const watches = watchNotifications(() => {
      vi.setSystemTime(Date.now() + 2_000);
      expect(head.mayContend()).toBe(true); // live heartbeat prevents ordinary stalled-head fallback
    });
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const abort = new AbortController();
    const lock = new MeshLock(root, { writeSignal: abort.signal }, () => undefined);
    const operation = vi.fn();
    let settled = false;
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error).finally(() => { settled = true; });
    try {
      await vi.advanceTimersByTimeAsync(0); // drain continuations without granting another millisecond
      expect(watches.handles).toHaveLength(1);
      expect(settled).toBe(true);
      expect(await pending).toBeInstanceOf(MeshLockQueueExpiredError);
      expect(await pending).toMatchObject({ kind: "pre-lock", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
        code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 0, waitersAhead: 1, waitedMs: 2_000 });
      expect(operation).not.toHaveBeenCalled();
      expect(mkdir.mock.calls.filter(([file]) => String(file) === path.join(root, ".lock"))).toEqual([]);
      expect(names(root)).toEqual([headName]);
      expect(watches.handles[0]!.close).toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      abort.abort(new Error("test cleanup"));
      await vi.advanceTimersByTimeAsync(100);
      await pending;
    }
  });

  it.each([1, 2, 3] as const)("protocol %s: slow release-watch setup cannot acquire past the deadline", async (protocol) => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const canonical = path.join(root, ".lock");
    fs.mkdirSync(canonical);
    fs.writeFileSync(path.join(canonical, "owner"), `live-holder\n${process.pid}\n${Date.now()}\n`);
    const lock = new MeshLock(root, { lockProtocol: protocol }, () => undefined);
    await lock.ownIncarnation;
    const watches = watchNotifications(() => {
      vi.setSystemTime(Date.now() + 2_001);
      fs.renameSync(canonical, path.join(root, "released-fixture"));
    });
    const operation = vi.fn(() => "acquired after budget");
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    expect(result).toBeInstanceOf(MeshLockTimeoutError);
    expect(result).not.toBeInstanceOf(MeshLockQueueExpiredError);
    expect(result).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", attempts: 1 });
    expect(operation).not.toHaveBeenCalled();
    expect(watches.attempts).toEqual([root]);
    expect(watches.handles[0]!.close).toHaveBeenCalled();
    expect(names(root)).toEqual([]);
    expect(fs.existsSync(canonical)).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([1, 2, 3] as const)("protocol %s: promotion during slow predecessor-watch setup cannot acquire past the deadline", async (protocol) => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const predecessor = path.join(meshLockQueueDirectory(root), names(root)[0]!);
    const canonical = path.join(root, ".lock");
    fs.mkdirSync(canonical);
    fs.writeFileSync(path.join(canonical, "owner"), `live-holder\n${process.pid}\n${Date.now()}\n`);
    const lock = new MeshLock(root, { lockProtocol: protocol }, () => undefined);
    await lock.ownIncarnation;
    const watches = watchNotifications(() => {
      vi.setSystemTime(Date.now() + 2_001);
      head.close(); // eligible on return, but only after its fixed acquisition budget
      fs.renameSync(canonical, path.join(root, "released-fixture"));
    });
    const operation = vi.fn(() => "acquired after budget");
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    expect(result).toBeInstanceOf(MeshLockQueueExpiredError);
    expect(result).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
      attempts: 0, waitedMs: 2_001, kind: "pre-lock" });
    expect(operation).not.toHaveBeenCalled();
    expect(watches.attempts).toEqual([predecessor]);
    expect(watches.handles[0]!.close).toHaveBeenCalled();
    expect(names(root)).toEqual([]);
    expect(fs.existsSync(canonical)).toBe(false);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("abort during synchronous watch setup rejects with the exact reason without sleeping or leaking admission", async () => {
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
    const root = scratch();
    const head = ticket(root, 10_000);
    expect(head.mayContend()).toBe(true);
    const headName = names(root)[0]!;
    const abort = new AbortController();
    const reason = new Error("cancelled while fs.watch was opening");
    const added = vi.spyOn(abort.signal, "addEventListener");
    const removed = vi.spyOn(abort.signal, "removeEventListener");
    const watches = watchNotifications(() => abort.abort(reason));
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const lock = new MeshLock(root, { writeSignal: abort.signal }, () => undefined);
    const operation = vi.fn();
    let settled = false;
    const pending = lock.withLock(operation, 2_000).catch((error: unknown) => error).finally(() => { settled = true; });
    try {
      await vi.advanceTimersByTimeAsync(0);
      expect(watches.handles).toHaveLength(1);
      expect(settled).toBe(true);
      expect(await pending).toBe(reason);
      for (const [type, listener] of added.mock.calls) {
        if (type === "abort") expect(removed).toHaveBeenCalledWith("abort", listener);
      }
      expect(operation).not.toHaveBeenCalled();
      expect(mkdir.mock.calls.filter(([file]) => String(file) === path.join(root, ".lock"))).toEqual([]);
      expect(names(root)).toEqual([headName]);
      expect(watches.handles[0]!.close).toHaveBeenCalled();
      expect(vi.getTimerCount()).toBe(0);
    } finally {
      abort.abort(reason);
      await vi.advanceTimersByTimeAsync(100); // also drains the current implementation's missed-abort timer on RED
      await pending;
    }
  });

  it.skipIf(process.platform === "win32")("multi-process: no queued Main starves or times out against legacy bargers", async () => {
    const { stdout } = await promisify(execFile)(process.execPath, ["scripts/benchmark-mesh-lock-fairness.mjs",
      "--n=12", "--legacy=2", "--try=2", "--seconds=2", "--budgetMs=10000", "--pLong=0"], { timeout: 85_000 });
    const report = JSON.parse(stdout);
    expect(report.timeouts.queued).toBe(0);
    expect(report.source.fallback).toBe(0);
    expect(report.source.queueHead).toBeGreaterThan(report.source.legacyBarge);
    expect(report.queuedWaitMs.max).toBeLessThan(8000);
  }, 90_000);
});
