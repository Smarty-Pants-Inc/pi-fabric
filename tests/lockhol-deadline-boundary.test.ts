import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// KEEP: recorded real catch, smarty-dev#9805. Synchronous admission/verifier work let a 2000 ms
// waiter enter the actual canonical operation at 2512 ms while its budget timer could not run.
// Real receipts, verifier, canonical publication/pinning and operation; only clocks/notifications
// are controlled. The host clock moves WITHOUT firing timers or relying on Date/performance.
// Emit the complete report/actual-entry bytes before assertions, including on RED, so the
// caller's retained stdout preserves them even when the test runner reclaims fixture scratch.
const parked: MeshLockTicket[] = [];
afterEach(() => {
  delete verifiedRemoveTestHooks.beforeOpen;
  for (const ticket of parked.splice(0)) ticket.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
});
const scratch = (): string => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "lockhol-deadline-boundary-"));
  const root = path.join(parent, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  return root;
};
const receipts = (root: string): string[] => fs.readdirSync(meshLockQueueDirectory(root))
  .filter(name => /^\d{24}-\d+-/.test(name)).sort();
const clock = () => {
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  vi.spyOn(performance, "now").mockReturnValue(0);
  const wall = Date.now(), base = process.hrtime.bigint();
  let elapsed = 0n;
  vi.spyOn(process.hrtime, "bigint").mockImplementation(() => base + elapsed);
  // Polling is the existing <=20 ms safety net; suppress real native watch scheduling.
  vi.spyOn(fs, "watch").mockImplementation((() => Object.assign(new EventEmitter(), {
    close() { /* no native watch opened */ },
  })) as unknown as typeof fs.watch);
  return {
    wall,
    get elapsedMs() { return Number(elapsed) / 1_000_000; },
    setElapsed(ns: bigint) { elapsed = ns; },
    async advance(ms: number) { elapsed += BigInt(ms) * 1_000_000n; await vi.advanceTimersByTimeAsync(ms); },
  };
};
const entry = (root: string, elapsedMs: number): string => {
  const owner = fs.readFileSync(path.join(root, ".lock", "owner"), "utf8");
  expect(owner.split("\n")[1]).toBe(String(process.pid));
  fs.writeFileSync(path.join(root, "actual-entry.json"), JSON.stringify({ elapsedMs, owner }));
  return "entered";
};
const report = (root: string, details: object): void => {
  const file = path.join(path.dirname(root), "report.json");
  const actualEntry = path.join(root, "actual-entry.json");
  const result = { root, ...details, actualEntry: fs.existsSync(actualEntry) ? fs.readFileSync(actualEntry, "utf8") : null };
  fs.writeFileSync(file, JSON.stringify(result, null, 2));
  console.log(`LOCKHOL_DEADLINE_REPORT ${JSON.stringify(result)}`);
};
const holder = (root: string): (() => void) => {
  const canonical = path.join(root, ".lock");
  fs.mkdirSync(canonical);
  fs.writeFileSync(path.join(canonical, "owner"), `live-holder\n${process.pid}\n${Date.now()}\n`);
  return () => fs.renameSync(canonical, path.join(root, "released-fixture"));
};

describe.each([1, 2, 3] as const)("protocol %s: lockhol post-admission deadline", protocol => {
  it.skipIf(process.platform !== "linux").each(["forward", "backward"] as const)("refuses real admission at 2512 ms with blocked timers and %s Date", async correction => {
    const root = scratch();
    const lock = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 2000 }, () => {});
    await lock.ownIncarnation;
    // Both parked receipts name this same live PID; their event loop makes no progress.
    parked.push(new MeshLockTicket(root, randomUUID(), 10_000), new MeshLockTicket(root, randomUUID(), 10_000));
    const original = receipts(root);
    expect(original).toHaveLength(2);
    const blockedFile = path.join(meshLockQueueDirectory(root), original[1]!);
    const time = clock();
    let blocked = false;
    verifiedRemoveTestHooks.beforeOpen = file => {
      if (file !== blockedFile || blocked) return;
      blocked = true;
      // No timer advancement here: model the recorded synchronous verifier stall exactly.
      time.setElapsed(2_512_000_000n);
      vi.setSystemTime(time.wall + (correction === "forward" ? 2512 : -60_000));
    };
    const mkdir = vi.spyOn(fs, "mkdirSync");
    const operation = vi.fn(() => entry(root, time.elapsedMs));
    const pending = lock.withLock(operation).catch((error: unknown) => error);
    for (let elapsed = 0; elapsed < 600 && !blocked; elapsed += 20) await time.advance(20);
    const result = await pending;
    const attempts = mkdir.mock.calls.filter(([file]) => String(file) === path.join(root, ".lock")).length;
    report(root, { case: "synchronous-admission", protocol, correction, budgetMs: 2000,
      elapsedMs: time.elapsedMs, original, blocked, attempts, entered: operation.mock.calls.length,
      outcome: result instanceof Error ? { name: result.name, message: result.message } : result });
    expect(blocked).toBe(true);
    expect(time.elapsedMs).toBe(2512);
    expect(result).toBeInstanceOf(MeshLockQueueExpiredError);
    expect(result).toMatchObject({ preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED", attempts: 0 });
    expect(attempts).toBe(0);
    expect(operation).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(receipts(root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it("rechecks at operation entry after synchronous owner publication spends the remaining budget", async () => {
    const root = scratch();
    const lock = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 2000 }, () => {});
    await lock.ownIncarnation;
    const release = holder(root);
    const time = clock();
    let released = false, published = false;
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation(((file: fs.PathOrFileDescriptor, data: string, options?: unknown) => {
      const result = write(file, data, options as undefined);
      if (released && String(file).endsWith(`${path.sep}owner`)) {
        published = true;
        time.setElapsed(2_512_000_000n);
        vi.setSystemTime(time.wall - 60_000);
      }
      return result;
    }) as typeof fs.writeFileSync);
    const operation = vi.fn(() => entry(root, time.elapsedMs));
    const pending = lock.withLock(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0); // first failed canonical attempt and first wait are armed
    // Fire the retry, not its continuation. Admission is timely; owner publication then stalls.
    time.setElapsed(1_000_000_000n);
    vi.advanceTimersByTime(1000);
    release(); released = true;
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    report(root, { case: "synchronous-publication", protocol, budgetMs: 2000,
      elapsedMs: time.elapsedMs, published, entered: operation.mock.calls.length,
      outcome: result instanceof Error ? { name: result.name, message: result.message } : result });
    expect(published).toBe(true);
    expect(result).toBeInstanceOf(MeshLockTimeoutError);
    expect(result).not.toBeInstanceOf(MeshLockQueueExpiredError);
    expect(operation).not.toHaveBeenCalled();
    // The real acquired/pinned canonical lock must be released on the pre-operation refusal.
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(receipts(root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });

  it.each([0n, 1n])("keeps the exact due-at-deadline probe, but refuses at deadline + %s ns", async extraNs => {
    const root = scratch();
    const lock = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 2000 }, () => {});
    await lock.ownIncarnation;
    const release = holder(root);
    const time = clock();
    const operation = vi.fn(() => entry(root, time.elapsedMs));
    const pending = lock.withLock(operation).catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    time.setElapsed(2_000_000_000n + extraNs);
    vi.advanceTimersByTime(2000); // +1 ms budget timer has NOT fired, retry continuation is pending
    release();
    await vi.advanceTimersByTimeAsync(0);
    const result = await pending;
    report(root, { case: "exact-deadline", protocol, budgetMs: 2000, extraNs: String(extraNs),
      elapsedMs: time.elapsedMs, entered: operation.mock.calls.length,
      outcome: result instanceof Error ? { name: result.name, message: result.message } : result });
    if (extraNs === 0n) {
      expect(result).toBe("entered");
      expect(operation).toHaveBeenCalledOnce();
    } else {
      expect(result).toBeInstanceOf(MeshLockTimeoutError);
      expect(result).not.toBeInstanceOf(MeshLockQueueExpiredError);
      expect(operation).not.toHaveBeenCalled();
    }
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
    expect(receipts(root)).toEqual([]);
    expect(vi.getTimerCount()).toBe(0);
  });
});
