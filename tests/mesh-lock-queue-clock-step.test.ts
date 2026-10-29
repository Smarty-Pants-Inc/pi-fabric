import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshLockTicket, meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { verifiedRemoveTestHooks } from "../src/storage/verified-remove.js";

// CDA-QUEUE-01: real advisory receipts, verified removal and canonical MeshLock operations.
// Date is the persisted cross-process mtime/owner clock; performance is local elapsed time.
// setSystemTime steps ONLY Date, independently of timer/performance progress and kernel mtimes.
const parents: string[] = [];
const tickets: MeshLockTicket[] = [];
const releases: Array<() => void> = [];
const pending: Promise<unknown>[] = [];
const scratch = (): string => {
  const parent = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-queue-clock-"));
  parents.push(parent);
  const root = path.join(parent, "mesh");
  fs.mkdirSync(root, { mode: 0o700 });
  vi.useFakeTimers({ toFake: ["Date", "performance", "setTimeout", "clearTimeout"] });
  return root;
};
const receipts = (root: string): string[] => fs.readdirSync(meshLockQueueDirectory(root))
  .filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort();
const fileFor = (root: string, name: string): string => path.join(meshLockQueueDirectory(root), name);
const ticket = (root: string, options: { passStalledHead?: boolean; stalledHeadBypassLock?: string } = {}, budget = 2500): MeshLockTicket => {
  const token = randomUUID();
  const created = new MeshLockTicket(root, token, budget, options);
  tickets.push(created);
  // The kernel is not on our fake Date clock. Persist the creating process's wall value explicitly,
  // just as heartbeat does, so a future/old kernel mtime cannot accidentally decide a test's result.
  const file = fileFor(root, receipts(root).find(name => name.endsWith(token))!);
  fs.utimesSync(file, Date.now() / 1000, Date.now() / 1000);
  return created;
};
const canonicalOptions = (root: string) => ({ stalledHeadBypassLock: path.join(root, ".lock") });
const acquire = (root: string, entered: number[], id: number): Promise<unknown> => {
  const result = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLock(() => {
    expect(Number(fs.readFileSync(path.join(root, ".lock", "owner"), "utf8").split("\n")[1])).toBe(process.pid);
    entered.push(id);
  }).catch((error: unknown) => error);
  pending.push(result);
  return result;
};
const step = (amount: number): void => {
  const mono = performance.now();
  vi.setSystemTime(Date.now() + amount);
  expect(performance.now()).toBe(mono);
};

afterEach(async () => {
  delete verifiedRemoveTestHooks.beforeOpen;
  for (const release of releases.splice(0)) release();
  for (const created of tickets.splice(0)) created.close();
  await vi.runAllTimersAsync();
  await Promise.all(pending.splice(0));
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const parent of parents.splice(0)) fs.rmSync(parent, { recursive: true, force: true });
});

describe.skipIf(process.platform !== "linux")("queue observation clocks (CDA-QUEUE-01)", () => {
  it.each([1000, -1000])("wall-only step %s ms before the next heartbeat neither removes nor bypasses a healthy head", async wallStep => {
    const root = scratch();
    const head = ticket(root, canonicalOptions(root));
    const successor = ticket(root, canonicalOptions(root));
    const observer = ticket(root, canonicalOptions(root));
    expect(head.mayContend()).toBe(true);
    const original = receipts(root);
    const headFile = fileFor(root, original[0]!);
    const judged = fs.lstatSync(headFile);
    expect(observer.mayContend()).toBe(false);
    vi.advanceTimersByTime(1);
    step(wallStep); // 1 ms elapsed: before 50 ms heartbeat, 250 ms stall, and 2000 ms fallback
    expect(observer.mayContend()).toBe(false);
    expect(observer.queued).toBe(true);
    expect(receipts(root)).toEqual(original);
    expect(fs.lstatSync(headFile).ino).toBe(judged.ino);
    expect(fs.lstatSync(headFile).mtimeMs).toBe(judged.mtimeMs);
    await vi.advanceTimersByTimeAsync(49);
    expect(head.mayContend()).toBe(true);
    // Heartbeat cadence is monotonic too, even when persisted Date moved backwards.
    expect(fs.lstatSync(headFile).mtimeMs).not.toBe(judged.mtimeMs);
    expect(observer.mayContend()).toBe(false);
    const entered: number[] = [];
    const first = acquire(root, entered, 1);
    const second = acquire(root, entered, 2);
    await vi.advanceTimersByTimeAsync(20);
    expect(entered).toEqual([]);
    head.close();
    expect(successor.mayContend()).toBe(true);
    await vi.advanceTimersByTimeAsync(20);
    expect(entered).toEqual([]);
    successor.close();
    expect(observer.mayContend()).toBe(true);
    await vi.advanceTimersByTimeAsync(20);
    expect(entered).toEqual([]); // promotion does not let the tail directly enter
    observer.close();
    await vi.advanceTimersByTimeAsync(50);
    expect(await first).toBeUndefined();
    expect(await second).toBeUndefined();
    expect(entered).toEqual([1, 2]);
    expect(receipts(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it.each([1000, -1000])("genuine elapsed stall recovers same-PID head AND successor with wall step %s ms", async wallStep => {
    const root = scratch();
    ticket(root, canonicalOptions(root));
    ticket(root, canonicalOptions(root));
    const original = receipts(root);
    expect(original.map(name => Number(name.split("-")[1]))).toEqual([process.pid, process.pid]);
    const verified: string[] = [];
    verifiedRemoveTestHooks.beforeOpen = target => { verified.push(target); };
    const entered: number[] = [];
    const first = acquire(root, entered, 1);
    const second = acquire(root, entered, 2);
    step(wallStep);
    await vi.advanceTimersByTimeAsync(250);
    expect(original.every(name => fs.existsSync(fileFor(root, name)))).toBe(true);
    expect(entered).toEqual([]);
    await vi.advanceTimersByTimeAsync(30);
    expect(fs.existsSync(fileFor(root, original[0]!))).toBe(false);
    expect(fs.existsSync(fileFor(root, original[1]!))).toBe(true); // promoted head gets a fresh observation
    expect(entered).toEqual([]);
    await vi.advanceTimersByTimeAsync(260);
    expect(await first).toBeUndefined();
    expect(await second).toBeUndefined();
    expect(entered).toEqual([1, 2]);
    expect(verified).toContain(fileFor(root, original[0]!));
    expect(verified).toContain(fileFor(root, original[1]!));
    expect(performance.now()).toBe(540); // recovery before the 80% fallback, with no wall-clock authority
    expect(receipts(root)).toEqual([]);
    expect(fs.existsSync(path.join(root, ".lock"))).toBe(false);
  });

  it.each([1000, -1000])("legacy early fallback measures unchanged head monotonically after wall step %s ms", async wallStep => {
    const root = scratch();
    ticket(root);
    ticket(root);
    const observer = ticket(root, { passStalledHead: true });
    const original = receipts(root);
    expect(observer.mayContend()).toBe(false);
    vi.advanceTimersByTime(50); // establish first head observation (legacy check cadence is 50 ms)
    expect(observer.mayContend()).toBe(false);
    step(wallStep);
    vi.advanceTimersByTime(1);
    expect(observer.mayContend()).toBe(false);
    expect(observer.queued).toBe(true);
    expect(receipts(root)).toEqual(original);
    vi.advanceTimersByTime(249);
    expect(observer.mayContend()).toBe(false); // exactly 250 ms unchanged is not past the stall bound
    vi.advanceTimersByTime(50); // next throttled check, 300 ms since first observation
    expect(observer.mayContend()).toBe(true);
    expect(observer.queued).toBe(false);
    expect(receipts(root)).toEqual(original.slice(0, 2)); // fallback closes only the observer
    for (const created of tickets) created.close();
    const entered: number[] = [];
    expect(await acquire(root, entered, 1)).toBeUndefined();
    expect(entered).toEqual([1]);
  });

  it.each([60_001, -60_001])("legacy predecessor cannot age on a %s ms wall-only step, but can on elapsed time", async wallStep => {
    const root = scratch();
    ticket(root);
    ticket(root);
    const observer = ticket(root);
    const original = receipts(root);
    expect(observer.mayContend()).toBe(false);
    const verified: string[] = [];
    verifiedRemoveTestHooks.beforeOpen = target => { verified.push(target); };
    vi.advanceTimersByTime(1);
    step(wallStep); // >30 seconds on Date, but only 1 ms of a 30-second predecessor observation
    expect(observer.mayContend()).toBe(false);
    expect(observer.queued).toBe(true);
    expect(receipts(root)).toEqual(original);
    expect(verified).toEqual([]);
    // Advance ONLY monotonic/timer time: a fresh observation is still required for the old head.
    const wall = Date.now();
    vi.spyOn(Date, "now").mockReturnValue(wall);
    vi.advanceTimersByTime(30_000);
    expect(observer.mayContend()).toBe(false);
    expect(fs.existsSync(fileFor(root, original[1]!))).toBe(false);
    expect(fs.existsSync(fileFor(root, original[0]!))).toBe(true);
    expect(verified).toContain(fileFor(root, original[1]!));
    vi.advanceTimersByTime(30_001);
    expect(observer.mayContend()).toBe(true);
    expect(verified).toContain(fileFor(root, original[0]!));
    observer.close();
    const entered: number[] = [];
    expect(await acquire(root, entered, 1)).toBeUndefined();
    expect(entered).toEqual([1]);
  });

  it.each([1000, -1000])("short-budget fallback at 80 percent is elapsed time, not a %s ms wall step", wallStep => {
    const root = scratch();
    ticket(root);
    const observer = ticket(root, {}, 400);
    expect(observer.mayContend()).toBe(false);
    step(wallStep);
    vi.advanceTimersByTime(1);
    expect(observer.mayContend()).toBe(false);
    expect(observer.queued).toBe(true);
    vi.advanceTimersByTime(318);
    expect(observer.mayContend()).toBe(false);
    vi.advanceTimersByTime(1);
    expect(observer.mayContend()).toBe(true);
    expect(observer.queued).toBe(false);
  });

  it.each([60_001, -60_001])("actual live canonical custody survives %s ms wall step and genuine stale observations", async wallStep => {
    const root = scratch();
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    releases.push(release);
    let heldEntered = false;
    const holder = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLockAcrossAwait(async () => {
      heldEntered = true;
      await held;
    });
    pending.push(holder);
    expect(heldEntered).toBe(true);
    const canonical = path.join(root, ".lock");
    const identity = fs.lstatSync(canonical);
    const owner = fs.readFileSync(path.join(canonical, "owner"), "utf8");
    ticket(root, canonicalOptions(root));
    const observer = ticket(root, canonicalOptions(root));
    const original = receipts(root);
    expect(observer.mayContend()).toBe(false);
    step(wallStep);
    vi.advanceTimersByTime(1);
    expect(observer.mayContend()).toBe(false);
    expect(receipts(root)).toEqual(original);
    await vi.advanceTimersByTimeAsync(30_001);
    expect(observer.mayContend()).toBe(true); // late plain contest remains available to recover DEAD canonical owners
    expect(observer.queued).toBe(false);
    expect(receipts(root)).toEqual(original.slice(0, -1)); // only OUR receipt closes; the live holder's receipt stays
    const entered: number[] = [];
    const contender = acquire(root, entered, 1);
    await vi.advanceTimersByTimeAsync(2600);
    expect(await contender).toBeInstanceOf(Error);
    expect(entered).toEqual([]);
    expect(fs.lstatSync(canonical).ino).toBe(identity.ino);
    expect(fs.readFileSync(path.join(canonical, "owner"), "utf8")).toBe(owner);
    release();
    await holder;
    expect(fs.existsSync(canonical)).toBe(false);
  });

  it("wall steps during watch setup only shorten or lengthen wait, never healthy-head custody or guarded admission", async () => {
    // KEEP: real #9805 deadline/admission boundary. Only clocks and notification delivery are controlled;
    // holder custody, FIFO receipts, verifier and the refused MeshLock operation remain the actual path.
    const { EventEmitter } = await import("node:events");
    const { MeshLockTimeoutError } = await import("../src/core/atomic-write.js");
    const { MeshLockQueueExpiredError } = await import("../src/mesh/mesh-lock.js");
    for (const direction of ["forward", "backward"] as const) {
      vi.useRealTimers(); // reset between private roots; leave the final fake clock for the existing afterEach
      const root = scratch();
      const wall = Date.now();
      const hostNow = process.hrtime.bigint.bind(process.hrtime);
      let fixedHost: bigint | undefined;
      let lastHost = 0n;
      let guardOrigin = 0n;
      // Pass through the REAL host clock until both receipt names exist. Freezing hrtime before
      // the follower's construction could reverse lexical FIFO and test a different admission path.
      const hostReads: bigint[] = [];
      vi.spyOn(process.hrtime, "bigint").mockImplementation(() => {
        lastHost = fixedHost ?? hostNow();
        hostReads.push(lastHost);
        return lastHost;
      });
      let release!: () => void;
      const held = new Promise<void>(resolve => { release = resolve; });
      releases.push(release);
      let holderEntered = false;
      const holder = new MeshLock(root, { lockTimeoutMs: 2500 }, () => {}).withLockAcrossAwait(async () => {
        holderEntered = true;
        await held;
      });
      pending.push(holder);
      expect(holderEntered).toBe(true);
      const canonical = path.join(root, ".lock");
      const ownerFile = path.join(canonical, "owner");
      const canonicalStat = fs.lstatSync(canonical);
      const owner = fs.readFileSync(ownerFile, "utf8");
      const original = receipts(root);
      expect(original).toHaveLength(1);
      const headFile = fileFor(root, original[0]!);
      const headStat = fs.lstatSync(headFile);
      const identity = (file: string) => {
        const stat = fs.lstatSync(file);
        return { dev: stat.dev, ino: stat.ino, mtimeMs: stat.mtimeMs };
      };
      const headIdentity = identity(headFile);
      const canonicalIdentity = identity(canonical);
      const verified: string[] = [];
      verifiedRemoveTestHooks.beforeOpen = target => { verified.push(target); };
      const timers: Array<{ ms: number | undefined; fired: boolean }> = [];
      const setTimer = globalThis.setTimeout;
      vi.spyOn(globalThis, "setTimeout").mockImplementation(((callback: () => void, ms?: number) => {
        const record = { ms, fired: false };
        timers.push(record);
        return setTimer(() => { record.fired = true; callback(); }, ms);
      }) as typeof setTimeout);
      let notify: (() => void) | undefined;
      let setupCount = 0;
      let uncorrectedSleep = -1;
      const setupMs = direction === "forward" ? 5 : 2495;
      vi.spyOn(fs, "watch").mockImplementation(((target: string, _options: unknown,
        listener: (event: string, name: string) => void) => {
        expect(target).toBe(headFile);
        expect(++setupCount).toBe(1);
        const enqueued = receipts(root);
        expect(enqueued).toHaveLength(2);
        expect(enqueued[0]).toBe(original[0]);
        expect(BigInt(enqueued[1]!.split("-")[0]!)).toBeGreaterThan(BigInt(original[0]!.split("-")[0]!));
        // The first wait's host deadline and +1 ms budget timer have ALREADY been armed.
        guardOrigin = lastHost;
        fixedHost = guardOrigin + BigInt(setupMs) * 1_000_000n;
        vi.advanceTimersByTime(setupMs); // synchronous setup time; the 2501 ms budget timer cannot fire
        uncorrectedSleep = Math.min(20, wall + 2500 - Date.now());
        step(direction === "forward" ? 60_000 : -60_000);
        notify = () => listener("rename", path.basename(headFile));
        return Object.assign(new EventEmitter(), { close() { /* deterministic delivery, no native watch */ } });
      }) as unknown as typeof fs.watch);
      const mkdir = vi.spyOn(fs, "mkdirSync");
      const entryFile = path.join(root, "actual-entry.json");
      const operation = vi.fn(() => {
        const entry = { direction, hostMs: Number(lastHost - guardOrigin) / 1_000_000,
          owner: fs.readFileSync(ownerFile, "utf8") };
        fs.writeFileSync(entryFile, JSON.stringify(entry));
        return "entered";
      });
      let operationErrors = 0;
      let settled = false;
      const contender = new MeshLock(root, { lockTimeoutMs: 2500 }, () => { operationErrors++; })
        .withLock(operation).catch((error: unknown) => error).finally(() => { settled = true; });
      pending.push(contender);
      await vi.advanceTimersByTimeAsync(0); // drain continuations, never the guard or retry timer
      expect(setupCount).toBe(1);
      expect(identity(headFile)).toEqual(headIdentity);
      expect(identity(canonical)).toEqual(canonicalIdentity);
      expect(fs.lstatSync(headFile).ino).toBe(headStat.ino);
      expect(fs.lstatSync(canonical).ino).toBe(canonicalStat.ino);
      expect(fs.readFileSync(ownerFile, "utf8")).toBe(owner);
      expect(verified).toEqual([]);
      if (direction === "forward") {
        expect(performance.now()).toBe(5);
        expect(uncorrectedSleep).toBe(20);
        expect(timers).toEqual([{ ms: 2501, fired: false }]); // the wall step ended wait() without a sleep
        expect(settled).toBe(true);
        expect(receipts(root)).toEqual(original); // only the refused follower closed; head was not skipped/reaped
      } else {
        expect(performance.now()).toBe(2495);
        expect(uncorrectedSleep).toBe(5);
        expect(timers).toEqual([{ ms: 2501, fired: false }, { ms: 20, fired: false }]);
        expect(settled).toBe(false); // backward correction lengthened the actual clipped sleep from 5 to 20 ms
        expect(receipts(root)).toHaveLength(2);
        // Model a synchronous pause independently of Date/performance/timers, then the REAL holder's
        // normal release. No queued ticket is deleted by the test and no late operation is manufactured.
        fixedHost = guardOrigin + 2_501_000_000n;
        expect(Date.now()).toBeLessThan(wall + 2500);
        expect(identity(headFile)).toEqual(headIdentity);
        expect(identity(canonical)).toEqual(canonicalIdentity);
        release();
        await holder; // canonical release and owned head receipt close are actual MeshLock cleanup
        expect(fs.existsSync(canonical)).toBe(false);
        expect(fs.existsSync(headFile)).toBe(false);
        notify!(); // release readiness must NOT authorize a late follower, even with the wall deadline ahead
        await vi.advanceTimersByTimeAsync(0);
        expect(settled).toBe(true);
        expect(hostReads).toContain(fixedHost); // the existing caller actually consulted the spent host guard
        expect(timers.every(timer => !timer.fired)).toBe(true); // neither relative callback caused refusal
        expect(performance.now()).toBe(2495);
        expect(Date.now()).toBe(wall + 2495 - 60_000);
        expect(receipts(root)).toEqual([]);
      }
      const result = await contender;
      const attempts = mkdir.mock.calls.filter(([file]) => String(file) === canonical).length;
      console.log(`LOCKHOL_WALL_WAIT_REPORT ${JSON.stringify({ direction, setupMs, uncorrectedSleep,
        timers, headIdentity, canonicalIdentity, owner, headPreservedUntilOwnerRelease: true,
        hostElapsedMs: Number((fixedHost ?? lastHost) - guardOrigin) / 1_000_000, attempts, entered: operation.mock.calls.length,
        operationErrors, verified, actualEntry: fs.existsSync(entryFile) ? fs.readFileSync(entryFile, "utf8") : null,
        outcome: result instanceof Error ? { name: result.name, message: result.message,
          ...("code" in result ? { code: result.code } : {}) } : result })}`);
      expect(result).toBeInstanceOf(MeshLockTimeoutError);
      expect(result).toBeInstanceOf(MeshLockQueueExpiredError);
      expect(result).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
        attempts: 0, waitersAhead: direction === "forward" ? 1 : 0 });
      expect(attempts).toBe(0);
      expect(operation).not.toHaveBeenCalled();
      expect(operationErrors).toBe(0); // a pre-lock refusal, not an operation failure
      expect(fs.existsSync(entryFile)).toBe(false);
      if (direction === "forward") {
        expect(identity(headFile)).toEqual(headIdentity);
        expect(identity(canonical)).toEqual(canonicalIdentity);
        expect(fs.readFileSync(ownerFile, "utf8")).toBe(owner);
        expect(verified).toEqual([]);
        release();
        await holder;
      }
      expect(fs.existsSync(canonical)).toBe(false);
      expect(receipts(root)).toEqual([]);
      expect(vi.getTimerCount()).toBe(0);
      delete verifiedRemoveTestHooks.beforeOpen;
      vi.restoreAllMocks();
    }
  });
});
