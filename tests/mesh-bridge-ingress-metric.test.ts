import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Linux-only physical cases: observe rmdirSync(.lock) and directory fsync; non-Linux uses rename/verifiedRemove.
// KEEP: INGRESS-METRIC-POSTEFFECT-1 real-path regression and data safety.
// Real MeshBridge -> StoreBridgeSide -> EventLog -> SQLite, source control ACK,
// generated key/no archive. Native fs/driver hooks delegate actual operations.
// Normal wakes: fs notifications plus product retries; finite watchdog per event.
// Fixed 40ms late wake; A 80ms actual holder; B 60ms actual ticket retirement.
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); vi.useRealTimers(); }
});
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const native = async (store: MeshStore, name: string) => {
  const now = Date.now(), id = `session:busy-streak-${name}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + 120_000,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + 120_000 });
  writeParticipantFile(store.root, { key: `topology/participants/${hash(id)}`, version: 1, updatedAt: now, updatedBy: identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name, label: name,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return identity;
};

const TOPIC = "fabric.control.ack";
const deferred = <T = void>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const within = async <T>(work: Promise<T>, label: string, ms = 5_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<T>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms} ms`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
const queueTickets = (root: string) => {
  try { return fs.readdirSync(meshLockQueueDirectory(root)).filter(name => /^\d{24}-\d+-[a-f0-9-]+$/.test(name)).sort(); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
};
const sqliteBusy = (error: unknown): boolean => {
  const code = (error as { errcode?: unknown } | null)?.errcode;
  return typeof code === "number" && (code & 0xff) === 5;
};
type PhysicalLockInterval = { acquiredAt: number; releasedAt?: number };
const sqliteBridgeFixture = async (name: string) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), `mesh-bridge-retention-${name}-`));
  const hub = new MeshStore(path.join(base, "hub"), 65_536, 100, { stateBackend: "sqlite", lockProtocol: 1, lockTimeoutMs: 500 });
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, { stateBackend: "sqlite", lockProtocol: 1, lockTimeoutMs: 500 });
  const sender = await native(far, `retention-${name}-far`), recipient = await native(hub, `retention-${name}-hub`);
  const local = new StoreBridgeSide(hub, "far"), remote = new StoreBridgeSide(far, "hub");
  const logs: string[] = [], cursorPath = path.join(base, "cursor.json");
  const bridge = new MeshBridge({ local, remote, localName: "hub", remoteName: "far", cursorPath, stopMs: 50,
    log: message => logs.push(message) });
  let running: Promise<void> | undefined, failure: unknown;
  await bridge.start();
  const launch = () => { running = bridge.run().catch(error => { failure = error; }); };
  cleanups.push(async () => {
    await bridge.stop();
    await running;
    hub.closeState(); far.closeState();
    fs.rmSync(base, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  return { base, hub, far, sender, recipient, local, remote, bridge, logs, cursorPath, launch,
    running: () => running, failure: () => failure };
};
const canonicalHolder = (root: string) => {
  const entered = deferred<void>(), released = deferred<void>();
  let held = false;
  const work = new MeshLock(root, { lockProtocol: 1, lockTimeoutMs: 2_000 }, () => undefined)
    .withLockAcrossAwait(async () => {
      held = true; entered.resolve();
      try { await released.promise; } finally { held = false; }
    });
  return { entered: entered.promise, work, release: () => released.resolve(), held: () => held };
};
const sameFile = (fd: number, file: string) => {
  try { const a = fs.fstatSync(fd), b = fs.statSync(file); return a.isFile() && a.dev === b.dev && a.ino === b.ino; }
  catch { return false; }
};


describe.skipIf(process.platform !== "linux")("bridge ingress busy-streak metric boundaries", () => {
  it.each(["A", "B"] as const)("case %s: excludes post-effect work from the real keyed ACK metric", async phase => {
    const f = await sqliteBridgeFixture(phase);
    const root = f.hub.root, lockPath = path.join(root, ".lock"), eventsPath = path.join(root, "events.jsonl");
    let raw: DatabaseSync | undefined, sqliteHeld = false, observeBusy = false, lateWakeScheduled = false;
    const releaseSqlite = () => { if (raw && sqliteHeld) { raw.exec("ROLLBACK"); sqliteHeld = false; } };
    const refusalReached = deferred<void>(), cursorReached = deferred<void>(), idleTail = deferred<void>();
    const originalTail = f.remote.tail.bind(f.remote);
    vi.spyOn(f.remote, "tail").mockImplementation(async (...args) => {
      const page = await originalTail(...args);
      if (page.events.length === 0) idleTail.resolve();
      return page;
    });
    const ingressText = "real-sqlite-retention-ingress";
    const intervals: PhysicalLockInterval[] = [], refusals: Array<{ at: number; ticket?: string; inode?: number; interval: number }> = [];
    let retainedTicket: { name: string; inode: number } | undefined;
    let activeInterval: PhysicalLockInterval | undefined, appendAt: number | undefined, appendInterval = -1;
    let expectedSequence: number | undefined, lateWakeMs: number | undefined;
    let retiredAt: number | undefined, retirementBeganAt: number | undefined;
    let retirementDelayMs = 0, durableRetirementAt: number | undefined;
    let canonicalAbsentAtRetirement = false, retainedTicketRemoved = false;
    let targetAppendCount = 0;
    let lateWakeTimer: ReturnType<typeof setTimeout> | undefined;
    let postAppendHolder: ReturnType<typeof canonicalHolder> | undefined;
    let postAppendReleaseTimer: ReturnType<typeof setTimeout> | undefined;
    const cursorFileReached = () => {
      if (expectedSequence === undefined) return;
      try {
        const cursor = JSON.parse(fs.readFileSync(f.cursorPath, "utf8")) as { toLocal?: { after?: number } };
        if ((cursor.toLocal?.after ?? 0) >= expectedSequence) cursorReached.resolve();
      } catch { /* the atomic cursor replacement has not happened */ }
    };
    const mkdir = fs.mkdirSync.bind(fs);
    vi.spyOn(fs, "mkdirSync").mockImplementation((target, options) => {
      const result = mkdir(target, options);
      if (typeof target === "string" && path.resolve(target) === lockPath) {
        activeInterval = { acquiredAt: monotonicClock.now() };
        intervals.push(activeInterval);
      }
      return result;
    });
    const rmdir = fs.rmdirSync.bind(fs);
    vi.spyOn(fs, "rmdirSync").mockImplementation((target) => {
      const result = rmdir(target);
      if (typeof target === "string" && path.resolve(target) === lockPath) {
        if (activeInterval) activeInterval.releasedAt = monotonicClock.now();
        activeInterval = undefined;
        // One independent event-loop stall begins only after the real native rmdir. The observer
        // below measures its physical release-to-reacquisition interval, not the requested retry sleep.
        if (refusals.length === 1 && !lateWakeScheduled) {
          lateWakeScheduled = true;
          lateWakeTimer = setTimeout(() => {
            const began = monotonicClock.now();
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 40);
            lateWakeMs = monotonicClock.now() - began;
          }, 0);
        }
      }
      return result;
    });
    const originalExec = DatabaseSync.prototype.exec;
    vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql: string) {
      try { return originalExec.call(this, sql); }
      catch (error) {
        if (observeBusy && this !== raw && sql.trim().toUpperCase() === "BEGIN IMMEDIATE" && sqliteBusy(error)) {
          const at = monotonicClock.now(), interval = activeInterval ? intervals.indexOf(activeInterval) : -1;
          if (interval >= 0) {
            let names: string[] = [], firstAttemptTicket: string | undefined;
            try {
              const [token, pid] = fs.readFileSync(path.join(lockPath, "owner"), "utf8").split("\n");
              names = queueTickets(root);
              firstAttemptTicket = names.find(name => name.endsWith(`-${pid}-${token}`));
              if (!retainedTicket && firstAttemptTicket) {
                const stat = fs.statSync(path.join(meshLockQueueDirectory(root), firstAttemptTicket));
                retainedTicket = { name: firstAttemptTicket, inode: stat.ino };
              }
            } catch { /* captured as a missing native receipt and asserted below */ }
            const ticket = retainedTicket && names.includes(retainedTicket.name) ? retainedTicket.name : undefined;
            if (ticket) {
              const inode = fs.statSync(path.join(meshLockQueueDirectory(root), ticket)).ino;
              refusals.push({ at, ticket, inode, interval });
              if (refusals.length === 4) refusalReached.resolve();
            }
          }
        }
        throw error;
      }
    });
    const append = fs.appendFileSync.bind(fs);
    vi.spyOn(fs, "appendFileSync").mockImplementation((file, data, options) => {
      const result = append(file, data, options);
      if (typeof file === "string" && path.resolve(file) === eventsPath && String(data).includes(ingressText)) {
        appendAt = monotonicClock.now(); targetAppendCount++;
        appendInterval = activeInterval ? intervals.indexOf(activeInterval) : -1;
      }
      return result;
    });
    const rename = fs.renameSync.bind(fs);
    vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      const result = rename(from, to);
      if (typeof to === "string" && path.resolve(to) === f.cursorPath) cursorFileReached();
      return result;
    });
    const sync = fs.fsyncSync.bind(fs);
    vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      sync(fd);
      if (phase === "A" && appendAt !== undefined && !postAppendHolder && sameFile(fd, eventsPath)) {
        // This is after the actual append. Hold the real canonical lock across receipt finalization
        // so the retention metric is proven not to include post-append receipt work.
        postAppendHolder = canonicalHolder(root);
        postAppendReleaseTimer = setTimeout(() => postAppendHolder?.release(), 80);
      }
    });
    const nativeRemove = fs.rmSync.bind(fs);
    vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (retainedTicket && appendAt !== undefined && typeof target === "string" &&
        path.resolve(target) === path.join(meshLockQueueDirectory(root), retainedTicket.name)) {
        retirementBeganAt = monotonicClock.now();
        canonicalAbsentAtRetirement = !fs.existsSync(lockPath);
        expect(fs.statSync(target).ino).toBe(retainedTicket.inode);
        if (phase === "B") {
          // Real native barriers before the fixed delay; no fabricated append/result.
          const eventFd = fs.openSync(eventsPath, "r"), rootFd = fs.openSync(root, "r");
          try { sync(eventFd); sync(rootFd); durableRetirementAt = monotonicClock.now(); }
          finally { fs.closeSync(eventFd); fs.closeSync(rootFd); }
          const began = monotonicClock.now();
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 60);
          retirementDelayMs = monotonicClock.now() - began;
        }
        const result = nativeRemove(target, options);
        retiredAt = monotonicClock.now(); retainedTicketRemoved = !fs.existsSync(target);
        return result;
      }
      return nativeRemove(target, options);
    });
    cleanups.push(async () => {
      if (lateWakeTimer !== undefined) clearTimeout(lateWakeTimer);
      if (postAppendReleaseTimer !== undefined) clearTimeout(postAppendReleaseTimer);
      releaseSqlite();
      postAppendHolder?.release();
      await f.bridge.stop();
      await f.running();
      await postAppendHolder?.work;
      raw?.close();
    });

    f.launch();
    await within(idleTail.promise, "real empty toLocal page before SQLite contention");
    await new Promise<void>(resolve => setImmediate(resolve));
    raw = new DatabaseSync(path.join(root, "state.db"));
    raw.exec("PRAGMA busy_timeout = 0");
    raw.exec("BEGIN IMMEDIATE");
    raw.prepare("UPDATE meta SET value = value WHERE name = 'commit_no'").run();
    sqliteHeld = true;
    observeBusy = true;
    const sent = await f.far.publish({ topic: TOPIC, kind: "ack", from: f.sender, to: f.recipient.id,
      text: ingressText, data: { commandId: `probe-${phase}`, targetId: f.sender.id, status: "ok" }, durable: true });
    expectedSequence = sent.sequence;
    cursorFileReached();
    await within(refusalReached.promise, "four native SQLite fence refusals");
    expect(refusals.length).toBeGreaterThan(3);
    releaseSqlite();
    await within(cursorReached.promise, "durable toLocal cursor checkpoint");
    expect(f.failure()).toBeUndefined();
    if (phase === "A") {
      expect(postAppendHolder).toBeDefined();
      await within(postAppendHolder!.entered, "real post-append receipt holder");
      await within(postAppendHolder!.work, "post-append receipt holder release");
    }
    await f.bridge.stop();
    await f.running();

    const committed = f.hub.read({ topic: TOPIC, after: 0 });
    expect(committed).toHaveLength(1);
    expect(committed[0]).toMatchObject({ text: ingressText, verification: "bridge", from: f.sender, to: f.recipient.id,
      data: { bridge: { from: "far", id: sent.id } } });
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8"))).toMatchObject({ toLocal: { after: sent.sequence } });

    const ack = committed[0]!;
    const receiptFile = path.join(root, "event-receipts", hash(ack.dedupeKey!) + ".json");
    const receiptRaw = fs.readFileSync(receiptFile, "utf8");
    const eventRaw = fs.readFileSync(eventsPath, "utf8"), cursorRaw = fs.readFileSync(f.cursorPath, "utf8");
    expect(ack.dedupeKey).toBe(`bridge-publish:${hash(JSON.stringify(["far", sent.id]))}`);
    expect(JSON.parse(receiptRaw)).toEqual(ack);
    expect(fs.existsSync(receiptFile.replace(/\.json$/, ".pending.json"))).toBe(false);
    expect(fs.readdirSync(path.join(root, "event-receipts")).filter(name => /^[a-f0-9]{64}\.json$/.test(name))).toHaveLength(1);
    expect(targetAppendCount).toBe(1);
    expect(canonicalAbsentAtRetirement).toBe(true);
    expect(retainedTicketRemoved).toBe(true);
    expect(queueTickets(root)).toEqual([]);
    expect(fs.existsSync(lockPath)).toBe(false);
    console.log("RETENTION_NATIVE_OBSERVATION " + JSON.stringify({ phase, refusals, intervals, appendAt, appendInterval,
      retirementBeganAt, retiredAt, retirementDelayMs, durableRetirementAt, canonicalAbsentAtRetirement,
      retainedTicketRemoved, targetAppendCount, eventRaw, cursorRaw, receiptRaw, receiptFile, logs: f.logs }));
    expect(refusals).toHaveLength(4);
    expect(refusals.every(refusal => refusal.ticket !== undefined && refusal.inode !== undefined)).toBe(true);
    expect(new Set(refusals.map(refusal => refusal.ticket)).size, "all physical fence reacquisitions kept one native FIFO receipt")
      .toBe(1);
    expect(new Set(refusals.map(refusal => refusal.inode)).size, "the same receipt inode survived each release/reacquire")
      .toBe(1);
    const firstRefusalInterval = refusals[0]!.interval;
    expect(firstRefusalInterval).toBeGreaterThanOrEqual(0);
    expect(appendInterval).toBe(firstRefusalInterval + refusals.length);
    const gaps = refusals.map((refusal, index) => {
      expect(refusal.interval).toBe(firstRefusalInterval + index);
      const releasedAt = intervals[refusal.interval]!.releasedAt;
      const reacquiredAt = intervals[refusal.interval + 1]!.acquiredAt;
      expect(releasedAt).toBeDefined();
      return reacquiredAt - releasedAt!;
    });
    expect(lateWakeMs).toBeGreaterThan(0);
    const physicalOffLockMs = gaps.reduce((sum, gap) => sum + gap, 0);
    const actualLockHoldsMs = refusals.reduce((sum, refusal) =>
      sum + intervals[refusal.interval]!.releasedAt! - intervals[refusal.interval]!.acquiredAt, 0);
    expect(actualLockHoldsMs).toBeGreaterThan(0);
    expect(appendAt).toBeGreaterThan(intervals[appendInterval]!.acquiredAt);
    const inner = f.logs.filter(message => message.startsWith("bridge ingress busy streak recovered (toLocal)"));
    expect(inner).toHaveLength(1);
    expect(inner[0]).toMatch(/^bridge ingress busy streak recovered \(toLocal\); \d+ fence refusals; retainedOffLockMs=\d+$/);
    const loggedRefusals = Number(inner[0]!.match(/; (\d+) fence refusals;/)![1]);
    const loggedOffLockMs = Number(inner[0]!.match(/; retainedOffLockMs=(\d+)$/)![1]);
    expect(loggedRefusals).toBe(refusals.length);
    expect(loggedOffLockMs).toBeGreaterThan(0);
    expect(loggedOffLockMs).toBeGreaterThanOrEqual(Math.floor(lateWakeMs!));
    // Native release-to-mkdir intervals include the actual late wake and reacquisition; small
    // per-edge sampling skew is bounded by the number of observed physical intervals.
    const postReleaseAt = intervals[appendInterval]!.releasedAt!;
    const retirementGapMs = retiredAt! - postReleaseAt;
    const metricExcessMs = loggedOffLockMs - physicalOffLockMs;
    const intendedExcluded = Math.abs(loggedOffLockMs - Math.round(physicalOffLockMs));
    console.log("METRIC_BOUNDARY " + JSON.stringify({ phase, physicalOffLockMs, retirementGapMs,
      retirementDelayMs, metricExcessMs, loggedOffLockMs, loggedRefusals, lateWakeMs, retirementBeganAt, retiredAt }));
    if (phase === "B") {
      expect(retirementDelayMs).toBeGreaterThanOrEqual(59);
      expect(durableRetirementAt).toBeGreaterThan(postReleaseAt);
    }
    expect(intendedExcluded, "post-effect native ticket retirement must be excluded; pre-effect late wake still counted")
      .toBeLessThanOrEqual(refusals.length * 2 + 2);
    expect(f.logs.some(message => message.includes("(toLocal); retrying in"))).toBe(false);
    expect(f.logs.some(message => message.includes("(toLocal)") && message.includes("failed direction attempts"))).toBe(false);
    const postAppendIntervals = intervals.slice(appendInterval + 1);
    expect(postAppendIntervals.length, "post-append receipt work reacquired the real canonical lock").toBeGreaterThan(0);
    const receiptWaitMs = postAppendIntervals.length > 1 ? postAppendIntervals[1]!.acquiredAt - postReleaseAt : 0;
    expect(retiredAt).toBeLessThan(postAppendIntervals[0]!.acquiredAt);
    if (phase === "A") {
      expect(postAppendIntervals.length).toBeGreaterThanOrEqual(2);
      expect(receiptWaitMs).toBeGreaterThanOrEqual(60);
      expect(loggedOffLockMs).toBeLessThan(physicalOffLockMs + receiptWaitMs / 2);
    }
    console.log("RETENTION_LOG_RESULT " + JSON.stringify({ test: "real SQLite keyed ACK", phase,
      retirementGapMs, retirementDelayMs, metricExcessMs, receiptWaitMs, physicalOffLockMs,
      refusals: refusals.length,
      ticket: refusals[0]!.ticket, receiptInode: refusals[0]!.inode, physicalOffLockGapsMs: gaps.map(gap => +gap.toFixed(2)),
      lateWakeActualMs: +lateWakeMs!.toFixed(2), metricRetainedOffLockMs: loggedOffLockMs,
      actualRefusalLockHoldsMs: +actualLockHoldsMs.toFixed(2), postAppendLockIntervals: postAppendIntervals.length,
      cursorToLocalAfter: sent.sequence, persistedIngressEvent: true, persistedCursor: true, outerDirectionRetry: false }));
  });

});
