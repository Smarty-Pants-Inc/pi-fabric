import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as atomic from "../src/core/atomic-write.js";
import * as namespace from "../src/core/pid-namespace.js";
import * as journal from "../src/mesh/read-journal.js";
import { RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide, type BridgePublish } from "../src/mesh/bridge.js";
import { MeshLock } from "../src/mesh/mesh-lock.js";
import { isMeshRetryableBusy } from "../src/mesh/state-backend.js";
import { MeshStore } from "../src/mesh/store.js";
import { runBridge } from "../src/mesh-bridge.js";

// Keep: smarty-dev#12036's two indistinguishable zero-attempt producers, plus the RPC trust boundary.
// Recorded-call fixtures only: no real lock acquisition, staging cleanup, census removal or teardown deletion.
const records = vi.hoisted(() => ({
  stats: { failed: vi.fn(), acquired: vi.fn() },
  events: { waitTimeout: vi.fn(), held: vi.fn() },
  ticket: vi.fn(),
  spawn: vi.fn(() => { throw new Error("unexpected transport spawn"); }),
  remove: vi.fn(() => { throw new Error("unexpected verified removal"); }),
}));
vi.mock("../src/mesh/commit-stats.js", async (original) => ({
  ...await original<typeof import("../src/mesh/commit-stats.js")>(),
  createLockStats: () => records.stats,
  createLockEvents: () => records.events,
}));
vi.mock("../src/mesh/lock-queue.js", async (original) => ({
  ...await original<typeof import("../src/mesh/lock-queue.js")>(),
  MeshLockTicket: class {
    constructor(...args: unknown[]) { records.ticket(...args); }
    mayContend(): boolean { return true; }
    get queued(): boolean { return false; }
    close(): void {}
  },
}));
vi.mock("../src/storage/verified-remove.js", () => ({ verifiedRemove: records.remove }));
vi.mock("node:child_process", async (original) => ({
  ...await original<typeof import("node:child_process")>(),
  spawn: records.spawn,
}));

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((done, failed) => { resolve = done; reject = failed; });
  return { promise, resolve, reject };
};
const yielded = (): Promise<void> => new Promise(resolve => setImmediate(resolve));
// Settlement is the event source; one event-loop turn is a bounded watchdog, not a polling loop.
const promptly = async <T>(promise: Promise<T>): Promise<T> => {
  let watchdog: ReturnType<typeof setImmediate> | undefined;
  const late = new Promise<never>((_resolve, reject) => {
    watchdog = setImmediate(() => reject(new Error("bounded call awaited unresolved preparation")));
  });
  try { return await Promise.race([promise, late]); }
  finally { if (watchdog !== undefined) clearImmediate(watchdog); }
};
const identity = { id: "session:ready", name: "ready", kind: "main" as const };
const event: BridgePublish = {
  topic: "fleet.work.ready", kind: "ask", from: identity, to: "session:recipient",
  data: { bridge: { from: "peer", id: "incarnation-regression" } },
};
const pending = (error: unknown): void => {
  expect(error).toBeInstanceOf(atomic.MeshIncarnationPendingError);
  expect(error).toMatchObject({ code: "FABRIC_MESH_INCARNATION_PENDING" });
  expect(atomic.isMeshIncarnationPending(error)).toBe(true);
  expect(isMeshRetryableBusy(error)).toBe(true);
  expect(atomic.isMeshLockTimeout(error)).toBe(false);
  expect(error).not.toBeInstanceOf(atomic.MeshLockTimeoutError);
  for (const field of ["holder", "attempts", "maxGapMs"]) expect(error).not.toHaveProperty(field);
};

let incarnation: ReturnType<typeof deferred<string | undefined>>;
let pidNamespace: ReturnType<typeof deferred<string | undefined>>;
let root: string;
let attempt: Error;
let removals: Array<{ mock: { calls: unknown[][] } }>;
let lockAttempts: string[];
let ownerReads: string[];

beforeEach(() => {
  vi.clearAllMocks();
  vi.stubEnv("PI_FABRIC_MESH_STATE_BACKEND", "file");
  incarnation = deferred<string | undefined>();
  pidNamespace = deferred<string | undefined>();
  vi.spyOn(atomic, "ownProcessIncarnation").mockReturnValue(incarnation.promise);
  vi.spyOn(namespace, "ownPidNamespace").mockReturnValue(pidNamespace.promise);
  root = path.join(os.tmpdir(), `mesh-incarnation-recorded-${randomUUID()}`); // deliberately never created
  attempt = new Error("recorded .lock attempt; do not acquire");
  lockAttempts = [];
  ownerReads = [];
  const mkdir = fs.mkdirSync;
  vi.spyOn(fs, "mkdirSync").mockImplementation(((target: fs.PathLike, ...args: unknown[]) => {
    if (String(target) === root || String(target).startsWith(`${root}${path.sep}`)) return undefined;
    return (mkdir as (...args: unknown[]) => unknown)(target, ...args);
  }) as typeof fs.mkdirSync);
  const mkdtemp = fs.mkdtempSync;
  vi.spyOn(fs, "mkdtempSync").mockImplementation(((prefix: string, ...args: unknown[]) => {
    if (String(prefix).startsWith(path.join(root, ".lock.pending."))) {
      lockAttempts.push(String(prefix));
      throw attempt; // before staging exists: no product release/cleanup path can run
    }
    return (mkdtemp as (...args: unknown[]) => unknown)(prefix, ...args);
  }) as typeof fs.mkdtempSync);
  const read = fs.readFileSync;
  vi.spyOn(fs, "readFileSync").mockImplementation(((target: fs.PathOrFileDescriptor, ...args: unknown[]) => {
    if (target === path.join(root, ".lock", "owner")) ownerReads.push(String(target));
    return (read as (...args: unknown[]) => unknown)(target, ...args);
  }) as typeof fs.readFileSync);
  const forbidden = (): never => { throw new Error("unexpected destructive I/O"); };
  removals = [
    vi.spyOn(fs, "rmSync").mockImplementation(forbidden),
    vi.spyOn(fs, "rmdirSync").mockImplementation(forbidden),
    vi.spyOn(fs, "unlinkSync").mockImplementation(forbidden),
    vi.spyOn(fs, "renameSync").mockImplementation(forbidden),
  ];
});
afterEach(() => {
  try {
    for (const removal of removals) expect(removal.mock.calls).toEqual([]);
    expect(records.remove).not.toHaveBeenCalled();
    expect(records.spawn).not.toHaveBeenCalled();
  } finally { vi.restoreAllMocks(); vi.unstubAllEnvs(); }
});
const noContention = (): void => {
  expect(lockAttempts).toEqual([]);
  expect(ownerReads).toEqual([]);
  expect(records.ticket).not.toHaveBeenCalled();
  expect(records.stats.failed).not.toHaveBeenCalled();
  expect(records.events.waitTimeout).not.toHaveBeenCalled();
};
const store = (protocol: 1 | 2 | 3 = 3): MeshStore =>
  new MeshStore(root, 65_536, 100, { lockProtocol: protocol, lockTimeoutMs: 1_000, stateBackend: "file" });

describe("mesh immutable preparation (smarty-dev#12036)", () => {
  it.each([
    [2, "incarnation"], [3, "incarnation"], [3, "namespace"],
  ] as const)("protocol %s: unresolved %s is promptly pending, not lock contention", async (protocol, unresolved) => {
    if (unresolved === "namespace") incarnation.resolve("1234");
    else pidNamespace.resolve(undefined);
    const lock = new MeshLock(root, { lockProtocol: protocol, lockTimeoutMs: 1_000 }, () => undefined);
    await yielded(); // protocol 3's lazy namespace import has reached the controlled reader
    const operation = vi.fn(() => 1);
    for (const call of [
      () => lock.withLock(operation, 0),
      () => lock.withLock(operation, 50),
      () => lock.withLock(operation, 1_000, "publish", true),
      () => lock.withTryLock(() => lock.withLock(operation), 0),
      () => lock.withTryLock(() => lock.withLock(operation), 50),
    ]) pending(await promptly(call()).catch(error => error));
    expect(lock.ownIncarnationReady).toBe(false);
    expect(operation).not.toHaveBeenCalled();
    noContention();
  });

  it("prepareLock waits outside custody, refuses a cold scope, and reuses one ready lock", async () => {
    const mesh = store();
    const getter = vi.spyOn(MeshLock.prototype, "ownIncarnation", "get");
    let finished = false;
    const first = mesh.prepareLock().then(() => { finished = true; });
    const second = mesh.prepareLock();
    await yielded();
    expect(finished).toBe(false);
    pending(await promptly(mesh.withTryLock(() => mesh.prepareLock(), 0)).catch(error => error));
    noContention();
    const lock = getter.mock.contexts[0] as MeshLock;
    const immutable = lock.ownIncarnation;
    incarnation.resolve("1234");
    pidNamespace.resolve(undefined);
    await Promise.all([first, second]);
    expect(lock.ownIncarnationReady).toBe(true);
    for (let index = 0; index < 2; index++) {
      await mesh.withTryLock(() => mesh.prepareLock(), 0);
      await expect(mesh.exclusive(() => 1, 0)).rejects.toBe(attempt);
    }
    expect(lock.ownIncarnation).toBe(immutable);
    expect(lockAttempts).toHaveLength(2);
    expect(atomic.ownProcessIncarnation).toHaveBeenCalledOnce();
    expect(namespace.ownPidNamespace).toHaveBeenCalledOnce();
    expect(ownerReads).toEqual([]);
    mesh.closeState();
  });

  it.each(["UNKNOWN", "rejected"] as const)("settled %s preparation permits a bounded .lock attempt", async outcome => {
    const mesh = store();
    const ready = mesh.prepareLock();
    if (outcome === "UNKNOWN") incarnation.resolve(undefined);
    else incarnation.reject(new Error("native identity unavailable"));
    pidNamespace.resolve(undefined);
    await ready;
    await expect(mesh.withTryLock(() => mesh.exclusive(() => 1), 0)).rejects.toBe(attempt);
    expect(lockAttempts).toHaveLength(1);
    expect(records.stats.failed).not.toHaveBeenCalled();
    expect(records.events.waitTimeout).not.toHaveBeenCalled();
    mesh.closeState();
  });

  it("protocol 1 preparation is a no-op without starting native readers", async () => {
    const mesh = store(1);
    await promptly(mesh.withTryLock(() => mesh.prepareLock(), 0));
    expect(atomic.ownProcessIncarnation).not.toHaveBeenCalled();
    expect(namespace.ownPidNamespace).not.toHaveBeenCalled();
    noContention();
    mesh.closeState();
  });
});

describe("real store-side bridge preparation and narrow RPC", () => {
  it("CLI boot prepares its one local store before transport and respects an abort during preparation", async () => {
    const preparation = vi.spyOn(MeshStore.prototype, "prepareLock");
    const abort = new AbortController();
    const child = vi.fn();
    const running = runBridge(new Map([
      ["mesh", root], ["name", "local"], ["remote", "peer"],
      ["cursor", path.join(root, "cursor.json")], ["lock-protocol", "3"],
    ]), ["recorded-transport-only"], abort.signal, child);
    await yielded();
    expect(preparation).toHaveBeenCalledOnce();
    expect((preparation.mock.contexts[0] as MeshStore).root).toBe(root);
    expect(child).not.toHaveBeenCalled();
    expect(records.spawn).not.toHaveBeenCalled();
    noContention();
    abort.abort();
    incarnation.resolve(undefined);
    pidNamespace.resolve(undefined);
    await expect(running).resolves.toBe(0);
    expect(child).not.toHaveBeenCalled();
    expect(records.spawn).not.toHaveBeenCalled();
    expect(atomic.ownProcessIncarnation).toHaveBeenCalledOnce();
    expect(namespace.ownPidNamespace).toHaveBeenCalledOnce();
    (preparation.mock.contexts[0] as MeshStore).closeState();
  });

  it("cold mirror and publication wait before writes; later bounded attempts reuse the same store/lock", async () => {
    const mesh = store();
    const side = new StoreBridgeSide(mesh, "peer");
    const writes = [vi.spyOn(mesh, "writeBatch"), vi.spyOn(mesh, "publish"), vi.spyOn(mesh, "publishBatch")];
    const locks = vi.spyOn(MeshLock.prototype, "withLock");
    const operations = [side.mirror({ hosts: [], participants: [] }), side.publish(event), side.publishBatch([{ event }])]
      .map(promise => promise.catch(error => error));
    await yielded();
    for (const write of writes) expect(write).not.toHaveBeenCalled();
    noContention();
    incarnation.resolve("1234");
    pidNamespace.resolve(undefined);
    for (const error of await Promise.all(operations)) expect(error).toBe(attempt);
    for (let index = 0; index < 2; index++) {
      await expect(mesh.withTryLock(() => side.publish(event), 0)).rejects.toBe(attempt);
    }
    expect(side.store).toBe(mesh);
    expect(new Set(locks.mock.contexts).size).toBe(1);
    expect((locks.mock.contexts[0] as MeshLock).ownIncarnationReady).toBe(true);
    expect(lockAttempts).toHaveLength(5);
    expect(atomic.ownProcessIncarnation).toHaveBeenCalledOnce();
    expect(namespace.ownPidNamespace).toHaveBeenCalledOnce();
    mesh.closeState();
  });

  it("agent hello prepares before subscription; RPC preserves pending/snapshot/timeout without trusting arbitrary codes", async () => {
    const mesh = store();
    const side = new StoreBridgeSide(mesh, "peer");
    const subscribe = vi.spyOn(side, "subscribeChanges").mockReturnValue(() => undefined);
    const toAgent = new PassThrough();
    const fromAgent = new PassThrough();
    const replies: Array<{ code?: string }> = [];
    fromAgent.on("data", chunk => replies.push(JSON.parse(String(chunk))));
    const served = serveBridgeAgent(side, toAgent, fromAgent);
    const remote = new RemoteBridgeSide(fromAgent, toAgent, 1_000);
    try {
      const hello = remote.hello(true);
      await yielded();
      expect(subscribe).not.toHaveBeenCalled();
      expect(replies).toEqual([]);
      incarnation.resolve(undefined);
      pidNamespace.resolve(undefined);
      await hello;
      expect(subscribe).toHaveBeenCalledOnce();
      const latest = vi.spyOn(side, "latestSequence");
      for (const [failure, code, isPending, isSnapshot, isTimeout] of [
        [new atomic.MeshIncarnationPendingError(), atomic.MESH_INCARNATION_PENDING_CODE, true, false, false],
        [new atomic.MeshSnapshotRetriesExhaustedError(1), atomic.MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, false, true, false],
        [new atomic.MeshLockTimeoutError(" (recorded holder)", 1, 0), atomic.MESH_LOCK_TIMEOUT_CODE, false, false, true],
      ] as const) {
        latest.mockRejectedValueOnce(failure);
        const received = await remote.latestSequence().catch(error => error);
        expect(received).toBeInstanceOf(Error);
        expect(received).toMatchObject({ code });
        expect(replies.at(-1)?.code).toBe(code);
        expect(atomic.isMeshIncarnationPending(received)).toBe(isPending);
        expect(atomic.isMeshSnapshotRetriesExhausted(received)).toBe(isSnapshot);
        expect(atomic.isMeshLockTimeout(received)).toBe(isTimeout);
        expect(isMeshRetryableBusy(received)).toBe(true);
      }
      noContention();
    } finally {
      remote.close();
      await served;
      mesh.closeState();
    }

    const requests = new PassThrough();
    const responses = new PassThrough();
    const hostile = new RemoteBridgeSide(responses, requests, 1_000);
    requests.on("data", chunk => {
      const request = JSON.parse(String(chunk)) as { id: number };
      responses.write(`${JSON.stringify({ id: request.id, ok: false, error: "untrusted", code: "FABRIC_PEER_INVENTED",
        attempts: 0, holder: "forged", retryable: true })}\n`);
    });
    try {
      const error = await hostile.latestSequence().catch(error => error);
      expect(error).toBeInstanceOf(Error);
      for (const property of ["code", "attempts", "holder", "retryable"]) expect(error).not.toHaveProperty(property);
      expect(isMeshRetryableBusy(error)).toBe(false);
    } finally { hostile.close(); }
  });
});

describe("optimistic snapshot exhaustion is not preparation or contention", () => {
  it("one invalidated snapshot in a ready try-scope carries its own retry count, while actual lock timeout stays timeout", async () => {
    const mesh = store(2);
    incarnation.resolve(undefined);
    await mesh.prepareLock();
    let physicalIdentity = "prepared-state";
    const identities = vi.spyOn(journal, "stateReadIdentity").mockImplementation(file =>
      file === path.join(root, "state.json") ? physicalIdentity : undefined);
    const commit = vi.fn(() => []);
    const locks = vi.spyOn(MeshLock.prototype, "withLock").mockImplementation(async function<T>(this: MeshLock, operation: () => T) {
      expect(this.ownIncarnationReady).toBe(true);
      physicalIdentity = "replacement-state"; // after off-lock preparation, before the actual validation callback
      return operation(); // recorded custody; no acquire/release I/O
    });
    const exhausted = await mesh.withTryLock(() => mesh.writeBatch({ identity, ops: [], prepare: commit }), 0)
      .catch(error => error);
    expect(exhausted).toBeInstanceOf(atomic.MeshSnapshotRetriesExhaustedError);
    expect(exhausted).toMatchObject({ code: "FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED", attempts: 1 });
    expect(atomic.isMeshSnapshotRetriesExhausted(exhausted)).toBe(true);
    expect(isMeshRetryableBusy(exhausted)).toBe(true);
    expect(atomic.isMeshIncarnationPending(exhausted)).toBe(false);
    expect(atomic.isMeshLockTimeout(exhausted)).toBe(false);
    expect(exhausted).not.toBeInstanceOf(atomic.MeshLockTimeoutError);
    expect(exhausted).not.toHaveProperty("holder");
    expect(locks).toHaveBeenCalledOnce();
    expect(identities).toHaveBeenCalled();
    expect(commit).not.toHaveBeenCalled(); // stale data never reaches the user's batch callback
    noContention();

    const timeout = new atomic.MeshLockTimeoutError(" (recorded contention)", 1, 0);
    locks.mockImplementationOnce(async () => {
      physicalIdentity = "replacement-during-contention";
      throw timeout;
    });
    const contended = await mesh.withTryLock(() => mesh.writeBatch({ identity, ops: [], prepare: commit }), 0)
      .catch(error => error);
    expect(contended).toBe(timeout);
    expect(atomic.isMeshLockTimeout(contended)).toBe(true);
    expect(atomic.isMeshSnapshotRetriesExhausted(contended)).toBe(false);
    expect(atomic.isMeshIncarnationPending(contended)).toBe(false);

    locks.mockImplementation(async function<T>(this: MeshLock, operation: () => T) {
      expect(this.ownIncarnationReady).toBe(true);
      return operation(); // unchanged physical identity: the write-free batch is safe to admit
    });
    await expect(mesh.withTryLock(() => mesh.writeBatch({ identity, ops: [], prepare: commit }), 0)).resolves.toEqual([]);
    expect(commit).toHaveBeenCalledOnce();
    expect(locks).toHaveBeenCalledTimes(3);
    noContention();
    mesh.closeState();
  });

  it("ordinary warm mirror exhausts two snapshot validations at its deadline, then admits on the same ready lock", async () => {
    const mesh = store(2);
    const getter = vi.spyOn(MeshLock.prototype, "ownIncarnation", "get");
    incarnation.resolve("1234");
    await mesh.prepareLock();
    const lock = getter.mock.contexts[0] as MeshLock;
    const immutable = lock.ownIncarnation;
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const side = new StoreBridgeSide(mesh, "peer");
    let physicalIdentity = "prepared-state";
    vi.spyOn(journal, "stateReadIdentity").mockImplementation(file =>
      file === path.join(root, "state.json") ? physicalIdentity : undefined);
    const admission = vi.fn();
    const committed = vi.fn();
    const writeBatch = mesh.writeBatch.bind(mesh);
    vi.spyOn(mesh, "writeBatch").mockImplementation(input => writeBatch({
      ...input,
      prepare: (...args) => { admission(); return input.prepare!(...args); },
      afterCommit: (...args) => { committed(); input.afterCommit!(...args); },
    }));
    const writes = [vi.spyOn(fs, "writeFileSync"), vi.spyOn(fs, "appendFileSync")];
    const budgets: Array<number | undefined> = [];
    let replace = true;
    const locks = vi.spyOn(MeshLock.prototype, "withLock").mockImplementation(async function<T>(
      this: MeshLock, operation: () => T, remaining?: number,
    ) {
      expect(this).toBe(lock);
      expect(this.ownIncarnationReady).toBe(true);
      expect(this.ownIncarnation).toBe(immutable);
      expect(this.tryLockScope.getStore()?.active).not.toBe(true);
      budgets.push(remaining);
      if (replace) {
        physicalIdentity = `replacement-state-${budgets.length}`;
        now += 500; // first validation can retry; the second reaches the fixed 1,000 ms deadline
      }
      return operation(); // real StateFile validation and admission, without acquire/release I/O
    });
    try {
      expect(mesh.tryLockBudgetMs).toBeUndefined();
      const exhausted = await side.mirror({ hosts: [], participants: [] }).catch(error => error);
      expect(exhausted).toBeInstanceOf(atomic.MeshSnapshotRetriesExhaustedError);
      expect(exhausted).toMatchObject({ code: "FABRIC_MESH_SNAPSHOT_RETRIES_EXHAUSTED", attempts: 2 });
      expect(isMeshRetryableBusy(exhausted)).toBe(true);
      expect(atomic.isMeshIncarnationPending(exhausted)).toBe(false);
      expect(atomic.isMeshLockTimeout(exhausted)).toBe(false);
      expect(exhausted).not.toHaveProperty("holder");
      expect(budgets).toEqual([1_000, 500]);
      expect(admission).not.toHaveBeenCalled();
      expect(committed).not.toHaveBeenCalled();
      expect(mesh.listAll("", { fresh: true })).toEqual([]);
      noContention();

      replace = false;
      await expect(side.mirror({ hosts: [], participants: [] })).resolves.toBeUndefined();
      expect(side.store).toBe(mesh);
      expect(lock.ownIncarnationReady).toBe(true);
      expect(lock.ownIncarnation).toBe(immutable);
      expect(atomic.ownProcessIncarnation).toHaveBeenCalledOnce();
      expect(locks).toHaveBeenCalledTimes(3);
      expect(budgets).toEqual([1_000, 500, 1_000]);
      expect(admission).toHaveBeenCalledOnce(); // the bridge's actual mirror plan ran only after validation
      expect(committed).toHaveBeenCalledOnce();
      expect(mesh.listAll("", { fresh: true })).toEqual([]);
      expect(await side.owned()).toEqual({ hosts: [], participants: [] });
      expect(fs.existsSync(root)).toBe(false);
      for (const write of writes) expect(write).not.toHaveBeenCalled();
      noContention();
    } finally { mesh.closeState(); }
  });

  it.each([
    ["mesh incarnation pending", () => new atomic.MeshIncarnationPendingError()],
    ["mesh snapshot retries exhausted", () => new atomic.MeshSnapshotRetriesExhaustedError(1)],
  ] as const)("background warning identifies %s, never lock contention or WAL cap", (label, refusal) => {
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const retry = new atomic.MeshBackgroundRetry("inc12036", 100, 5_000, 0);
    try {
      const error = refusal();
      expect(retry.failure(error)).toBe(true);
      expect(warning).toHaveBeenCalledOnce();
      const message = String(warning.mock.calls[0]![0]);
      expect(message).toContain(`${label}; retrying in `);
      expect(message).toContain(error.code);
      expect(message).not.toMatch(/mesh lock timeout|mesh WAL cap|SQLite WAL|held by/);
      noContention();
    } finally { retry.close(); }
  });
});
