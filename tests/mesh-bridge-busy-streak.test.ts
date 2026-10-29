import { createHash } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance as monotonicClock } from "node:perf_hooks";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshIncarnationPendingError, MeshLockForeignHolderError, MeshLockTimeoutError,
  MeshSnapshotRetriesExhaustedError } from "../src/core/atomic-write.js";
import { BridgeMirrorDeferredError, MeshBridge, RemoteBridgeSide, serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { meshLockQueueDirectory } from "../src/mesh/lock-queue.js";
import { MeshLock, MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";
import { MeshStateWalCapError } from "../src/mesh/state-sqlite.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Linux-only physical case: observes rmdirSync(.lock); non-Linux releases through rename/verifiedRemove.
// KEEP: failure paths cannot cheaply sustain >10 s of deterministic mixed native
// refusals. Inject real typed failures only at the source page boundary; run(),
// both direction loops, ownership, durable publication and cursor saves are real.
const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.restoreAllMocks(); vi.useRealTimers(); }
});
const hash = (value: string) => createHash("sha256").update(value).digest("hex");
const native = async (store: MeshStore, name: string, ttl = 120_000) => {
  const now = Date.now(), id = `session:busy-streak-${name}`;
  const identity: MeshIdentity = { id, name: "main", kind: "main", sessionId: name };
  await store.put({ key: `topology/hosts/${hash(id)}`, identity, value: {
    format: 1, id, rootId: id, identity, startedAt: now, updatedAt: now, expiresAt: now + ttl,
  } });
  writeHostLease(store.root, { id, rootId: id, identityId: id, startedAt: now, updatedAt: now, expiresAt: now + ttl });
  writeParticipantFile(store.root, { key: `topology/participants/${hash(id)}`, version: 1, updatedAt: now, updatedBy: identity,
    value: { format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id, name, label: name,
      status: "idle", runner: "pi", transport: "host", capabilities: ["steer"], startedAt: now, updatedAt: now, controlProtocol: "v1" } });
  return identity;
};
const PRIVATE_SENTINELS = ["BUSY_UNTRUSTED_MESSAGE_SENTINEL", "BUSY_UNTRUSTED_OWNER_SENTINEL",
  "/untrusted-busy-sentinel/private/.lock", "BUSY_UNTRUSTED_RECOVERY_SENTINEL"] as const;
const withUntrustedDiagnostics = <T extends Error>(error: T): T => {
  // Deliberately overwrite AFTER construction: constructor-level projection cannot mask a
  // missing redaction at the outer streak callsite. The typed error still selects real retry policy.
  error.message = `${error.message}\n${PRIVATE_SENTINELS.join(" ")}`;
  return Object.assign(error, { owner: PRIVATE_SENTINELS[1], path: PRIVATE_SENTINELS[2], recovery: PRIVATE_SENTINELS[3] });
};
const assertSafeLogs = (logs: readonly { message: string }[]) => {
  for (const { message } of logs) for (const sentinel of PRIVATE_SENTINELS) expect(message).not.toContain(sentinel);
};
const timeout = () => withUntrustedDiagnostics(new MeshLockTimeoutError("held fixture", 1, 0));
const pending = () => withUntrustedDiagnostics(new MeshIncarnationPendingError());
const queueExpired = () => withUntrustedDiagnostics(new MeshLockQueueExpiredError("fixture/.lock", 1, 1_000));
const TOPIC = "fleet.work.busy-streak.1";

const fixture = async (fail: (attempt: number) => Error | undefined | Promise<Error | undefined>, log?: (message: string) => void, overRpc = false, ttl = 120_000) => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-busy-streak-"));
  const hub = new MeshStore(path.join(base, "hub"), 65_536, 100, { stateBackend: "file" });
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, { stateBackend: "file" });
  vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout"] });
  const epoch = Date.now();
  let wallCorrection = 0;
  // The existing node:perf_hooks clock seam remains monotonic while Date jumps.
  const now = () => Date.now() - wallCorrection;
  vi.spyOn(monotonicClock, "now").mockImplementation(now);
  vi.spyOn(Math, "random").mockReturnValue(0); // deterministic equal-jitter lower edge
  const sender = await native(hub, "hub", ttl), receiver = await native(far, "far", ttl);
  const local = new StoreBridgeSide(hub, "far", now), agent = new StoreBridgeSide(far, "hub", now);
  // As in membership-defer's scheduler fixture, withhold unrelated topology
  // notifications: retry timers must be sufficient; no fake read/publish results.
  vi.spyOn(local, "subscribeChanges").mockImplementation(() => () => undefined);
  vi.spyOn(agent, "subscribeChanges").mockImplementation(() => () => undefined);
  let remote: StoreBridgeSide | RemoteBridgeSide = agent;
  let closeTransport: (() => Promise<void>) | undefined;
  if (overRpc) {
    const toAgent = new PassThrough(), fromAgent = new PassThrough();
    const rpc = new RemoteBridgeSide(fromAgent, toAgent);
    const served = serveBridgeAgent(agent, toAgent, fromAgent);
    remote = rpc;
    closeTransport = async () => { rpc.close(); await served; };
    await rpc.hello(); // Negotiate tail so the refusal traverses the agent's real source page operation.
  }
  const direction = overRpc ? "toLocal" : "toRemote";
  const logs: Array<{ message: string; at: number }> = [];
  let didRecover!: () => void, didInitialRetry!: () => void;
  const recovered = new Promise<void>(done => { didRecover = done; });
  const initialRetry = new Promise<void>(done => { didInitialRetry = done; });
  const cursorPath = path.join(base, "cursor.json");
  const bridge = new MeshBridge({ local, remote, localName: "hub", remoteName: "far", cursorPath,
    clock: now, stopMs: 50, log: message => {
      logs.push({ message, at: now() - epoch });
      if (message.startsWith(`bridge busy streak recovered (${direction})`)) didRecover();
      if (message.includes(`(${direction}); retrying in`) && message.includes("failed direction attempts")) didInitialRetry();
      log?.(message);
    } });
  let running: Promise<void> | undefined, failure: unknown;
  cleanups.push(async () => { await bridge.stop(); await running; await closeTransport?.(); hub.closeState(); far.closeState(); });
  await bridge.start();
  const sent = await (overRpc ? far : hub).publish({ topic: TOPIC, kind: "ask", from: overRpc ? receiver : sender,
    to: overRpc ? sender.id : receiver.id, text: "deliver once" });
  const source = overRpc ? agent : local;
  const attempts: number[] = [], tail = source.tail.bind(source);
  vi.spyOn(source, "tail").mockImplementation(async (...args) => {
    attempts.push(now() - epoch);
    const error = await fail(attempts.length);
    if (error) throw error;
    return tail(...args);
  });
  return {
    bridge, hub, far, local, agent, remote, sent, attempts, recovered, logs,
    launch: async () => {
      running = bridge.run().catch(error => { failure = error; });
      // Native recovery/subscription I/O can outlive a zero-time fake-timer flush. Wait for the
      // first real direction retry log; the injected tail attempt is timestamped before this signal,
      // while fake time is still exactly zero and no retry timer has been advanced.
      await initialRetry;
      expect(attempts[0]).toBe(0);
    },
    jumpWall(delta: number) { wallCorrection += delta; vi.setSystemTime(Date.now() + delta); },
    failure: () => failure,
    streak: () => logs.filter(({ message }) => message.includes(`(${direction})`) && message.includes("failed direction attempts")),
    cursor: () => (JSON.parse(fs.readFileSync(cursorPath, "utf8")) as Record<typeof direction, { after: number }>)[direction].after,
    received: () => (overRpc ? hub : far).read({ topic: TOPIC, after: 0 }),
  };
};
type Fixture = Awaited<ReturnType<typeof fixture>>;
const assertDelivered = (f: Fixture) => {
  expect(f.failure()).toBeUndefined();
  expect(f.received()).toMatchObject([{ text: "deliver once", verification: "bridge", data: { bridge: { id: f.sent.id } } }]);
  expect(f.cursor()).toBe(f.sent.sequence);
};
const summary = (count: number, elapsed: number, first: string, last: string) =>
  `; ${count} failed direction attempts, elapsed ${elapsed} ms; first=${first}; last=${last}; retainedOffLockMs=0`;
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

describe("real MeshBridge direction busy streaks", () => {
  // KEEP: the recorded mixed-refusal failure path needs minutes of virtual time;
  // real stores, routing fences, durable delivery and cursor saves remain in use.
  it("re-samples on a lock-to-mirror failure change and bounds repeated degraded streaks without restarting", async () => {
    let blocked = true;
    const f = await fixture(attempt => attempt === 1 ? timeout() : blocked ?
      withUntrustedDiagnostics(new BridgeMirrorDeferredError(true, 30_000)) : undefined, undefined, false, 600_000);
    const samples: number[] = [], presence = f.agent.presence.bind(f.agent);
    vi.spyOn(f.agent, "presence").mockImplementation(() => { samples.push(monotonicClock.now()); return presence(); });
    const started = monotonicClock.now();
    await f.launch();
    await vi.advanceTimersByTimeAsync(1_200);
    expect(samples.map(at => at - started)).toContain(200); // a NEW sample after the changed failure
    expect(f.logs.some(({ message }) => message.includes("failure class changed") &&
      message.includes("mesh lock timeout") && message.includes("bridge mirror degraded"))).toBe(true);
    expect(f.received()).toEqual([]);
    expect(f.cursor()).toBeLessThan(f.sent.sequence);
    await vi.advanceTimersByTimeAsync(149_000);
    const alarm = f.logs.find(({ message }) => message.includes("alarm: bridge busy streak") && message.includes("toRemote"));
    expect(alarm).toMatchObject({ at: 150_200 });
    expect(alarm!.message).toContain("150000 ms bound");
    expect(alarm!.message).toContain("bridge mirror degraded");
    expect(samples.map(at => at - started)).toContain(150_200);
    expect(f.failure()).toBeUndefined();
    expect(f.received()).toEqual([]);
    blocked = false;
    await vi.advanceTimersByTimeAsync(1_000);
    await f.recovered;
    assertDelivered(f);
    await f.bridge.stop();
    assertSafeLogs(f.logs);
  });

  it("alarms at the streak deadline during an in-flight attempt without abandoning its fence", async () => {
    const gate = deferred(), entered = deferred();
    const f = await fixture(async attempt => {
      if (attempt === 1) return timeout();
      if (attempt === 2) { entered.resolve(); await gate.promise; }
      return undefined;
    }, undefined, false, 600_000);
    cleanups.push(async () => { gate.resolve(); });
    await f.launch();
    await vi.advanceTimersByTimeAsync(200);
    await entered.promise;
    await vi.advanceTimersByTimeAsync(149_800);
    expect(f.logs.find(({ message }) => message.includes("alarm: bridge busy streak"))).toMatchObject({ at: 150_000 });
    expect(f.attempts).toHaveLength(2); // never start a competing drain while a fenced attempt is pending
    expect(f.received()).toEqual([]);
    gate.resolve();
    await vi.advanceTimersByTimeAsync(0);
    await new Promise<void>(resolve => setImmediate(resolve));
    assertDelivered(f);
    await f.bridge.stop();
  });

  it.each([
    { name: "timeout", error: timeout, label: "mesh lock timeout", delay: 200 },
    { name: "incarnation pending", error: pending, label: "mesh incarnation pending", delay: 200 },
    { name: "snapshot exhaustion", error: () => withUntrustedDiagnostics(new MeshSnapshotRetriesExhaustedError(3)),
      label: "mesh snapshot retries exhausted", delay: 200 },
    { name: "queue expiry", error: queueExpired, label: "mesh pre-lock queue expiry", delay: 0 },
    { name: "foreign holder", error: () => withUntrustedDiagnostics(new MeshLockForeignHolderError(
      PRIVATE_SENTINELS[1], 1, 0, "foreign", PRIVATE_SENTINELS[1], PRIVATE_SENTINELS[2], PRIVATE_SENTINELS[3])),
      label: "mesh lock timeout", delay: 200 },
    { name: "mirror deferred", error: () => withUntrustedDiagnostics(new BridgeMirrorDeferredError(true, 1_000)),
      label: "bridge mirror degraded", delay: 1_000 },
    { name: "typed WAL cap", error: () => withUntrustedDiagnostics(new MeshStateWalCapError(120, 96.5, PRIVATE_SENTINELS[1], 2)),
      label: "mesh state WAL cap", delay: 1_000,
      detail: "FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is 120 bytes, above the 96.5-byte cap; 2 WAL reader(s)" },
    { name: "invalid WAL facts", error: () => withUntrustedDiagnostics(new MeshStateWalCapError(NaN, Infinity, PRIVATE_SENTINELS[1], -1)),
      label: "mesh state WAL cap", delay: 1_000,
      detail: "FABRIC_MESH_STATE_WAL_CAP (sizes not available); unknown WAL reader(s)" },
    { name: "throwing WAL projection", error: () => Object.defineProperty(
      withUntrustedDiagnostics(new MeshStateWalCapError(120, 96.5, "", 2)), "walBytes", {
        get() { throw new Error(PRIVATE_SENTINELS[0]); },
      }), label: "mesh state WAL cap", delay: 1_000 },
  ])("projects $name diagnostics at the outer streak callsite even when the log sink throws", async ({ error, label, delay, ...facts }) => {
    const refused = error();
    expect(refused.message).toContain(PRIVATE_SENTINELS[0]);
    const f = await fixture(attempt => attempt === 1 ? refused : undefined, message => {
      if (message.includes("failed direction attempts")) throw new Error("fixture diagnostic sink failure");
    });
    await f.launch();
    expect(f.streak()[0]!.message).toBe(`${label} (toRemote); retrying in ${delay} ms` +
      summary(1, 0, label, label) + `: ${"detail" in facts ? facts.detail : label}`);
    await vi.advanceTimersByTimeAsync(Math.max(1, delay));
    await f.recovered;
    assertDelivered(f);
    expect(f.streak()).toHaveLength(2);
    const recovery = f.streak()[1]!;
    expect(recovery.at).toBeGreaterThanOrEqual(delay);
    expect(recovery.at).toBeLessThanOrEqual(Math.max(1, delay));
    expect(recovery.message).toBe("bridge busy streak recovered (toRemote)" + summary(1, recovery.at, label, label));
    await f.bridge.stop();
    assertSafeLogs(f.logs);
  });

  it.each([
    { name: "numeric", message: "FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is 120 bytes, above the 96.5-byte cap; 2 WAL reader(s)" },
    { name: "exponential", message: "FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is 1e+21 bytes, above the 1e-7-byte cap; 0 WAL reader(s)" },
    { name: "unavailable", message: "FABRIC_MESH_STATE_WAL_CAP (sizes not available); unknown WAL reader(s)" },
  ])("retains only projected $name WAL facts after the lossy timeout wire remap", async ({ message }) => {
    const refused = withUntrustedDiagnostics(new MeshLockTimeoutError("held fixture", 1, 0));
    // The legacy wire has no WAL fields, only this canonical projection and a timeout code.
    refused.message = message;
    const f = await fixture(attempt => attempt === 1 ? refused : undefined);
    await f.launch();
    expect(f.streak()[0]!.message).toBe("mesh lock timeout (toRemote); retrying in 200 ms" +
      summary(1, 0, "mesh lock timeout", "mesh lock timeout") + `: ${message}`);
    await vi.advanceTimersByTimeAsync(200);
    await f.recovered;
    assertDelivered(f);
    expect(f.streak()[1]!.message).toBe("bridge busy streak recovered (toRemote)" +
      summary(1, 200, "mesh lock timeout", "mesh lock timeout"));
    assertSafeLogs(f.logs);
  });

  it("does not copy even a valid WAL summary when the timeout message has an untrusted suffix", async () => {
    const refused = withUntrustedDiagnostics(new MeshLockTimeoutError("held fixture", 1, 0));
    refused.message = "FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is 120 bytes, above the 96.5-byte cap; 2 WAL reader(s)" +
      `\n${PRIVATE_SENTINELS.join(" ")}`;
    const f = await fixture(attempt => attempt === 1 ? refused : undefined);
    await f.launch();
    expect(f.streak()[0]!.message).toBe("mesh lock timeout (toRemote); retrying in 200 ms" +
      summary(1, 0, "mesh lock timeout", "mesh lock timeout") + ": mesh lock timeout");
    await vi.advanceTimersByTimeAsync(200);
    await f.recovered;
    assertDelivered(f);
    assertSafeLogs(f.logs);
  });

  const rpcWalSummary = "FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is 120 bytes, above the 96.5-byte cap; 2 WAL reader(s)";
  const rpcExponentialSummary = "FABRIC_MESH_STATE_WAL_CAP: the SQLite WAL is 1e+21 bytes, above the 1e-7-byte cap; 0 WAL reader(s)";
  const rpcUnavailableSummary = "FABRIC_MESH_STATE_WAL_CAP (sizes not available); unknown WAL reader(s)";
  const rpcQueueExpiry = new MeshLockQueueExpiredError("fixture/.lock", 1, 1_000).message;
  it.each([
    { name: "numeric WAL facts", refusal: () => withUntrustedDiagnostics(new MeshStateWalCapError(120, 96.5, PRIVATE_SENTINELS[1], 2)),
      message: rpcWalSummary, detail: rpcWalSummary },
    { name: "exponential WAL facts", refusal: () => withUntrustedDiagnostics(new MeshStateWalCapError(1e21, 1e-7, PRIVATE_SENTINELS[1], 0)),
      message: rpcExponentialSummary, detail: rpcExponentialSummary },
    { name: "unavailable WAL facts", refusal: () => withUntrustedDiagnostics(new MeshStateWalCapError(NaN, Infinity, PRIVATE_SENTINELS[1], -1)),
      message: rpcUnavailableSummary, detail: rpcUnavailableSummary },
    { name: "hostile suffix", refusal: () => Object.assign(timeout(), { message: `${rpcWalSummary}\n${PRIVATE_SENTINELS.join(" ")}` }),
      message: `${rpcWalSummary}\n${PRIVATE_SENTINELS.join(" ")}`, detail: "mesh lock timeout" },
    { name: "queue marker in hostile suffix", refusal: () => Object.assign(timeout(), {
      message: `${rpcWalSummary}\n${PRIVATE_SENTINELS.join(" ")} FABRIC_MESH_LOCK_QUEUE_EXPIRED` }),
      message: `${rpcWalSummary}\n${PRIVATE_SENTINELS.join(" ")} FABRIC_MESH_LOCK_QUEUE_EXPIRED`, detail: "mesh lock timeout" },
    { name: "queue marker without an expiry envelope", refusal: () => Object.assign(timeout(), {
      message: `FABRIC_MESH_LOCK_QUEUE_EXPIRED ${PRIVATE_SENTINELS.join(" ")}` }),
      message: `FABRIC_MESH_LOCK_QUEUE_EXPIRED ${PRIVATE_SENTINELS.join(" ")}`, detail: "mesh lock timeout" },
    { name: "queue expiry envelope with hostile suffix", refusal: () => Object.assign(timeout(), {
      message: `${rpcQueueExpiry}\n${PRIVATE_SENTINELS.join(" ")}` }),
      message: `${rpcQueueExpiry}\n${PRIVATE_SENTINELS.join(" ")}`, detail: "mesh lock timeout" },
    { name: "double wrapper", refusal: () => Object.assign(timeout(), { message: `Bridge agent: ${rpcWalSummary}` }),
      message: `Bridge agent: ${rpcWalSummary}`, detail: "mesh lock timeout" },
  ])("projects only safe facts for $name through the real agent and RemoteBridgeSide without changing timeout retry", async ({ refusal, message, detail }) => {
    const refused = refusal();
    const f = await fixture(attempt => attempt === 1 ? refused : undefined, undefined, true);
    const decoded: unknown[] = [], tail = f.remote.tail.bind(f.remote);
    vi.spyOn(f.remote, "tail").mockImplementation(async (after: number, offset?: number) => {
      try { return await tail(after, offset); }
      catch (error) { decoded.push(error); throw error; }
    });
    await f.launch();
    expect(decoded).toHaveLength(1);
    expect(decoded[0]).toBeInstanceOf(Error);
    expect(decoded[0]).toHaveProperty("code", "FABRIC_MESH_LOCK_TIMEOUT");
    expect(decoded[0]).toHaveProperty("message", `Bridge agent: ${message}`);
    for (const field of ["walBytes", "capBytes", "readerCount", "owner", "path", "recovery", "preLockCode", "kind", "retryable", "waitersAhead", "waitedMs"]) {
      expect(decoded[0]).not.toHaveProperty(field);
    }
    expect(f.streak()).toHaveLength(1);
    expect(f.streak()[0]!.message).toBe("mesh lock timeout (toLocal); retrying in 200 ms" +
      summary(1, 0, "mesh lock timeout", "mesh lock timeout") + `: ${detail}`);
    expect(f.received()).toEqual([]);
    expect(f.cursor()).toBeLessThan(f.sent.sequence);
    await vi.advanceTimersByTimeAsync(199);
    expect(f.attempts).toEqual([0]);
    expect(f.streak()).toHaveLength(1);
    expect(f.received()).toEqual([]);
    expect(f.cursor()).toBeLessThan(f.sent.sequence);
    await vi.advanceTimersByTimeAsync(1);
    await f.recovered;
    assertDelivered(f);
    expect(f.attempts.slice(0, 2)).toEqual([0, 200]);
    expect(f.streak()).toHaveLength(2);
    expect(f.streak()[1]).toEqual({ at: 200, message: "bridge busy streak recovered (toLocal)" +
      summary(1, 200, "mesh lock timeout", "mesh lock timeout") });
    if (detail === "mesh lock timeout") {
      for (const { message: logged } of f.logs) {
        expect(logged).not.toContain("FABRIC_MESH_STATE_WAL_CAP");
        expect(logged).not.toContain("WAL reader(s)");
        expect(logged).not.toContain("120 bytes");
        expect(logged).not.toContain("96.5-byte cap");
      }
    }
    await f.bridge.stop();
    expect(f.streak()).toHaveLength(2);
    assertSafeLogs(f.logs);
  });

  it.each([1, undefined])("keeps genuine RPC queue expiry eligible for a zero-delay retry (waiters=%s)", async waiters => {
    const refused = new MeshLockQueueExpiredError("fixture/.lock", waiters, 1_000);
    const f = await fixture(attempt => attempt === 1 ? refused : undefined, undefined, true);
    const decoded: unknown[] = [], tail = f.remote.tail.bind(f.remote);
    vi.spyOn(f.remote, "tail").mockImplementation(async (after: number, offset?: number) => {
      try { return await tail(after, offset); }
      catch (error) { decoded.push(error); throw error; }
    });
    await f.launch();
    expect(decoded).toHaveLength(1);
    expect(decoded[0]).toBeInstanceOf(Error);
    expect(decoded[0]).toHaveProperty("code", "FABRIC_MESH_LOCK_TIMEOUT");
    expect(decoded[0]).toHaveProperty("message", `Bridge agent: ${refused.message}`);
    for (const field of ["preLockCode", "kind", "retryable", "waitersAhead", "waitedMs"]) {
      expect(decoded[0]).not.toHaveProperty(field);
    }
    const label = "mesh pre-lock queue expiry";
    expect(f.streak()[0]).toEqual({ at: 0, message: `${label} (toLocal); retrying in 0 ms` +
      summary(1, 0, label, label) + `: ${label}` });
    await vi.advanceTimersByTimeAsync(1);
    await f.recovered;
    assertDelivered(f);
    expect(f.attempts.slice(0, 2)).toHaveLength(2);
    expect(f.attempts[1]).toBeGreaterThanOrEqual(0);
    expect(f.attempts[1]).toBeLessThanOrEqual(1);
    expect(f.streak()).toHaveLength(2);
    const recovery = f.streak()[1]!;
    expect(recovery.at).toBeLessThanOrEqual(1);
    expect(recovery.message).toBe("bridge busy streak recovered (toLocal)" + summary(1, recovery.at, label, label));
    await f.bridge.stop();
    expect(f.streak()).toHaveLength(2);
    for (const { message } of f.logs) expect(message).not.toContain("fixture/.lock");
    assertSafeLogs(f.logs);
  });

  it.skipIf(process.platform !== "linux")("logs actual held-ingress off-lock retention across real SQLite fence retries, not outer direction errors", async () => {
    const f = await sqliteBridgeFixture("busy");
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
        appendAt = monotonicClock.now();
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
      if (appendAt !== undefined && !postAppendHolder && sameFile(fd, eventsPath)) {
        // This is after the actual append. Hold the real canonical lock across receipt finalization
        // so the retention metric is proven not to include post-append receipt work.
        postAppendHolder = canonicalHolder(root);
        postAppendReleaseTimer = setTimeout(() => postAppendHolder?.release(), 80);
      }
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
    const sent = await f.far.publish({ topic: TOPIC, kind: "ask", from: f.sender, to: f.recipient.id,
      text: ingressText, durable: true });
    expectedSequence = sent.sequence;
    cursorFileReached();
    await within(refusalReached.promise, "five native SQLite fence refusals");
    expect(refusals.length).toBeGreaterThan(3);
    releaseSqlite();
    await within(cursorReached.promise, "durable toLocal cursor checkpoint");
    expect(f.failure()).toBeUndefined();
    expect(postAppendHolder).toBeDefined();
    await within(postAppendHolder!.entered, "real post-append receipt holder");
    await within(postAppendHolder!.work, "post-append receipt holder release");
    await f.bridge.stop();
    await f.running();

    const committed = f.hub.read({ topic: TOPIC, after: 0 });
    expect(committed).toHaveLength(1);
    expect(committed[0]).toMatchObject({ text: ingressText, verification: "bridge", from: f.sender, to: f.recipient.id,
      data: { bridge: { from: "far", id: sent.id } } });
    expect(JSON.parse(fs.readFileSync(f.cursorPath, "utf8"))).toMatchObject({ toLocal: { after: sent.sequence } });

    console.log("RETENTION_NATIVE_OBSERVATION " + JSON.stringify({ refusals, intervals, appendInterval, logs: f.logs }));
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
    expect(Math.abs(loggedOffLockMs - Math.round(physicalOffLockMs))).toBeLessThanOrEqual(refusals.length * 2 + 2);
    expect(loggedOffLockMs).toBeLessThan(Math.round(physicalOffLockMs + actualLockHoldsMs));
    expect(f.logs.some(message => message.includes("(toLocal); retrying in"))).toBe(false);
    expect(f.logs.some(message => message.includes("(toLocal)") && message.includes("failed direction attempts"))).toBe(false);
    const postAppendIntervals = intervals.slice(appendInterval + 1);
    expect(postAppendIntervals.length, "post-append receipt work reacquired the real canonical lock").toBeGreaterThan(0);
    console.log("RETENTION_LOG_RESULT " + JSON.stringify({ test: "real SQLite held ingress", refusals: refusals.length,
      ticket: refusals[0]!.ticket, receiptInode: refusals[0]!.inode, physicalOffLockGapsMs: gaps.map(gap => +gap.toFixed(2)),
      lateWakeActualMs: +lateWakeMs!.toFixed(2), metricRetainedOffLockMs: loggedOffLockMs,
      actualRefusalLockHoldsMs: +actualLockHoldsMs.toFixed(2), postAppendLockIntervals: postAppendIntervals.length,
      cursorToLocalAfter: sent.sequence, persistedIngressEvent: true, persistedCursor: true, outerDirectionRetry: false }));
  });

  it("does not invent an ingress streak or retention cost for an idle bridge with no fence refusal", async () => {
    const f = await sqliteBridgeFixture("idle"), idleTail = deferred<void>();
    const tail = f.remote.tail.bind(f.remote);
    vi.spyOn(f.remote, "tail").mockImplementation(async (...args) => {
      const page = await tail(...args);
      if (page.events.length === 0) idleTail.resolve();
      return page;
    });
    let busyRefusals = 0;
    const originalExec = DatabaseSync.prototype.exec;
    vi.spyOn(DatabaseSync.prototype, "exec").mockImplementation(function (this: DatabaseSync, sql: string) {
      try { return originalExec.call(this, sql); }
      catch (error) { if (sql.trim().toUpperCase() === "BEGIN IMMEDIATE" && sqliteBusy(error)) busyRefusals++; throw error; }
    });
    f.launch();
    await within(idleTail.promise, "real empty toLocal page");
    await new Promise<void>(resolve => setImmediate(resolve));
    await f.bridge.stop();
    await f.running();
    expect(busyRefusals).toBe(0);
    expect(f.logs.some(message => message.startsWith("bridge ingress busy streak"))).toBe(false);
    expect(f.logs.some(message => message.includes("retainedOffLockMs="))).toBe(false);
    console.log("RETENTION_LOG_RESULT " + JSON.stringify({ test: "idle/no refusal", nativeFenceRefusals: busyRefusals,
      ingressStreakLogs: 0, retainedOffLockCostInvented: false }));
  });
  it("counts failed direction attempts, caps progress at 10 s, and recovers with monotonic duration despite backward Date", async () => {
    const f = await fixture(attempt => attempt <= 13 ? timeout() : undefined);
    await f.launch();
    expect(f.streak()).toHaveLength(1);
    expect(f.streak()[0]!.message).toContain("mesh lock timeout (toRemote); retrying in 200 ms" +
      summary(1, 0, "mesh lock timeout", "mesh lock timeout"));
    await vi.advanceTimersByTimeAsync(1_000);
    f.jumpWall(-60_000);
    await vi.advanceTimersByTimeAsync(9_399);
    expect(f.streak()).toHaveLength(1); // no progress line before the 10 s minimum
    expect(f.received()).toEqual([]);
    expect(f.cursor()).toBeLessThan(f.sent.sequence);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.streak()).toHaveLength(2);
    expect(f.streak()[1]!.message).toContain(summary(13, 10_400, "mesh lock timeout", "mesh lock timeout"));
    const progress = f.streak().filter(({ message }) => message.includes("retrying"));
    expect(progress[1]!.at - progress[0]!.at).toBeGreaterThanOrEqual(10_000);
    await vi.advanceTimersByTimeAsync(1_000);
    await f.recovered; // real fsync and the checkpoint, not merely visible append bytes
    assertDelivered(f);
    expect(f.streak()).toHaveLength(3);
    expect(f.streak()[2]!.message).toBe("bridge busy streak recovered (toRemote)" +
      summary(13, 11_400, "mesh lock timeout", "mesh lock timeout"));
    await f.bridge.stop();
    expect(f.streak().some(({ message }) => /terminal|stopped/.test(message))).toBe(false);
    assertSafeLogs(f.logs); // first, progress and recovery all saw post-construction unsafe messages
  });

  it("logs the failed streak once on terminal failure and never advances its durable cursor", async () => {
    const terminal = withUntrustedDiagnostics(new Error("fixture terminal read failure"));
    const f = await fixture(attempt => attempt === 1 ? timeout() : attempt === 2 ? pending() : terminal);
    await f.launch();
    await vi.advanceTimersByTimeAsync(600);
    expect(f.failure()).toBe(terminal);
    expect(f.streak().map(({ message }) => message)).toEqual([
      expect.stringContaining(summary(1, 0, "mesh lock timeout", "mesh lock timeout")),
      "bridge busy streak resampling (toRemote)" + summary(1, 200, "mesh lock timeout", "mesh lock timeout"),
      expect.stringContaining(summary(1, 0, "mesh incarnation pending", "mesh incarnation pending")),
      "bridge busy streak terminal (toRemote)" + summary(1, 200, "mesh incarnation pending", "mesh incarnation pending"),
    ]);
    expect(f.received()).toEqual([]);
    expect(f.cursor()).toBeLessThan(f.sent.sequence);
    await f.bridge.stop();
    expect(f.streak()).toHaveLength(4);
    assertSafeLogs(f.logs);
  });

  it("logs stopped with the accumulated count/duration and cannot forward after stop", async () => {
    const f = await fixture(() => timeout());
    await f.launch();
    await vi.advanceTimersByTimeAsync(75);
    await f.bridge.stop();
    expect(f.streak().at(-1)!.message).toBe("bridge busy streak stopped (toRemote)" +
      summary(1, 75, "mesh lock timeout", "mesh lock timeout"));
    await vi.advanceTimersByTimeAsync(2_000);
    expect(f.streak()).toHaveLength(2);
    expect(f.received()).toEqual([]);
    expect(f.cursor()).toBeLessThan(f.sent.sequence);
    assertSafeLogs(f.logs);
  });

  it("queue expiry yields immediately without spending the next contention exponential delay", async () => {
    const f = await fixture(attempt => attempt <= 3 ? queueExpired() : attempt === 4 ? timeout() : undefined);
    await f.launch();
    await vi.advanceTimersByTimeAsync(210);
    await f.recovered;
    assertDelivered(f);
    // The real successful drain may read a subsequent empty page. Only the
    // refusal prefix and its first successful page determine these retry gaps.
    const at = f.attempts.slice(0, 5);
    expect(at).toHaveLength(5);
    for (let index = 1; index <= 3; index++) expect(at[index]! - at[index - 1]!).toBeLessThanOrEqual(1);
    expect(at[4]! - at[3]!).toBe(200); // three queue refusals did NOT double the contention retry
    expect(f.streak()[0]!.message).toContain("mesh pre-lock queue expiry (toRemote); retrying in 0 ms" +
      summary(1, 0, "mesh pre-lock queue expiry", "mesh pre-lock queue expiry"));
    const recovery = f.streak().at(-1)!;
    expect(recovery.message).toBe("bridge busy streak recovered (toRemote)" +
      summary(1, recovery.at - at[3]!, "mesh lock timeout", "mesh lock timeout"));
    expect(f.streak()).toHaveLength(4); // queue-expiry streak ends when contention becomes the failure class
    assertSafeLogs(f.logs);
  });
});
