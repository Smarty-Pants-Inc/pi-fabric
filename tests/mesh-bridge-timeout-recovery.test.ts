import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isMeshLockTimeout, isMeshSnapshotRetriesExhausted, MeshLockTimeoutError } from "../src/core/atomic-write.js";
import { BridgeOwnershipError, BridgeStaleIncarnationError, MeshBridge, RemoteBridgeSide,
  serveBridgeAgent, StoreBridgeSide } from "../src/mesh/bridge.js";
import { MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";
import { isMeshRetryableBusy, MeshStateBusyError } from "../src/mesh/state-backend.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { writeHostLease } from "../src/topology/host-leases.js";
import { writeParticipantFile } from "../src/topology/participant-files.js";

// Existing-behavior recovery guard, not a new retry or fairness fix.
// The CLI canary's sub-budget hold cannot reach this full-budget failure path.
// Same native-presence + PassThrough RPC fixture as mesh-bridge-busy-streak. No fake
// clocks, timeout injection, admission changes, or extra production retry layer.
// 500 ms is the COMPLETE configured store budget, not a withTryLock/zero-budget
// refusal. This guards recovery policy, not fairness or the production 10 s tail.
const BUDGET_MS = 500;
const TOPIC = "fabric.control.ack";
const WORK_TOPIC = "fleet.work.timeout-recovery";
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  const errors: unknown[] = [];
  for (const close of cleanups.splice(0).reverse()) {
    try { await close(); } catch (error) { errors.push(error); }
  }
  vi.restoreAllMocks();
  if (errors.length) throw new AggregateError(errors, "timeout-recovery fixture cleanup failed");
});
const deferred = <T = void>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
};
const within = async <T>(work: Promise<T>, label: string, ms = 5_000): Promise<T> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<T>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} did not settle within ${ms} ms`)), ms);
    })]);
  } finally { if (timer !== undefined) clearTimeout(timer); }
};
// Normal wake: real fs.watch (as production uses); one deadline is only a missed-event
// safety net. Check after installing the watch too, so a completed checkpoint cannot be missed.
const changed = <T>(root: string, read: () => T | undefined): Promise<T> => new Promise((resolve, reject) => {
  const finish = (error?: Error, value?: T) => {
    clearTimeout(timer); watcher.close();
    if (error) reject(error); else resolve(value!);
  };
  const probe = () => {
    try { const value = read(); if (value !== undefined) finish(undefined, value); }
    catch (error) { finish(error as Error); }
  };
  const watcher = fs.watch(root, probe);
  watcher.once("error", finish);
  const timer = setTimeout(() => finish(new Error("checkpoint/delivery notification missing")), 5_000);
  probe();
});
const hash = (id: string) => createHash("sha256").update(id).digest("hex");
const native = async (store: MeshStore, name: string): Promise<MeshIdentity> => {
  const now = Date.now(), id = `session:timeout-${name}-00000000`;
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
const fixture = async () => {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "mesh-bridge-timeout-recovery-"));
  const options = { stateBackend: "sqlite" as const, lockProtocol: 1 as const, lockTimeoutMs: BUDGET_MS };
  const hub = new MeshStore(path.join(base, "hub"), 65_536, 100, options);
  const far = new MeshStore(path.join(base, "far"), 65_536, 100, options);
  const local = new StoreBridgeSide(hub, "far"), agent = new StoreBridgeSide(far, "hub");
  const input = new PassThrough(), output = new PassThrough();
  const served = serveBridgeAgent(agent, input, output);
  const remote = new RemoteBridgeSide(output, input, 5_000);
  const cursorPath = path.join(base, "cursor.json"), logs: string[] = [];
  const retry = deferred(), recovered = deferred(), toLocalRetry = deferred<string>();
  const bridge = new MeshBridge({ local, remote, localName: "hub", remoteName: "far", cursorPath,
    startDeadlineMs: 6_000, log: message => {
      logs.push(message);
      if (message.includes("retrying in")) retry.resolve();
      if (message.includes(" (toLocal); retrying in ")) toLocalRetry.resolve(message);
      if (message.startsWith("bridge busy streak recovered (toLocal)")) recovered.resolve();
    } });
  let work = Promise.resolve(), settled = false, failure: unknown;
  cleanups.push(async () => {
    await bridge.stop();
    remote.close();
    await within(Promise.all([work, served]), "bridge and RPC joined");
    hub.closeState(); far.closeState();
    fs.rmSync(base, { recursive: true, force: true });
  });
  expect([hub.stateBackend, far.stateBackend]).toEqual(["sqlite", "sqlite"]);
  const hubId = await native(hub, "hub"), farId = await native(far, "far");
  await remote.hello();
  const starts = vi.spyOn(bridge, "start");
  const cursor = () => JSON.parse(fs.readFileSync(cursorPath, "utf8")) as {
    toLocal: { after: number; mark: number }; toRemote: { after: number; mark: number };
  };
  return { base, hub, far, local, agent, remote, bridge, cursor, cursorPath, hubId, farId, logs, retry, recovered, toLocalRetry, starts,
    launch(method: "startWithBackoff" | "run") {
      settled = false; failure = undefined;
      work = bridge[method]().then(() => { settled = true; }, error => { failure = error; settled = true; });
    },
    work: () => work, settled: () => settled, failure: () => failure,
  };
};
type Fixture = Awaited<ReturnType<typeof fixture>>;

// This independently owned Node child holds a real SQLite writer, never a same-process
// raw descriptor (closing one could drop the store's POSIX locks). It self-expires in
// 10 s; spawn has a 12 s kill cap; teardown releases/kills ONLY this child and joins close.
const holdWriter = async (store: MeshStore) => {
  const child = spawn(process.execPath, ["-e", `
    const { DatabaseSync } = require("node:sqlite");
    const db = new DatabaseSync(process.argv[1], { timeout: 0 });
    db.exec("BEGIN IMMEDIATE");
    db.prepare("UPDATE meta SET value = value WHERE name = 'commit_no'").run();
    let done = false;
    const finish = code => {
      if (done) return; done = true; clearTimeout(timer);
      try { db.exec("ROLLBACK"); } finally { db.close(); process.stdin.destroy(); }
      process.exitCode = code;
    };
    const timer = setTimeout(() => finish(2), 10000);
    process.stdin.once("data", () => finish(0));
    process.stdin.once("end", () => finish(0));
    process.stdout.write("HELD\\n");
  `, path.join(store.root, "state.db")], { stdio: ["pipe", "pipe", "pipe"], timeout: 12_000, killSignal: "SIGKILL" });
  const ready = deferred();
  let stdout = "", stderr = "";
  child.stdout.on("data", chunk => { stdout = (stdout + chunk).slice(-2_048); if (stdout.includes("HELD\n")) ready.resolve(); });
  child.stderr.on("data", chunk => { stderr = (stderr + chunk).slice(-2_048); });
  let spawnError: Error | undefined;
  child.once("error", error => { spawnError = error; });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => {
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  child.stdin.on("error", () => undefined); // A failed/expired child must still be joined.
  const release = () => { if (!child.stdin.destroyed && !child.stdin.writableEnded) child.stdin.end("release\n"); };
  cleanups.push(async () => {
    release();
    try { await within(closed, "SQLite holder close", 2_000); }
    catch { child.kill("SIGKILL"); await within(closed, "killed SQLite holder close", 2_000); }
  });
  await within(Promise.race([ready.promise, closed.then(() => {
    throw spawnError ?? new Error(`SQLite holder exited before HELD: ${stderr}`);
  })]), "SQLite writer acquired");
  return {
    held: () => child.exitCode === null && child.signalCode === null && !child.stdin.writableEnded,
    release: async () => { release(); expect(await within(closed, "SQLite writer released")).toEqual({ code: 0, signal: null }); },
  };
};
const send = (f: Fixture, text = "deliver once") => f.far.publish({ topic: TOPIC, kind: "ack", from: f.farId,
  to: f.hubId.id, text, durable: true, data: { version: 1, commandId: text, targetId: f.farId.id, accepted: true } });
const receipts = (store: MeshStore) => {
  const directory = path.join(store.root, "event-receipts");
  return fs.existsSync(directory) ? fs.readdirSync(directory).filter(name => name.endsWith(".json"))
    .map(name => JSON.parse(fs.readFileSync(path.join(directory, name), "utf8")) as MeshEvent) : [];
};
const delivered = (f: Fixture, sent: MeshEvent) => changed(f.base, () => f.cursor().toLocal.after >= sent.sequence ? true : undefined);
const assertOnce = (f: Fixture, sent: MeshEvent) => {
  const events = f.hub.read({ topic: TOPIC });
  expect(events).toHaveLength(1);
  expect(events[0]).toMatchObject({ verification: "bridge", text: sent.text, data: { bridge: { from: "far", id: sent.id } } });
  expect(receipts(f.hub)).toEqual([events[0]]);
  expect(f.cursor().toLocal.after).toBeGreaterThanOrEqual(sent.sequence);
  expect(f.cursor().toLocal.mark).toBe(events[0]!.sequence);
  expect(f.failure()).toBeUndefined();
};
const idleDirections = async (f: Fixture) => {
  const idle = [deferred(), deferred()];
  [f.local, f.agent].forEach((side, index) => {
    const tail = side.tail.bind(side);
    vi.spyOn(side, "tail").mockImplementation(async (...args) => {
      const page = await tail(...args);
      if (page.events.length === 0) idle[index]!.resolve();
      return page;
    });
  });
  f.launch("run");
  await within(Promise.all(idle.map(value => value.promise)), "both live directions idle");
};

describe("bridge recovery after a real complete lock budget", () => {
  it.each(["startWithBackoff", "run"] as const)("%s retains readiness/backlog through full-budget SQLite mirror timeouts", async method => {
    const f = await fixture(), writer = await holdWriter(f.far);
    const failures: unknown[] = [], wire: unknown[] = [];
    const write = f.far.writeBatch.bind(f.far), mirror = f.remote.mirror.bind(f.remote);
    vi.spyOn(f.far, "writeBatch").mockImplementation(async (...args) => {
      try { return await write(...args); } catch (error) { failures.push(error); throw error; }
    });
    vi.spyOn(f.remote, "mirror").mockImplementation(async (...args) => {
      try { return await mirror(...args); } catch (error) { wire.push(error); throw error; }
    });
    f.launch(method);
    await within(f.retry.promise, "readiness retry after three exhausted writer budgets");
    expect(writer.held()).toBe(true);
    expect(f.settled()).toBe(false);
    expect(failures).toHaveLength(3);
    for (const error of failures) {
      expect(error).toBeInstanceOf(MeshStateBusyError);
      expect(isMeshLockTimeout(error)).toBe(true);
      expect((error as MeshStateBusyError).waitedMs).toBeGreaterThanOrEqual(BUDGET_MS);
      expect((error as MeshStateBusyError).attempts).toBeGreaterThan(1);
      expect((error as MeshStateBusyError).database).toBe(path.join(f.far.root, "state.db"));
    }
    // mirror has its own existing three-plan bound: do not pretend its outer error
    // is still a lock timeout, or that the lossy wire retains SQLite holder fields.
    expect(wire).toHaveLength(1);
    expect(isMeshSnapshotRetriesExhausted(wire[0])).toBe(true);
    expect(isMeshRetryableBusy(wire[0])).toBe(true);
    expect(wire[0]).not.toHaveProperty("busyCode");
    expect(f.logs.some(line => line.includes("mesh snapshot retries exhausted; retrying in 100 ms"))).toBe(true);
    const before = f.cursor().toLocal;
    const sent = await send(f); // Arrives AFTER the saved heads, while readiness is refused.
    expect(before.after).toBeLessThan(sent.sequence);
    expect(f.cursor().toLocal).toEqual(before);
    expect(f.hub.read({ topic: TOPIC })).toEqual([]);
    expect(receipts(f.hub)).toEqual([]);
    await writer.release();
    if (method === "startWithBackoff") {
      await within(f.work(), "readiness recovered");
      expect(f.failure()).toBeUndefined();
      expect(f.cursor().toLocal).toEqual(before); // start() must not adopt a new source head.
      f.launch("run");
    }
    await delivered(f, sent);
    const starts = f.starts.mock.calls.length;
    expect(starts).toBeGreaterThanOrEqual(2);
    expect(f.settled()).toBe(false);
    await f.bridge.stop(); await f.work();
    expect(f.starts).toHaveBeenCalledTimes(starts);
    assertOnce(f, sent);
  }, 15_000);

  it("keeps the same run and opposite direction alive after SQLite exhausts held-ingress admission", async () => {
    const f = await fixture();
    await f.bridge.startWithBackoff();
    await idleDirections(f);
    const before = f.cursor().toLocal, writer = await holdWriter(f.hub);
    const nativeRefusals: MeshStateBusyError[] = [];
    const timeouts: Array<{ error: unknown; elapsedMs: number }> = [];
    const publish = f.local.publishWithReceipt.bind(f.local);
    vi.spyOn(f.local, "publishWithReceipt").mockImplementation(async (...args) => {
      const began = performance.now();
      try { return await publish(...args); }
      catch (error) { timeouts.push({ error, elapsedMs: performance.now() - began }); throw error; }
    });
    const fence = f.hub.withStateWriteFence.bind(f.hub);
    vi.spyOn(f.hub, "withStateWriteFence").mockImplementation((...args) => {
      try { return fence(...args); }
      catch (error) { if (error instanceof MeshStateBusyError) nativeRefusals.push(error); throw error; }
    });
    const sent = await send(f);
    // Release only AFTER this direction's actual outer retry, not any readiness/opposite-direction
    // log or the first zero-wait fence refusal: retained initial admission spends 2 x 500 ms.
    const outerRetry = await within(f.toLocalRetry.promise, "outer toLocal held-ingress exhaustion retry");
    expect(writer.held()).toBe(true);
    expect(nativeRefusals.length).toBeGreaterThan(1);
    for (const refusal of nativeRefusals) {
      expect(refusal).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", busyCode: "FABRIC_MESH_STATE_BUSY",
        database: path.join(f.hub.root, "state.db"), attempts: 1, waitedMs: 0 });
    }
    expect(timeouts).toHaveLength(1);
    const { error, elapsedMs } = timeouts[0]!;
    expect(isMeshLockTimeout(error)).toBe(true);
    expect(isMeshRetryableBusy(error)).toBe(true);
    expect(elapsedMs).toBeGreaterThanOrEqual(BUDGET_MS); // Not an immediate/zero-budget outer refusal.
    // Existing retained-deadline exits: queued expiry, the last native fence BUSY, or a plain
    // zero-probe timeout after short-budget admission closes at 80%. Require each exact subtype
    // and deadline shape AND matching outer label; the shared timeout code alone is not enough.
    let label: string;
    if (error instanceof MeshLockQueueExpiredError) {
      expect(error).toMatchObject({ code: "FABRIC_MESH_LOCK_TIMEOUT", preLockCode: "FABRIC_MESH_LOCK_QUEUE_EXPIRED",
        kind: "pre-lock", attempts: 0, lockPath: path.join(f.hub.root, ".lock") });
      expect(error.waitedMs).toBeGreaterThanOrEqual(BUDGET_MS);
      label = "mesh pre-lock queue expiry";
      expect(outerRetry).toContain("; retrying in 0 ms;");
    } else {
      if (error instanceof MeshStateBusyError) {
        expect(error).toBe(nativeRefusals.at(-1)); // Only the observed, call-through native refusal is allowed.
      } else {
        expect(Object.getPrototypeOf(error)).toBe(MeshLockTimeoutError.prototype);
        expect(error).toMatchObject({ name: "MeshLockTimeoutError", code: "FABRIC_MESH_LOCK_TIMEOUT",
          attempts: 0, maxGapMs: 0, stalled: false, holder: " (lock directory has no owner record)" });
        expect(error).not.toHaveProperty("busyCode");
      }
      expect(error).not.toHaveProperty("preLockCode");
      label = "mesh lock timeout";
    }
    expect(outerRetry).toMatch(new RegExp(`^${label} \\(toLocal\\); retrying in \\d+ ms; 1 failed direction attempts, ` +
      `elapsed \\d+ ms; first=${label}; last=${label}; retainedOffLockMs=\\d+: ${label}$`));
    expect(f.logs.filter(line => line.includes(" (toLocal); retrying in "))).toEqual([outerRetry]);
    expect(f.cursor().toLocal).toEqual(before);
    expect(f.hub.read({ topic: TOPIC })).toEqual([]);
    expect(receipts(f.hub)).toEqual([]);
    expect(f.settled()).toBe(false);
    const reverse = await f.hub.publish({ topic: WORK_TOPIC, kind: "ask", from: f.hubId, to: f.farId.id, text: "opposite direction" });
    await changed(f.far.root, () => f.far.read({ topic: WORK_TOPIC }).find(event =>
      (event.data as { bridge?: { id?: string } })?.bridge?.id === reverse.id));
    expect(writer.held()).toBe(true);
    await writer.release();
    await within(f.recovered.promise, "same direction recovered");
    await delivered(f, sent);
    expect(f.settled()).toBe(false);
    expect(f.starts).toHaveBeenCalledOnce(); // No restart/start(), no fresh cursor baseline.
    await expect(f.remote.latestSequence()).resolves.toBeGreaterThan(0); // Same RPC transport, still usable.
    await f.bridge.stop(); await f.work();
    assertOnce(f, sent);
  }, 15_000);

  it.each([
    { name: "stale incarnation", error: () => new BridgeStaleIncarnationError("session:stale-00000000") },
    { name: "ownership", error: () => new BridgeOwnershipError("fixture ownership denied") },
    { name: "authorization", error: () => Object.assign(new Error("fixture unauthorized"), { code: "DENIED" }) },
  ])("does not turn permanent $name failures into readiness or direction retries over RPC", async ({ error }) => {
    const f = await fixture();
    // Injection is ONLY the permanent-error counterexample. Timeout cases above
    // depend exclusively on physical writer custody and call-through observations.
    const mirror = vi.spyOn(f.agent, "mirror").mockRejectedValue(error());
    const refused = await f.bridge.startWithBackoff().catch(failure => failure as unknown);
    expect(refused).toBeInstanceOf(Error);
    expect(isMeshRetryableBusy(refused)).toBe(false);
    expect(mirror).toHaveBeenCalledOnce();
    expect(f.starts).toHaveBeenCalledOnce();
    expect(f.logs.some(line => line.includes("retrying"))).toBe(false);
    mirror.mockRestore();
    // A new fixture is required: a stale refusal permanently fences its bridge.
    const live = await fixture();
    await live.bridge.startWithBackoff();
    await idleDirections(live);
    const before = live.cursor().toLocal;
    const tail = vi.spyOn(live.agent, "tail").mockClear().mockRejectedValue(error());
    await send(live);
    await within(live.work(), "permanent direction failure terminates run");
    expect(live.failure()).toBeInstanceOf(Error);
    expect(isMeshRetryableBusy(live.failure())).toBe(false);
    expect(tail).toHaveBeenCalledOnce();
    expect(live.starts).toHaveBeenCalledOnce();
    expect(live.logs.some(line => line.includes("retrying"))).toBe(false);
    expect(live.cursor().toLocal).toEqual(before);
    expect(live.hub.read({ topic: TOPIC })).toEqual([]);
    expect(receipts(live.hub)).toEqual([]);
  }, 15_000);
});
