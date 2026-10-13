import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import * as retention from "../src/storage/retention.js";
import { ActorManager, FABRIC_RETENTION_SWEEP_LOCK_ALARM_TOPIC } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";

// smarty-dev#9508 (pi-fabric#883 SEC P2s): the owner's sweep-lock guards.
type Start = typeof retention.startLockedMeshRetentionSweep;
const control = vi.hoisted(() => ({ impl: undefined as undefined | ((...args: unknown[]) => Promise<unknown>),
  spawnArgs: undefined as undefined | ((script: string) => Promise<void>) }));
vi.mock("../src/agents/transports/process-utils.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/agents/transports/process-utils.js")>();
  return { ...actual, scriptSpawnArgs: async (...args: Parameters<typeof actual.scriptSpawnArgs>) => {
    await control.spawnArgs?.(args[0]);
    return actual.scriptSpawnArgs(...args);
  } };
});
vi.mock("../src/storage/retention.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../src/storage/retention.js")>();
  return { ...actual, startLockedMeshRetentionSweep: (...args: unknown[]) =>
    (control.impl ?? (actual.startLockedMeshRetentionSweep as (...a: unknown[]) => Promise<unknown>))(...args) };
});
const realStart = async (): Promise<Start> => (await vi.importActual<typeof import("../src/storage/retention.js")>("../src/storage/retention.js"))
  .startLockedMeshRetentionSweep;

const DAY = 24 * 60 * 60 * 1_000;
const cleanups: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  control.impl = undefined;
  control.spawnArgs = undefined;
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

const owner = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-sweep-guards-"));
  const meshRoot = path.join(root, "mesh");
  const actorRoot = path.join(meshRoot, "actors", "session-a");
  fs.mkdirSync(actorRoot, { recursive: true });
  const script = path.join(root, "sweep.mjs");
  fs.writeFileSync(script, "");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const alarms: unknown[] = [];
  const publish = mesh.publish.bind(mesh);
  vi.spyOn(mesh, "publish").mockImplementation(async (input) => {
    if (input.topic === FABRIC_RETENTION_SWEEP_LOCK_ALARM_TOPIC) alarms.push(input);
    return publish(input);
  });
  vi.spyOn(console, "warn").mockImplementation(() => undefined);
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  const manager = new ActorManager("sweep-guards", { id: "session:sweep-guards", name: "main", kind: "main", sessionId: "sweep-guards" }, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true }, agents, () => {}, {
      actorRoot, persistent: true, canConsumeMesh: () => true, reapDeadSessionPresence: false,
      meshRetentionSweepPath: script, retentionSweepMs: 20, closeGraceMs: 0,
    });
  cleanups.push(async () => {
    await manager.close();
    await agents.close();
    fs.rmSync(root, { recursive: true, force: true });
  });
  return { meshRoot, manager, alarms, script };
};

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
};
// Bounded, turn-counted (not wall-clock) observation: true when `promise` settled within `turns` timer+check turns.
const settlesWithin = async (promise: Promise<unknown>, turns = 250): Promise<boolean> => {
  let settled = false;
  void promise.then(() => { settled = true; }, () => { settled = true; });
  for (let turn = 0; turn < turns && !settled; turn++) {
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  return settled;
};
// Descriptors of this process still open on the sweep lock file.
const heldLockFds = (meshRoot: string): string[] => {
  const lock = path.join(meshRoot, retention.MESH_RETENTION_SWEEP_LOCK);
  return fs.readdirSync("/proc/self/fd").filter((fd) => {
    try { return fs.readlinkSync(path.join("/proc/self/fd", fd)) === lock; } catch { return false; }
  });
};

describe.skipIf(process.platform !== "linux")("mesh retention sweep lock guards (smarty-dev#9508)", () => {
  it("(a) uncertain, busy, uncertain raises exactly one alarm: only a successful acquire re-arms it", async () => {
    const { alarms } = owner();
    const outcomes: unknown[] = [{ uncertain: "flock: not found" }, "busy", { uncertain: "flock: not found" }];
    let calls = 0;
    control.impl = async () => { calls += 1; return outcomes.shift() ?? "busy"; };
    await vi.waitFor(() => expect(calls).toBeGreaterThanOrEqual(5), { timeout: 5_000 });
    expect(alarms).toHaveLength(1);
  });

  it.each(["started", "claimed"] as const)("(a') a real acquire (%s) between two uncertain ticks re-arms the alarm", async (acquired) => {
    const { alarms } = owner();
    const outcomes: unknown[] = [{ uncertain: "flock: not found" }, acquired, { uncertain: "flock: not found" }];
    let calls = 0;
    control.impl = async () => { calls += 1; return outcomes.shift() ?? "busy"; };
    await vi.waitFor(() => expect(calls).toBeGreaterThanOrEqual(5), { timeout: 5_000 });
    expect(alarms).toHaveLength(2);
  });

  it("(b) an owner closed during the acquire claims nothing, spawns nothing and holds no lock", async () => {
    const start = await realStart(); // before the owner exists: its first tick must already see control.impl
    let started = false;
    let closing: Promise<void> | undefined;
    let manager!: ActorManager;
    const outcome = new Promise<unknown>((resolve) => {
      control.impl = async (...args: unknown[]) => {
        control.impl = async () => "busy"; // later ticks
        const [root, interval, spawnSweep, now, wanted] = args as Parameters<Start>;
        closing = manager.close(); // the owner closes while its acquire is in flight
        const result = await start(root, interval, (fd) => { started = true; spawnSweep(fd); }, now, wanted);
        resolve(result);
        return result;
      };
    });
    const { meshRoot, manager: created } = owner();
    manager = created;
    expect(await outcome).toBe("cancelled");
    await closing;
    expect(started).toBe(false);
    expect(fs.readdirSync(meshRoot).filter((name) => name.startsWith(retention.MESH_RETENTION_SWEEP_PREFIX))).toEqual([]);
    // The lock was released: a fresh acquire takes it at once.
    expect(await start(meshRoot, DAY, () => undefined, 10 * DAY)).toBe("started");
  });

  it("(c) close joins an in-flight lock acquisition: it waits for it, which then cancels and releases the lock", async () => {
    const start = await realStart();
    const gate = deferred();
    let entered = false;
    let spawned = 0;
    let outcome: unknown;
    control.impl = async (...args: unknown[]) => {
      control.impl = async () => "busy"; // later ticks
      const [root, interval, , now, wanted] = args as Parameters<Start>;
      entered = true;
      await gate.promise; // the real acquire is delayed until the test releases it
      // Never the real spawn: record it. The lock fd is closed by startLockedMeshRetentionSweep itself.
      outcome = await start(root, interval, () => { spawned += 1; }, now, wanted);
      return outcome;
    };
    const { meshRoot, manager } = owner();
    try {
      await vi.waitFor(() => expect(entered).toBe(true), { timeout: 5_000 });
      const closing = manager.close();
      // RED before the fix: close resolves while the acquisition is still pending (it is a detached `void`).
      expect(await settlesWithin(closing)).toBe(false);
      expect(outcome).toBeUndefined();
      gate.resolve();
      await closing;
      // Close settled only after the acquire did, and the acquire saw the close.
      expect(outcome).toBe("cancelled");
    } finally { gate.resolve(); }
    expect(spawned).toBe(0);
    expect(fs.readdirSync(meshRoot).filter((name) => name.startsWith(retention.MESH_RETENTION_SWEEP_PREFIX))).toEqual([]);
    expect(heldLockFds(meshRoot)).toEqual([]);
    // The lock is free: a bare non-blocking lock (no slot claim, no sweep) succeeds at once.
    const fd = await retention.lockMeshRetentionSweep(meshRoot);
    expect(typeof fd).toBe("number");
    if (typeof fd === "number") fs.closeSync(fd);
  });

  it("(d) close joins a pending scriptSpawnArgs: it waits for it, then no lock is attempted", async () => {
    const gate = deferred();
    let entered = false;
    let acquires = 0;
    control.impl = async () => { acquires += 1; return "busy"; };
    let sweepScript = "";
    control.spawnArgs = async (script) => {
      if (script !== sweepScript) return;
      control.spawnArgs = undefined; // only the first sweep tick is held
      entered = true;
      await gate.promise;
    };
    const { manager, meshRoot, script } = owner();
    // Synchronous after construction: the first sweep tick yields (setImmediate) before it reaches scriptSpawnArgs.
    sweepScript = script;
    try {
      await vi.waitFor(() => expect(entered).toBe(true), { timeout: 5_000 });
      const closing = manager.close();
      // RED before the fix: close does not wait for the detached sweep start.
      expect(await settlesWithin(closing)).toBe(false);
      gate.resolve();
      await closing;
    } finally { gate.resolve(); }
    expect(acquires).toBe(0);
    expect(heldLockFds(meshRoot)).toEqual([]);
  });
});
