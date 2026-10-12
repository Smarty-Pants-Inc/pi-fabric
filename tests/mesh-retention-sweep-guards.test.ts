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
const control = vi.hoisted(() => ({ impl: undefined as undefined | ((...args: unknown[]) => Promise<unknown>) }));
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
  return { meshRoot, manager, alarms };
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
});
