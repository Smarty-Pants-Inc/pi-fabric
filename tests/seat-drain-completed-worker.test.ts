import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, ExtensionEvent } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { registerFabricActorHostEventObservers } from "../src/actors/host-event-observer.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { sameActorSpec } from "../src/main-drain/actor-restore.js";
import { MainDrainControl } from "../src/main-drain/control.js";
import { procStartTime, procWorkerExited } from "../src/main-drain/binding.js";
import { MainDrainServer } from "../src/main-drain/server.js";
import { MeshStore } from "../src/mesh/store.js";
import { callMainDrain, mainDrainSocket } from "../src/main-drain/client.js";
import { createSeat, ROOT, SID, type Seat } from "./seat-drain-fixture.js";

const linux = process.platform === "linux" && typeof process.getuid === "function";
const generation = "a".repeat(32);
const liveModel = "test/live-model";
const liveModelReason = "saved live model override";

interface CompletedRun {
  id: string;
  worker: { pid: number; startTime: string };
  directory: string;
}

interface Harness {
  seat: Seat;
  runRoot: string;
  actorRoot: string;
  readonly agents: AgentManager;
  readonly actors: ActorManager;
  emitMainSettle(payload?: Record<string, unknown>): number;
  restart(options?: { newAgentManager?: boolean; newMain?: boolean; afterClose?: () => void }): Promise<void>;
  close(): Promise<void>;
}

const rewriteActorRow = (harness: Harness, id: string, update: (row: Record<string, unknown>) => Record<string, unknown>): void => {
  const file = path.join(harness.actorRoot, "actors.json");
  const registry = JSON.parse(fs.readFileSync(file, "utf8")) as { actors: Array<Record<string, unknown>> };
  let found = false;
  registry.actors = registry.actors.map(row => {
    if (row.id !== id) return row;
    found = true;
    return update(row);
  });
  if (!found) throw new Error(`Missing actor row ${id}`);
  fs.writeFileSync(file, JSON.stringify(registry));
};

const bind = (seat: Seat, phase: "drain" | "drain-undo" = "drain") => ({
  runDir: seat.runDir,
  run: seat.run,
  runSha256: seat.runSha256,
  phase,
  operationId: seat.operationId,
  generation,
  lockFd: seat.lockFd(),
  podUid: null,
});

const mainSocket = (seat: Seat) => mainDrainSocket(seat.meshRoot, SID, process.getuid!());

const harness = async (): Promise<Harness> => {
  const seat = await createSeat();
  const runRoot = path.join(seat.base, "runs");
  const actorRoot = path.join(seat.meshRoot, "actors", SID);
  const mesh = new MeshStore(seat.meshRoot, 256 * 1024, 100);
  const identity = { id: ROOT, name: "Main", kind: "main" as const, sessionId: SID };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const makeAgents = () => new AgentManager(seat.base, {
    ...DEFAULT_FABRIC_CONFIG.agents,
    model: "test/project-model",
    thinking: "low",
    transport: "process",
    retainRuns: true,
  }, {
    runRoot,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    preparePiModel: async model => model,
  });
  let agents = makeAgents();
  const makeActors = (manager: AgentManager) => new ActorManager(SID, identity, mesh, meshConfig, manager, () => {}, {
    actorRoot,
    persistent: true,
    rootId: ROOT,
    claimResidency: "session",
    reapDeadSessionPresence: false,
    resolvePiModel: model => model,
  });
  let actors = makeActors(agents);
  let replacementServer: MainDrainServer | undefined;
  let lastDispatch = 0;
  const handlers = new Map<string, (event: ExtensionEvent, context: ExtensionContext) => void>();
  const pi = {
    ...seat.native.pi,
    on: (event: string, handler: (event: ExtensionEvent, context: ExtensionContext) => void) => handlers.set(event, handler),
  } as unknown as ExtensionAPI;
  let incarnation = 1;
  seat.host.ownerIncarnation = () => `00000000-0000-4000-8000-${String(incarnation).padStart(12, "0")}`;
  seat.host.pi = pi;
  seat.host.actors = () => actors;
  seat.host.agents = () => agents;
  seat.host.completions = () => 0;
  seat.host.stopWaitMs = 60;
  registerFabricActorHostEventObservers(pi, (eventName, event, context) => {
    lastDispatch = actors.dispatchHostEvent(eventName, {
      event: eventName,
      session: { id: SID, cwd: seat.base },
      digest: {},
      transcript: [],
      signal: { payload: event, idle: context.isIdle(), observedAt: Date.now() },
    });
  });
  const result: Harness = {
    seat,
    runRoot,
    actorRoot,
    get agents() { return agents; },
    get actors() { return actors; },
    emitMainSettle(payload = {}) {
      const handler = handlers.get("agent_settled");
      if (!handler) throw new Error("Native agent_settled observer was not registered");
      lastDispatch = -1;
      handler(payload as unknown as ExtensionEvent, seat.native.ctx());
      return lastDispatch;
    },
    async restart(options = {}) {
      const newAgentManager = options.newAgentManager ?? true;
      const newMain = options.newMain ?? true;
      await actors.close();
      if (newAgentManager) await agents.close();
      options.afterClose?.();
      if (newMain) await seat.server.close();
      if (newAgentManager) agents = makeAgents();
      actors = makeActors(agents);
      seat.host.actors = () => actors;
      seat.host.agents = () => agents;
      if (newMain) {
        incarnation++;
        const server = new MainDrainServer(new MainDrainControl(seat.host));
        if (await server.start() !== "listening") throw new Error("Drain server did not restart");
        replacementServer = server;
        seat.server = server;
      }
    },
    async close() {
      await actors.close();
      await agents.close();
      await replacementServer?.close();
      await seat.close();
    },
  };
  return result;
};

const createSupervisor = async (harness: Harness): Promise<string> => {
  const actor = await harness.actors.create({
    name: "supervisor",
    instructions: "Observe this Main and report only when useful.",
    events: ["agent_settled"],
    responseMode: "directive",
    delivery: "steer",
    triggerTurn: true,
    residency: "session",
    runner: "pi",
    transport: "process",
    model: "test/project-model",
    modelReason: "project model for the test actor",
    thinking: "low",
    tools: [],
  });
  await harness.actors.setModel(actor.id, liveModel, "session", undefined, liveModelReason);
  await harness.actors.setThinking(actor.id, "high", "session");
  return actor.id;
};

const completeSupervisorRun = async (harness: Harness, actorId: string): Promise<CompletedRun> => {
  const release = path.join(harness.seat.base, "worker-release");
  const entered = `${release}.entered`;
  expect(harness.emitMainSettle({ fakeWorkerFinishGate: release })).toBe(1);
  await vi.waitFor(() => expect(fs.existsSync(entered)).toBe(true), { timeout: 10_000, interval: 20 });
  const run = harness.agents.list().find(candidate => candidate.actorName === "supervisor" && candidate.status === "running");
  expect(run).toBeDefined();
  if (!run) throw new Error("The native supervisor event did not launch a run");
  const worker = harness.agents.workerIdentity(run.id);
  expect(worker?.startTime).toBeTypeOf("string");
  if (!worker?.startTime) throw new Error("The worker process birth was unavailable");
  fs.writeFileSync(release, "release");
  await vi.waitFor(() => {
    const status = harness.actors.status(actorId);
    expect(status.status).toBe("idle");
    expect(status.lastRunId).toBe(run.id);
    expect(harness.agents.hasRunCustody(run.id)).toBe(false);
  }, { timeout: 15_000, interval: 20 });
  const directory = path.join(harness.runRoot, run.id);
  const record = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")) as { status: string };
  expect(record.status).toBe("completed");
  expect(harness.agents.workerIdentity(run.id)).toBeUndefined();
  expect(procWorkerExited(worker.pid, worker.startTime)).toBe(true);
  return { id: run.id, worker: { pid: worker.pid, startTime: worker.startTime }, directory };
};

const drainThenStop = async (harness: Harness) => {
  const socket = mainSocket(harness.seat);
  const drained = await callMainDrain(socket, SID, "drain", bind(harness.seat));
  expect(drained.native.inputsFenced).toBe(true);
  return callMainDrain(socket, SID, "stop-actors", bind(harness.seat));
};

const undo = (harness: Harness) => callMainDrain(mainSocket(harness.seat), SID, "undo", bind(harness.seat, "drain-undo"));

const fixtures: Harness[] = [];
afterEach(async () => {
  for (const value of fixtures.splice(0)) await value.close();
});

// Keep: recorded completed-run drain failure and data safety at the native worker/owner-socket boundary.
// The real managers, durable registry and local process worker run offline; no model service is contacted.
describe.skipIf(!linux)("completed session actor drain proof", () => {
  it("stops a completed supervisor after AgentManager has dropped its run handle", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    expect(fs.existsSync(completed.directory)).toBe(true);
    expect(h.agents.hasRunCustody(completed.id)).toBe(false);

    const stopped = await drainThenStop(h);
    expect(stopped.stopRefused).toBeNull();
    expect(stopped.stopped.map(row => row.id)).toContain(actorId);
    expect(h.actors.status(actorId).status).toBe("stopped");
  }, 30_000);

  it.each([false, true])("undo recreates a stopped supervisor exactly after a later actor stop is refused (restart=%s)", async restart => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const blocker = await h.actors.create({ name: "idle-blocker", instructions: "Remain idle.", residency: "session" });
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    if (restart) await h.restart({ newAgentManager: true, newMain: true });
    await callMainDrain(mainSocket(h.seat), SID, "drain", bind(h.seat));

    const originalStop = h.actors.stop.bind(h.actors);
    const stopSpy = vi.spyOn(h.actors, "stop").mockImplementation(async (...args: Parameters<ActorManager["stop"]>) => {
      if (args[0] === blocker.id) throw new Error("synthetic later stop refusal");
      return originalStop(...args);
    });
    let stopped: Awaited<ReturnType<typeof callMainDrain>>;
    try { stopped = await callMainDrain(mainSocket(h.seat), SID, "stop-actors", bind(h.seat)); }
    finally { stopSpy.mockRestore(); }
    const restoredReply = await undo(h);

    expect(stopped.stopRefused).toBe("ACTOR_STOP_FAILED");
    expect(stopped.stopped.map(row => row.id)).toContain(actorId);
    expect(restoredReply.undo?.actorsRestored).toBe(true);
    const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped");
    expect(restored).toBeDefined();
    if (!restored) return;
    expect(restored.id).not.toBe(actorId);
    expect(h.actors.status(restored.id)).toMatchObject({
      status: "idle",
      supervisorFor: ROOT,
      model: liveModel,
      modelReason: liveModelReason,
      thinking: "high",
    });
    expect(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
    expect(fs.existsSync(completed.directory)).toBe(true);
  }, 30_000);

  it("uses the durable worker receipt after Main and both managers restart", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    await h.restart({ newAgentManager: true, newMain: true });

    expect(h.agents.hasRunCustody(completed.id)).toBe(false);
    expect(h.agents.workerIdentity(completed.id)).toBeUndefined();
    expect(fs.existsSync(completed.directory)).toBe(true);
    const registry = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    expect(registry.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toMatchObject({
      actorId, runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime,
    });
    expect(h.actors.drainWorker(actorId)).toMatchObject({
      runId: completed.id,
      pid: completed.worker.pid,
      startTime: completed.worker.startTime,
    });
    const stopped = await drainThenStop(h);
    expect(stopped.stopRefused).toBeNull();
    expect(stopped.stopped.map(row => row.id)).toContain(actorId);
    expect((await undo(h)).undo?.actorsRestored).toBe(true);
    const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped")!;
    expect(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
  }, 30_000);

  it.each([false, true])("refuses a last run with no matching durable terminal proof (unknownRun=%s)", async unknownRun => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    await h.restart({ newAgentManager: true, newMain: true, afterClose: () => rewriteActorRow(h, actorId, row => {
      const { lastCleanedRunWorker: _missing, ...withoutProof } = row;
      return unknownRun ? { ...withoutProof, lastRunId: "f".repeat(32) } : withoutProof;
    }) });

    expect(h.actors.drainWorker(actorId)).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(path.join(completed.directory, "status.json"), "utf8")).status).toBe("completed");
    const stopped = await drainThenStop(h);
    expect(stopped.stopRefused).toBe("ACTOR_STOP_UNPROVEN");
    expect(stopped.stopped.map(row => row.id)).not.toContain(actorId);
    const undone = await undo(h);
    expect(undone.undo?.actorsRestored).toBe(false);
    expect(h.actors.listOwned().filter(row => row.name === "supervisor" && row.status !== "stopped")).toHaveLength(0);
  }, 30_000);

  it("refuses a terminal proof whose recorded worker PID and birth are still live, without undo recreation", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    const startTime = procStartTime(process.pid);
    expect(startTime).toBeTypeOf("string");
    if (!startTime) throw new Error("The test process birth was unavailable");
    await h.restart({ newAgentManager: true, newMain: true, afterClose: () => rewriteActorRow(h, actorId, row => ({ ...row,
      lastCleanedRunWorker: { actorId, runId: completed.id, pid: process.pid, startTime },
    })) });

    expect(h.actors.drainWorker(actorId)).toEqual({ runId: completed.id, pid: process.pid, startTime });
    const stopped = await drainThenStop(h);
    expect(stopped.stopRefused).toBe("ACTOR_STOP_UNPROVEN");
    expect(stopped.stopped.map(row => row.id)).not.toContain(actorId);
    const undone = await undo(h);
    expect(undone.undo?.actorsRestored).toBe(false);
    const rows = h.actors.listOwned().filter(row => row.name === "supervisor");
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ id: actorId, status: "stopped" });

    // A reused PID with a different kernel birth is the counterexample: the recorded worker is gone.
    await h.restart({ newMain: false, afterClose: () => rewriteActorRow(h, actorId, row => ({ ...row,
      lastCleanedRunWorker: { actorId, runId: completed.id, pid: process.pid, startTime: String(BigInt(startTime) + 1n) },
    })) });
    expect((await undo(h)).undo?.actorsRestored).toBe(true);
    const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped")!;
    expect(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
  }, 30_000);

  it("drains and restores the supervisor after a native Main settle arrives during the fence", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const socket = mainSocket(h.seat);
    const drained = await callMainDrain(socket, SID, "drain", bind(h.seat));
    expect(drained.native.inputsFenced).toBe(true);

    const release = path.join(h.seat.base, "late-worker-release");
    const dispatched = h.emitMainSettle({ fakeWorkerFinishGate: release });
    expect(dispatched).toBe(1);
    // A settle can activate the supervisor during the fence; old completed-run proof must not mask its live worker.
    await vi.waitFor(() => expect(fs.existsSync(`${release}.entered`)).toBe(true), { timeout: 10_000, interval: 20 });
    const active = h.agents.list().find(row => row.actorName === "supervisor" && row.status === "running")!;
    expect(active.id).not.toBe(completed.id);
    const currentWorker = h.agents.workerIdentity(active.id)!;
    expect(h.actors.drainWorker(actorId)).toEqual({ runId: active.id, pid: currentWorker.pid, startTime: currentWorker.startTime });
    expect(h.actors.drainSpec(actorId).lastCleanedRunWorker).toBeUndefined();
    fs.writeFileSync(release, "release");
    await vi.waitFor(() => expect(h.actors.status(actorId).lastRunId).toBe(active.id), { timeout: 10_000, interval: 20 });
    await vi.waitFor(() => expect(h.actors.status(actorId).status).toBe("idle"), { timeout: 10_000, interval: 20 });
    await vi.waitFor(() => expect(h.agents.hasRunCustody(h.actors.status(actorId).lastRunId!)).toBe(false),
      { timeout: 10_000, interval: 20 });
    const saved = h.actors.drainSpec(actorId);
    const stopped = await callMainDrain(socket, SID, "stop-actors", bind(h.seat));
    expect(stopped.stopRefused).toBeNull();
    expect(stopped.stopped.map(row => row.id)).toContain(actorId);
    const undone = await undo(h);
    expect(undone.undo?.actorsRestored).toBe(true);
    const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped")!;
    expect(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
    expect(h.agents.hasRunCustody(completed.id)).toBe(false);
  }, 30_000);
});
