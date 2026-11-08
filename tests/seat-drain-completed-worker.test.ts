import fs from "node:fs";
import { spawn } from "node:child_process";
import os from "node:os";
import { createRequire } from "node:module";
import { testTempOwnerMarker, testTempProcessIdentity, testTempRunEnvironment } from "../scripts/test-temp.js";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext, ExtensionEvent } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { registerFabricActorHostEventObservers } from "../src/actors/host-event-observer.js";
import { AgentManager } from "../src/agents/manager.js";
import { readRunStopIntent } from "../src/agents/archive-custody.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { sameActorSpec } from "../src/main-drain/actor-restore.js";
import { MainDrainControl } from "../src/main-drain/control.js";
import { procStartTime, procWorkerExited } from "../src/main-drain/binding.js";
import { MainDrainServer } from "../src/main-drain/server.js";
import { MeshStore } from "../src/mesh/store.js";
import { callMainDrain, mainDrainSocket } from "../src/main-drain/client.js";
import { FABRIC_MAIN_DRAINING } from "../src/main-drain/gate.js";
import { ACTOR_ACTIVATION_HEADER, ENODEV_DELIVERED_FILE, ENODEV_UNRESOLVED_REASON } from "../src/agents/enodev-recovery.js";
import { createSeat, ROOT, SID, type Seat } from "./seat-drain-fixture.js";
import { fakeManagerHeapWorker } from "./fixtures/manager-heap-probe.js";

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
  restart(options?: { newAgentManager?: boolean; newMain?: boolean; newActorsPaused?: boolean; afterClose?: () => void }): Promise<void>;
  failSupervisorModelPreparation(): void;
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

const harness = async (options: { maxConcurrent?: number; agentTimeoutMs?: number; closeGraceMs?: number } = {}): Promise<Harness> => {
  const seat = await createSeat();
  const runRoot = path.join(seat.base, "runs");
  const actorRoot = path.join(seat.meshRoot, "actors", SID);
  const mesh = new MeshStore(seat.meshRoot, 256 * 1024, 100);
  const identity = { id: ROOT, name: "Main", kind: "main" as const, sessionId: SID };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  let rejectSupervisorModelPreparation = false;
  const makeAgents = () => new AgentManager(seat.base, {
    ...DEFAULT_FABRIC_CONFIG.agents,
    model: "test/project-model",
    thinking: "low",
    transport: "process",
    maxConcurrent: options.maxConcurrent ?? DEFAULT_FABRIC_CONFIG.agents.maxConcurrent,
    timeoutMs: options.agentTimeoutMs ?? DEFAULT_FABRIC_CONFIG.agents.timeoutMs,
    retainRuns: true,
  }, {
    mainAgentId: ROOT,
    runRoot,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    preparePiModel: async model => {
      if (rejectSupervisorModelPreparation && model === liveModel) throw new Error("synthetic generic supervisor model preparation failure");
      return model;
    },
  });
  let agents = makeAgents();
  const makeActors = (manager: AgentManager, releasePaused = false) => new ActorManager(SID, identity, mesh, meshConfig, manager, () => {}, {
    releasePaused,
    ...(options.closeGraceMs !== undefined ? { closeGraceMs: options.closeGraceMs } : {}),
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
      actors = makeActors(agents, options.newActorsPaused ?? false);
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
    failSupervisorModelPreparation() { rejectSupervisorModelPreparation = true; },
    async close() {
      await actors.close();
      await agents.close();
      await replacementServer?.close();
      await seat.close();
    },
  };
  return result;
};

const createSupervisor = async (harness: Harness, options: { batchWindowMs?: number } = {}): Promise<string> => {
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
    ...(options.batchWindowMs !== undefined ? { batchWindowMs: options.batchWindowMs } : {}),
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

const finishSupervisorTerminalRun = async (harness: Harness, actorId: string, outcome: "failed" | "timed_out", receiptSavePending = false): Promise<CompletedRun> => {
  const release = path.join(harness.seat.base, `terminal-${outcome}-release`);
  const payload = { fakeWorkerFinishGate: release, resultMarker: outcome === "failed" ? "FAIL_DIRECTIVE" : "HANG_WITH_PROGRESS" };
  const launches: CompletedRun[] = [];
  const invalidLaunches: string[] = [];
  const realRun = harness.agents.run.bind(harness.agents);
  const observer = vi.spyOn(harness.agents, "run").mockImplementation((...args) => {
    if (args[0].actorId === actorId) {
      const onLaunched = args[2];
      args[2] = handle => {
        // run's third callback is spawn's onLaunched, after real host registration.
        // FAIL_DIRECTIVE has toolCalls=0 and can legitimately launch an unstarted redrive.
        const worker = harness.agents.workerIdentity(handle.id);
        if (worker?.startTime && worker.open) launches.push({
          id: handle.id, worker: { pid: worker.pid, startTime: worker.startTime },
          directory: path.join(harness.runRoot, handle.id),
        });
        else invalidLaunches.push(handle.id);
        onLaunched?.(handle);
      };
    }
    return realRun(...args);
  });
  try {
    expect(harness.emitMainSettle(payload)).toBe(1);
    await vi.waitFor(() => {
      expect(fs.existsSync(`${release}.entered`)).toBe(true);
      expect(launches.length).toBeGreaterThan(0);
    }, { timeout: 10_000, interval: 20 });
    const first = launches[0]!;
    expect(harness.agents.status(first.id).status).toBe("running");
    expect(harness.agents.workerIdentity(first.id)).toMatchObject({ ...first.worker, open: true });
    fs.writeFileSync(release, "release");
    await vi.waitFor(() => {
      expect(invalidLaunches).toEqual([]);
      expect(launches.length).toBeGreaterThan(0);
      const last = launches[launches.length - 1]!;
      expect(harness.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0, lastRunId: last.id });
      expect(harness.actors.status(actorId).inFlightRun).toBeUndefined();
      if (receiptSavePending) expect(harness.actors.drainQuiescence(actorId)).toContain("pending-reset-or-archive");
      else expect(harness.actors.drainQuiescence(actorId)).toEqual([]);
      expect(harness.agents.status(last.id).status).toBe(outcome);
    }, { timeout: 15_000, interval: 20 });
    const last = launches[launches.length - 1]!;
    expect(harness.agents.workerIdentity(last.id)).toMatchObject({ ...last.worker, open: false });
    for (const launched of launches) expect(procWorkerExited(launched.worker.pid, launched.worker.startTime)).toBe(true);
    return last;
  } finally {
    observer.mockRestore();
  }
};

const registerRetainedHandlePressure = async (agents: AgentManager): Promise<void> => {
  // Only pressure workers use the existing fake transport; native supervisors stay real.
  const launch = vi.spyOn(ProcessTransport.prototype, "launch")
    .mockImplementation(request => fakeManagerHeapWorker(request));
  try {
    for (let index = 0; index < 1_000; index++) {
      const result = await agents.run({
        task: `retained handle pressure ${index}`, transport: "process", extensions: false,
      });
      expect(result.status).toBe("completed");
    }
    expect(launch).toHaveBeenCalledTimes(1_000);
  } finally {
    launch.mockRestore();
  }
};

const evictRetainedTerminalHandle = async (harness: Harness, terminal: CompletedRun): Promise<void> => {
  expect(harness.agents.hasRunCustody(terminal.id)).toBe(true);
  expect(harness.agents.workerIdentity(terminal.id)).toMatchObject({ ...terminal.worker, open: false });
  expect(procWorkerExited(terminal.worker.pid, terminal.worker.startTime)).toBe(true);
  const terminalRecord = fs.readFileSync(path.join(terminal.directory, "status.json"), "utf8");
  expect(procStartTime(2147483647)).toBeUndefined();
  expect(procWorkerExited(2147483647, "1")).toBe(true);

  await registerRetainedHandlePressure(harness.agents);

  // Prove actual retained-handle eviction, not UI truncation or a restored summary.
  expect(harness.agents.hasRunCustody(terminal.id)).toBe(false);
  expect(harness.agents.runDirectory(terminal.id)).toBeUndefined();
  expect(harness.agents.workerIdentity(terminal.id)).toBeUndefined();
  expect(() => harness.agents.status(terminal.id)).toThrow(/Unknown Fabric agent/);
  expect(harness.agents.list()).toHaveLength(1_000);
  expect(fs.readFileSync(path.join(terminal.directory, "status.json"), "utf8")).toBe(terminalRecord);
};

const treeSnapshot = (directory: string): string => {
  const entries: string[] = [];
  const visit = (current: string, relative = ""): void => {
    for (const name of fs.readdirSync(current).sort()) {
      const file = path.join(current, name);
      const entry = relative ? `${relative}/${name}` : name;
      const stat = fs.lstatSync(file);
      if (stat.isDirectory()) {
        entries.push(`d ${entry}`);
        visit(file, entry);
      } else if (stat.isSymbolicLink()) entries.push(`l ${entry} ${fs.readlinkSync(file)}`);
      else entries.push(`f ${entry} ${fs.readFileSync(file).toString("base64")}`);
    }
  };
  visit(directory);
  return entries.join("\n");
};

const drainThenStop = async (harness: Harness) => {
  const socket = mainSocket(harness.seat);
  const drained = await callMainDrain(socket, SID, "drain", bind(harness.seat));
  expect(drained.native.inputsFenced).toBe(true);
  return callMainDrain(socket, SID, "stop-actors", bind(harness.seat));
};

const undo = (harness: Harness) => callMainDrain(mainSocket(harness.seat), SID, "undo", bind(harness.seat, "drain-undo"));

const queueRestoredSupervisorActivation = async (harness: Harness, actorId: string): Promise<string> => {
  // A real batch receives its immutable activation ID before AgentManager.run. Pause only the
  // manager after the event enters ActorManager, so the persisted item is a genuine restore case.
  harness.agents.pauseForRelease();
  expect(harness.emitMainSettle()).toBe(1);
  const actorDirectory = path.join(harness.actorRoot, actorId);
  let item: Record<string, unknown> | undefined;
  await vi.waitFor(() => {
    item = fs.readdirSync(actorDirectory)
      .filter(name => /^queue-[a-f0-9]{16}\.json$/.test(name))
      .flatMap(name => (JSON.parse(fs.readFileSync(path.join(actorDirectory, name), "utf8")) as { items: Array<Record<string, unknown>> }).items)
      .find(row => row.source === "host:agent_settled" && typeof row.activationId === "string");
    expect(item?.activationId).toBeTypeOf("string");
    expect(item?.batchIds).toEqual([item?.id]);
  }, { timeout: 10_000, interval: 20 });
  if (typeof item?.activationId !== "string") throw new Error("The native supervisor batch was not checkpointed with its activation ID");
  harness.actors.pauseForRelease();
  return item.activationId;
};

type RecoveryScenario =
  | { kind: "durable"; status: "stopped" | "timed_out" | "failed" | "completed" }
  | { kind: "enodev"; outcome: "completed" | "replay" | "stopped" | "timed_out" };

const seedRecoveryReceipt = (harness: Harness, actorId: string, activationId: string, completed: CompletedRun,
  scenario: RecoveryScenario): { id: string; directory: string } => {
  const id = scenario.kind === "durable" ? "d".repeat(32) : "e".repeat(32);
  const directory = path.join(harness.runRoot, id);
  if (fs.existsSync(directory)) throw new Error(`Recovery fixture already exists: ${directory}`);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const status = scenario.kind === "durable" ? scenario.status
    : scenario.outcome === "completed" || scenario.outcome === "replay" ? "running" : "recovery-pending";
  const text = scenario.kind === "durable" && scenario.status === "completed" ? '{"action":"silent"}' : "";
  const record = {
    id, actorId, name: "supervisor-recovered-activation", task: "", status, runner: "pi", transport: "process",
    cwd: harness.seat.base, sessionId: String(completed.worker.pid), processStartTime: completed.worker.startTime,
    startedAt: Date.now() - 1_000, updatedAt: Date.now(), turns: 0, toolCalls: 0, text,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  };
  fs.writeFileSync(path.join(directory, "task.txt"), `${ACTOR_ACTIVATION_HEADER}${JSON.stringify({ activationId, batchIds: [activationId] })}\n` +
    `Fabric actor message from host:agent_settled:\n${JSON.stringify({ actorId, id: activationId }, null, 2)}\n`);
  fs.writeFileSync(path.join(directory, "status.json"), JSON.stringify(record));
  const reason = scenario.kind === "durable" || scenario.kind === "enodev" && scenario.outcome === "stopped" ? "explicit" : "deadline";
  if (scenario.kind === "durable" || scenario.outcome === "stopped" || scenario.outcome === "timed_out") {
    fs.writeFileSync(path.join(directory, "archive-pending.json"), JSON.stringify({ format: 1, awaitingResult: true, ownerPid: 1,
      stopIntent: { reason, requestedAt: Date.now() - 1, by: { kind: "main", id: ROOT }, path: "actors.stop", recoveryRecord: record },
    }));
  }
  if (scenario.kind === "enodev") {
    fs.writeFileSync(path.join(directory, "unresolved-worker.json"), JSON.stringify({
      reason: `${ENODEV_UNRESOLVED_REASON}: no such device, read`, markedAt: Date.now() - 1, runId: id,
    }));
    if (scenario.outcome === "completed" || scenario.outcome === "replay") {
      fs.writeFileSync(path.join(directory, "settlement.json"), JSON.stringify({
        version: 1, runId: id, outcome: "completed", text: '{"action":"silent"}',
      }));
    }
    if (scenario.outcome === "replay") {
      fs.writeFileSync(path.join(directory, ENODEV_DELIVERED_FILE), JSON.stringify({
        version: 1, runId: id, activationId, deliveredAt: Date.now() - 1,
      }));
    }
  }
  return { id, directory };
};

const resumeRecoveredActivationWhileFenced = async (harness: Harness, actorId: string, activationId: string,
  onRecoveryLaunched?: (id: string) => void) => {
  const socket = mainSocket(harness.seat);
  const drained = await callMainDrain(socket, SID, "drain", bind(harness.seat));
  expect(drained.native.inputsFenced).toBe(true);
  const spawned = vi.spyOn(harness.agents, "spawn");
  const realRun = harness.agents.run.bind(harness.agents);
  let request: Parameters<AgentManager["run"]>[0] | undefined;
  let result: Awaited<ReturnType<AgentManager["run"]>> | undefined;
  vi.spyOn(harness.agents, "run").mockImplementation(async (...args) => {
    request = args[0];
    const onLaunched = args[2];
    args[2] = handle => { onRecoveryLaunched?.(handle.id); onLaunched?.(handle); };
    result = await realRun(...args);
    return result;
  });
  harness.actors.resumeAfterRelease();
  await vi.waitFor(() => expect(result).toBeDefined(), { timeout: 15_000, interval: 20 });
  await vi.waitFor(() => {
    expect(harness.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
    expect(harness.actors.drainQuiescence(actorId)).toEqual([]);
  }, { timeout: 10_000, interval: 20 });
  expect(request).toMatchObject({ actorId, activationId });
  expect(spawned).not.toHaveBeenCalled();
  if (!result) throw new Error("The real AgentManager recovery returned no result");
  expect(harness.agents.hasRunCustody(result.id)).toBe(false);
  return { result, request, socket };
};

const expectPriorWorkerStopAndUndo = async (harness: Harness, actorId: string, completed: CompletedRun,
  saved: Record<string, unknown>, socket = mainSocket(harness.seat)): Promise<void> => {
  const identity = { runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime };
  const status = harness.actors.status(actorId);
  const registry = JSON.parse(fs.readFileSync(path.join(harness.actorRoot, "actors.json"), "utf8")) as {
    actors: Array<Record<string, unknown>>;
  };
  const row = registry.actors.find(candidate => candidate.id === actorId);
  expect.soft(status.lastRunId).toBe(completed.id);
  expect.soft(row?.lastRunId).toBe(completed.id);
  expect.soft(row?.lastCleanedRunWorker).toEqual({ actorId, ...identity });
  expect.soft(harness.actors.drainWorker(actorId)).toEqual(identity);

  const stopped = await callMainDrain(socket, SID, "stop-actors", bind(harness.seat));
  expect.soft(stopped.stopRefused).toBeNull();
  expect.soft(stopped.stopped.map(candidate => candidate.id)).toContain(actorId);
  expect.soft(harness.actors.status(actorId).status).toBe("stopped");
  const restoredReply = await undo(harness);
  expect.soft(restoredReply.undo?.actorsRestored).toBe(true);
  const restored = harness.actors.listOwned().find(candidate => candidate.name === "supervisor" && candidate.status !== "stopped");
  expect.soft(restored).toBeDefined();
  if (restored) {
    expect.soft(harness.actors.status(restored.id)).toMatchObject({
      status: "idle", supervisorFor: ROOT, model: liveModel, modelReason: liveModelReason, thinking: "high",
    });
    expect.soft(sameActorSpec(saved, harness.actors.drainSpec(restored.id))).toBe(true);
  }
};

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

  it("replaces a completed receipt with the genuinely launched next worker before drain", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const oldDirectory = treeSnapshot(completed.directory);
    expect(h.actors.drainWorker(actorId)).toEqual({
      runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime,
    });

    const release = path.join(h.seat.base, "next-worker-release");
    expect(h.emitMainSettle({ fakeWorkerFinishGate: release })).toBe(1);
    await vi.waitFor(() => expect(fs.existsSync(`${release}.entered`)).toBe(true), { timeout: 10_000, interval: 20 });
    const active = h.agents.list().find(row => row.actorName === "supervisor" && row.status === "running");
    expect(active).toBeDefined();
    if (!active) throw new Error("The native supervisor event did not launch the next run");
    expect(active.id).not.toBe(completed.id);
    const currentWorker = h.agents.workerIdentity(active.id);
    expect(currentWorker?.startTime).toBeTypeOf("string");
    if (!currentWorker?.startTime) throw new Error("The next worker process birth was unavailable");
    expect(currentWorker.open).toBe(true);
    expect(h.actors.drainWorker(actorId)).toEqual({ runId: active.id, pid: currentWorker.pid, startTime: currentWorker.startTime });
    await vi.waitFor(() => {
      const registry = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
        actors: Array<Record<string, unknown>>;
      };
      expect(registry.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toBeUndefined();
    }, { timeout: 10_000, interval: 20 });
    fs.writeFileSync(release, "release");
    await vi.waitFor(() => {
      const status = h.actors.status(actorId);
      expect(status.status).toBe("idle");
      expect(status.lastRunId).toBe(active.id);
      expect(h.agents.hasRunCustody(active.id)).toBe(false);
      expect(h.actors.drainWorker(actorId)).toEqual({ runId: active.id, pid: currentWorker.pid, startTime: currentWorker.startTime });
    }, { timeout: 15_000, interval: 20 });

    expect(treeSnapshot(completed.directory)).toBe(oldDirectory);
    const registry = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    expect(registry.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toEqual({
      actorId, runId: active.id, pid: currentWorker.pid, startTime: currentWorker.startTime,
    });
    expect(JSON.parse(fs.readFileSync(path.join(h.runRoot, active.id, "status.json"), "utf8")).status).toBe("completed");

    const saved = h.actors.drainSpec(actorId);
    const stopped = await drainThenStop(h);
    expect(stopped.stopRefused).toBeNull();
    expect(stopped.stopped.map(row => row.id)).toContain(actorId);
    const undone = await undo(h);
    expect(undone.undo?.actorsRestored).toBe(true);
    const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped")!;
    expect(restored).toMatchObject({ status: "idle", supervisorFor: ROOT, model: liveModel,
      modelReason: liveModelReason, thinking: "high" });
    expect(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
    expect(h.agents.hasRunCustody(completed.id)).toBe(false);
  }, 30_000);

  it("refuses a native settle during the fence without losing the completed receipt", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    const registryBefore = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    expect(registryBefore.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toEqual({
      actorId, runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime,
    });
    expect(h.actors.drainWorker(actorId)).toEqual({
      runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime,
    });
    const runSnapshot = treeSnapshot(h.runRoot);
    const priorRunIds = h.agents.list().map(row => row.id).sort();

    const socket = mainSocket(h.seat);
    const drained = await callMainDrain(socket, SID, "drain", bind(h.seat));
    expect(drained.native.inputsFenced).toBe(true);
    const release = path.join(h.seat.base, "refused-worker-release");
    expect(h.emitMainSettle({ fakeWorkerFinishGate: release })).toBe(1);
    await vi.waitFor(() => {
      const status = h.actors.status(actorId);
      expect(status.status).toBe("idle");
      expect(status.queued).toBe(0);
      expect(status.inFlightRun).toBeUndefined();
      expect(status.lastError).toContain(FABRIC_MAIN_DRAINING);
    }, { timeout: 15_000, interval: 20 });

    expect(h.actors.status(actorId).lastRunId).toBe(completed.id);
    expect(h.agents.list().map(row => row.id).sort()).toEqual(priorRunIds);
    expect(treeSnapshot(h.runRoot)).toBe(runSnapshot);
    expect(fs.existsSync(`${release}.entered`)).toBe(false);
    const workerAfter = h.actors.drainWorker(actorId);
    const registryAfter = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };

    const stopped = await callMainDrain(socket, SID, "stop-actors", bind(h.seat));
    expect(stopped.ledger.some(row => row.path === "agents.spawn")).toBe(true);
    expect(stopped.stopRefused).toBeNull();
    expect(workerAfter).toEqual({
      runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime,
    });
    expect(registryAfter.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toEqual({
      actorId, runId: completed.id, pid: completed.worker.pid, startTime: completed.worker.startTime,
    });
    expect(stopped.stopped.map(row => row.id)).toContain(actorId);
    expect(h.actors.status(actorId).status).toBe("stopped");
    const undone = await undo(h);
    expect(undone.undo?.actorsRestored).toBe(true);
    const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped")!;
    expect(restored).toMatchObject({ status: "idle", supervisorFor: ROOT, model: liveModel,
      modelReason: liveModelReason, thinking: "high" });
    expect(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
  }, 30_000);

  it.each(["stopped", "timed_out", "failed", "completed"] as const)(
    "preserves the cleaned supervisor worker through durable terminal-%s recovery while fenced", async status => {
      const h = await harness(); fixtures.push(h);
      const actorId = await createSupervisor(h, { batchWindowMs: 1 });
      const completed = await completeSupervisorRun(h, actorId);
      const saved = h.actors.drainSpec(actorId);
      const activationId = await queueRestoredSupervisorActivation(h, actorId);
      let seeded: { id: string; directory: string } | undefined;
      await h.restart({ newAgentManager: true, newMain: true, newActorsPaused: true,
        afterClose: () => { seeded = seedRecoveryReceipt(h, actorId, activationId, completed, { kind: "durable", status }); },
      });
      if (!seeded) throw new Error("The durable terminal recovery fixture was not seeded");

      const recovered = await resumeRecoveredActivationWhileFenced(h, actorId, activationId);
      expect(recovered.result).toMatchObject({ id: seeded.id, status });
      await expectPriorWorkerStopAndUndo(h, actorId, completed, saved, recovered.socket);
    }, 30_000);

  it.each(["completed", "replay", "stopped", "timed_out"] as const)(
    "preserves the cleaned supervisor worker through ENODEV-%s recovery while fenced", async outcome => {
      const h = await harness(); fixtures.push(h);
      const actorId = await createSupervisor(h, { batchWindowMs: 1 });
      const completed = await completeSupervisorRun(h, actorId);
      const saved = h.actors.drainSpec(actorId);
      const activationId = await queueRestoredSupervisorActivation(h, actorId);
      let seeded: { id: string; directory: string } | undefined;
      await h.restart({ newAgentManager: true, newMain: true, newActorsPaused: true,
        afterClose: () => { seeded = seedRecoveryReceipt(h, actorId, activationId, completed, { kind: "enodev", outcome }); },
      });
      if (!seeded) throw new Error("The ENODEV recovery fixture was not seeded");

      const recovered = await resumeRecoveredActivationWhileFenced(h, actorId, activationId);
      expect(recovered.result).toMatchObject({ id: seeded.id,
        status: outcome === "stopped" || outcome === "timed_out" ? outcome : "completed" });
      if (outcome === "completed" || outcome === "replay") expect(recovered.result).toMatchObject({ enodevRecovered: true });
      if (outcome === "replay") expect(recovered.result).toMatchObject({ enodevReplay: true });
      await expectPriorWorkerStopAndUndo(h, actorId, completed, saved, recovered.socket);
    }, 30_000);

  it("first activation: a confirmed-unlaunched generic queue refusal must not invent an execution", async () => {
    // Keep: first-activation stop/undo data safety at the real permit queue and native owner socket.
    const h = await harness({ maxConcurrent: 1 }); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const saved = h.actors.drainSpec(actorId);
    expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
    expect(h.actors.status(actorId).lastRunId).toBeUndefined();
    expect(h.actors.status(actorId).inFlightRun).toBeUndefined();
    expect(h.actors.drainWorker(actorId)).toBeNull();
    expect(saved.lastCleanedRunWorker).toBeUndefined();
    expect(h.agents.list()).toEqual([]);
    const registryRow = () => {
      const registry = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
        actors: Array<Record<string, unknown>>;
      };
      return registry.actors.find(row => row.id === actorId);
    };
    expect(registryRow()).toBeDefined();
    expect(registryRow()?.lastRunId).toBeUndefined();
    expect(registryRow()?.lastCleanedRunWorker).toBeUndefined();

    const blockerRelease = path.join(h.seat.base, "first-refusal-blocker-release");
    const supervisorRelease = path.join(h.seat.base, "first-unlaunched-supervisor-release");
    const realRun = h.agents.run.bind(h.agents);
    const results: Awaited<ReturnType<AgentManager["run"]>>[] = [];
    const launched: string[] = [];
    const observer = vi.spyOn(h.agents, "run").mockImplementation(async (...args) => {
      if (args[0].actorId !== actorId) return realRun(...args);
      const onLaunched = args[2];
      args[2] = handle => { launched.push(handle.id); onLaunched?.(handle); };
      const result = await realRun(...args);
      results.push(result);
      return result;
    });
    const blocker = h.agents.run({ task: JSON.stringify({ fakeWorkerFinishGate: blockerRelease }), name: "first-refusal-blocker",
      runner: "pi", transport: "process", model: "test/project-model" });
    void blocker.catch(() => undefined);
    try {
      await vi.waitFor(() => expect(fs.existsSync(`${blockerRelease}.entered`)).toBe(true), { timeout: 10_000, interval: 20 });
      const blockerRun = h.agents.list().find(row => row.name === "first-refusal-blocker" && row.status === "running");
      if (!blockerRun) throw new Error("The first-activation admission blocker did not launch");
      const blockerWorker = h.agents.workerIdentity(blockerRun.id);
      expect(blockerWorker).toMatchObject({ open: true });
      if (!blockerWorker?.startTime) throw new Error("The admission blocker's native process birth was unavailable");
      expect(procWorkerExited(blockerWorker.pid, blockerWorker.startTime)).toBe(false);

      expect(h.emitMainSettle({ fakeWorkerFinishGate: supervisorRelease })).toBe(1);
      await vi.waitFor(() => expect(h.actors.status(actorId)).toMatchObject({
        status: "waiting", preparing: { phase: "waiting" },
      }), { timeout: 10_000, interval: 20 });
      const queuedId = h.actors.status(actorId).preparing?.runId;
      expect(queuedId).toBeTypeOf("string");
      if (!queuedId) throw new Error("The first native supervisor activation did not enter the real permit queue");
      expect(h.actors.status(actorId).lastRunId).toBeUndefined();
      expect(launched).toEqual([]);
      expect(h.agents.workerIdentity(queuedId)).toBeUndefined();

      const socket = mainSocket(h.seat);
      const beginning = await callMainDrain(socket, SID, "drain", bind(h.seat));
      expect(beginning.inbox.closed).toBe(true);
      expect(beginning.native.inputsFenced).toBe(false);
      h.failSupervisorModelPreparation();
      fs.writeFileSync(blockerRelease, "release");
      expect((await blocker).status).toBe("completed");
      expect(procWorkerExited(blockerWorker.pid, blockerWorker.startTime)).toBe(true);
      await vi.waitFor(() => {
        expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
        expect(h.actors.status(actorId).inFlightRun).toBeUndefined();
        expect(h.actors.drainQuiescence(actorId)).toEqual([]);
        expect(results).toHaveLength(1);
      }, { timeout: 15_000, interval: 20 });
      expect(results[0]).toMatchObject({ id: queuedId, status: "failed" });
      expect(results[0]?.error).toContain("synthetic generic supervisor model preparation failure");
      expect(results[0]?.launchPreparationTimeoutMs).toBeUndefined();
      expect(results[0]?.launchDenial).toBeUndefined();
      expect(launched).toEqual([]);
      expect(h.agents.workerIdentity(queuedId)).toBeUndefined();
      expect(h.agents.list().filter(row => row.actorId === actorId && row.status === "running")).toEqual([]);
      expect(fs.existsSync(`${supervisorRelease}.entered`)).toBe(false);
      expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);

      expect.soft(h.actors.status(actorId).lastRunId).toBeUndefined();
      expect.soft(registryRow()?.lastRunId).toBeUndefined();
      expect.soft(registryRow()?.lastCleanedRunWorker).toBeUndefined();
      expect.soft(h.actors.drainWorker(actorId)).toBeNull();
      const stopped = await callMainDrain(socket, SID, "stop-actors", bind(h.seat));
      expect.soft(stopped.stopRefused).toBeNull();
      expect.soft(stopped.stopped.map(row => row.id)).toContain(actorId);
      expect.soft(h.actors.status(actorId).status).toBe("stopped");
      const restoredReply = await undo(h);
      expect.soft(restoredReply.undo?.actorsRestored).toBe(true);
      const restored = h.actors.listOwned().find(row => row.name === "supervisor" && row.status !== "stopped");
      expect.soft(restored).toBeDefined();
      if (restored) {
        expect.soft(restored.id).not.toBe(actorId);
        expect.soft(h.actors.status(restored.id)).toMatchObject({
          status: "idle", supervisorFor: ROOT, model: liveModel, modelReason: liveModelReason, thinking: "high",
        });
        expect.soft(sameActorSpec(saved, h.actors.drainSpec(restored.id))).toBe(true);
      }
    } finally {
      observer.mockRestore();
      fs.writeFileSync(blockerRelease, "release");
      fs.writeFileSync(supervisorRelease, "release");
      await blocker.catch(() => undefined);
    }
  }, 45_000);

  it("preserves marker-only R1 proof through an unlaunched queued generic refusal and repairs its durable receipt", async () => {
    // Keep: F-A data safety/liveness at the real durable receipt, permit queue and owner socket.
    const h = await harness({ maxConcurrent: 1, agentTimeoutMs: 2_000 }); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const saved = h.actors.drainSpec(actorId);
    const registryRow = () => (JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    }).actors.find(row => row.id === actorId);
    let nativeRunId: string | undefined;
    let holdReceiptSave = true;
    let vetoedWrites = 0;
    const launched: string[] = [];
    const results: Awaited<ReturnType<AgentManager["run"]>>[] = [];
    let observer: { mockRestore(): void } | undefined;
    const realLaunch = ProcessTransport.prototype.launch;
    const launchObserver = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const worker = await realLaunch.call(this, request);
      nativeRunId ??= request.id;
      launched.push(request.id);
      return worker;
    });
    const realWrite = fs.writeFileSync;
    const receiptFault = vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      // Atomic registry writes target temporary paths: inspect the serialized actors array.
      // Only the genuine R1 receipt write fails; worker/status/proof I/O stays real.
      if (holdReceiptSave && nativeRunId && (typeof data === "string" || Buffer.isBuffer(data))) {
        let value: { actors?: Array<{ id?: string; lastCleanedRunWorker?: { runId?: string } }> } | undefined;
        try { value = JSON.parse(data.toString()); } catch { /* non-JSON worker/task data */ }
        if (Array.isArray(value?.actors) && value.actors.some(row =>
          row.id === actorId && row.lastCleanedRunWorker?.runId === nativeRunId)) {
          vetoedWrites++;
          throw Object.assign(new Error("synthetic R1 durable receipt save EIO"), { code: "EIO" });
        }
      }
      return realWrite(file, data, options);
    });
    const blockerRelease = path.join(h.seat.base, "marker-only-blocker-release");
    const supervisorRelease = path.join(h.seat.base, "marker-only-unlaunched-release");
    let blocker: ReturnType<AgentManager["run"]> | undefined;
    try {
      const terminal = await finishSupervisorTerminalRun(h, actorId, "timed_out", true);
      launchObserver.mockRestore();
      const realRun = h.agents.run.bind(h.agents);
      observer = vi.spyOn(h.agents, "run").mockImplementation(async (...args) => {
        if (args[0].actorId !== actorId) return realRun(...args);
        const onLaunched = args[2];
        args[2] = handle => { launched.push(handle.id); onLaunched?.(handle); };
        const result = await realRun(...args);
        results.push(result);
        return result;
      });
      expect(terminal.id).toBe(nativeRunId);
      expect(launched).toEqual([terminal.id]);
      expect(vetoedWrites).toBeGreaterThan(0);
      const markerFile = path.join(terminal.directory, "actor-run-archive-pending.json");
      const marker = () => JSON.parse(fs.readFileSync(markerFile, "utf8"));
      expect(marker()).toMatchObject({ runId: terminal.id, actorId,
        nativeExit: { ...terminal.worker, status: "timed_out" } });
      expect(h.actors.status(actorId).lastRunId).toBe(terminal.id);
      expect(registryRow()?.lastRunId).toBe(terminal.id);
      expect(registryRow()?.lastCleanedRunWorker).toBeUndefined();
      expect(h.actors.drainSpec(actorId).lastCleanedRunWorker).toBeUndefined();

      blocker = h.agents.run({ task: JSON.stringify({ fakeWorkerFinishGate: blockerRelease }), name: "marker-only-blocker",
        runner: "pi", transport: "process", model: "test/project-model", timeoutMs: 30_000 });
      void blocker.catch(() => undefined);
      await vi.waitFor(() => expect(fs.existsSync(`${blockerRelease}.entered`)).toBe(true), { timeout: 10_000, interval: 20 });
      expect(h.emitMainSettle({ fakeWorkerFinishGate: supervisorRelease })).toBe(1);
      await vi.waitFor(() => expect(h.actors.status(actorId)).toMatchObject({
        status: "waiting", preparing: { phase: "waiting" },
      }), { timeout: 10_000, interval: 20 });
      const queuedId = h.actors.status(actorId).preparing?.runId;
      if (!queuedId) throw new Error("The marker-only supervisor activation did not enter the real permit queue");
      expect(queuedId).not.toBe(terminal.id);
      expect(h.agents.workerIdentity(queuedId)).toBeUndefined();
      const socket = mainSocket(h.seat);
      expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(false);
      h.failSupervisorModelPreparation();
      fs.writeFileSync(blockerRelease, "release");
      expect((await blocker).status).toBe("completed");
      await vi.waitFor(() => {
        expect(results.find(result => result.id === queuedId)).toMatchObject({ id: queuedId, status: "failed" });
        expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
        expect(h.actors.status(actorId).inFlightRun).toBeUndefined();
      }, { timeout: 15_000, interval: 20 });
      const refused = results.find(result => result.id === queuedId)!;
      expect(refused.error).toContain("synthetic generic supervisor model preparation failure");
      expect(refused.launchPreparationTimeoutMs).toBeUndefined();
      expect(refused.launchDenial).toBeUndefined();
      expect(launched).toEqual([terminal.id]);
      expect(h.agents.workerIdentity(queuedId)).toBeUndefined();
      expect(fs.existsSync(`${supervisorRelease}.entered`)).toBe(false);
      // d6 RED: unlaunched R2 rekeys both copies and strands R1's marker-held proof.
      expect.soft(h.actors.status(actorId).lastRunId).toBe(terminal.id);
      expect.soft(registryRow()?.lastRunId).toBe(terminal.id);
      expect.soft(registryRow()?.lastCleanedRunWorker).toBeUndefined();
      expect.soft(h.actors.drainSpec(actorId).lastCleanedRunWorker).toBeUndefined();
      expect(marker()).toMatchObject({ runId: terminal.id, actorId,
        nativeExit: { ...terminal.worker, status: "timed_out" } });
      expect(h.actors.drainQuiescence(actorId)).toContain("pending-reset-or-archive");

      holdReceiptSave = false; // Existing mesh wake / idle maintenance must repair R1.
      await vi.waitFor(() => {
        expect(registryRow()?.lastCleanedRunWorker).toEqual({ actorId, runId: terminal.id, ...terminal.worker });
        expect(h.actors.drainQuiescence(actorId)).toEqual([]);
        expect(fs.existsSync(markerFile)).toBe(false);
      }, { timeout: 20_000, interval: 20 });
      expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);
      await expectPriorWorkerStopAndUndo(h, actorId, terminal, saved, socket);
    } finally {
      holdReceiptSave = false;
      receiptFault.mockRestore();
      observer?.mockRestore();
      launchObserver.mockRestore();
      fs.writeFileSync(blockerRelease, "release");
      fs.writeFileSync(supervisorRelease, "release");
      fs.writeFileSync(path.join(h.seat.base, "terminal-timed_out-release"), "release");
      await blocker?.catch(() => undefined);
    }
  }, 75_000);

  it("preserves a cleaned supervisor worker when a pre-begin queued generic refusal settles after begin", async () => {
    const h = await harness({ maxConcurrent: 1 }); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    const blockerRelease = path.join(h.seat.base, "admission-blocker-release");
    const blocker = h.agents.run({ task: JSON.stringify({ fakeWorkerFinishGate: blockerRelease }), name: "admission-blocker",
      runner: "pi", transport: "process", model: "test/project-model" });
    void blocker.catch(() => undefined);
    await vi.waitFor(() => expect(fs.existsSync(`${blockerRelease}.entered`)).toBe(true), { timeout: 10_000, interval: 20 });

    const supervisorRelease = path.join(h.seat.base, "queued-supervisor-release");
    const realRun = h.agents.run.bind(h.agents);
    let queuedRequest: Parameters<AgentManager["run"]>[0] | undefined;
    let queuedResult: Awaited<ReturnType<AgentManager["run"]>> | undefined;
    vi.spyOn(h.agents, "run").mockImplementation(async (...args) => {
      if (args[0].actorId === actorId) {
        queuedRequest = args[0];
        queuedResult = await realRun(...args);
        return queuedResult;
      }
      return realRun(...args);
    });
    expect(h.emitMainSettle({ fakeWorkerFinishGate: supervisorRelease })).toBe(1);
    await vi.waitFor(() => expect(h.actors.status(actorId)).toMatchObject({
      status: "waiting", preparing: { phase: "waiting" },
    }), { timeout: 10_000, interval: 20 });
    const waitingRunId = h.actors.status(actorId).preparing?.runId;
    expect(waitingRunId).toBeTypeOf("string");
    if (!waitingRunId) throw new Error("The supervisor activation did not enter AgentManager's permit queue");

    const socket = mainSocket(h.seat);
    const beginning = await callMainDrain(socket, SID, "drain", bind(h.seat));
    expect(beginning.inbox.closed).toBe(true);
    expect(beginning.native.inputsFenced).toBe(false); // visible blocker and queued admission prevent the native fence
    await expect(h.agents.spawn({ task: "fresh post-begin admission must be refused" })).rejects.toThrow(FABRIC_MAIN_DRAINING);
    h.failSupervisorModelPreparation();
    fs.writeFileSync(blockerRelease, "release");
    await blocker;
    await vi.waitFor(() => expect(queuedResult).toBeDefined(), { timeout: 15_000, interval: 20 });
    expect(queuedRequest).toMatchObject({ actorId });
    expect(queuedResult).toMatchObject({ status: "failed" });
    expect(queuedResult?.error).toContain("synthetic generic supervisor model preparation failure");
    expect(queuedResult?.launchPreparationTimeoutMs).toBeUndefined();
    expect(queuedResult?.launchDenial).toBeUndefined();
    expect(h.agents.workerIdentity(waitingRunId)).toBeUndefined();
    expect(fs.existsSync(`${supervisorRelease}.entered`)).toBe(false);
    await vi.waitFor(() => {
      expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
      expect(h.actors.drainQuiescence(actorId)).toEqual([]);
    }, { timeout: 10_000, interval: 20 });

    const fenced = await callMainDrain(socket, SID, "drain", bind(h.seat));
    expect(fenced.native.inputsFenced).toBe(true);
    await expectPriorWorkerStopAndUndo(h, actorId, completed, saved, socket);
  }, 30_000);

  it.each(["failed", "timed_out"] as const)(
    "retains the last real %s supervisor worker after a fenced refused settle", async outcome => {
      const h = await harness(outcome === "timed_out" ? { agentTimeoutMs: 1_000 } : {}); fixtures.push(h);
      const actorId = await createSupervisor(h);
      await completeSupervisorRun(h, actorId); // the next actual launch supersedes its completed receipt
      const saved = h.actors.drainSpec(actorId);
      const terminal = await finishSupervisorTerminalRun(h, actorId, outcome);
      const identity = { runId: terminal.id, pid: terminal.worker.pid, startTime: terminal.worker.startTime };
      expect(h.actors.status(actorId).lastRunId).toBe(terminal.id);
      expect(h.actors.drainWorker(actorId)).toEqual(identity);
      const socket = mainSocket(h.seat);
      expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);

      expect(h.emitMainSettle()).toBe(1);
      await vi.waitFor(() => {
        expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0, lastRunId: terminal.id });
        expect(h.actors.status(actorId).lastError).toContain(FABRIC_MAIN_DRAINING);
        expect(h.actors.drainQuiescence(actorId)).toEqual([]);
      }, { timeout: 15_000, interval: 20 });
      await expectPriorWorkerStopAndUndo(h, actorId, terminal, saved, socket);
    }, 30_000);

  it.each(["failed", "timed_out"] as const)(
    "preserves the last native %s worker after actual retained-handle eviction and a Main/managers restart", async outcome => {
      const h = await harness(outcome === "timed_out" ? { agentTimeoutMs: 1_000 } : {}); fixtures.push(h);
      const actorId = await createSupervisor(h);
      await completeSupervisorRun(h, actorId);
      const saved = h.actors.drainSpec(actorId);
      const terminal = await finishSupervisorTerminalRun(h, actorId, outcome);
      await evictRetainedTerminalHandle(h, terminal);
      expect(h.actors.status(actorId).lastRunId).toBe(terminal.id);
      // Soft until after restart: missing F2 publication must not bypass real eviction/reload.
      expect.soft(h.actors.drainWorker(actorId)).toEqual({ runId: terminal.id, ...terminal.worker });
      await h.restart({ newAgentManager: true, newMain: true });
      expect(h.agents.hasRunCustody(terminal.id)).toBe(false);
      expect(h.agents.workerIdentity(terminal.id)).toBeUndefined();
      expect(fs.existsSync(path.join(terminal.directory, "status.json"))).toBe(true);
      const socket = mainSocket(h.seat);
      expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);
      await expectPriorWorkerStopAndUndo(h, actorId, terminal, saved, socket);
    }, 90_000);

  it.each(["failed", "timed_out"] as const)(
    "preserves the last native %s worker when halt cancels its next queued activation after begin", async outcome => {
      const h = await harness({ maxConcurrent: 1, ...(outcome === "timed_out" ? { agentTimeoutMs: 1_000 } : {}) }); fixtures.push(h);
      const actorId = await createSupervisor(h);
      await completeSupervisorRun(h, actorId);
      const saved = h.actors.drainSpec(actorId);
      const terminal = await finishSupervisorTerminalRun(h, actorId, outcome);
      const blockerRelease = path.join(h.seat.base, "cancel-blocker-release");
      const blocker = h.agents.run({ task: JSON.stringify({ fakeWorkerFinishGate: blockerRelease }), name: "cancel-blocker",
        runner: "pi", transport: "process", model: "test/project-model", timeoutMs: 30_000 });
      void blocker.catch(() => undefined);
      await vi.waitFor(() => expect(fs.existsSync(`${blockerRelease}.entered`)).toBe(true), { timeout: 10_000, interval: 20 });
      const blockerRun = h.agents.list().find(row => row.name === "cancel-blocker" && row.status === "running");
      if (!blockerRun) throw new Error("The native cancellation blocker did not launch");
      const blockerWorker = h.agents.workerIdentity(blockerRun.id);
      expect(blockerWorker).toMatchObject({ open: true });
      if (!blockerWorker?.startTime) throw new Error("The native cancellation blocker birth was unavailable");

      const supervisorRelease = path.join(h.seat.base, "cancelled-supervisor-release");
      const realRun = h.agents.run.bind(h.agents);
      let queuedResult: Awaited<ReturnType<AgentManager["run"]>> | undefined;
      const launched: string[] = [];
      const observer = vi.spyOn(h.agents, "run").mockImplementation(async (...args) => {
        if (args[0].actorId !== actorId) return realRun(...args);
        const onLaunched = args[2];
        args[2] = handle => { launched.push(handle.id); onLaunched?.(handle); };
        queuedResult = await realRun(...args);
        return queuedResult;
      });
      try {
        expect(h.emitMainSettle({ fakeWorkerFinishGate: supervisorRelease })).toBe(1);
        await vi.waitFor(() => expect(h.actors.status(actorId)).toMatchObject({
          status: "waiting", preparing: { phase: "waiting" },
        }), { timeout: 10_000, interval: 20 });
        const waitingRunId = h.actors.status(actorId).preparing?.runId;
        expect(waitingRunId).toBeTypeOf("string");
        if (!waitingRunId) throw new Error("The next supervisor activation did not enter the real permit queue");
        expect(h.agents.workerIdentity(waitingRunId)).toBeUndefined();
        const socket = mainSocket(h.seat);
        const beginning = await callMainDrain(socket, SID, "drain", bind(h.seat));
        expect(beginning.inbox.closed).toBe(true);
        expect(beginning.native.inputsFenced).toBe(false);
        expect(h.actors.haltAll().halted).toBe(1); // public ESC API: cancels work, not the supervisor
        await vi.waitFor(() => {
          expect(queuedResult).toMatchObject({ id: waitingRunId, status: "stopped" });
          expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
          expect(h.actors.drainQuiescence(actorId)).toEqual([]);
        }, { timeout: 15_000, interval: 20 });
        expect(launched).toEqual([]);
        expect(h.agents.workerIdentity(waitingRunId)).toBeUndefined();
        expect(fs.existsSync(`${supervisorRelease}.entered`)).toBe(false);
        expect(h.actors.listOwned().some(row => row.id === actorId && row.status !== "stopped")).toBe(true);
        expect(h.agents.hasRunCustody(terminal.id)).toBe(false); // shared finally archived the prior native handle
        expect(fs.existsSync(path.join(terminal.directory, "status.json"))).toBe(true);
        expect(h.agents.workerIdentity(blockerRun.id)).toMatchObject({ ...blockerWorker, open: true });
        fs.writeFileSync(blockerRelease, "release");
        expect((await blocker).status).toBe("completed");
        expect(procWorkerExited(blockerWorker.pid, blockerWorker.startTime)).toBe(true);
        expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);
        await expectPriorWorkerStopAndUndo(h, actorId, terminal, saved, socket);
      } finally {
        observer.mockRestore();
        fs.writeFileSync(blockerRelease, "release");
        await blocker.catch(() => undefined);
      }
    }, 60_000);

  it("uses the genuinely launched next worker when halt cancels a callerless activation before result-pointer assignment", async () => {
    const h = await harness(); fixtures.push(h);
    const actorId = await createSupervisor(h);
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    const release = path.join(h.seat.base, "launched-cancel-release");
    const realRun = h.agents.run.bind(h.agents);
    let launched: CompletedRun | undefined;
    let result: Awaited<ReturnType<AgentManager["run"]>> | undefined;
    const observer = vi.spyOn(h.agents, "run").mockImplementation(async (...args) => {
      if (args[0].actorId !== actorId) return realRun(...args);
      const onLaunched = args[2];
      args[2] = handle => {
        const worker = h.agents.workerIdentity(handle.id);
        if (worker?.startTime && worker.open) launched = {
          id: handle.id, worker: { pid: worker.pid, startTime: worker.startTime },
          directory: path.join(h.runRoot, handle.id),
        };
        onLaunched?.(handle);
      };
      result = await realRun(...args);
      return result;
    });
    try {
      expect(h.emitMainSettle({ fakeWorkerFinishGate: release, resultMarker: "HANG_WITH_PROGRESS" })).toBe(1);
      await vi.waitFor(() => {
        expect(fs.existsSync(`${release}.entered`)).toBe(true);
        expect(launched).toBeDefined();
      }, { timeout: 10_000, interval: 20 });
      if (!launched) throw new Error("The callerless activation did not capture a native launched worker");
      const native = launched;
      expect(native.id).not.toBe(completed.id);
      expect(h.agents.workerIdentity(native.id)).toMatchObject({ ...native.worker, open: true });
      expect(h.actors.status(actorId).inFlightRun?.id).toBe(native.id);
      expect(h.actors.drainWorker(actorId)).toEqual({ runId: native.id, ...native.worker }); // identity alone is not exit proof
      fs.writeFileSync(release, "release");
      await vi.waitFor(() => expect(h.agents.status(native.id)).toMatchObject({ toolCalls: 1 }), { timeout: 10_000, interval: 20 });
      expect(h.actors.haltAll().halted).toBe(1);
      // Progress survives caller abort by design. Join the real worker's explicit stop,
      // while ActorManager still follows its callerless-cancel continue path.
      await h.agents.stop(native.id);
      await vi.waitFor(() => {
        expect(result).toMatchObject({ id: native.id, status: "stopped", toolCalls: 1 });
        expect(h.actors.status(actorId)).toMatchObject({ status: "idle", queued: 0 });
        expect(h.actors.drainQuiescence(actorId)).toEqual([]);
        expect(procWorkerExited(native.worker.pid, native.worker.startTime)).toBe(true);
      }, { timeout: 15_000, interval: 20 });
      expect(h.agents.workerIdentity(native.id)).toMatchObject({ ...native.worker, open: false });
      expect(JSON.parse(fs.readFileSync(path.join(native.directory, "status.json"), "utf8")).status).toBe("stopped");
      expect(h.actors.listOwned().some(row => row.id === actorId && row.status !== "stopped")).toBe(true);
      expect(fs.existsSync(path.join(completed.directory, "status.json"))).toBe(true);
      const socket = mainSocket(h.seat);
      expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);
      await expectPriorWorkerStopAndUndo(h, actorId, native, saved, socket);
    } finally {
      observer.mockRestore();
      fs.writeFileSync(release, "release");
      if (launched && h.agents.hasRunCustody(launched.id)) await h.agents.stop(launched.id).catch(() => undefined);
    }
  }, 45_000);

  it("recovers the genuinely launched next activation after Main/managers restart without resurrecting the completed worker receipt", async () => {
    // Keep: native worker identity / durable stop / owner-socket failure boundary.
    // Hold only delivery of the REAL run result, not launch, status, identity or stop.
    const h = await harness({ closeGraceMs: 50 }); fixtures.push(h);
    const actorId = await createSupervisor(h, { batchWindowMs: 1 });
    const completed = await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    const completedBytes = treeSnapshot(completed.directory);
    const oldAgents = h.agents;
    const oldActors = h.actors;
    const release = path.join(h.seat.base, "restart-launched-worker-release");
    let releaseDelivery!: () => void;
    const deliveryGate = new Promise<void>(resolve => { releaseDelivery = resolve; });
    let launched: CompletedRun | undefined;
    let request: Parameters<AgentManager["run"]>[0] | undefined;
    let nativeResult: Awaited<ReturnType<AgentManager["run"]>> | undefined;
    let oldDelivery: Promise<Awaited<ReturnType<AgentManager["run"]>>> | undefined;
    let delivered = false;
    const realRun = oldAgents.run.bind(oldAgents);
    const observer = vi.spyOn(oldAgents, "run").mockImplementation((...args) => {
      if (args[0].actorId !== actorId) return realRun(...args);
      request = args[0];
      const onLaunched = args[2];
      args[2] = handle => {
        const worker = oldAgents.workerIdentity(handle.id);
        if (worker?.startTime && worker.open) launched = {
          id: handle.id, worker: { pid: worker.pid, startTime: worker.startTime },
          directory: path.join(h.runRoot, handle.id),
        };
        onLaunched?.(handle); // the real callback invalidates R0's durable publication
      };
      oldDelivery = (async () => {
        nativeResult = await realRun(...args);
        await deliveryGate;
        delivered = true;
        return nativeResult;
      })();
      return oldDelivery;
    });
    const queuedBatch = (): Array<Record<string, unknown>> => fs.readdirSync(path.join(h.actorRoot, actorId))
      .filter(name => /^queue-[a-f0-9]{16}\.json$/.test(name))
      .flatMap(name => (JSON.parse(fs.readFileSync(path.join(h.actorRoot, actorId, name), "utf8")) as {
        items: Array<Record<string, unknown>>;
      }).items);
    try {
      expect(h.actors.drainWorker(actorId)).toEqual({ runId: completed.id, ...completed.worker });
      expect(h.emitMainSettle({ fakeWorkerFinishGate: release, resultMarker: "HANG_WITH_PROGRESS" })).toBe(1);
      await vi.waitFor(() => {
        expect(launched).toBeDefined();
        expect(fs.existsSync(`${release}.entered`)).toBe(true);
      }, { timeout: 10_000, interval: 20 });
      if (!launched) throw new Error("R1 did not launch a real native worker");
      const native = launched;
      expect(native.id).not.toBe(completed.id);
      expect(oldAgents.workerIdentity(native.id)).toMatchObject({ ...native.worker, open: true });
      expect(procWorkerExited(native.worker.pid, native.worker.startTime)).toBe(false);
      expect(request?.activationId).toBeTypeOf("string");
      if (!request?.activationId) throw new Error("R1 has no genuine batch activation ID");
      const activationId = request.activationId;
      const pending = queuedBatch();
      expect(pending).toHaveLength(1);
      expect(pending[0]).toMatchObject({ activationId, batchIds: [activationId], executionStarted: true, launchEvidenceVersion: 1 });
      await vi.waitFor(() => {
        const registry = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
          actors: Array<Record<string, unknown>>;
        };
        expect(registry.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toBeUndefined();
      }, { timeout: 10_000, interval: 20 });
      fs.writeFileSync(release, "release");
      await vi.waitFor(() => expect(oldAgents.status(native.id)).toMatchObject({ toolCalls: 1 }), { timeout: 10_000, interval: 20 });
      // Real explicit stop writes intent before execution close, just as in agent-stop-recovery.
      expect(await oldAgents.stop(native.id)).toMatchObject({ id: native.id, status: "stopped", toolCalls: 1 });
      await vi.waitFor(() => expect(nativeResult).toMatchObject({ id: native.id, status: "stopped" }),
        { timeout: 10_000, interval: 20 });
      expect(delivered).toBe(false);
      await registerRetainedHandlePressure(oldAgents);
      expect(oldAgents.hasRunCustody(native.id)).toBe(true);
      expect(oldAgents.workerIdentity(native.id)).toMatchObject({ ...native.worker, open: false });
      expect(oldActors.status(actorId).inFlightRun?.id).toBe(native.id);
      expect(procWorkerExited(native.worker.pid, native.worker.startTime)).toBe(true);
      expect(JSON.parse(fs.readFileSync(path.join(native.directory, "status.json"), "utf8"))).toMatchObject({
        id: native.id, sessionId: String(native.worker.pid),
        status: "stopped", stopIntent: { reason: "explicit" },
      });
      // The worker-writable status need not publish kernel birth. The real host's
      // durable stop snapshot, captured before native stop, owns that identity.
      expect(readRunStopIntent(native.directory)?.recoveryRecord).toMatchObject({
        id: native.id, actorId, transport: "process",
        sessionId: String(native.worker.pid), processStartTime: native.worker.startTime,
      });
      expect(fs.readFileSync(path.join(native.directory, "task.txt"), "utf8"))
        .toContain(`${ACTOR_ACTIVATION_HEADER}${JSON.stringify({ activationId, batchIds: [activationId] })}`);

      await h.restart({ newAgentManager: true, newMain: true, newActorsPaused: true });
      expect(h.agents).not.toBe(oldAgents);
      expect(h.actors).not.toBe(oldActors);
      expect(delivered).toBe(false); // Main + both managers replaced before old ActorManager sees R1
      // Restore rearms this same real batch: attempts increments and executionStarted
      // resets, but its immutable activation, batch IDs, input and binding do not.
      const restoredPending = pending.map(item => ({ ...item, attempts: Number(item.attempts) + 1, executionStarted: false }));
      expect(queuedBatch()).toEqual(restoredPending);
      expect(h.agents.hasRunCustody(native.id)).toBe(false);
      expect(h.agents.workerIdentity(native.id)).toBeUndefined();
      const runsBefore = h.agents.list().map(row => row.id).sort();
      // The closed old incarnation may now unwind, but MUST be terminal before recovery judgment.
      releaseDelivery();
      expect(await oldDelivery).toMatchObject({ id: native.id, status: "stopped" });
      await vi.waitFor(() => expect(oldActors.drainQuiescence(actorId)).toEqual([]), { timeout: 5_000, interval: 20 });
      await oldAgents.close();
      expect(queuedBatch()).toEqual(restoredPending); // old closing continuation cannot ack the replacement's batch
      const recoveryLaunched: string[] = [];
      const recovered = await resumeRecoveredActivationWhileFenced(h, actorId, activationId,
        id => { recoveryLaunched.push(id); });
      expect(recovered.result).toMatchObject({ id: native.id, status: "stopped", toolCalls: 1,
        sessionId: String(native.worker.pid) });
      expect(recoveryLaunched).toEqual([]);
      expect(h.agents.list().map(row => row.id).sort()).toEqual(runsBefore);
      expect(h.agents.hasRunCustody(native.id)).toBe(false);
      expect(treeSnapshot(completed.directory)).toBe(completedBytes);
      // Expected RED until recovered native-owner proof is published: R1, never R0/no-proof.
      await expectPriorWorkerStopAndUndo(h, actorId, native, saved, recovered.socket);
    } finally {
      // HANG_WITH_PROGRESS intentionally survives abort. Stop + join before fixture actors.close.
      oldActors.pauseForRelease();
      fs.writeFileSync(release, "release");
      try {
        if (launched && oldAgents.hasRunCustody(launched.id)) await oldAgents.stop(launched.id);
      } finally {
        releaseDelivery();
        try {
          // Also joins any real launch that failed before its onLaunched observer ran.
          await oldAgents.close();
          await oldDelivery;
          await vi.waitFor(() => expect(oldActors.drainQuiescence(actorId)).toEqual([]), { timeout: 5_000, interval: 20 });
          if (launched) expect(procWorkerExited(launched.worker.pid, launched.worker.startTime)).toBe(true);
        } finally { observer.mockRestore(); }
      }
    }
  }, 60_000);

  it("preserves unproven R1 custody after owning Main hard-exits before native stop/join", async () => {
    // Keep: the auditor's real stop-intent / native-custody / owner-socket data-safety failure.
    // One test, selected recursively in a disposable fork; SIGKILL bypasses Vitest's process.exit shim.
    const checkpointEnvironment = "FABRIC_TEST_HARD_EXIT_CHECKPOINT";
    const checkpointFile = process.env[checkpointEnvironment];
    type Checkpoint = {
      base: string; meshRoot: string; runRoot: string; actorRoot: string; actorId: string;
      completed: CompletedRun; native: CompletedRun; release: string; activationId: string;
      saved: Record<string, unknown>; completedBytes: string; main: { pid: number; startTime: string };
    };
    if (checkpointFile) {
      const h = await harness();
      const owned: CompletedRun[] = [];
      const ownershipFile = path.join(path.dirname(checkpointFile), "workers.json");
      let completed: CompletedRun | undefined;
      let activationId: string | undefined;
      let crashArmed = false;
      fs.writeFileSync(ownershipFile, JSON.stringify({ base: h.seat.base, owned }));
      const realLaunch = ProcessTransport.prototype.launch;
      const launches = vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
        const handle = await realLaunch.call(this, request);
        const pid = Number(handle.sessionId);
        const startTime = procStartTime(pid);
        if (!startTime) throw new Error("The hard-exit fixture's native worker birth was unavailable");
        const native = { id: request.id, worker: { pid, startTime }, directory: path.join(h.runRoot, request.id) };
        owned.push(native);
        fs.writeFileSync(ownershipFile, JSON.stringify({ base: h.seat.base, owned }));
        const prior = completed;
        if (prior) {
          // Only interpose at the exact stop boundary. Launch, callback, durable intent and worker are real.
          const realStop = handle.stop.bind(handle);
          handle.stop = async () => {
            if (!crashArmed) return realStop();
            expect(readRunStopIntent(native.directory)).toMatchObject({ reason: "explicit", by: { kind: "main", id: ROOT },
              path: "agents.stop", recoveryRecord: { id: native.id, actorId, sessionId: String(pid), processStartTime: startTime } });
            const marker = JSON.parse(fs.readFileSync(path.join(native.directory, "actor-run-archive-pending.json"), "utf8"));
            expect(marker.nativeExit).toBeUndefined();
            expect(procWorkerExited(pid, startTime)).toBe(false);
            const mainBirth = procStartTime(process.pid);
            if (!mainBirth || !activationId) throw new Error("The hard-exit checkpoint lacks native Main/batch identity");
            fs.writeFileSync(checkpointFile, JSON.stringify({ base: h.seat.base, meshRoot: h.seat.meshRoot,
              runRoot: h.runRoot, actorRoot: h.actorRoot, actorId, completed: prior, native, release, activationId,
              saved, completedBytes, main: { pid: process.pid, startTime: mainBirth } } satisfies Checkpoint));
            // Preserve the raw pre-recovery status/stop-intent/custody and actor session/queue/registry bytes.
            fs.cpSync(native.directory, path.join(path.dirname(checkpointFile), "before-hard-exit-run"), { recursive: true });
            fs.cpSync(h.actorRoot, path.join(path.dirname(checkpointFile), "before-hard-exit-actors"), { recursive: true });
            process.kill(process.pid, "SIGKILL");
            await new Promise<void>(() => {}); // No native stop, join, manager close or finally can run.
          };
        }
        return handle;
      });
      let actorId = "";
      let saved: Record<string, unknown> = {};
      let completedBytes = "";
      const release = path.join(h.seat.base, "hard-exit-R1-release");
      const realRun = h.agents.run.bind(h.agents);
      const runs = vi.spyOn(h.agents, "run").mockImplementation((...args) => {
        if (completed && args[0].actorId === actorId) activationId = args[0].activationId;
        return realRun(...args);
      });
      try {
        actorId = await createSupervisor(h, { batchWindowMs: 1 });
        completed = await completeSupervisorRun(h, actorId);
        saved = h.actors.drainSpec(actorId);
        completedBytes = treeSnapshot(completed.directory);
        expect(h.actors.drainWorker(actorId)).toEqual({ runId: completed.id, ...completed.worker });
        expect(saved.lastCleanedRunWorker).toEqual({ actorId, runId: completed.id, ...completed.worker });
        expect(h.emitMainSettle({ fakeWorkerFinishGate: release })).toBe(1);
        await vi.waitFor(() => {
          expect(owned).toHaveLength(2);
          expect(fs.existsSync(`${release}.entered`)).toBe(true);
          expect(h.actors.status(actorId).lastRunId).toBe(owned[1]!.id);
          const registry = JSON.parse(fs.readFileSync(path.join(h.actorRoot, "actors.json"), "utf8")) as {
            actors: Array<Record<string, unknown>>;
          };
          expect(registry.actors.find(row => row.id === actorId)?.lastCleanedRunWorker).toBeUndefined();
        }, { timeout: 10_000, interval: 20 });
        expect(owned[1]!.id).not.toBe(completed.id);
        crashArmed = true;
        await h.agents.stop(owned[1]!.id); // Real durable stop-intent persistence precedes the overridden transport hook.
        throw new Error("The owning Main did not hard-exit at its native stop boundary");
      } finally {
        crashArmed = false;
        runs.mockRestore();
        launches.mockRestore();
        fs.writeFileSync(release, "release");
        await h.close(); // Only the non-crashing error path reaches this cleanup.
      }
    }

    // The CHILD must not own this root: config/global teardown otherwise deletes its fixture on fork death.
    // A parent-owned launcher marker makes isolatedTestTemp reuse it without registering child cleanup.
    const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-vitest-")));
    fs.chmodSync(directory, 0o700);
    const ownerIdentity = testTempProcessIdentity(process.pid);
    if (!ownerIdentity) throw new Error("The parent test's native temp ownership identity was unavailable");
    fs.writeFileSync(path.join(directory, testTempOwnerMarker), JSON.stringify({ version: 1, pid: process.pid,
      startIdentity: ownerIdentity }), { mode: 0o600 });
    const checkpointPath = path.join(directory, "checkpoint.json");
    const log = fs.openSync(path.join(directory, "child.log"), "w");
    const cli = path.join(path.dirname(createRequire(import.meta.url).resolve("vitest/package.json")), "vitest.mjs");
    const child = spawn(process.execPath, [cli, "run", "tests/seat-drain-completed-worker.test.ts", "--project=default",
      "--pool=forks", "--maxWorkers=1", "--retry=0", "-t", "preserves unproven R1 custody after owning Main hard-exits before native stop/join"], {
      detached: true, stdio: ["ignore", log, log], env: { ...process.env, CI: "false", TMPDIR: directory, TMP: directory, TEMP: directory,
        [testTempRunEnvironment]: directory, [checkpointEnvironment]: checkpointPath },
    });
    const childExit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code, signal) => resolve({ code, signal }));
    });
    void childExit.catch(() => undefined);
    const childGroupAlive = (): boolean => {
      if (!child.pid) return false;
      for (const pid of fs.readdirSync("/proc").filter(name => /^\d+$/.test(name))) {
        try {
          const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
          const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
          if (Number(fields[2]) === child.pid && !["Z", "X", "x"].includes(fields[0]!)) return true;
        } catch (error) {
          if (!["ENOENT", "ESRCH"].includes((error as NodeJS.ErrnoException).code ?? "")) return true;
        }
      }
      return false;
    };
    const timeout = setTimeout(() => { if (child.pid) { try { process.kill(-child.pid, "SIGKILL"); } catch { /* already gone */ } } }, 45_000);
    let seat: Seat | undefined;
    let agents: AgentManager | undefined;
    let actors: ActorManager | undefined;
    console.info(`hard-exit regression raw traces: ${directory}`);
    try {
      const exited = await childExit;
      fs.writeFileSync(path.join(directory, "child-exit.json"), JSON.stringify(exited));
      expect(exited.code).not.toBe(0); // Vitest reports the SIGKILLed test fork as an unexpected worker exit.
      expect(fs.existsSync(checkpointPath), fs.readFileSync(path.join(directory, "child.log"), "utf8")).toBe(true);
      const cp = JSON.parse(fs.readFileSync(checkpointPath, "utf8")) as Checkpoint;
      expect(procWorkerExited(cp.main.pid, cp.main.startTime)).toBe(true);
      expect(cp.native.id).not.toBe(cp.completed.id);
      expect(procWorkerExited(cp.native.worker.pid, cp.native.worker.startTime)).toBe(false);
      expect(readRunStopIntent(cp.native.directory)?.recoveryRecord).toMatchObject({ id: cp.native.id, actorId: cp.actorId,
        sessionId: String(cp.native.worker.pid), processStartTime: cp.native.worker.startTime });
      expect(JSON.parse(fs.readFileSync(path.join(cp.native.directory, "actor-run-archive-pending.json"), "utf8")).nativeExit).toBeUndefined();
      fs.writeFileSync(cp.release, "release"); // The real R1 worker finishes naturally; no replacement owner stops it.
      await vi.waitFor(() => expect(procWorkerExited(cp.native.worker.pid, cp.native.worker.startTime)).toBe(true),
        { timeout: 15_000, interval: 20 });
      expect(JSON.parse(fs.readFileSync(path.join(cp.native.directory, "status.json"), "utf8")).status).toBe("completed");
      expect(treeSnapshot(cp.completed.directory)).toBe(cp.completedBytes);

      agents = new AgentManager(cp.base, { ...DEFAULT_FABRIC_CONFIG.agents, model: "test/project-model", thinking: "low",
        transport: "process", retainRuns: true }, { mainAgentId: ROOT, runRoot: cp.runRoot,
        workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), preparePiModel: async model => model });
      actors = new ActorManager(SID, { id: ROOT, name: "Main", kind: "main", sessionId: SID },
        new MeshStore(cp.meshRoot, 256 * 1024, 100), { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
          releasePaused: true, actorRoot: cp.actorRoot, persistent: true, rootId: ROOT, claimResidency: "session",
          reapDeadSessionPresence: false, resolvePiModel: model => model,
        });
      const recoveredAgents = agents;
      const recoveredActors = actors;
      seat = await createSeat(); // New native owner socket; the managers restore the original durable roots, not a seeded run.
      seat.host.actors = () => recoveredActors;
      seat.host.agents = () => recoveredAgents;
      seat.host.completions = () => 0;
      seat.host.stopWaitMs = 60;
      const socket = mainSocket(seat);
      expect((await callMainDrain(socket, SID, "drain", bind(seat))).native.inputsFenced).toBe(true);
      const spawned = vi.spyOn(agents, "spawn");
      const nativeLaunches = vi.spyOn(ProcessTransport.prototype, "launch");
      const recoveryLaunched: string[] = [];
      const recovered: Awaited<ReturnType<AgentManager["run"]>>[] = [];
      const requests: Parameters<AgentManager["run"]>[0][] = [];
      const realRun = agents.run.bind(agents);
      vi.spyOn(agents, "run").mockImplementation(async (...args) => {
        requests.push(args[0]);
        const onLaunched = args[2];
        args[2] = handle => { recoveryLaunched.push(handle.id); onLaunched?.(handle); };
        const result = await realRun(...args);
        recovered.push(result);
        return result;
      });
      actors.resumeAfterRelease();
      await vi.waitFor(() => {
        expect(recovered).toHaveLength(1);
        expect(recoveredActors.status(cp.actorId)).toMatchObject({ status: "idle", queued: 0 });
        // Retained unproven source custody may legitimately remain pending.
      }, { timeout: 15_000, interval: 20 });
      expect(requests).toHaveLength(1);
      expect(requests[0]).toMatchObject({ actorId: cp.actorId, activationId: cp.activationId });
      expect(recovered[0]).toMatchObject({ id: cp.native.id, actorId: cp.actorId, status: "stopped",
        sessionId: String(cp.native.worker.pid),
        model: liveModel, modelReason: liveModelReason, thinking: "high", stopIntent: { reason: "explicit", path: "agents.stop" } });
      expect(spawned).not.toHaveBeenCalled();
      expect(nativeLaunches).not.toHaveBeenCalled();
      expect(recoveryLaunched).toEqual([]);
      expect(agents.hasRunCustody(cp.native.id)).toBe(false);
      expect(treeSnapshot(cp.completed.directory)).toBe(cp.completedBytes);
      const registryRow = () => (JSON.parse(fs.readFileSync(path.join(cp.actorRoot, "actors.json"), "utf8")) as {
        actors: Array<Record<string, unknown>>;
      }).actors.find(candidate => candidate.id === cp.actorId);
      const source = (name: string): Record<string, unknown> | undefined => {
        const file = path.join(cp.native.directory, name);
        return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : undefined;
      };
      const custody = () => ({ row: registryRow(), status: recoveredActors.status(cp.actorId),
        worker: recoveredActors.drainWorker(cp.actorId), marker: source("actor-run-archive-pending.json"),
        stopSource: source("archive-pending.json") });
      const assertUnprovenCustody = () => {
        const evidence = custody();
        expect.soft(evidence.status.lastRunId).toBe(cp.native.id);
        expect.soft(evidence.row?.lastRunId).toBe(cp.native.id);
        expect.soft(evidence.row?.lastCleanedRunWorker).toBeUndefined();
        expect.soft(recoveredActors.drainSpec(cp.actorId).lastCleanedRunWorker).toBeUndefined();
        expect.soft(evidence.worker).toBeUndefined();
        expect.soft(evidence.marker).toMatchObject({ format: 1, runId: cp.native.id, actorId: cp.actorId });
        expect.soft(evidence.marker?.nativeExit).toBeUndefined();
        expect.soft(evidence.stopSource).toMatchObject({ stopIntent: { reason: "explicit", path: "agents.stop",
          recoveryRecord: { id: cp.native.id, actorId: cp.actorId,
            sessionId: String(cp.native.worker.pid), processStartTime: cp.native.worker.startTime } } });
      };
      fs.writeFileSync(path.join(directory, "recovery.json"), JSON.stringify({ recovered, requests, ...custody() }, null, 2));
      // Option A: primary PID/birth gone is NOT whole-execution proof. Preserve R1 and
      // both durable sources for a later genuine join; never mint nativeExit or archive as non-native.
      expect(recoveredActors.status(cp.actorId)).toMatchObject({ model: liveModel, modelReason: liveModelReason, thinking: "high" });
      assertUnprovenCustody();
      const stopped = await callMainDrain(socket, SID, "stop-actors", bind(seat));
      expect.soft(["ACTOR_STOP_UNPROVEN", "ACTORS_NOT_QUIESCED"]).toContain(stopped.stopRefused);
      expect.soft(stopped.stopped.map(candidate => candidate.id)).not.toContain(cp.actorId);
      assertUnprovenCustody(); // Refusal must not erase identity/source or publish false proof.
      expect(spawned).not.toHaveBeenCalled();
      expect(nativeLaunches).not.toHaveBeenCalled();
      fs.writeFileSync(path.join(directory, "stop-unproven.json"), JSON.stringify({ stopped, ...custody() }, null, 2));
    } finally {
      clearTimeout(timeout);
      if (child.exitCode === null && child.signalCode === null && child.pid) {
        try { process.kill(-child.pid, "SIGKILL"); } catch { /* already gone */ }
      }
      await childExit.catch(() => undefined);
      fs.closeSync(log);
      // A non-crashing child cleans itself. A killed child leaves exact launch identities for this bounded fallback.
      const ownershipFile = path.join(directory, "workers.json");
      try {
        if (childGroupAlive() && child.pid) {
          try { process.kill(-child.pid, "SIGKILL"); } catch { /* raced the final native exit */ }
          await vi.waitFor(() => expect(childGroupAlive()).toBe(false), { timeout: 5_000, interval: 20 });
        }
        if (fs.existsSync(ownershipFile)) {
          const owned = JSON.parse(fs.readFileSync(ownershipFile, "utf8")) as { base: string; owned: CompletedRun[] };
          if (fs.existsSync(owned.base)) fs.writeFileSync(path.join(owned.base, "hard-exit-R1-release"), "release");
          for (const run of owned.owned) {
            if (!procWorkerExited(run.worker.pid, run.worker.startTime)) {
              // Exact PID/birth only; never signal a replacement process or an unrelated runner group.
              try { if (procStartTime(run.worker.pid) === run.worker.startTime) process.kill(run.worker.pid, "SIGKILL"); }
              catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
              await vi.waitFor(() => expect(procWorkerExited(run.worker.pid, run.worker.startTime)).toBe(true),
                { timeout: 5_000, interval: 20 });
            }
          }
        }
      } finally {
        try { await actors?.close(); } finally {
          try { await agents?.close(); } finally { await seat?.close(); }
        }
      }
      // Keep child.log, checkpoint, original session/run bytes and recovery receipts until the OUTER launcher cleans its owned temp.
    }
  }, 120_000);

  it("retains a timed-out supervisor worker's durable stop proof across a manager and Main restart", async () => {
    const h = await harness({ agentTimeoutMs: 1_000 }); fixtures.push(h);
    const actorId = await createSupervisor(h);
    await completeSupervisorRun(h, actorId);
    const saved = h.actors.drainSpec(actorId);
    const terminal = await finishSupervisorTerminalRun(h, actorId, "timed_out");
    await h.restart({ newAgentManager: true, newMain: true });

    const socket = mainSocket(h.seat);
    expect((await callMainDrain(socket, SID, "drain", bind(h.seat))).native.inputsFenced).toBe(true);
    await expectPriorWorkerStopAndUndo(h, actorId, terminal, saved, socket);
  }, 30_000);
});
