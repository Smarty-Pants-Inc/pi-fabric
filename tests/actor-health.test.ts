import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import type { FabricActorHealth } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, type FabricAgentConfig } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const roots: string[] = [];
const fixtures: Array<{ actors: ActorManager; agents: AgentManager; workers: Map<string, number> }> = [];

const killWorker = (pid: number): void => {
  try { process.kill(pid, "SIGKILL"); } catch { /* The exact worker has already exited. */ }
};

afterEach(async () => {
  for (const fixture of fixtures.splice(0)) {
    for (const [runId, pid] of fixture.workers) {
      const worker = fixture.agents.workerIdentity(runId);
      if (worker?.pid === pid && worker.open) killWorker(pid);
    }
    await fixture.actors.close().catch(() => undefined);
    await fixture.agents.close().catch(() => undefined);
  }
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
}, 30_000);

const waitFor = async (predicate: () => boolean, timeoutMs = 5_000, intervalMs = 20): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor health state");
    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }
};

const terminalHeldWorker = `import fs from "node:fs";
import path from "node:path";
const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i].slice(2), process.argv[i + 1]);
const statusFile = args.get("status-file");
const taskFile = args.get("task-file");
const task = fs.readFileSync(taskFile, "utf8");
const directory = path.dirname(statusFile);
const ready = path.join(directory, "actor-health-ready");
const finish = path.join(directory, "actor-health-finish");
const publish = record => {
  const temporary = statusFile + ".health-test.tmp";
  fs.writeFileSync(temporary, JSON.stringify(record));
  fs.renameSync(temporary, statusFile);
};
const startedAt = Date.now();
const running = {
  id: args.get("id"), name: args.get("name"), task, status: "running",
  runner: args.get("runner") ?? "pi", transport: args.get("transport"),
  sessionId: String(process.pid), cwd: args.get("cwd"), startedAt, updatedAt: startedAt,
  turns: 0, toolCalls: 0, text: "", exitCode: null,
  usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
};
fs.mkdirSync(directory, { recursive: true });
publish(running);
fs.writeFileSync(ready, "ready");
process.on("SIGTERM", () => process.exit(0));
process.on("SIGINT", () => process.exit(0));
while (!fs.existsSync(finish)) await new Promise(resolve => setTimeout(resolve, 10));
await new Promise(resolve => setTimeout(resolve, 30));
const finishedAt = Date.now();
publish({ ...running, status: "completed", updatedAt: finishedAt, finishedAt, turns: 1,
  text: "terminal worker result", exitCode: 0,
  usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: 0 } });
setInterval(() => {}, 1_000);
`;

const setup = (
  workerPath = path.resolve("tests/fixtures/fake-worker.mjs"),
  options: { piBinary?: string; agentsConfig?: Partial<FabricAgentConfig> } = {},
) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-health-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true, ...options.agentsConfig }, {
    workerPath, ...(options.piBinary ? { piBinary: options.piBinary } : {}), runRoot: path.join(root, "runs"),
  });
  const identity: MeshIdentity = { id: "session:health-test", name: "main", kind: "main", sessionId: "health-test" };
  const actors = new ActorManager("health-test", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), retentionSweepMs: 20,
    });
  const fixture = { actors, agents, workers: new Map<string, number>() };
  fixtures.push(fixture);
  return { ...fixture, root };
};

const createActor = async (state: ReturnType<typeof setup>, name: string) =>
  state.actors.create({ name, instructions: "Respond to the queued work." });

const startWorker = async (state: ReturnType<typeof setup>, actorId: string, message: string) => {
  state.actors.tell(actorId, message);
  let runId: string | undefined;
  let worker: ReturnType<AgentManager["workerIdentity"]> = undefined;
  await waitFor(() => {
    runId = state.actors.status(actorId).inFlightRun?.id;
    worker = runId ? state.agents.workerIdentity(runId) : undefined;
    const runDirectory = runId ? state.agents.runDirectory(runId) : undefined;
    if (runId && worker?.pid) state.workers.set(runId, worker.pid);
    if (!runId || !worker?.pid || !worker.open || !runDirectory) return false;
    try {
      return statusRecord(path.join(runDirectory, "status.json")).status === "running" &&
        readHealth(state.actors, actorId)?.state === "healthy";
    } catch { return false; }
  }, 10_000);
  return { runId: runId!, worker: worker!, runDirectory: state.agents.runDirectory(runId!)! };
};

const readHealth = (actors: ActorManager, actorId: string): FabricActorHealth | undefined =>
  actors.status(actorId).health;

const statusRecord = (file: string): Record<string, unknown> =>
  JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;

describe("owner-derived actor health", () => {
  it("keeps actor creation and status usable when health evidence is unavailable", async () => {
    const state = setup();
    vi.spyOn(state.agents, "actorHealthEvidence").mockImplementation(() => { throw new Error("unreadable evidence"); });
    const actor = await createActor(state, "unavailable health evidence");
    expect(actor.health).toEqual({ state: "unknown", lastProgressAt: null });
    expect(state.actors.status(actor.id)).toMatchObject({ id: actor.id, status: "idle", health: actor.health });
    expect(state.actors.listOwned()).toHaveLength(1);
  });

  it("keeps a silent live worker healthy without advancing progress on reads or config edits", async () => {
    const state = setup();
    const actor = await createActor(state, "silent health probe");
    const run = await startWorker(state, actor.id, "HANG silent worker");
    const statusFile = path.join(run.runDirectory, "status.json");
    const record = statusRecord(statusFile);
    expect(record.status).toBe("running");
    expect(record.turns).toBe(0);
    expect(record.toolCalls).toBe(0);
    // An old, frozen worker progress timestamp is not evidence that a live silent worker is dead.
    fs.writeFileSync(statusFile, JSON.stringify({ ...record, updatedAt: Date.now() - 60 * 60_000 }));

    const initial = readHealth(state.actors, actor.id)!;
    expect(initial).toMatchObject({ state: "healthy", lastProgressAt: expect.any(Number) });
    const lastProgressAt = initial.lastProgressAt;
    expect(statusRecord(statusFile).updatedAt).toBeLessThan(Date.now() - 60_000);

    await state.actors.setInstructions(actor.id, "Updated instructions for the next activation.");
    for (let i = 0; i < 5; i++) {
      expect(readHealth(state.actors, actor.id)).toEqual({ state: "healthy", lastProgressAt });
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  });

  it("marks a killed owned worker stale with its real run still retained and status still running", async () => {
    const state = setup();
    const actor = await createActor(state, "worker death probe");
    const realRun = state.agents.run.bind(state.agents);
    let announceResult!: (result: Awaited<ReturnType<AgentManager["run"]>>) => void;
    const resultReady = new Promise<Awaited<ReturnType<AgentManager["run"]>>>(resolve => { announceResult = resolve; });
    let returnedResult: Awaited<ReturnType<AgentManager["run"]>> | undefined;
    void resultReady.then(result => { returnedResult = result; });
    let releaseBarrier!: () => void;
    const barrier = new Promise<void>(resolve => { releaseBarrier = resolve; });
    let held = false;
    const runSpy = vi.spyOn(state.agents, "run").mockImplementation(async (...args) => {
      const result = await realRun(...args);
      if (!held) {
        held = true;
        announceResult(result);
        await barrier;
      }
      return result;
    });

    try {
      const run = await startWorker(state, actor.id, "HANG_WITH_PROGRESS owner health kill");
      const statusFile = path.join(run.runDirectory, "status.json");
      const runningRecord = statusRecord(statusFile);
      const initial = readHealth(state.actors, actor.id)!;
      expect(initial.state).toBe("healthy");
      expect(runningRecord.status).toBe("running");
      expect(run.worker.open).toBe(true);
      state.actors.tell(actor.id, "queued behind the killed run one");
      state.actors.tell(actor.id, "queued behind the killed run two");
      await waitFor(() => state.actors.status(actor.id).queued === 2);

      // This is the actual owned PID. Hold actor processing only after AgentManager returns its terminal result.
      process.kill(run.worker.pid, "SIGKILL");
      await waitFor(() => returnedResult !== undefined, 5_000);
      const result = returnedResult!;
      expect(result).toMatchObject({ id: run.runId, finishedAt: expect.any(Number) });
      expect(result.status).not.toBe("running");
      expect(state.agents.workerIdentity(run.runId)).toMatchObject({ pid: run.worker.pid, open: false });

      // Preserve the worker's final nonterminal snapshot: health must trust owner run evidence, not this file.
      fs.writeFileSync(statusFile, JSON.stringify(runningRecord));
      const retained = state.actors.status(actor.id);
      expect(statusRecord(statusFile).status).toBe("running");
      expect(retained).toMatchObject({ inFlightRun: { id: run.runId }, queued: 2,
        health: { state: "stale", lastProgressAt: expect.any(Number) } });
      expect(retained.health!.lastProgressAt!).toBeGreaterThan(initial.lastProgressAt!);
    } finally {
      releaseBarrier();
      runSpy.mockRestore();
    }
  }, 15_000);

  it("marks a terminal worker stale while its real process is open and actor work queues", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-terminal-health-worker-"));
    roots.push(root);
    const workerPath = path.join(root, "terminal-health-worker.mjs");
    fs.writeFileSync(workerPath, terminalHeldWorker);
    const state = setup(workerPath);
    const actor = await createActor(state, "terminal health probe");
    const run = await startWorker(state, actor.id, "terminal worker health barrier");
    const statusFile = path.join(run.runDirectory, "status.json");
    const readyFile = path.join(run.runDirectory, "actor-health-ready");
    const finishFile = path.join(run.runDirectory, "actor-health-finish");
    await waitFor(() => fs.existsSync(readyFile));
    const initial = readHealth(state.actors, actor.id)!;
    expect(initial).toMatchObject({ state: "healthy", lastProgressAt: expect.any(Number) });

    state.actors.tell(actor.id, "queued behind the terminal run one");
    state.actors.tell(actor.id, "queued behind the terminal run two");
    await waitFor(() => state.actors.status(actor.id).queued === 2);
    fs.writeFileSync(finishFile, "finish");

    await waitFor(() => {
      const status = statusRecord(statusFile);
      const health = readHealth(state.actors, actor.id);
      const worker = state.agents.workerIdentity(run.runId);
      return status.status === "completed" && typeof status.finishedAt === "number" &&
        health?.state === "stale" && health.lastProgressAt !== null &&
        health.lastProgressAt > (initial.lastProgressAt ?? 0) && worker?.open === true;
    }, 3_000, 10);
    const current = state.actors.status(actor.id);
    const health = current.health!;
    expect(statusRecord(statusFile)).toMatchObject({ status: "completed", finishedAt: expect.any(Number) });
    expect(state.agents.workerIdentity(run.runId)).toMatchObject({ pid: run.worker.pid, open: true });
    expect(current).toMatchObject({ inFlightRun: { id: run.runId }, queued: 2 });
    expect(health.state).toBe("stale");
    expect(health.lastProgressAt!).toBeGreaterThan(initial.lastProgressAt!);
  }, 15_000);

  it.skipIf(process.platform !== "linux" || !fs.existsSync(path.resolve("dist/worker.js")))(
    "keeps a native completed result stale behind actor processing after held child teardown",
    { timeout: 30_000 },
    async () => {
      const nativeRoot = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-native-actor-health-"));
      roots.push(nativeRoot);
      const pidFile = path.join(nativeRoot, "native-pi-pid");
      const releaseFile = path.join(nativeRoot, "native-pi-release");
      const settleFile = path.join(nativeRoot, "native-pi-settle");
      vi.stubEnv("FAKE_PI_BEHAVIOR", "finishing-root-stubborn");
      vi.stubEnv("FAKE_PI_FINISHING_PID", pidFile);
      vi.stubEnv("FAKE_PI_FINISHING_RELEASE", releaseFile);
      vi.stubEnv("FAKE_PI_FINISHING_SETTLE", settleFile);
      fs.writeFileSync(settleFile, "allow native final settlement");

      const state = setup(path.resolve("dist/worker.js"), {
        piBinary: path.resolve("tests/fixtures/fake-pi.mjs"),
        agentsConfig: { budgetUsd: 0, deniedModels: [], timeoutMs: 20_000 },
      });
      const actor = await createActor(state, "native result health probe");
      const realRun = state.agents.run.bind(state.agents);
      let announceResult!: (result: Awaited<ReturnType<AgentManager["run"]>>) => void;
      const resultReady = new Promise<Awaited<ReturnType<AgentManager["run"]>>>(resolve => { announceResult = resolve; });
      let returnedResult: Awaited<ReturnType<AgentManager["run"]>> | undefined;
      void resultReady.then(result => { returnedResult = result; });
      let releaseBarrier!: () => void;
      const barrier = new Promise<void>(resolve => { releaseBarrier = resolve; });
      const runSpy = vi.spyOn(state.agents, "run").mockImplementation(async (...args) => {
        const result = await realRun(...args);
        announceResult(result);
        await barrier;
        return result;
      });

      try {
        const run = await startWorker(state, actor.id, "native held child final answer");
        const initial = readHealth(state.actors, actor.id)!;
        expect(initial.state).toBe("healthy");
        state.actors.tell(actor.id, "queued behind native completion one");
        state.actors.tell(actor.id, "queued behind native completion two");
        await waitFor(() => state.actors.status(actor.id).queued === 2);
        await waitFor(() => fs.existsSync(pidFile), 10_000);

        await waitFor(() => returnedResult !== undefined, 15_000);
        const result = returnedResult!;
        expect(result).toMatchObject({ id: run.runId, status: "completed", text: "QUIESCENT", finishedAt: expect.any(Number) });
        expect(result.warnings).toEqual([expect.stringContaining("did not exit after stdin closed for 5000ms")]);
        const retained = state.actors.status(actor.id);
        expect(retained).toMatchObject({ inFlightRun: { id: run.runId }, queued: 2,
          health: { state: "stale", lastProgressAt: expect.any(Number) } });
        expect(retained.health!.lastProgressAt!).toBeGreaterThan(initial.lastProgressAt!);
      } finally {
        vi.stubEnv("FAKE_PI_BEHAVIOR", "success");
        releaseBarrier();
        runSpy.mockRestore();
      }
    },
  );
});
