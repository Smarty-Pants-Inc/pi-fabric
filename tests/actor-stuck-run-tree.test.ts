import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorLogStore } from "../src/actors/log-store.js";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { processIsAlive } from "../src/agents/transports/process-utils.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#16333 round 4. The stuck-run check may end a drain's wait on a run whose worker is proven gone
// (tests/actor-stuck-run-unwind.test.ts). Two guards on that unwind:
// - P1: a dead worker pid does not end the run. AgentManager holds an actor run open until its whole process tree
//   has exited (no second writer on the actor session), so the check unwinds only when that tree is proven gone, and
//   a run whose real result exists delivers it instead of a worker-gone failure.
// - P2: the second proof that confirms a gone worker counts time, not sweeps: while the startup sweep keeps failing,
//   every mesh poll sweeps again, seconds apart.
const MINUTE = 60_000;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);
// Real-time waits: these tests skew or freeze Date.
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000): Promise<void> => {
  const until = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() > until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
/** About twenty 20 ms sweeps. */
const ticks = () => new Promise(resolve => setTimeout(resolve, 400));
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const tempRoot = (prefix: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  return root;
};
const stuckAlarms = (mesh: MeshStore, actorId: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
  .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === actorId);
const workerGoneFailures = (actors: ActorManager, actorId: string, runId: string) => actors.messages(actorId, 200)
  .filter(message => message.error?.includes(runId) && message.error.includes("worker-gone"));

describe("P1: a stuck run whose worker exited while its process tree lives (smarty-dev#16333 round 4)", () => {
  // The review's repro: a real process-transport run. The worker starts one child in its process group, writes a
  // completed result and exits; the child keeps running. Only the clock is skewed past the stuck threshold.
  it("is not unwound while a descendant lives; its real result is delivered and the next message starts only after the child exits", async () => {
    const realNow = Date.now.bind(Date);
    const clock = { skew: 0 };
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
    const root = tempRoot("pi-fabric-stuck-tree-");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const delivered: string[] = [];
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
      (event) => { if (event.message.text) delivered.push(event.message.text); }, {
        actorRoot: path.join(root, "actors"), persistent: false, retentionSweepMs: 20 });
    const release = path.join(root, "release-child");
    // Reverse order: release the child first, so even a red run leaves no process behind.
    closers.push(() => agents.close(), () => actors.close(), async () => fs.writeFileSync(release, ""));
    let childPid: number | undefined;
    /** Each activation's start, and whether the lingering child was alive at that moment. */
    const starts: Array<{ message: string; childAlive: boolean }> = [];
    const run = agents.run.bind(agents);
    vi.spyOn(agents, "run").mockImplementation((request, ...rest) => {
      starts.push({ message: /MSG-\d/.exec(request.task)?.[0] ?? "unknown", childAlive: childPid !== undefined && processIsAlive(childPid) });
      return run(request, ...rest);
    });

    const actor = await actors.create({ name: "lingering", instructions: "Run", coalesce: false, delivery: "followUp", triggerTurn: false });
    const answer = actors.ask(actor.id, "MSG-1", { fakeWorkerLingerChild: release })
      .then(message => ({ message }), (error: unknown) => ({ error }));
    await waitFor(() => {
      try { childPid = Number(fs.readFileSync(`${release}.pid`, "utf8")); } catch { childPid = undefined; }
      return Number.isSafeInteger(childPid) && childPid! > 0 && actors.status(actor.id).inFlightRun !== undefined;
    });
    const runId = actors.status(actor.id).inFlightRun!.id;
    const worker = agents.workerIdentity(runId)!;
    expect(worker).toMatchObject({ pid: expect.any(Number) });
    // The worker wrote its completed result and exited; its child lives on, so AgentManager keeps the run open.
    await waitFor(() => !processIsAlive(worker.pid));
    expect(JSON.parse(fs.readFileSync(path.join(agents.runDirectory(runId)!, "status.json"), "utf8")))
      .toMatchObject({ status: "completed", text: "fake worker complete" });
    expect(processIsAlive(childPid!)).toBe(true);
    // AgentManager already reports the worker's completed record; it holds the run only for the live child.
    expect(agents.status(runId)).toMatchObject({ status: "completed", text: "fake worker complete" });
    actors.tell(actor.id, "MSG-2");
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });

    // Past the stuck threshold the check proves the worker gone and alarms, as before.
    clock.skew = 31 * MINUTE;
    await waitFor(() => stuckAlarms(mesh, actor.id).length === 1);
    expect(stuckAlarms(mesh, actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId });
    // Many sweeps, and well past the confirmation gap (60 s at this sweep interval) several times: the worker stays
    // proven gone, but its tree lives, so the run is not unwound and MSG-2 does not start.
    for (const step of [2, 4, 8]) {
      clock.skew = (31 + step) * MINUTE;
      await ticks();
    }
    expect(processIsAlive(childPid!)).toBe(true);
    expect(starts.map(start => start.message)).toEqual(["MSG-1"]);
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });
    expect(workerGoneFailures(actors, actor.id, runId)).toHaveLength(0);
    expect(await Promise.race([answer, new Promise(resolve => setTimeout(() => resolve("pending"), 50))])).toBe("pending");
    // The manager's own answer agrees: the run's execution is not over.
    await expect(agents.executionExit(runId)).resolves.toEqual({ exited: false });

    // The child exits: the normal settle path delivers the real result, and only then does MSG-2 start.
    fs.writeFileSync(release, "");
    await waitFor(() => !processIsAlive(childPid!));
    const settled = await answer;
    expect(settled).toMatchObject({ message: { direction: "out", text: "fake worker complete" } });
    await waitFor(() => starts.length === 2 && actors.status(actor.id).queued === 0 && !actors.inFlightActorIds().includes(actor.id));
    expect(starts[1]).toEqual({ message: "MSG-2", childAlive: false });
    expect(workerGoneFailures(actors, actor.id, runId)).toHaveLength(0);
    // Delivered once: MSG-1's real result, then MSG-2's.
    expect(delivered).toEqual(["fake worker complete", "fake worker complete"]);
    expect(actors.messages(actor.id, 200).filter(message => message.direction === "out" && message.text === "fake worker complete"))
      .toHaveLength(2);
    await ticks();
    expect(starts).toHaveLength(2);
    expect(stuckAlarms(mesh, actor.id)).toHaveLength(1);
  }, 60_000);

  // A handle that never settles, with a real process: AgentManager's monitor for the run dies (one injected liveness
  // probe error), so nothing settles the run. The worker then writes its completed result and exits with no child.
  it("a run whose whole tree is gone and whose completed result exists delivers that result, never worker-gone", async () => {
    const realNow = Date.now.bind(Date);
    const clock = { skew: 0 };
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
    const root = tempRoot("pi-fabric-stuck-tree-");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: false, retentionSweepMs: 20 });
    const release = path.join(root, "release-worker");
    closers.push(() => agents.close(), () => actors.close(), async () => fs.writeFileSync(release, ""));
    const failProbe = { armed: false, failed: false };
    const launch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, ...args) {
      const handle = await launch.apply(this, args);
      const isAlive = handle.isAlive.bind(handle);
      handle.isAlive = async () => {
        if (failProbe.armed) { failProbe.armed = false; failProbe.failed = true; throw new Error("injected liveness probe failure"); }
        return isAlive();
      };
      return handle;
    });
    const starts: string[] = [];
    const run = agents.run.bind(agents);
    vi.spyOn(agents, "run").mockImplementation((request, ...rest) => {
      starts.push(/MSG-\d/.exec(request.task)?.[0] ?? "unknown");
      return run(request, ...rest);
    });
    const actor = await actors.create({ name: "orphaned", instructions: "Run", coalesce: false });
    const answer = actors.ask(actor.id, "MSG-1 LIVE_WITH_PROGRESS", { fakeWorkerReleasePath: release })
      .then(message => ({ message }), (error: unknown) => ({ error }));
    await waitFor(() => actors.status(actor.id).inFlightRun !== undefined);
    const runId = actors.status(actor.id).inFlightRun!.id;
    await waitFor(() => fs.existsSync(path.join(agents.runDirectory(runId)!, "status.json")));
    const worker = agents.workerIdentity(runId)!;
    failProbe.armed = true;
    await waitFor(() => failProbe.failed);
    // The monitor is gone. The worker finishes on its own: a completed result, then exit, with no descendant.
    fs.writeFileSync(release, "");
    await waitFor(() => !processIsAlive(worker.pid));
    await ticks();
    expect(agents.status(runId)).toMatchObject({ status: "completed", text: "live attempt 1 complete" });
    actors.tell(actor.id, "MSG-2");
    await ticks();
    // Nothing settles it: the drain still waits on the run, with MSG-2 queued behind.
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });

    clock.skew = 31 * MINUTE;
    await waitFor(() => stuckAlarms(mesh, actor.id).length === 1);
    expect(stuckAlarms(mesh, actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId });
    await ticks();
    expect(starts).toEqual(["MSG-1"]);
    clock.skew = 31 * MINUTE + 61_000;
    // AgentManager proves the whole execution over and hands back the worker's genuine verdict: the drain delivers
    // the run's own completed result, then MSG-2 runs once.
    expect(await answer).toMatchObject({ message: { direction: "out", text: "live attempt 1 complete" } });
    await waitFor(() => starts.length === 2 && actors.status(actor.id).queued === 0 && !actors.inFlightActorIds().includes(actor.id));
    expect(workerGoneFailures(actors, actor.id, runId)).toHaveLength(0);
    expect(actors.messages(actor.id, 200).filter(message => message.direction === "out" && message.text === "live attempt 1 complete"))
      .toHaveLength(1);
    await ticks();
    expect(starts).toEqual(["MSG-1", "MSG-2"]);
    expect(stuckAlarms(mesh, actor.id)).toHaveLength(1);
  }, 60_000);
});

/**
 * The incident's handle, as in tests/actor-stuck-run-unwind.test.ts: agents.run is a fake whose first handle never
 * settles, and the stuck-run check sees its dead worker through spied AgentManager accessors.
 */
const stubbed = async (options: { retentionSweepMs?: number; canConsumeMesh?: () => boolean } = {}) => {
  const root = tempRoot("pi-fabric-stuck-proof-");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  const runDirectory = path.join(root, "phantom-run");
  fs.mkdirSync(runDirectory);
  fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify({ status: "running" }));
  const { spawn } = await import("node:child_process");
  const exited = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  await new Promise(resolve => exited.once("exit", resolve));
  const dead = exited.pid!;
  const launches: string[] = [];
  let phantom: string | undefined;
  let releasePhantom: (() => void) | undefined;
  const completed = (id: string, task: string): AgentRunResult => ({
    id, name: "fake", task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
    startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: `done ${id}`,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  });
  vi.spyOn(agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
    const id = randomUUID().replaceAll("-", "");
    launches.push(/MSG-\d/.exec(request.task)?.[0] ?? "unknown");
    onSpawned?.({ id, name: "fake", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    if (phantom !== undefined) return completed(id, request.task);
    phantom = id;
    return new Promise<AgentRunResult>(resolve => { releasePhantom = () => resolve(completed(id, request.task)); });
  });
  const realRunDirectory = agents.runDirectory.bind(agents);
  const realWorkerIdentity = agents.workerIdentity.bind(agents);
  vi.spyOn(agents, "runDirectory").mockImplementation(id => id === phantom ? runDirectory : realRunDirectory(id));
  vi.spyOn(agents, "workerIdentity").mockImplementation(id =>
    id === phantom ? { pid: dead, startTime: undefined, open: false } : realWorkerIdentity(id));
  const delivered: string[] = [];
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
    (event) => { if (event.message.text) delivered.push(event.message.text); }, {
      actorRoot: path.join(root, "actors"), persistent: false,
      ...(options.retentionSweepMs === undefined ? {} : { retentionSweepMs: options.retentionSweepMs }),
      ...(options.canConsumeMesh ? { canConsumeMesh: options.canConsumeMesh } : {}) });
  closers.push(() => agents.close(), () => actors.close(), async () => releasePhantom?.());
  /** An actor with the phantom in flight for an ask (MSG-1), and MSG-2 queued behind it. */
  const stuckActor = async () => {
    const actor = await actors.create({ name: "stuck", instructions: "Run", coalesce: false, delivery: "followUp", triggerTurn: false });
    const answer = actors.ask(actor.id, "MSG-1").then(message => ({ message }), (error: unknown) => ({ error }));
    await waitFor(() => phantom !== undefined && actors.status(actor.id).inFlightRun?.id === phantom);
    actors.tell(actor.id, "MSG-2");
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: phantom } });
    return { actor, answer };
  };
  return { root, mesh, agents, actors, launches, delivered, stuckActor, completed,
    phantom: () => phantom!, release: () => releasePhantom?.() };
};

describe("P1: what the stuck-run check does once the worker is proven gone twice", () => {
  it("while the manager says the tree may live, it only alarms, for hours; the run's own result then continues the queue", async () => {
    const realNow = Date.now.bind(Date);
    const clock = { skew: 0 };
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
    const s = await stubbed({ retentionSweepMs: 20 });
    const exit = vi.spyOn(s.agents, "executionExit").mockResolvedValue({ exited: false });
    const { actor, answer } = await s.stuckActor();
    const phantom = s.phantom();
    clock.skew = 31 * MINUTE;
    await waitFor(() => stuckAlarms(s.mesh, actor.id).length === 1);
    for (const hours of [1, 2, 5]) {
      clock.skew = hours * 60 * MINUTE;
      await ticks();
    }
    expect(exit).toHaveBeenCalledWith(phantom);
    expect(s.launches).toEqual(["MSG-1"]);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: phantom } });
    expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(0);
    s.release();
    expect(await answer).toMatchObject({ message: { text: `done ${phantom}` } });
    await waitFor(() => s.launches.length === 2 && s.actors.status(actor.id).queued === 0 &&
      !s.actors.inFlightActorIds().includes(actor.id));
    expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(0);
  }, 60_000);

  it("a tree proven gone with the run's genuine result delivers that result, never a worker-gone failure", async () => {
    const realNow = Date.now.bind(Date);
    const clock = { skew: 0 };
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
    const s = await stubbed({ retentionSweepMs: 20 });
    const { actor, answer } = await s.stuckActor();
    const phantom = s.phantom();
    const result = { ...s.completed(phantom, "MSG-1"), text: "the real MSG-1 answer" };
    const realExit = s.agents.executionExit.bind(s.agents);
    vi.spyOn(s.agents, "executionExit").mockImplementation(async id => id === phantom ? { exited: true, result } : realExit(id));
    clock.skew = 31 * MINUTE;
    await waitFor(() => stuckAlarms(s.mesh, actor.id).length === 1);
    clock.skew = 31 * MINUTE + 61_000;
    expect(await answer).toMatchObject({ message: { direction: "out", text: "the real MSG-1 answer" } });
    await waitFor(() => s.launches.length === 2 && s.actors.status(actor.id).queued === 0 &&
      !s.actors.inFlightActorIds().includes(actor.id));
    expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(0);
    expect(s.delivered).toEqual(["the real MSG-1 answer", `done ${s.actors.status(actor.id).lastRunId}`]);
    await ticks();
    expect(s.launches).toEqual(["MSG-1", "MSG-2"]);
    expect(s.delivered).toHaveLength(2);
    expect(stuckAlarms(s.mesh, actor.id)).toHaveLength(1);
  }, 60_000);
});

describe("P2: the two-proof rule counts time, not sweeps (smarty-dev#16333 round 4)", () => {
  it("while the startup sweep keeps failing, every mesh poll sweeps again, but only a proof one sweep interval after the first unwinds", async () => {
    vi.useFakeTimers({ toFake: ["Date"], now: Date.parse("2026-10-24T00:00:00.000Z") });
    // The startup sweep never reaches its end: first mesh consumption is off as it finishes, then its run pruning
    // throws. So the manager keeps its initial-retention flag and every mesh poll starts another sweep: proofs land
    // seconds apart, not 15 min.
    let sweeps = 0;
    vi.spyOn(ActorLogStore.prototype, "pruneRuns").mockImplementation(() => { sweeps++; throw new Error("prune failed"); });
    let consume = false;
    const s = await stubbed({ canConsumeMesh: () => consume }); // the production 15 min interval: it never fires here
    const { actor, answer } = await s.stuckActor();
    const phantom = s.phantom();
    consume = true;
    /** Mesh writes wake the actor mesh monitor; each poll starts a sweep. */
    const polls = async (count: number): Promise<void> => {
      const before = sweeps;
      for (let index = 0; index < count; index++) {
        await s.mesh.publish({ topic: "test.noise", from: identity, text: `poll ${index}` });
        await new Promise(resolve => setTimeout(resolve, 40));
      }
      await waitFor(() => sweeps >= before + 2);
    };
    const startedAt = Date.now();
    const firstProofAt = startedAt + 31 * MINUTE;
    vi.setSystemTime(firstProofAt);
    await polls(3);
    await waitFor(() => stuckAlarms(s.mesh, actor.id).length === 1);
    expect(stuckAlarms(s.mesh, actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId: phantom });
    // Proof after proof at the same instant, and then up to just short of one sweep interval (less 5 s of timer
    // jitter): still no unwind.
    for (const at of [firstProofAt, firstProofAt + 30_000, firstProofAt + 5 * MINUTE, firstProofAt + 15 * MINUTE - 5_001]) {
      vi.setSystemTime(at);
      await polls(4);
      expect(s.launches).toEqual(["MSG-1"]);
      expect(s.actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: phantom } });
      expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(0);
    }
    // One sweep interval after the first proof, the next proof confirms it: the drain unwinds, the run fails with
    // worker-gone (no tree, no result), and MSG-2 runs once.
    vi.setSystemTime(firstProofAt + 15 * MINUTE - 5_000);
    await polls(2);
    expect(await answer).toMatchObject({ error: expect.objectContaining({ message: expect.stringContaining("worker-gone") }) });
    await waitFor(() => s.launches.length === 2 && s.actors.status(actor.id).queued === 0 &&
      !s.actors.inFlightActorIds().includes(actor.id));
    expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(1);
    await polls(3);
    expect(s.launches).toEqual(["MSG-1", "MSG-2"]);
    expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(1);
  }, 60_000);

  it("at a short test sweep interval the gap is still at least 60 s", async () => {
    vi.useFakeTimers({ toFake: ["Date"], now: Date.parse("2026-10-24T00:00:00.000Z") });
    const s = await stubbed({ retentionSweepMs: 20 });
    const { actor, answer } = await s.stuckActor();
    const phantom = s.phantom();
    const firstProofAt = Date.now() + 31 * MINUTE;
    vi.setSystemTime(firstProofAt);
    await waitFor(() => stuckAlarms(s.mesh, actor.id).length === 1);
    vi.setSystemTime(firstProofAt + 59_999);
    await ticks();
    expect(s.launches).toEqual(["MSG-1"]);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: phantom } });
    vi.setSystemTime(firstProofAt + 60_000);
    expect(await answer).toMatchObject({ error: expect.objectContaining({ message: expect.stringContaining("worker-gone") }) });
    await waitFor(() => s.launches.length === 2 && !s.actors.inFlightActorIds().includes(actor.id));
    expect(workerGoneFailures(s.actors, actor.id, phantom)).toHaveLength(1);
  }, 60_000);
});
