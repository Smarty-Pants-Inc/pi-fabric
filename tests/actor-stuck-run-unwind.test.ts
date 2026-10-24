import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#16333: the owner's drain awaited a run handle that never settled (no events, no worker, and
// agents.stop(runId) answered "Unknown Fabric participant"). The existing stuck-run check saw worker-gone but only
// alarmed, so every message queued behind that run waited forever. No real worker runs here: agents.run is a fake
// whose first handle never settles, and the stuck-run check reads that run's worker through spied AgentManager
// accessors. The retention sweep that runs the check ticks every 20 ms.
const MINUTE = 60_000;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);
const waitFor = async (predicate: () => boolean, timeoutMs = 10_000): Promise<void> => {
  const until = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
/** About twenty 20 ms retention sweeps: N ticks for the no-loop bound. */
const ticks = () => new Promise(resolve => setTimeout(resolve, 400));
/** A real process that has exited: its pid is not an invented one. */
const deadPid = async (): Promise<number> => {
  const { spawn } = await import("node:child_process");
  const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
  await new Promise(resolve => child.once("exit", resolve));
  return child.pid!;
};

const setup = async (worker: "gone" | "alive") => {
  const realNow = Date.now.bind(Date);
  const clock = { skew: 0 };
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-stuck-run-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  // The phantom run's directory and status record exist, as for the incident's worker-gone alarm.
  const runDirectory = path.join(root, "phantom-run");
  fs.mkdirSync(runDirectory);
  fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify({ status: "running" }));
  const dead = await deadPid();
  /** The phantom's worker as the stuck-run check sees it; a test may flip it (a relaunch). */
  const state = { worker };
  /** Ordered: each launch (`run:<message>`) and the stuck-run check's first look at the phantom's worker (`check`). */
  const timeline: string[] = [];
  const launches: Array<{ id: string; message: string; phantomFailedBefore: boolean }> = [];
  const hooks: { phantomFailed?: () => boolean; afterCheck?: () => void } = {};
  let phantom: string | undefined;
  let releasePhantom: (() => void) | undefined;
  const completed = (id: string, task: string): AgentRunResult => ({
    id, name: "fake", task, status: "completed", runner: "pi", transport: "process", cwd: process.cwd(),
    startedAt: Date.now(), updatedAt: Date.now(), turns: 1, toolCalls: 0, inferenceStarted: true, text: `done ${id}`,
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
  });
  vi.spyOn(agents, "run").mockImplementation(async (request, _signal, onSpawned) => {
    const id = randomUUID().replaceAll("-", "");
    const message = /MSG-\d/.exec(request.task)?.[0] ?? "unknown";
    timeline.push(`run:${message}`);
    launches.push({ id, message, phantomFailedBefore: hooks.phantomFailed?.() ?? false });
    onSpawned?.({ id, name: "fake", status: "running", runner: "pi", transport: "process", cwd: process.cwd() });
    if (phantom !== undefined) return completed(id, request.task);
    phantom = id;
    // The incident's handle: it never settles, and an abort does not settle it either. Only teardown releases it.
    return new Promise<AgentRunResult>(resolve => { releasePhantom = () => resolve(completed(id, request.task)); });
  });
  const realRunDirectory = agents.runDirectory.bind(agents);
  const realWorkerIdentity = agents.workerIdentity.bind(agents);
  const realStatus = agents.status.bind(agents);
  vi.spyOn(agents, "runDirectory").mockImplementation(id => id === phantom ? runDirectory : realRunDirectory(id));
  vi.spyOn(agents, "workerIdentity").mockImplementation(id => {
    if (id !== phantom) return realWorkerIdentity(id);
    if (timeline.at(-1) !== "check") timeline.push("check");
    // No recorded start time (as off Linux): a dead pid is worker-gone; a live pid on an open handle is alive.
    const seen = state.worker === "gone" ? { pid: dead, startTime: undefined, open: false }
      : { pid: process.pid, startTime: undefined, open: true };
    hooks.afterCheck?.();
    return seen;
  });
  // The incident's lookups of the run: the fix must not depend on any of them.
  const unknown = () => new Error("Unknown Fabric participant (no record on this mesh root)");
  vi.spyOn(agents, "status").mockImplementation(id => { if (id === phantom) throw unknown(); return realStatus(id); });
  const stop = vi.spyOn(agents, "stop").mockImplementation(async () => { throw unknown(); });
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: false, retentionSweepMs: 20 });
  // Reverse order: release a still-pending phantom first, so even a red run tears down.
  closers.push(() => agents.close(), () => actors.close(), async () => releasePhantom?.());
  const alarms = (id: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
    .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === id);
  /** Messages that record the phantom run as failed for worker-gone. */
  const phantomFailures = (id: string) => actors.messages(id, 200)
    .filter(message => phantom !== undefined && message.error?.includes(phantom) && message.error.includes("worker-gone"));
  /** An actor with the phantom in flight and two messages queued behind it. */
  const stuckActor = async () => {
    const actor = await actors.create({ name: "stuck", instructions: "Run", coalesce: false });
    actors.tell(actor.id, "MSG-1");
    await waitFor(() => phantom !== undefined && actors.status(actor.id).inFlightRun?.id === phantom);
    actors.tell(actor.id, "MSG-2");
    actors.tell(actor.id, "MSG-3");
    expect(actors.status(actor.id)).toMatchObject({ queued: 2, inFlightRun: { id: phantom } });
    hooks.phantomFailed = () => phantomFailures(actor.id).length > 0;
    return actor;
  };
  return { actors, agents, clock, state, hooks, timeline, launches, alarms, phantomFailures, stuckActor, stop,
    phantom: () => phantom!, release: () => releasePhantom?.() };
};

describe("an actor run whose worker is gone (smarty-dev#16333)", () => {
  it("the stuck-run check unwinds the drain: the run fails with worker-gone and each queued message runs once, with no loop", async () => {
    const s = await setup("gone");
    const actor = await s.stuckActor();
    const phantom = s.phantom();
    // Inside the existing threshold (30 min minimum) the check does not even look at the worker.
    s.clock.skew = 29 * MINUTE;
    await ticks();
    expect(s.timeline).toEqual(["run:MSG-1"]);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 2, inFlightRun: { id: phantom } });

    s.clock.skew = 31 * MINUTE;
    // 1. The check confirms worker-gone (its existing alarm; the wait ends on the next consecutive proof).
    await waitFor(() => s.alarms(actor.id).length === 1);
    expect(s.alarms(actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId: phantom });
    // 2. The drain unwinds: the phantom is no longer in flight and the queue moves.
    await waitFor(() => s.actors.status(actor.id).inFlightRun?.id !== phantom);
    await waitFor(() => s.launches.length === 3 && s.actors.status(actor.id).queued === 0 &&
      !s.actors.inFlightActorIds().includes(actor.id));
    expect(s.timeline).toEqual(["run:MSG-1", "check", "run:MSG-2", "run:MSG-3"]);
    // 3. The run is recorded failed with worker-gone, before message 2 launched.
    const failures = s.phantomFailures(actor.id);
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ runId: phantom, data: { runId: phantom, reason: "worker-gone" } });
    expect(s.launches.map(launch => [launch.message, launch.phantomFailedBefore]))
      .toEqual([["MSG-1", false], ["MSG-2", true], ["MSG-3", true]]);
    // 4. Message 2 (and 3) ran exactly once, in order, and each output was recorded once.
    const outputs = s.actors.messages(actor.id, 200).filter(message => message.text?.startsWith("done "));
    expect(outputs.map(message => message.text)).toEqual([`done ${s.launches[1]!.id}`, `done ${s.launches[2]!.id}`]);
    // 5. No loop: N more ticks add no activation, alarm or failure record.
    await ticks();
    expect(s.launches).toHaveLength(3);
    expect(s.alarms(actor.id)).toHaveLength(1);
    expect(s.phantomFailures(actor.id)).toHaveLength(1);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 0 });
    expect(s.actors.status(actor.id).inFlightRun).toBeUndefined();
    // Nothing depended on an AgentManager record of the run.
    expect(s.stop).not.toHaveBeenCalled();
  }, 60_000);

  it("a live slow run (handle pending, worker alive) is not unwound", async () => {
    const s = await setup("alive");
    const actor = await s.stuckActor();
    const phantom = s.phantom();
    s.clock.skew = 31 * MINUTE;
    await waitFor(() => s.timeline.includes("check"));
    s.clock.skew = 5 * 60 * MINUTE;
    await ticks();
    expect(s.timeline).toEqual(["run:MSG-1", "check"]);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 2, inFlightRun: { id: phantom } });
    expect(s.alarms(actor.id)).toHaveLength(0);
    expect(s.phantomFailures(actor.id)).toHaveLength(0);
    // Its own completion still continues the queue, once each.
    s.release();
    await waitFor(() => s.launches.length === 3 && s.actors.status(actor.id).queued === 0 &&
      !s.actors.inFlightActorIds().includes(actor.id));
    await ticks();
    expect(s.timeline.filter(entry => entry.startsWith("run:"))).toEqual(["run:MSG-1", "run:MSG-2", "run:MSG-3"]);
    expect(s.phantomFailures(actor.id)).toHaveLength(0);
  }, 60_000);

  it("one proof is not enough: a worker gone at one check and alive at the next (a relaunch) keeps its run", async () => {
    const s = await setup("gone");
    const actor = await s.stuckActor();
    const phantom = s.phantom();
    // The first proof alarms; by the next sweep the manager has relaunched the worker.
    s.hooks.afterCheck = () => { s.state.worker = "alive"; };
    s.clock.skew = 31 * MINUTE;
    await waitFor(() => s.alarms(actor.id).length === 1);
    expect(s.alarms(actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId: phantom });
    await ticks();
    expect(s.timeline).toEqual(["run:MSG-1", "check"]);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 2, inFlightRun: { id: phantom } });
    expect(s.phantomFailures(actor.id)).toHaveLength(0);
  }, 60_000);

  it("a stopped actor's stuck run unwinds without starting a new run", async () => {
    const s = await setup("gone");
    const actor = await s.stuckActor();
    const phantom = s.phantom();
    // The stop aborts the run's signal, which does not settle this handle: the drain stays stuck behind it.
    await s.actors.stop(actor.id);
    await ticks();
    expect(s.actors.status(actor.id)).toMatchObject({ status: "stopped", inFlightRun: { id: phantom } });
    expect(s.actors.inFlightActorIds()).toContain(actor.id);

    s.clock.skew = 31 * MINUTE;
    await waitFor(() => s.alarms(actor.id).length === 1);
    expect(s.alarms(actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId: phantom });
    await waitFor(() => !s.actors.inFlightActorIds().includes(actor.id));
    expect(s.actors.status(actor.id).inFlightRun).toBeUndefined();
    expect(s.phantomFailures(actor.id)).toHaveLength(1);
    await ticks();
    expect(s.timeline).toEqual(["run:MSG-1", "check"]);
    expect(s.actors.status(actor.id)).toMatchObject({ status: "stopped", queued: 0 });
    expect(s.alarms(actor.id)).toHaveLength(1);
    expect(s.stop).not.toHaveBeenCalled();
  }, 60_000);
});
