import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { processIsAlive } from "../src/agents/transports/process-utils.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// smarty-dev#16333 round 5. A run whose transport latched lost contact never unwinds by itself, even with its whole
// tree gone and a real result written: AgentManager cannot prove its execution over, so it never settles the run, and
// the stuck-run check only alarms (correct: with contact lost the tree may live). Before this round nothing kept the
// queue: the owner's agents.stop(runId) threw "custody retained", resetSession hung, and an actor stop dropped the
// queue. ActorManager.abandonStuckRun (agents.abandonStuckRun) is the owner's documented, queue-preserving way out.
const MINUTE = 60_000;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);
// Real-time waits: these tests skew Date.
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000): Promise<void> => {
  const until = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() > until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
/** About twenty 20 ms retention sweeps. */
const ticks = () => new Promise(resolve => setTimeout(resolve, 400));
const pending = <T>(promise: Promise<T>) => Promise.race([promise, new Promise(resolve => setTimeout(() => resolve("pending"), 50))]);
const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
const tempRoot = (prefix: string): string => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  roots.push(root);
  return root;
};
const stuckAlarms = (mesh: MeshStore, actorId: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
  .filter(event => event.kind === "actor-run-stuck" && (event.data as { actorId?: string }).actorId === actorId);
const droppedAlarms = (mesh: MeshStore, actorId: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
  .filter(event => event.kind === "actor-event-dropped" && (event.data as { actorId?: string }).actorId === actorId);
/** Records of the run failing for any unwind reason, or of queued work dropped. */
const failures = (actors: ActorManager, actorId: string, runId: string) => actors.messages(actorId, 200)
  .filter(message => message.error?.includes(runId) || /worker-gone|operator-abandoned|while messages were queued/.test(message.error ?? ""));

describe("abandonStuckRun: a run whose transport latched lost contact (smarty-dev#16333 round 5)", () => {
  // The round-4 review's probe, on the fixed code: a real ProcessTransport and AgentManager. The one injected fault is
  // the launched handle's lostContact() reporting the transport's own latched custody debt (process-utils
  // unconfirmed()). The worker then writes its completed result and exits with no descendant: its tree is gone.
  it("is stuck for hours before the call; after it MSG-1's real result is delivered once, MSG-2 runs once, nothing is dropped", async () => {
    const realNow = Date.now.bind(Date);
    const clock = { skew: 0 };
    vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
    const root = tempRoot("pi-fabric-abandon-lost-");
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
    /** What reaches the owner's Main: run results, and (separately) host notices. */
    const delivered: string[] = [];
    const notices: string[] = [];
    const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents,
      (event) => {
        if (event.message.source === "fabric-host") notices.push(event.message.text ?? "");
        else if (event.message.text) delivered.push(event.message.text);
      }, { actorRoot: path.join(root, "actors"), persistent: false, retentionSweepMs: 20 });
    const release = path.join(root, "release-worker");
    const latch = { armed: false };
    // Reverse order: free the worker and lift the injected latch first, so even a red run tears down.
    closers.push(() => agents.close(), () => actors.close(), async () => { latch.armed = false; fs.writeFileSync(release, ""); });
    const launch = ProcessTransport.prototype.launch;
    let latched = false;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, ...args) {
      const handle = await launch.apply(this, args);
      if (latched) return handle; // Only MSG-1's worker loses contact; MSG-2's launch is healthy.
      latched = true;
      const real = handle.lostContact?.bind(handle);
      handle.lostContact = () => latch.armed ? "Owned process worker did not confirm native close within 7000ms" : real?.();
      return handle;
    });
    const starts: string[] = [];
    const run = agents.run.bind(agents);
    vi.spyOn(agents, "run").mockImplementation((request, ...rest) => {
      starts.push(/MSG-\d/.exec(request.task)?.[0] ?? "unknown");
      return run(request, ...rest);
    });

    const actor = await actors.create({ name: "lost", instructions: "Run", coalesce: false, delivery: "followUp", triggerTurn: false });
    // Teardown hygiene for a red run: a drain still stuck would hold actors.close() (the review's probe hung there).
    closers.push(async () => {
      const stuck = actors.status(actor.id).inFlightRun?.id;
      const abandon = (actors as { abandonStuckRun?: ActorManager["abandonStuckRun"] }).abandonStuckRun;
      if (stuck && abandon) try { abandon.call(actors, actor.id, stuck); } catch { /* already unwound */ }
    });
    const answer = actors.ask(actor.id, "MSG-1 LIVE_WITH_PROGRESS", { fakeWorkerReleasePath: release })
      .then(message => ({ message }), (error: unknown) => ({ error }));
    await waitFor(() => actors.status(actor.id).inFlightRun !== undefined);
    const runId = actors.status(actor.id).inFlightRun!.id;
    const directory = agents.runDirectory(runId)!;
    await waitFor(() => fs.existsSync(path.join(directory, "status.json")));
    const worker = agents.workerIdentity(runId)!;
    latch.armed = true;
    fs.writeFileSync(release, "");
    await waitFor(() => !processIsAlive(worker.pid));
    await ticks(); await ticks();
    // The worker's genuine verdict is on disk, and AgentManager marked the run unresolved with the transport's reason:
    // what the operator reads before the call.
    expect(JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8")))
      .toMatchObject({ status: "completed", text: "live attempt 1 complete" });
    await waitFor(() => fs.existsSync(path.join(directory, "unresolved-worker.json")));
    expect(JSON.parse(fs.readFileSync(path.join(directory, "unresolved-worker.json"), "utf8")))
      .toMatchObject({ reason: expect.stringContaining("did not confirm native close"), runId });
    actors.tell(actor.id, "MSG-2");
    await ticks();
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });

    // Stuck BEFORE the call: past the threshold the check proves the worker gone and alarms, naming the remedy; for a
    // day of sweeps, far past every confirmation gap, nothing unwinds, because the manager cannot prove the tree gone.
    clock.skew = 31 * MINUTE;
    await waitFor(() => stuckAlarms(mesh, actor.id).length === 1);
    expect(stuckAlarms(mesh, actor.id)[0]!.data).toMatchObject({ reason: "worker-gone", runId, remedy: "abandonStuckRun" });
    for (const minutes of [33, 60, 120, 300, 24 * 60]) {
      clock.skew = minutes * MINUTE;
      await ticks(); await ticks();
      expect(starts).toEqual(["MSG-1"]);
      expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });
    }
    expect(await pending(answer)).toBe("pending");
    await expect(agents.executionExit(runId)).resolves.toEqual({ exited: false });
    expect(failures(actors, actor.id, runId)).toHaveLength(0);
    expect(delivered).toEqual([]);
    // The repeat alarm's notice to the owner's Main names the remedy with its ids.
    expect(notices.at(-1)).toContain(`Fabric host notice: actor-run-stuck; reason: worker-gone.`);
    expect(notices.at(-1)).toContain(`agents.abandonStuckRun({ id: ${JSON.stringify(actor.id)}, runId: ${JSON.stringify(runId)} }) ends that run and keeps the queue.`);

    // A wrong run id is refused and changes nothing.
    expect(() => actors.abandonStuckRun(actor.id, "f".repeat(32)))
      .toThrow(`did not abandon run ${"f".repeat(32)}: it is not the in-flight run (${runId}); nothing changed`);
    await ticks();
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });
    expect(starts).toEqual(["MSG-1"]);

    // The owner's call: the run's genuine result is delivered as its own, and the queue continues.
    const alarmsAtCall = stuckAlarms(mesh, actor.id).length;
    expect(actors.abandonStuckRun(actor.id, runId)).toMatchObject({ id: actor.id, queued: 1 });
    expect(await answer).toMatchObject({ message: { direction: "out", text: "live attempt 1 complete" } });
    await waitFor(() => starts.length === 2 && actors.status(actor.id).queued === 0 && !actors.inFlightActorIds().includes(actor.id));
    expect(starts).toEqual(["MSG-1", "MSG-2"]);
    expect(actors.status(actor.id).inFlightRun).toBeUndefined();
    expect(delivered).toEqual(["live attempt 1 complete", "fake worker complete"]);
    const outs = actors.messages(actor.id, 200).filter(message => message.direction === "out");
    expect(outs.filter(message => message.text === "live attempt 1 complete")).toHaveLength(1);
    expect(outs.filter(message => message.text === "fake worker complete")).toHaveLength(1);
    // Nothing dropped or failed: no failure record for the run and no dropped-event alarm.
    expect(failures(actors, actor.id, runId)).toHaveLength(0);
    expect(droppedAlarms(mesh, actor.id)).toHaveLength(0);

    // A second call is a clean no-op error.
    expect(() => actors.abandonStuckRun(actor.id, runId))
      .toThrow(`did not abandon run ${runId}: the actor has no run in flight; nothing changed`);
    // No loop: more sweeps, hours later, add no launch, delivery or alarm.
    for (const minutes of [25 * 60, 30 * 60]) {
      clock.skew = minutes * MINUTE;
      await ticks();
    }
    expect(starts).toEqual(["MSG-1", "MSG-2"]);
    expect(delivered).toHaveLength(2);
    expect(stuckAlarms(mesh, actor.id).length).toBe(alarmsAtCall);
  }, 90_000);
});

/**
 * The incident's handle, as in tests/actor-stuck-run-unwind.test.ts: agents.run is a fake whose first handle never
 * settles and that AgentManager never registered (agents.stop answered "Unknown Fabric participant"), so the run has
 * no record and no result. The registry is persistent, so a second manager on it can be a non-owner.
 */
const stubbed = async () => {
  const root = tempRoot("pi-fabric-abandon-gates-");
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
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
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const actors = new ActorManager("test", identity, mesh, meshConfig, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent: true });
  closers.push(() => agents.close(), () => actors.close(), async () => releasePhantom?.());
  const actor = await actors.create({ name: "stuck", instructions: "Run", coalesce: false, delivery: "followUp", triggerTurn: false });
  const answer = actors.ask(actor.id, "MSG-1").then(message => ({ message }), (error: unknown) => ({ error }));
  await waitFor(() => phantom !== undefined && actors.status(actor.id).inFlightRun?.id === phantom);
  actors.tell(actor.id, "MSG-2");
  expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: phantom } });
  /** Nothing changed: the phantom still in flight, MSG-2 still queued and not launched, the ask still pending. */
  const unchanged = async () => {
    await ticks();
    expect(actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: phantom } });
    expect(launches).toEqual(["MSG-1"]);
    expect(await pending(answer)).toBe("pending");
  };
  return { root, mesh, agents, actors, meshConfig, actor, answer, launches, unchanged, phantom: phantom!, release: () => releasePhantom?.() };
};

describe("abandonStuckRun: the gates, and a run with no result (smarty-dev#16333 round 5)", () => {
  it("refuses a wrong run id, an unknown actor and a non-owner, changing nothing; the owner's call fails the run operator-abandoned and keeps the queue", async () => {
    const s = await stubbed();
    expect(() => s.actors.abandonStuckRun(s.actor.id, "0".repeat(32)))
      .toThrow(`did not abandon run ${"0".repeat(32)}: it is not the in-flight run (${s.phantom}); nothing changed`);
    // Only the exact run id: a prefix of the in-flight run is not it.
    expect(() => s.actors.abandonStuckRun(s.actor.id, s.phantom.slice(0, 8))).toThrow("it is not the in-flight run");
    expect(() => s.actors.abandonStuckRun("no-such-actor", s.phantom)).toThrow("Unknown Fabric actor");
    await s.unchanged();
    // A non-owner: another host on the same registry, whose participant directory names a live foreign owner.
    const peerIdentity: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
    const peer = new ActorManager("peer", peerIdentity, s.mesh, s.meshConfig, s.agents, () => {}, {
      actorRoot: path.join(s.root, "actors"), persistent: true, claimResidency: "session", rootId: peerIdentity.id,
      canManageActor: () => false });
    closers.push(() => peer.close());
    expect(peer.owns(s.actor.id)).toBe(false);
    expect(() => peer.abandonStuckRun(s.actor.id, s.phantom)).toThrow(`Fabric actor is owned by another host: ${s.actor.id}`);
    await s.unchanged();

    // A reset requested meanwhile waits for the stuck run (the incident: resetSession hung).
    const reset = s.actors.resetSession(s.actor.id).then(() => "reset", (error: unknown) => String(error));
    expect(await pending(reset)).toBe("pending");

    // The owner's call on a run with no record (never registered by AgentManager): it fails operator-abandoned.
    s.actors.abandonStuckRun(s.actor.id, s.phantom);
    expect(await s.answer).toMatchObject({ error: expect.objectContaining({
      message: expect.stringContaining(`Fabric actor run ${s.phantom} failed: its owner abandoned it (operator-abandoned)`) }) });
    await waitFor(() => s.launches.length === 2 && s.actors.status(s.actor.id).queued === 0 &&
      !s.actors.inFlightActorIds().includes(s.actor.id));
    expect(s.launches).toEqual(["MSG-1", "MSG-2"]);
    // The pending reset completed at the activation boundary the call freed.
    expect(await reset).toBe("reset");
    const records = s.actors.messages(s.actor.id, 200).filter(message => message.error?.includes(s.phantom));
    expect(records).toHaveLength(1);
    expect(records[0]).toMatchObject({ runId: s.phantom, data: { runId: s.phantom, reason: "operator-abandoned" } });
    // MSG-2 ran once and was recorded once; nothing was dropped.
    expect(s.actors.messages(s.actor.id, 200).filter(message => message.text?.startsWith("done "))).toHaveLength(1);
    expect(s.actors.messages(s.actor.id, 200).filter(message => /while messages were queued/.test(message.error ?? ""))).toHaveLength(0);
    expect(() => s.actors.abandonStuckRun(s.actor.id, s.phantom)).toThrow("the actor has no run in flight; nothing changed");
    // A late settle of the abandoned handle is ignored: no second record, no relaunch.
    s.release();
    await ticks();
    expect(s.launches).toEqual(["MSG-1", "MSG-2"]);
    expect(s.actors.messages(s.actor.id, 200).filter(message => message.text === `done ${s.phantom}`)).toHaveLength(0);
  }, 60_000);

  it("refuses while its host is closing, changing nothing", async () => {
    const s = await stubbed();
    const closing = s.actors.close();
    expect(() => s.actors.abandonStuckRun(s.actor.id, s.phantom))
      .toThrow(`did not abandon run ${s.phantom}: this host is closing; nothing changed`);
    expect(s.actors.status(s.actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: s.phantom } });
    expect(s.launches).toEqual(["MSG-1"]);
    s.release();
    await closing;
    expect(s.launches).toEqual(["MSG-1"]);
  }, 60_000);
});
