import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmKey } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import * as processIdentity from "../src/residency/process-identity.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import type { FabricActorDeliveryRequest } from "../src/actors/types.js";
import { processIsAlive } from "../src/agents/transports/process-utils.js";

// smarty-dev#9332: actor states that cannot act raise fabric.alarm.actor-activation (same envelope, ownerRoot routing),
// evaluated on the existing retention sweep (retentionSweepMs, no new timer); repeat policy depends on alarm kind.
const MINUTE = 60_000, HOUR = 60 * MINUTE;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  // Close under the test's clock: a deadline taken while it was skewed must not land in the future.
  for (const close of closers.splice(0).reverse()) await close().catch(() => undefined);
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
}, 60_000);
const waitFor = async (predicate: () => boolean, timeoutMs = 15_000): Promise<void> => {
  const until = performance.now() + timeoutMs;
  while (!predicate()) {
    if (performance.now() > until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const settle = () => new Promise(resolve => setTimeout(resolve, 250)); // about a dozen 20 ms sweeps

const setup = (persistent = false) => {
  const realNow = Date.now.bind(Date);
  const clock = { skew: 0, now: undefined as number | undefined };
  vi.spyOn(Date, "now").mockImplementation(() => clock.now ?? realNow() + clock.skew);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-alarms-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  const onDeliver = vi.fn<(request: FabricActorDeliveryRequest) => void | boolean | Promise<void | boolean>>();
  let actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, onDeliver, {
    actorRoot: path.join(root, "actors"), persistent, retentionSweepMs: 20 });
  closers.push(() => agents.close(), () => actors.close());
  const alarms = (kind: string, id: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
    .filter(event => event.kind === kind && (event.data as { actorId?: string }).actorId === id);
  const hostNotices = (id: string) => onDeliver.mock.calls.map(([request]) => request).filter(request =>
    request.actor.id === id && request.message.text?.includes("actor-stopped") === true);
  /** A new manager on the same actor root and mesh (a host restart). */
  const restart = async (beforeReload?: () => void) => {
    await actors.close();
    beforeReload?.();
    actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, onDeliver, {
      actorRoot: path.join(root, "actors"), persistent, retentionSweepMs: 20 });
    return actors;
  };
  return { root, mesh, agents, get actors() { return actors; }, clock, alarms, onDeliver, hostNotices, restart, deliveries: onDeliver };
};

describe("actor state alarms (smarty-dev#9332)", () => {
  it("actor-stopped: one mesh alarm and Main notice at 10m, 15m, and >6h for one unchanged stop", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "stopped", instructions: "Run", residency: "durable" });
    const other = await s.actors.create({ name: "running", instructions: "Run", residency: "durable" });
    await s.actors.stop(actor.id);
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(0);
    expect(s.hostNotices(actor.id)).toHaveLength(0);
    s.clock.skew = 10 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 1);
    await settle();
    expect(s.hostNotices(actor.id)).toHaveLength(1);
    expect(s.hostNotices(actor.id)[0]).toMatchObject({ delivery: "followUp", triggerTurn: true,
      message: { source: "fabric-host" } });
    expect(s.alarms("actor-stopped", actor.id)[0]).toMatchObject({ data: { actorId: actor.id, actorName: "stopped",
      ownerRoot: "session:test", reason: "stop", stopReason: "explicit", residency: "durable", stoppedForMs: expect.any(Number) } });
    const firstSince = (s.alarms("actor-stopped", actor.id)[0]!.data as { since: number }).since;
    const alarmKey = actorAlarmKey(actor.id, "actor-stopped");

    const registryFile = path.join(s.root, "actors", "actors.json");
    const restarted = await s.restart();
    expect(restarted.status(actor.id).status).toBe("stopped");
    const saved = JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> };
    expect(saved.actors.find(row => row.id === actor.id)?.stoppedAt).toBe(firstSince);
    s.clock.skew = 15 * MINUTE + 1_000;
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(1);

    // Delete only the temporary claim, not the actor registry or the mesh publication's dedupe receipt.
    await s.mesh.delete({ key: alarmKey });
    expect(s.mesh.get(alarmKey)).toBeUndefined();
    const afterClaimRemoval = await s.restart();
    expect(afterClaimRemoval.status(actor.id).status).toBe("stopped");
    s.clock.skew = 6 * HOUR + 15 * MINUTE + 1_000;
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(1);
    expect(s.hostNotices(actor.id)).toHaveLength(1);
    // The actor registry, not a surviving mesh claim or receipt, remembers the notified episode.
    expect(s.mesh.get(alarmKey)).toBeUndefined();
    const notified = JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> };
    expect(notified.actors.find(row => row.id === actor.id)?.stoppedAlarmSince).toBe(firstSince);
    expect(s.alarms("actor-stopped", other.id)).toHaveLength(0);
    expect(s.hostNotices(other.id)).toHaveLength(0);
  }, 60_000);

  it("actor-stopped: a fresh persisted stoppedAt for the same actor publishes and notifies again; removal stays terminal", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "stopped-again", instructions: "Run", residency: "durable" });
    await s.actors.stop(actor.id);
    s.clock.skew = 10 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 1);
    await settle();
    const firstSince = (s.alarms("actor-stopped", actor.id)[0]!.data as { since: number }).since;

    // A public stop is terminal; model a new active episode through this fixture's persisted actor row,
    // then use the public stop API to establish a distinct stoppedAt for the same actor id.
    const registryFile = path.join(s.root, "actors", "actors.json");
    const fresh = await s.restart(() => {
      const registry = JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> };
      const row = registry.actors.find(entry => entry.id === actor.id)!;
      row.status = "idle";
      delete row.stoppedAt;
      delete row.stopReason;
      row.updatedAt = Date.now();
      fs.writeFileSync(registryFile, JSON.stringify(registry));
    });
    expect(fresh.status(actor.id).status).toBe("idle");
    await fresh.stop(actor.id);
    s.clock.skew += 10 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 2);
    await settle();
    const secondSince = (s.alarms("actor-stopped", actor.id)[1]!.data as { since: number }).since;
    expect(secondSince).toBeGreaterThan(firstSince);
    expect(s.hostNotices(actor.id)).toHaveLength(2);

    const alarmKey = actorAlarmKey(actor.id, "actor-stopped");
    const alarm = s.mesh.get(alarmKey);
    const sentinel = path.join(path.dirname(actor.sessionFile!), "kept-11335.txt");
    if (process.platform !== "linux" && process.platform !== "darwin" && process.platform !== "win32") {
      expect(alarm).toBeDefined();
      fs.writeFileSync(sentinel, "history\n");
    }
    const removal = await fresh.remove(actor.id);
    if (process.platform === "linux" || process.platform === "darwin" || process.platform === "win32") {
      expect(s.mesh.get(alarmKey)).toBeUndefined();
    } else {
      // smarty-dev#8090 / follow-up smarty-dev#11335: on unsupported platforms registry revocation commits,
      // but tree deletion refuses before alarm-key cleanup. Cleanup stays pending; no new alarms are eligible.
      expect(removal).toEqual({ removed: true, cleaned: false,
        pending: `removal cleanup failed: Root actor tree deletion needs the Main publication fence, which this platform (${process.platform}) lacks (smarty-dev#8090); follow-up smarty-dev#11335; the root session:test actor tree is kept (a later remove or host start finishes it)` });
      expect(fs.readFileSync(sentinel, "utf8")).toBe("history\n");
      expect(s.mesh.get(alarmKey)).toEqual(alarm);
    }
    s.clock.skew += 6 * HOUR;
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(2);
    expect(s.hostNotices(actor.id)).toHaveLength(2);
  }, 60_000);

  it("actor-stopped: restarting past 6h keeps one mesh alarm and one Main notice for the persisted stop", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "restart-stopped", instructions: "Run", residency: "durable" });
    await s.actors.stop(actor.id);
    s.clock.skew = 10 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 1);
    await settle();
    const firstSince = (s.alarms("actor-stopped", actor.id)[0]!.data as { since: number }).since;

    const restarted = await s.restart(() => { s.clock.skew = 6 * HOUR + 15 * MINUTE + 1_000; });
    expect(restarted.status(actor.id).status).toBe("stopped");
    const saved = JSON.parse(fs.readFileSync(path.join(s.root, "actors", "actors.json"), "utf8")) as {
      actors: Array<Record<string, unknown>>;
    };
    expect(saved.actors.find(row => row.id === actor.id)?.stoppedAt).toBe(firstSince);
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(1);
    expect(s.hostNotices(actor.id)).toHaveLength(1);
  }, 60_000);

  it("actor-stopped: a publish-before-delivery crash replays the episode until Main confirms, then stays silent", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "crash-stopped", instructions: "Run", residency: "durable" });
    await s.actors.stop(actor.id);
    const publish = s.mesh.publish.bind(s.mesh);
    let interrupted = false;
    const fault = vi.spyOn(s.mesh, "publish").mockImplementation(async input => {
      const event = await publish(input);
      if (input.kind === "actor-stopped" && !interrupted) {
        interrupted = true;
        throw new Error("crashed after alarm publication, before onDeliver");
      }
      return event;
    });
    let confirmed = false;
    s.onDeliver.mockImplementation(() => confirmed);
    s.clock.skew = 10 * MINUTE + 1_000;
    await waitFor(() => interrupted && s.alarms("actor-stopped", actor.id).length === 1);
    fault.mockRestore();
    await waitFor(() => s.hostNotices(actor.id).length > 0);
    const registryFile = path.join(s.root, "actors", "actors.json");
    const row = () => (JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> })
      .actors.find(entry => entry.id === actor.id)!;
    expect(row().stoppedAlarmSince).toBeUndefined(); // publication and pending custody are not a Main acknowledgement
    const firstId = s.hostNotices(actor.id)[0]!.message.id;
    confirmed = true;
    await waitFor(() => row().stoppedAlarmSince === row().stoppedAt);
    expect(s.hostNotices(actor.id).every(request => request.message.id === firstId)).toBe(true);
    const admitted = s.hostNotices(actor.id).length;
    await s.restart(() => { s.clock.skew += 6 * HOUR; });
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(1);
    expect(s.hostNotices(actor.id)).toHaveLength(admitted);
  }, 60_000);

  it("a legacy stopped actor keeps one alarm episode across idle-exit registry rewrites", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "legacy-stop", instructions: "Run", responseMode: "directive", residency: "durable" });
    s.clock.skew = -11 * MINUTE;
    const stop = await s.actors.ask(actor.id, "STOP_DIRECTIVE");
    expect(stop.action).toBe("stop");
    const stopAt = stop.createdAt;
    await s.actors.close();

    const registryFile = path.join(s.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> };
    const legacy = registry.actors.find(row => row.id === actor.id)!;
    delete legacy.stoppedAt;
    legacy.updatedAt = stopAt + 2 * MINUTE;
    fs.writeFileSync(registryFile, JSON.stringify(registry));
    s.clock.skew = 0;

    const firstReload = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" },
      s.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, s.agents, () => {}, {
        actorRoot: path.join(s.root, "actors"), persistent: true, retentionSweepMs: 20 });
    closers.push(() => firstReload.close());
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 1);
    await waitFor(() => {
      const saved = JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> };
      return saved.actors.find(row => row.id === actor.id)?.stoppedAt === stopAt;
    });

    await firstReload.close(); // the idle-exit rewrite advances updatedAt but keeps the migrated episode
    const rewritten = JSON.parse(fs.readFileSync(registryFile, "utf8")) as { actors: Array<Record<string, unknown>> };
    const rewrittenActor = rewritten.actors.find(row => row.id === actor.id)!;
    expect(rewrittenActor.stoppedAt).toBe(stopAt);
    expect(rewrittenActor.updatedAt).toBeGreaterThan(stopAt);

    s.clock.skew = 11 * MINUTE;
    const secondReload = new ActorManager("test", { id: "session:test", name: "main", kind: "main", sessionId: "test" },
      s.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, s.agents, () => {}, {
        actorRoot: path.join(s.root, "actors"), persistent: true, retentionSweepMs: 20 });
    closers.push(() => secondReload.close());
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(1);
    expect(secondReload.status(actor.id).status).toBe("stopped");
  }, 60_000);

  it("actor-event-dropped: one dropped callerless event alarms at once; more drops in the episode do not; 6 h later it re-alarms; a completed run ends it", async () => {
    const s = setup();
    const actor = await s.actors.create({ name: "dropper", instructions: "Respond.", responseMode: "directive", coalesce: false });
    await s.actors.tell(actor.id, "FAIL_DIRECTIVE one");
    await waitFor(() => s.alarms("actor-event-dropped", actor.id).length === 1);
    expect(s.alarms("actor-event-dropped", actor.id)[0]).toMatchObject({ data: { actorId: actor.id, ownerRoot: "session:test",
      count: 1, reason: expect.any(String), eventRef: expect.objectContaining({ source: expect.any(String) }), pendingEffects: "reconcile-required" } });
    // Fewer than three failures: the streak alarm has not fired; the drop alarm did.
    expect(s.alarms("actor-activation-blocked", actor.id)).toHaveLength(0);
    await s.actors.tell(actor.id, "FAIL_DIRECTIVE two");
    await waitFor(() => !s.actors.status(actor.id).inFlightRun && s.actors.status(actor.id).queued === 0);
    await settle();
    expect(s.alarms("actor-event-dropped", actor.id)).toHaveLength(1);
    s.clock.skew = 6 * HOUR + 1_000;
    await waitFor(() => s.alarms("actor-event-dropped", actor.id).length === 2);
    expect(s.alarms("actor-event-dropped", actor.id)[1]!.data).toMatchObject({ count: 2 });
    await s.actors.ask(actor.id, "all good");
    s.clock.skew += 6 * HOUR;
    await settle();
    expect(s.alarms("actor-event-dropped", actor.id)).toHaveLength(2);
  }, 60_000);

  it("an ask (a caller hears the failure) is not a dropped event", async () => {
    const s = setup();
    const actor = await s.actors.create({ name: "asked", instructions: "Respond.", responseMode: "directive" });
    await s.actors.ask(actor.id, "FAIL_DIRECTIVE");
    await settle();
    expect(s.alarms("actor-event-dropped", actor.id)).toHaveLength(0);
  }, 60_000);

  it("unknown block: one failure with code unknown alarms once it is 30 min old, not after three failures", async () => {
    const s = setup();
    const actor = await s.actors.create({ name: "unknown", instructions: "Respond.", responseMode: "directive" });
    await s.actors.ask(actor.id, "FAIL_DIRECTIVE");
    expect(s.actors.status(actor.id).activationBlocked).toMatchObject({ code: "unknown", count: 1 });
    await settle();
    expect(s.alarms("actor-activation-blocked", actor.id)).toHaveLength(0);
    s.clock.skew = 30 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-activation-blocked", actor.id).length === 1);
    expect(s.alarms("actor-activation-blocked", actor.id)[0]!.data).toMatchObject({ code: "unknown", count: 1, early: true, ownerRoot: "session:test" });
    s.clock.skew = 2 * HOUR - MINUTE;
    await settle();
    expect(s.alarms("actor-activation-blocked", actor.id)).toHaveLength(1);
    s.clock.skew = 2 * HOUR + MINUTE; // then the #7782 repeats, every 6 h
    await waitFor(() => s.alarms("actor-activation-blocked", actor.id).length === 2);
    expect(s.alarms("actor-activation-blocked", actor.id)[1]!.data).toMatchObject({ repeat: true });
  }, 60_000);

  // Keep: #1695's real false-alarm catch and the data-safety boundary. Missing directory/status/host verification
  // still warn after 30 min, but NEVER prove death or admit NEXT/retry. Worker-gone/reused also need exact native
  // exit proof older than 5 s and unfinished work; worker-writable status is only an untrusted hint (OPTION 1).
  // Two real hanging runs: "stuck" gets the fault, "live" stays healthy (the counterexample, never alarmed).
  const stuckFixture = async () => {
    const s = setup();
    const launches = vi.spyOn(ProcessTransport.prototype, "launch");
    const stuck = await s.actors.create({ name: "stuck", instructions: "Run", coalesce: false });
    const live = await s.actors.create({ name: "live", instructions: "Run", coalesce: false });
    await s.actors.tell(stuck.id, "HANG_WITH_PROGRESS stuck");
    await s.actors.tell(live.id, "HANG_WITH_PROGRESS live");
    await waitFor(() => !!s.actors.status(stuck.id).inFlightRun && !!s.actors.status(live.id).inFlightRun);
    const runId = s.actors.status(stuck.id).inFlightRun!.id;
    const directory = s.agents.runDirectory(runId)!;
    await waitFor(() => fs.existsSync(path.join(directory, "status.json")) &&
      fs.existsSync(path.join(s.agents.runDirectory(s.actors.status(live.id).inFlightRun!.id)!, "status.json")));
    const owned = s.agents.workerIdentity(runId)!;
    expect(owned).toMatchObject({ pid: expect.any(Number), open: true });
    await s.actors.tell(stuck.id, "NEXT must stay queued");
    const stuckAlarms = () => s.alarms("actor-run-stuck", stuck.id);
    const expectNoLossOrNext = () => {
      expect(s.actors.status(stuck.id)).toMatchObject({ status: "running", queued: 1, inFlightRun: { id: runId } });
      expect(s.agents.actorRunUnfinished(runId, stuck.id)).toBe(true);
      expect(s.agents.actorRunLost(runId)).toBe(false);
      expect(processIsAlive(owned.pid)).toBe(true);
      expect(launches).toHaveBeenCalledTimes(2); // only the two originals: no NEXT or retry
    };
    const teardown = async () => {
      // Stop actor admission BEFORE ending its worker, so teardown cannot itself admit the queued NEXT.
      for (const id of [stuck.id, live.id]) {
        const inFlight = s.actors.status(id).inFlightRun?.id;
        await s.actors.stop(id).catch(() => undefined);
        if (inFlight) await s.agents.stop(inFlight).catch(() => undefined);
      }
    };
    /** Past the threshold, the stuck run alarms once with `reason`; the healthy run never does. */
    const expectAlarm = async (reason: string, thresholdMs = 30 * MINUTE) => {
      s.clock.skew = 29 * MINUTE;
      await settle();
      expect(stuckAlarms()).toHaveLength(0);
      expectNoLossOrNext();
      s.clock.skew = 31 * MINUTE;
      await waitFor(() => stuckAlarms().length === 1);
      const data = stuckAlarms()[0]!.data as Record<string, unknown>;
      expect(data).toMatchObject({ runId, reason, ownerRoot: "session:test", thresholdMs });
      await settle();
      expect(stuckAlarms()).toHaveLength(1);
      expect(s.alarms("actor-run-stuck", live.id)).toHaveLength(0);
      expectNoLossOrNext();
      return data;
    };
    return { s, stuck, runId, directory, owned, teardown, expectAlarm, stuckAlarms, expectNoLossOrNext };
  };
  // The identity mock changes only the warning's reason. Supply the SEPARATE exact run/actor native evidence
  // explicitly; it becomes available at 31 min, so the short fallback then owns slot 0 (15 s runtime threshold).
  // Its real live transport still forbids any synthetic loss, which expectNoLossOrNext checks.
  const mockNativeProof = (f: Awaited<ReturnType<typeof stuckFixture>>) => {
    const observeExit = f.s.agents.actorWorkerExit.bind(f.s.agents);
    let exitedAt: number | undefined;
    return vi.spyOn(f.s.agents, "actorWorkerExit").mockImplementation((runId, actorId) => {
      if (runId !== f.runId || actorId !== f.stuck.id) return observeExit(runId, actorId);
      if (f.s.clock.skew < 31 * MINUTE) return undefined;
      exitedAt ??= Date.now() - 5_001;
      return { exitedAt, unfinished: true };
    });
  };
  // The dead worker of (a), and (c)'s live process with a different start time: real processes, not invented pids.
  const deadPid = async (): Promise<number> => {
    const { spawn } = await import("node:child_process");
    const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
    await new Promise(resolve => child.once("exit", resolve));
    return child.pid!;
  };

  const holdWarning = (s: ReturnType<typeof setup>, actorId: string, boundary: "claim" | "publish") => {
    const key = actorAlarmKey(actorId, "actor-run-stuck");
    let reached = false;
    let release!: () => void;
    const gate = new Promise<void>(resolve => { release = resolve; });
    closers.push(async () => { release(); });
    if (boundary === "claim") {
      const writeBatch = s.mesh.writeBatch.bind(s.mesh);
      vi.spyOn(s.mesh, "writeBatch").mockImplementation(async request => {
        const results = await writeBatch(request);
        if (!reached && results.some(result => result.key === key && result.applied)) {
          reached = true;
          await gate;
        }
        return results;
      });
    } else {
      const publish = s.mesh.publish.bind(s.mesh);
      vi.spyOn(s.mesh, "publish").mockImplementation(async input => {
        const event = await publish(input);
        if (!reached && input.kind === "actor-run-stuck" && (input.data as { actorId?: string })?.actorId === actorId) {
          reached = true;
          await gate;
        }
        return event;
      });
    }
    return { reached: () => reached, release };
  };

  it.each([undefined, 0, 5_000])("worker-gone/reused identity alone cannot alarm or advance work without old exact native proof (exit age %s)", async age => {
    const f = await stuckFixture();
    const pid = await deadPid();
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    let identity = { pid, startTime: "1", open: true };
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? identity : real(id));
    f.s.clock.now = Date.now() + 31 * MINUTE; // freeze the strict 0/5000 ms boundary, not just the run's age
    const observeExit = f.s.agents.actorWorkerExit.bind(f.s.agents);
    vi.spyOn(f.s.agents, "actorWorkerExit").mockImplementation((runId, actorId) =>
      runId === f.runId && actorId === f.stuck.id
        ? age === undefined ? undefined : { exitedAt: Date.now() - age, unfinished: true }
        : observeExit(runId, actorId));
    try {
      await settle();
      expect(f.stuckAlarms()).toHaveLength(0);
      f.expectNoLossOrNext();
      if (process.platform === "linux") {
        identity = { pid: process.pid, startTime: "1", open: true }; // positively live PID, wrong kernel birth
        await settle();
        expect(f.stuckAlarms()).toHaveLength(0);
        f.expectNoLossOrNext();
      }
    } finally {
      f.s.clock.now = undefined;
      f.s.clock.skew = 0;
      vi.mocked(f.s.agents.actorWorkerExit).mockRestore();
      vi.mocked(f.s.agents.workerIdentity).mockRestore();
      await f.teardown();
    }
  }, 60_000);

  it.each(["claim", "publish"] as const)("a genuine completion suppresses a pending unverified warning after %s, even with the SAME in-flight run id", async boundary => {
    const s = setup();
    const actor = await s.actors.create({ name: "warning-completes", instructions: "Respond.", coalesce: false });
    const finish = path.join(s.root, "finish-original");
    const launches = vi.spyOn(ProcessTransport.prototype, "launch");
    const run = s.agents.run.bind(s.agents);
    let completed: Awaited<ReturnType<typeof run>> | undefined;
    let releaseRun!: () => void;
    const runGate = new Promise<void>(resolve => { releaseRun = resolve; });
    closers.push(async () => { fs.writeFileSync(finish, "finish"); releaseRun(); });
    vi.spyOn(s.agents, "run").mockImplementation(async (...args) => {
      const result = await run(...args);
      if (!completed) { completed = result; await runGate; }
      return result;
    });
    await s.actors.tell(actor.id, "complete only on release", { fakeWorkerFinishGate: finish });
    await waitFor(() => !!s.actors.status(actor.id).inFlightRun && fs.existsSync(`${finish}.entered`));
    const runId = s.actors.status(actor.id).inFlightRun!.id;
    const owned = s.agents.workerIdentity(runId)!;
    expect(owned.open).toBe(true);
    const workerIdentity = s.agents.workerIdentity.bind(s.agents);
    vi.spyOn(s.agents, "workerIdentity").mockImplementation(id => id === runId ? undefined : workerIdentity(id));
    // Keep native reconciliation out of this WARNING interleaving, including after real success.
    const observeExit = s.agents.actorWorkerExit.bind(s.agents);
    vi.spyOn(s.agents, "actorWorkerExit").mockImplementation((id, actorId) =>
      id === runId && actorId === actor.id ? undefined : observeExit(id, actorId));
    await s.actors.tell(actor.id, "NEXT after real completion");
    const held = holdWarning(s, actor.id, boundary);
    s.clock.skew = 31 * MINUTE;
    await waitFor(held.reached);
    const alarms = () => s.alarms("actor-run-stuck", actor.id);
    expect(s.agents.actorRunUnfinished(runId, actor.id)).toBe(true);
    expect(processIsAlive(owned.pid)).toBe(true);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });
    expect(launches).toHaveBeenCalledTimes(1);
    expect(s.agents.actorRunLost(runId)).toBe(false);
    if (boundary === "publish") expect(alarms()[0]!.data).toMatchObject({ reason: "worker-unverified", thresholdMs: 30 * MINUTE });

    fs.writeFileSync(finish, "finish"); // the actual worker writes its completed result, not a forged status
    await waitFor(() => completed?.id === runId);
    expect(completed!.status).toBe("completed");
    expect(s.agents.actorRunUnfinished(runId, actor.id)).toBe(false);
    expect(s.actors.status(actor.id)).toMatchObject({ queued: 1, inFlightRun: { id: runId } });
    held.release();
    await settle();
    // An event already committed before success is not retracted; its stale Main wake MUST be suppressed.
    expect(alarms()).toHaveLength(boundary === "publish" ? 1 : 0);
    expect(s.deliveries.mock.calls.filter(([request]) => request.triggerTurn && request.message.text?.includes("actor-run-stuck"))).toHaveLength(0);
    expect(launches).toHaveBeenCalledTimes(1);
    expect(s.agents.actorRunLost(runId)).toBe(false);
    releaseRun();
    await waitFor(() => launches.mock.calls.length === 2 && s.actors.status(actor.id).status === "idle");
    expect(launches).toHaveBeenCalledTimes(2); // exactly one NEXT, never a replay
  }, 60_000);

  it("strict owner release suppresses a claimed unknown warning without stopping its live run or admitting NEXT", async () => {
    const f = await stuckFixture();
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? undefined : real(id));
    const held = holdWarning(f.s, f.stuck.id, "claim");
    try {
      f.s.clock.skew = 31 * MINUTE;
      await waitFor(held.reached);
      f.expectNoLossOrNext();
      f.s.actors.pauseForRelease({ strict: true });
      held.release();
      await settle();
      expect(f.stuckAlarms()).toHaveLength(0);
      expect(f.s.deliveries.mock.calls.filter(([request]) => request.triggerTurn && request.message.text?.includes("actor-run-stuck"))).toHaveLength(0);
      f.expectNoLossOrNext();
    } finally { held.release(); vi.mocked(f.s.agents.workerIdentity).mockRestore(); await f.teardown(); }
  }, 60_000);

  it("actor-run-stuck (d): a missing run directory alarms with the generic run-dir-missing reason and no path", async () => {
    const f = await stuckFixture();
    fs.renameSync(f.directory, `${f.directory}.gone`);
    try {
      const data = await f.expectAlarm("run-dir-missing");
      expect(JSON.stringify(data)).not.toContain(path.dirname(f.directory));
    } finally { fs.renameSync(`${f.directory}.gone`, f.directory); await f.teardown(); }
  }, 60_000);

  it("actor-run-stuck (a): a forged running status.json does not hide a dead owned worker", async () => {
    const f = await stuckFixture();
    const pid = await deadPid();
    // The legacy identity mock alone is NOT native death authority; pair it with exact native proof.
    const nativeProof = mockNativeProof(f);
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? { pid, startTime: "1", open: true } : real(id));
    const status = JSON.parse(fs.readFileSync(path.join(f.directory, "status.json"), "utf8"));
    fs.writeFileSync(path.join(f.directory, "status.json"), JSON.stringify({ ...status, status: "running", sessionId: String(process.pid) }));
    try {
      const data = await f.expectAlarm("worker-gone", 15_000);
      expect(data.workerStatusHint).toEqual({ untrusted: true, status: "running" });
    } finally { nativeProof.mockRestore(); vi.mocked(f.s.agents.workerIdentity).mockRestore(); await f.teardown(); }
  }, 60_000);

  it("actor-run-stuck (b): a malformed status.json past the threshold alarms as worker-unverified", async () => {
    const f = await stuckFixture();
    fs.writeFileSync(path.join(f.directory, "status.json"), "{\"status\":\"runn");
    try { await f.expectAlarm("worker-unverified"); } finally { await f.teardown(); }
  }, 60_000);

  // A kernel start time exists only where processStartTime reads /proc (Linux, its own platform check). Off Linux there is
  // nothing to compare, and the run is judged by the open owned handle ("no start time (as off Linux)" below).
  it.skipIf(process.platform !== "linux")("actor-run-stuck (c): a live pid whose start time is not the recorded one (pid reuse) alarms", async () => {
    const f = await stuckFixture();
    // This process is alive, but it is not the worker the host launched: its start time differs.
    const nativeProof = mockNativeProof(f);
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? { pid: process.pid, startTime: "1", open: true } : real(id));
    try { await f.expectAlarm("worker-pid-reused", 15_000); }
    finally { nativeProof.mockRestore(); vi.mocked(f.s.agents.workerIdentity).mockRestore(); await f.teardown(); }
  }, 60_000);

  it("actor-run-stuck: an absent status record inside the startup window gives no alarm; past the threshold it alarms", async () => {
    const f = await stuckFixture();
    fs.rmSync(path.join(f.directory, "status.json"));
    try {
      await settle(); // a fresh run, no status record yet
      expect(f.stuckAlarms()).toHaveLength(0);
      await f.expectAlarm("worker-unverified");
    } finally { await f.teardown(); }
  }, 60_000);

  // pi-fabric#944 round 2.
  it("no start time (as off Linux): a live owned handle is proof, no alarm; with the handle closed and the pid alive: worker-unverified", async () => {
    vi.spyOn(processIdentity, "processStartTime").mockReturnValue(undefined); // before the spawn: none is recorded
    const f = await stuckFixture();
    try {
      expect(f.owned.startTime).toBeUndefined();
      f.s.clock.skew = 31 * MINUTE;
      await settle();
      expect(f.stuckAlarms()).toHaveLength(0); // the live worker of an open owned handle
      const real = f.s.agents.workerIdentity.bind(f.s.agents);
      vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => {
        const identity = real(id);
        return id === f.runId && identity ? { ...identity, open: false } : identity; // the handle closed, the pid still alive
      });
      await waitFor(() => f.stuckAlarms().length === 1);
      expect(f.stuckAlarms()[0]!.data).toMatchObject({ reason: "worker-unverified", runId: f.runId, thresholdMs: 30 * MINUTE });
      f.expectNoLossOrNext();
      vi.mocked(f.s.agents.workerIdentity).mockRestore();
    } finally { await f.teardown(); }
  }, 60_000);

  it("a dropped-event episode survives a restart: the next slot re-alarms once from the new manager", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "dropper", instructions: "Respond.", responseMode: "directive", coalesce: false });
    await s.actors.tell(actor.id, "FAIL_DIRECTIVE one");
    await waitFor(() => s.alarms("actor-event-dropped", actor.id).length === 1);
    await waitFor(() => !s.actors.status(actor.id).inFlightRun && s.actors.status(actor.id).queued === 0);
    const next = await s.restart();
    expect(next.status(actor.id)).toBeDefined();
    s.clock.skew = 6 * HOUR + 1_000;
    await waitFor(() => s.alarms("actor-event-dropped", actor.id).length === 2);
    await settle();
    expect(s.alarms("actor-event-dropped", actor.id)).toHaveLength(2);
    expect(s.alarms("actor-event-dropped", actor.id)[1]!.data).toMatchObject({ count: 1, since: (s.alarms("actor-event-dropped", actor.id)[0]!.data as { since: number }).since });
  }, 60_000);

  it("alarm data carries fixed failure codes, never raw error text (paths, secrets)", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "leaky", instructions: "Run", residency: "durable", coalesce: false });
    const leak = "Error: open /home/paul/.config/secret.json failed: Authorization: Bearer sk-abcdefghijklmnop0123";
    vi.spyOn(s.agents, "run").mockRejectedValue(new Error(leak));
    await s.actors.tell(actor.id, "anything");
    await waitFor(() => s.alarms("actor-event-dropped", actor.id).length === 1);
    await waitFor(() => !s.actors.status(actor.id).inFlightRun && !s.actors.status(actor.id).preparing);
    await s.actors.stop(actor.id);
    s.clock.skew = 10 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 1);
    for (const alarm of [...s.alarms("actor-event-dropped", actor.id), ...s.alarms("actor-stopped", actor.id)]) {
      const text = JSON.stringify(alarm.data);
      expect(text).not.toContain("/home/");
      expect(text).not.toContain("Bearer");
      expect(text).not.toContain("sk-abcdef");
    }
    expect(s.alarms("actor-event-dropped", actor.id)[0]!.data).toMatchObject({ reason: expect.stringMatching(/^[a-z-]+$/) });
  }, 60_000);

  it.skipIf(process.platform !== "linux")("processStartTime counts field 22 from after the LAST ')' (a comm with spaces and parentheses)", async () => {
    const { spawn } = await import("node:child_process");
    const child = spawn(process.execPath, ["-e", "process.title = 'a) b (c'; setTimeout(() => {}, 10_000); process.stdout.write('ready')"], { stdio: ["ignore", "pipe", "ignore"] });
    try {
      await new Promise(resolve => child.stdout!.once("data", resolve));
      const stat = fs.readFileSync(`/proc/${child.pid}/stat`, "utf8");
      expect(stat).toContain("(a) b (c)");
      const fields = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/);
      expect(processIdentity.processStartTime(child.pid!)).toBe(fields[19]);
      expect(fields[19]).toMatch(/^\d+$/);
      // A naive split from the first ')' would land elsewhere: the start time must not depend on comm.
      expect(stat.slice(stat.indexOf(")") + 2).trim().split(/\s+/)[19]).not.toBe(fields[19]);
    } finally { child.kill("SIGKILL"); }
  });

  // smarty-dev#10067 P2-1: a native close is seen before the monitor marks executionExited.
  it("a worker whose transport closed is not open, even before executionExited; with its pid reused the run alarms", async () => {
    vi.spyOn(processIdentity, "processStartTime").mockReturnValue(undefined); // off Linux: no start time
    const s = setup();
    const launch = ProcessTransport.prototype.launch;
    let handle: Awaited<ReturnType<typeof launch>> | undefined;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, ...args) {
      handle = await launch.apply(this, args);
      return handle;
    });
    const run = await s.agents.spawn({ task: "HANG", transport: "process" });
    await waitFor(() => handle !== undefined && s.agents.workerIdentity(run.id)?.open === true);
    const pid = s.agents.workerIdentity(run.id)!.pid;
    let atClose: ReturnType<AgentManager["workerIdentity"]> | "unset" = "unset";
    const closed = handle!.closed!.then(() => { atClose = s.agents.workerIdentity(run.id); });
    process.kill(pid, "SIGKILL");
    await closed;
    // The moment the close is observable, before the monitor reacts: the handle is not open.
    expect(atClose).toMatchObject({ pid, open: false });
    // The actor decision on that identity, with the pid now naming another live process: worker-unverified, alarmed.
    const actor = await s.actors.create({ name: "reused", instructions: "Run", coalesce: false });
    await s.actors.tell(actor.id, "HANG_WITH_PROGRESS reused");
    await waitFor(() => !!s.actors.status(actor.id).inFlightRun);
    const runId = s.actors.status(actor.id).inFlightRun!.id;
    await waitFor(() => fs.existsSync(path.join(s.agents.runDirectory(runId)!, "status.json")));
    const real = s.agents.workerIdentity.bind(s.agents);
    vi.spyOn(s.agents, "workerIdentity").mockImplementation(id => id === runId ? { ...(atClose as object), pid: process.pid } as ReturnType<typeof real> : real(id));
    try {
      s.clock.skew = 31 * MINUTE;
      await waitFor(() => s.alarms("actor-run-stuck", actor.id).length === 1);
      expect(s.alarms("actor-run-stuck", actor.id)[0]!.data).toMatchObject({ reason: "worker-unverified", runId });
    } finally {
      vi.mocked(s.agents.workerIdentity).mockRestore();
      await s.agents.stop(runId).catch(() => undefined);
      await s.actors.stop(actor.id).catch(() => undefined);
    }
  }, 60_000);

  // smarty-dev#10067 P2-2: the streak alarm and the early unknown-block alarm carry the fixed code, not lastError.
  it("a lastError with a path or a secret never appears in the blocked alarms (streak and early unknown)", async () => {
    const s = setup();
    const leak = "Error: open /home/paul/.config/secret.json failed: Authorization: Bearer sk-abcdefghijklmnop0123";
    vi.spyOn(s.agents, "run").mockRejectedValue(new Error(leak));
    const early = await s.actors.create({ name: "early", instructions: "Run", responseMode: "text" });
    const streak = await s.actors.create({ name: "streak", instructions: "Run", responseMode: "text" });
    await s.actors.ask(early.id, "go").catch(() => undefined);
    for (let run = 0; run < 3; run++) await s.actors.ask(streak.id, "go").catch(() => undefined);
    await waitFor(() => s.alarms("actor-activation-blocked", streak.id).length === 1);
    s.clock.skew = 30 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-activation-blocked", early.id).length === 1);
    for (const alarm of [...s.alarms("actor-activation-blocked", streak.id), ...s.alarms("actor-activation-blocked", early.id)]) {
      const text = JSON.stringify(alarm.data);
      for (const secret of ["/home/", "Bearer", "sk-abcdef", "secret.json"]) expect(text).not.toContain(secret);
      expect(alarm.data).toMatchObject({ reason: "unknown", code: "unknown" });
    }
  }, 60_000);

  // smarty-dev#10067 P2-3: the worker-written status is allowlisted.
  it("an unknown worker status value becomes unknown in the alarm hint", async () => {
    const f = await stuckFixture();
    const nativeProof = mockNativeProof(f);
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? { pid: 2147483647, startTime: "1", open: true } : real(id));
    const status = JSON.parse(fs.readFileSync(path.join(f.directory, "status.json"), "utf8"));
    fs.writeFileSync(path.join(f.directory, "status.json"), JSON.stringify({ ...status, status: "ok /home/paul/x Bearer y" }));
    try {
      const data = await f.expectAlarm("worker-gone", 15_000);
      expect(data.workerStatusHint).toEqual({ untrusted: true, status: "unknown" });
    } finally { nativeProof.mockRestore(); vi.mocked(f.s.agents.workerIdentity).mockRestore(); await f.teardown(); }
  }, 60_000);

  // smarty-dev#8903: the publish is idempotent per slot, so a crash between it and the `published` mark sends one alarm.
  it("a crash after the re-alarm publish and before the published mark: a later sweep takes the claim over and the owner gets ONE alarm", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "blocked", instructions: "Respond.", responseMode: "directive" });
    for (let run = 0; run < 3; run++) await s.actors.ask(actor.id, "FAIL_DIRECTIVE").catch(() => undefined);
    await waitFor(() => s.alarms("actor-activation-blocked", actor.id).length === 1); // the streak alarm
    const repeats = () => s.alarms("actor-activation-blocked", actor.id).filter(event => (event.data as { repeat?: boolean }).repeat);
    const put = s.mesh.put.bind(s.mesh);
    let crashed = 0;
    vi.spyOn(s.mesh, "put").mockImplementation(async (input) => {
      // The process dies after its publish: the `published` mark is never written.
      if (input.key === actorAlarmKey(actor.id, "actor-activation-blocked") && (input.value as { state?: string }).state === "published") {
        crashed++;
        throw new Error("crashed before the published mark");
      }
      return put(input);
    });
    s.clock.skew = 2 * HOUR + 1_000;
    await waitFor(() => repeats().length === 1 && crashed === 1);
    expect(s.mesh.get(actorAlarmKey(actor.id, "actor-activation-blocked"))?.value).toMatchObject({ state: "pending", slot: 0 });
    s.clock.skew += 5 * MINUTE + 1_000; // the pending claim is overdue: a later sweep takes it over and publishes again
    await waitFor(() => crashed === 2);
    await settle();
    expect(repeats()).toHaveLength(1);
  }, 60_000);
});
