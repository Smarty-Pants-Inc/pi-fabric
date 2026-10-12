import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmKey } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import * as processIdentity from "../src/residency/process-identity.js";

// smarty-dev#9332: actor states that cannot act raise fabric.alarm.actor-activation (same envelope, ownerRoot routing),
// evaluated on the existing retention sweep (retentionSweepMs, no new timer), re-alarmed once per 6 h slot.
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
  const until = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > until) throw new Error("Timed out");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const settle = () => new Promise(resolve => setTimeout(resolve, 250)); // about a dozen 20 ms sweeps

const setup = (persistent = false) => {
  const realNow = Date.now.bind(Date);
  const clock = { skew: 0 };
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-alarms-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs") });
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
    actorRoot: path.join(root, "actors"), persistent, retentionSweepMs: 20 });
  closers.push(() => agents.close(), () => actors.close());
  const alarms = (kind: string, id: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 200 })
    .filter(event => event.kind === kind && (event.data as { actorId?: string }).actorId === id);
  /** A new manager on the same actor root (a host restart). */
  const restart = async () => {
    await actors.close();
    const next = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent, retentionSweepMs: 20 });
    closers.push(() => next.close());
    return next;
  };
  return { root, mesh, agents, actors, clock, alarms, restart };
};

describe("actor state alarms (smarty-dev#9332)", () => {
  it("actor-stopped: a durable actor stopped for over 10 min alarms with its reason; once per 6 h slot; not after removal", async () => {
    const s = setup(true);
    const actor = await s.actors.create({ name: "stopped", instructions: "Run", residency: "durable" });
    const other = await s.actors.create({ name: "running", instructions: "Run", residency: "durable" });
    await s.actors.stop(actor.id);
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(0);
    s.clock.skew = 10 * MINUTE + 1_000;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 1);
    expect(s.alarms("actor-stopped", actor.id)[0]).toMatchObject({ data: { actorId: actor.id, actorName: "stopped",
      ownerRoot: "session:test", reason: "stop", stopReason: "explicit", residency: "durable", stoppedForMs: expect.any(Number) } });
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(1); // dedupe inside the slot
    expect(s.alarms("actor-stopped", other.id)).toHaveLength(0);
    s.clock.skew += 6 * HOUR;
    await waitFor(() => s.alarms("actor-stopped", actor.id).length === 2); // re-alarm in the next slot
    await s.actors.remove(actor.id);
    expect(s.mesh.get(actorAlarmKey(actor.id, "actor-stopped"))).toBeUndefined();
    s.clock.skew += 6 * HOUR;
    await settle();
    expect(s.alarms("actor-stopped", actor.id)).toHaveLength(2);
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

  // actor-run-stuck (pi-fabric#944 round 1): liveness only from this host's own record of the launched worker (pid plus
  // /proc start time at spawn), never from the worker-writable status.json; anything unproven fails toward the alarm.
  // Two real hanging runs: "stuck" gets the fault, "live" stays healthy (the counterexample, never alarmed).
  const stuckFixture = async () => {
    const s = setup();
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
    const stuckAlarms = () => s.alarms("actor-run-stuck", stuck.id);
    const teardown = async () => {
      // A run that did work detaches on an actor stop (fake-worker HANG_WITH_PROGRESS): end the runs themselves.
      for (const id of [stuck.id, live.id]) {
        const inFlight = s.actors.status(id).inFlightRun?.id;
        if (inFlight) await s.agents.stop(inFlight).catch(() => undefined);
        await s.actors.stop(id).catch(() => undefined);
      }
    };
    /** Past the threshold, the stuck run alarms once with `reason`; the healthy run never does. */
    const expectAlarm = async (reason: string) => {
      s.clock.skew = 29 * MINUTE;
      await settle();
      expect(stuckAlarms()).toHaveLength(0);
      s.clock.skew = 31 * MINUTE;
      await waitFor(() => stuckAlarms().length === 1);
      const data = stuckAlarms()[0]!.data as Record<string, unknown>;
      expect(data).toMatchObject({ runId, reason, ownerRoot: "session:test", thresholdMs: 30 * MINUTE });
      await settle();
      expect(stuckAlarms()).toHaveLength(1);
      expect(s.alarms("actor-run-stuck", live.id)).toHaveLength(0);
      return data;
    };
    return { s, runId, directory, owned, teardown, expectAlarm, stuckAlarms };
  };
  // The dead worker of (a), and (c)'s live process with a different start time: real processes, not invented pids.
  const deadPid = async (): Promise<number> => {
    const { spawn } = await import("node:child_process");
    const child = spawn(process.execPath, ["-e", ""], { stdio: "ignore" });
    await new Promise(resolve => child.once("exit", resolve));
    return child.pid!;
  };

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
    // The host's record names a worker that has exited; the status file claims it is running.
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? { pid, startTime: "1", open: true } : real(id));
    const status = JSON.parse(fs.readFileSync(path.join(f.directory, "status.json"), "utf8"));
    fs.writeFileSync(path.join(f.directory, "status.json"), JSON.stringify({ ...status, status: "running", sessionId: String(process.pid) }));
    try {
      const data = await f.expectAlarm("worker-gone");
      expect(data.workerStatusHint).toEqual({ untrusted: true, status: "running" });
    } finally { vi.mocked(f.s.agents.workerIdentity).mockRestore(); await f.teardown(); }
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
    const real = f.s.agents.workerIdentity.bind(f.s.agents);
    vi.spyOn(f.s.agents, "workerIdentity").mockImplementation(id => id === f.runId ? { pid: process.pid, startTime: "1", open: true } : real(id));
    try { await f.expectAlarm("worker-pid-reused"); } finally { vi.mocked(f.s.agents.workerIdentity).mockRestore(); await f.teardown(); }
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
      expect(f.stuckAlarms()[0]!.data).toMatchObject({ reason: "worker-unverified", runId: f.runId });
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
});
