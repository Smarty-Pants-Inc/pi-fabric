import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmKey } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

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
  return { root, mesh, agents, actors, clock, alarms };
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

  it("actor-run-stuck: an in-flight run older than 30 min whose run directory is gone alarms; a live one does not", async () => {
    const s = setup();
    const stuck = await s.actors.create({ name: "stuck", instructions: "Run", coalesce: false });
    const live = await s.actors.create({ name: "live", instructions: "Run", coalesce: false });
    await s.actors.tell(stuck.id, "HANG_WITH_PROGRESS stuck");
    await s.actors.tell(live.id, "HANG_WITH_PROGRESS live");
    await waitFor(() => !!s.actors.status(stuck.id).inFlightRun && !!s.actors.status(live.id).inFlightRun);
    const runId = s.actors.status(stuck.id).inFlightRun!.id;
    const directory = s.agents.runDirectory(runId)!;
    await waitFor(() => fs.existsSync(path.join(directory, "status.json")));
    fs.renameSync(directory, `${directory}.gone`); // the run's directory disappears; the actor still records the run
    try {
    s.clock.skew = 29 * MINUTE;
    await settle();
    expect(s.alarms("actor-run-stuck", stuck.id)).toHaveLength(0);
    s.clock.skew = 31 * MINUTE;
    await waitFor(() => s.alarms("actor-run-stuck", stuck.id).length === 1);
    expect(s.alarms("actor-run-stuck", stuck.id)[0]!.data).toMatchObject({ runId, ownerRoot: "session:test",
      reason: expect.stringContaining("missing"), thresholdMs: 30 * MINUTE });
    await settle();
    expect(s.alarms("actor-run-stuck", stuck.id)).toHaveLength(1);
    expect(s.alarms("actor-run-stuck", live.id)).toHaveLength(0); // its worker and directory are there
    } finally {
      fs.renameSync(`${directory}.gone`, directory); // so the teardown can stop the worker
      // A run that did work detaches on an actor stop (fake-worker HANG_WITH_PROGRESS): end the runs themselves.
      for (const id of [stuck.id, live.id]) {
        const inFlight = s.actors.status(id).inFlightRun?.id;
        if (inFlight) await s.agents.stop(inFlight).catch(() => undefined);
        await s.actors.stop(id).catch(() => undefined);
      }
    }
  }, 60_000);
});
