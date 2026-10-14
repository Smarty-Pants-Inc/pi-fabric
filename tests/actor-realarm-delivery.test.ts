import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  ActorManager, FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, actorAlarmDedupeKey, actorAlarmKey,
} from "../src/actors/manager.js";
import type { FabricActorDeliveryRequest } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

// Keep reason: smarty-dev#12512, a real mesh receipt replay must not duplicate the owner Main callback.
// No product deletion: only this fixture's private temp roots are cleaned up.
const MINUTE = 60_000, HOUR = 60 * MINUTE;
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
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
const settle = () => new Promise(resolve => setTimeout(resolve, 250)); // several retention sweeps
const SOURCE_TOKEN = "r2-12512-source-token-a71c9";
const SOURCE_PATH = "/home/paul/.config/r2-12512-source-path.json";
const SOURCE_ERROR = `Error: open ${SOURCE_PATH} failed: Authorization: Bearer ${SOURCE_TOKEN}`;
const LEGACY_LAST_ERROR = "r2-12512-legacy-last-error-token-b82da";
const LEGACY_REASON = "r2-12512-legacy-reason-token-c93eb /private/r2-12512-reason.json";
const LEGACY_CLASSIFICATION = "r2-12512-legacy-classification-token-da4fc /private/r2-12512-classification.json";
const expectSafeAlarm = (bytes: string, sensitive: string[]) => {
  // Match serialized bytes too on Windows, where real path backslashes are JSON-escaped.
  for (const marker of sensitive) expect(bytes).not.toContain(JSON.stringify(marker).slice(1, -1));
  for (const key of ["lastError", "sessionFile", "logDir", "classification", "error"]) {
    expect(bytes).not.toContain(`${JSON.stringify(key)}:`);
  }
};
const expectSafeDelivery = (bytes: string, sensitive: string[]) => {
  expectSafeAlarm(bytes, sensitive);
  const request = JSON.parse(bytes) as FabricActorDeliveryRequest;
  expect(request.actor).not.toHaveProperty("activationBlocked.reason");
  expect(request.actor).not.toHaveProperty("activationBlocked.classification");
};

const setup = () => {
  const realNow = Date.now.bind(Date);
  const clock = { skew: 0 };
  vi.spyOn(Date, "now").mockImplementation(() => realNow() + clock.skew);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-realarm-delivery-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"),
  });
  // Inject the source failure, not the alarm or receipt machinery; no worker process is needed.
  vi.spyOn(agents, "run").mockRejectedValue(new Error(SOURCE_ERROR));
  const identity: MeshIdentity = { id: "session:test", name: "main", kind: "main", sessionId: "test" };
  // Snapshot the complete emitted request at the callback boundary, before anything can change it.
  const delivered: string[] = [];
  const onDeliver = vi.fn<(request: FabricActorDeliveryRequest) => void>(request => {
    delivered.push(JSON.stringify(request));
  });
  closers.push(() => agents.close());
  const makeActors = () => {
    const manager = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, onDeliver, {
      actorRoot: path.join(root, "actors"), persistent: true, retentionSweepMs: 20,
      meshRetentionSweepPath: false, reapDeadSessionPresence: false,
    });
    closers.push(() => manager.close());
    return manager;
  };
  const actors = makeActors();
  const alarms = (id: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 100 })
    .filter(event => event.kind === "actor-activation-blocked" && (event.data as { actorId?: string }).actorId === id);
  const repeats = (id: string) => alarms(id).filter(event => (event.data as { repeat?: boolean }).repeat);
  const fail = async (count: number) => {
    const actor = await actors.create({ name: "blocked", instructions: "Respond.", responseMode: "text", delivery: "mailbox" });
    for (let run = 0; run < count; run++) {
      await actors.ask(actor.id, "go").catch(() => undefined);
      await waitFor(() => !actors.status(actor.id).inFlightRun && !actors.status(actor.id).preparing);
    }
    expect(actors.status(actor.id).activationBlocked).toMatchObject({ code: "unknown", count });
    return { actor, since: actors.status(actor.id).activationBlocked!.since,
      key: actorAlarmKey(actor.id, "actor-activation-blocked") };
  };
  const block = async () => {
    const blocked = await fail(3);
    const { actor } = blocked;
    await waitFor(() => alarms(actor.id).length === 1 && onDeliver.mock.calls.length === 1);
    // This is the initial streak notice, not the state re-alarm under test.
    expect(alarms(actor.id)[0]!.data).not.toHaveProperty("repeat");
    expect(onDeliver.mock.calls[0]![0].message.text).toContain("failed its last 3 activations");
    expect(repeats(actor.id)).toHaveLength(0);
    onDeliver.mockClear();
    delivered.length = 0;
    return blocked;
  };
  return { root, mesh, actors, makeActors, clock, onDeliver, delivered, alarms, repeats, fail, block };
};

describe("actor state re-alarm Main delivery (smarty-dev#12512)", () => {
  it("keeps source errors and actual actor paths out of the entire first notice and mesh alarm", async () => {
    const s = setup();
    const { actor } = await s.fail(3);
    await waitFor(() => s.alarms(actor.id).length === 1 && s.delivered.length === 1);
    const status = s.actors.status(actor.id);
    // Prove the source failure reached real actor state; a missing marker is not a privacy pass.
    expect(status.lastError).toContain(SOURCE_TOKEN);
    expect(status.activationBlocked!.reason).toContain(SOURCE_PATH);
    expect(status.sessionFile).toBe(path.join(s.root, "actors", actor.id, "session.jsonl"));
    expect(status.logDir).toBe(path.join(s.root, "actors", actor.id, "runs"));
    const sensitive = [SOURCE_ERROR, SOURCE_TOKEN, SOURCE_PATH, status.sessionFile!, status.logDir!, "Bearer"];
    const request = JSON.parse(s.delivered[0]!) as FabricActorDeliveryRequest;
    expect(request).toMatchObject({ actor: { id: actor.id, name: "blocked", rootId: "session:test" },
      delivery: "followUp", triggerTurn: true, message: { source: "fabric-host" } });
    expect(request.message.text).toContain("failed its last 3 activations");
    expectSafeDelivery(s.delivered[0]!, sensitive);
    expectSafeAlarm(JSON.stringify(s.alarms(actor.id)[0]!.data), sensitive);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
  }, 60_000);

  it("keeps restored legacy errors, reasons, classification and real paths out of the whole repeat request and mesh data", async () => {
    const s = setup();
    const actor = await s.actors.create({ name: "legacy-blocked", instructions: "Respond.", responseMode: "text", delivery: "mailbox" });
    await s.actors.close();
    const file = path.join(s.root, "actors", "actors.json");
    const registry = JSON.parse(fs.readFileSync(file, "utf8")) as { actors: Array<Record<string, unknown>> };
    const row = registry.actors.find(record => record.id === actor.id)!;
    expect(row).toBeDefined();
    const since = Date.now();
    // auth is hard and is NOT one of the codes re-derived from reason on restore.
    row.activationBlocked = { reason: LEGACY_REASON, code: "auth", classification: LEGACY_CLASSIFICATION, since, count: 3 };
    row.failureStreak = { count: 3, notified: true };
    // The supported stopped/orphan restore path retains lastError (ordinary idle rows do not).
    // Explicit stop prevents same-session host-exit revival; session residency avoids stopped-actor alarms.
    row.status = "stopped";
    row.stopReason = "explicit";
    row.lastError = LEGACY_LAST_ERROR;
    row.sessionOrphan = { oldRoot: row.rootId, oldHost: "r2-legacy-host", reason: "root-gone",
      orphanedAt: since, lastUpdated: since };
    fs.writeFileSync(file, JSON.stringify(registry));
    const restored = s.makeActors();
    const status = restored.status(actor.id);
    expect(status).toMatchObject({ status: "stopped", lastError: LEGACY_LAST_ERROR,
      activationBlocked: { reason: LEGACY_REASON, code: "auth", class: "hard", classification: LEGACY_CLASSIFICATION, since } });
    expect(status.sessionFile).toBe(path.join(s.root, "actors", actor.id, "session.jsonl"));
    expect(status.logDir).toBe(path.join(s.root, "actors", actor.id, "runs"));
    expect(s.onDeliver).not.toHaveBeenCalled();
    const key = actorAlarmKey(actor.id, "actor-activation-blocked");
    s.clock.skew = 2 * HOUR + MINUTE;
    await waitFor(() => s.delivered.length === 1 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.repeats(actor.id)).toHaveLength(1);
    expect(s.repeats(actor.id)[0]).toMatchObject({ data: { reason: "auth", repeat: true } });
    expect(restored.status(actor.id)).toMatchObject({ lastError: LEGACY_LAST_ERROR,
      activationBlocked: { reason: LEGACY_REASON, classification: LEGACY_CLASSIFICATION } });
    const request = JSON.parse(s.delivered[0]!) as FabricActorDeliveryRequest;
    expect(request).toMatchObject({ actor: { id: actor.id, name: "legacy-blocked", rootId: "session:test" },
      delivery: "followUp", triggerTurn: true,
      message: { id: actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 0) } });
    const sensitive = [LEGACY_LAST_ERROR, LEGACY_REASON, LEGACY_CLASSIFICATION,
      "r2-12512-legacy-reason-token-c93eb", "r2-12512-legacy-classification-token-da4fc",
      "/private/r2-12512-reason.json", "/private/r2-12512-classification.json", status.sessionFile!, status.logDir!];
    expectSafeDelivery(s.delivered[0]!, sensitive);
    expectSafeAlarm(JSON.stringify(s.repeats(actor.id)[0]!.data), sensitive);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
  }, 60_000);

  it.each([1, 2])("delivers an unnotified unknown block after %i failures once in early slot -1, then again in slot 0", async count => {
    const s = setup();
    const { actor, since, key } = await s.fail(count);
    expect(s.alarms(actor.id)).toHaveLength(0);
    expect(s.onDeliver).not.toHaveBeenCalled();
    s.clock.skew = 30 * MINUTE + 1_000;
    const earlyKey = actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, -1);
    await waitFor(() => s.alarms(actor.id).length === 1 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "published", slot: -1, since });
    expect(s.alarms(actor.id)[0]).toMatchObject({ dedupeKey: earlyKey,
      data: { actorId: actor.id, reason: "unknown", early: true, count } });
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    expect(s.onDeliver.mock.calls[0]![0]).toMatchObject({ delivery: "followUp", triggerTurn: true,
      message: { id: earlyKey, source: "fabric-host" } });
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    expect(s.alarms(actor.id)).toHaveLength(1);
    s.clock.skew = 2 * HOUR + MINUTE;
    await waitFor(() => s.onDeliver.mock.calls.length === 2 &&
      (s.mesh.get(key)?.value as { slot?: number; state?: string })?.slot === 0 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.onDeliver.mock.calls[1]![0].message.id).toBe(actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 0));
    expect(s.repeats(actor.id)).toHaveLength(1);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(2);
    expect(s.alarms(actor.id)).toHaveLength(2);
  }, 60_000);

  it("does not redeliver early slot -1 when a crash replays its receipt, but does deliver slot 0", async () => {
    const s = setup();
    const { actor, since, key } = await s.fail(1);
    const put = s.mesh.put.bind(s.mesh);
    let crashed = 0;
    const mark = vi.spyOn(s.mesh, "put").mockImplementation(async input => {
      if (input.key === key && (input.value as { state?: string }).state === "published") {
        crashed++;
        throw new Error("crashed before early slot published mark");
      }
      return put(input);
    });
    const publish = vi.spyOn(s.mesh, "publish"); // real durable receipt, no fake dedupe
    s.clock.skew = 30 * MINUTE + 1_000;
    await waitFor(() => crashed === 1);
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "pending", slot: -1 });
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    const first = s.alarms(actor.id)[0]!;
    const earlyKey = actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, -1);
    expect(first).toMatchObject({ dedupeKey: earlyKey, data: { early: true } });
    expect(s.onDeliver.mock.calls[0]![0].message.id).toBe(earlyKey);
    s.clock.skew += 5 * MINUTE + 1_000;
    await waitFor(() => crashed === 2);
    await settle();
    expect(publish).toHaveBeenCalledTimes(2);
    const original = await publish.mock.results[0]!.value;
    const replay = await publish.mock.results[1]!.value;
    expect(original.id).toBe(first.id);
    expect(replay.id).toBe(first.id);
    expect(publish.mock.calls[0]![0].id).toBe(first.id);
    expect(publish.mock.calls[1]![0].id).not.toBe(first.id);
    expect(s.alarms(actor.id)).toHaveLength(1);
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    mark.mockRestore();
    s.clock.skew = 2 * HOUR + MINUTE;
    await waitFor(() => s.onDeliver.mock.calls.length === 2 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "published", slot: 0 });
    expect(s.onDeliver.mock.calls[1]![0].message.id).toBe(actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 0));
    expect(s.repeats(actor.id)[0]!.id).not.toBe(first.id);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(2);
    expect(s.alarms(actor.id)).toHaveLength(2);
  }, 60_000);

  it("does not add early slot -1 to an already-notified streak, while slot 0 still reaches Main", async () => {
    const s = setup();
    const { actor, since, key } = await s.block(); // first streak notice already delivered and spy cleared
    s.clock.skew = 30 * MINUTE + 1_000;
    await settle();
    expect(s.onDeliver).not.toHaveBeenCalled();
    expect(s.alarms(actor.id)).toHaveLength(1); // only the original streak alarm
    expect(s.alarms(actor.id).some(event => (event.data as { early?: boolean }).early)).toBe(false);
    expect(s.mesh.get(key)).toBeUndefined(); // no early claim, not even a suppressed callback
    s.clock.skew = 2 * HOUR + MINUTE;
    await waitFor(() => s.onDeliver.mock.calls.length === 1 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "published", slot: 0 });
    expect(s.onDeliver.mock.calls[0]![0].message.id).toBe(actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 0));
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    expect(s.repeats(actor.id)).toHaveLength(1);
  }, 60_000);

  it("delivers beyond slot zero once per slot, with a host followUp that starts a turn", async () => {
    const s = setup();
    const { actor, since, key } = await s.block();
    s.clock.skew = 8 * HOUR + MINUTE; // slot 1: beyond the first 2 h repeat
    const dedupeKey = actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 1);
    await waitFor(() => s.onDeliver.mock.calls.length === 1 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "published", slot: 1 });
    expect(s.repeats(actor.id)).toHaveLength(1);
    expect(s.repeats(actor.id)[0]).toMatchObject({ dedupeKey, data: { ownerRoot: "session:test", reason: "unknown" } });
    const request = s.onDeliver.mock.calls[0]![0];
    expect(request).toMatchObject({ actor: { id: actor.id, name: "blocked", rootId: "session:test" },
      delivery: "followUp", triggerTurn: true, message: { id: dedupeKey, actorId: actor.id, actorName: "blocked",
        direction: "out", source: "fabric-host", action: "message", createdAt: expect.any(Number) } });
    expect(request.message.text).toContain("actor-activation-blocked; reason: unknown");
    const status = s.actors.status(actor.id);
    const sensitive = [SOURCE_ERROR, SOURCE_TOKEN, SOURCE_PATH, "/home/", "Bearer", status.sessionFile!, status.logDir!];
    expectSafeDelivery(s.delivered[0]!, sensitive);
    expectSafeAlarm(JSON.stringify(s.repeats(actor.id)[0]!.data), sensitive);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    expect(s.repeats(actor.id)).toHaveLength(1);
    s.clock.skew += 6 * HOUR;
    await waitFor(() => s.onDeliver.mock.calls.length === 2);
    expect(s.onDeliver.mock.calls[1]![0].message.id).toBe(actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 2));
    expect(s.onDeliver.mock.calls[1]![0].message.id).not.toBe(dedupeKey);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(2);
    expect(s.repeats(actor.id)).toHaveLength(2);
  }, 60_000);

  it("does not redeliver a receipt after a crash before the published mark; the next slot still delivers", async () => {
    const s = setup();
    const { actor, since, key } = await s.block();
    const put = s.mesh.put.bind(s.mesh);
    let crashed = 0;
    const mark = vi.spyOn(s.mesh, "put").mockImplementation(async input => {
      if (input.key === key && (input.value as { state?: string }).state === "published") {
        // The callback already ran, but a crash prevents its slot's published mark.
        expect(s.onDeliver).toHaveBeenCalledTimes(1);
        crashed++;
        throw new Error("crashed before the published mark");
      }
      return put(input);
    });
    const publish = vi.spyOn(s.mesh, "publish"); // calls through to the real receipt store
    s.clock.skew = 2 * HOUR + MINUTE;
    await waitFor(() => crashed === 1);
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "pending", slot: 0 });
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    const first = s.repeats(actor.id)[0]!;
    expect(first.dedupeKey).toBe(actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 0));
    s.clock.skew += 5 * MINUTE + 1_000; // overdue pending claim: take over the same slot
    await waitFor(() => crashed === 2);
    await settle();
    expect(publish).toHaveBeenCalledTimes(2);
    const original = await publish.mock.results[0]!.value;
    const replay = await publish.mock.results[1]!.value;
    expect(original.id).toBe(first.id);
    expect(replay.id).toBe(first.id);
    expect(publish.mock.calls[0]![0].id).toBe(first.id);
    expect(publish.mock.calls[1]![0].id).not.toBe(first.id);
    expect(s.repeats(actor.id)).toHaveLength(1);
    expect(s.onDeliver).toHaveBeenCalledTimes(1); // count at Main's callback, not just in the mesh
    mark.mockRestore();
    s.clock.skew += 6 * HOUR;
    await waitFor(() => s.onDeliver.mock.calls.length === 2 &&
      (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    expect(s.onDeliver.mock.calls[1]![0].message.id).toBe(actorAlarmDedupeKey("actor-activation-blocked", actor.id, since, 1));
    expect(s.repeats(actor.id)[1]!.id).not.toBe(first.id);
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(2);
    expect(s.repeats(actor.id)).toHaveLength(2);
  }, 60_000);

  it("keeps the mesh alarm and published mark when Main delivery throws", async () => {
    const s = setup();
    const { actor, key } = await s.block();
    s.onDeliver.mockImplementation(() => { throw new Error("Main delivery unavailable"); });
    s.clock.skew = 2 * HOUR + MINUTE;
    await waitFor(() => (s.mesh.get(key)?.value as { state?: string })?.state === "published");
    await settle();
    expect(s.onDeliver).toHaveBeenCalledTimes(1);
    expect(s.repeats(actor.id)).toHaveLength(1);
    expect(s.mesh.get(key)?.value).toMatchObject({ state: "published", slot: 0 });
  }, 60_000);
});
