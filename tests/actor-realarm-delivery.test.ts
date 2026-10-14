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
const SOURCE_ERROR = "Error: open /home/paul/.config/secret.json failed: Authorization: Bearer [REDACTED:api-key]";

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
  const onDeliver = vi.fn<(request: FabricActorDeliveryRequest) => void>();
  const actors = new ActorManager("test", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, onDeliver, {
    actorRoot: path.join(root, "actors"), persistent: true, retentionSweepMs: 20,
    meshRetentionSweepPath: false, reapDeadSessionPresence: false,
  });
  closers.push(() => agents.close(), () => actors.close());
  const alarms = (id: string) => mesh.read({ topic: FABRIC_ACTOR_ACTIVATION_ALARM_TOPIC, limit: 100 })
    .filter(event => event.kind === "actor-activation-blocked" && (event.data as { actorId?: string }).actorId === id);
  const repeats = (id: string) => alarms(id).filter(event => (event.data as { repeat?: boolean }).repeat);
  const block = async () => {
    const actor = await actors.create({ name: "blocked", instructions: "Respond.", responseMode: "text", delivery: "mailbox" });
    for (let run = 0; run < 3; run++) await actors.ask(actor.id, "go").catch(() => undefined);
    await waitFor(() => alarms(actor.id).length === 1 && onDeliver.mock.calls.length === 1);
    // This is the initial streak notice, not the state re-alarm under test.
    expect(alarms(actor.id)[0]!.data).not.toHaveProperty("repeat");
    expect(onDeliver.mock.calls[0]![0].message.text).toContain("failed its last 3 activations");
    expect(repeats(actor.id)).toHaveLength(0);
    onDeliver.mockClear();
    const since = actors.status(actor.id).activationBlocked!.since;
    return { actor, since, key: actorAlarmKey(actor.id, "actor-activation-blocked") };
  };
  return { mesh, actors, clock, onDeliver, repeats, block };
};

describe("actor state re-alarm Main delivery (smarty-dev#12512)", () => {
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
    for (const sensitive of [SOURCE_ERROR, "/home/", "secret.json", "Bearer", "sk-abcdef", "classification"]) {
      expect(request.message.text).not.toContain(sensitive);
    }
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
