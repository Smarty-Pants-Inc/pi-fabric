// Keep: native business ingress and body retention are trust/data-safety boundaries.
// These inspect actual history/archive bytes; transport/control/ledger bodies intentionally remain.
import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createNativeActorFixture, deferred, nativePosix, type NativeActorFixture } from "./helpers/native-actor-ingress.js";

const fixtures: NativeActorFixture[] = [];
const fixture = async (...args: Parameters<typeof createNativeActorFixture>) => {
  const f = await createNativeActorFixture(...args); fixtures.push(f); return f;
};
afterEach(async () => { vi.restoreAllMocks(); await Promise.all(fixtures.splice(0).map(f => f.close())); });
const actorBytes = (f: NativeActorFixture): string => fs.readdirSync(f.actorRoot, { recursive: true })
  .map(name => path.join(f.actorRoot, String(name))).filter(file => fs.lstatSync(file).isFile())
  .map(file => fs.readFileSync(file, "utf8")).join("\n");

describe.skipIf(!nativePosix)("native ask-only sources and metadata-only history", () => {
  it("refuses tell/steer/followUp, model setters, predicates, subscriptions, batching and archive requeue", async () => {
    const f = await fixture(), id = f.binding.actorId, spies = f.modelSpies();
    expect(() => f.actors.tell(id, "SECRET_TELL", {})).toThrow(/Native/);
    await expect(f.actors.steerRemote(id, "SECRET_STEER", "steer")).rejects.toThrow(/Native/);
    await expect(f.actors.steerRemote(id, "SECRET_FOLLOWUP", "followUp")).rejects.toThrow(/Native/);
    expect(() => f.actors.resolveBinding(id)).toThrow(/Native/);
    const refused = [
      () => f.actors.resolveActivationBinding(id), () => f.actors.setModel(id, "forged/model"),
      () => f.actors.setThinking(id, "high"), () => f.actors.setThinking(id, "high", "project"),
      () => f.actors.setTools(id, ["bash"]), () => f.actors.setInstructions(id, "SECRET_INSTRUCTIONS"),
      () => f.actors.setInferenceContext(id, "activation"), () => f.actors.setEvents(id, ["turn_end"]),
      () => f.actors.setTopics(id, ["anything"]), () => f.actors.setCoalesceKey(id, "data.key"),
      () => f.actors.setDeliveryPolicy(id, "followUp", true), () => f.actors.setBatching(id, { batchWindowMs: 1 }),
      () => f.actors.setActivationFilter(id, ["hold"]), () => f.actors.resetSession(id),
      () => f.actors.requeueArchived(id, "/not-an-archive"),
    ];
    for (const operation of refused) await expect(operation()).rejects.toThrow(/Native/);
    expect(f.handler).not.toHaveBeenCalled();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    expect(f.actors.status(id)).toMatchObject({ events: [], topics: [], coalesce: false, delivery: "mailbox" });
  });

  it("ignores addressed mesh, host and lifecycle sources instead of starting native/model work", async () => {
    const f = await fixture(), spies = f.modelSpies(), observed = deferred<void>();
    const unsub = f.actors.subscribeMesh(() => observed.resolve());
    try {
      expect(f.actors.dispatchHostEvent("turn_end", { secret: "SECRET_HOST" })).toBe(0);
      expect(f.actors.dispatchHostEvent("agent_settled", { secret: "SECRET_LIFECYCLE" })).toBe(0);
      await f.mesh.publish({ topic: "arbitrary.topic", kind: "message", from: f.identity, to: f.binding.actorId,
        text: "SECRET_TOPIC", data: { secret: "SECRET_TOPIC" } });
      await observed.promise;
      expect(f.handler).not.toHaveBeenCalled();
      for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    } finally { unsub(); }
  });

  it("keeps exact direct response but writes metadata-only success/error/drop/cancel history and no body broadcasts", async () => {
    const entered = deferred<void>(), release = deferred<void>();
    let calls = 0;
    const f = await fixture({ handler: async request => {
      ++calls;
      if (request.message === "throw") throw new Error("SECRET_ERROR");
      if (request.message === "wait") { entered.resolve(); await release.promise; }
      return { text: "SECRET_RESPONSE_TEXT", data: { result: "SECRET_RESPONSE_DATA" } };
    } });
    const spies = f.modelSpies();
    const reply = await f.ask("success", { secret: "SECRET_REQUEST" });
    expect(reply.text).toBe("SECRET_RESPONSE_TEXT"); expect(reply.data).toEqual({ result: "SECRET_RESPONSE_DATA" });
    await expect(f.ask("throw", { secret: "SECRET_THROW_INPUT" })).rejects.toThrow(/Native actor ask failed/);
    const controller = new AbortController();
    const running = f.ask("wait", { secret: "SECRET_CANCEL_INPUT" }, controller.signal).catch(error => error as Error);
    try {
      await entered.promise;
      const proof = await f.issue("never-enter", { secret: "SECRET_DROPPED_INPUT" });
      const queued = f.actors.ask(f.binding.actorId, "never-enter", { secret: "SECRET_DROPPED_INPUT" }, undefined, proof).catch(error => error as Error);
      await Promise.resolve(); await Promise.resolve();
      controller.abort();
      await f.actors.stop(f.binding.actorId, undefined, false, "actors.stop", { dropQueued: true });
      expect(await running).toBeInstanceOf(Error); expect(await queued).toBeInstanceOf(Error);
    } finally {
      release.resolve(); await f.actors.stop(f.binding.actorId, undefined, true);
    }
    expect(calls).toBe(3);
    const history = f.actors.messages(f.binding.actorId);
    expect(history.length).toBeGreaterThan(0);
    for (const message of history) {
      expect(message).not.toHaveProperty("text"); expect(message).not.toHaveProperty("data");
      expect(message).not.toHaveProperty("principal"); expect(message).not.toHaveProperty("error");
    }
    expect(actorBytes(f)).not.toMatch(/SECRET_/);
    const broadcasts = f.mesh.read({ after: 0, limit: 1000 }).filter(event =>
      event.topic === "fabric.actor.input" || event.topic === "fabric.actor.output" || event.topic === "fabric.actor.lifecycle");
    expect(broadcasts).toEqual([]);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("does not restore callerless native queues or serialized context/proof labels on reconstruction", async () => {
    const f = await fixture(); await f.ask("rpc", { body: "DIRECT_INPUT" }); await f.actors.close();
    const directory = path.join(f.actorRoot, f.binding.actorId);
    const lineage = createHash("sha256").update([f.binding.rootId, "durable"].join("\0")).digest("hex").slice(0, 16);
    const file = path.join(directory, `queue-${lineage}.json`);
    // A volatile ask normally leaves no queue file at all. If a checkpoint exists,
    // it must still contain no caller body/capability. Seed the real lineage path.
    const queue = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, "utf8")) : { format: 1, items: [] };
    expect(JSON.stringify(queue)).not.toMatch(/DIRECT_INPUT|nativeAdmission/);
    queue.items = [{ id: "forged-item", source: "direct", createdAt: Date.now(), payload: { message: "rpc", data: { stolen: true } },
      binding: {}, bindingMode: "owner-defaults", nativeAdmission: {}, nativeContext: { sourceMachine: "forged" },
      activation: { kind: "direct", id: "forged-item", source: "direct", sequence: 1, createdAt: Date.now() } }];
    fs.writeFileSync(file, JSON.stringify(queue));
    vi.mocked(f.handler).mockClear();
    const spies = f.modelSpies(), next = f.openManager();
    next.resumeQueued();
    expect(next.status(f.binding.actorId).queued).toBe(0); expect(f.handler).not.toHaveBeenCalled();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("keeps ordinary actors' subscriptions, body history, notifications and model path intact", async () => {
    const f = await fixture();
    vi.spyOn(f.agents, "prepareModelWithReason").mockResolvedValue({ fallback: false });
    const ordinary = await f.actors.create({ name: "ordinary-control", instructions: "Ordinary model control", runner: "pi",
      model: "cliproxyapi/gpt-6.1-sol", residency: "durable", topics: ["ordinary.topic"], events: ["turn_end"] });
    const run = vi.spyOn(f.agents, "run").mockImplementation(async () => { throw new Error("ORDINARY_MODEL_SENTINEL"); });
    await expect(f.actors.ask(ordinary.id, "ORDINARY_BODY", { intact: "ORDINARY_DATA" })).rejects.toThrow("ORDINARY_MODEL_SENTINEL");
    expect(run).toHaveBeenCalledTimes(1);
    expect(f.actors.status(ordinary.id)).toMatchObject({ topics: ["ordinary.topic"], events: ["turn_end"] });
    expect(JSON.stringify(f.actors.messages(ordinary.id))).toContain("ORDINARY_BODY");
    expect(JSON.stringify(f.actors.messages(ordinary.id))).toContain("ORDINARY_DATA");
    expect(f.mesh.read({ after: 0, limit: 1000 }).some(event => event.topic === "fabric.actor.input" && event.text === "ORDINARY_BODY")).toBe(true);
    expect(f.handler).not.toHaveBeenCalled();
  });
});
