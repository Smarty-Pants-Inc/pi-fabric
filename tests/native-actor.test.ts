// Keep: trust boundary + data safety. Exercises carrier-backed proof -> actual serial native
// callback -> unchanged direct wrapper. Not the frozen-provider/broker/native-owner acceptance.
import { afterEach, describe, expect, it, vi } from "vitest";
import type { NativeActorContext, NativeActorRequest } from "../src/actors/native-handler.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import { natsLead, POD_MACHINE } from "./helpers/bridge-nats-l2-fakes.js";
import { createNativeActorFixture, deferred, nativePosix, type NativeActorFixture } from "./helpers/native-actor-ingress.js";

const fixtures: NativeActorFixture[] = [];
const fixture = async (...args: Parameters<typeof createNativeActorFixture>) => {
  const f = await createNativeActorFixture(...args); fixtures.push(f); return f;
};
afterEach(async () => { vi.restoreAllMocks(); await Promise.all(fixtures.splice(0).map(f => f.close())); });

describe.skipIf(!nativePosix)("native actor exact-ID execution", () => {
  it("materializes the exact durable session row without any model default, preparation or launch", async () => {
    const f = await fixture({ ensure: false });
    const spies = f.modelSpies();
    const first = await f.actors.ensureNativeActor(f.binding), second = await f.actors.ensureNativeActor({ ...f.binding });
    expect(first.id).toBe(f.binding.actorId); expect(second.id).toBe(first.id);
    expect(f.rows()).toHaveLength(1);
    expect(f.rows()[0]).toMatchObject({ id: first.id, rootId: f.binding.rootId, runner: "native", instructions: null,
      execution: { kind: "native", version: 1, handlerKey: f.binding.handlerKey }, residency: "durable", events: [], topics: [],
      delivery: "mailbox", responseMode: "text", triggerTurn: false, coalesce: false });
    expect(first).not.toHaveProperty("model"); expect(first).not.toHaveProperty("binding");
    const participant = actorParticipantRecord(first, f.binding.rootId, f.identity.id, f.identity.id, f.binding.rootId);
    expect(participant.kind).toBe("actor"); expect(participant.capabilities).toEqual(["stop", "ask"]);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("passes immutable exact message/data and genuine canonical source; direct data/text are never redacted", async () => {
    let request: Readonly<NativeActorRequest> | undefined, context: Readonly<NativeActorContext> | undefined;
    const f = await fixture({ handler: async (input, ctx) => {
      request = input; context = ctx;
      expect(Object.isFrozen(input)).toBe(true); expect(Object.isFrozen(input.data)).toBe(true);
      expect(Object.isFrozen(ctx)).toBe(true); expect(Object.isFrozen(ctx.sender)).toBe(true);
      return { data: { secretReply: ["EXACT_RESPONSE", { ok: true }] }, text: "EXACT_TEXT" };
    } });
    const spies = f.modelSpies(), message = " smarty-inbox.rpc.v1 ", data = { requestBody: ["EXACT_REQUEST"] };
    const { nativeAdmission } = await f.issue(message, data);
    const pending = f.actors.ask(f.binding.actorId, message, data, undefined, { nativeAdmission });
    data.requestBody.push("MUTATED_AFTER_CALL");
    const reply = await pending;
    expect(request).toEqual({ message, data: { requestBody: ["EXACT_REQUEST"] } });
    expect(context).toMatchObject({ actorId: f.binding.actorId, source: "nats-bridge", sourceMachine: POD_MACHINE,
      sender: { id: natsLead.id, kind: "main", sessionId: natsLead.id.slice(8) } });
    expect(reply).toMatchObject({ actorId: f.binding.actorId, direction: "out", source: "direct", text: "EXACT_TEXT",
      data: { secretReply: ["EXACT_RESPONSE", { ok: true }] } });
    expect(reply).not.toHaveProperty("runId"); expect(reply).not.toHaveProperty("usage");
    expect(f.actors.status(f.binding.actorId)).not.toHaveProperty("lastRunId");
    expect(f.deliveries).toEqual([]);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("refuses local/forged/reused/altered proofs, even through direct actors.ask", async () => {
    const f = await fixture(), spies = f.modelSpies();
    await expect(f.actors.ask(f.binding.actorId, "rpc", {})).rejects.toThrow(/Native/);
    await expect(f.actors.ask(f.binding.actorId, "rpc", {}, undefined,
      { nativeAdmission: {} as never })).rejects.toThrow(/admission/);
    const proof = await f.issue("rpc", { a: 1 });
    await expect(f.actors.ask(f.binding.actorId, "rpc ", { a: 1 }, undefined, proof)).rejects.toThrow(/admission/);
    const valid = await f.issue("rpc", { a: 2 });
    await f.actors.ask(f.binding.actorId, "rpc", { a: 2 }, undefined, valid);
    await expect(f.actors.ask(f.binding.actorId, "rpc", { a: 2 }, undefined, valid)).rejects.toThrow(/admission/);
    expect(f.handler).toHaveBeenCalledTimes(1);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("rejects native model overrides before model work or callback", async () => {
    const f = await fixture(), spies = f.modelSpies();
    const proof = await f.issue("rpc", { a: 1 });
    await expect(f.actors.ask(f.binding.actorId, "rpc", { a: 1 }, undefined,
      { ...proof, overrides: { model: "forged/model" } })).rejects.toThrow(/Native/);
    expect(f.handler).not.toHaveBeenCalled();
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
  });

  it("uses one callback at a time in the existing serial queue", async () => {
    const entered = deferred<void>(), release = deferred<void>();
    let calls = 0, active = 0, peak = 0;
    const f = await fixture({ handler: async request => {
      ++calls; ++active; peak = Math.max(peak, active);
      if (calls === 1) { entered.resolve(); await release.promise; }
      --active; return { data: request.data };
    } });
    try {
      const a = f.ask("rpc", { n: 1 }); await entered.promise;
      const proof = await f.issue("rpc", { n: 2 });
      const b = f.actors.ask(f.binding.actorId, "rpc", { n: 2 }, undefined, proof);
      await Promise.resolve();
      expect(calls).toBe(1); expect(f.actors.releaseCustodyCount()).toBeGreaterThan(0);
      release.resolve();
      expect((await a).data).toEqual({ n: 1 }); expect((await b).data).toEqual({ n: 2 });
      expect(peak).toBe(1);
    } finally { release.resolve(); }
  });

  it("redacts callback exception bodies and rejects invalid/oversized returns without fallback", async () => {
    const f = await fixture({ handler: async () => { throw new Error("SECRET_CALLBACK_ERROR"); } });
    const spies = f.modelSpies();
    await expect(f.ask("rpc", { password: "SECRET_INPUT" })).rejects.toThrow("Native actor ask failed");
    expect(JSON.stringify(f.actors.messages(f.binding.actorId))).not.toMatch(/SECRET_/);
    expect(f.actors.status(f.binding.actorId).lastError).not.toMatch(/SECRET_/);
    for (const spy of spies) expect(spy).not.toHaveBeenCalled();
    const bad = await fixture({ handler: async () => ({ data: { huge: "x".repeat(300_000) } }) });
    await expect(bad.ask("rpc", {})).rejects.toThrow("Native actor ask failed");
  });
});
