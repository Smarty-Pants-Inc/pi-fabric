// L1 (smarty-dev#15310, N4 consume half, PR 2): the NATS event-stream leg admits an event only from a
// live NATS root on another machine, names the sender from KV presence (never the body), never
// re-forwards a bridged event, and ACKs only when the file half reports the append durable.
import { describe, expect, it } from "vitest";
import {
  decodeNatsBridgeEvent,
  natsBridgeEventSubjects,
  runNatsBridgeEvents,
  type NatsBridgeEventDelivery,
  type NatsBridgeEventDisposition,
  type NatsBridgeEventMessage,
  type NatsBridgeEventSource,
} from "../src/mesh/bridge-nats-events.js";
import type { BridgeNatsPortResolved } from "../src/mesh/bridge-nats-types.js";

const ORG = "example-org";
const BRIDGE = "ryzen1-bridge";
const LEAD = "session:22222222-2222-4222-8222-222222222222";
const MIRRORED = "session:11111111-1111-4111-8111-111111111111";

interface FakeMessage extends NatsBridgeEventMessage { calls: string[] }

class ScriptedEvents implements NatsBridgeEventSource {
  verified = true;
  constructor(readonly messages: FakeMessage[]) {}
  isVerified(): boolean { return this.verified; }
  async *consume(signal: AbortSignal): AsyncIterable<NatsBridgeEventMessage> {
    for (const message of this.messages) { if (signal.aborted) return; yield message; }
  }
}

function eventMessage(topic: string, body: Record<string, unknown> = {}, options: { subject?: string; sequence?: number; msgId?: string; attempt?: number; raw?: string } = {}): FakeMessage {
  const event = { id: "evt-1", topic, kind: "comment", from: { id: LEAD, name: "claimed-name", kind: "main" }, to: "factory",
    text: "hello", createdAt: 5, sequence: 999, ...body };
  const calls: string[] = [];
  return {
    calls, subject: options.subject ?? `fabric.ev.${ORG}.${topic}`,
    data: Buffer.from(options.raw ?? JSON.stringify(event)),
    headers: { get: (name: string) => name === "Nats-Msg-Id" ? options.msgId ?? "" : "" },
    info: { streamSequence: options.sequence ?? 10, deliveryCount: options.attempt ?? 1 },
    async ackAck() { calls.push("ack"); return true; },
    nak(delay?: number) { calls.push(`nak:${delay ?? ""}`); },
    term(reason?: string) { calls.push(`term:${reason ?? ""}`); },
  };
}

const resolved = (id: string, machine: string, live = true, kind: "main" | "actor" = "main"): BridgeNatsPortResolved => ({
  route: { machine, ownerHostId: id, ownerIdentityId: id, ownerIncarnation: "inc", capabilities: ["followUp"], live },
  identity: { id, name: "v2-lead", kind, sessionId: id.slice(8) },
});

async function run(messages: FakeMessage[], options: { resolve?: (id: string) => BridgeNatsPortResolved | undefined;
  onEvent?: (delivery: NatsBridgeEventDelivery) => Promise<NatsBridgeEventDisposition>; verified?: boolean } = {}) {
  const delivered: NatsBridgeEventDelivery[] = [];
  const refused: string[] = [];
  const source = new ScriptedEvents(messages);
  if (options.verified === false) source.verified = false;
  await runNatsBridgeEvents({
    org: ORG, machine: BRIDGE, topics: ["fleet.work.>", "ops.owner"], source,
    resolve: options.resolve ?? (id => id === LEAD ? resolved(LEAD, "v2-org") : id === MIRRORED ? resolved(MIRRORED, BRIDGE) : undefined),
    onEvent: options.onEvent ?? (async delivery => { delivered.push(delivery); return "accepted"; }),
    onRefused: code => { refused.push(code); },
    retryDelayScheduleMs: [100, 200],
  }, new AbortController().signal);
  return { delivered, refused };
}

describe("NATS bridge events: subjects", () => {
  it("maps exact topics and whole-remainder prefixes under the org, and refuses wildcards elsewhere", () => {
    expect(natsBridgeEventSubjects(ORG, ["ops.owner", "fleet.work.>", "ops.owner"]))
      .toEqual([`fabric.ev.${ORG}.fleet.work.>`, `fabric.ev.${ORG}.ops.owner`]);
    for (const bad of ["*", "fleet.*.x", "fleet..work", ">", "", "fleet.work.>.x"]) {
      expect(() => natsBridgeEventSubjects(ORG, [bad])).toThrow();
    }
    expect(() => natsBridgeEventSubjects("sys", ["ops.owner"])).toThrow();
    expect(() => natsBridgeEventSubjects(ORG, [])).toThrow();
  });
});

describe("NATS bridge events: consume leg", () => {
  it("delivers an event from a live NATS root with the KV identity, and ACKs after an accepted append", async () => {
    const message = eventMessage("fleet.work.pi-fabric.15310", {}, { msgId: "evt-1", sequence: 42, attempt: 3 });
    const { delivered, refused } = await run([message]);
    expect(refused).toEqual([]);
    expect(delivered).toHaveLength(1);
    expect(delivered[0]).toMatchObject({ messageId: "evt-1", streamSequence: 42, attempt: 3, senderMachine: "v2-org",
      claimedFrom: { id: LEAD, name: "claimed-name" } });
    // The sender identity is the KV record's, and the stream owns the sequence.
    expect(delivered[0]!.event.from).toEqual({ id: LEAD, name: "v2-lead", kind: "main", sessionId: LEAD.slice(8) });
    expect(delivered[0]!.event.sequence).toBe(42);
    expect(message.calls).toEqual(["ack"]);
  });

  it("falls back to the stream sequence as the message id", async () => {
    const { delivered } = await run([eventMessage("ops.owner", { kind: "pr.wake" }, { sequence: 77 })]);
    expect(delivered[0]!.messageId).toBe("77");
  });

  it("refuses senders it cannot verify and anything already bridged, by TERM", async () => {
    const unknown = eventMessage("fleet.work.a.1", { from: { id: "session:nobody-000000", name: "x", kind: "main" } });
    const stale = eventMessage("fleet.work.a.2");
    const mirror = eventMessage("fleet.work.a.3", { from: { id: MIRRORED, name: "file-lead", kind: "main" } });
    const bridged = eventMessage("fleet.work.a.4", { data: { bridge: { from: "spoke", id: "x" } } });
    const actor = eventMessage("fleet.work.a.5", { from: { id: "actor-1", name: "a", kind: "actor" } });
    const lookups: string[] = [];
    const { delivered, refused } = await run([unknown, stale, mirror, bridged, actor], {
      resolve: id => {
        lookups.push(id);
        if (id === LEAD) return resolved(LEAD, "v2-org", lookups.filter(seen => seen === LEAD).length > 1);
        if (id === MIRRORED) return resolved(MIRRORED, BRIDGE);
        if (id === "actor-1") return resolved("actor-1", "v2-org", true, "actor");
        return undefined;
      },
    });
    expect(delivered).toEqual([]);
    expect(refused).toEqual(["BRIDGE_NATS_SENDER_UNVERIFIED", "BRIDGE_NATS_SENDER_UNVERIFIED", "BRIDGE_NATS_SENDER_IS_BRIDGE",
      "BRIDGE_NATS_BRIDGED_AGAIN", "BRIDGE_NATS_SENDER_UNVERIFIED"]);
    for (const message of [unknown, stale, mirror, bridged, actor]) expect(message.calls).toEqual([expect.stringMatching(/^term:/)]);
  });

  it("TERMs a malformed event or one whose subject is not its topic", async () => {
    const raw = eventMessage("fleet.work.a.1", {}, { raw: "{" });
    const moved = eventMessage("fleet.work.a.1", {}, { subject: `fabric.ev.${ORG}.fleet.work.a.2` });
    const { delivered, refused } = await run([raw, moved]);
    expect(delivered).toEqual([]);
    expect(refused).toEqual(["BRIDGE_NATS_EVENT_MALFORMED", "BRIDGE_NATS_EVENT_MALFORMED"]);
    expect(raw.calls[0]).toMatch(/^term:/);
    expect(moved.calls[0]).toMatch(/^term:/);
  });

  it("NAKs with the retry schedule on retry or a thrown append, and TERMs a route refusal", async () => {
    const retry = eventMessage("fleet.work.a.1", {}, { attempt: 1 });
    const thrown = eventMessage("fleet.work.a.2", {}, { attempt: 5 });
    const refusedRoute = eventMessage("fleet.work.a.3");
    const outcomes: Array<() => Promise<NatsBridgeEventDisposition>> = [
      async () => "retry", async () => { throw new Error("mesh lock busy"); }, async () => "refused"];
    await run([retry, thrown, refusedRoute], { onEvent: () => outcomes.shift()!() });
    expect(retry.calls).toEqual(["nak:100"]);
    expect(thrown.calls).toEqual(["nak:200"]);
    expect(refusedRoute.calls).toEqual(["term:refused by the bridge route rules"]);
  });

  it("NAKs (never refuses) an event whose sender cannot be looked up yet", async () => {
    const message = eventMessage("fleet.work.a.1");
    const { delivered, refused } = await run([message], { resolve: () => {
      throw Object.assign(new Error("not ready"), { code: "FABRIC_DIRECTORY_NOT_READY" });
    } });
    expect(delivered).toEqual([]);
    expect(refused).toEqual([]);
    expect(message.calls).toEqual(["nak:100"]);
  });

  it("neither admits nor settles anything while the connection awaits re-verification", async () => {
    const message = eventMessage("fleet.work.a.1");
    const { delivered } = await run([message], { verified: false });
    expect(delivered).toEqual([]);
    expect(message.calls).toEqual([]);
  });

  it("NAKs a subject outside its filters instead of handling it", async () => {
    const message = eventMessage("github.thing", {}, { subject: `fabric.ev.${ORG}.github.thing` });
    const { delivered } = await run([message]);
    expect(delivered).toEqual([]);
    expect(message.calls).toEqual(["nak:100"]);
  });

  it("decodes with the org's subject rule", () => {
    const message = eventMessage("ops.owner", {}, { msgId: "m" });
    expect(decodeNatsBridgeEvent(ORG, message).messageId).toBe("m");
    expect(() => decodeNatsBridgeEvent("other-org", message)).toThrow();
  });
});
