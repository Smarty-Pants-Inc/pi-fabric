import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage, sessionHoldsInboxBatch } from "../src/topology/root-inbox.js";

// smarty-dev#754 §3.2 step 3: a Main reconciles the work events addressed to it that no steer
// delivered. A batch stays pending until the session holds it; the cursor moves only then.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:me", name: "main", kind: "main", sessionId: "me" };
const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
const held = () => true;
const notHeld = () => false;

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-root-inbox-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let offset = 0;
  const clock = { now: () => Date.now() + offset, advance: (ms: number) => { offset += ms; } };
  const inbox = (steerGraceMs = 0) =>
    new RootInbox(mesh, me, () => [me.id, "fabric-v2"], { now: clock.now, steerGraceMs });
  const work = (text: string, to = me.id, topic = "fleet.work.pi-fabric.1", key = text) =>
    mesh.publish({ topic, to, kind: "ack", from: peer, text, data: { ref: "Smarty-Pants-Inc/pi-fabric#1", key } });
  const texts = (events: MeshEvent[]) => events.map((event) => event.text);
  return { mesh, clock, inbox, work, texts };
};

afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("RootInbox", () => {
  it("starts at the present, then brings work events addressed to this root by id or name", async () => {
    const { clock, inbox, work, texts } = setup();
    await work("before the inbox existed");
    const box = inbox();
    expect((await box.next(held)).events).toEqual([]);
    await work("by id");
    await work("by name", "fabric-v2");
    await work("to someone else", "session:other");
    await work("not a work topic", me.id, "ops.owner");
    clock.advance(1);
    expect(texts((await box.next(held)).events)).toEqual(["by id", "by name"]);
    expect((await box.next(held)).events).toEqual([]);
  });

  it("leaves a young event for a later reconcile, and never moves past it", async () => {
    const { clock, inbox, work, texts } = setup();
    const box = inbox(60_000);
    await box.next(held);
    await work("young");
    expect((await box.next(held)).events).toEqual([]);
    clock.advance(61_000);
    expect(texts((await box.next(held)).events)).toEqual(["young"]);
  });

  it("delivers a batch again until the session holds it, also after a restart (review F1)", async () => {
    const { clock, inbox, work, texts } = setup();
    const first = inbox();
    await first.next(held);
    await work("must arrive");
    clock.advance(1);
    const batch = await first.next(held);
    expect(texts(batch.events)).toEqual(["must arrive"]);
    // A stop before the session wrote the message: the restarted inbox delivers it again.
    const second = inbox();
    expect(texts((await second.next(notHeld)).events)).toEqual(["must arrive"]);
    expect(texts((await second.next(notHeld)).events)).toEqual(["must arrive"]);
    // Once the session holds it, the cursor moves and it does not come again.
    expect((await second.next(held)).events).toEqual([]);
    expect((await inbox().next(held)).events).toEqual([]);
  });

  it("stops delivering a batch the session never records after five attempts", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox();
    await box.next(held);
    await work("stuck");
    clock.advance(1);
    for (let attempt = 1; attempt <= 5; attempt++) expect((await box.next(notHeld)).events).toHaveLength(1);
    expect((await box.next(notHeld)).events).toEqual([]);
  });

  it("lets a steer receipt stand only for the one shadow record it copied, near its time (review F2)", async () => {
    const { clock, inbox, work, texts } = setup();
    const box = inbox();
    await box.next(held);
    box.noteDelivered(peer.id, "  ack ");
    await work("ack", me.id, "fleet.work.pi-fabric.1", "ack:1");
    await work("ack", me.id, "fleet.work.pi-fabric.2", "ack:2");      // same text, other work, no steer
    clock.advance(1);
    expect((await box.next(held)).events.map((event) => (event.data as { key: string }).key)).toEqual(["ack:2"]);
    // Twenty minutes later, still within the receipt's life: the window keeps it from matching.
    clock.advance(-20 * 60_000);
    box.noteDelivered(peer.id, "mid");
    clock.advance(20 * 60_000);
    await work("mid");
    clock.advance(1);
    expect(texts((await box.next(held)).events)).toEqual(["mid"]);
    // Hours later, the same text again with no new steer: an old receipt must not hide it.
    box.noteDelivered(peer.id, "late");
    clock.advance(7 * 60 * 60_000);
    await work("late");
    clock.advance(1);
    expect(texts((await box.next(held)).events)).toEqual(["late"]);
  });

  it("bounds a batch by count and text, and loses none of a longer backlog (review F4)", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox();
    await box.next(held);
    for (let index = 1; index <= 25; index++) await work(`item ${index}`);
    clock.advance(1);
    const first = await box.next(held);
    expect(first.events).toHaveLength(20);
    const second = await box.next(held);
    expect(second.events.map((event) => event.text)).toEqual(["item 21", "item 22", "item 23", "item 24", "item 25"]);
    for (let index = 1; index <= 6; index++) await work(`${index}${"x".repeat(10_000)}`);
    clock.advance(1);
    expect((await box.next(held)).events).toHaveLength(4);         // 4 x 8 KiB of text fills a batch
    expect((await box.next(held)).events).toHaveLength(2);
  });

  it("formats one message with each event's sender, ref and key, and cuts a long text", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox();
    await box.next(held);
    await work("ETA ~09:30Z <PR>");
    await work(`long ${"y".repeat(20_000)}`);
    clock.advance(1);
    const message = rootInboxMessage((await box.next(held)).events);
    expect(message.customType).toBe("pi-fabric-inbox");
    expect(message.content).toContain('ref="Smarty-Pants-Inc/pi-fabric#1"');
    expect(message.content).toContain("ETA ~09:30Z &lt;PR&gt;");
    expect(message.content).toContain("[cut at 8192 bytes; read the whole event with mesh.read(");
    expect(message.content.length).toBeLessThan(12_000);
  });

  it("finds the batch message among a session's recent entries", () => {
    const entry = { type: "custom_message", customType: "pi-fabric-inbox", details: { ids: ["a", "b"] } };
    expect(sessionHoldsInboxBatch([{ type: "message" }, entry], ["a", "b"])).toBe(true);
    expect(sessionHoldsInboxBatch([entry], ["a", "c"])).toBe(false);
    expect(sessionHoldsInboxBatch([], ["a"])).toBe(false);
  });
});

describe("MainAgentController delivery observer", () => {
  it("reports each agent message it delivers, for the inbox to skip its shadow record", () => {
    const pi = { sendMessage: vi.fn(), getThinkingLevel: vi.fn() } as unknown as ExtensionAPI;
    const main = new MainAgentController(pi, "session:me", true, process.cwd(), "me");
    const seen: Array<[string, string]> = [];
    main.deliveryObserver = (fromId, text) => seen.push([fromId, text]);
    main.deliverAgent({ from: { id: "session:peer", name: "main", kind: "main" }, message: "hello", delivery: "followUp" } as never);
    expect(seen).toEqual([["session:peer", "hello"]]);
  });
});
