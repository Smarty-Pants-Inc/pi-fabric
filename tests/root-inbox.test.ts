import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage, rootInboxSession, sessionHoldsInboxBatch, type RootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#754 §3.2 step 3: a Main reconciles the work events addressed to it that no steer
// delivered. A batch stays pending until the session holds it; the cursor moves only then.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:me", name: "main", kind: "main", sessionId: "me" };
const peer: MeshIdentity = { id: "session:peer", name: "main", kind: "main", sessionId: "peer" };
const held: RootInboxSession = { holdsBatch: () => true, holdsSteer: () => false };
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
// A session whose entries hold the steers delivered so far (the pi-fabric-agent-message entries).
const withSteers = (steers: Array<{ from: string; data: unknown }>): RootInboxSession => ({
  holdsBatch: () => true,
  holdsSteer: rootInboxSession(steers.map(({ from, data }) => ({
    type: "custom_message", customType: "pi-fabric-agent-message", details: { from: { id: from }, data },
  }))).holdsSteer,
});

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-root-inbox-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let offset = 0;
  const clock = { now: () => Date.now() + offset, advance: (ms: number) => { offset += ms; } };
  const inbox = (steerGraceMs = 0, wakeCooldownMs = 5 * 60_000) =>
    new RootInbox(mesh, me, () => [me.id, "fabric-v2"], { now: clock.now, steerGraceMs, wakeCooldownMs });
  const work = (text: string, to = me.id, topic = "fleet.work.pi-fabric.1", key = text, kind = "ack") =>
    mesh.publish({ topic, to, kind, from: peer, text, data: { ref: "Smarty-Pants-Inc/pi-fabric#1", key } });
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

  it("keeps a batch pending through any number of undelivered attempts, until the session holds it (review F1)", async () => {
    const { clock, inbox, work, texts } = setup();
    await inbox().next(held);
    await work("stuck");
    clock.advance(1);
    // Each attempt is a stop between the pending save and the session's write: a fresh process.
    for (let attempt = 1; attempt <= 8; attempt++) expect(texts((await inbox().next(notHeld)).events)).toEqual(["stuck"]);
    expect((await inbox().next(held)).events).toEqual([]);
    expect((await inbox().next(notHeld)).events).toEqual([]);
  });

  it("skips a shadow record only when the session holds the steer with its sender and work key (review F2, round 3 F1)", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox();
    await box.next(held);
    const keys = async (session: RootInboxSession) => (await box.next(session)).events.map((event) => (event.data as { key: string }).key);
    // The first steer failed and the second reached the session, both with the text "ack".
    await work("ack", me.id, "fleet.work.pi-fabric.1", "ack:A");
    await work("ack", me.id, "fleet.work.pi-fabric.2", "ack:B");
    clock.advance(1);
    expect(await keys(withSteers([{ from: peer.id, data: { key: "ack:B" } }]))).toEqual(["ack:A"]);
    // A steer that was only enqueued (Pi has not recorded it) suppresses nothing.
    await work("queued", me.id, "fleet.work.pi-fabric.3", "queued:C");
    clock.advance(1);
    expect(await keys(withSteers([]))).toEqual(["queued:C"]);
    // Another sender's steer with the same key is not this work; a steer without a key is none.
    await work("other", me.id, "fleet.work.pi-fabric.4", "shared");
    await work("plain", me.id, "fleet.work.pi-fabric.5", "plain");
    clock.advance(1);
    expect(await keys(withSteers([{ from: "session:else", data: { key: "shared" } }, { from: peer.id, data: {} }]))).toEqual(["shared", "plain"]);
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

  it("recognises split per-sender receipts and does not return the pending aggregate", async () => {
    const { mesh, clock, inbox, work } = setup();
    const box = inbox(); await box.next(held);
    await work("one");
    await mesh.publish({ topic: "fleet.work.pi-fabric.1", to: me.id, from: { ...peer, id: "agent:other", kind: "agent" }, text: "two" });
    clock.advance(1);
    const batch = await box.next(notHeld);
    const entries: Array<{ type: string } & ReturnType<typeof rootInboxMessage>> = [];
    deliverRootInbox({ hostCapabilities: { turnProvenance: 1 },
      sendMessage: (message: ReturnType<typeof rootInboxMessage>) => entries.push({ type: "custom_message", ...message }),
    } as unknown as ExtensionAPI, batch.events);
    expect(entries).toHaveLength(2);
    const ids = batch.events.map(event => event.id);
    expect(sessionHoldsInboxBatch(entries, ids)).toBe(true);
    expect(rootInboxSession(entries).holdsBatch(ids)).toBe(true);
    expect(rootInboxSession(entries.slice(0, 1)).holdsBatch(ids)).toBe(false);
    expect(rootInboxSession(entries, 1).holdsBatch(ids)).toBe(false);
    expect(sessionHoldsInboxBatch(entries.slice(0, 1), ids)).toBe(false);
    expect(sessionHoldsInboxBatch(entries, ids, 1)).toBe(false);
    expect(sessionHoldsInboxBatch([{ type: "message", customType: "pi-fabric-inbox", details: { ids } }], ids)).toBe(false);
    expect((await box.next(rootInboxSession(entries))).events).toEqual([]);
    expect((await inbox().next(rootInboxSession(entries))).events).toEqual([]);
  });

  it("finds the batch message among a session's recent entries", () => {
    const entry = { type: "custom_message", customType: "pi-fabric-inbox", details: { ids: ["a", "b"] } };
    expect(sessionHoldsInboxBatch([{ type: "message" }, entry], ["a", "b"])).toBe(true);
    expect(sessionHoldsInboxBatch([entry], ["a", "c"])).toBe(false);
    expect(sessionHoldsInboxBatch([], ["a"])).toBe(false);
  });
});

// smarty-dev#1595: an idle Main wakes for its inbox on a timer. Every wake is a model turn
// (smarty-dev#1579): only addressed events, at most one wake per cooldown unless one is urgent.
describe("RootInbox.wake", () => {
  const idle = () => true;
  const wakes = (mesh: MeshStore) => mesh.read({ after: 0, limit: 500 }).filter((event) => event.topic === "fabric.inbox.wake");

  it("wakes once for an addressed event older than the grace, and publishes one wake event", async () => {
    const { mesh, clock, inbox, work, texts } = setup();
    const box = inbox(60_000);
    await box.next(held);
    await work("addressed");
    expect(await box.wake(held, idle)).toBeUndefined();            // inside the steer grace
    clock.advance(61_000);
    const batch = await box.wake(notHeld, idle);
    expect(texts(batch!.events)).toEqual(["addressed"]);
    // A second tick before the turn holds it: no second delivery.
    expect(await box.wake(notHeld, idle)).toBeUndefined();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(wakes(mesh).map((event) => [event.kind, event.from.id, event.data])).toEqual([
      ["idle-wake", me.id, { count: 1, reason: "idle", ids: batch!.events.map((event) => event.id) }],
    ]);
  });

  it.each(["answer", "blocker", "handoff", "completion"])("queues a late addressed %s without a turn, including a mixed fresh batch", async (kind) => {
    const { mesh, work } = setup();
    let now = Date.now();
    vi.spyOn(Date, "now").mockImplementation(() => now);
    const box = new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0, wakeCooldownMs: 0 });
    box.start();
    const late = await work("late actionable result", me.id, "fleet.work.tasks", "late", kind);
    now += 3 * 60 * 60_000;
    const pending = await box.wake(notHeld, idle);
    expect(pending!.events.map(event => event.id)).toEqual([late.id]);
    expect(wakes(mesh)).toEqual([]);
    const entries: Array<{ type: string } & ReturnType<typeof rootInboxMessage>> = [];
    const options: Array<Parameters<ExtensionAPI["sendMessage"]>[1]> = [];
    const pi = { sendMessage: (message: ReturnType<typeof rootInboxMessage>, delivery: Parameters<ExtensionAPI["sendMessage"]>[1]) => {
      entries.push({ type: "custom_message", ...message }); options.push(delivery);
    } } as unknown as ExtensionAPI;
    deliverRootInbox(pi, pending!.events);
    expect(entries.map(entry => entry.details.ids)).toEqual([[late.id]]);
    expect(options).toEqual([{ deliverAs: "followUp", triggerTurn: false }]);
    expect(await box.wake(rootInboxSession(entries), idle)).toBeUndefined();
    // A mixed batch can wake for fresh work only, without a late message triggering it.
    const another = await work("another late result", me.id, "fleet.work.tasks", "another", kind);
    now += 3 * 60 * 60_000;
    const fresh = await work("fresh work"); now++;
    const mixed = await box.wake(rootInboxSession(entries), idle);
    expect(mixed!.events.map(event => event.id)).toEqual([another.id, fresh.id]);
    deliverRootInbox(pi, mixed!.events);
    expect(entries.map(entry => entry.details.ids)).toEqual([[late.id], [another.id], [fresh.id]]);
    expect(options.slice(1)).toEqual([
      { deliverAs: "followUp", triggerTurn: false }, { deliverAs: "followUp", triggerTurn: true },
    ]);
    expect((await box.next(rootInboxSession(entries))).events).toEqual([]);
    await box.close();
    expect(wakes(mesh).map(event => event.data)).toEqual([{ count: 1, reason: "idle", ids: [fresh.id] }]);
  });

  it("never wakes for a broadcast without `to`, or for an event to someone else", async () => {
    const { mesh, clock, inbox, work } = setup();
    const box = inbox(0, 0);
    await box.next(held);
    await mesh.publish({ topic: "fleet.work.pi-fabric.1", kind: "p0", from: peer, text: "to everyone" });
    await work("to another", "session:other", "fleet.work.pi-fabric.1", "k", "p0");
    clock.advance(1);
    expect(await box.wake(held, idle)).toBeUndefined();
    expect(wakes(mesh)).toEqual([]);
  });

  it("wakes none for a shadow copy whose steer the session holds", async () => {
    const { clock, inbox, work } = setup();
    const box = inbox(0, 0);
    await box.next(held);
    await work("steered", me.id, "fleet.work.pi-fabric.1", "k1");
    clock.advance(1);
    expect(await box.wake(withSteers([{ from: peer.id, data: { key: "k1" } }]), idle)).toBeUndefined();
  });

  it("coalesces: two events a minute apart give one wake, a p0 wakes at once, and after 5 min the next batch wakes", async () => {
    const { clock, inbox, work, texts } = setup();
    const box = inbox(0);
    await box.next(held);
    await work("first");
    clock.advance(1);
    expect(texts((await box.wake(held, idle))!.events)).toEqual(["first"]);
    clock.advance(60_000);
    await work("a minute later");
    clock.advance(1);
    expect(await box.wake(held, idle)).toBeUndefined();
    // The held-back event is not pending, so a p0 behind it still wakes at once, with both.
    await work("urgent", me.id, "fleet.work.pi-fabric.2", "urgent", "p0");
    clock.advance(1);
    expect(texts((await box.wake(held, idle))!.events)).toEqual(["a minute later", "urgent"]);
    await work("steer kind", me.id, "fleet.work.pi-fabric.3", "s", "steer");
    clock.advance(1);
    expect(texts((await box.wake(held, idle))!.events)).toEqual(["steer kind"]);
    await work("later");
    clock.advance(4 * 60_000);
    expect(await box.wake(held, idle)).toBeUndefined();
    clock.advance(60_000);
    expect(texts((await box.wake(held, idle))!.events)).toEqual(["later"]);
  });

  it("leaves the batch pending, with no wake counted, when a turn started during the read", async () => {
    const { mesh, clock, inbox, work, texts } = setup();
    const box = inbox(0);
    await box.next(held);
    await work("raced");
    clock.advance(1);
    expect(await box.wake(held, () => false)).toBeUndefined();
    // The turn that started takes it at its own start, and the next idle wake is not held back.
    expect(texts((await box.next(notHeld)).events)).toEqual(["raced"]);
    expect(texts((await box.wake(notHeld, idle))!.events)).toEqual(["raced"]);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(wakes(mesh)).toHaveLength(1);
  });

  it("wakes at once for an urgent event behind a full ordinary batch, by count or by text (review F1)", async () => {
    const { mesh, clock, inbox, work, texts } = setup();
    const box = inbox(0);
    await box.next(held);
    await work("first");
    clock.advance(1);
    expect(await box.wake(held, idle)).toBeDefined();               // the cooldown starts here
    for (let index = 1; index <= 20; index++) await work(`item ${index}`);
    await work("urgent", me.id, "fleet.work.pi-fabric.2", "urgent", "p0");
    clock.advance(1);
    const first = await box.wake(held, idle);
    expect(first!.events).toHaveLength(20);
    // The settle of that run brings the next batch, with the urgent event: none is skipped.
    expect(texts((await box.next(held)).events)).toEqual(["urgent"]);
    for (let index = 1; index <= 4; index++) await work(`${index}${"x".repeat(9_000)}`);
    await work("urgent steer", me.id, "fleet.work.pi-fabric.3", "s", "steer");
    clock.advance(1);
    expect((await box.wake(held, idle))!.events).toHaveLength(4);
    expect(texts((await box.next(held)).events)).toEqual(["urgent steer"]);
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(wakes(mesh).map((event) => (event.data as { reason: string }).reason)).toEqual(["idle", "p0", "p0"]);
    // Without an urgent event, a full batch inside the cooldown still waits.
    for (let index = 1; index <= 21; index++) await work(`plain ${index}`);
    clock.advance(1);
    expect(await box.wake(held, idle)).toBeUndefined();
  });

  it("starts at the moment it becomes available, so an event before the first read is not skipped (review F3)", async () => {
    const { clock, inbox, work, texts } = setup();
    await work("before the root existed");
    const box = inbox(0);
    box.start();
    await work("after start, before the first tick");
    clock.advance(1);
    expect(texts((await box.wake(held, idle))!.events)).toEqual(["after start, before the first tick"]);
  });
});

