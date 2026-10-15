import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#12140: a work key (data.key) is not unique per notification. Two distinct durable
// events with the same sender, topic and key (a watcher's rc=1 then rc=124) must both deliver.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:incident-probe", name: "incident-probe", kind: "main" };
const watcher: MeshIdentity = { id: "fleet-tool:lane-watcher", name: "lane-watcher", kind: "main" };
const TOPIC = "fleet.work.tasks";
const KEY = "watcher-lucky-6289-m4";

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-distinct-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const entries: unknown[] = [];
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = () => new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0, wakeCooldownMs: 0 });
  const pi = {
    sendMessage: (message: unknown) => entries.push({ type: "custom_message", timestamp: new Date(now).toISOString(), ...(message as object) }),
  } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, me.id, true, root, "incident-probe");
  const publish = (text: string, data: Record<string, unknown> = { key: KEY }) =>
    mesh.publish({ topic: TOPIC, kind: "answer", from: watcher, to: me.id, text, data });
  /** The session takes the inbox carrier into a turn: entry written and ids consumed. */
  const take = async (inbox: RootInbox, events: readonly MeshEvent[]) => {
    const message = rootInboxMessage(events);
    entries.push({ type: "custom_message", timestamp: new Date(now).toISOString(), ...message });
    await inbox.consume(message.details.ids, message.details.receipts);
  };
  const replay = (event: MeshEvent) => {
    const sequence = mesh.latestSequence() + 1;
    fs.appendFileSync(path.join(mesh.root, "events.jsonl"), `${JSON.stringify({ ...event, sequence })}\n`);
    fs.writeFileSync(path.join(mesh.root, "sequence"), `${sequence}\n`);
  };
  return { mesh, entries, box, main, publish, take, replay, advance: (ms: number) => { now += ms; }, session: () => rootInboxSession(entries) };
};
const ids = (events: readonly MeshEvent[] | undefined) => (events ?? []).map(event => event.id);

afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("distinct same-key mesh events (smarty-dev#12140)", () => {
  it("wakes an idle root for a second same-key event after the first was consumed, and suppresses a same-id replay", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const first = await h.publish("WATCHER lucky-6289-m4: rc=1"); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([first.id]);
    await h.take(inbox, batch.events);
    expect((await inbox.next(h.session())).events).toEqual([]);

    const second = await h.publish("WATCHER lucky-6289-m4: rc=124"); h.advance(1);
    const woke = await inbox.wake(h.session(), () => true);
    expect(ids(woke?.events)).toEqual([second.id]);
    await h.take(inbox, woke!.events);
    await inbox.close(); // Flush the idle-wake notification before appending the replay.

    // The same envelope id replayed at a new sequence is a duplicate, also after reload.
    h.replay(second); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
    expect(await h.box().wake(h.session(), () => true)).toBeUndefined();
    h.entries.length = 0;
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("delivers distinct same-key events in one batch, once each", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const first = await h.publish("rc=1");
    const second = await h.publish("rc=124"); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(inbox, batch.events);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("never lets a legacy persisted topic-key fallback receipt suppress distinct new events", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    // The hash older runtimes stored for a consumed mesh event's sender/topic/key fallback.
    const legacy = createHash("sha256").update(JSON.stringify([watcher.id, "topic-key", JSON.stringify([TOPIC, KEY])])).digest("hex");
    const now = Date.now();
    await h.mesh.put({ key: inbox.key, identity: me, value: { after: h.mesh.latestSequence(), delivered: [legacy], deliveredAt: [now] } });
    h.entries.push({ type: "custom_message", timestamp: new Date(now).toISOString(), customType: "pi-fabric-inbox",
      details: { ids: ["older-event"], receipts: [legacy] } });
    const first = await h.publish("rc=1");
    const second = await h.publish("rc=124"); h.advance(1);
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(recovered, batch.events);
    const third = await h.publish("rc=0"); h.advance(1);
    expect(ids((await h.box().wake(h.session(), () => true))?.events)).toEqual([third.id]);
  });

  it.each(["messageId", "deliveryId"] as const)("delivers a distinct addressed event with a different %s after a key-only native carrier", async (identity) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "rc=1", delivery: "steer", data: { key: KEY, topic: TOPIC } });
    await h.publish("rc=1 shadow");
    const distinct = await h.publish("rc=124 distinct", { key: KEY, [identity]: `watcher-local-${identity}` }); h.advance(1);
    // The legacy key-only shadow is still suppressed, but its distinct explicit identity is not.
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([distinct.id]);
    await h.take(inbox, batch.events);
    // The native claim persists in the recipient cursor once the session history is gone.
    h.entries.length = 0;
    await h.publish("rc=1 shadow again"); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
    // Another topic or sender is not the claimed work.
    const otherTopic = await h.mesh.publish({ topic: "fleet.work.other", kind: "answer", from: watcher, to: me.id, text: "other", data: { key: KEY } });
    h.advance(1);
    expect(ids((await h.box().next(h.session())).events)).toEqual([otherTopic.id]);
  });

  it("keeps explicit delivery identity shadow suppression for distinct envelope ids", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const first = await h.publish("rc=1", { key: KEY, deliveryId: "delivery-1" });
    await h.publish("rc=1 shadow", { key: KEY, deliveryId: "delivery-1" });
    const second = await h.publish("rc=124", { key: KEY, deliveryId: "delivery-2" }); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(inbox, batch.events);
    await h.publish("rc=124 shadow", { key: KEY, deliveryId: "delivery-2" }); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });
});
