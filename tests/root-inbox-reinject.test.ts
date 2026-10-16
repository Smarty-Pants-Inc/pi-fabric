import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import {
  INBOX_INJECT_MAX_TEXT_BYTES, RootInbox, rootInboxMessage, rootInboxSession, type RootInboxSession,
} from "../src/topology/root-inbox.js";

// R-token-waste (ops-lead): the same 20-event <fabric-inbox> batch (~15k tokens) was injected on
// several turns in a row, although every event had already been delivered as a followUp.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:ops-lead", name: "ops-lead", kind: "main" };
const peer: MeshIdentity = { id: "session:dev-lead", name: "dev-lead", kind: "main" };
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const TOPIC = "fleet.work.lanes";

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-reinject-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const entries: unknown[] = [];
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = () => new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0 });
  const pi = {
    sendMessage: (message: unknown) => entries.push({ type: "custom_message", timestamp: new Date(now).toISOString(), ...(message as object) }),
  } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, me.id, true, root, "ops-lead");
  const publish = (data: unknown, text = "shadow", topic = TOPIC, kind = "report") =>
    mesh.publish({ topic, kind, from: peer, to: me.id, text, data });
  return { root, mesh, entries, pi, box, main, publish, advance: (ms: number) => { now += ms; }, session: () => rootInboxSession(entries) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("root inbox injects a batch once (R-token-waste)", () => {
  it("never injects a consumed batch again, unconfirmed, across turns and a restart", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const events: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) events.push(await h.publish({ key: `report:${index}`, ...(index === 3 ? { deliveryId: "delivery:report:3" } : {}) }, `report ${index}`));
    h.advance(1);
    const batch = await inbox.next(notHeld);
    expect(batch.events.map(event => event.id)).toEqual(events.map(event => event.id));
    // Before the fix: an unconfirmed pending batch came back on every reconcile.
    expect((await h.box().next(notHeld)).events).toHaveLength(20);
    let shown = rootInboxMessage(batch.events).details;
    await inbox.consume(shown.ids, shown.receipts);
    let remaining = await inbox.next(notHeld);
    while (remaining.events.length) {
      shown = rootInboxMessage(remaining.events).details;
      await inbox.consume(shown.ids, shown.receipts);
      remaining = await inbox.next(notHeld);
    }
    for (let turn = 0; turn < 5; turn++) expect((await inbox.next(notHeld)).events).toEqual([]);
    expect((await h.box().next(notHeld)).events).toEqual([]);
    const cursor = h.mesh.get(inbox.key, { fresh: true })!.value as { after: number; pending?: unknown };
    expect(cursor.pending).toBeUndefined();
    expect(cursor.after).toBe(events.at(-1)!.sequence);
    // A later shadow copy of consumed work has the same explicit delivery identity.
    await h.publish({ key: "report:3", deliveryId: "delivery:report:3" }); h.advance(1);
    expect((await h.box().next(notHeld)).events).toEqual([]);
    // New work still arrives.
    const fresh = await h.publish({ key: "report:new" }); h.advance(1);
    expect((await h.box().next(notHeld)).events.map(event => event.id)).toEqual([fresh.id]);
  });

  it("delivers a distinct messageId when a follow-up carried only the same work key", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: peer, message: "already handled", delivery: "followUp", data: { key: "lane:7709", topic: TOPIC } });
    const distinct = await h.publish({ key: "lane:7709", messageId: "sender-local-7709" }); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(batch.events.map(event => event.id)).toEqual([distinct.id]);
    await inbox.consume(rootInboxMessage(batch.events).details.ids, rootInboxMessage(batch.events).details.receipts);
    // A carrier with its own, different identity is distinct work and stays deliverable.
    h.main.deliverAgent({ from: peer, message: "other", delivery: "followUp", data: { key: "lane:42", messageId: "A", topic: TOPIC } });
    const other = await h.publish({ key: "lane:42", messageId: "B" }); h.advance(1);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([other.id]);
  });

  it.each(["steer", "followUp"] as const)("suppresses a shadow whose messageId is the %s's control delivery id", async (delivery) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.attachFollowUpDrain({ isIdle: () => true, hasPendingMessages: () => false,
      sessionManager: { getEntries: () => h.entries }, signal: { aborted: false },
    } as unknown as ExtensionContext, 0, path.join(h.root, "followups.json"));
    try {
      // agents-message-router delivers a control command with deliveryId = commandId.
      h.main.deliverAgent({ from: peer, message: "delivered", delivery, deliveryId: "cmd-1125444" });
      await h.publish({ messageId: "cmd-1125444", key: "lane:1" }); h.advance(1);
      expect((await inbox.next(h.session())).events).toEqual([]);
    } finally { h.main.closeFollowUpDrain(); }
  });

  it("injects distinct event identities in a backlog after key-only follow-ups", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const expected: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) {
      if (index % 4 === 0) {
        expected.push(await h.publish({ key: `work:${index}` }));
        continue;
      }
      h.main.deliverAgent({ from: peer, message: `work ${index}`, delivery: "followUp", data: { key: `work:${index}`, topic: TOPIC } });
      expected.push(await h.publish({ key: `work:${index}`, messageId: `local:${index}` }));
    }
    h.advance(1);
    expect((await inbox.next(h.session())).events.map(event => event.id)).toEqual(expected.map(event => event.id));
  });

  // review-fleet-2 security P1 on cdb29afd: a work-key receipt matched on sender + key only, so an
  // unrelated event on another topic with a reused key was suppressed instead of reaching the lead.
  it("does not suppress same-key notifications across topics or without shared delivery identity", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const first = await h.publish({ key: "lane:1", deliveryId: "delivery:lane:1" }, "first", "fleet.work.lanes"); h.advance(1);
    const batch = await inbox.next(notHeld);
    expect(batch.events.map(event => event.id)).toEqual([first.id]);
    const { details } = rootInboxMessage(batch.events);
    await inbox.consume(details.ids, details.receipts);
    // Same sender, same key, another topic: unrelated work, injected.
    const other = await h.publish({ key: "lane:1" }, "unrelated", "fleet.alarm.ci"); h.advance(1);
    const second = await inbox.next(notHeld);
    expect(second.events.map(event => event.id)).toEqual([other.id]);
    const delivered = rootInboxMessage(second.events).details;
    await inbox.consume(delivered.ids, delivered.receipts);
    // Reusing the key is a distinct notification; only the shared delivery identity marks a true shadow.
    const distinct = await h.publish({ key: "lane:1" }, "new notification", "fleet.work.lanes");
    await h.publish({ key: "lane:1", deliveryId: "delivery:lane:1" }, "copy", "fleet.work.lanes"); h.advance(1);
    expect((await h.box().next(notHeld)).events.map(event => event.id)).toEqual([distinct.id]);
  });

  it("keeps distinct events with the same sender and key across and within topics in one batch", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const a = await h.publish({ key: "lane:2" }, "a", "fleet.work.lanes");
    const b = await h.publish({ key: "lane:2" }, "b", "fleet.work.other");
    const aAgain = await h.publish({ key: "lane:2" }, "a again", "fleet.work.lanes"); h.advance(1);
    expect((await inbox.next(notHeld)).events.map(event => event.id)).toEqual([a.id, b.id, aAgain.id]);
  });

  it("matches a follow-up's work key on the topic it names only, and fails open without a topic", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: peer, message: "handled", delivery: "followUp", data: { key: "lane:3", topic: "fleet.work.lanes" } });
    await h.publish({ key: "lane:3" }, "copy", "fleet.work.lanes");
    const copyWithId = await h.publish({ key: "lane:3", messageId: "copy-3" }, "copy", "fleet.work.lanes");
    const other = await h.publish({ key: "lane:3" }, "unrelated", "fleet.alarm.ci");
    const otherWithId = await h.publish({ key: "lane:3", messageId: "unrelated-3" }, "unrelated", "fleet.alarm.ci"); h.advance(1);
    const first = await inbox.next(h.session());
    expect(first.events.map(event => event.id)).toEqual([copyWithId.id, other.id, otherWithId.id]);
    const delivered = rootInboxMessage(first.events).details;
    await inbox.consume(delivered.ids, delivered.receipts);
    // A follow-up that names no topic has no verifiable topic: its key suppresses nothing.
    h.main.deliverAgent({ from: peer, message: "no topic", delivery: "followUp", data: { key: "lane:4" } });
    const plain = await h.publish({ key: "lane:4" }, "plain");
    const withId = await h.publish({ key: "lane:4", messageId: "local-4" }, "with id"); h.advance(1);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([plain.id, withId.id]);
  });

  it("never suppresses with an older topic-less sender+key receipt", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const legacy = createHash("sha256").update(JSON.stringify([peer.id, "key", "lane:5"])).digest("hex");
    const session: RootInboxSession = { ...notHeld, delivered: new Set([legacy]) };
    await inbox.consume(["older-event"], [legacy]);
    const plain = await h.publish({ key: "lane:5" });
    const withId = await h.publish({ key: "lane:5", messageId: "local-5" }); h.advance(1);
    expect((await inbox.next(session)).events.map(event => event.id)).toEqual([plain.id, withId.id]);
  });

  it("bounds each injection and consumes only displayed events across turns and restart", async () => {
    const h = setup(); let inbox = h.box(); inbox.start();
    const events: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) events.push(await h.publish({ key: `big:${index}`, ...(index === 3 ? { deliveryId: "delivery:report:3" } : {}) }, `info ${index} ${"x".repeat(1400)}`, TOPIC, "info"));
    h.advance(1);
    const batch = await inbox.next(notHeld);
    expect(batch.events).toHaveLength(20);
    const message = rootInboxMessage(batch.events);
    const rendered = message.content.match(/<event /g) ?? [];
    expect(rendered.length).toBeLessThan(20);
    expect(message.content).toContain(`<fabric-inbox count="20" shown="${rendered.length}">`);
    expect(message.content.match(/<overflow /g)).toHaveLength(1);
    expect(message.content).toMatch(new RegExp(`<overflow count="${20 - rendered.length}">\\+${20 - rendered.length} more addressed event`));
    expect(Buffer.byteLength(message.content)).toBeLessThan(INBOX_INJECT_MAX_TEXT_BYTES + 4096);
    expect(message.details.ids).toEqual(batch.events.slice(0, rendered.length).map(event => event.id));

    // Delivery splits by provenance on capable Pi: the budget and the one line hold across messages.
    const sent: Array<{ content: string; details: { ids: string[] } }> = [];
    const capable = { hostCapabilities: { turnProvenance: 1 }, sendMessage: (value: never) => sent.push(value) } as unknown as ExtensionAPI;
    const mixed = batch.events.map((event, index) => ({ ...event, verification: "mesh" as const,
      from: index % 2 ? event.from : { ...event.from, id: `session:writer-${index}` } }));
    deliverRootInbox(capable, mixed);
    const total = sent.reduce((bytes, value) => bytes + Buffer.byteLength(value.content), 0);
    expect(total).toBeLessThan(INBOX_INJECT_MAX_TEXT_BYTES + 4096 * 2);
    expect(sent.map(value => value.content).join("\n").match(/<overflow /g)).toHaveLength(1);
    expect(sent.flatMap(value => value.details.ids)).toEqual(mixed.slice(0, rendered.length).map(event => event.id));

    // Receipt only what each bounded injection rendered; the remainder must survive restart.
    const delivered: string[] = [];
    let current = batch;
    for (let turn = 0; turn < 3; turn++) {
      const before = h.entries.length;
      deliverRootInbox(h.pi, current.events);
      const messages = h.entries.slice(before) as Array<ReturnType<typeof rootInboxMessage>>;
      expect(messages.flatMap(value => value.details.ids)).toHaveLength(Math.min(8, current.events.length));
      for (const value of messages) {
        delivered.push(...value.details.ids);
        await inbox.consume(value.details.ids, value.details.receipts);
      }
      if (turn === 0) {
        await inbox.close();
        inbox = h.box();
      }
      if (turn < 2) current = await inbox.next(h.session());
    }
    expect(delivered).toEqual(events.map(event => event.id));
    expect(new Set(delivered).size).toBe(20);
    expect((await inbox.next(h.session())).events).toEqual([]);
    const cursor = h.mesh.get(inbox.key, { fresh: true })!.value as { after: number; pending?: unknown };
    expect(cursor.pending).toBeUndefined();
    expect(cursor.after).toBe(events.at(-1)!.sequence);

    // A later shadow of consumed work is still suppressed; unrelated fresh work remains deliverable.
    await h.publish({ key: "big:3", deliveryId: "delivery:report:3" }); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
    const fresh = await h.publish({ key: "report:new" }); h.advance(1);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([fresh.id]);
  });
});
