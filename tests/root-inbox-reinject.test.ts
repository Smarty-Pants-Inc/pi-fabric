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
  const publish = (data: unknown, text = "shadow") =>
    mesh.publish({ topic: "fleet.work.lanes", kind: "report", from: peer, to: me.id, text, data });
  return { root, mesh, entries, box, main, publish, advance: (ms: number) => { now += ms; }, session: () => rootInboxSession(entries) };
};
afterEach(() => {
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("root inbox injects a batch once (R-token-waste)", () => {
  it("never injects a consumed batch again, unconfirmed, across turns and a restart", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const events: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) events.push(await h.publish({ key: `report:${index}` }, `report ${index}`));
    h.advance(1);
    const batch = await inbox.next(notHeld);
    expect(batch.events.map(event => event.id)).toEqual(events.map(event => event.id));
    // Before the fix: an unconfirmed pending batch came back on every reconcile.
    expect((await h.box().next(notHeld)).events).toHaveLength(20);
    const { details } = rootInboxMessage(batch.events);
    await inbox.consume(details.ids, details.receipts);
    for (let turn = 0; turn < 5; turn++) expect((await inbox.next(notHeld)).events).toEqual([]);
    expect((await h.box().next(notHeld)).events).toEqual([]);
    const cursor = h.mesh.get(inbox.key, { fresh: true })!.value as { after: number; pending?: unknown };
    expect(cursor.pending).toBeUndefined();
    expect(cursor.after).toBe(events.at(-1)!.sequence);
    // A later shadow copy of consumed work (new event id, same work key) is not injected either.
    await h.publish({ key: "report:3" }); h.advance(1);
    expect((await h.box().next(notHeld)).events).toEqual([]);
    // New work still arrives.
    const fresh = await h.publish({ key: "report:new" }); h.advance(1);
    expect((await h.box().next(notHeld)).events.map(event => event.id)).toEqual([fresh.id]);
  });

  it("suppresses a shadow carrying a messageId when the follow-up carried the same key without an identity", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: peer, message: "already handled", delivery: "followUp", data: { key: "lane:7709" } });
    await h.publish({ key: "lane:7709", messageId: "sender-local-7709" }); h.advance(1);
    expect((await inbox.next(h.session())).events).toEqual([]);
    // A carrier with its own, different identity is distinct work and stays deliverable.
    h.main.deliverAgent({ from: peer, message: "other", delivery: "followUp", data: { key: "lane:42", messageId: "A" } });
    const distinct = await h.publish({ key: "lane:42", messageId: "B" }); h.advance(1);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([distinct.id]);
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

  it("suppresses already-delivered follow-ups in a 20-event backlog, injecting only the undelivered rest", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const undelivered: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) {
      if (index % 4 === 0) {
        undelivered.push(await h.publish({ key: `work:${index}` }));
        continue;
      }
      h.main.deliverAgent({ from: peer, message: `work ${index}`, delivery: "followUp", data: { key: `work:${index}` } });
      await h.publish({ key: `work:${index}`, messageId: `local:${index}` });
    }
    h.advance(1);
    expect((await inbox.next(h.session())).events.map(event => event.id)).toEqual(undelivered.map(event => event.id));
  });

  it("bounds one injection and summarizes the overflow in one line", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    for (let index = 0; index < 20; index++) await h.publish({ key: `big:${index}` }, `report ${index} ${"x".repeat(1400)}`);
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
    expect(message.details.ids).toEqual(batch.events.map(event => event.id));

    // Delivery splits by provenance on capable Pi: the budget and the one line hold across messages.
    const sent: Array<{ content: string; details: { ids: string[] } }> = [];
    const capable = { hostCapabilities: { turnProvenance: 1 }, sendMessage: (value: never) => sent.push(value) } as unknown as ExtensionAPI;
    const mixed = batch.events.map((event, index) => ({ ...event, verification: "mesh" as const,
      from: index % 2 ? event.from : { ...event.from, id: `session:writer-${index}` } }));
    deliverRootInbox(capable, mixed);
    const total = sent.reduce((bytes, value) => bytes + Buffer.byteLength(value.content), 0);
    expect(total).toBeLessThan(INBOX_INJECT_MAX_TEXT_BYTES + 4096 * 2);
    expect(sent.map(value => value.content).join("\n").match(/<overflow /g)).toHaveLength(1);
    expect(sent.flatMap(value => value.details.ids)).toEqual(mixed.map(event => event.id));
  });
});
