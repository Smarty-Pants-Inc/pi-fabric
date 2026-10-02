import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

const roots: string[] = [];
const me: MeshIdentity = { id: "session:recipient", name: "recipient", kind: "main" };
const peer: MeshIdentity = { id: "session:sender", name: "sender", kind: "main" };
const HOUR = 60 * 60_000;
const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-dedup-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  const entries: unknown[] = [];
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = (options = {}, identity = me) => new RootInbox(mesh, identity, () => [identity.id], { now: () => now, steerGraceMs: 0, ...options });
  const pi = {
    sendMessage: (message: unknown) => entries.push({ type: "custom_message", ...(message as object) }),
  } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, me.id, true, root, "recipient");
  const followUp = (data?: unknown, delivery: "steer" | "followUp" = "followUp") => main.deliverAgent({ from: peer, message: "already handled", delivery, data });
  const publish = (data: unknown = { key: "work:1" }, to = me.id, from = peer) => mesh.publish({ topic: "fleet.work.pi-fabric.3036", kind: "ack", from, to, text: "shadow", data });
  return { root, mesh, entries, box, main, followUp, publish, advance: (ms: number) => { now += ms; }, session: () => rootInboxSession(entries) };
};
afterEach(() => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("recipient-scoped shadow dedup (smarty-dev#3036)", () => {
  it.each(["followUp", "steer"] as const)("remembers a delivered %s before its shadow, beyond 500 entries and a restart", async (delivery) => {
    const h = setup();
    const inbox = h.box(); inbox.start();
    h.followUp({ ref: "pi-fabric#3036", key: "work:1" }, delivery);
    h.entries.push(...Array.from({ length: 600 }, () => ({ type: "message" })));
    await inbox.next(h.session()); // Persist recipient receipts even before any shadow exists.
    h.entries.length = 0; // Reload/compaction no longer exposes the original message.
    await h.publish({ ref: "pi-fabric#3036", key: "work:1" });
    h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("matches a ref-only follow-up, but not another sender or recipient", async () => {
    const h = setup();
    const inbox = h.box(); inbox.start();
    h.followUp({ ref: "unique-work-ref" });
    const shadow = await h.publish({ ref: "unique-work-ref" });
    const otherSender = await h.publish({ ref: "unique-work-ref" }, me.id, { ...peer, id: "session:else" });
    const other: MeshIdentity = { ...me, id: "session:other" };
    const otherBox = h.box({}, other); otherBox.start();
    const otherRecipient = await h.publish({ ref: "unique-work-ref" }, other.id);
    h.advance(1);
    expect((await inbox.next(h.session())).events.map(e => e.id)).toEqual([otherSender.id]);
    expect((await otherBox.next(rootInboxSession([]))).events.map(e => e.id)).toEqual([otherRecipient.id]);
    expect(shadow.id).not.toBe(otherSender.id);
  });

  it("matches the native message id when a shadow has no work key", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const receipt = h.followUp();
    await h.publish({ messageId: receipt.messageId }); h.advance(1);
    expect((await inbox.next(h.session())).events).toEqual([]);
  });

  it("matches a durable delivery id from a real delivered follow-up", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.attachFollowUpDrain({ isIdle: () => true, hasPendingMessages: () => false,
      sessionManager: { getEntries: () => h.entries }, signal: { aborted: false },
    } as unknown as ExtensionContext, 0, path.join(h.root, "followups.json"));
    try {
      h.main.deliverAgent({ from: peer, message: "delivered", delivery: "followUp", deliveryId: "durable:3036" });
      await h.publish({ deliveryId: "durable:3036" }); h.advance(1);
      expect((await inbox.next(h.session())).events).toEqual([]);
    } finally { h.main.closeFollowUpDrain(); }
  });

  it("rechecks a pending shadow when its original follow-up arrives late", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    await h.publish(); h.advance(1);
    expect((await inbox.next(h.session())).events).toHaveLength(1);
    h.followUp({ key: "work:1" });
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("remembers consumed inbox work identities when a new shadow id repeats the work", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    await h.publish(); h.advance(1);
    const batch = await inbox.next(h.session());
    h.entries.push({ type: "custom_message", ...rootInboxMessage(batch.events) });
    await inbox.next(h.session());
    h.entries.length = 0;
    await h.publish(); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("recognises pre-upgrade inbox receipts beyond the recent-entry window", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const event = await h.publish(); h.advance(1);
    // Old bundles wrote ids only, without stable work receipt metadata.
    h.entries.push({ type: "custom_message", customType: "pi-fabric-inbox", details: { ids: [event.id] } });
    h.entries.push(...Array.from({ length: 600 }, () => ({ type: "message" })));
    expect((await inbox.next(h.session())).events).toEqual([]);
    h.entries.length = 0;
    await h.publish(); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("preserves pending recovery uncertainty when the mesh temporarily cannot return an event", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const event = await h.publish(); h.advance(1);
    expect((await inbox.next(h.session())).events.map(e => e.id)).toEqual([event.id]);
    const read = vi.spyOn(h.mesh, "read").mockReturnValue([]);
    expect((await inbox.next(h.session())).events).toEqual([]);
    expect((h.mesh.get(inbox.key)?.value as { pending?: unknown }).pending).toBeDefined();
    read.mockRestore();
    expect((await h.box().next(h.session())).events.map(e => e.id)).toEqual([event.id]);
  });

  it("does not treat a queued or failed native follow-up as delivered", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const held = new MainAgentController({ on: () => undefined, sendMessage: () => { throw new Error("not delivered"); } } as unknown as ExtensionAPI, me.id, true, h.root, "held");
    held.attachFollowUpDrain({ isIdle: () => false, hasPendingMessages: () => false, signal: { aborted: false } } as unknown as ExtensionContext, 120_000);
    try {
      held.deliverAgent({ from: peer, message: "queued", delivery: "followUp", data: { key: "work:1" } });
      await h.publish(); h.advance(1);
      const first = await inbox.next(h.session());
      expect(first.events).toHaveLength(1);
      expect((await h.box().next(h.session())).events.map(e => e.id)).toEqual(first.events.map(e => e.id));
    } finally { held.closeFollowUpDrain(); }
  });
});

describe("addressed-shadow age horizon (smarty-dev#3036)", () => {
  it("skips a 20-hour backlog on the first drain in one summary, without idle wakes", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    for (let index = 0; index < 65; index++) await h.publish({ key: `old:${index}` });
    await h.publish({ key: "not ours" }, "session:else");
    h.advance(20 * HOUR);
    const batch = await inbox.next(h.session());
    expect(batch.events).toEqual([]);
    expect(batch).toMatchObject({ skippedStale: 65, horizonMs: 2 * HOUR });
    expect((await h.box().next(h.session()))).not.toHaveProperty("skippedStale");
    expect(await inbox.wake(h.session(), () => true)).toBeUndefined();
    expect(h.mesh.read({ after: 0, limit: 100 }).filter(e => e.topic === "fabric.inbox.wake")).toEqual([]);
  });

  it("drops stale pending work after restart, rather than replaying 20-event batches", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    await h.publish(); h.advance(1);
    expect((await inbox.next(h.session())).events).toHaveLength(1);
    h.advance(3 * HOUR);
    expect(await h.box().next(h.session())).toMatchObject({ events: [], skippedStale: 1 });
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("honours a configured horizon and preserves fresh work behind a stale backlog", async () => {
    vi.stubEnv("PI_FABRIC_INBOX_HORIZON_MS", String(30 * 60_000));
    const h = setup(); const inbox = h.box(); inbox.start();
    for (let index = 0; index < 25; index++) await h.publish({ key: `old:${index}` });
    h.advance(31 * 60_000);
    const fresh = await h.publish({ key: "fresh" }); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(batch.events.map(e => e.id)).toEqual([fresh.id]);
    expect(batch).toMatchObject({ skippedStale: 25, horizonMs: 30 * 60_000 });
  });
});
