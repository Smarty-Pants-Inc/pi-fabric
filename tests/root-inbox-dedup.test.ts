import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, confirmedRootInboxSession, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

const roots: string[] = [];
const me: MeshIdentity = { id: "session:recipient", name: "recipient", kind: "main" };
const peer: MeshIdentity = { id: "session:sender", name: "sender", kind: "main" };
const HOUR = 60 * 60_000;
const setup = (maxEventBytes = 64 * 1024) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-dedup-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), maxEventBytes, 500);
  const entries: unknown[] = [];
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = (options = {}, identity = me) => new RootInbox(mesh, identity, { now: () => now, steerGraceMs: 0, ...options });
  const pi = {
    sendMessage: (message: unknown) => entries.push({ type: "custom_message", timestamp: new Date(now).toISOString(), ...(message as object) }),
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

  it("does not re-deliver a replayed dedupeKey with a new event id, even across reload", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const packet = { topic: "fleet.work.inbox-receipts", kind: "rerouted", from: peer, to: me.id,
      dedupeKey: "inbox-disposition:replayed", text: "rerouted notification" };
    const first = await h.mesh.publish(packet);
    h.advance(1);
    const delivered = await inbox.next(h.session());
    expect(delivered.events.map(event => event.id)).toEqual([first.id]);
    const execute = vi.fn();
    delivered.events.forEach(execute);
    h.entries.push({ type: "custom_message", ...rootInboxMessage(delivered.events) });
    await inbox.next(h.session()); // Confirm and persist the consumer's key receipt.
    h.entries.length = 0;
    // Replay at the consumer boundary, not via publish (which now returns its receipt).
    const replay = { ...first, id: "replayed-with-a-new-id", sequence: first.sequence + 1 };
    fs.appendFileSync(path.join(h.mesh.root, "events.jsonl"), `${JSON.stringify(replay)}\n`);
    fs.writeFileSync(path.join(h.mesh.root, "sequence"), `${replay.sequence}\n`);
    const batch = await h.box().next(h.session());
    batch.events.forEach(execute);
    expect(batch.events).toEqual([]);
    expect(execute).toHaveBeenCalledTimes(1);
    // A different maintenance owner replaying the same host-only key is also suppressed;
    // an unrelated publication key remains deliverable.
    const other = await h.mesh.publish({ ...packet, from: { ...peer, id: "session:other-sender" }, dedupeKey: "other-publication" });
    const otherReplay = { ...other, id: "other-sender-replay", sequence: other.sequence + 1, dedupeKey: packet.dedupeKey };
    fs.appendFileSync(path.join(h.mesh.root, "events.jsonl"), `${JSON.stringify(otherReplay)}\n`);
    h.advance(1);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([other.id]);
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

describe("confirmed persisted Main/inbox receipts (review F5)", () => {
  const persisted = () => {
    const h = setup();
    let manager = SessionManager.create(h.root, path.join(h.root, "sessions"));
    const pi = {
      on: () => undefined,
      sendMessage: (message: { customType: string; content: string; display: boolean; details: unknown }) =>
        manager.appendCustomMessageEntry(message.customType, message.content, message.display, message.details),
    } as unknown as ExtensionAPI;
    const main = new MainAgentController(pi, me.id, true, h.root, "persisted");
    main.attachFollowUpDrain({ isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false },
      sessionManager: { getEntries: () => manager.getEntries(), getSessionFile: () => manager.getSessionFile(), isPersisted: () => true },
    } as unknown as ExtensionContext, 0, path.join(h.root, "persisted-followups.json"));
    return { ...h, main, manager: () => manager, reload: () => { manager = SessionManager.open(manager.getSessionFile()!); },
      session: () => confirmedRootInboxSession(manager),
      followUp: () => main.deliverAgent({ from: peer, message: "native work", delivery: "followUp", data: { key: "work:1" }, triggerTurn: false }),
      receipts: (inbox: RootInbox) => (h.mesh.get(inbox.key, { fresh: true })?.value as { delivered?: string[] } | undefined)?.delivered ?? [],
      record: (events: Parameters<typeof rootInboxMessage>[0]) => {
        const message = rootInboxMessage(events);
        manager.appendCustomMessageEntry(message.customType, message.content, message.display, message.details);
      },
    };
  };

  it("never promotes failed native appends, retries the shadow after reload and caches only the confirmed retry", async () => {
    const h = persisted(); const inbox = h.box(); inbox.start();
    h.manager().appendMessage(fauxAssistantMessage("warm persisted session"));
    const file = h.manager().getSessionFile()!;
    const backup = `${file}.backup`;
    try {
      fs.renameSync(file, backup); fs.mkdirSync(file);
      // Injection is below the native in-memory index, at the real session append boundary.
      expect(() => h.followUp()).toThrow();
      expect(h.manager().getEntries().filter(entry => entry.type === "custom_message")).toHaveLength(1);
      fs.rmSync(file, { recursive: true }); fs.renameSync(backup, file);
      await inbox.next(h.session()); // Drain BEFORE there is a shadow to recover.
      expect(h.receipts(inbox)).toEqual([]);
      h.main.closeFollowUpDrain(); h.reload();
      expect(h.manager().getEntries().filter(entry => entry.type === "custom_message")).toHaveLength(0);
      const event = await h.publish(); h.advance(1);
      const retry = h.box();
      const batch = await retry.next(h.session());
      expect(batch.events.map(event => event.id)).toEqual([event.id]);
      expect(h.receipts(retry)).toEqual([]);
      h.record(batch.events);
      expect((await retry.next(h.session())).events).toEqual([]);
      expect(h.receipts(retry).length).toBeGreaterThan(0);
      await h.publish(); h.advance(1);
      expect((await h.box().next(h.session())).events).toEqual([]);
      expect(h.manager().getEntries().filter(entry => entry.type === "custom_message" && entry.customType === "pi-fabric-inbox")).toHaveLength(1);
    } finally { h.main.closeFollowUpDrain(); }
  });

  it("keeps deferred first native and inbox writes recoverable until the first assistant confirms them", async () => {
    const h = persisted(); const inbox = h.box(); inbox.start();
    try {
      h.followUp();
      expect(h.manager().getEntries().filter(entry => entry.type === "custom_message")).toHaveLength(1);
      expect(fs.existsSync(h.manager().getSessionFile()!)).toBe(false);
      await inbox.next(h.session());
      expect(h.receipts(inbox)).toEqual([]);
      const event = await h.publish(); h.advance(1);
      const batch = await inbox.next(h.session());
      expect(batch.events.map(event => event.id)).toEqual([event.id]);
      h.record(batch.events); // Also deferred: it must NOT commit the pending batch.
      expect(h.session().holdsBatch([event.id])).toBe(false);
      expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([event.id]);
      expect(h.receipts(inbox)).toEqual([]);
      h.manager().appendMessage(fauxAssistantMessage("first durable write"));
      expect(h.session().holdsBatch([event.id])).toBe(true);
      expect((await inbox.next(h.session())).events).toEqual([]);
      expect(h.receipts(inbox).length).toBeGreaterThan(0);
      await h.publish(); h.advance(1);
      expect((await h.box().next(h.session())).events).toEqual([]);
    } finally { h.main.closeFollowUpDrain(); }
  });

  it("does not cache an unconfirmed session barrier and rechecks before suppressing confirmed native redelivery", async () => {
    const h = persisted(); const inbox = h.box(); inbox.start();
    try {
      h.followUp(); h.manager().appendMessage(fauxAssistantMessage("persisted"));
      const fileStat = fs.statSync(h.manager().getSessionFile()!);
      const sync = fs.fsyncSync.bind(fs);
      const barrier = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
        const stat = fs.fstatSync(fd);
        if (stat.dev === fileStat.dev && stat.ino === fileStat.ino) throw new Error("session receipt barrier failed");
        sync(fd);
      });
      try {
        await inbox.next(h.session());
        expect(h.receipts(inbox)).toEqual([]);
      } finally { barrier.mockRestore(); }
      await inbox.next(h.session());
      expect(h.receipts(inbox).length).toBeGreaterThan(0);
      const receipts = h.receipts(inbox);
      await h.publish(); h.advance(1);
      expect((await h.box().next(h.session())).events).toEqual([]);
      expect(h.receipts(inbox)).toEqual(expect.arrayContaining(receipts));
    } finally { h.main.closeFollowUpDrain(); }
  });
});

describe("receipt capacity and strong identities (review F1/F2)", () => {
  it.each(["deliveryId", "messageId"])("keeps distinct %s values on a shared ref after a native receipt and reload", async (field) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.followUp({ ref: "ticket:42", [field]: "A" });
    await inbox.next(h.session());
    h.entries.length = 0;
    await h.publish({ ref: "ticket:42", [field]: "A" });
    const b = await h.publish({ ref: "ticket:42", [field]: "B" }); h.advance(1);
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(batch.events.map(event => event.id)).toEqual([b.id]);
    h.entries.push({ type: "custom_message", ...rootInboxMessage(batch.events) });
    await recovered.next(h.session());
    await h.publish({ ref: "ticket:42", [field]: "B" }); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it.each(["deliveryId", "messageId"])("delivers two distinct %s values on one ref once each, suppressing true shadows in the batch", async (field) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const a = await h.publish({ ref: "ticket:42", [field]: "A" });
    const b = await h.publish({ ref: "ticket:42", [field]: "B" });
    await h.publish({ ref: "ticket:42", [field]: "A" }); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(batch.events.map(event => event.id)).toEqual([a.id, b.id]);
    h.entries.push({ type: "custom_message", ...rootInboxMessage(batch.events) });
    await inbox.next(h.session());
    h.entries.length = 0;
    await h.publish({ ref: "ticket:42", [field]: "A" });
    await h.publish({ ref: "ticket:42", [field]: "B" }); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it.each([64 * 1024, 256 * 1024])("recovers at and above the %i-byte mesh limit with pending headroom", async (limit) => {
    const h = setup(limit); const inbox = h.box(); inbox.start();
    // Real native follow-ups contribute two 64-character receipt hashes each.
    // First drain approximates the value limit; then cross it on the upgrade/history drain.
    const threshold = Math.floor((limit - 26) / 134);
    for (let index = 0; index < threshold; index++) {
      h.followUp({ key: `capacity:${index}` }); h.advance(1);
    }
    expect(Buffer.byteLength(JSON.stringify({ after: 0, delivered: [...h.session().delivered!] }))).toBeLessThanOrEqual(limit);
    await inbox.next(h.session());
    for (let index = threshold; index < threshold + 40; index++) {
      h.followUp({ key: `capacity:${index}` }); h.advance(1);
    }
    expect(Buffer.byteLength(JSON.stringify({ after: 0, delivered: [...h.session().delivered!] }))).toBeGreaterThan(limit);
    // Even an evicted old receipt is matched from canonical session history, not re-injected.
    await h.publish({ key: "capacity:0" });
    const fresh = await h.publish({ key: "fresh unseen" }); h.advance(1);
    const batch = await h.box().next(h.session());
    expect(batch.events.map(event => event.id)).toEqual([fresh.id]);
    const value = h.mesh.get(inbox.key)!.value as { delivered: string[]; pending?: unknown };
    expect(value.pending).toBeDefined();
    expect(value.delivered.length).toBeLessThanOrEqual(1024);
    expect(Buffer.byteLength(JSON.stringify(value))).toBeLessThan(limit * 0.75);
    const last = [...rootInboxSession([{ type: "custom_message", customType: "pi-fabric-agent-message",
      details: { from: peer, data: { key: `capacity:${threshold + 39}` } } }]).delivered!][0];
    const first = [...rootInboxSession([{ type: "custom_message", customType: "pi-fabric-agent-message",
      details: { from: peer, data: { key: "capacity:0" } } }]).delivered!][0];
    expect(value.delivered).toContain(last);
    expect(value.delivered).not.toContain(first);
    h.entries.length = 0;
    // Reload cannot lose the pending work, and the newest native shadow is still suppressed.
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([fresh.id]);
    h.entries.push({ type: "custom_message", ...rootInboxMessage(batch.events) });
    await h.box().next(h.session());
    h.entries.length = 0;
    await h.publish({ key: `capacity:${threshold + 39}` });
    const another = await h.publish({ key: "another unseen" }); h.advance(1);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual([another.id]);
  });

  it("migrates an at-limit legacy receipt value and retains pending recovery", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    for (let index = 0; index < 488; index++) { h.followUp({ key: `legacy:${index}` }); h.advance(1); }
    const delivered = [...h.session().delivered!];
    const fresh = [];
    for (let index = 0; index < 20; index++) fresh.push(await h.publish({ key: `legacy fresh:${index}` }));
    h.advance(1);
    // Old runtime's string-only ledger fit before pending metadata was added.
    const legacy = { after: 0, delivered };
    expect(Buffer.byteLength(JSON.stringify(legacy))).toBeGreaterThan(64 * 1024 - 150);
    await h.mesh.put({ key: inbox.key, identity: me, value: legacy });
    h.entries.length = 0;
    const recovered = h.box();
    expect((await recovered.next(h.session())).events.map(event => event.id)).toEqual(fresh.map(event => event.id));
    const saved = h.mesh.get(inbox.key)!.value as { delivered: string[]; deliveredAt: number[]; pending?: unknown };
    expect(saved.deliveredAt).toHaveLength(saved.delivered.length);
    expect(saved.pending).toBeDefined();
    expect(Buffer.byteLength(JSON.stringify(saved))).toBeLessThan(64 * 1024 * 0.75);
    expect((await h.box().next(h.session())).events.map(event => event.id)).toEqual(fresh.map(event => event.id));
  });

  it.each(["deliveryId", "messageId"])("does not let a shared key override distinct %s values", async (field) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.followUp({ key: "shared-key", ref: "ticket:42", [field]: "A" });
    await h.publish({ key: "shared-key", ref: "ticket:42", [field]: "A" });
    const b = await h.publish({ key: "shared-key", ref: "ticket:42", [field]: "B" }); h.advance(1);
    expect((await inbox.next(h.session())).events.map(event => event.id)).toEqual([b.id]);
  });

  it("delivery IDs outrank a shared message ID in one batch", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const a = await h.publish({ ref: "ticket:42", messageId: "same", deliveryId: "A" });
    const b = await h.publish({ ref: "ticket:42", messageId: "same", deliveryId: "B" });
    await h.publish({ ref: "ticket:42", messageId: "same", deliveryId: "A" }); h.advance(1);
    expect((await inbox.next(h.session())).events.map(event => event.id)).toEqual([a.id, b.id]);
  });

  it("prunes expired receipts durably on an empty reload drain", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.followUp({ key: "old without shadow" });
    await inbox.next(h.session());
    h.advance(3 * HOUR);
    expect((await h.box().next(h.session())).events).toEqual([]);
    expect((h.mesh.get(inbox.key)!.value as { delivered: string[] }).delivered).toEqual([]);
  });

  it("prunes expired native receipts without refreshing their age on every history scan", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.followUp({ key: "old" });
    await inbox.next(h.session());
    expect((h.mesh.get(inbox.key)!.value as { delivered: string[] }).delivered.length).toBeGreaterThan(0);
    const old = await h.publish({ key: "old" });
    h.advance(3 * HOUR);
    expect(await h.box().next(h.session())).toMatchObject({ events: [], skippedStale: 1 });
    expect((h.mesh.get(inbox.key)!.value as { delivered: string[] }).delivered).toEqual([]);
    expect(old).toBeDefined();
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
