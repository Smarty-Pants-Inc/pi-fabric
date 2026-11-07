import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { NativeRecoveryTransfer } from "../src/core/session-receipts.js";
import { hasDeliveryOutcome } from "../src/mesh/delivery-outcomes.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, ROOT_INBOX_ALARM_TOPIC, ROOT_INBOX_MAX_FAILED_OFFERS, type RootInboxDeadLetter } from "../src/topology/root-inbox.js";
import { eventReceipt, eventReceipts, rootInboxMessage, type RootInboxSession } from "../src/topology/root-inbox-shared.js";

// Keep: data safety and an otherwise expensive-to-reach renderer failure on the real
// MeshStore -> RootInbox -> carrier admission -> consume/delivery-outcome path (#18131).
const me: MeshIdentity = { id: "session:main", name: "Main", kind: "main" };
const peer: MeshIdentity = { id: "session:peer", name: "Peer", kind: "main" };
const empty: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const closes: Array<() => unknown | Promise<unknown>> = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const close of closes.splice(0).reverse()) await close();
});
const setup = (maxBytes = 64 * 1024, grace = 0) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "root-inbox-dead-letter-"));
  closes.push(() => fs.rmSync(root, { recursive: true, force: true }));
  let now = Date.now();
  const store = () => {
    const mesh = new MeshStore(root, maxBytes, 500);
    closes.push(() => mesh.closeState());
    return mesh;
  };
  const mesh = store();
  const box = (source = mesh, identity = me) => {
    const inbox = new RootInbox(source, identity, () => [identity.id], { now: () => now, steerGraceMs: grace, wakeCooldownMs: 0 });
    closes.push(() => inbox.close());
    return inbox;
  };
  const publish = async (input: Partial<Parameters<MeshStore["publish"]>[0]> = {}) => {
    const event = await mesh.publish({ from: peer, to: me.id, topic: "fleet.work.test", kind: "done", text: "work", ...input });
    now = Math.max(now, event.createdAt + 1);
    return event;
  };
  const key = (inbox: RootInbox, id: string) => `${inbox.key}/dead-letter/${createHash("sha256").update(id).digest("hex")}`;
  const row = (inbox: RootInbox, id: string) => inbox.mesh.get(key(inbox, id), { fresh: true })?.value as RootInboxDeadLetter | undefined;
  const saved = (inbox: RootInbox) => inbox.mesh.get(inbox.key, { fresh: true })!.value as { after: number; pending?: { ids: string[] }; delivered?: string[] };
  const delivered = (event: MeshEvent) => hasDeliveryOutcome(root, { eventId: event.id, from: event.from.id, to: me.id, mode: "publish" }, "delivered");
  const alarms = () => mesh.read({ after: 0, topic: ROOT_INBOX_ALARM_TOPIC, limit: 500 }).filter(event => (event.data as { reason?: string })?.reason === "dead_letter");
  return { root, mesh, store, box, publish, row, saved, delivered, alarms, advance: (ms: number) => { now += ms; } };
};
const park = async (inbox: RootInbox) => {
  for (let attempt = 0; attempt < ROOT_INBOX_MAX_FAILED_OFFERS; attempt++) await inbox.next(empty);
};

describe("root inbox bounded failed offers / durable DLQ", () => {
  it("parks a throwing singleton, progresses later valid work, and delivers one addressed owner report across reloads", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: 17 as unknown as string, data: { deliveryId: "bad-delivery", untouched: [1, "two"] } });
    const good = await h.publish({ text: "later valid event" });
    expect(() => rootInboxMessage([bad])).toThrow();
    for (let attempt = 1; attempt < ROOT_INBOX_MAX_FAILED_OFFERS; attempt++) {
      expect((await inbox.next(empty)).events).toEqual([]);
      expect(h.row(inbox, bad.id)).toMatchObject({ status: "failed", failures: attempt });
      expect(inbox.acked(bad.id, bad.sequence)).toBe(false);
    }
    const batch = await inbox.next(empty);
    expect(batch.events.map(event => event.id)).toContain(good.id);
    expect(batch.events.map(event => event.id)).not.toContain(bad.id);
    expect(h.row(inbox, bad.id)).toMatchObject({ status: "parked", failures: ROOT_INBOX_MAX_FAILED_OFFERS });
    expect(inbox.readDeadLetter(bad.id)).toEqual(bad);
    await inbox.consume([good.id], eventReceipts(good));
    expect(h.saved(inbox).after).toBeGreaterThanOrEqual(good.sequence);
    expect(inbox.acked(bad.id, bad.sequence)).toBe(false); // Cursor/high-water is NOT delivery.
    expect(h.saved(inbox).delivered).not.toContain(eventReceipt(bad.id));
    expect(h.delivered(bad)).toBe(false);
    expect(h.delivered(good)).toBe(true);
    await inbox.consume([bad.id], eventReceipts(bad)); // A stale parked carrier is not a replay ack.
    expect(inbox.acked(bad.id, bad.sequence)).toBe(false);
    expect(h.delivered(bad)).toBe(false);
    await inbox.close(); // Existing notification queue flush, not a timer or a model turn.
    expect(h.alarms()).toHaveLength(1);
    expect(h.alarms()[0]).toMatchObject({ to: me.id, data: { eventId: bad.id, reason: "dead_letter" } });
    const reloaded = h.box(h.store());
    const report = await reloaded.next(empty, { priority: true });
    expect(rootInboxMessage(report.events).details.ids).toContain(h.alarms()[0]!.id);
    expect(reloaded.acked(bad.id, bad.sequence)).toBe(false);
    await reloaded.consume(report.events.map(event => event.id), report.events.flatMap(eventReceipts));
    await reloaded.close();
    const again = h.box(h.store());
    expect((await again.next(empty)).events).toEqual([]);
    expect(again.acked(bad.id, bad.sequence)).toBe(false);
    expect(h.alarms()).toHaveLength(1);
  });

  it("persists actual failed-attempt counts through fresh MeshStore/RootInbox instances", async () => {
    const h = setup(); let inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: { malformed: true } as unknown as string });
    for (let attempt = 1; attempt <= ROOT_INBOX_MAX_FAILED_OFFERS; attempt++) {
      await inbox.next(empty);
      expect(h.row(inbox, bad.id)?.failures).toBe(attempt);
      await inbox.close();
      inbox = h.box(h.store());
    }
    expect(h.row(inbox, bad.id)?.status).toBe("parked");
    expect(inbox.readDeadLetter(bad.id)).toEqual(bad);
    expect(inbox.acked(bad.id, bad.sequence)).toBe(false);
  });

  it.each([-1e17, 1e17])("probes invalid Date %s before lateness AND nonpriority steer-grace holds", async createdAt => {
    const h = setup(64 * 1024, 60_000), first = h.box();
    await first.next(empty); // Persist the original cursor before creating retained history.
    const bad = await h.publish({ kind: "handoff" });
    const good = await h.publish({ text: "valid after malformed date" });
    await first.close();
    // Historical/mixed-version retained envelope: finite JSON number, outside Date's range.
    const file = path.join(h.root, "events.jsonl");
    const events = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as MeshEvent);
    events.find(event => event.id === bad.id)!.createdAt = createdAt;
    fs.writeFileSync(file, events.map(event => JSON.stringify(event)).join("\n") + "\n");
    h.advance(60_001);
    const inbox = h.box(h.store());
    for (let attempt = 1; attempt <= ROOT_INBOX_MAX_FAILED_OFFERS; attempt++) {
      const batch = await inbox.next(empty, { turnEnd: true }); // Explicitly NONpriority.
      expect(h.row(inbox, bad.id)?.failures).toBe(attempt);
      if (attempt === ROOT_INBOX_MAX_FAILED_OFFERS) expect(batch.events.map(event => event.id)).toContain(good.id);
    }
    expect(inbox.readDeadLetter(bad.id).createdAt).toBe(createdAt);
  });

  it("parks a malformed retained sender before receipt/grace priority lookahead can throw", async () => {
    const h = setup(64 * 1024, 60_000), first = h.box(); await first.next(empty);
    const bad = await h.publish({ kind: "handoff" }), good = await h.publish(); await first.close();
    const file = path.join(h.root, "events.jsonl");
    const events = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line) as MeshEvent);
    events.find(event => event.id === bad.id)!.from = null as unknown as MeshIdentity;
    fs.writeFileSync(file, events.map(event => JSON.stringify(event)).join("\n") + "\n");
    const inbox = h.box(h.store());
    for (let attempt = 0; attempt < ROOT_INBOX_MAX_FAILED_OFFERS; attempt++) await inbox.next(empty, { priority: true });
    expect(h.row(inbox, bad.id)?.status).toBe("parked");
    expect(inbox.readDeadLetter(bad.id)).toEqual({ ...bad, from: null });
    expect((await inbox.next(empty, { priority: true })).events.map(event => event.id)).toContain(good.id);
    expect(inbox.acked(bad.id, bad.sequence)).toBe(false);
  });

  it("parks singleton action admission failure, not only renderer exceptions", async () => {
    const h = setup(256 * 1024), inbox = h.box(); inbox.start();
    const bad = await h.publish({ kind: "handoff", from: { ...peer, name: "name".repeat(36_000) } });
    const good = await h.publish();
    expect(rootInboxMessage([bad]).details.ids).toEqual([]);
    await park(inbox);
    expect(h.row(inbox, bad.id)).toMatchObject({ status: "parked", reason: expect.stringContaining("Singleton") });
    expect((await inbox.next(empty)).events.map(event => event.id)).toContain(good.id);
    expect(inbox.readDeadLetter(bad.id)).toEqual(bad);
  });

  it("never counts renderable busy-delayed work or valid aggregate action overflow, even after hours/many reoffers", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const events: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) events.push(await h.publish({ kind: "handoff", from: { ...peer, name: "n".repeat(8_000) } }));
    expect(events.every(event => rootInboxMessage([event]).details.ids.includes(event.id))).toBe(true);
    expect(rootInboxMessage(events).details.ids.length).toBeLessThan(events.length);
    for (let attempt = 0; attempt < 40; attempt++) {
      h.advance(15 * 60_000);
      const batch = await inbox.next(empty, { turnEnd: true });
      expect(batch.events.map(event => event.id)).toEqual(events.map(event => event.id));
    }
    expect(h.mesh.listAll(`${inbox.key}/dead-letter/`)).toEqual([]);
    const reloaded = h.box(h.store());
    expect((await reloaded.next(empty)).events.map(event => event.id)).toEqual(events.map(event => event.id));
    expect(reloaded.acked(events[0]!.id, events[0]!.sequence)).toBe(false);
  });

  it("preserves several full-size-ish original payloads outside the cursor value budget, across reload", async () => {
    const h = setup(16 * 1024), inbox = h.box(); inbox.start();
    const bad: MeshEvent[] = [];
    for (let index = 0; index < 4; index++) bad.push(await h.publish({ text: 12 as unknown as string,
      data: { index, bytes: "\\\"\n💾".repeat(1200) } }));
    expect(bad.every(event => Buffer.byteLength(JSON.stringify(event)) > 10 * 1024)).toBe(true);
    await park(inbox); await inbox.close();
    const reloaded = h.box(h.store()); await reloaded.next(empty);
    for (const event of bad) {
      expect(reloaded.readDeadLetter(event.id)).toEqual(event);
      expect(reloaded.acked(event.id, event.sequence)).toBe(false);
      expect(h.row(reloaded, event.id)?.status).toBe("parked");
    }
    const cursor = h.mesh.get(inbox.key, { fresh: true })!.value;
    expect(Buffer.byteLength(JSON.stringify(cursor))).toBeLessThan(16 * 1024 * 0.75);
    const good = await h.publish();
    expect((await reloaded.next(empty)).events.map(event => event.id)).toContain(good.id);
  });

  it.each(["payload", "metadata", "cursor", "barrier"])("cannot lose a parked event when its %s save fails", async stage => {
    const h = setup(), inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: 1 as unknown as string });
    for (let count = 1; count < ROOT_INBOX_MAX_FAILED_OFFERS; count++) await inbox.next(empty);
    const put = h.mesh.put.bind(h.mesh), sync = h.mesh.sync.bind(h.mesh);
    let fail = true;
    const putSpy = vi.spyOn(h.mesh, "put").mockImplementation(input => {
      const matches = stage === "payload" ? input.key.includes("/dead-letter-bytes/")
        : stage === "metadata" ? input.key.includes("/dead-letter/") : stage === "cursor" ? input.key === inbox.key : false;
      if (fail && matches) { fail = false; return Promise.reject(new Error("injected DLQ write failure")); }
      return put(input);
    });
    const syncSpy = vi.spyOn(h.mesh, "sync").mockImplementation(async budget => {
      if (fail && stage === "barrier") { fail = false; throw new Error("injected DLQ write failure"); }
      return sync(budget);
    });
    await expect(inbox.next(empty)).rejects.toThrow("injected DLQ write failure");
    putSpy.mockRestore(); syncSpy.mockRestore();
    const durable = h.saved(inbox);
    if (durable.after >= bad.sequence) expect(h.row(inbox, bad.id)?.original).toBeDefined();
    const reloaded = h.box(h.store());
    await reloaded.next(empty); await reloaded.close();
    expect(h.row(reloaded, bad.id)?.status).toBe("parked");
    expect(reloaded.readDeadLetter(bad.id)).toEqual(bad);
    expect(reloaded.acked(bad.id, bad.sequence)).toBe(false);
    expect(h.delivered(bad)).toBe(false);
  });

  it("preserves receipt-only ack calls while rejecting all-filtered parked ids", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: 1 as unknown as string }); await park(inbox);
    await inbox.consume([], ["receipt-only-native-proof"]);
    expect(h.saved(inbox).delivered).toContain("receipt-only-native-proof");
    await inbox.consume([bad.id], eventReceipts(bad));
    expect(h.saved(inbox).delivered).not.toContain(eventReceipt(bad.id));
    expect(inbox.acked(bad.id, bad.sequence)).toBe(false);
  });

  it("does not block cursor progress when owner-report publication fails; retries via existing reconcile", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: 1 as unknown as string }), good = await h.publish();
    const publish = h.mesh.publish.bind(h.mesh);
    const refusal = vi.spyOn(h.mesh, "publish").mockImplementation(input => input.topic === ROOT_INBOX_ALARM_TOPIC
      ? Promise.reject(new Error("owner-report publication unavailable")) : publish(input));
    await park(inbox); await inbox.consume([good.id], eventReceipts(good)); await inbox.close();
    expect(h.saved(inbox).after).toBeGreaterThanOrEqual(good.sequence);
    expect(h.row(inbox, bad.id)?.status).toBe("parked");
    expect(h.alarms()).toEqual([]);
    refusal.mockRestore();
    const reloaded = h.box(h.store()); await reloaded.next(empty); await reloaded.close();
    expect(h.alarms()).toHaveLength(1);
    const reporting = h.box(h.store());
    expect((await reporting.next(empty, { priority: true })).events.map(event => event.id)).toContain(h.alarms()[0]!.id);
  });

  it("reports once after publish-before-marker failure, without blocking later cursor progress", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: 1 as unknown as string }), good = await h.publish();
    const put = h.mesh.put.bind(h.mesh);
    vi.spyOn(h.mesh, "put").mockImplementation(input => {
      if ((input.value as Partial<RootInboxDeadLetter>)?.reported) return Promise.reject(new Error("report marker failed"));
      return put(input);
    });
    await park(inbox); await inbox.consume([good.id], eventReceipts(good)); await inbox.close();
    vi.restoreAllMocks();
    expect(h.alarms()).toHaveLength(1);
    const reloaded = h.box(h.store()); await reloaded.next(empty); await reloaded.close();
    expect(h.alarms()).toHaveLength(1);
    expect(h.row(reloaded, bad.id)?.reported).toBe(true);
    expect(h.saved(reloaded).after).toBeGreaterThanOrEqual(good.sequence);
  });

  it("replays original identities past high-water, retains original bytes, and records delivered only on actual ack", async () => {
    const h = setup(), inbox = h.box(); inbox.start();
    const bad = await h.publish({ text: 1 as unknown as string, data: { deliveryId: "replay-me" } }), good = await h.publish();
    await park(inbox); await inbox.consume([good.id], eventReceipts(good)); await inbox.close();
    const reloaded = h.box(h.store());
    const repaired = await reloaded.replayDeadLetter(bad.id, { text: "repaired renderable event" });
    expect(repaired).toEqual({ ...bad, text: "repaired renderable event" });
    expect(reloaded.readDeadLetter(bad.id)).toEqual(bad);
    expect(reloaded.acked(bad.id, bad.sequence)).toBe(false);
    expect(h.delivered(bad)).toBe(false);
    await reloaded.close();
    const replaying = h.box(h.store());
    const batch = await replaying.wake(empty, () => true);
    expect(batch).toBeDefined();
    expect(rootInboxMessage(batch!.events).details.ids).toContain(bad.id);
    expect(replaying.acked(bad.id, bad.sequence)).toBe(false);
    expect(h.delivered(bad)).toBe(false);
    await replaying.consume([bad.id], eventReceipts(repaired));
    expect(replaying.acked(bad.id, bad.sequence)).toBe(true);
    expect(h.delivered(bad)).toBe(true);
    await replaying.close();
    const consumed = h.box(h.store());
    expect((await consumed.next(empty)).events.map(event => event.id)).not.toContain(bad.id);
    expect(consumed.acked(bad.id, bad.sequence)).toBe(true);
    expect(consumed.readDeadLetter(bad.id)).toEqual(bad);
    await expect(consumed.replayDeadLetter(bad.id)).rejects.toThrow("not replayable");
  });

  it("routes an adopted predecessor replay/ack to its original ledger, not the successor high-water", async () => {
    const h = setup(), source = h.box(); source.start();
    const bad = await h.publish({ text: 1 as unknown as string }), good = await h.publish();
    await park(source); await source.consume([good.id], eventReceipts(good));
    const transfer: NativeRecoveryTransfer = {
      version: 1, operationId: "replay-move", preparedDigest: "b".repeat(64), preparedEntryId: "prepared", phase: "received", receivedEntryId: "received",
      source: { sessionId: "main", sessionFile: path.join(h.root, "main.jsonl") },
      target: { sessionId: "target", sessionFile: path.join(h.root, "target.jsonl") }, occurrences: [],
    };
    const handoff = await source.parkForNativeRecovery(transfer);
    const target = h.box(h.mesh, { id: "session:target", name: "Target", kind: "main" }); target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [transfer] }));
    const repaired = await target.replayDeadLetter(bad.id, { text: "restored predecessor event" });
    const batch = await target.next(empty, { priority: true });
    expect(batch.events.map(event => event.id)).toContain(bad.id);
    expect(target.acked(bad.id, bad.sequence)).toBe(false);
    await target.consume([bad.id], eventReceipts(repaired));
    expect(h.row(source, bad.id)?.status).toBe("consumed");
    expect(h.saved(source).delivered).toContain(eventReceipt(bad.id));
    expect(h.saved(target).delivered).not.toContain(eventReceipt(bad.id));
    expect(target.acked(bad.id, bad.sequence)).toBe(true);
    expect(target.readDeadLetter(bad.id)).toEqual(bad);
  });

  it("never poisons a valid native-reserved/received carrier through many delayed reoffers", async () => {
    const h = setup(), source = h.box(); source.start();
    const event = await h.publish({ kind: "handoff" }); await source.next(empty);
    const transfer: NativeRecoveryTransfer = {
      version: 1, operationId: "native-move", preparedDigest: "a".repeat(64), preparedEntryId: "prepared", phase: "received", receivedEntryId: "received",
      source: { sessionId: "main", sessionFile: path.join(h.root, "main.jsonl") },
      target: { sessionId: "target", sessionFile: path.join(h.root, "target.jsonl") },
      occurrences: [{ ordinal: 0, state: "pending", customMessage: { customType: "pi-fabric-inbox", details: { ids: [event.id] } } }],
    };
    const handoff = await source.parkForNativeRecovery(transfer);
    const target = h.box(h.mesh, { id: "session:target", name: "Target", kind: "main" }); target.start();
    await target.adoptNativeRecoveryInbox(handoff, () => ({ status: "validated", transfers: [transfer] }));
    for (let attempt = 0; attempt < 30; attempt++) {
      h.advance(60 * 60_000);
      expect((await target.next(empty)).events).toEqual([]);
      expect(target.acked(event.id, event.sequence)).toBe(false);
    }
    expect(h.saved(source).after).toBe(handoff.after);
    expect(h.row(source, event.id)).toBeUndefined();
    transfer.occurrences[0]!.state = "appended";
    expect((await target.next(empty)).events).toEqual([]);
    expect(target.acked(event.id, event.sequence)).toBe(true);
    expect(h.row(source, event.id)).toBeUndefined();
  });
});
