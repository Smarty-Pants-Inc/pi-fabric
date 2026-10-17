import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, rootInboxMessage, rootInboxSession, type RootInboxSession } from "../src/topology/root-inbox.js";

// Keep reason: #14468's real idle-Main fanout and receipt durability/data-safety boundary.
// Count only inbox puts, observing the real store, and check its persisted cursor as well.
const me: MeshIdentity = { id: "session:checkpoint-main", name: "checkpoint-main", kind: "main" };
const peer: MeshIdentity = { id: "session:checkpoint-peer", name: "checkpoint-peer", kind: "main" };
const empty: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
const INTERVAL = 10 * 60_000;
const cleanups: Array<() => void | Promise<void>> = [];
const setup = (options: { horizonMs?: number } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-checkpoint-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  let now = Date.now();
  const startedAt = now;
  vi.spyOn(Date, "now").mockImplementation(() => now);
  cleanups.push(() => { mesh.closeState(); fs.rmSync(root, { recursive: true, force: true }); });
  const box = () => {
    const inbox = new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs: 0, ...options });
    cleanups.push(() => inbox.close());
    return inbox;
  };
  const put = vi.spyOn(mesh, "put"); // Call-through, not a fake state backend.
  const saved = (inbox: RootInbox) => mesh.get(inbox.key, { fresh: true })?.value as {
    after: number; pending?: { through: number; ids: string[] }; delivered: string[]; deliveredAt: number[];
    fallbacks: string[]; fallbackAt: number[]; fallbackThrough?: number;
  };
  const inboxPuts = (inbox: RootInbox) => put.mock.calls.filter(([input]) => input.key === inbox.key);
  const publish = (to = "session:unrelated", data?: unknown, kind = "progress") =>
    mesh.publish({ topic: "fleet.work.checkpoint", kind, from: peer, to, text: "checkpoint work", data });
  return { mesh, box, put, saved, inboxPuts, publish, now: () => now,
    advance: (ms: number) => { now += ms; }, at: (elapsed: number) => { now = startedAt + elapsed; } };
};
afterEach(async () => {
  for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  vi.restoreAllMocks();
});

describe("root inbox receipt saves versus cursor checkpoints (#14468)", () => {
  it("makes zero inbox puts for 12 unrelated appends on a warmed idle Main before ten minutes", async () => {
    const h = setup(); const inbox = h.box();
    await inbox.next(empty); // Initial durable checkpoint/receipt-shape migration is outside the budget.
    const initial = h.saved(inbox);
    h.put.mockClear();
    let through = initial.after;
    for (let n = 0; n < 12; n++) {
      through = (await h.publish()).sequence;
      h.advance(1);
      expect(await inbox.wake(empty, () => true)).toBeUndefined();
    }
    // Include the next idle tick: a deferred cursor must not masquerade as dirty receipts.
    expect(await inbox.wake(empty, () => true)).toBeUndefined();
    expect(h.inboxPuts(inbox)).toHaveLength(0);
    expect(h.saved(inbox)).toEqual(initial);
    h.at(INTERVAL - 1);
    await inbox.next(empty);
    expect(h.inboxPuts(inbox)).toHaveLength(0);
    h.at(INTERVAL);
    expect(await inbox.next(empty)).toMatchObject({ events: [], through });
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    expect(h.saved(inbox)).toMatchObject({ after: through });
    await inbox.next(empty);
    expect(h.inboxPuts(inbox)).toHaveLength(1);
  });

  it("saves addressed admission immediately despite a deferred unrelated cursor, and reloads pending work", async () => {
    const h = setup(); const inbox = h.box();
    await inbox.next(empty);
    const before = h.saved(inbox);
    h.put.mockClear();
    await h.publish();
    await inbox.next(empty);
    const event = await h.publish(me.id);
    expect((await inbox.next(empty)).events.map(row => row.id)).toEqual([event.id]);
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    expect(h.saved(inbox)).toMatchObject({ after: event.sequence - 1,
      pending: { through: event.sequence, ids: [event.id] } });
    expect(h.saved(inbox).after).toBeGreaterThan(before.after);
    expect((await h.box().next(empty)).events.map(row => row.id)).toEqual([event.id]);
  });

  it.each(["consume", "ack", "held batch"] as const)("durably saves %s receipts inside the checkpoint interval", async mode => {
    const h = setup(); const inbox = h.box();
    await inbox.next(empty);
    const event = await h.publish(me.id, { deliveryId: `durable-${mode}` });
    const batch = await inbox.next(empty);
    const receipts = rootInboxMessage(batch.events).details.receipts;
    h.put.mockClear();
    if (mode === "consume") await inbox.consume([event.id], receipts);
    else if (mode === "ack") {
      inbox.ack([event.id], receipts);
      await inbox.next(empty, { commitOnly: true });
    } else await inbox.next({ ...empty, holdsBatch: () => true });
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    expect(h.saved(inbox).after).toBe(event.sequence);
    expect(h.saved(inbox).pending).toBeUndefined();
    expect(h.saved(inbox).delivered).toEqual(expect.arrayContaining(receipts));
    expect((await h.box().next(empty)).events).toEqual([]);
    await h.publish(me.id, { deliveryId: `durable-${mode}` });
    expect((await h.box().next(empty)).events).toEqual([]);
  });

  const native = (at: number, data: object) => rootInboxSession([{ type: "custom_message",
    customType: "pi-fabric-agent-message", timestamp: new Date(at).toISOString(),
    details: { from: peer, data: { topic: "fleet.work.checkpoint", ...data } } }]);

  it("saves new canonical/native receipts and newer receipt times immediately, but not unchanged history", async () => {
    const h = setup(); const inbox = h.box();
    await inbox.next(empty);
    h.put.mockClear();
    const session = native(h.now(), { deliveryId: "native-identity" });
    expect(session.delivered!.size).toBeGreaterThan(0);
    await inbox.next(session);
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    expect(h.saved(inbox).delivered).toEqual(expect.arrayContaining([...session.delivered!]));
    await inbox.next(session);
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    h.advance(1);
    await inbox.next(native(h.now(), { deliveryId: "native-identity" }));
    expect(h.inboxPuts(inbox)).toHaveLength(2);
    expect(h.saved(inbox).deliveredAt.every(at => at === h.now())).toBe(true);
    await h.publish(me.id, { deliveryId: "native-identity" });
    expect((await h.box().next(empty)).events).toEqual([]);
  });

  it("retries a failed receipt save immediately even after canonical history disappears", async () => {
    const h = setup(); const inbox = h.box();
    await inbox.next(empty);
    const before = h.saved(inbox);
    const session = native(h.now(), { deliveryId: "retry-native" });
    h.put.mockClear();
    h.put.mockRejectedValueOnce(new Error("injected receipt put failure"));
    await expect(inbox.next(session)).rejects.toThrow("injected receipt put failure");
    expect(h.saved(inbox)).toEqual(before);
    // remember() now has nothing new and trim() removes nothing: unsaved receipts are still dirty.
    await inbox.next(empty, { commitOnly: true });
    expect(h.inboxPuts(inbox)).toHaveLength(2);
    expect(h.saved(inbox).delivered).toEqual(expect.arrayContaining([...session.delivered!]));
    await h.publish(me.id, { deliveryId: "retry-native" });
    expect((await h.box().next(empty)).events).toEqual([]);
  });

  it("retries a failed native-shadow receipt/fallback save without re-scanning the deferred cursor", async () => {
    const h = setup(); const inbox = h.box();
    await inbox.next(empty);
    await inbox.next(native(h.now(), { key: "one-use-claim" }));
    const before = h.saved(inbox);
    const first = await h.publish(me.id, { key: "one-use-claim" });
    h.put.mockClear();
    h.put.mockRejectedValueOnce(new Error("injected fallback put failure"));
    await expect(inbox.next(empty)).rejects.toThrow("injected fallback put failure");
    expect(h.saved(inbox)).toEqual(before);
    await inbox.next(empty);
    expect(h.inboxPuts(inbox)).toHaveLength(2);
    expect(h.saved(inbox).after).toBe(first.sequence);
    expect(h.saved(inbox).fallbacks).toHaveLength(1);
    expect(h.saved(inbox).fallbackAt).toEqual([h.now()]);
    const distinct = await h.publish(me.id, { key: "one-use-claim" });
    expect((await h.box().next(empty)).events.map(row => row.id)).toEqual([distinct.id]);
  });

  it("saves receipt expiry and its fallback retirement watermark immediately, including a failed-save retry", async () => {
    const h = setup({ horizonMs: 1_000 }); const inbox = h.box();
    await inbox.next(empty);
    await inbox.next(native(h.now(), { key: "expiring-claim" }));
    await h.publish(me.id, { key: "expiring-claim" });
    await inbox.next(empty);
    const before = h.saved(inbox);
    expect(before.fallbacks).toHaveLength(1);
    h.advance(1_001); // Well inside the cursor checkpoint interval.
    h.put.mockClear();
    h.put.mockRejectedValueOnce(new Error("injected expiry put failure"));
    await expect(inbox.next(empty)).rejects.toThrow("injected expiry put failure");
    expect(h.saved(inbox)).toEqual(before);
    await inbox.next(empty);
    expect(h.inboxPuts(inbox)).toHaveLength(2);
    expect(h.saved(inbox)).toMatchObject({ delivered: [], deliveredAt: [], fallbacks: [], fallbackAt: [],
      fallbackThrough: before.fallbackAt[0] });
    expect((await h.box().next(empty)).events).toEqual([]);
    const fresh = await h.publish(me.id, { key: "expiring-claim" });
    expect((await h.box().next(empty)).events.map(row => row.id)).toEqual([fresh.id]);
  });

  it("migrates a legacy string-only receipt ledger immediately on an otherwise empty reload", async () => {
    const h = setup(); const inbox = h.box();
    const session = native(h.now(), { key: "undated-legacy" });
    await h.mesh.put({ key: inbox.key, identity: me, value: { after: h.mesh.latestSequence(),
      delivered: [...session.delivered!] } });
    h.put.mockClear();
    await inbox.next(empty);
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    expect(h.saved(inbox)).toMatchObject({ delivered: [], deliveredAt: [], fallbacks: [], fallbackAt: [] });
    const distinct = await h.publish(me.id, { key: "undated-legacy" });
    expect((await h.box().next(empty)).events.map(row => row.id)).toEqual([distinct.id]);
  });

  it("persists stale addressed-event expiry inside the interval without waking Main or repeating on reload", async () => {
    const h = setup({ horizonMs: 1_000 }); const inbox = h.box();
    await inbox.next(empty);
    const stale = await h.publish(me.id);
    h.advance(1_001);
    h.put.mockClear();
    expect(await inbox.wake(empty, () => true)).toMatchObject({ events: [], skippedStale: 1 });
    expect(h.inboxPuts(inbox)).toHaveLength(1);
    expect(h.saved(inbox).after).toBe(stale.sequence);
    expect(await h.box().next(empty)).not.toHaveProperty("skippedStale");
    expect(h.mesh.read({ after: 0 }).filter(event => event.topic === "fabric.inbox.wake")).toEqual([]);
  });
});
