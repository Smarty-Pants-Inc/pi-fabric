import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { RootInbox, projectRootInboxReceipt, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

// smarty-dev#12140: a work key (data.key) is not unique per notification. Two distinct durable
// events with the same sender, topic and key (a watcher's rc=1 then rc=124) must both deliver.
const roots: string[] = [];
const me: MeshIdentity = { id: "session:incident-probe", name: "incident-probe", kind: "main" };
const watcher: MeshIdentity = { id: "fleet-tool:lane-watcher", name: "lane-watcher", kind: "main" };
const TOPIC = "fleet.work.tasks";
const KEY = "watcher-lucky-6289-m4";

const setup = (steerGraceMs = 0, options: { maxEventBytes?: number; horizonMs?: number } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-distinct-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), options.maxEventBytes ?? 64 * 1024, 500);
  const entries: unknown[] = [];
  let now = Date.now();
  vi.spyOn(Date, "now").mockImplementation(() => now);
  const box = () => new RootInbox(mesh, me, () => [me.id], { now: () => now, steerGraceMs, wakeCooldownMs: 0, ...(options.horizonMs === undefined ? {} : { horizonMs: options.horizonMs }) });
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

  it.each(["key", "ref"] as const)("spends one %s-only fallback across peek/wake/scan, then survives reload", async (kind) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const claim = { [kind]: KEY };
    h.main.deliverAgent({ from: watcher, message: "rc=1", delivery: "steer", data: { ...claim, topic: TOPIC } });
    const first = await h.publish("rc=1 legacy", claim);
    const second = await h.publish("rc=124 legacy", claim); h.advance(1);
    // wake peeks first, but its reservation is observational; the real scan consumes it once.
    const woke = await inbox.wake(h.session(), () => true);
    expect(ids(woke?.events)).toEqual([second.id]);
    await h.take(inbox, woke!.events);
    h.entries.length = 0;
    const third = await h.publish("rc=0 legacy", claim); h.advance(1);
    expect(ids((await h.box().next(h.session())).events)).toEqual([third.id]);
    expect(first.id).not.toBe(third.id);
  });

  it("wakes immediately for a later same-key event past a one-use shadow inside steer grace", async () => {
    const h = setup(60_000); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "native", delivery: "steer", data: { key: KEY, topic: TOPIC } });
    await h.publish("legacy shadow");
    const distinct = await h.publish("new legacy work"); h.advance(1);
    const woke = await inbox.wake(h.session(), () => true);
    expect(ids(woke?.events)).toEqual([distinct.id]);
    await h.take(inbox, woke!.events);
    await inbox.close();
  });

  it("does not spend a legacy fallback on an exact-id replay", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const original = await h.publish("previous work"); h.advance(1);
    await h.take(inbox, (await inbox.next(h.session())).events);
    await inbox.next(h.session());
    h.main.deliverAgent({ from: watcher, message: "new native work", delivery: "steer", data: { key: KEY, topic: TOPIC } });
    h.replay(original);
    await h.publish("first unmatched legacy shadow");
    const distinct = await h.publish("later distinct legacy work"); h.advance(1);
    expect(ids((await inbox.next(h.session())).events)).toEqual([distinct.id]);
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

  it.each(["messageId", "deliveryId", "commandId"] as const)("delivers a distinct addressed event with a different %s after a key-only native carrier", async (identity) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "rc=1", delivery: "steer", data: { key: KEY, topic: TOPIC } });
    const distinct = await h.publish("rc=124 distinct", { key: KEY, [identity]: `watcher-local-${identity}` });
    // Explicit work comes first: it must not spend the carrier's fallback on its way past.
    await h.publish("rc=1 shadow"); h.advance(1);
    // The legacy key-only shadow is still suppressed, but its distinct explicit identity is not.
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([distinct.id]);
    await h.take(inbox, batch.events);
    // The native claim persists in the recipient cursor once the session history is gone.
    h.entries.length = 0;
    const laterLegacy = await h.publish("rc=1 shadow again"); h.advance(1);
    // A key-only carrier is one-use: its first legacy shadow was suppressed, so this later
    // identity-less event remains deliverable even after the cursor reloads.
    const later = await h.box().next(h.session());
    expect(ids(later.events)).toEqual([laterLegacy.id]);
    await h.take(inbox, later.events);
    // Another topic or sender is not the claimed work.
    const otherTopic = await h.mesh.publish({ topic: "fleet.work.other", kind: "answer", from: watcher, to: me.id, text: "other", data: { key: KEY } });
    h.advance(1);
    expect(ids((await h.box().next(h.session())).events)).toEqual([otherTopic.id]);
  });

  it.each([[4096, 42], [128 * 1024, 1100]])("bounds fallback and delivered receipts together at %i bytes", async (maxEventBytes, count) => {
    const h = setup(0, { maxEventBytes });
    const hash = (key: string) => createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, key])])).digest("hex");
    const now = Date.now();
    const fallbacks = Array.from({ length: count }, (_, i) => hash(`capacity-${i}`));
    const delivered = [hash("newest-a"), hash("newest-b")];
    const seed = { after: h.mesh.latestSequence(), delivered, deliveredAt: [now, now], fallbacks,
      fallbackAt: fallbacks.map((_, i) => now - count + i) };
    const bytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value), "utf8");
    expect(bytes(seed)).toBeLessThan(maxEventBytes);
    if (maxEventBytes === 4096) expect(bytes(seed)).toBeGreaterThan(maxEventBytes * 0.75);
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: seed });
    expect((await h.box().next(h.session(), { commitOnly: true })).events).toEqual([]);
    const saved = h.mesh.get(key)!.value as typeof seed;
    expect(bytes(saved)).toBeLessThanOrEqual(Math.floor(maxEventBytes * 0.75));
    expect(saved.delivered.length + saved.fallbacks.length).toBeLessThanOrEqual(1024);
    expect(saved.delivered).toEqual(delivered);
    expect(saved.fallbacks.length).toBeGreaterThan(0);
    expect(saved.fallbacks.length).toBeLessThan(fallbacks.length);
    expect(saved.fallbacks).toEqual(fallbacks.slice(-saved.fallbacks.length));
    expect(saved.fallbackAt).toEqual(seed.fallbackAt.slice(-saved.fallbacks.length));
    await h.box().next(h.session(), { commitOnly: true });
    expect(h.mesh.get(key)!.value).toEqual(saved); // No resurrection from the stored cursor on save.
  });

  it.each([false, true])("fails open after trimming a spent fallback (old session carrier: %s)", async (keepCarrier) => {
    const h = setup(0, { maxEventBytes: 4096 });
    const hash = (key: string) => createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, key])])).digest("hex");
    const now = Date.now();
    const trimmed = hash("trimmed"), retained = hash("retained");
    const fillers = Array.from({ length: 38 }, (_, i) => hash(`filler-${i}`));
    const seed = { after: h.mesh.latestSequence(), delivered: [trimmed, retained], deliveredAt: [now - 1000, now - 1],
      fallbacks: [trimmed, ...fillers, retained], fallbackAt: [now - 1000, ...fillers.map((_, i) => now - 100 + i), now - 1] };
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: seed });
    await h.box().next(h.session(), { commitOnly: true });
    const saved = h.mesh.get(key)!.value as typeof seed;
    expect(saved.fallbacks).not.toContain(trimmed);
    expect(saved.fallbacks).toContain(retained);
    expect((saved as typeof seed & { fallbackThrough?: number }).fallbackThrough).toBeGreaterThanOrEqual(now - 1000);
    // Canonical native history remains, but predates the eviction watermark. Reloading must
    // not turn that already-spent carrier into a new claim when its marker is gone.
    if (keepCarrier) {
      h.advance(-1000);
      for (const workKey of ["trimmed", "retained"]) {
        h.main.deliverAgent({ from: watcher, message: "old native", delivery: "steer", data: { key: workKey, topic: TOPIC } });
      }
      h.advance(1000);
    }
    const failOpen = await h.publish("new identity-less work", { key: "trimmed" });
    const retainedFirst = await h.publish("already spent, later work", { key: "retained" });
    const retainedSecond = await h.publish("already spent, still later work", { key: "retained" });
    h.main.deliverAgent({ from: watcher, message: "fresh native", delivery: "steer", data: { key: "fresh", topic: TOPIC } });
    await h.publish("fresh native shadow", { key: "fresh" });
    const freshSecond = await h.publish("fresh claim is one-use", { key: "fresh" });
    h.advance(1);
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual([failOpen.id, retainedFirst.id, retainedSecond.id, freshSecond.id]);
    await h.take(recovered, batch.events);
    h.entries.length = 0;
    const later = await h.publish("trimmed claim stays fail-open after reload", { key: "trimmed" });
    h.advance(1);
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
  });

  it.each(["key", "ref"] as const)("fails open after an UNUSED %s-only native claim expires without a shadow", async (kind) => {
    const h = setup(0, { horizonMs: 100 });
    const inbox = h.box(); inbox.start();
    const claim = { [kind]: KEY };
    const hash = createHash("sha256").update(JSON.stringify([watcher.id, `native-topic-${kind}`, JSON.stringify([TOPIC, KEY])])).digest("hex");
    h.main.deliverAgent({ from: watcher, message: "unused native", delivery: "steer", data: { ...claim, topic: TOPIC } });
    // Persist the native receipt without ever publishing its legacy shadow.
    await inbox.next(h.session(), { commitOnly: true });
    const initial = h.mesh.get(inbox.key)!.value as { delivered?: string[]; fallbacks?: string[] };
    expect(initial.delivered).toContain(hash);
    expect(initial.fallbacks ?? []).toEqual([]);
    h.advance(101);
    await h.box().next(h.session(), { commitOnly: true });
    const expired = h.mesh.get(inbox.key)!.value as typeof initial;
    expect(expired.delivered ?? []).not.toContain(hash);
    expect(expired.fallbacks ?? []).toEqual([]);
    expect(h.session().delivered?.has(hash)).toBe(true); // Canonical old carrier remains.
    const later = await h.publish("new identity-less work after unused claim expiry", claim);
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
  });

  it.each(["key", "ref"] as const)("keeps an UNUSED %s-only native claim fresh at the exact 100ms boundary", async (kind) => {
    const h = setup(0, { horizonMs: 100 });
    const inbox = h.box(); inbox.start();
    const claim = { [kind]: KEY };
    h.main.deliverAgent({ from: watcher, message: "fresh native", delivery: "steer", data: { ...claim, topic: TOPIC } });
    await inbox.next(h.session(), { commitOnly: true });
    h.advance(100);
    await h.box().next(h.session(), { commitOnly: true });
    await h.publish("first legacy shadow at exact boundary", claim);
    const later = await h.publish("subsequent legacy work at exact boundary", claim);
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
  });

  it.each([[4096, 42], [128 * 1024, 1100]])("fails open after capacity trimming an UNUSED native receipt at %i bytes", async (maxEventBytes, count) => {
    const h = setup(0, { maxEventBytes });
    const now = Date.now();
    const hash = (key: string) => createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, key])])).digest("hex");
    h.advance(-1000);
    h.main.deliverAgent({ from: watcher, message: "unused old native", delivery: "steer", data: { key: KEY, topic: TOPIC } });
    h.advance(1000);
    const native = hash(KEY);
    const delivered = [native, ...Array.from({ length: count - 1 }, (_, i) => hash(`unused-capacity-${i}`))];
    const seed = { after: h.mesh.latestSequence(), delivered,
      deliveredAt: [now - 1000, ...delivered.slice(1).map((_, i) => now - 100 + Math.floor(i / count * 99))] };
    const bytes = (value: unknown) => Buffer.byteLength(JSON.stringify(value), "utf8");
    expect(bytes(seed)).toBeLessThan(maxEventBytes);
    if (maxEventBytes === 4096) expect(bytes(seed)).toBeGreaterThan(maxEventBytes * 0.75);
    const key = h.box().key;
    // No fallbacks/spent marker: only an unused native receipt plus delivered fillers.
    await h.mesh.put({ key, identity: me, value: seed });
    await h.box().next(h.session(), { commitOnly: true });
    const saved = h.mesh.get(key)!.value as typeof seed & { fallbacks?: string[]; fallbackThrough?: number };
    expect(saved.delivered).not.toContain(native);
    expect(saved.fallbacks ?? []).toEqual([]);
    expect(saved.delivered.length).toBeLessThan(delivered.length);
    expect(bytes(saved)).toBeLessThanOrEqual(Math.floor(maxEventBytes * 0.75));
    expect(h.session().delivered?.has(native)).toBe(true);
    const later = await h.publish("new identity-less work after unused receipt eviction");
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
    expect(saved.fallbackThrough).toBeGreaterThanOrEqual(now - 1000);
  });

  it("expires fallback claims and native receipts at the same horizon and durably saves expiry", async () => {
    const horizonMs = 100;
    const h = setup(0, { horizonMs });
    const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "native", delivery: "steer", data: { key: KEY, topic: TOPIC } });
    await h.publish("native shadow");
    expect((await inbox.next(h.session())).events).toEqual([]);
    const hash = createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, KEY])])).digest("hex");
    type Cursor = { fallbacks?: string[]; fallbackAt?: number[]; delivered?: string[]; deliveredAt?: number[] };
    const initial = h.mesh.get(inbox.key)!.value as Cursor;
    expect(initial.fallbacks).toContain(hash);
    expect(initial.delivered).toContain(hash);
    h.advance(horizonMs);
    await h.box().next(h.session(), { commitOnly: true });
    const boundary = h.mesh.get(inbox.key)!.value as Cursor;
    expect(boundary.fallbacks).toContain(hash);
    expect(boundary.delivered).toContain(hash);
    h.advance(1);
    // Keep the old native carrier in session history: scanning it must not refresh its age.
    await h.box().next(h.session(), { commitOnly: true });
    const expired = h.mesh.get(inbox.key)!.value as Cursor;
    expect(expired.fallbacks ?? []).toEqual([]);
    expect(expired.fallbackAt ?? []).toEqual([]);
    expect(expired.delivered ?? []).toEqual([]);
    expect(expired.deliveredAt ?? []).toEqual([]);
    await h.box().next(h.session(), { commitOnly: true });
    expect(h.mesh.get(inbox.key)!.value).toEqual(expired);
    const later = await h.publish("new work after native receipt expiry");
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
  });

  it.each([["deliveryId", "messageId"], ["deliveryId", "commandId"], ["messageId", "commandId"]] as const)("prefers %s over %s for delivery and replay after reload", async (primary, secondary) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const first = await h.publish("first", { key: KEY, [primary]: "primary-1", [secondary]: "shared-secondary" });
    const second = await h.publish("second", { key: KEY, [primary]: "primary-2", [secondary]: "shared-secondary" });
    h.advance(1);
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(inbox, batch.events);
    h.entries.length = 0;
    await h.publish("same primary, different secondary", { key: KEY, [primary]: "primary-2", [secondary]: "changed-secondary" });
    h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("matches commandId native receipts after canonical projection and cursor reload", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "native command", delivery: "steer", data: { key: KEY, topic: TOPIC, commandId: "native-command" } });
    await h.publish("command shadow", { key: KEY, commandId: "native-command" });
    const distinct = await h.publish("different command", { key: KEY, commandId: "other-command" });
    h.advance(1);
    const projected = rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const batch = await inbox.next(projected);
    expect(ids(batch.events)).toEqual([distinct.id]);
    await h.take(inbox, batch.events);
    h.entries.length = 0;
    await h.publish("command shadow after reload", { key: KEY, commandId: "native-command" }); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it.each(["deliveryId", "messageId", "commandId"] as const)("keeps explicit %s shadow suppression for distinct envelope ids", async (identity) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const first = await h.publish("rc=1", { key: KEY, [identity]: "delivery-1" });
    await h.publish("rc=1 shadow", { key: KEY, [identity]: "delivery-1" });
    const second = await h.publish("rc=124", { key: KEY, [identity]: "delivery-2" }); h.advance(1);
    const batch = await inbox.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(inbox, batch.events);
    h.entries.length = 0;
    await h.publish("rc=124 shadow", { key: KEY, [identity]: "delivery-2" }); h.advance(1);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });
});
