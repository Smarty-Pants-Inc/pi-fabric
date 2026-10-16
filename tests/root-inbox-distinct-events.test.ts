import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
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

  it.each(["commandId", "deliveryId", "messageId"] as const)("fails open for a fresh %s carrier's ambiguous key-only shadow, but suppresses exact identity and envelope replays", async (identity) => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "native identity-bearing work", delivery: "steer",
      data: { topic: TOPIC, key: KEY, ref: KEY, [identity]: "native-identity" } });
    await h.publish("exact native identity shadow", { key: KEY, [identity]: "native-identity" });
    const ambiguous = await h.publish("ambiguous key-only shadow");
    const distinct = await h.publish("different explicit identity", { key: KEY, [identity]: "distinct-identity" });
    h.advance(1);
    const session = rootInboxSession(h.entries.map(projectRootInboxReceipt));
    // Option (a): key-only fixtures and genuinely distinct key-only work are indistinguishable.
    // Fail open for BOTH; real shadows retain identity, and explicit carriers get no key/ref receipt.
    for (const kind of ["key", "ref"] as const) {
      const fallback = createHash("sha256").update(JSON.stringify([watcher.id, `native-topic-${kind}`, JSON.stringify([TOPIC, KEY])])).digest("hex");
      expect(session.delivered?.has(fallback)).toBe(false);
      expect(session.deliveredAt?.has(fallback)).toBe(false);
    }
    const woke = await inbox.wake(session, () => true);
    expect(ids(woke?.events)).toEqual([ambiguous.id, distinct.id]);
    // Why true: presence includes explicit carriers; an ambiguous key-only fixture is not a real identity-preserving shadow.
    expect(session.holdsSteer(watcher.id, KEY, TOPIC)).toBe(true);
    await h.take(inbox, woke!.events);
    await inbox.close();

    // Exact identity evidence and consumed envelope receipts survive losing session history.
    h.entries.length = 0;
    h.replay(ambiguous);
    await h.publish("exact native identity shadow after reload", { key: KEY, [identity]: "native-identity" });
    await h.publish("exact consumed identity shadow after reload", { key: KEY, [identity]: "distinct-identity" });
    h.advance(1);
    const recovered = h.box();
    expect((await recovered.next(h.session())).events).toEqual([]);
    expect(await recovered.wake(h.session(), () => true)).toBeUndefined();
    await recovered.close();
  });

  it.each(["commandId", "deliveryId", "messageId"] as const)("does not resurrect a %s carrier's topic/key claim after the horizon and cursor reload", async (identity) => {
    const h = setup(0, { horizonMs: 100 });
    const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "native with an explicit identity", delivery: "steer",
      data: { topic: TOPIC, key: KEY, [identity]: "old-native-identity" } });
    await inbox.next(h.session(), { commitOnly: true });
    await inbox.close();
    h.advance(101);
    // Keep canonical native history: removing it would hide the holdsSteer resurrection bug.
    const session = rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const recovered = h.box();
    await recovered.next(session, { commitOnly: true });
    const later = await h.publish("later distinct key-only work");
    const woke = await recovered.wake(session, () => true);
    expect(ids(woke?.events)).toEqual([later.id]);
    // Why true: canonical carrier presence survives expiry, but cannot suppress new key-only work without a dated native-topic receipt.
    expect(session.holdsSteer(watcher.id, KEY, TOPIC)).toBe(true);
    await h.take(recovered, woke!.events);
    await recovered.close();
    h.replay(later);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("records a top-level native deliveryId without a key fallback and suppresses exact shadows after reload", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    const deliveryId = "native:top-level-delivery";
    const hash = (kind: string, value: string) => createHash("sha256").update(JSON.stringify([watcher.id, kind, value])).digest("hex");
    const exact = hash("delivery", deliveryId);
    const fallback = hash("native-topic-key", JSON.stringify([TOPIC, KEY]));
    h.main.attachFollowUpDrain({ isIdle: () => true, hasPendingMessages: () => false,
      sessionManager: { getEntries: () => h.entries }, signal: { aborted: false },
    } as unknown as ExtensionContext, 0, path.join(path.dirname(h.mesh.root), "followups.json"));
    try {
      h.main.deliverAgent({ from: watcher, message: "native with top-level identity", delivery: "followUp",
        deliveryId, data: { topic: TOPIC, key: KEY } });
      expect(h.entries).toHaveLength(1);
      for (const session of [h.session(), rootInboxSession(h.entries.map(projectRootInboxReceipt))]) {
        expect(session.delivered?.has(exact)).toBe(true);
        expect(session.delivered?.has(fallback)).toBe(false);
        expect(session.deliveredAt?.has(fallback)).toBe(false);
        // Why true: top-level deliveryId excludes fallback authority, not presence; key-only work is not an exact shadow.
        expect(session.holdsSteer(watcher.id, KEY, TOPIC)).toBe(true);
      }
      await inbox.next(rootInboxSession(h.entries.map(projectRootInboxReceipt)), { commitOnly: true });
      const saved = h.mesh.get(inbox.key)!.value as { delivered: string[]; fallbacks?: string[] };
      expect(saved.delivered).toContain(exact);
      expect(saved.delivered).not.toContain(fallback);
      expect(saved.fallbacks ?? []).toEqual([]);
      const ambiguous = await h.publish("key-only work must fail open");
      await h.publish("exact top-level identity shadow", { key: KEY, deliveryId }); h.advance(1);
      const batch = await inbox.next(rootInboxSession(h.entries.map(projectRootInboxReceipt)));
      expect(ids(batch.events)).toEqual([ambiguous.id]);
      await h.take(inbox, batch.events);
      h.entries.length = 0;
      await h.publish("exact shadow after history removal", { key: KEY, deliveryId }); h.advance(1);
      expect((await h.box().next(h.session())).events).toEqual([]);
    } finally {
      h.main.closeFollowUpDrain();
      await inbox.close();
    }
  });

  it.each(["missing", "null", "invalid", "fresh"] as const)("reloads a stored native-topic-key claim with %s deliveredAt without inventing a timestamp", async (timestamp) => {
    const h = setup(0, { horizonMs: 100 });
    const claim = createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, KEY])])).digest("hex");
    const observedAt = Date.now() - 10;
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: { after: h.mesh.latestSequence(), delivered: [claim],
      ...(timestamp === "missing" ? {} : { deliveredAt: [timestamp === "fresh" ? observedAt : timestamp === "null" ? null : "not-a-timestamp"] }),
    } });
    // Persist an empty reconciliation, then reload again before any shadow exists.
    // Legacy/malformed receipt ages must not be upgraded to either load's Date.now().
    const initial = h.box();
    await initial.next(h.session(), { commitOnly: true });
    await initial.close();
    const saved = h.mesh.get(key)!.value as { delivered: string[]; deliveredAt: number[] };
    expect(saved.delivered).toEqual(timestamp === "fresh" ? [claim] : []);
    expect(saved.deliveredAt).toEqual(timestamp === "fresh" ? [observedAt] : []);
    h.advance(1);
    const first = await h.publish("first legacy shadow");
    const second = await h.publish("second legacy shadow"); h.advance(1);
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual(timestamp === "fresh" ? [second.id] : [first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
    h.entries.length = 0;
    const later = await h.publish("later same-key work after another reload"); h.advance(1);
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
  });

  it.each([60_001, 86_400_000, 60_000, -10])("validates persisted native claim age at offset %ims through empty reconciliation and reload", async (offset) => {
    const h = setup();
    const claim = createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, KEY])])).digest("hex");
    const deliveredAt = Date.now() + offset;
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: { after: h.mesh.latestSequence(), delivered: [claim], deliveredAt: [deliveredAt] } });
    expect(h.entries).toEqual([]); // Only the persisted cursor supplies suppression evidence.
    const initial = h.box();
    expect((await initial.next(h.session(), { commitOnly: true })).events).toEqual([]);
    await initial.close();
    const saved = h.mesh.get(key)!.value as { delivered: string[]; deliveredAt: number[] };
    const valid = offset <= 60_000;
    expect(saved.delivered).toEqual(valid ? [claim] : []);
    expect(saved.deliveredAt).toEqual(valid ? [deliveredAt] : []);
    const first = await h.publish("first persisted-claim legacy shadow");
    const second = await h.publish("distinct same-key persisted-claim work");
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual(valid ? [second.id] : [first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
    h.entries.length = 0;
    const later = await h.publish("same-key work after spent-claim reload");
    const reloaded = h.box();
    expect(ids((await reloaded.next(h.session())).events)).toEqual([later.id]);
    await reloaded.close();
  });

  it.each<[number, boolean]>([
    [60_001, false], [86_400_000, false], [60_001, true], [86_400_000, true],
  ])("never revives a spent persisted claim with future fallbackAt +%ims (empty reconciliation: %s)", async (offset, reconcile) => {
    const h = setup();
    const claim = createHash("sha256").update(JSON.stringify([watcher.id, "native-topic-key", JSON.stringify([TOPIC, KEY])])).digest("hex");
    const now = Date.now();
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: { after: h.mesh.latestSequence(), delivered: [claim], deliveredAt: [now - 10],
      fallbacks: [claim], fallbackAt: [now + offset] } });
    expect(h.entries).toEqual([]);
    if (reconcile) {
      const initial = h.box();
      expect((await initial.next(h.session(), { commitOnly: true })).events).toEqual([]);
      await initial.close();
    }
    // A malformed spent-marker age cannot become suppression authority; simply dropping
    // that marker is also unsafe if it revives the matching, otherwise fresh native claim.
    const first = await h.publish("first distinct work after future spent marker");
    const second = await h.publish("second distinct same-key work after future spent marker");
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
    h.entries.length = 0;
    const third = await h.publish("third distinct work after spent-marker reload");
    const fourth = await h.publish("fourth distinct same-key work after spent-marker reload");
    const reloaded = h.box();
    expect(ids((await reloaded.next(h.session())).events)).toEqual([third.id, fourth.id]);
    await reloaded.close();
  });

  it.each(
    (["key", "ref"] as const).flatMap(kind =>
      ([
        ["missing", undefined], ["null", null], ["invalid string", "not-a-timestamp"],
        ["numeric string", "1700000000000"], ["NaN", Number.NaN],
        ["positive infinity", Number.POSITIVE_INFINITY], ["negative infinity", Number.NEGATIVE_INFINITY],
        ["zero", 0], ["negative", -1], ["fractional", 0.5], ["negative fractional", -0.5],
      ] as const).flatMap(([timestamp, at]) =>
        [false, true].map(reconcile => [kind, timestamp, at, reconcile] as const))),
  )("never revives a spent %s claim with %s fallbackAt (value: %s; empty reconciliation: %s)", async (kind, _timestamp, at, reconcile) => {
    const h = setup();
    const claim = createHash("sha256").update(JSON.stringify([watcher.id, `native-topic-${kind}`, JSON.stringify([TOPIC, KEY])])).digest("hex");
    const observedAt = Date.now() - 10;
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: { after: h.mesh.latestSequence(), delivered: [claim], deliveredAt: [observedAt],
      fallbacks: [claim], ...(at === undefined ? {} : { fallbackAt: [at] }) } });
    expect(h.entries).toEqual([]); // A fresh persisted native claim remains; its fallback is already spent.
    if (reconcile) {
      const initial = h.box();
      expect((await initial.next(h.session(), { commitOnly: true })).events).toEqual([]);
      await initial.close();
    }
    const first = await h.publish("first distinct work after malformed spent marker", { [kind]: KEY });
    const second = await h.publish("second distinct same-key work after malformed spent marker", { [kind]: KEY });
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
    h.entries.length = 0;
    h.replay(first);
    h.replay(second);
    const third = await h.publish("third distinct work after spent-marker reload", { [kind]: KEY });
    const fourth = await h.publish("fourth distinct same-key work after spent-marker reload", { [kind]: KEY });
    const reloaded = h.box();
    const later = await reloaded.next(h.session());
    expect(ids(later.events)).toEqual([third.id, fourth.id]);
    await h.take(reloaded, later.events);
    await reloaded.close();
    h.entries.length = 0;
    h.replay(third);
    h.replay(fourth);
    const replayed = h.box();
    expect((await replayed.next(h.session())).events).toEqual([]);
    await replayed.close();
  });

  it.each(["key", "ref"] as const)("retains an undated spent %s marker before canonical history restores its native claim", async (kind) => {
    const h = setup();
    h.main.deliverAgent({ from: watcher, message: "already spent native work", delivery: "steer", data: { [kind]: KEY, topic: TOPIC } });
    const claim = createHash("sha256").update(JSON.stringify([watcher.id, `native-topic-${kind}`, JSON.stringify([TOPIC, KEY])])).digest("hex");
    const key = h.box().key;
    // No persisted claim can supply the retention age. The marker must survive until
    // #remember sees canonical history; retaining it does not prove a delivery occurred.
    await h.mesh.put({ key, identity: me, value: { after: h.mesh.latestSequence(), fallbacks: [claim] } });
    const initial = h.box();
    expect((await initial.next(h.session(), { commitOnly: true })).events).toEqual([]);
    await initial.close();
    h.entries.length = 0;
    const first = await h.publish("first new work after restoring a spent claim", { [kind]: KEY });
    const second = await h.publish("second new work after restoring a spent claim", { [kind]: KEY });
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
  });

  it.each((["key", "ref"] as const).flatMap(kind =>
    [undefined, "not-a-timestamp"].map(at => [kind, at] as const),
  ))("retires a spent %s marker with fallbackAt %s through its newer restored canonical claim", async (kind, at) => {
    const h = setup(0, { horizonMs: 100 });
    const now = Date.now(), observedAt = now + 50;
    h.main.deliverAgent({ from: watcher, message: "already spent near-future native work", delivery: "steer",
      data: { [kind]: KEY, topic: TOPIC } });
    h.entries[0] = { ...(h.entries[0] as object), timestamp: new Date(observedAt).toISOString() };
    const session = () => rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const claim = createHash("sha256").update(JSON.stringify([watcher.id, `native-topic-${kind}`, JSON.stringify([TOPIC, KEY])])).digest("hex");
    const key = h.box().key;
    // The persisted marker has no delivered claim from which #load can recover its age.
    await h.mesh.put({ key, identity: me, value: { after: h.mesh.latestSequence(), fallbacks: [claim],
      ...(at === undefined ? {} : { fallbackAt: [at] }) } });
    const initial = h.box();
    expect((await initial.next(session(), { commitOnly: true })).events).toEqual([]);
    await initial.close();
    type Cursor = { delivered: string[]; deliveredAt: number[]; fallbacks: string[]; fallbackAt: number[]; fallbackThrough?: number };
    const restored = h.mesh.get(key)!.value as Cursor;
    expect(restored.fallbacks).toEqual([claim]);
    expect(restored.fallbackAt).toEqual([now]);
    // Main also retains its exact deliveryId receipt; check the native claim's aligned age.
    expect(restored.delivered).toContain(claim);
    expect(restored.deliveredAt[restored.delivered.indexOf(claim)]).toBe(observedAt);

    h.advance(101); // Marker expired, but the restored native claim is only 51ms old.
    expect(session().deliveredAt?.get(claim)).toBe(observedAt); // Keep canonical history.
    const recovered = h.box();
    expect((await recovered.next(session(), { commitOnly: true })).events).toEqual([]);
    const expired = h.mesh.get(key)!.value as Cursor;
    expect(expired.fallbacks).toEqual([]);
    expect(expired.delivered).toContain(claim);
    expect(expired.fallbackThrough).toBeGreaterThanOrEqual(observedAt);
    const first = await h.publish("first distinct work after marker expiry", { [kind]: KEY });
    const second = await h.publish("second distinct work after marker expiry", { [kind]: KEY });
    const batch = await recovered.next(session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
    h.entries.splice(1); // Retain only the canonical native carrier, not inbox carriers.
    h.replay(first);
    h.replay(second);
    const reloaded = h.box();
    expect((await reloaded.next(session())).events).toEqual([]);
    await reloaded.close();
  });

  it.each((["key", "ref"] as const).flatMap(kind =>
    ([[4096, 42], [128 * 1024, 1100]] as const).map(([maxEventBytes, count]) => [kind, maxEventBytes, count] as const),
  ))("retires a capacity-evicted spent %s marker through its newer claim at %i bytes (%i markers)", async (kind, maxEventBytes, count) => {
    const h = setup(0, { maxEventBytes });
    const now = Date.now(), observedAt = now + 50;
    const hash = (value: string) => createHash("sha256").update(JSON.stringify([watcher.id, `native-topic-${kind}`, JSON.stringify([TOPIC, value])])).digest("hex");
    const claim = hash(KEY);
    h.main.deliverAgent({ from: watcher, message: "already spent native work", delivery: "steer", data: { [kind]: KEY, topic: TOPIC } });
    h.entries[0] = { ...(h.entries[0] as object), timestamp: new Date(observedAt).toISOString() };
    const fallbacks = [claim, ...Array.from({ length: count - 1 }, (_, i) => hash(`newer-marker-${i}`))];
    const seed = { after: h.mesh.latestSequence(), delivered: [claim], deliveredAt: [observedAt], fallbacks,
      fallbackAt: [now, ...fallbacks.slice(1).map(() => now + 1)] };
    const key = h.box().key;
    await h.mesh.put({ key, identity: me, value: seed });
    const initial = h.box();
    expect((await initial.next(h.session(), { commitOnly: true })).events).toEqual([]);
    await initial.close();
    const saved = h.mesh.get(key)!.value as typeof seed & { fallbackThrough?: number };
    expect(saved.fallbacks).not.toContain(claim);
    expect(saved.delivered).toContain(claim);
    expect(saved.fallbackThrough).toBeGreaterThanOrEqual(observedAt);
    expect(saved.delivered.length + saved.fallbacks.length).toBeLessThanOrEqual(1024);
    expect(Buffer.byteLength(JSON.stringify(saved), "utf8")).toBeLessThanOrEqual(Math.floor(maxEventBytes * 0.75));
    // New receipts must be newer than the deliberately future-dated capacity seed.
    h.advance(51);
    const first = await h.publish("first distinct work after marker eviction", { [kind]: KEY });
    const second = await h.publish("second distinct work after marker eviction", { [kind]: KEY });
    const recovered = h.box();
    const batch = await recovered.next(h.session());
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(recovered, batch.events);
    await recovered.close();
    h.entries.splice(1);
    h.replay(first);
    h.replay(second);
    const reloaded = h.box();
    expect((await reloaded.next(h.session())).events).toEqual([]);
    await reloaded.close();
  });

  it("fails open for direct holdsSteer evidence without a deliveredAt timestamp", async () => {
    const h = setup(); const inbox = h.box(); inbox.start();
    h.main.deliverAgent({ from: watcher, message: "legacy native work", delivery: "steer", data: { topic: TOPIC, key: KEY } });
    const canonical = h.session();
    // A compatibility caller exposes only the boolean, not dated delivery receipts.
    const undated = { holdsBatch: canonical.holdsBatch, holdsSteer: canonical.holdsSteer };
    expect(undated.holdsSteer(watcher.id, KEY, TOPIC)).toBe(true);
    const first = await h.publish("undated claim cannot suppress this event");
    const second = await h.publish("nor this later same-key event"); h.advance(1);
    const batch = await inbox.next(undated);
    expect(ids(batch.events)).toEqual([first.id, second.id]);
    await h.take(inbox, batch.events);
    await inbox.close();
    h.entries.length = 0;
    h.replay(first);
    const later = await h.publish("distinct work after undated caller reload"); h.advance(1);
    const recovered = h.box();
    expect(ids((await recovered.wake(h.session(), () => true))?.events)).toEqual([later.id]);
    await recovered.close();
  });

  it.each([undefined, "not-a-timestamp"])("fails open for canonical key-only native history with timestamp %s instead of renewing it on reload", async (timestamp) => {
    const h = setup(0, { horizonMs: 100 });
    h.main.deliverAgent({ from: watcher, message: "undated canonical native", delivery: "steer", data: { topic: TOPIC, key: KEY } });
    h.entries[0] = { ...(h.entries[0] as object), timestamp };
    h.advance(101);
    const session = rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const inbox = h.box();
    await inbox.next(session, { commitOnly: true });
    await inbox.close();
    h.advance(101);
    // Reading the same malformed historical entry again must not manufacture a fresh age.
    const reloadedSession = rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const first = await h.publish("distinct key-only work after undated history");
    const second = await h.publish("another distinct key-only event"); h.advance(1);
    const recovered = h.box();
    const woke = await recovered.wake(reloadedSession, () => true);
    expect(ids(woke?.events)).toEqual([first.id, second.id]);
    await h.take(recovered, woke!.events);
    await recovered.close();
    h.entries.length = 0;
    h.replay(first);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it.each<[string, (now: number) => unknown]>([
    ["malformed year", () => "9999"],
    ["ISO without milliseconds", now => new Date(now).toISOString().replace(/\.\d{3}Z$/, "Z")],
    ["ISO with offset", now => new Date(now).toISOString().replace("Z", "+00:00")],
    ["ISO without Z", now => new Date(now).toISOString().slice(0, -1)],
    ["numeric string", now => String(now)],
    ["far-future canonical ISO", now => new Date(now + 86_400_000).toISOString()],
    ["far-future epoch milliseconds", now => now + 86_400_000],
    ["ISO beyond clock-skew allowance", now => new Date(now + 60_001).toISOString()],
    ["epoch milliseconds beyond clock-skew allowance", now => now + 60_001],
    ["noninteger epoch milliseconds", now => now + 0.5],
    ["zero", () => 0],
    ["negative", () => -1],
    ["NaN", () => Number.NaN],
    ["positive infinity", () => Number.POSITIVE_INFINITY],
    ["negative infinity", () => Number.NEGATIVE_INFINITY],
  ])("fails open for %s native timestamps through canonical projection and reload", async (_kind, timestamp) => {
    const h = setup();
    h.main.deliverAgent({ from: watcher, message: "untrusted native age", delivery: "steer", data: { topic: TOPIC, key: KEY } });
    h.entries[0] = { ...(h.entries[0] as object), timestamp: timestamp(Date.now()) };
    const session = () => rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const initial = h.box();
    await initial.next(session(), { commitOnly: true });
    await initial.close();
    const first = await h.publish("fresh distinct key-only work");
    const second = await h.publish("another distinct key-only event");
    const recovered = h.box();
    const woke = await recovered.wake(session(), () => true);
    expect(ids(woke?.events)).toEqual([first.id, second.id]);
    await h.take(recovered, woke!.events);
    await recovered.close();
    h.entries.length = 0;
    h.replay(first);
    const later = await h.publish("distinct work after another reload");
    expect(ids((await h.box().next(h.session())).events)).toEqual([later.id]);
  });

  it.each<[string, (now: number) => unknown]>([
    ["fresh canonical ISO", now => new Date(now).toISOString()],
    ["fresh epoch milliseconds", now => now],
    ["canonical ISO at clock-skew boundary", now => new Date(now + 60_000).toISOString()],
    ["epoch milliseconds at clock-skew boundary", now => now + 60_000],
  ])("accepts %s for exactly one legacy shadow across canonical projection and reload", async (_kind, timestamp) => {
    const h = setup();
    h.main.deliverAgent({ from: watcher, message: "dated native work", delivery: "steer", data: { topic: TOPIC, key: KEY } });
    h.entries[0] = { ...(h.entries[0] as object), timestamp: timestamp(Date.now()) };
    const session = () => rootInboxSession(h.entries.map(projectRootInboxReceipt));
    const initial = h.box();
    await initial.next(session(), { commitOnly: true });
    await initial.close();
    await h.publish("first legacy shadow");
    const recovered = h.box();
    expect((await recovered.next(session())).events).toEqual([]);
    await recovered.close();
    const second = await h.publish("second legacy event is distinct work");
    const reloaded = h.box();
    const batch = await reloaded.next(session());
    expect(ids(batch.events)).toEqual([second.id]);
    await h.take(reloaded, batch.events);
    await reloaded.close();
    h.entries.length = 0;
    h.replay(second);
    expect((await h.box().next(h.session())).events).toEqual([]);
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
