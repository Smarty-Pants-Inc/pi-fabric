import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it } from "vitest";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { deliverRootInbox } from "../src/topology/root-inbox-delivery.js";
import {
  INBOX_INJECT_MAX_TEXT_BYTES, RootInbox, rootInboxMessage, rootInboxSession, type RootInboxSession,
} from "../src/topology/root-inbox.js";

// smarty-dev#12356: a real incident showed that overflow can hide blocked work; keep the regression
// on the real MeshStore -> RootInbox -> Pi injection -> session receipt/consume path.
const roots: string[] = [];
const inboxes: RootInbox[] = [];
const meshes: MeshStore[] = [];
const me: MeshIdentity = { id: "session:inbox-priority", name: "inbox-priority", kind: "main" };
const peer: MeshIdentity = { id: "session:priority-sender", name: "priority-sender", kind: "main" };
const TOPIC = "fleet.work.priority";
const notHeld: RootInboxSession = { holdsBatch: () => false, holdsSteer: () => false };
type InboxMessage = ReturnType<typeof rootInboxMessage>;

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-inbox-priority-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
  meshes.push(mesh);
  const entries: unknown[] = [];
  const sent: InboxMessage[] = [];
  const pi = {
    hostCapabilities: { turnProvenance: 1 },
    sendMessage: (message: unknown) => sent.push(message as InboxMessage),
  } as unknown as ExtensionAPI;
  const box = () => {
    const inbox = new RootInbox(mesh, me, () => [me.id], { now: Date.now, steerGraceMs: 0, wakeCooldownMs: 0 });
    inboxes.push(inbox);
    return inbox;
  };
  const publish = (kind: string, text: string, data?: unknown) => mesh.publish({
    topic: TOPIC, kind, from: peer, to: me.id, text, ...(data === undefined ? {} : { data }),
  });
  const publishMany = async (inputs: Parameters<MeshStore["publish"]>[0][]) => {
    const events: MeshEvent[] = [];
    // Single publication does not reach publishBatch's product-retention deletion path (#4664).
    for (const input of inputs) events.push(await mesh.publish(input));
    return events;
  };
  const inject = async (inbox: RootInbox, events: readonly MeshEvent[]) => {
    sent.length = 0;
    deliverRootInbox(pi, events);
    const messages = [...sent];
    for (const message of messages) {
      entries.push({ type: "custom_message", timestamp: new Date().toISOString(), ...message });
      await inbox.consume(message.details.ids, message.details.receipts);
    }
    return messages;
  };
  return {
    root, mesh, entries, sent, pi, box, publish, publishMany, inject,
    session: () => rootInboxSession(entries),
    now: () => Date.now(),
  };
};

const ids = (events: readonly MeshEvent[]) => events.map(event => event.id);
const messageIds = (messages: readonly InboxMessage[]) => messages.flatMap(message => message.details.ids);
const expectAckHeadroom = (mesh: MeshStore, inbox: RootInbox) => {
  const state = mesh.get(inbox.key, { fresh: true })!.value as {
    after: number; pending?: { ids: string[] }; acked?: Array<[string, number]>;
  };
  const acked = new Set(state.acked?.map(([id]) => id));
  const reserved = state.pending?.ids.filter(id => !acked.has(id)).length ?? 0;
  expect(acked.size + reserved).toBeLessThanOrEqual(256);
  return state;
};
const wakeIds = (messages: readonly InboxMessage[]) => messages.flatMap(message => {
  const details = message.details as Record<string, unknown>;
  const causes = [details.wakeCause, ...(Array.isArray(details.wakeCauses) ? details.wakeCauses : [])];
  return causes.flatMap(cause => cause && typeof cause === "object" && typeof (cause as { key?: unknown }).key === "string"
    ? [(cause as { key: string }).key] : []);
});
const outcomes = (root: string): Array<{ eventId?: string; outcome?: string }> => {
  const directory = path.join(root, "mesh", "delivery-outcomes");
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory).filter(name => name.endsWith(".jsonl")).flatMap(name =>
    fs.readFileSync(path.join(directory, name), "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)));
};

afterEach(async () => {
  for (const inbox of inboxes.splice(0)) await inbox.close();
  for (const mesh of meshes.splice(0)) await mesh.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("root inbox priority and displayed-only overflow (smarty-dev#12356)", () => {
  it("keeps the pending prefix bounded and injects kind=blocked from sequence 31 first", async () => {
    const h = setup();
    let inbox = h.box();
    inbox.start();
    const answers: MeshEvent[] = [];
    for (let index = 0; index < 30; index++) answers.push(await h.publish("answer", `answer ${index + 1}`));
    const blocked = await h.publish("blocked", "blocked work #12356");
    expect(blocked.sequence).toBe(answers.at(-1)!.sequence + 1);

    let batch = await inbox.next(notHeld);
    const pending = h.mesh.get(inbox.key, { fresh: true })!.value as { pending: { ids: string[] } };
    expect(pending.pending.ids).toHaveLength(20); // The durable contiguous prefix stays bounded despite lookahead.
    const expected = new Set([...ids(answers), blocked.id]);
    const firstMessages = await h.inject(inbox, batch.events);
    const injected = [...messageIds(firstMessages)];
    expect(injected[0]).toBe(blocked.id); // Sequence 31 wins the very first injection.
    await inbox.close(); // Displayed receipts and all withheld answer events survive reload.
    inbox = h.box();
    batch = await inbox.next(h.session());
    for (let turn = 0; turn < 5 && batch.events.length; turn++) {
      const messages = await h.inject(inbox, batch.events);
      injected.push(...messageIds(messages));
      batch = await inbox.next(h.session());
    }
    expect(batch.events).toEqual([]);
    expect(injected).toHaveLength(31);
    expect(new Set(injected).size).toBe(31);
    expect(new Set(injected)).toEqual(expected);
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it.each([
    { earlierKind: "answer", selectedKind: "done", scheduled: false },
    { earlierKind: "answer", selectedKind: "wait", scheduled: true },
    { earlierKind: "info", selectedKind: "answer", scheduled: false },
  ])("injects $selectedKind before thirty $earlierKind events despite the scan cap", async ({ earlierKind, selectedKind, scheduled }) => {
    const h = setup();
    const inbox = h.box();
    inbox.start();
    for (let index = 0; index < 30; index++) await h.publish(earlierKind, `${earlierKind} ${index + 1}`);
    let selectedId: string;
    let dueAt: number | undefined;
    if (scheduled) {
      dueAt = h.now() + 1_000;
      const scheduledEvent = await h.mesh.schedule({ topic: TOPIC, kind: selectedKind, from: peer, to: me.id,
        text: `selected ${selectedKind}`, dueAt }, h.now());
      selectedId = scheduledEvent.id;
    } else {
      selectedId = (await h.publish(selectedKind, `selected ${selectedKind}`)).id;
    }

    let batch = await inbox.next(notHeld);
    const pending = h.mesh.get(inbox.key, { fresh: true })!.value as { pending: { ids: string[] } };
    expect(pending.pending.ids).toHaveLength(20);
    if (scheduled) {
      expect(ids(batch.events)).not.toContain(selectedId);
      expect(await h.mesh.releaseDueSchedules(dueAt! - 1)).toEqual([]);
      expect(ids(await h.mesh.releaseDueSchedules(dueAt!))).toEqual([selectedId]);
      batch = await inbox.next(h.session());
    }
    const messages = await h.inject(inbox, batch.events);
    expect(messageIds(messages)[0]).toBe(selectedId);
  });

  it("shows all twelve mixed actionable events even when their text exceeds the byte budget", async () => {
    const h = setup();
    const inbox = h.box();
    inbox.start();
    const kinds = ["blocked", "blocker", "ask", "handoff"];
    const events: MeshEvent[] = [];
    for (let index = 0; index < 12; index++) {
      events.push(await h.publish(kinds[index % kinds.length]!, `${index}: ${"x".repeat(1_500)}`));
    }
    const batch = await inbox.next(notHeld);
    expect(batch.events).toHaveLength(12);

    const shaped = rootInboxMessage(batch.events);
    expect(shaped.content.match(/<event /g)).toHaveLength(12);
    expect(Buffer.byteLength(shaped.content)).toBeGreaterThan(INBOX_INJECT_MAX_TEXT_BYTES);
    expect(new Set(shaped.details.ids)).toEqual(new Set(ids(events)));

    const messages = await h.inject(inbox, batch.events);
    expect(messages.flatMap(message => message.content.match(/<event /g) ?? [])).toHaveLength(12);
    expect(new Set(messageIds(messages))).toEqual(new Set(ids(events)));
    expect((await inbox.next(h.session())).events).toEqual([]);
  });

  it("prioritizes a fresh blocker over retained low-priority overflow without losing either class", async () => {
    const h = setup();
    let inbox = h.box();
    inbox.start();
    const info: MeshEvent[] = [];
    for (let index = 0; index < 20; index++) info.push(await h.publish("info", `info ${index + 1} ${"i".repeat(1_400)}`));
    const first = await inbox.next(notHeld);
    expect(first.events).toHaveLength(20);
    const firstMessages = await h.inject(inbox, first.events);
    const shown = messageIds(firstMessages);
    expect(shown).toHaveLength(8);
    expect(shown).toEqual(ids(info.slice(0, 8)));
    const overflow = info.slice(8);
    expect(h.session().holdsBatch(ids(overflow))).toBe(false);
    const heldReceipts = h.session().delivered!;
    for (const event of overflow) {
      expect(rootInboxMessage([event]).details.receipts.some(receipt => heldReceipts.has(receipt))).toBe(false);
    }
    expect(outcomes(h.root).some(row => overflow.some(event => row.eventId === event.id && row.outcome === "delivered"))).toBe(false);
    expect(wakeIds(firstMessages)).toEqual(shown);
    expect(messageIds(firstMessages).some(id => ids(overflow).includes(id))).toBe(false);

    // Restart with twelve low-priority events still pending, then append a new urgent action.
    await inbox.close();
    inbox = h.box();
    const fresh = await h.publish("blocker", "newly blocked action");
    let batch = await inbox.next(h.session());

    const delivered = [...shown];
    for (let turn = 1; turn < 3; turn++) {
      const messages = await h.inject(inbox, batch.events);
      const turnIds = messageIds(messages);
      expect(turnIds.length).toBeGreaterThan(0);
      if (turn === 1) expect(turnIds[0]).toBe(fresh.id);
      delivered.push(...turnIds);
      batch = await inbox.next(h.session());
      if (!batch.events.length) break;
    }
    expect(batch.events).toEqual([]);
    expect(delivered).toHaveLength(21);
    expect(new Set(delivered).size).toBe(21);
    expect(new Set(delivered)).toEqual(new Set([...ids(info), fresh.id]));
    expect((await h.box().next(h.session())).events).toEqual([]);
  });

  it("drains more than 1024 actions across reloads without reoffering consumed work or starving the prefix", async () => {
    const h = setup();
    let inbox = h.box();
    inbox.start();
    const info = await h.publishMany(Array.from({ length: 20 }, (_, index) => ({
      topic: TOPIC, kind: "info", from: peer, to: me.id, text: `pinned info ${index + 1}`,
    })));
    const actions = await h.publishMany(Array.from({ length: 1_100 }, (_, index) => ({
      topic: TOPIC, kind: "blocked", from: peer, to: me.id, text: `burst action ${index + 1}`,
    })));
    const consumed = new Set<string>();
    const prefixIds = new Set(ids(info));

    for (let turn = 0; turn < 32; turn++) {
      // No session receipt history: persisted acks/cursor must suffice after each reload.
      const batch = await inbox.next(notHeld);
      expectAckHeadroom(h.mesh, inbox);
      for (const id of ids(batch.events)) expect(consumed.has(id)).toBe(false);
      if (!batch.events.length) break;
      if (turn === 0) {
        expect(batch.events.filter(event => !prefixIds.has(event.id))).toHaveLength(236);
        expect(batch.events.filter(event => event.kind === "blocked")).toHaveLength(236);
      } else if (turn <= 3) {
        // The first carrier filled the tail window, so the pinned prefix gets three turns.
        expect(batch.events.every(event => prefixIds.has(event.id))).toBe(true);
      }
      const shown = messageIds(await h.inject(inbox, batch.events));
      expect(shown.length).toBeGreaterThan(0);
      for (const id of shown) {
        expect(consumed.has(id)).toBe(false);
        consumed.add(id);
      }
      expectAckHeadroom(h.mesh, inbox);
      await inbox.close();
      inbox = h.box();
    }

    expect(consumed.size).toBe(1_120);
    expect(consumed).toEqual(new Set([...ids(info), ...ids(actions)]));
    expect((await inbox.next(notHeld)).events).toEqual([]);
    expect((await h.box().next(notHeld)).events).toEqual([]);
  }, 30_000);

  it("shares cumulative ack slots with done/wait/answer lookahead and reserves the whole pending prefix", async () => {
    const h = setup();
    let inbox = h.box();
    inbox.start();
    const info = await h.publishMany(Array.from({ length: 20 }, (_, index) => ({
      topic: TOPIC, kind: "info", from: peer, to: me.id, text: `reserved info ${index + 1}`,
    })));
    const actions = await h.publishMany(Array.from({ length: 230 }, (_, index) => ({
      topic: TOPIC, kind: "ask", from: peer, to: me.id, text: `reserved action ${index + 1}`,
    })));
    const priorities = await h.publishMany(Array.from({ length: 60 }, (_, index) => ({
      topic: TOPIC, kind: ["done", "wait", "answer"][index % 3]!, from: peer, to: me.id,
      text: `nonaction priority ${index + 1}`,
    })));
    const consumed = new Set<string>();
    const prefixIds = new Set(ids(info));
    const priorityIds = new Set(ids(priorities));

    for (let turn = 0; turn < 32; turn++) {
      const batch = await inbox.next(notHeld);
      expectAckHeadroom(h.mesh, inbox);
      for (const id of ids(batch.events)) expect(consumed.has(id)).toBe(false);
      if (!batch.events.length) break;
      if (turn === 0) {
        expect(batch.events.filter(event => !prefixIds.has(event.id))).toHaveLength(236);
        expect(batch.events.filter(event => priorityIds.has(event.id))).toHaveLength(6);
      } else if (turn === 1) {
        // 230 outstanding action acks + 20 pending ids leave six slots, not another top 20.
        expect(batch.events.filter(event => priorityIds.has(event.id))).toHaveLength(6);
        expect(batch.events).toHaveLength(26);
      } else if (turn <= 4) {
        // Six priorities plus two infos were shown on turn 1; the other 18 infos must progress.
        expect(batch.events.every(event => prefixIds.has(event.id))).toBe(true);
      }
      const shown = messageIds(await h.inject(inbox, batch.events));
      expect(shown.length).toBeGreaterThan(0);
      if (turn === 0) expect(shown).toEqual(ids(actions));
      if (turn === 1) {
        expect(shown.filter(id => priorityIds.has(id))).toHaveLength(6);
        expect(shown.filter(id => prefixIds.has(id))).toEqual(ids(info.slice(0, 2)));
      }
      for (const id of shown) {
        expect(consumed.has(id)).toBe(false);
        consumed.add(id);
      }
      expectAckHeadroom(h.mesh, inbox);
      await inbox.close();
      inbox = h.box();
    }

    expect(consumed).toEqual(new Set([...ids(info), ...ids(actions), ...ids(priorities)]));
    expect((await inbox.next(notHeld)).events).toEqual([]);
    expect((await h.box().next(notHeld)).events).toEqual([]);
  }, 30_000);

  it("prioritizes a blocked action beyond the bounded handoff lookahead", async () => {
    const h = setup();
    const inbox = h.box();
    inbox.start();
    await h.publishMany(Array.from({ length: 20 }, (_, index) => ({
      topic: TOPIC, kind: "info", from: peer, to: me.id, text: `prefix ${index}`,
    })));
    await h.publishMany(Array.from({ length: 300 }, (_, index) => ({
      topic: TOPIC, kind: "handoff", from: peer, to: me.id, text: `handoff ${index}`,
    })));
    const blocked = await h.publish("blocked", "highest-priority action after handoff burst");
    const batch = await inbox.next(notHeld);
    expectAckHeadroom(h.mesh, inbox);
    expect(messageIds(await h.inject(inbox, batch.events))[0]).toBe(blocked.id);
  });

  it("keeps FIFO within blocker, answer, and ordinary classes", async () => {
    const h = setup();
    const inbox = h.box();
    inbox.start();
    const answer1 = await h.publish("answer", "answer one");
    const blocker1 = await h.publish("blocker", "blocker one");
    const answer2 = await h.publish("answer", "answer two");
    const blocker2 = await h.publish("blocker", "blocker two");
    const info1 = await h.publish("info", "info one");
    const info2 = await h.publish("info", "info two");

    const batch = await inbox.next(notHeld);
    const messages = await h.inject(inbox, batch.events);
    expect(messageIds(messages)).toEqual([blocker1.id, blocker2.id, answer1.id, answer2.id, info1.id, info2.id]);
    expect((await inbox.next(h.session())).events).toEqual([]);
  });

  it("does not expose a scheduled blocker before its due time", async () => {
    const h = setup();
    const inbox = h.box();
    inbox.start();
    const dueAt = h.now() + 1_000;
    const schedule = await h.mesh.schedule({ topic: TOPIC, kind: "blocker", from: peer, to: me.id,
      text: "due blocker", data: { key: "due-blocker" }, dueAt }, h.now());

    expect((await inbox.next(notHeld)).events).toEqual([]);
    expect(await h.mesh.releaseDueSchedules(dueAt - 1)).toEqual([]);
    expect((await inbox.next(notHeld)).events).toEqual([]);
    expect(ids(await h.mesh.releaseDueSchedules(dueAt))).toEqual([schedule.id]);

    const due = await inbox.next(notHeld);
    expect(ids(due.events)).toEqual([schedule.id]);
    const messages = await h.inject(inbox, due.events);
    expect(messageIds(messages)).toEqual([schedule.id]);
    expect((await inbox.next(h.session())).events).toEqual([]);
  });
});
