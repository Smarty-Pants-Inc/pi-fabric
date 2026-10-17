import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FOLLOW_UP_LIMITS, MainAgentController } from "../src/main-agent.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { eventReceipts, topicWorkReceipt } from "../src/topology/root-inbox-shared.js";
import { confirmedRootInboxSession, RootInbox } from "../src/topology/root-inbox.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-relaunch-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 1024 * 1024, 500);
  const main: MeshIdentity = { id: "session:root-relaunch", name: "main", kind: "main", sessionId: "root-relaunch" };
  const sender: MeshIdentity = { id: "session:sender", name: "sender", kind: "main", sessionId: "sender" };
  const inbox = (identity: MeshIdentity = main) => new RootInbox(mesh, identity, () => [identity.id], { steerGraceMs: 0 });
  const work = (text: string, from: MeshIdentity, topic: string, key?: string, extra: Record<string, unknown> = {}) =>
    mesh.publish({ topic, to: main.id, kind: "handoff", from, text, data: { ...(key === undefined ? {} : { key }), ...extra } });
  return { root, mesh, main, sender, inbox, work };
};

type Sent = { message: { details: Record<string, unknown>; content: string }; options: unknown };
type Handler = (event: unknown, ctx: ExtensionContext) => unknown;

/** Send through the real FabricControlPlane command parser and AgentMessageRouter.acceptControl,
 * then capture MainAgentController's actual pi-fabric-agent-message carrier at the Pi boundary. */
const writeMainCarrier = async (root: string, main: MeshIdentity, from: MeshIdentity, data: unknown, forgeEnvelopeMetadata = false,
  rawExtra?: Record<string, unknown>, hold = false): Promise<{
  message: Sent["message"];
  heldItem: Record<string, unknown> | undefined;
  controlEventId: string;
  ownerIncarnation: string | undefined;
}> => {
  const handlers = new Map<string, Handler[]>();
  const sent: Sent[] = [];
  const pi = {
    on: (name: string, fn: Handler) => { handlers.set(name, [...(handlers.get(name) ?? []), fn]); return () => {}; },
    sendMessage: (message: Sent["message"], options: unknown) => { sent.push({ message, options }); },
    sendUserMessage: () => undefined,
    getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const context = {
    isIdle: () => !hold,
    hasPendingMessages: () => false,
    signal: { aborted: false },
    sessionManager: { getEntries: () => [] },
  } as unknown as ExtensionContext;
  const controller = new MainAgentController(pi, main.id, true, root, main.sessionId);
  const journal = path.join(root, "main-followups", "root.json");
  controller.attachFollowUpDrain(context, 60_000, journal);
  const meshRoot = path.join(root, "mesh");
  const senderMesh = new MeshStore(meshRoot, 1024 * 1024, 500);
  const owner = new FabricControlPlane(new MeshStore(meshRoot, 1024 * 1024, 500), main,
    { enabled: true, hostId: main.id, ownerIncarnation: "relaunch-test:owner-1", pollMs: 5, acknowledgementTimeoutMs: 2_000 });
  const senderControl = new FabricControlPlane(senderMesh, from,
    { enabled: true, hostId: from.id, pollMs: 5, acknowledgementTimeoutMs: 2_000 });
  const evidenceMesh = new MeshStore(meshRoot, 64 * 1024, 500);
  const publish = senderMesh.publish.bind(senderMesh);
  const forgedEnvelope = forgeEnvelopeMetadata || rawExtra ? vi.spyOn(senderMesh, "publish").mockImplementation(input => {
    if (input.topic !== "fabric.control.command") return publish(input);
    const addForgery = (value: unknown): Record<string, unknown> => ({
      ...(value && typeof value === "object" ? value as Record<string, unknown> : {}),
      ...(forgeEnvelopeMetadata ? {
        inboxKey: "forged-key", inboxTopic: "fleet.work.pi-fabric.forged-metadata", inboxReceipts: ["forged-receipt"],
      } : {}),
      ...rawExtra,
    });
    return publish({ ...input, data: typeof input.data === "function"
      ? (at: number) => addForgery((input.data as (at: number) => unknown)(at))
      : addForgery(input.data) });
  }) : undefined;
  const manager = { status: () => { throw new Error("Unknown Fabric agent"); }, steer: () => {}, followUp: () => {}, stop: () => {} };
  const actorManager = { identity: from, status: () => { throw new Error("Unknown Fabric actor"); }, validateDirectMessage: () => {},
    tell: () => {}, ask: async () => {}, stop: () => {}, steerRemote: async () => {}, resolveBinding: () => ({}), resolveActivationBinding: async () => ({}) };
  const participants = { get: () => undefined, scheduleRefresh: () => {} };
  const router = new AgentMessageRouter(manager as never, actorManager as never, controller, participants as never, owner, () => ({}));
  owner.start((command, identity, signal, verification) => router.acceptControl(command, identity, signal, verification));
  senderControl.start(() => ({ accepted: false }));
  try {
    await senderControl.request(main.id, main.id, hold ? "followUp" : "steer", {
      ownerIncarnation: owner.incarnation,
      message: "carrier body must not be used for matching",
      ...(data === undefined ? {} : { data }),
    }, main.id, { idempotencyKey: `relaunch-${Math.random()}` });
    const heldItem = hold ? JSON.parse(fs.readFileSync(journal, "utf8")).items[0] as Record<string, unknown> : undefined;
    if (hold) {
      expect(sent).toHaveLength(0);
      controller.closeFollowUpDrain();
      const reloaded = new MainAgentController(pi, main.id, true, root, main.sessionId);
      try {
        reloaded.attachFollowUpDrain({ ...context, isIdle: () => true }, 0, journal);
      } finally { reloaded.closeFollowUpDrain(); }
    }
    expect(sent).toHaveLength(1);
    const command = evidenceMesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1);
    expect(command).toBeDefined();
    return { message: sent[0]!.message, heldItem, controlEventId: command!.id, ownerIncarnation: owner.incarnation };
  } finally {
    forgedEnvelope?.mockRestore();
    controller.closeFollowUpDrain();
    await Promise.all([owner.close(), senderControl.close()]);
    evidenceMesh.closeState();
  }
};

/** Persist a canonical Pi session header and custom_message JSONL entry, then use the real
 * confirmed-file receipt parser. The session id is deliberately stable across owner objects. */
const confirmedSession = (root: string, sessionId: string, messages: readonly unknown[]) => {
  const file = path.join(root, "session.jsonl");
  fs.writeFileSync(file, [
    JSON.stringify({ type: "session", version: 3, id: sessionId, timestamp: new Date().toISOString(), cwd: root }),
    ...messages.map(message => JSON.stringify(message)),
    "",
  ].join("\n"));
  return confirmedRootInboxSession({
    getEntries: () => [],
    getSessionFile: () => file,
    isPersisted: () => true,
  });
};

const asSessionEntry = (message: { details: Record<string, unknown>; content: string }) => ({
  type: "custom_message",
  timestamp: new Date().toISOString(),
  customType: "pi-fabric-agent-message",
  content: message.content,
  details: message.details,
});

const emptySession = { holdsBatch: () => false, holdsSteer: () => false };

describe("RootInbox after a Main session relaunch", () => {
  it("uses a consumed Main steer from the same canonical session after a new owner incarnation is created", async () => {
    const { root, mesh, main, sender, inbox, work } = setup();
    const topic = "fleet.work.pi-fabric.relaunch";
    const sessionId = main.sessionId!;
    const written = await writeMainCarrier(root, main, sender, { key: "relaunch-key", topic });
    const oldEntry = asSessionEntry(written.message);
    expect(written.message.details).toMatchObject({ inboxEventId: written.controlEventId, inboxTopic: "fabric.control.command" });
    expect(written.message.details).not.toHaveProperty("inboxKey");
    expect(written.message.details.data).toMatchObject({ key: "relaunch-key", topic });
    expect(written.message.details.receipts).toEqual(expect.arrayContaining([expect.any(String)]));
    const confirmed = confirmedSession(root, sessionId, [oldEntry]);

    const box = inbox();
    await box.next(emptySession);
    // Real shadows retain the source event id; a key-only fixture is unrelated work.
    await work("a new shadow copy; matching must not use this body", sender, topic, "relaunch-key",
      { messageId: written.controlEventId });

    const successorOwner: MeshIdentity = { ...main };
    const successorControl = new FabricControlPlane(mesh, successorOwner,
      { enabled: true, hostId: main.id, ownerIncarnation: "relaunch-test:owner-2" });
    expect(successorOwner.sessionId).toBe(sessionId);
    expect(successorControl.incarnation).not.toBe(written.ownerIncarnation);
    try {
      expect((await inbox(successorOwner).next(confirmed, { priority: true })).events).toEqual([]);
      await work("distinct event with the same sender, topic and key", sender, topic, "relaunch-key",
        { messageId: `${written.controlEventId}:distinct` });
      expect((await inbox(successorOwner).next(confirmed, { priority: true })).events.map(event => event.text)).toEqual([
        "distinct event with the same sender, topic and key",
      ]);
    } finally { await successorControl.close(); }
  });

  it("scopes consumed work by both topic and sender", async () => {
    const { root, main, sender, inbox, work } = setup();
    const topic = "fleet.work.pi-fabric.same-topic";
    const written = await writeMainCarrier(root, main, sender, { key: "shared-key", topic });
    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(written.message)]);
    const box = inbox();
    await box.next(emptySession);
    await work("distinct event with the same sender, topic and key", sender, topic, "shared-key",
      { messageId: `${written.controlEventId}:distinct` });
    const distinct = await box.next(confirmed, { priority: true });
    expect(distinct.events.map(event => event.text)).toEqual(["distinct event with the same sender, topic and key"]);
    await box.consume(distinct.events.map(event => event.id));
    // Real shadows retain the source event id; the same key alone cannot establish identity.
    await work("same identity", sender, topic, "shared-key", { messageId: written.controlEventId });
    await work("same sender and key, different topic", sender, "fleet.work.pi-fabric.other-topic", "shared-key");
    await work("same topic and key, different sender", { ...sender, id: "session:other-sender" }, topic, "shared-key");

    expect((await box.next(confirmed, { priority: true })).events.map(event => event.text)).toEqual([
      "same sender and key, different topic",
      "same topic and key, different sender",
    ]);
  });

  it("uses an id-only carrier receipt without requiring a work key or topic", async () => {
    const { root, main, sender, inbox, work } = setup();
    const written = await writeMainCarrier(root, main, sender, undefined);
    expect(written.message.details).not.toHaveProperty("data");
    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(written.message)]);
    const box = inbox();
    await box.next(emptySession);
    // Only the admitted outer event id links this shadow to its consumed steer.
    await work("receipt-id match", sender, "fleet.work.pi-fabric.receipts", undefined, { messageId: written.controlEventId });
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events).toEqual([]);
  });

  it("retains both source receipts when held followUps are journaled, reloaded, and batched", async () => {
    const { root, main, sender, inbox, work } = setup();
    inbox().start();
    const events = [
      await work("first receipt-only followUp", sender, "fleet.work.pi-fabric.batch"),
      await work("second receipt-only followUp", { ...sender, id: "session:second-sender" }, "fleet.work.pi-fabric.batch"),
    ];
    const sent: Sent["message"][] = [];
    const pi = {
      on: () => () => {},
      sendMessage: (message: Sent["message"]) => { sent.push(message); },
    } as unknown as ExtensionAPI;
    let idle = false;
    const context = {
      isIdle: () => idle, hasPendingMessages: () => false, signal: { aborted: false },
      sessionManager: { getEntries: () => [] },
    } as unknown as ExtensionContext;
    const journal = path.join(root, "main-followups", "batch.json");
    const first = new MainAgentController(pi, main.id, true, root, main.sessionId);
    first.attachFollowUpDrain(context, 60_000, journal);
    try {
      for (const event of events) first.deliverAgent({
        from: event.from, message: event.text!, delivery: "followUp", deliveryId: `delivery-${event.id}`,
        inboxEventId: event.id, inboxTopic: event.topic, inboxReceipts: eventReceipts(event),
      });
      expect(sent).toEqual([]);
    } finally { first.closeFollowUpDrain(); }
    idle = true;
    const reloaded = new MainAgentController(pi, main.id, true, root, main.sessionId);
    try {
      reloaded.attachFollowUpDrain(context, 60_000, journal);
      expect(sent).toHaveLength(1);
      expect(sent[0]!.details.receipts).toEqual(events.flatMap(eventReceipts));
      expect(sent[0]!.details.items).toHaveLength(2);
      const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(sent[0]!)]);
      expect((await inbox({ ...main }).next(confirmed, { priority: true })).events).toEqual([]);
    } finally { reloaded.closeFollowUpDrain(); }
  });

  it("retains every source receipt through journal reload and suppresses only linked same-sender shadows", async () => {
    const { root, main, sender, inbox, work } = setup();
    const topic = "fleet.work.pi-fabric.coalesced-receipts";
    const sources = [
      await work("source one", sender, topic),
      await work("source two", sender, topic),
    ];
    const sent: Sent["message"][] = [];
    const pi = {
      on: () => () => {},
      sendMessage: (message: Sent["message"]) => { sent.push(message); },
    } as unknown as ExtensionAPI;
    let idle = false;
    const context = {
      isIdle: () => idle, hasPendingMessages: () => false, signal: { aborted: false },
      sessionManager: { getEntries: () => [] },
    } as unknown as ExtensionContext;
    const journal = path.join(root, "main-followups", "coalesced.json");
    const first = new MainAgentController(pi, main.id, true, root, main.sessionId);
    first.attachFollowUpDrain(context, 60_000, journal);
    try {
      sources.forEach((event, index) => first.deliverAgent({
        from: event.from, message: `coalesced source ${index}`, delivery: "followUp",
        data: { coalesceKey: "same-held-followup" }, inboxEventId: event.id,
        inboxTopic: event.topic, inboxReceipts: eventReceipts(event),
      }));
      expect(sent).toEqual([]);
    } finally { first.closeFollowUpDrain(); }

    idle = true;
    const reloaded = new MainAgentController(pi, main.id, true, root, main.sessionId);
    let carrier: Sent["message"];
    try {
      reloaded.attachFollowUpDrain(context, 0, journal);
      expect(sent).toHaveLength(1);
      carrier = sent[0]!;
      const items = carrier.details.items as Array<{ inboxEventId: string; from: MeshIdentity }>;
      expect(items.map(item => item.inboxEventId)).toEqual(sources.map(event => event.id));
      expect(items.map(item => item.from)).toEqual(sources.map(event => event.from));
    } finally { reloaded.closeFollowUpDrain(); }

    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(carrier!)]);
    const box = inbox();
    await box.next(confirmed);
    await work("same sender shadow for E1", sender, topic, undefined, { messageId: sources[0]!.id });
    await work("same sender shadow for E2", sender, topic, undefined, { messageId: sources[1]!.id });
    await work("different sender linked to E1", { ...sender, id: "session:linked-other-sender" }, topic,
      undefined, { messageId: sources[0]!.id });
    await work("different topic with an unconsumed identity", sender,
      "fleet.work.pi-fabric.coalesced-other-topic", undefined, { messageId: "unconsumed-distinct-identity" });
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events.map(event => event.text)).toEqual([
      "different sender linked to E1",
      "different topic with an unconsumed identity",
    ]);
    expect(carrier!.details.receipts).toEqual([...new Set(sources.flatMap(eventReceipts))]);
  });

  it("refuses an over-budget same-key receipt append without consuming or replacing the survivor", async () => {
    const { root, main, sender } = setup();
    const pi = { on: () => () => {}, sendMessage: () => {} } as unknown as ExtensionAPI;
    const context = {
      isIdle: () => false, hasPendingMessages: () => false, signal: { aborted: false },
      sessionManager: { getEntries: () => [] },
    } as unknown as ExtensionContext;
    const journal = path.join(root, "main-followups", "receipt-quota.json");
    const controller = new MainAgentController(pi, main.id, true, root, main.sessionId);
    controller.attachFollowUpDrain(context, 60_000, journal);
    const originalReceipt = "source-receipt-original";
    try {
      controller.deliverAgent({ from: sender, message: "original survivor", delivery: "followUp",
        data: { coalesceKey: "quota-key" }, inboxReceipts: [originalReceipt] });
      const before = fs.readFileSync(journal, "utf8");
      expect(() => controller.deliverAgent({ from: sender, message: "replacement", delivery: "followUp",
        data: { coalesceKey: "quota-key" }, inboxReceipts: ["x".repeat(270 * 1024)] })).toThrow(/queue is full/);
      expect(fs.readFileSync(journal, "utf8")).toBe(before);
      const saved = JSON.parse(before) as { items?: Array<{ message?: string; inboxReceipts?: string[] }> };
      expect(saved.items).toHaveLength(1);
      expect(saved.items![0]).toMatchObject({ message: "original survivor", inboxReceipts: [originalReceipt] });
    } finally { controller.closeFollowUpDrain(); }
  });

  it("ignores a raw outer work key through control parsing, held journal, native carrier and relaunch", async () => {
    const { root, main, sender, inbox, work } = setup();
    const rawKey = "raw-" + "x".repeat(FOLLOW_UP_LIMITS.senderBytes);
    const written = await writeMainCarrier(root, main, sender, undefined, false, { key: rawKey }, true);
    expect.soft(Object.hasOwn(written.heldItem!, "inboxKey")).toBe(false);
    expect.soft(Object.hasOwn(written.heldItem!, "data")).toBe(false);
    expect.soft(Object.hasOwn(written.message.details, "inboxKey")).toBe(false);
    expect.soft(Object.hasOwn(written.message.details, "data")).toBe(false);
    const rawReceipt = topicWorkReceipt(sender.id, "key", "fabric.control.command", rawKey);
    expect.soft(written.heldItem?.inboxReceipts).not.toContain(rawReceipt);
    expect.soft(written.message.details.receipts).not.toContain(rawReceipt);
    expect(written.message.details.inboxEventId).toBe(written.controlEventId);
    const box = inbox();
    await box.next(emptySession);
    await work("an unknown outer key cannot consume this work", sender, "fleet.work.pi-fabric.raw-key", rawKey);
    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(written.message)]);
    expect(confirmed.holdsSteer(sender.id, rawKey, "fabric.control.command")).toBe(false);
    expect(confirmed.delivered?.has(rawReceipt)).toBe(false);
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events.map(event => event.text))
      .toContain("an unknown outer key cannot consume this work");
  });

  it("preserves a legitimate typed nested work key despite a conflicting raw outer key", async () => {
    const { root, main, sender, inbox, work } = setup();
    const data = { key: "typed-nested-key", topic: "fleet.work.pi-fabric.typed-counterexample" };
    const written = await writeMainCarrier(root, main, sender, data, false, { key: "noncanonical-outer-key" }, true);
    expect(written.heldItem).not.toHaveProperty("inboxKey");
    expect(written.heldItem?.data).toEqual(data);
    expect(written.message.details).not.toHaveProperty("inboxKey");
    expect(written.message.details.data).toEqual(data);
    const box = inbox();
    await box.next(emptySession);
    // Real shadows retain the source event id even when the typed work has a nested key.
    await work("nested key still claims its typed work", sender, data.topic, data.key,
      { messageId: written.controlEventId });
    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(written.message)]);
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events).toEqual([]);
    await work("distinct event with the same typed key", sender, data.topic, data.key,
      { messageId: `${written.controlEventId}:distinct` });
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events.map(event => event.text)).toEqual([
      "distinct event with the same typed key",
    ]);
  });

  it.each([undefined, { explicit: true }])("refuses an oversized retained inboxKey with data %j before journal mutation", data => {
    const { root, main, sender } = setup();
    const sent: unknown[] = [];
    const pi = { on: () => () => {}, sendMessage: (message: unknown) => sent.push(message) } as unknown as ExtensionAPI;
    const context = { isIdle: () => false, hasPendingMessages: () => false,
      signal: { aborted: false } } as unknown as ExtensionContext;
    const journal = path.join(root, "main-followups", "oversized-key.json");
    const controller = new MainAgentController(pi, main.id, true, root, main.sessionId);
    controller.attachFollowUpDrain(context, 60_000, journal);
    try {
      controller.deliverAgent({ from: sender, message: "survivor", delivery: "followUp" });
      const before = fs.readFileSync(journal, "utf8");
      expect(() => controller.deliverAgent({ from: sender, message: "oversized", delivery: "followUp",
        inboxKey: "x".repeat(FOLLOW_UP_LIMITS.senderBytes), data })).toThrow(/queue is full/);
      expect(controller.queueDepth().pendingFollowUps).toBe(1);
      expect(fs.readFileSync(journal, "utf8")).toBe(before);
      expect(sent).toEqual([]);
    } finally { controller.closeFollowUpDrain(); }
  });

  it.each(["synthesized", "nested", "nested-and-key"] as const)("charges exact %s data and retained-key bytes before insertion or replacement", shape => {
    const { root, main, sender } = setup();
    const sent: Sent["message"][] = [];
    const pi = { on: () => () => {}, sendMessage: (message: Sent["message"]) => sent.push(message) } as unknown as ExtensionAPI;
    const context = { isIdle: () => false, hasPendingMessages: () => false,
      signal: { aborted: false } } as unknown as ExtensionContext;
    const journal = path.join(root, "main-followups", "exact-key-quota.json");
    const controller = new MainAgentController(pi, main.id, true, root, main.sessionId);
    controller.attachFollowUpDrain(context, 60_000, journal);
    const survivor = { from: sender, message: "survivor", delivery: "followUp" as const, data: { coalesceKey: "keep" } };
    const inboxKey = shape === "nested" ? undefined : 'é"\\'.repeat(20);
    const inboxTopic = "fleet.work.pi-fabric.quota";
    const data = shape === "synthesized" ? undefined : { key: "explicit-key", topic: inboxTopic, value: 'é"\\' };
    const nativeData = shape === "synthesized" ? { key: inboxKey, topic: inboxTopic } : data;
    const serializedBytes = (value: unknown) => value === undefined ? 0 : Buffer.byteLength(JSON.stringify(value));
    const message = "x".repeat(FOLLOW_UP_LIMITS.senderBytes - Buffer.byteLength(survivor.message) -
      serializedBytes(survivor.data) - serializedBytes(inboxKey) - serializedBytes(nativeData));
    const candidate = { from: sender, message, delivery: "followUp" as const, inboxKey, inboxTopic, data };
    try {
      controller.deliverAgent(survivor);
      const before = fs.readFileSync(journal, "utf8");
      expect(() => controller.deliverAgent({ ...candidate, message: message + "x" })).toThrow(/queue is full/);
      expect(controller.queueDepth().pendingFollowUps).toBe(1);
      expect(fs.readFileSync(journal, "utf8")).toBe(before);
      controller.deliverAgent(candidate); // Equality at the quota is admitted, not rounded down.
      expect(controller.queueDepth().pendingFollowUps).toBe(2);
      const exact = fs.readFileSync(journal, "utf8");
      expect(() => controller.deliverAgent({ ...survivor, message: survivor.message + "x" })).toThrow(/queue is full/);
      expect(controller.queueDepth().pendingFollowUps).toBe(2);
      expect(fs.readFileSync(journal, "utf8")).toBe(exact);
      expect(sent).toEqual([]);
      controller.closeFollowUpDrain();
      const reloaded = new MainAgentController(pi, main.id, true, root, main.sessionId);
      try {
        reloaded.attachFollowUpDrain({ ...context, isIdle: () => true }, 0, journal);
      } finally { reloaded.closeFollowUpDrain(); }
      const details = sent.flatMap(message => (message.details.items as Record<string, unknown>[] | undefined) ?? [message.details]);
      const carried = details.find(item => item.data && (item.data as { key?: string }).key === (nativeData as { key: string }).key);
      expect(carried?.data).toEqual(nativeData);
      if (inboxKey !== undefined) expect(carried?.inboxKey).toBe(inboxKey);
    } finally { controller.closeFollowUpDrain(); }
  });

  it("fails open for a payload-forged inboxKey instead of trusting it as host metadata", async () => {
    const { root, main, sender, inbox, work } = setup();
    const topic = "fleet.work.pi-fabric.forged-metadata";
    const written = await writeMainCarrier(root, main, sender, undefined, true);
    expect(written.message.details).not.toMatchObject({ inboxKey: "forged-key" });
    expect(written.message.details.receipts ?? []).not.toContain("forged-receipt");
    const box = inbox();
    await box.next(emptySession);
    await work("forged metadata cannot claim this", sender, topic, "forged-key");
    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(written.message)]);
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events.map(event => event.text)).toEqual([
      "forged metadata cannot claim this",
    ]);
  });

  it("fails open for legacy session carriers that lack topic and receipt metadata", async () => {
    const { root, main, sender, inbox, work } = setup();
    const box = inbox();
    await box.next(emptySession);
    await work("must remain deliverable", sender, "fleet.work.pi-fabric.legacy", "legacy-key");
    const legacyEntry = {
      type: "custom_message",
      timestamp: new Date().toISOString(),
      customType: "pi-fabric-agent-message",
      content: "legacy carrier body must not be used for matching",
      details: { id: "legacy-message-id", from: sender, data: { key: "legacy-key" } },
    };
    const confirmed = confirmedSession(root, main.sessionId!, [legacyEntry]);
    expect((await inbox({ ...main }).next(confirmed, { priority: true })).events.map(event => event.text)).toEqual([
      "must remain deliverable",
    ]);
  });
});
