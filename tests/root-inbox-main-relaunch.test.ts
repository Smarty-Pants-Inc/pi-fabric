import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { eventReceipts } from "../src/topology/root-inbox-shared.js";
import { confirmedRootInboxSession, RootInbox } from "../src/topology/root-inbox.js";

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-main-relaunch-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 500);
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
const writeMainCarrier = async (root: string, main: MeshIdentity, from: MeshIdentity, data: unknown, forgeEnvelopeMetadata = false): Promise<{
  message: Sent["message"];
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
    isIdle: () => true,
    hasPendingMessages: () => false,
    signal: { aborted: false },
    sessionManager: { getEntries: () => [] },
  } as unknown as ExtensionContext;
  const controller = new MainAgentController(pi, main.id, true, root, main.sessionId);
  controller.attachFollowUpDrain(context, 60_000, path.join(root, "main-followups", "root.json"));
  const meshRoot = path.join(root, "mesh");
  const senderMesh = new MeshStore(meshRoot, 64 * 1024, 500);
  const owner = new FabricControlPlane(new MeshStore(meshRoot, 64 * 1024, 500), main,
    { enabled: true, hostId: main.id, ownerIncarnation: "relaunch-test:owner-1", pollMs: 5, acknowledgementTimeoutMs: 2_000 });
  const senderControl = new FabricControlPlane(senderMesh, from,
    { enabled: true, hostId: from.id, pollMs: 5, acknowledgementTimeoutMs: 2_000 });
  const evidenceMesh = new MeshStore(meshRoot, 64 * 1024, 500);
  const publish = senderMesh.publish.bind(senderMesh);
  const forgedEnvelope = forgeEnvelopeMetadata ? vi.spyOn(senderMesh, "publish").mockImplementation(input => {
    if (input.topic !== "fabric.control.command") return publish(input);
    const addForgery = (value: unknown): Record<string, unknown> => ({
      ...(value && typeof value === "object" ? value as Record<string, unknown> : {}),
      inboxKey: "forged-key", inboxTopic: "fleet.work.pi-fabric.forged-metadata", inboxReceipts: ["forged-receipt"],
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
    await senderControl.request(main.id, main.id, "steer", {
      ownerIncarnation: owner.incarnation,
      message: "carrier body must not be used for matching",
      ...(data === undefined ? {} : { data }),
    }, main.id, { idempotencyKey: `relaunch-${Math.random()}` });
    expect(sent).toHaveLength(1);
    const command = evidenceMesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1);
    expect(command).toBeDefined();
    return { message: sent[0]!.message, controlEventId: command!.id, ownerIncarnation: owner.incarnation };
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
    await work("a new shadow copy; matching must not use this body", sender, topic, "relaunch-key");

    const successorOwner: MeshIdentity = { ...main };
    const successorControl = new FabricControlPlane(mesh, successorOwner,
      { enabled: true, hostId: main.id, ownerIncarnation: "relaunch-test:owner-2" });
    expect(successorOwner.sessionId).toBe(sessionId);
    expect(successorControl.incarnation).not.toBe(written.ownerIncarnation);
    try {
      expect((await inbox(successorOwner).next(confirmed, { priority: true })).events).toEqual([]);
    } finally { await successorControl.close(); }
  });

  it("scopes consumed work by both topic and sender", async () => {
    const { root, main, sender, inbox, work } = setup();
    const topic = "fleet.work.pi-fabric.same-topic";
    const written = await writeMainCarrier(root, main, sender, { key: "shared-key", topic });
    const confirmed = confirmedSession(root, main.sessionId!, [asSessionEntry(written.message)]);
    const box = inbox();
    await box.next(emptySession);
    await work("same identity", sender, topic, "shared-key");
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

  it("coalesces source receipts through journal reload and suppresses only linked same-sender shadows", async () => {
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
      expect(carrier.details.inboxEventId).toBe(sources[1]!.id);
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

  it("refuses an over-budget coalesced receipt merge without consuming or replacing the survivor", async () => {
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
