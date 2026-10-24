import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ACTOR_MESSAGE_ENVELOPE_BYTES } from "../src/actors/log-store.js";
import { createAgentServiceClient, createAgentsProvider } from "../src/agents/service-provider.js";
import { FOLLOW_UP_LIMITS, MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { MeshProvider } from "../src/providers/mesh-provider.js";
import { RootInbox, rootInboxSession as unsignedRootInboxSession } from "../src/topology/root-inbox.js";
import { rootInboxMessage, rootInboxSession } from "./root-inbox-issued-fixture.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { eventReceipts } from "../src/topology/root-inbox-shared.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";

const text = "head fedc9876";
const notice = "unverified ids: fedc9876";
const identity: MeshIdentity = { id: "session:sender", name: "Sender", kind: "main" };
const session = (read = false) => {
  const manager = SessionManager.inMemory(process.cwd());
  if (read) manager.appendMessage({ role: "toolResult", toolCallId: "read", toolName: "bash",
    content: [{ type: "text", text: "fedc9876" }], isError: false, timestamp: 1 });
  return manager;
};
const invocation = (manager: SessionManager): FabricInvocationContext => ({
  cwd: process.cwd(), signal: undefined, parentToolCallId: "outer", nestedToolCallId: "nested", update() {},
  extensionContext: { sessionManager: manager } as unknown as ExtensionContext,
});
type Ports = ConstructorParameters<typeof AgentsProvider>;
const mainProvider = (main: MainAgentController, hosted: boolean) => hosted
  ? createAgentsProvider(createAgentServiceClient(async (action, args) => main.deliverAgent({
    from: identity, message: args.message as string, delivery: action as "steer" | "followUp",
  }), { steer: true, followUp: true }))
  : new AgentsProvider({} as Ports[0], { identity } as Ports[1], {} as Ports[2], main,
    { get: () => undefined } as unknown as Ports[4], undefined, {} as Ports[6]);
const busyMain = (journal?: string) => {
  const sent: string[] = [];
  const handlers = new Map<string, Array<(event: unknown, ctx: ExtensionContext) => void>>();
  const pi = { on: (name: string, fn: (event: unknown, ctx: ExtensionContext) => void) => {
    handlers.set(name, [...(handlers.get(name) ?? []), fn]);
    return () => handlers.set(name, (handlers.get(name) ?? []).filter(handler => handler !== fn));
  }, sendMessage: (message: { content: string }) => sent.push(message.content),
    getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
  const main = new MainAgentController(pi, "session:main", true, process.cwd(), "main");
  const context = { isIdle: () => false, hasPendingMessages: () => false,
    signal: { aborted: false } } as unknown as ExtensionContext;
  main.attachFollowUpDrain(context, 120_000, journal);
  const emit = (name: string, event: unknown) => {
    for (const handler of handlers.get(name) ?? []) handler(event, context);
  };
  return { main, sent, emit };
};

// Round-1 F1: the original text fits the real admission limit, but the marker does not.
describe("round-1 admission invariance", () => {
  it("keeps a near-limit serialized mesh event deliverable and preserves the genuine cap", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "id-mesh-admission-"));
    try {
      const store = new MeshStore(root, 262_144, 100);
      const provider = new MeshProvider(store, identity, {} as ConstructorParameters<typeof MeshProvider>[2]);
      const seed = await store.publish({ topic: "team", from: identity, text: `${text}\n\"é` });
      const message = seed.text! + "x".repeat(262_128 - Buffer.byteLength(JSON.stringify(seed)));
      const before = await provider.invoke("publish", { topic: "team", text: message }, invocation(session(true)));
      expect(before).not.toHaveProperty("notice");
      expect(Buffer.byteLength(JSON.stringify(before))).toBe(262_128);
      const after = await provider.invoke("publish", { topic: "team", text: message }, invocation(session()));
      expect(after).toMatchObject({ text: message });
      expect(after).not.toHaveProperty("notice");
      expect(store.read()).toHaveLength(3); // Exactly one persisted event per successful publish.
      expect(store.read().at(-1)?.text).toBe(message);
      await expect(provider.invoke("publish", { topic: "team", text: message + "x".repeat(17) }, invocation(session())))
        .rejects.toThrow("Mesh event exceeds 262144 bytes");
      expect(store.read()).toHaveLength(3);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });

  it.each(["steer", "followUp"] as const)("keeps a near-limit actor %s payload deliverable", async action => {
    const maxEventBytes = 64 * 1024;
    const validate = ActorManager.prototype.validateDirectMessage.bind({ mesh: { maxEventBytes } } as ActorManager);
    const data = { untouched: "é\"" };
    const maxPayloadBytes = maxEventBytes - ACTOR_MESSAGE_ENVELOPE_BYTES;
    const message = `${text} ` + "x".repeat(maxPayloadBytes - 16 - Buffer.byteLength(JSON.stringify({ message: `${text} `, data })));
    expect(() => validate(message, data)).not.toThrow();
    expect(() => validate(`${message}\n\n${notice}`, data)).toThrow("Actor message exceeds");
    const delivered: string[] = [];
    const provider = new AgentsProvider({ status: () => { throw new Error("Unknown Fabric agent"); } } as unknown as Ports[0], {
      identity, validateDirectMessage: validate, status: () => ({ id: "actor", name: "Actor", runner: "pi" }),
      tell: (_id: string, message: string, input: unknown) => {
        validate(message, input); delivered.push(message); return { queued: true, messageId: "actor-ack" };
      },
    } as unknown as Ports[1], {} as Ports[2], { matches: () => false } as unknown as Ports[3],
    { get: () => undefined } as unknown as Ports[4], undefined, {} as Ports[6]);
    expect(await provider.invoke(action, { id: "actor", message, data }, invocation(session(true)))).not.toHaveProperty("notice");
    expect(await provider.invoke(action, { id: "actor", message, data }, invocation(session()))).not.toHaveProperty("notice");
    expect(delivered).toEqual([message, message]);
    await expect(provider.invoke(action, { id: "actor", message: message + "x".repeat(17), data }, invocation(session())))
      .rejects.toThrow("Actor message exceeds");
    expect(delivered).toHaveLength(2);
  });

  it.each([
    [false, "sender"], [true, "sender"], [false, "total"], [true, "total"],
  ] as const)("keeps the busy-Main %s / %s followUp byte quota unchanged", async (hosted, quota) => {
    const message = quota === "sender"
      ? `${text} ` + "x".repeat(FOLLOW_UP_LIMITS.senderBytes - 16 - Buffer.byteLength(`${text} `))
      : text;
    const before = busyMain();
    const after = busyMain();
    try {
      let seeded = 0;
      if (quota === "total") {
        let remaining = FOLLOW_UP_LIMITS.totalBytes - Buffer.byteLength(message) - 16;
        while (remaining > 0) {
          const bytes = Math.min(remaining, FOLLOW_UP_LIMITS.senderBytes);
          const request = { from: { ...identity, id: `seed-${seeded}` }, message: "x".repeat(bytes), delivery: "followUp" as const };
          before.main.deliverAgent(request);
          after.main.deliverAgent(request);
          remaining -= bytes;
          seeded++;
        }
      }
      const base = mainProvider(before.main, hosted);
      const checked = mainProvider(after.main, hosted);
      expect(await base.invoke("followUp", { id: "main", message }, invocation(session(true)))).not.toHaveProperty("notice");
      const result = await checked.invoke("followUp", { id: "main", message }, invocation(session()));
      expect(result).not.toHaveProperty("notice");
      expect(before.main.queueDepth().pendingFollowUps).toBe(seeded + 1);
      expect(after.main.queueDepth().pendingFollowUps).toBe(seeded + 1);
      after.main.closeFollowUpDrain(); // Inspect the text actually handed to Pi, not just a receipt.
      expect(after.sent.join("\n")).toContain(message);
      expect(after.sent.join("\n")).not.toContain(notice);
      await expect(base.invoke("followUp", { id: "main", message: "x".repeat(17) }, invocation(session())))
        .rejects.toThrow("Main's followUp queue is full");
      expect(before.main.queueDepth().pendingFollowUps).toBe(seeded + 1);
    } finally { before.main.closeFollowUpDrain(); after.main.closeFollowUpDrain(); }
  });

  it("sends original text once to remote Main and preserves its definitive quota rejection", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "id-remote-admission-"));
    const before = busyMain();
    // Remote control commands require durable delivery ids, hence a real Main journal.
    const journal = path.join(root, "main-followups.json");
    const after = busyMain(journal);
    const senderStore = new MeshStore(root, 64 * 1024, 100);
    const ownerIdentity: MeshIdentity = { id: after.main.id, name: "Main", kind: "main" };
    const sender = new FabricControlPlane(senderStore, identity, {
      enabled: true, hostId: "sender-host", pollMs: 10, acknowledgementTimeoutMs: 1_000,
    });
    const owner = new FabricControlPlane(new MeshStore(root, 64 * 1024, 100), ownerIdentity, {
      enabled: true, hostId: "owner-host", pollMs: 10, acknowledgementTimeoutMs: 1_000,
    });
    try {
      // Stable control IDs and the two event inbox receipts are SHA-256 hex (64 chars).
      // smarty-dev#13383 preserves/charges serialized inboxReceipts too: reserve their
      // real journal cost so only the optional marker exceeds the unchanged quota.
      const deliveryIdBytes = Buffer.byteLength(JSON.stringify("0".repeat(64)));
      const inboxReceiptBytes = Buffer.byteLength(JSON.stringify(["0".repeat(64), "0".repeat(64)]));
      const seed = { from: identity, message: "x".repeat(FOLLOW_UP_LIMITS.senderBytes - Buffer.byteLength(text) - deliveryIdBytes - inboxReceiptBytes - 16), delivery: "followUp" as const };
      before.main.deliverAgent(seed);
      after.main.deliverAgent(seed);
      expect(before.main.deliverAgent({ from: identity, message: text, delivery: "followUp" })).toMatchObject({ queued: true });
      const router = new AgentMessageRouter({} as never, { identity: ownerIdentity } as never,
        after.main, { get: () => undefined } as never, undefined, binding => binding);
      sender.start(() => ({ accepted: false }));
      owner.start((command, from, signal) => router.acceptControl(command, from, signal));
      const participant = { id: after.main.id, kind: "root", local: false, capabilities: ["followUp"],
        ownerHostId: "owner-host", ownerIdentityId: ownerIdentity.id, ownerIncarnation: owner.incarnation, controlProtocol: "v1" };
      const provider = new AgentsProvider({} as Ports[0], { identity } as Ports[1], {} as Ports[2],
        { matches: () => false } as unknown as Ports[3], { get: () => participant } as unknown as Ports[4], sender, {} as Ports[6]);
      expect(await provider.invoke("followUp", { id: after.main.id, message: text }, invocation(session())))
        .toMatchObject({ queued: true, acknowledged: true });
      expect(after.main.queueDepth().pendingFollowUps).toBe(2); // Seed plus one report, not two reports.
      const commands = senderStore.read({ topic: "fabric.control.command" });
      expect(commands).toHaveLength(1);
      expect(commands.every(event => /^[a-f0-9]{64}$/.test((event.data as { commandId: string }).commandId))).toBe(true);
      expect(commands.map(event => (event.data as { message: string }).message)).toEqual([text]);
      const acknowledgements = () => senderStore.read({ topic: "fabric.control.ack" })
        .map(event => event.data as { commandId: string; accepted: boolean; error?: string; notRun?: boolean });
      expect(acknowledgements()).toMatchObject([
        { commandId: (commands[0]!.data as { commandId: string }).commandId, accepted: true },
      ]);
      const items = JSON.parse(fs.readFileSync(journal, "utf8")).items as Array<{
        message: string; deliveryId?: string; inboxReceipts?: string[];
      }>;
      expect(items).toHaveLength(2);
      expect(items[1]).toMatchObject({ message: text, inboxReceipts: eventReceipts(commands[0]!) });
      expect(Buffer.byteLength(JSON.stringify(items[1]!.inboxReceipts))).toBe(inboxReceiptBytes);
      expect(Buffer.byteLength(JSON.stringify(items[1]!.deliveryId))).toBe(deliveryIdBytes);

      // Only 16 bytes remain: another report genuinely cannot fit; do not retry it.
      await expect(provider.invoke("followUp", { id: after.main.id, message: text }, invocation(session())))
        .rejects.toThrow(/^Main's followUp queue is full \([^\n]*\); Main is busy and reads followUps only at its next tool boundary\. Wait, or send a short steer\.$/);
      expect(after.main.queueDepth().pendingFollowUps).toBe(2);
      expect(senderStore.read({ topic: "fabric.control.command" })
        .map(event => (event.data as { message: string }).message)).toEqual([text, text]);
      expect(acknowledgements().map(ack => ack.accepted)).toEqual([true, false]);
      expect(acknowledgements().filter(ack => !ack.accepted).every(ack =>
        ack.error?.startsWith("Main's followUp queue is full") && !ack.error.includes("not delivered, safe to resend") && ack.notRun !== true)).toBe(true);
      expect(JSON.parse(fs.readFileSync(journal, "utf8")).items).toEqual(items);
      after.emit("agent_before_settle", { outcome: "completed", context: { pendingMessages: [] } });
      after.main.closeFollowUpDrain();
      expect(after.sent.join("\n")).toContain(text);
      expect(after.sent.join("\n")).not.toContain(notice);
      expect(after.sent.join("\n").split(text)).toHaveLength(2); // Exactly one report handed to Pi.
    } finally {
      await Promise.all([sender.close(), owner.close()]);
      before.main.closeFollowUpDrain();
      after.main.closeFollowUpDrain();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  it.each([false, true])("does not retry delivery failures or unknown outcomes (hosted: %s)", async hosted => {
    const send = vi.fn(() => { throw new Error("delivery failed after handoff; outcome unknown"); });
    const provider = hosted ? createAgentsProvider(createAgentServiceClient(send, { followUp: true }))
      : new AgentsProvider({} as Ports[0], { identity } as Ports[1], {} as Ports[2], {
        id: "main", local: true, matches: () => true, deliverAgent: send,
      } as unknown as Ports[3], { get: () => undefined } as unknown as Ports[4], undefined, {} as Ports[6]);
    await expect(provider.invoke("followUp", { id: "main", message: text }, invocation(session()))).rejects.toThrow("outcome unknown");
    expect(send).toHaveBeenCalledOnce();
  });
});

// Round-1 F2: native inbox messages carry authorship in event text, not details.from.
describe("round-1 native root inbox read provenance", () => {
  it.each([false, true])("keeps native inbox issuer verification and original self/peer text (mixed: %s)", async mixed => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "id-inbox-self-"));
    try {
      const manager = session();
      const own: MeshIdentity = { ...identity, id: `session:${manager.getSessionId()}` };
      const store = new MeshStore(root, 64 * 1024, 100);
      // An immediate addressed publish needs a known target (smarty-dev#5518); the self-publish targets own.id.
      const participants = { get: (id: string) => id === own.id ? { id } : undefined, list: () => [] };
      const provider = new MeshProvider(store, own, participants as unknown as ConstructorParameters<typeof MeshProvider>[2]);
      const inbox = new RootInbox(store, own, () => [own.id], { steerGraceMs: 0 });
      inbox.start();
      expect(await provider.invoke("publish", { topic: "fleet.report", to: own.id, text }, invocation(manager)))
        .not.toHaveProperty("notice");
      if (mixed) await store.publish({ topic: "fleet.report", from: { id: "session:peer", name: "Peer", kind: "main" },
        to: own.id, text: "head abc1234" });
      const batch = await inbox.next(rootInboxSession(manager.getEntries()));
      expect(batch.events).toHaveLength(mixed ? 2 : 1);
      expect(batch.events.map(event => event.text)).toEqual(mixed ? [text, "head abc1234"] : [text]);
      const message = rootInboxMessage(batch.events);
      expect(message.details).toEqual({
        nonce: expect.any(String),
        ids: batch.events.map(event => event.id),
        receipts: batch.events.flatMap(event => [
          ["", "event", event.id],
          [event.from.id, "id", event.id],
        ].map(fields => createHash("sha256").update(JSON.stringify(fields)).digest("hex"))),
      });
      manager.appendCustomMessageEntry(message.customType, message.content, message.display, message.details);
      // The same public carrier cannot settle work without its private issuer verifier.
      expect((await inbox.next(unsignedRootInboxSession(manager.getEntries()))).events).toEqual(batch.events);
      expect((await inbox.next(rootInboxSession(manager.getEntries()))).events).toEqual([]);
      const result = await provider.invoke("publish", { topic: "team", text: mixed ? `${text}; head abc1234` : text }, invocation(manager));
      expect(result).not.toHaveProperty("notice");
      expect(store.read({ topic: "team" }).map(event => event.text)).toEqual([mixed ? `${text}; head abc1234` : text]);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
});
