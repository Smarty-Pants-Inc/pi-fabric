import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { RootInbox, rootInboxMessage, rootInboxSession } from "../src/topology/root-inbox.js";

const roots: string[] = [];
const closers: Array<() => void | Promise<void>> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const outcomes = (root: string) => {
  const directory = path.join(root, "delivery-outcomes");
  return fs.existsSync(directory) ? fs.readdirSync(directory).flatMap(file =>
    fs.readFileSync(path.join(directory, file), "utf8").trim().split("\n").map(line => JSON.parse(line))) : [];
};

it.each(["Unknown Fabric participant: missing", "Main's followUp queue is full"])("the local sender records a definite refusal (%s)", async reason => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-local-refusal-")); roots.push(root);
  const from = identity("session:sender");
  const router = new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); } } as never,
    { identity: from, mesh: { root }, status: () => { throw new Error("Unknown Fabric actor"); }, validateDirectMessage: () => {} } as never,
    { id: "session:main", local: true, matches: (id: string) => id === "main", deliverAgent: () => { throw new Error(reason); } } as never,
    { get: () => undefined, lastKnown: () => undefined } as never, undefined, binding => binding,
  );
  const target = reason.startsWith("Unknown") ? "session:missing" : "main";
  await expect(router.routeMessage(target, "hello", undefined, "followUp", undefined, { idempotencyKey: "send" })).rejects.toThrow(reason.startsWith("Unknown") ? "Unknown Fabric participant" : reason);
  expect(outcomes(root)).toEqual([{ eventId: "send", to: target, from: from.id, mode: "followUp", outcome: "failed", reason: expect.stringContaining(reason.startsWith("Unknown") ? "Unknown Fabric participant" : reason), at: expect.any(Number) }]);
});

it("a sender timeout can be followed by receiver-delivered evidence for the same wire event", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-late-delivery-")); roots.push(root);
  const entries: unknown[] = []; const sent: Array<{ details: unknown }> = [];
  const pi = { on: () => () => {}, sendMessage: (message: { details: unknown }) => sent.push(message) } as unknown as ExtensionAPI;
  const context = { isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false },
    sessionManager: { getEntries: () => entries } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, "session:receiver", true, root, "receiver", true, undefined, root);
  main.attachFollowUpDrain(context, 0, path.join(root, "main-followups", "receiver.json"));
  closers.push(() => main.closeFollowUpDrain());
  const make = (id: string) => {
    const value = new FabricControlPlane(new MeshStore(root, 64 * 1024, 100), identity(id),
      { enabled: true, hostId: id, pollMs: 5, acknowledgementTimeoutMs: 200 });
    closers.push(() => value.close()); return value;
  };
  const sender = make("host:sender"), owner = make("host:owner");
  let acknowledge!: () => void;
  const held = new Promise<void>(resolve => { acknowledge = resolve; });
  owner.start(async (command, from) => {
    const result = main.deliverAgent({ from, delivery: "steer", message: command.message!, deliveryId: command.commandId,
      outcomeSend: { eventId: command.eventId!, to: main.id, from: from.id, mode: "steer" } });
    await held;
    return { accepted: true, messageId: result.messageId };
  });
  sender.start(() => ({ accepted: false }));
  try {
    await expect(sender.request("host:owner", main.id, "steer", { message: "late receipt" })).rejects.toThrow("outcome is unknown");
    expect(sent).toHaveLength(1);
    expect(outcomes(root).map(row => row.outcome)).toEqual(["unknown"]);
    entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: sent[0]!.details });
    main.confirmInbox();
    expect(outcomes(root).map(row => row.outcome)).toEqual(["unknown", "delivered"]);
    expect(outcomes(root)[1].eventId).toBe(outcomes(root)[0].eventId);
    expect(outcomes(root)[1]).toMatchObject({ to: main.id, from: "host:sender", mode: "steer" });
  } finally { acknowledge(); }
});

it("a targeted Main publish is delivered only when the session holds its deduplicated inbox batch", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-publish-inbox-")); roots.push(root);
  const mesh = new MeshStore(root, 64 * 1024, 100);
  const me = identity("session:main");
  const box = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
  closers.push(() => box.close()); box.start();
  const event = await mesh.publish({ topic: "fleet.work", to: me.id, from: identity("session:sender"), text: "work" });
  await vi.waitFor(async () => expect((await box.next(rootInboxSession([]))).events).toHaveLength(1));
  expect(outcomes(root)).toEqual([]);
  const message = rootInboxMessage([event, event]);
  expect(message.details.ids).toEqual([event.id]);
  expect(message.content.match(/<event /g)).toHaveLength(1);
  await box.next(rootInboxSession([{ type: "custom_message", ...message }]));
  await box.next(rootInboxSession([{ type: "custom_message", ...message }]));
  expect(outcomes(root)).toEqual([{ eventId: event.id, to: me.id, from: "session:sender", mode: "publish", outcome: "delivered",
    reason: "consumed by Main inbox as itself", at: expect.any(Number) }]);
});
