import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, expect, it, vi } from "vitest";
import { AgentMessageRouter, FabricParticipantNonInteractiveError, FabricParticipantNotYetMirroredError, FabricRouteAuthorityError } from "../src/providers/agents-message-router.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlIncarnationRequiredError, FabricControlPlane } from "../src/topology/control-plane.js";
import { RootInbox } from "../src/topology/root-inbox.js";
import { rootInboxMessage, rootInboxSession } from "./root-inbox-issued-fixture.js";

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

// Keep reason: refusal/error honesty and durable receiver admission are data-safety boundaries.
it.each(["Unknown Fabric participant: missing", "Main's followUp queue is full"])("the local sender preserves a definite refusal without a sender log (%s)", async reason => {
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
  expect(outcomes(root)).toEqual([]);
  expect(fs.existsSync(path.join(root, "delivery-outcomes"))).toBe(false);
});

it.each(([
  ["FABRIC_ROUTE_AUTHORITY_CHANGED", () => new FabricRouteAuthorityError("main")],
  ["FABRIC_CONTROL_INCARNATION_REQUIRED", () => new FabricControlIncarnationRequiredError("main")],
  ["FABRIC_PARTICIPANT_NOT_YET_MIRRORED", () => new FabricParticipantNotYetMirroredError("main", "host:peer")],
  ["FABRIC_PARTICIPANT_NON_INTERACTIVE", () => new FabricParticipantNonInteractiveError("main")],
  [undefined, () => new Error("Fabric mesh bridge routing to remote host peer is unavailable for main; the routed owner could not be revalidated; this attempt was not published.")],
] as const).flatMap(([code, refusal]) => [false, true].map(sinkUnavailable => ({ code, refusal, sinkUnavailable }))))(
  "the local sender preserves routing refusal $code without a sender log (unavailable outcome sink=$sinkUnavailable)", async ({ code, refusal, sinkUnavailable }) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-prepublication-refusal-")); roots.push(root);
  const sink = path.join(root, "delivery-outcomes");
  // A real path collision makes mkdir/append fail; no logger mocks hide the failure mode.
  if (sinkUnavailable) fs.writeFileSync(sink, "unavailable sink");
  const from = identity("session:sender");
  const error = refusal();
  const router = new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); } } as never,
    { identity: from, mesh: { root }, status: () => { throw new Error("Unknown Fabric actor"); }, validateDirectMessage: () => {} } as never,
    { id: "session:main", local: true, matches: (id: string) => id === "main", deliverAgent: () => { throw error; } } as never,
    { get: () => undefined, lastKnown: () => undefined } as never, undefined, binding => binding,
  );
  await expect(router.routeMessage("main", "hello", undefined, "steer", undefined, { idempotencyKey: "refused" })).rejects.toBe(error);
  if (code) expect(error).toMatchObject({ code });
  if (sinkUnavailable) expect(fs.readFileSync(sink, "utf8")).toBe("unavailable sink");
  else {
    expect(outcomes(root)).toEqual([]);
    expect(fs.existsSync(sink)).toBe(false);
  }
});

it.each([false, true])("a real routed Main payload is admitted once and retains receiver evidence (sender timeout=%s)", async timeout => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-late-delivery-")); roots.push(root);
  const entries: unknown[] = []; const sent: Array<{ details: unknown }> = [];
  const pi = { on: () => () => {}, sendMessage: (message: { details: unknown }) => sent.push(message) } as unknown as ExtensionAPI;
  const context = { isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false },
    sessionManager: { getEntries: () => entries } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, "session:receiver", true, root, "receiver", true, undefined, {}, undefined, root);
  main.attachFollowUpDrain(context, 0, path.join(root, "main-followups", "receiver.json"));
  closers.push(() => main.closeFollowUpDrain());
  const make = (id: string) => {
    const value = new FabricControlPlane(new MeshStore(root, 64 * 1024, 100), identity(id),
      { enabled: true, hostId: id, pollMs: 5, acknowledgementTimeoutMs: 200 });
    closers.push(() => value.close()); return value;
  };
  const sender = make("host:sender"), owner = make("host:owner");
  const participant = { id: main.id, kind: "root", rootId: main.id, local: false,
    ownerHostId: "host:owner", ownerIdentityId: "host:owner", ownerIncarnation: owner.incarnation,
    controlProtocol: "v1", capabilities: ["steer"] };
  const router = (from: MeshIdentity, target: ConstructorParameters<typeof AgentMessageRouter>[2], control?: FabricControlPlane) => new AgentMessageRouter(
    { status: () => { throw new Error("Unknown Fabric agent"); } } as never,
    { identity: from, mesh: { root } } as never, target,
    { get: (id: string) => id === main.id ? participant : undefined, lastKnown: () => undefined } as never,
    control, binding => binding,
  );
  const sending = router(identity("host:sender"), { id: "session:sender", local: true, matches: () => false } as never, sender);
  const receiving = router(identity("host:owner"), main);
  let acknowledge!: () => void;
  const held = new Promise<void>(resolve => { acknowledge = resolve; });
  // Runtime bootstrap uses this same verified control -> router -> Main admission path.
  owner.start(async (command, from, signal, verification) => {
    const result = await receiving.acceptControl(command, from, signal, verification);
    if (timeout) await held;
    return result;
  });
  sender.start(() => ({ accepted: false }));
  try {
    const request = sending.routeMessage(main.id, "late receipt", { sender: "forged" }, "steer", undefined, { idempotencyKey: "one-payload" });
    if (timeout) await expect(request).rejects.toThrow("outcome is unknown");
    else await expect(request).resolves.toMatchObject({ queued: true, acknowledged: true });
    expect(sent).toHaveLength(1);
    const journal = JSON.parse(fs.readFileSync(path.join(root, "main-followups", "receiver.json"), "utf8"));
    expect(journal.items).toHaveLength(1);
    const admitted = journal.items[0];
    expect(admitted).toMatchObject({ message: "late receipt", data: { sender: "forged" }, from: { id: "host:sender" },
      provenance: { sender: { id: "host:sender", verified: "mesh" } },
      outcomeSend: { to: main.id, from: "host:sender", mode: "steer" } });
    expect(outcomes(root).map(row => row.outcome)).toEqual(timeout ? ["unknown"] : []);
    entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: sent[0]!.details });
    main.confirmInbox();
    main.confirmInbox();
    expect(sent).toHaveLength(1);
    expect(outcomes(root).map(row => row.outcome)).toEqual(timeout ? ["unknown", "delivered"] : ["delivered"]);
    expect(outcomes(root).every(row => row.eventId === admitted.outcomeSend.eventId)).toBe(true);
    expect(outcomes(root).at(-1)).toMatchObject({ to: main.id, from: "host:sender", mode: "steer" });
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

it("a targeted Main publish consumed through the normal inbox ack path records exactly one delivered outcome", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-publish-inbox-ack-")); roots.push(root);
  const mesh = new MeshStore(root, 64 * 1024, 100);
  const me = identity("session:main");
  const box = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
  closers.push(() => box.close()); box.start();
  const event = await mesh.publish({ topic: "fleet.work", to: me.id, from: identity("session:sender"), text: "work" });
  await vi.waitFor(async () => expect((await box.next(rootInboxSession([]))).events).toHaveLength(1));
  expect(outcomes(root)).toEqual([]);
  const message = rootInboxMessage([event]);
  // extension.ts message_end: the inbox carrier entered a turn, so consumeRootInbox -> consume -> ack.
  await box.consume(message.details.ids, message.details.receipts);
  expect((mesh.get(box.key)?.value as { pending?: unknown }).pending).toBeUndefined();
  const delivered = [{ eventId: event.id, to: me.id, from: "session:sender", mode: "publish", outcome: "delivered",
    reason: "consumed by Main inbox as itself", at: expect.any(Number) }];
  expect(outcomes(root)).toEqual(delivered);
  // A repeated ack and later reconciles, with or without the carrier, never record it again.
  await box.consume(message.details.ids, message.details.receipts);
  expect((await box.next(rootInboxSession([{ type: "custom_message", ...message }]))).events).toEqual([]);
  expect((await box.next(rootInboxSession([]))).events).toEqual([]);
  expect(outcomes(root)).toEqual(delivered);
});

it("a partial inbox ack then next() records exactly one delivered outcome per event", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-publish-inbox-partial-")); roots.push(root);
  const mesh = new MeshStore(root, 64 * 1024, 100);
  const me = identity("session:main");
  const box = new RootInbox(mesh, me, () => [me.id], { steerGraceMs: 0 });
  closers.push(() => box.close()); box.start();
  const first = await mesh.publish({ topic: "fleet.work", to: me.id, from: identity("session:one"), text: "one" });
  const second = await mesh.publish({ topic: "fleet.work", to: me.id, from: identity("session:two"), text: "two" });
  await vi.waitFor(async () => expect((await box.next(rootInboxSession([]))).events).toHaveLength(2));
  await box.consume([first.id]);
  expect((mesh.get(box.key)?.value as { pending?: { ids: string[] } }).pending?.ids).toEqual([first.id, second.id]);
  expect(outcomes(root).map(row => [row.eventId, row.from, row.outcome])).toEqual([[first.id, "session:one", "delivered"]]);
  const message = rootInboxMessage([first, second]);
  expect((await box.next(rootInboxSession([{ type: "custom_message", ...message }]))).events).toEqual([]);
  await box.consume(message.details.ids, message.details.receipts);
  await box.next(rootInboxSession([{ type: "custom_message", ...message }]));
  expect(outcomes(root).map(row => [row.eventId, row.from, row.mode, row.outcome])).toEqual([
    [first.id, "session:one", "publish", "delivered"], [second.id, "session:two", "publish", "delivered"],
  ]);
});
