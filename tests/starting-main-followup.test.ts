// Keep: smarty-dev#13108/#12486, inbox data safety and the authenticated-envelope trust boundary.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID, createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager, type ExtensionAPI, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage } from "@earendil-works/pi-ai";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MainAgentController } from "../src/main-agent.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { claimStartingHost, startingHost, assertStartupOwner } from "../src/topology/starting-host.js";
import { readHostLeaseCurrent, renewHostLease, writeHostLease, FabricHostLeaseSupersededError,
  FabricParticipantStaleError, type FabricHostLease } from "../src/topology/host-leases.js";
import { registerFabricPrincipalCapture } from "../src/fabric-provenance.js";
import type { FabricInvocationContext } from "../src/protocol.js";

const roots: string[] = [], planes: FabricControlPlane[] = [], mains: MainAgentController[] = [];
afterEach(async () => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const p of planes.splice(0)) await p.close();
  for (const m of mains.splice(0)) m.prepareReload();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const temp = () => { const root = fs.mkdtempSync(path.join(os.tmpdir(), "starting-main-")); roots.push(root); return root; };
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const route = (identity: MeshIdentity, main: Ports[2], control?: Ports[4], source?: Ports[3]) => new AgentMessageRouter(
  { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
  { identity, owns: () => false, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); } } as unknown as Ports[1],
  main, source ?? { get: () => undefined, lastKnown: () => undefined, scheduleRefresh: () => {} }, control, b => b);
const rootMain = (id: string) => ({ id, local: true, interactive: true, matches: (target: string) => target === id }) as Ports[2];
const plane = (mesh: MeshStore, identity: MeshIdentity, incarnation?: string) => {
  const p = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id,
    canConsumeMesh: () => true, ...(incarnation ? { ownerIncarnation: incarnation } : {}) }); planes.push(p); return p;
};
const claim = async (root: string, sessionId: string) => (await claimStartingHost({
  meshRoot: root, cwd: root, id: `session:${sessionId}`, sessionId, lockProtocol: 1, stateBackend: "file",
}))!;

describe("same-session native Main startup inbox", () => {
  it("queues at 31s, survives sender close and closed admission window, and dedups the journal/outcome gap", async () => {
    const root = temp(), mesh = new MeshStore(root, 65_536, 1_000);
    const manager = SessionManager.create(root, path.join(root, "sessions"));
    const sessionId = manager.getSessionId(), targetId = `session:${sessionId}`;
    const senderIdentity: MeshIdentity = { id: "session:sender", name: "Sender", kind: "main" };
    const receiverIdentity: MeshIdentity = { id: targetId, name: "Receiver", kind: "main" };
    // No predecessor file: a clean shutdown must not prevent early startup admission.
    const receipt = await claim(root, sessionId);
    expect(receipt.lease.incarnationToken).toBeTruthy();
    const old = plane(new MeshStore(root, 65_536, 1_000), receiverIdentity, "old-control-epoch");
    const oldHandler = vi.fn(() => ({ accepted: true })); old.start(oldHandler);
    const sender = plane(mesh, senderIdentity);
    const r = route(senderIdentity, rootMain(senderIdentity.id), sender);
    // Capture a host-authenticated principal; nested message data must never replace it.
    const hooks = new Map<string, Function>();
    registerFabricPrincipalCapture({ on: (name: string, fn: Function) => hooks.set(name, fn) } as unknown as ExtensionAPI);
    const callerContext = { sessionManager: {} };
    hooks.get("message_start")!({ message: { role: "user", provenance: { v: 1, channel: "keyboard", turnId: "turn",
      receivedAt: new Date().toISOString(), principal: { id: "trusted-human", binding: "herdr-client" } } } }, callerContext);
    const invocation = { cwd: root, extensionContext: callerContext } as FabricInvocationContext;
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(Date.now() + 31_000); // Receiver runtime import has not happened.
    const queued = await r.routeMessage(targetId, "one native delivery", { principal: { id: "forged" } }, "followUp", invocation,
      { idempotencyKey: "stable-request" });
    const duplicate = await r.routeMessage(targetId, "one native delivery", { principal: { id: "forged" } }, "followUp", invocation,
      { idempotencyKey: "stable-request" });
    expect(queued).toMatchObject({ queued: true, reason: "startup-inbox-durable", triggered: false });
    expect(queued).not.toHaveProperty("acknowledged");
    expect(duplicate.messageId).toBe(queued.messageId);
    expect(mesh.listAll("topology/starting-followups/")).toHaveLength(1);
    await sender.close();
    vi.setSystemTime(receipt.lease.startup!.until + 1_000);
    expect(startingHost(root, targetId)).toBeUndefined(); // New admission is closed.
    expect(() => assertStartupOwner(root, receipt.lease)).not.toThrow(); // Adoption is not new admission.
    const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: targetId, rootId: targetId,
      identity: receiverIdentity, startingHost: receipt });
    expect(directory.ownerIncarnation).toBe(receipt.lease.ownerIncarnation);
    const native: Array<{ message: any; options: any }> = [];
    const api = { on: () => {}, sendMessage: (message: any, options: any) => {
      native.push({ message, options });
      manager.appendCustomMessageEntry(message.customType, message.content, true, message.details);
      manager.appendMessage(fauxAssistantMessage("persist receipt"));
    } } as unknown as ExtensionAPI;
    const main = new MainAgentController(api, targetId, true, root, sessionId); mains.push(main);
    main.attachFollowUpDrain({ sessionManager: manager, isIdle: () => true, hasPendingMessages: () => false } as unknown as ExtensionContext,
      60_000, path.join(root, "main-followups", `${sessionId}.json`));
    const targetRouter = route(receiverIdentity, main);
    const receiver = plane(new MeshStore(root, 65_536, 1_000), receiverIdentity, directory.ownerIncarnation);
    // Simulate a crash gap after Main persisted its delivery receipt but before the
    // startup queue outcome commits. Replay must call the real Main receipt dedup.
    const write = receiver.mesh.writeBatch.bind(receiver.mesh);
    let gap = true;
    vi.spyOn(receiver.mesh, "writeBatch").mockImplementation(input => write({ ...input, prepare: (view, fileRead) => {
      const ops = input.prepare ? input.prepare(view, fileRead) : input.ops;
      if (gap && ops.some(op => op.kind === "put" && op.key.startsWith("topology/starting-followups/"))) {
        gap = false; throw Object.assign(new Error("injected journal/outcome gap"), { code: "FABRIC_MESH_LOCK_TIMEOUT" });
      }
      return ops;
    } }));
    receiver.start((...args) => targetRouter.acceptControl(...args));
    await vi.waitFor(() => expect((mesh.listAll("topology/starting-followups/", { fresh: true })[0]!.value as any).acceptance?.accepted).toBe(true));
    expect(gap).toBe(false);
    expect(native).toHaveLength(1);
    expect(native[0]!.options.triggerTurn).toBe(true);
    const envelope = (mesh.listAll("topology/starting-followups/", { fresh: true })[0]!.value as any).event;
    expect(envelope.principal).toEqual({ id: "trusted-human", binding: "herdr-client" });
    expect(envelope.from.id).toBe(senderIdentity.id);
    expect(oldHandler).not.toHaveBeenCalled();
    expect(mesh.read({ topic: "fabric.control.ack", limit: 100 })).toEqual([]);
    await directory.close();
  });

  it("wakes event-driven consumption when the first event drains before the durable row commits", async () => {
    const root = temp(), sessionId = randomUUID(), id = `session:${sessionId}`;
    const receipt = await claim(root, sessionId);
    const senderIdentity: MeshIdentity = { id: "session:sender", name: "sender", kind: "main" };
    const receiverIdentity: MeshIdentity = { id, name: "receiver", kind: "main" };
    const sender = plane(new MeshStore(root, 65_536, 1_000), senderIdentity);
    const receiver = plane(new MeshStore(root, 65_536, 1_000), receiverIdentity, receipt.lease.ownerIncarnation);
    let release!: () => void, entered!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const rowEntered = new Promise<void>(resolve => { entered = resolve; });
    const write = sender.mesh.writeBatch.bind(sender.mesh);
    vi.spyOn(sender.mesh, "writeBatch").mockImplementation(async input => { entered(); await held; return write(input); });
    const pending = route(senderIdentity, rootMain(senderIdentity.id), sender).routeMessage(id, "after initial drain", undefined,
      "followUp", undefined, { idempotencyKey: "row-race" });
    await rowEntered;
    let readCommand = false;
    const tail = receiver.mesh.tail.bind(receiver.mesh);
    vi.spyOn(receiver.mesh, "tail").mockImplementation((...args) => {
      const result = tail(...args);
      if (result.events.some(e => e.topic === "fabric.control.command")) readCommand = true;
      return result;
    });
    const accepted: string[] = [];
    receiver.start(command => { accepted.push(command.message!); return { accepted: true }; });
    await vi.waitFor(() => expect(readCommand).toBe(true));
    expect(accepted).toEqual([]);
    release(); await pending;
    const directory = new ParticipantDirectory(receiver.mesh, { enabled: true, hostId: id, rootId: id,
      identity: receiverIdentity, startingHost: receipt });
    await directory.finishStartup(); receiver.notifyStartupReady();
    await vi.waitFor(() => expect((receiver.mesh.listAll("topology/starting-followups/", { fresh: true })[0]!.value as any).acceptance?.accepted).toBe(true));
    expect(accepted).toEqual(["after initial drain"]);
    await directory.close();
  });

  it("fences the old lease forever and refuses replaced tokens, dead PID/start identity, and late admission", async () => {
    const root = temp(), mesh = new MeshStore(root, 65_536, 1_000), sessionId = randomUUID(), id = `session:${sessionId}`;
    const previous: FabricHostLease = { id, rootId: id, identityId: id, incarnationToken: "old-token", startedAt: 1,
      updatedAt: 1, expiresAt: 1, session: { id: sessionId, startedAt: 1, updatedAt: 1, expiresAt: 1 },
      writer: { pid: 2_147_483_647, host: os.hostname(), startedAt: 1, releaseSha: "old-release", lockProtocol: 1, stateBackend: "file" } };
    writeHostLease(root, previous);
    const receipt = await claim(root, sessionId);
    await expect(renewHostLease(mesh, { ...previous, updatedAt: Date.now(), expiresAt: Date.now() + 60_000 }))
      .rejects.toBeInstanceOf(FabricHostLeaseSupersededError);
    expect(readHostLeaseCurrent(root, id)?.incarnationToken).toBe(receipt.lease.incarnationToken);
    writeHostLease(root, { ...receipt.lease, incarnationToken: "replacement", ownerIncarnation: "replacement-control" });
    expect(() => assertStartupOwner(root, receipt.lease)).toThrow(FabricParticipantStaleError);
    writeHostLease(root, { ...receipt.lease, startup: { ...receipt.lease.startup!, pid: 2_147_483_647 } });
    expect(startingHost(root, id)).toBeUndefined();
    const senderIdentity: MeshIdentity = { id: "session:sender", name: "sender", kind: "main" };
    const sender = plane(mesh, senderIdentity);
    await expect(route(senderIdentity, rootMain(senderIdentity.id), sender).routeMessage(id,
      "dead startup must not use its future TTL", undefined, "followUp"))
      .rejects.toBeInstanceOf(FabricParticipantStaleError);
    writeHostLease(root, { ...receipt.lease, process: { ...receipt.lease.process!, host: "foreign-native-host" },
      startup: { ...receipt.lease.startup!, host: "foreign-native-host" } });
    await expect(sender.queueStartingFollowUp(id, { message: "remote path unchanged" }, "foreign"))
      .resolves.toBeUndefined();
    writeHostLease(root, receipt.lease);
    vi.useFakeTimers({ toFake: ["Date"] }); vi.setSystemTime(receipt.lease.startup!.until + 1);
    expect(startingHost(root, id)).toBeUndefined();
  });

  it("a truly dead nonstarting session still returns the exact FabricParticipantStaleError", async () => {
    const root = temp(), mesh = new MeshStore(root, 65_536, 1_000), sessionId = randomUUID(), id = `session:${sessionId}`;
    const target: MeshIdentity = { id, name: "dead", kind: "main" };
    const key = (prefix: string) => prefix + createHash("sha256").update(id).digest("hex");
    await mesh.put({ key: key("topology/hosts/"), identity: target, value: {
      format: 1, id, rootId: id, identity: target, startedAt: Date.now() - 30_000, updatedAt: Date.now() - 2_000, expiresAt: Date.now() - 2_000,
    } });
    await mesh.put({ key: key("topology/participants/"), identity: target, value: {
      format: 1, id, rootId: id, kind: "root", ownerHostId: id, ownerIdentityId: id, name: "dead", status: "idle",
      runner: "pi", controlProtocol: "v1", transport: "host", cwd: root, sessionId, capabilities: ["followUp", "steer"],
      startedAt: Date.now() - 30_000, updatedAt: Date.now() - 20_000,
    } });
    const senderIdentity: MeshIdentity = { id: "session:sender", name: "sender", kind: "main" };
    const source = new ParticipantDirectory(mesh, { enabled: true, hostId: senderIdentity.id, rootId: senderIdentity.id,
      identity: senderIdentity, routingLease: { waitMs: 0, lockWaiting: () => true } });
    await source.refresh();
    const sender = plane(mesh, senderIdentity);
    const r = route(senderIdentity, rootMain(senderIdentity.id), sender, source);
    await expect(r.routeMessage(id, "dead is not starting", undefined, "followUp"))
      .rejects.toBeInstanceOf(FabricParticipantStaleError);
    await source.close();
  });
});
