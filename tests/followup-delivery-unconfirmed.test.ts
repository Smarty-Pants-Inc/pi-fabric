import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane, type FabricControlCommand } from "../src/topology/control-plane.js";
import { FabricTargetTerminalError } from "../src/agents/terminal-target.js";
import { hasDeliveryOutcome } from "../src/mesh/delivery-outcomes.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshLockQueueExpiredError } from "../src/mesh/mesh-lock.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";

type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const stores: MeshStore[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  for (const store of stores.splice(0)) store.closeState();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const fixture = (targetId = "session:receiver") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-followup-outcome-")); roots.push(root);
  const from: MeshIdentity = { id: "session:sender", name: "sender", kind: "main" };
  const to: MeshIdentity = { id: "session:receiver", name: "receiver", kind: "main" };
  const store = () => { const mesh = new MeshStore(root, 64 * 1024, 1000); stores.push(mesh); return mesh; };
  const mesh = store();
  const plane = (identity: MeshIdentity, mesh: MeshStore) => {
    const value = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id,
      ownerIncarnation: `test:${identity.name}`, pollMs: 20, acknowledgementTimeoutMs: 120 });
    planes.push(value); return value;
  };
  const sender = plane(from, mesh), receiver = plane(to, store());
  sender.start(() => ({ accepted: true }));
  const participant = { ...to, id: targetId, kind: targetId === to.id ? "root" : "agent", local: false, rootId: to.id, status: "running",
    ownerHostId: to.id, ownerIdentityId: to.id, ownerIncarnation: receiver.incarnation,
    controlProtocol: "v1", capabilities: ["followUp", "steer"] } as FabricParticipantInfo;
  const router = new AgentMessageRouter(
    { status(id: string) { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0],
    { identity: from, mesh: { root }, status(id: string) { throw new Error(`Unknown Fabric actor: ${id}`); } } as unknown as Ports[1],
    { id: from.id, local: true, matches: (id: string) => id === from.id } as Ports[2],
    { get: (id: string) => id === targetId ? participant : undefined, scheduleRefresh() {}, lastKnown: () => undefined },
    sender, binding => binding,
  );
  const commands = () => mesh.read({ topic: "fabric.control.command", limit: 1000 })
    .filter(event => event.kind === "followUp");
  const send = (signal?: AbortSignal) => router.routeMessage(targetId, "same immutable message", { token: "unchanged" }, "followUp",
    signal ? { signal, cwd: root, parentToolCallId: "test", nestedToolCallId: "test", extensionContext: {}, update() {} } as unknown as FabricInvocationContext : undefined,
    { idempotencyKey: "same-logical-delivery" });
  return { root, from, to, targetId, mesh, receiver, commands, send };
};

describe("followUp durable publication outcome (#14867)", () => {
  // Keep reason: real owner ACK attribution and incarnation fences are a data-safety
  // boundary. Publication alone must not overwrite a proven refusal with unknown.
  it("keeps a real stale-incarnation owner refusal out of the unknown outcome ledger", async () => {
    const f = fixture();
    await f.receiver.close();
    const replacementStore = new MeshStore(f.root, 64 * 1024, 1000); stores.push(replacementStore);
    const replacement = new FabricControlPlane(replacementStore, f.to, { enabled: true, hostId: f.to.id,
      ownerIncarnation: "test:replacement", pollMs: 20, acknowledgementTimeoutMs: 120 });
    planes.push(replacement);
    const delivered = vi.fn(() => ({ accepted: true }));
    replacement.start(delivered);

    const error = await f.send().catch(error => error);
    expect(error).toMatchObject({ name: "FabricControlStaleIncarnationError", code: CONTROL_STALE_INCARNATION });
    expect(f.commands()).toHaveLength(1);
    const event = f.commands()[0]!;
    const command = event.data as FabricControlCommand;
    expect(command.ownerIncarnation).toBe(f.receiver.incarnation);
    expect(delivered).not.toHaveBeenCalled();
    expect(f.mesh.listAll("topology/control-seen/")).toEqual([]);
    expect(f.mesh.read({ topic: "fabric.control.ack", limit: 1000 })).toContainEqual(expect.objectContaining({
      from: expect.objectContaining({ id: f.to.id }), to: f.from.id,
      data: expect.objectContaining({ commandId: command.commandId, targetId: f.targetId, accepted: false,
        errorCode: CONTROL_STALE_INCARNATION, ownerIncarnation: replacement.incarnation,
        staleIncarnation: f.receiver.incarnation }),
    }));
    expect(hasDeliveryOutcome(f.root, { eventId: event.id, to: f.targetId, from: f.from.id, mode: "followUp" }, "unknown")).toBe(false);
  });

  it("keeps a typed terminal refusal through the receiver router and ACK out of the unknown ledger", async () => {
    const f = fixture("agent:terminal");
    const terminal = new FabricTargetTerminalError(f.targetId, "final-answer:recorded", "completed");
    // Only the task-ingress refusal is a port fixture, as in topology-terminal-task.
    // Receiver router, control claim, bounded ACK, attribution and sender router are real.
    const ingress = vi.fn(() => { throw terminal; });
    const ownerRouter = new AgentMessageRouter(
      { status: () => ({ id: f.targetId, status: "completed" }), followUp: ingress } as unknown as Ports[0],
      { identity: f.to } as unknown as Ports[1],
      { id: f.to.id, local: true, matches: (id: string) => id === f.to.id } as Ports[2],
      { get: () => undefined, scheduleRefresh() {}, lastKnown: () => undefined }, undefined, binding => binding,
    );
    f.receiver.start((...args) => ownerRouter.acceptControl(...args));

    const error = await f.send().catch(error => error);
    expect(error).toMatchObject({ name: "FabricTargetTerminalError", code: "FABRIC_TARGET_TERMINAL",
      targetId: f.targetId, finalAnswerReceiptId: terminal.finalAnswerReceiptId, runState: "completed" });
    expect(ingress).toHaveBeenCalledOnce();
    expect(f.commands()).toHaveLength(1);
    const event = f.commands()[0]!;
    const command = event.data as FabricControlCommand;
    expect(command.ownerIncarnation).toBe(f.receiver.incarnation);
    expect(f.mesh.read({ topic: "fabric.control.ack", limit: 1000 })).toContainEqual(expect.objectContaining({
      from: expect.objectContaining({ id: f.to.id }), to: f.from.id,
      data: expect.objectContaining({ commandId: command.commandId, accepted: false,
        ownerIncarnation: f.receiver.incarnation, code: "FABRIC_TARGET_TERMINAL", targetId: f.targetId,
        finalAnswerReceiptId: terminal.finalAnswerReceiptId, runState: "completed" }),
    }));
    expect(hasDeliveryOutcome(f.root, { eventId: event.id, to: f.targetId, from: f.from.id, mode: "followUp" }, "unknown")).toBe(false);
  });

  it.each(["queue expiry", "CAS loss", "misleading quota text"] as const)("reports delivery unconfirmed after append then %s, without replay", async fault => {
    const f = fixture();
    const original = f.mesh.publish.bind(f.mesh);
    // The same text is definitive only in a real rejected owner ACK, not an arbitrary
    // post-append failure. Keep reason: data safety after an already committed command.
    const failure = fault === "misleading quota text"
      ? new Error("Main's followUp queue is full (50 of yours); Main is busy and reads followUps only at its next tool boundary. Wait, or send a short steer.")
      : new MeshLockQueueExpiredError(path.join(f.root, ".lock"), 3, 10000);
    if (fault === "CAS loss") await f.mesh.put({ key: "test/delivery-cas", value: "winner", identity: f.from, ifVersion: 0 });
    const publish = vi.spyOn(f.mesh, "publish").mockImplementationOnce(async input => {
      await original({ ...input, durable: true }); // Actual durable native command, not a mocked receipt.
      if (fault === "CAS loss") await f.mesh.put({ key: "test/delivery-cas", value: "loser", identity: f.from, ifVersion: 0 });
      throw failure;
    });
    const error = await f.send().catch(error => error);
    expect(f.commands()).toHaveLength(1);
    const event = f.commands()[0]!;
    expect(error).toMatchObject({ name: "FabricDeliveryUnconfirmedError", code: "FABRIC_DELIVERY_UNCONFIRMED",
      deliveryId: (event.data as FabricControlCommand).commandId, targetId: f.to.id, idempotencyKey: "same-logical-delivery" });
    expect(error.message).toMatch(/do not resend/i);
    expect(error.message).toContain((event.data as FabricControlCommand).commandId);
    expect(error).not.toBe(failure);
    if (fault !== "CAS loss") expect(error.cause).toBe(failure);
    else expect(error.cause).toBeInstanceOf(Error);
    expect(publish.mock.calls.filter(([input]) => input.topic === "fabric.control.command" && input.kind === "followUp")).toHaveLength(1);
    const delivered = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    f.receiver.start(delivered);
    await vi.waitFor(() => expect(delivered).toHaveBeenCalledOnce(), { timeout: 3000 });
    expect(delivered.mock.calls[0]![0]).toMatchObject({ message: "same immutable message", data: { token: "unchanged" } });
    expect(f.commands()).toHaveLength(1);
  });

  it("keeps an arbitrary rejected owner ACK unconfirmed after admission", async () => {
    const f = fixture();
    const reason = "owner handoff failed after admission";
    const delivered = vi.fn(() => ({ accepted: false, error: reason }));
    f.receiver.start(delivered);

    const error = await f.send().catch(error => error);
    expect(delivered).toHaveBeenCalledOnce();
    expect(f.commands()).toHaveLength(1);
    const event = f.commands()[0]!;
    expect(error).toMatchObject({ name: "FabricDeliveryUnconfirmedError", code: "FABRIC_DELIVERY_UNCONFIRMED",
      deliveryId: (event.data as FabricControlCommand).commandId, targetId: f.to.id, idempotencyKey: "same-logical-delivery" });
    expect(error.cause).toMatchObject({ message: reason, rejection: "FABRIC_CONTROL_REJECTED", notRun: false });
    expect(hasDeliveryOutcome(f.root, { eventId: event.id, to: f.targetId, from: f.from.id, mode: "followUp" }, "unknown")).toBe(true);
  });

  it("reports delivery unconfirmed when the owner accepts but its ACK cannot return", async () => {
    const f = fixture();
    const publish = f.receiver.mesh.publish.bind(f.receiver.mesh);
    vi.spyOn(f.receiver.mesh, "publish").mockImplementation(input => input.topic === "fabric.control.ack"
      ? Promise.reject(new Error("return bridge unavailable")) : publish(input));
    const delivered = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    f.receiver.start(delivered);
    const error = await f.send().catch(error => error);
    expect(delivered).toHaveBeenCalledOnce();
    expect(f.commands()).toHaveLength(1);
    expect(error).toMatchObject({ name: "FabricDeliveryUnconfirmedError", code: "FABRIC_DELIVERY_UNCONFIRMED",
      deliveryId: (f.commands()[0]!.data as FabricControlCommand).commandId,
      targetId: f.to.id, idempotencyKey: "same-logical-delivery" });
    expect(error.message).toMatch(/do not resend/i);
    expect(error.cause).toBeInstanceOf(Error);
    expect(error.cause.message).toMatch(/outcome is unknown/);
    expect(hasDeliveryOutcome(f.root, { eventId: f.commands()[0]!.id, to: f.targetId, from: f.from.id, mode: "followUp" }, "unknown")).toBe(true);
  });

  it("preserves the delivery identity when the actual guest abort race wins after append", async () => {
    const f = fixture();
    const controller = new AbortController();
    const original = f.mesh.publish.bind(f.mesh);
    vi.spyOn(f.mesh, "publish").mockImplementationOnce(async input => {
      await original({ ...input, durable: true });
      controller.abort(new Error("stop after committed followUp"));
      throw controller.signal.reason;
    });
    const result = await new QuickJsRuntime().execute(
      'await agents.followUp({ id: "session:receiver", message: "same immutable message", idempotencyKey: "same-logical-delivery" });',
      async (ref, _args, signal) => { expect(ref).toBe("agents.followUp"); return f.send(signal); },
      { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024, signal: controller.signal },
    );
    expect(f.commands()).toHaveLength(1);
    expect(result.error).toContain("FABRIC_DELIVERY_UNCONFIRMED");
    expect(result.error).toContain((f.commands()[0]!.data as FabricControlCommand).commandId);
    expect(result.value).toBeUndefined();
  });

  it("preserves a pre-append queue failure unchanged and publishes nothing", async () => {
    const f = fixture();
    const failure = new MeshLockQueueExpiredError(path.join(f.root, ".lock"), 3, 10000);
    vi.spyOn(f.mesh, "publish").mockRejectedValueOnce(failure);
    await expect(f.send()).rejects.toBe(failure);
    expect(f.commands()).toEqual([]);
  });

  it("still acknowledges ordinary delivery exactly once", async () => {
    const f = fixture();
    const delivered = vi.fn((command: FabricControlCommand) => ({ accepted: true, messageId: command.commandId }));
    f.receiver.start(delivered);
    await expect(f.send()).resolves.toMatchObject({ queued: true, routed: "mesh", acknowledged: true });
    expect(delivered).toHaveBeenCalledOnce();
    expect(f.commands()).toHaveLength(1);
  });
});
