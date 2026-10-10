import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricTargetTerminalError } from "../src/agents/terminal-target.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";
import { agentParticipantRecords } from "../src/topology/records.js";
import type { AgentRunRecord } from "../src/agents/types.js";

const roots: string[] = [], planes: FabricControlPlane[] = [];
afterEach(async () => {
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const router = (manager: Ports[0], who: MeshIdentity, entries: FabricParticipantInfo[] = [], control?: Ports[4]) => {
  const actors = { identity: who, status: (id: string) => { throw new Error(`Unknown Fabric actor: ${id}`); },
    validateDirectMessage: vi.fn(), owns: () => true, tell: vi.fn(), ask: vi.fn(), stop: vi.fn(),
    steerRemote: vi.fn(), resolveBinding: vi.fn(), resolveActivationBinding: vi.fn() } as unknown as Ports[1];
  const main = { id: who.id, local: true, matches: (id: string) => id === who.id, deliverAgent: vi.fn() } as unknown as Ports[2];
  const participants = { get: (id: string) => entries.find(entry => entry.id === id), scheduleRefresh: vi.fn(), lastKnown: () => undefined };
  return new AgentMessageRouter(manager, actors, main, participants, control, binding => binding);
};
const plane = (root: string, who: MeshIdentity) => {
  const value = new FabricControlPlane(new MeshStore(root, 64 * 1024, 1000), who, {
    enabled: true, hostId: who.id, pollMs: 20, acknowledgementTimeoutMs: 1000,
  });
  planes.push(value); return value;
};

describe("terminal task topology", () => {
  it.each(["steer", "followUp"] as const)("keeps typed %s receipt rejection across the terminal directory/owner ACK path", async delivery => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "terminal-topology-")); roots.push(root);
    const targetId = "task-target", receiptId = "answer-receipt";
    const ownerIdentity = identity("session:owner"), senderIdentity = identity("session:sender");
    const terminalError = new FabricTargetTerminalError(targetId, receiptId);
    const ingress = vi.fn((_id: string, _message: string, _data?: unknown, _provenance?: unknown) => { throw terminalError; });
    const manager = { status: () => ({ id: targetId, name: targetId, status: "completed" }), steer: ingress, followUp: ingress, stop: vi.fn() } as unknown as Ports[0];
    const ownerRouter = router(manager, ownerIdentity);
    const owner = plane(root, ownerIdentity), sender = plane(root, senderIdentity);
    owner.start((command, from, signal, verification) => ownerRouter.acceptControl(command, from, signal, verification));
    sender.start(() => ({ accepted: false }));
    const now = Date.now();
    const record: AgentRunRecord = { id: targetId, name: targetId, task: "done", status: "completed", runner: "pi", transport: "process", cwd: root,
      startedAt: now, updatedAt: now, finishedAt: now, turns: 1, toolCalls: 0, text: "done", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      finalAnswerReceipt: { id: receiptId, recordedAt: now } };
    const published = agentParticipantRecords([record], ownerIdentity.id, ownerIdentity.id, ownerIdentity.id, ownerIdentity.id, new Map(), now)[0]!;
    expect(published.capabilities).not.toContain(delivery);
    const participant = { ...published, local: false, stale: false } as FabricParticipantInfo;
    const unknown = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
    const senderRouter = router(unknown, senderIdentity, [participant], sender);
    await expect(senderRouter.routeMessage(targetId, "stale instruction", undefined, delivery)).rejects.toMatchObject({
      name: "FabricTargetTerminalError", code: "FABRIC_TARGET_TERMINAL", targetId, finalAnswerReceiptId: receiptId,
    });
    expect(ingress).toHaveBeenCalledOnce();
    const provenance = ingress.mock.calls[0]![3] as { sender: { id: string } };
    expect(provenance.sender.id).toBe(senderIdentity.id);
    const ack = sender.mesh.read({ topic: "fabric.control.ack", limit: 10 })[0]!.data;
    expect(ack).toMatchObject({ accepted: false, code: "FABRIC_TARGET_TERMINAL", targetId, finalAnswerReceiptId: receiptId });
  });

  it("does not publish to a stale terminal owner", async () => {
    const control = { request: vi.fn() } as unknown as Ports[4];
    const unknown = { status: (id: string) => { throw new Error(`Unknown Fabric agent: ${id}`); } } as unknown as Ports[0];
    const participant = { id: "target", kind: "agent", runner: "pi", transport: "process", status: "completed", local: false, stale: true,
      rootId: "session:owner", ownerHostId: "session:owner", ownerIdentityId: "session:owner", capabilities: [] } as unknown as FabricParticipantInfo;
    await expect(router(unknown, identity("session:sender"), [participant], control).routeMessage("target", "stale", undefined, "steer"))
      .rejects.toMatchObject({ code: "FABRIC_ROUTE_AUTHORITY_CHANGED" });
    expect(control!.request).not.toHaveBeenCalled();
  });

  it("exposes only branded terminal fields to a real QuickJS guest catch", async () => {
    const error = Object.assign(new FabricTargetTerminalError("target", "receipt"), { secret: "private" });
    const result = await new QuickJsRuntime().execute(`try { await agents.steer({ id: "target", message: "late" }); }
      catch (error) { return { name: error.name, code: error.code, targetId: error.targetId, finalAnswerReceiptId: error.finalAnswerReceiptId, secret: error.secret }; }`,
      async () => { throw error; }, { timeoutMs: 5000, memoryLimitBytes: 64 * 1024 * 1024 });
    expect(result.value).toEqual({ name: "FabricTargetTerminalError", code: "FABRIC_TARGET_TERMINAL", targetId: "target", finalAnswerReceiptId: "receipt" });
  });
});
