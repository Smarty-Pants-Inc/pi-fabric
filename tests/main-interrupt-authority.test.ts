import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { isMainInterruptSupervisor } from "../src/interrupt-authority.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";

const owner: MeshIdentity = { id: "session:owner", name: "owner", kind: "main" };
const peer: MeshIdentity = { id: "session:peer", name: "peer", kind: "main" };
const roots: string[] = [];
const closers: Array<() => void> = [];
afterEach(() => {
  for (const close of closers.splice(0).reverse()) close();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
const setup = (allow: string[] = [], supervisor?: { id: string; rootId: string; supervisorFor?: string; status: string }) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-interrupt-authority-")); roots.push(root);
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => void>>();
  const state = { idle: false, aborted: false };
  const sent = vi.fn();
  const abort = vi.fn(() => { state.aborted = true; });
  const pi = { on: (name: string, fn: any) => {
    handlers.set(name, [...(handlers.get(name) ?? []), fn]);
    return () => handlers.set(name, (handlers.get(name) ?? []).filter(h => h !== fn));
  }, sendMessage: sent, sendUserMessage: vi.fn(), getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => state.idle, hasPendingMessages: () => false, abort,
    signal: { get aborted() { return state.aborted; } },
    sessionManager: { getSessionFile: () => undefined, getBranch: () => [] },
  } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, owner.id, true, root, "owner", true, undefined, {
    interruptFrom: () => allow,
    isSupervisor: (sender, rootId) => isMainInterruptSupervisor(sender, rootId, supervisor),
  });
  main.attachFollowUpDrain(ctx, 120_000, path.join(root, "journal.json"));
  closers.push(() => main.closeFollowUpDrain());
  const emit = (name: string, event: any = {}) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); };
  const start = () => { state.idle = false; state.aborted = false; emit("agent_start"); emit("tool_execution_start", { toolCallId: "tool" }); };
  const settle = () => { state.idle = true; emit("agent_settled", { outcome: state.aborted ? "aborted" : "completed" }); };
  const router = new AgentMessageRouter({ status: () => { throw new Error("Unknown Fabric agent"); } } as any,
    { identity: peer } as any, main,
    { get: () => undefined, lastKnown: () => undefined, scheduleRefresh: () => {} } as any, undefined, binding => binding);
  const interrupt = (from = owner, message = "HOLD", deliveryId?: string) => main.deliverAgent({
    from, verification: "mesh", delivery: "steer", priority: "interrupt", message, ...(deliveryId ? { deliveryId } : {}),
  });
  start();
  return { main, root, sent, abort, state, emit, start, settle, router, interrupt };
};

describe("interrupt-specific receiver authority (#7452)", () => {
  it("refuses an ordinary steerer locally and via owner control without delivery or downgrade", async () => {
    const f = setup();
    await expect(f.router.routeMessage(owner.id, "forbidden", undefined, "steer", undefined, { priority: "interrupt" }))
      .rejects.toMatchObject({ code: "FABRIC_INTERRUPT_NOT_AUTHORIZED" });
    const ack = await f.router.acceptControl({ commandId: "refused", targetId: owner.id, operation: "steer", message: "forbidden", priority: "interrupt" } as any, peer, undefined, "mesh");
    expect(ack).toMatchObject({ accepted: false, errorCode: "FABRIC_INTERRUPT_NOT_AUTHORIZED", notRun: true });
    expect(f.abort).not.toHaveBeenCalled(); expect(f.sent).not.toHaveBeenCalled();
    expect(f.main.queueDepth(peer.id).pendingFollowUps).toBe(0);
    f.settle(); expect(f.sent).not.toHaveBeenCalled();
    await f.router.routeMessage(owner.id, "ordinary", undefined, "steer");
    expect(f.sent).toHaveBeenCalledTimes(1); expect(f.abort).not.toHaveBeenCalled();
  });
  it.each([peer.id, "peer", "peer-main"])("admits only a verified host-allowlisted Main/session (%s)", entry => {
    const sender = entry === "peer-main" ? { ...peer, name: "peer-main" } : peer;
    const f = setup([entry]); f.interrupt(sender); expect(f.abort).toHaveBeenCalledTimes(1);
  });
  it("own root interrupts but an unverified identity or same-name actor cannot", () => {
    const f = setup(["owner", "peer"]);
    for (const from of [owner, peer]) expect(() => f.main.deliverAgent({ from, delivery: "steer", priority: "interrupt", message: "unverified" }))
      .toThrow(expect.objectContaining({ code: "FABRIC_INTERRUPT_NOT_AUTHORIZED" }));
    expect(() => f.interrupt({ ...peer, kind: "actor" })).toThrow(expect.objectContaining({ code: "FABRIC_INTERRUPT_NOT_AUTHORIZED" }));
    f.interrupt(owner); expect(f.abort).toHaveBeenCalledTimes(1);
  });
  it("uses the bridge-verified sender, not a forged identity or name in data", async () => {
    const f = setup([owner.id]);
    const command = { commandId: "bridge", targetId: owner.id, operation: "steer", message: "HOLD", priority: "interrupt", data: { from: owner, supervisorFor: owner.id } } as any;
    expect(await f.router.acceptControl(command, peer, undefined, "bridge"))
      .toMatchObject({ accepted: false, errorCode: "FABRIC_INTERRUPT_NOT_AUTHORIZED" });
    expect(f.sent).not.toHaveBeenCalled(); expect(f.abort).not.toHaveBeenCalled();
    expect(await f.router.acceptControl({ ...command, commandId: "owner-bridge" }, owner, undefined, "bridge"))
      .toMatchObject({ accepted: true }); expect(f.abort).toHaveBeenCalledTimes(1);
  });
  it("requires an immutable native supervisor binding to the exact current root", () => {
    const actor = { id: "supervisor-actor", rootId: owner.id, supervisorFor: owner.id, status: "running" };
    const sender: MeshIdentity = { id: actor.id, name: "irrelevant", kind: "actor" };
    const f = setup([], actor); f.interrupt(sender); expect(f.abort).toHaveBeenCalledTimes(1);
    for (const invalid of [undefined, { id: actor.id, rootId: actor.rootId, status: actor.status }, { ...actor, rootId: peer.id }, { ...actor, id: "another" }, { ...actor, status: "stopped" }, { ...actor, removal: {} }]) {
      expect(isMainInterruptSupervisor(sender, owner.id, invalid)).toBe(false);
    }
  });
  it("preserves the typed authorization refusal across a real mesh ACK", async () => {
    const f = setup();
    const mesh = new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100);
    const receiver = new FabricControlPlane(mesh, owner, { enabled: true, hostId: owner.id, pollMs: 5 });
    const sender = new FabricControlPlane(mesh, peer, { enabled: true, hostId: peer.id, pollMs: 5 });
    closers.push(() => receiver.close(), () => sender.close());
    receiver.start((command, from, signal) => f.router.acceptControl(command, from, signal, "mesh"));
    sender.start(() => ({ accepted: false }));
    await expect(sender.request(owner.id, owner.id, "steer", { message: "forbidden", priority: "interrupt" }))
      .rejects.toMatchObject({ code: "FABRIC_INTERRUPT_NOT_AUTHORIZED", notRun: true });
    const guest = await new QuickJsRuntime().execute(
      'try { await agents.send({ id: "main", message: "forbidden", priority: "interrupt" }); } catch (error) { return error.code; }',
      async () => sender.request(owner.id, owner.id, "steer", { message: "guest forbidden", priority: "interrupt" }),
      { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(guest.value).toBe("FABRIC_INTERRUPT_NOT_AUTHORIZED");
    expect(f.abort).not.toHaveBeenCalled(); f.settle(); expect(f.sent).not.toHaveBeenCalled();
  });
});

describe("typed interrupt cooldown over mesh (#7452)", () => {
  it("returns FABRIC_INTERRUPT_RATE_LIMITED through the verified owner ACK", async () => {
    const f = setup([peer.id]);
    const mesh = new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100);
    const receiver = new FabricControlPlane(mesh, owner, { enabled: true, hostId: owner.id, pollMs: 5 });
    const sender = new FabricControlPlane(mesh, peer, { enabled: true, hostId: peer.id, pollMs: 5 });
    closers.push(() => receiver.close(), () => sender.close());
    receiver.start((command, from, signal) => f.router.acceptControl(command, from, signal, "mesh"));
    sender.start(() => ({ accepted: false }));
    await sender.request(owner.id, owner.id, "steer", { message: "first HOLD", priority: "interrupt" });
    await expect(sender.request(owner.id, owner.id, "steer", { message: "rate refused", priority: "interrupt" }))
      .rejects.toMatchObject({ code: "FABRIC_INTERRUPT_RATE_LIMITED", notRun: true });
    const guest = await new QuickJsRuntime().execute(
      'try { await agents.send({ id: "main", message: "rate refused", priority: "interrupt" }); } catch (error) { return error.code; }',
      async () => sender.request(owner.id, owner.id, "steer", { message: "guest rate refused", priority: "interrupt" }),
      { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(guest.value).toBe("FABRIC_INTERRUPT_RATE_LIMITED");
    expect(f.abort).toHaveBeenCalledTimes(1); f.settle();
    expect(f.sent).toHaveBeenCalledTimes(1);
    expect(JSON.stringify(f.sent.mock.calls)).not.toContain("rate refused");
  });
});

describe("interrupt budget and monotonic sender cooldown (#7452)", () => {
  beforeEach(() => { vi.useFakeTimers(); });
  it("allows one abort per turn including its interrupted restart; another sender becomes ordinary", () => {
    const f = setup([peer.id]); f.interrupt();
    const excess = f.interrupt(peer, "second"); expect(excess.reason).toContain("ordinary steer");
    expect(f.abort).toHaveBeenCalledTimes(1);
    f.settle(); f.start(); // HOLD continuation, not a fresh turn budget.
    f.interrupt(peer, "third"); expect(f.abort).toHaveBeenCalledTimes(1);
    expect(f.sent.mock.calls.at(-1)?.[1]).toMatchObject({ deliverAs: "steer" });
    f.settle(); f.start(); // Independently started next turn.
    f.interrupt(peer, "fourth"); expect(f.abort).toHaveBeenCalledTimes(2);
  });
  it("rejects repeat sender until exactly 60 seconds across successive turns, without downgrade", () => {
    const f = setup(); f.interrupt(); f.settle(); f.start();
    const before = f.sent.mock.calls.length;
    vi.advanceTimersByTime(59_999);
    expect(() => f.interrupt()).toThrow(expect.objectContaining({ code: "FABRIC_INTERRUPT_RATE_LIMITED" }));
    expect(f.sent).toHaveBeenCalledTimes(before); expect(f.abort).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(1);
    // This is still the interrupted turn: cooldown expires but its budget remains used.
    f.interrupt(); expect(f.abort).toHaveBeenCalledTimes(1);
    f.settle(); f.start(); f.interrupt(); expect(f.abort).toHaveBeenCalledTimes(2);
  });
  it("does not consume another sender's budget or reject an idempotent retry", () => {
    const f = setup([peer.id]); const first = f.interrupt(owner, "HOLD", "durable");
    expect(f.interrupt(owner, "HOLD", "durable")).toMatchObject({ messageId: first.messageId, duplicate: true });
    expect(() => f.interrupt(owner, "again")).toThrow(expect.objectContaining({ code: "FABRIC_INTERRUPT_RATE_LIMITED" }));
    f.interrupt(peer); f.settle(); f.start(); f.settle(); f.start();
    f.interrupt(peer); expect(f.abort).toHaveBeenCalledTimes(2);
  });
  it("ordinary steer remains admissible during cooldown", () => {
    const f = setup(); f.interrupt(); f.settle(); f.start();
    const before = f.sent.mock.calls.length;
    f.main.deliverAgent({ from: owner, delivery: "steer", message: "ordinary correction" });
    expect(f.sent).toHaveBeenCalledTimes(before + 1); expect(f.abort).toHaveBeenCalledTimes(1);
  });
});
