import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController } from "../src/main-agent.js";
import { closeMainInbox, reopenMainInbox } from "../src/main-drain/gate.js";
import { peekDrainSlot } from "../src/main-drain/slot.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import type { ParticipantRoute, ParticipantView } from "../src/topology/participant-view.js";
import { TransportUnavailableError, TransportUnsupportedError, type FabricTransport, type TransportDelivery, type TransportDrainState,
  type TransportEnvelope, type TransportKv, type TransportOwner, type TransportSent } from "../src/transport/types.js";

// D3 seat drain (smarty-dev#15310), challenge r2 N7: a draining Main's refusal reaches the SENDER as the typed,
// notRun FABRIC_MAIN_DRAINING over the file transport and the NATS ACK path, and a guest program sees the code.
// It is a visible refusal, not delivery: the sender keeps the message (the honest claim for non-retrying producers).
const owner: MeshIdentity = { id: "session:00000000-0000-4000-8000-0000000000d1", name: "owner", kind: "main" };
const peer: MeshIdentity = { id: "session:00000000-0000-4000-8000-0000000000d2", name: "peer", kind: "main" };
const roots: string[] = [];
const closers: Array<() => unknown> = [];
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  reopenMainInbox(owner.id);
  ((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.main.drain-slot.v1")] as Map<string, unknown> | undefined)?.delete(owner.id);
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const ownerMain = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-drain-ack-")); roots.push(root);
  const sent = vi.fn();
  const pi = { on: () => () => {}, sendMessage: sent, sendUserMessage: vi.fn(), getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false },
    sessionManager: { getSessionFile: () => undefined, getBranch: () => [], getEntries: () => [] } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, owner.id, true, root, "owner", true);
  main.attachFollowUpDrain(ctx, 0, path.join(root, "main-followups", "owner.json"));
  closers.push(() => main.closeFollowUpDrain());
  const router = new AgentMessageRouter({ status: () => { throw new Error("Unknown Fabric agent"); } } as any,
    { identity: peer } as any, main,
    { get: () => undefined, lastKnown: () => undefined, scheduleRefresh: () => {} } as any, undefined, binding => binding);
  return { root, main, router, sent, journal: path.join(root, "main-followups", "owner.json") };
};

describe("FABRIC_MAIN_DRAINING end to end (r2 N7)", () => {
  it("the owner's ACK carries errorCode FABRIC_MAIN_DRAINING with notRun, and nothing is journaled or sent", async () => {
    const f = ownerMain();
    closeMainInbox(owner.id);
    const ack = await f.router.acceptControl({ commandId: "c-1", targetId: owner.id, operation: "followUp", message: "during the move" } as any,
      peer, undefined, "mesh");
    expect(ack).toMatchObject({ accepted: false, errorCode: "FABRIC_MAIN_DRAINING", notRun: true });
    expect(ack.error).toContain("with a NEW message");
    expect(fs.existsSync(f.journal)).toBe(false);
    expect(f.sent).not.toHaveBeenCalled();
    expect(peekDrainSlot(owner.id)?.ledger).toEqual([expect.objectContaining({ id: "c-1", fromId: peer.id, path: "fabric-inbox" })]);
  });

  it("over the file transport the sender's request rejects typed; a guest program sees the code", async () => {
    const f = ownerMain();
    const mesh = new MeshStore(path.join(f.root, "mesh"), 64 * 1024, 100);
    const receiver = new FabricControlPlane(mesh, owner, { enabled: true, hostId: owner.id, pollMs: 5 });
    const sender = new FabricControlPlane(mesh, peer, { enabled: true, hostId: peer.id, pollMs: 5 });
    closers.push(() => receiver.close(), () => sender.close());
    receiver.start((command, from, signal) => f.router.acceptControl(command, from, signal, "mesh"));
    sender.start(() => ({ accepted: false }));
    closeMainInbox(owner.id);
    await expect(sender.request(owner.id, owner.id, "followUp", { message: "fleet notice" }))
      .rejects.toMatchObject({ code: "FABRIC_MAIN_DRAINING", notRun: true, name: "FabricMainDrainingError" });
    const guest = await new QuickJsRuntime().execute(
      'try { await agents.followUp({ id: "main", message: "guest notice" }); } catch (error) { return error.code; }',
      async () => sender.request(owner.id, owner.id, "followUp", { message: "guest notice" }),
      { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(guest.value).toBe("FABRIC_MAIN_DRAINING");
    expect(f.sent).not.toHaveBeenCalled();
    // After the undo, a NEW message is delivered once.
    reopenMainInbox(owner.id);
    await expect(sender.request(owner.id, owner.id, "followUp", { message: "after the move" })).resolves.toMatchObject({ queued: true });
    expect(f.sent).toHaveBeenCalledTimes(1);
  });

  it("the local in-process producer gets the typed refusal too", async () => {
    const f = ownerMain();
    closeMainInbox(owner.id);
    await expect(f.router.routeMessage(owner.id, "local", undefined, "followUp")).rejects.toMatchObject({ code: "FABRIC_MAIN_DRAINING", notRun: true });
  });
});

// ---- NATS ACK path: the real request/inbox/handler with a controlled carrier (as control-plane-nats-replay.test.ts).
const OWNER_HOST = "runtime:drain-owner";
const CALLER_HOST = "runtime:drain-caller";
const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main", sessionId: id });
const address = (endpoint: TransportOwner): string => `${endpoint.machine}/${endpoint.hostId}`;
class Carrier {
  readonly destinations = new Map<string, Endpoint>();
  sequence = 0;
  endpoint(self: TransportOwner): Endpoint { const endpoint = new Endpoint(this, self); this.destinations.set(address(self), endpoint); return endpoint; }
}
class Endpoint implements FabricTransport {
  readonly kind = "nats" as const;
  readonly #deliveries: Array<TransportDelivery<TransportEnvelope>> = [];
  #wake: (() => void) | undefined;
  #closed = new AbortController();
  constructor(readonly carrier: Carrier, readonly self: TransportOwner) {}
  push(delivery: TransportDelivery<TransportEnvelope>): void { this.#deliveries.push(delivery); this.#wake?.(); }
  async send<T>(envelope: TransportEnvelope<T>, signal?: AbortSignal): Promise<TransportSent<T>> {
    signal?.throwIfAborted();
    const destination = this.carrier.destinations.get(address(envelope.to));
    if (!destination) throw new TransportUnavailableError("Fixture destination absent");
    const createdAt = Date.now();
    const body = typeof envelope.body === "function" ? (envelope.body as (at: number) => T)(createdAt) : envelope.body;
    const resolved = JSON.parse(JSON.stringify({ ...envelope, body, createdAt })) as TransportEnvelope<T> & { body: T; createdAt: number };
    const sequence = ++this.carrier.sequence;
    destination.push({ payload: resolved as unknown as TransportEnvelope, sourceMachine: this.self.machine, verification: "bridge",
      attempt: 1, sequence, working() {}, async ack() {}, nak() {}, async term() {} });
    return { id: envelope.id, sequence, duplicate: false, envelope: resolved };
  }
  async *inbox(signal: AbortSignal): AsyncIterable<TransportDelivery<TransportEnvelope>> {
    while (!signal.aborted && !this.#closed.signal.aborted) {
      const next = this.#deliveries.shift();
      if (next) { yield next; continue; }
      await new Promise<void>(resolve => {
        this.#wake = () => { this.#wake = undefined; resolve(); };
        signal.addEventListener("abort", () => resolve(), { once: true });
        this.#closed.signal.addEventListener("abort", () => resolve(), { once: true });
      });
    }
  }
  kv(): TransportKv { throw new TransportUnsupportedError("Control-only fixture has no KV"); }
  async drainState(): Promise<TransportDrainState> { throw new TransportUnsupportedError("No broker drain receipt"); }
  async close(): Promise<void> { this.#closed.abort(); this.#wake?.(); }
}

describe("FABRIC_MAIN_DRAINING over the NATS ACK path (r2 N7)", () => {
  it("the caller's NATS request rejects typed and notRun", async () => {
    const f = ownerMain();
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-drain-nats-")); roots.push(root);
    const carrier = new Carrier();
    const routes = new Map<string, ParticipantRoute>();
    const participants: ParticipantView = { resolve: id => routes.get(id), leaseLive: () => true,
      whenChanged: async () => { throw new Error("No directory watch expected"); } };
    const runtime = (hostId: string, id: string, machine: string, incarnation: string) => {
      const transport = carrier.endpoint({ hostId, machine });
      const mesh = new MeshStore(root, 64 * 1024, 1_000, { stateBackend: "file" });
      const plane = new FabricControlPlane(mesh, identity(id), { enabled: true, hostId, ownerIncarnation: incarnation, participants, transport,
        relayOperatorMachine: "fabric-ops", pollMs: 20, acknowledgementTimeoutMs: 2_000 });
      closers.push(() => plane.close(), () => transport.close(), () => mesh.closeState());
      return plane;
    };
    const ownerPlane = runtime(OWNER_HOST, owner.id, "drain-owner", "drain:owner");
    routes.set(owner.id, { machine: "drain-owner", ownerHostId: OWNER_HOST, ownerIdentityId: owner.id, ownerIncarnation: "drain:owner",
      capabilities: ["followUp"], live: true });
    ownerPlane.start((command, from, signal) => f.router.acceptControl(command, from, signal, "bridge"));
    const callerPlane = runtime(CALLER_HOST, peer.id, "drain-caller", "drain:caller");
    routes.set(peer.id, { machine: "drain-caller", ownerHostId: CALLER_HOST, ownerIdentityId: peer.id, ownerIncarnation: "drain:caller",
      capabilities: [], live: true });
    callerPlane.start(() => { throw new Error("Caller must never execute this command"); });
    closeMainInbox(owner.id);
    await expect(callerPlane.request(OWNER_HOST, owner.id, "followUp", { message: "over nats" }, owner.id, { idempotencyKey: "drain-nats-1" }))
      .rejects.toMatchObject({ code: "FABRIC_MAIN_DRAINING", notRun: true, name: "FabricMainDrainingError" });
    expect(f.sent).not.toHaveBeenCalled();
  });
});
