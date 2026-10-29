import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MainAgentController, type FabricMainAgentDeliveryRequest } from "../src/main-agent.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { controlBodyFingerprint } from "../src/mesh/control-idempotency.js";
import { admitScriptedInput, admitBoundScriptedInput, bindInputAdmission } from "../src/recovery/input-admission.js";
import * as control from "../src/topology/control-plane.js";
import type { ParticipantRoute, ParticipantView } from "../src/topology/participant-view.js";
import { TransportUnsupportedError, type FabricTransport, type TransportDelivery, type TransportDrainState,
  type TransportEnvelope, type TransportKv, type TransportSent } from "../src/transport/types.js";

// Failure-path/data-safety integration fixture: REAL control inbox and persisted private
// seen store, plus REAL Main journal. Transport credentials/portable lease are a fixture,
// not live NATS or physical-host qualification. Event gates avoid clock/polling races.
const SESSION = "11111111-1111-4111-8111-111111111111", TARGET = `session:${SESSION}`;
const FROM: MeshIdentity = { id: "session:22222222-2222-4222-8222-222222222222", name: "Sender", kind: "main" };
const OWNER = "runtime:receiver", CALLER = "runtime:sender", OLD = "old-epoch", CURRENT = "current-epoch";
const offered = () => ({ scriptedInput: { version: 1, inputId: "round-1", sessionId: SESSION, deadlineAt: Date.now() + 60_000 } });
function gate<T>() { let resolve!: (v: T) => void; const promise = new Promise<T>(done => { resolve = done; }); return { promise, resolve }; }
class Queue<T> {
  values: T[] = []; waiter: ((v: T | undefined) => void) | undefined;
  push(v: T) { if (this.waiter) { const fn = this.waiter; this.waiter = undefined; fn(v); } else this.values.push(v); }
  take(signal?: AbortSignal): Promise<T | undefined> {
    if (signal?.aborted) return Promise.resolve(undefined);
    if (this.values.length) return Promise.resolve(this.values.shift());
    return new Promise(resolve => { const finish = (v: T | undefined) => { signal?.removeEventListener("abort", abort); this.waiter = undefined; resolve(v); };
      const abort = () => finish(undefined); this.waiter = finish; signal?.addEventListener("abort", abort, { once: true }); });
  }
  async required(): Promise<T> { const v = await this.take(); if (!v) throw new Error("fixture gate ended"); return v; }
}
class Carrier implements FabricTransport {
  readonly kind = "nats" as const;
  readonly self = { machine: "receiver-new", hostId: OWNER };
  readonly incoming = new Queue<TransportDelivery<TransportEnvelope>>();
  readonly acknowledgements = new Queue<TransportEnvelope<any>>();
  readonly publications = new Queue<TransportEnvelope<any>>();
  constructor(readonly machine = "receiver-new", readonly hostId = OWNER) { this.self = { machine, hostId }; }
  async send<T>(envelope: TransportEnvelope<T>): Promise<TransportSent<T>> {
    const body = typeof envelope.body === "function" ? (envelope.body as (at: number) => T)(Date.now()) : envelope.body;
    const resolved = JSON.parse(JSON.stringify({ ...envelope, body }));
    (envelope.kind === "ack" ? this.acknowledgements : this.publications).push(resolved);
    return { id: envelope.id, sequence: 1, duplicate: false, envelope: resolved };
  }
  async *inbox(signal: AbortSignal) { while (!signal.aborted) { const v = await this.incoming.take(signal); if (!v) return; yield v; } }
  kv(): TransportKv { throw new TransportUnsupportedError("no KV in control fixture"); }
  async drainState(): Promise<TransportDrainState> { throw new TransportUnsupportedError("no broker drain proof"); }
  async close() {}
  deliver(command: control.FabricControlCommand, sourceMachine = "sender-new") {
    const done = gate<string>();
    this.incoming.push({ payload: { id: "wire-event", kind: "command", to: this.self, from: FROM, operation: command.operation,
      targetId: command.targetId, body: command }, sourceMachine, verification: "bridge", sequence: 1, attempt: 1,
      working() {}, async ack() { done.resolve("ack"); }, nak() { done.resolve("nak"); }, async term() { done.resolve("term"); } });
    return done.promise;
  }
}
const cleanups: Array<() => unknown> = [];
afterEach(async () => { vi.restoreAllMocks(); for (const fn of cleanups.splice(0).reverse()) await fn(); });
async function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "scripted-control-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  const mesh = new MeshStore(root, 65_536, 1_000, { stateBackend: "file" }); cleanups.push(() => mesh.closeState());
  const route: ParticipantRoute = { machine: "receiver-new", ownerHostId: OWNER, ownerIdentityId: TARGET,
    ownerIncarnation: CURRENT, capabilities: ["followUp", "steer"], live: true };
  const view: ParticipantView = { leaseLive: () => true, resolve: id => id === TARGET ? route : id === FROM.id
    ? { ...route, machine: "sender-new", ownerHostId: CALLER, ownerIdentityId: FROM.id } : undefined,
    whenChanged: async () => { throw new Error("no watch expected"); } };
  const transport = new Carrier();
  const plane = new control.FabricControlPlane(mesh, { id: TARGET, name: "Receiver", kind: "main" }, {
    enabled: true, hostId: OWNER, ownerIncarnation: CURRENT, transport, participants: view, relayOperatorMachine: "fabric-ops", pollMs: 20 });
  cleanups.push(() => plane.close());
  const seen = new MeshStore(path.join(root, "control-seen", createHash("sha256").update(OWNER).digest("hex").slice(0, 32)),
    65_536, 1_000, { stateBackend: "file" }); cleanups.push(() => seen.closeState());
  const key = (id: string) => "topology/control-seen/" + createHash("sha256").update(`${OWNER}\0${id}`).digest("hex");
  const command = (data?: unknown, ownerIncarnation = CURRENT): control.FabricControlCommand => ({ version: 1,
    commandId: "logical-command", targetId: TARGET, operation: "steer", replyTo: CALLER, ownerIncarnation,
    requestedAt: Date.now(), deadlineAt: Date.now() + 60_000, message: "original", ...(data ? { data } : {}) });
  return { root, plane, seen, transport, key, command };
}
function native(root: string) {
  const sent: any[] = [], entries: any[] = [];
  const pi = { on() { return () => {}; }, sendMessage: (m: unknown) => sent.push(m), sendUserMessage() {}, getThinkingLevel: () => "off" } as unknown as ExtensionAPI;
  const ctx = { isIdle: () => true, hasPendingMessages: () => false, signal: { aborted: false }, sessionManager: {
    getEntries: () => entries, getSessionId: () => SESSION } } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, TARGET, true, process.cwd(), SESSION);
  main.attachFollowUpDrain(ctx, 60_000, path.join(root, "main-followups", "main.json"));
  cleanups.push(() => main.closeFollowUpDrain()); return { main, sent };
}
const bindReconciler = (fn: (input: { root: string; sessionId: string; request: FabricMainAgentDeliveryRequest }) => ReturnType<MainAgentController["deliverAgent"]> | undefined) => {
  cleanups.push(control.bindScriptedInputControlReconciler(fn));
};

describe("scripted-only orphan reconciliation and original absolute deadlines", () => {
  it.skipIf(process.platform === "win32").each(["claim-before-B5", "native-before-outcome"])("reconciles %s through bound native admission, not the generic handler", async window => {
    const f = await fixture(), n = native(f.root), data = offered(), cmd = f.command(data);
    const request: FabricMainAgentDeliveryRequest = { from: FROM, verification: "bridge", message: "original", delivery: "steer", data, triggerTurn: true };
    if (window === "native-before-outcome") admitScriptedInput({ root: f.root, sessionId: SESSION, mainAgent: n.main, request });
    await f.seen.put({ key: f.key(cmd.commandId), identity: FROM, value: { format: 1, carrier: "nats", hostId: OWNER,
      commandId: cmd.commandId, targetId: TARGET, ownerIncarnation: OLD, expiresAt: Date.now() + 86_400_000,
      bodyDigest: controlBodyFingerprint(cmd), sourceMachine: "sender-old", requesterId: FROM.id } });
    cleanups.push(bindInputAdmission({ root: f.root, sessionId: SESSION, mainAgent: n.main }));
    bindReconciler(input => {
      expect(input.root).toBe(f.root); expect(input.sessionId).toBe(SESSION);
      return admitBoundScriptedInput(input);
    });
    const generic = vi.fn(() => ({ accepted: true })); f.plane.start(generic);
    const disposition = f.transport.deliver(cmd);
    const ack = await f.transport.acknowledgements.required();
    expect(ack.body).toMatchObject({ accepted: true, ownerIncarnation: CURRENT });
    expect(await disposition).toBe("ack");
    expect(generic).not.toHaveBeenCalled(); expect(n.sent).toHaveLength(1);
    expect((f.seen.get(f.key(cmd.commandId), { fresh: true })?.value as any).acceptance.accepted).toBe(true);
    const changed = { ...cmd, message: "changed bytes" };
    const rejected = f.transport.deliver(changed);
    expect((await f.transport.acknowledgements.required()).body).toMatchObject({ accepted: false, errorCode: "FABRIC_CONTROL_ID_CONFLICT" });
    expect(await rejected).toBe("ack"); expect(n.sent).toHaveLength(1);
  });

  it.skipIf(process.platform === "win32").each(["claim-before-B5", "native-before-outcome"])("reconciles file-carrier %s and replays the saved receipt under the current epoch", async window => {
    const f = await fixture(), n = native(f.root), data = offered(), cmd = { ...f.command(data), triggerTurn: true };
    const key = "file-logical-key";
    cmd.commandId = createHash("sha256").update(JSON.stringify([CALLER, OWNER, TARGET, TARGET, "steer", key, 0])).digest("hex");
    if (window === "native-before-outcome") admitScriptedInput({ root: f.root, sessionId: SESSION, mainAgent: n.main,
      request: { from: FROM, verification: "mesh", message: cmd.message!, data, delivery: "steer", triggerTurn: true } });
    await f.seen.put({ key: f.key(cmd.commandId), identity: FROM, value: { format: 1, hostId: OWNER, commandId: cmd.commandId,
      targetId: TARGET, ownerIncarnation: OLD, expiresAt: Date.now() + 86_400_000, bodyDigest: controlBodyFingerprint(cmd), requesterId: FROM.id } });
    cleanups.push(bindInputAdmission({ root: f.root, sessionId: SESSION, mainAgent: n.main }));
    bindReconciler(admitBoundScriptedInput);
    const make = (id: string, who: MeshIdentity) => {
      const mesh = new MeshStore(f.root, 65_536, 1_000, { stateBackend: "file" }); cleanups.push(() => mesh.closeState());
      const plane = new control.FabricControlPlane(mesh, who, { enabled: true, hostId: id, ownerIncarnation: CURRENT, pollMs: 20 });
      cleanups.push(() => plane.close()); return plane;
    };
    const owner = make(OWNER, { id: TARGET, name: "Receiver", kind: "main" }), sender = make(CALLER, FROM);
    const generic = vi.fn(() => ({ accepted: true })); owner.start(generic); sender.start(() => ({ accepted: false }));
    const send = () => sender.request(OWNER, TARGET, "steer", { message: cmd.message!, data, triggerTurn: true, ownerIncarnation: CURRENT }, TARGET, { idempotencyKey: key });
    await expect(send()).resolves.toMatchObject({ acknowledged: true });
    await expect(send()).resolves.toMatchObject({ acknowledged: true });
    expect(n.sent).toHaveLength(1); expect(generic).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")("leaves a stale scripted orphan unadmitted, then reconciles the unchanged current-epoch retry", async () => {
    const f = await fixture(), n = native(f.root), data = offered(), old = f.command(data, OLD);
    await f.seen.put({ key: f.key(old.commandId), identity: FROM, value: { format: 1, carrier: "nats", hostId: OWNER,
      commandId: old.commandId, targetId: TARGET, ownerIncarnation: OLD, expiresAt: Date.now() + 86_400_000,
      bodyDigest: controlBodyFingerprint(old), sourceMachine: "sender-new", requesterId: FROM.id } });
    cleanups.push(bindInputAdmission({ root: f.root, sessionId: SESSION, mainAgent: n.main })); bindReconciler(admitBoundScriptedInput);
    const generic = vi.fn(() => ({ accepted: true })); f.plane.start(generic);
    const stale = f.transport.deliver(old);
    expect((await f.transport.acknowledgements.required()).body.accepted).toBe(false);
    expect(await stale).toBe("ack"); expect(n.sent).toHaveLength(0);
    expect((f.seen.get(f.key(old.commandId), { fresh: true })?.value as any).acceptance).toBeUndefined();
    const retry = f.transport.deliver({ ...old, ownerIncarnation: CURRENT });
    expect((await f.transport.acknowledgements.required()).body.accepted).toBe(true);
    expect(await retry).toBe("ack"); expect(n.sent).toHaveLength(1); expect(generic).not.toHaveBeenCalled();
  });

  it("keeps ordinary orphan claims indeterminate and never consults scripted reconciliation", async () => {
    const f = await fixture(), cmd = f.command();
    await f.seen.put({ key: f.key(cmd.commandId), identity: FROM, value: { format: 1, carrier: "nats", hostId: OWNER,
      commandId: cmd.commandId, targetId: TARGET, ownerIncarnation: OLD, expiresAt: Date.now() + 86_400_000 } });
    const reconcile = vi.fn(); bindReconciler(reconcile);
    const generic = vi.fn(() => ({ accepted: true })); f.plane.start(generic);
    const disposition = f.transport.deliver(cmd); const ack = await f.transport.acknowledgements.required();
    expect(ack.body).toMatchObject({ accepted: false, error: expect.stringContaining("indeterminate") });
    expect(await disposition).toBe("ack"); expect(reconcile).not.toHaveBeenCalled(); expect(generic).not.toHaveBeenCalled();
  });

  it.skipIf(process.platform === "win32")("does not newly admit a stale epoch or an authenticated-source mismatch", async () => {
    const f = await fixture(), reconcile = vi.fn(), generic = vi.fn(() => ({ accepted: true })); bindReconciler(reconcile); f.plane.start(generic);
    for (const [epoch, source] of [[OLD, "sender-new"], [CURRENT, "sender-old"]]) {
      const disposition = f.transport.deliver(f.command(offered(), epoch), source);
      const ack = await f.transport.acknowledgements.required(); expect(ack.body.accepted).toBe(false);
      expect(await disposition).toBe("ack");
    }
    expect(reconcile).not.toHaveBeenCalled(); expect(generic).not.toHaveBeenCalled();
  });

  it("caps wire deadline and sender wait even under the NATS timeout floor", async () => {
    const f = await fixture(), data = offered(); data.scriptedInput.deadlineAt = Date.now() + 120;
    // This is sender control protocol only: hold its ACK, no model or broker watchdog.
    const sending = f.plane.request(OWNER, TARGET, "steer", { message: "original", data }, TARGET, { idempotencyKey: "round-key" });
    const observed = sending.catch(error => error);
    const wire = await f.transport.publications.required();
    expect(wire.body.deadlineAt).toBeLessThanOrEqual(data.scriptedInput.deadlineAt);
    const result = await Promise.race([observed, new Promise(resolve => { const timer = setTimeout(() => resolve("WAIT_RESET"), 1_000); timer.unref(); })]);
    expect(result).not.toBe("WAIT_RESET");
  });

  it.skipIf(process.platform === "win32")("syncs the exact private outcome store before publishing its ACK", async () => {
    const f = await fixture(), originalSync = MeshStore.prototype.sync, synced: string[] = [];
    vi.spyOn(MeshStore.prototype, "sync").mockImplementation(async function(this: MeshStore, ...args) {
      synced.push(this.root); return originalSync.apply(this, args);
    });
    f.plane.start(() => ({ accepted: true, messageId: "accepted" }));
    const disposition = f.transport.deliver(f.command(offered()));
    await f.transport.acknowledgements.required();
    expect(synced).toContain(f.seen.root);
    expect(await disposition).toBe("ack");
  });
});
