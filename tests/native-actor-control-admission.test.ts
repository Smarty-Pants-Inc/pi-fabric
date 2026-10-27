// Keep: trust boundary/data safety. Exercise the real FILE reader, native verifier,
// private ledger producer and outcome/ACK stores. Fake presence substitutes for NATS
// KV/credentials, NOT proof of real-broker identity, ActorManager dispatch or residency.
// Router/provider adapter stubs prove ordering/argument fidelity only; never mint a
// positive capability by casting a context or by writing a hand-authored receipt.
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { NativeActorBinding, NativeActorContext } from "../src/actors/native-handler.js";
import type { ActorManager } from "../src/actors/manager.js";
import type { FabricActorMessage } from "../src/actors/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { StoreBridgeSide } from "../src/mesh/bridge.js";
import { NatsBridgeCarrier, type NatsInbound } from "../src/mesh/bridge-nats-carrier.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../src/mesh/bridge-nats-identity.js";
import {
  claimNativeAskAdmission, consumeNativeAskAdmission, createNativeActorIngress,
  type NativeActorIngress, type NativeControlAdmission,
} from "../src/mesh/bridge-nats-ingress.js";
import { openBridgeNatsLedger, type FileBridgeNatsLedger } from "../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger, natsLinkAuthorizers } from "../src/mesh/bridge-nats-link.js";
import { bridgeNatsMappedCommandId, type BridgeNatsPresenceRecord } from "../src/mesh/bridge-nats-types.js";
import { MeshStore, type MeshEvent, type MeshIdentity } from "../src/mesh/store.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { residentHostId } from "../src/residency/protocol.js";
import { CONTROL_STALE_INCARNATION, FabricControlPlane, controlActorBindingOptions, type FabricControlCommand } from "../src/topology/control-plane.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { BRIDGE_MACHINE, POD_MACHINE, LINK, FakePresence, fileLead, natsLead, otherNats, route } from "./helpers/bridge-nats-l2-fakes.js";

const ACTOR = "34200000000000000000000000000001";
const OWNER = residentHostId(fileLead.id);
const INCARNATION = "native-control-owner";
const MESSAGE = " \nsmarty-inbox.rpc.v1\t ";
const roots: string[] = [];
const planes: FabricControlPlane[] = [];
const stores: MeshStore[] = [];
const ledgers: FileBridgeNatsLedger[] = [];
const releasePending: Array<() => void> = [];
const POSIX = process.platform !== "win32" && !!fs.constants.O_NOFOLLOW && !!fs.constants.O_DIRECTORY && !!process.geteuid;
const seenKey = (commandId: string) => "topology/control-seen/" + createHash("sha256").update(`${OWNER}\0${commandId}`).digest("hex");
const mapped = (source = "source-ask") => bridgeNatsMappedCommandId(LINK, "toFile", natsLead.id, source);
const body = (event: MeshEvent) => event.data as Record<string, unknown>;
const eventOf = (plan: NatsInbound): MeshEvent => {
  if (plan.kind !== "event") throw new Error(`Private producer did not forward an event: ${plan.kind}`);
  return plan.event;
};
const gate = () => {
  let open!: () => void;
  const promise = new Promise<void>(resolve => { open = resolve; });
  releasePending.push(open);
  return { promise, open };
};

afterEach(async () => {
  for (const open of releasePending.splice(0)) open();
  await Promise.all(planes.splice(0).map(plane => plane.close()));
  for (const ledger of ledgers.splice(0)) await ledger.close();
  for (const store of stores.splice(0)) store.closeState();
  vi.restoreAllMocks();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

type RouterPorts = ConstructorParameters<typeof AgentMessageRouter>;
const adapter = () => {
  const model = vi.fn((): never => { throw new Error("MODEL PREFLIGHT MUST NOT RUN"); });
  const reply: FabricActorMessage = { id: "native-reply", actorId: ACTOR, actorName: "native", direction: "out", source: "direct", createdAt: Date.now(), text: "", data: { opaque: [1, null, { ok: true }] } };
  const actor = { id: ACTOR, name: "native", rootId: fileLead.id, runner: "native", residency: "durable", status: "idle" };
  const actors = {
    identity: fileLead, status: vi.fn((id: string) => {
      if (id !== ACTOR && id !== actor.name) throw new Error(`Unknown Fabric actor: ${id}`);
      return actor;
    }),
    isNativeActor: (id: string): boolean => id === ACTOR, owns: () => true,
    validateDirectMessage: vi.fn(), ask: vi.fn(async (_id: string, _message: string, _data: unknown, _signal?: AbortSignal,
      _options?: Parameters<ActorManager["ask"]>[4]) => reply), tell: model,
    stop: vi.fn(), steerRemote: model, resolveBinding: model, resolveActivationBinding: model,
  };
  const manager = { status: (id: string): never => { throw new Error(`Unknown Fabric agent: ${id}`); }, steer: model, followUp: model, stop: vi.fn() };
  const main = { id: fileLead.id, local: true, matches: (id: string) => id === fileLead.id, deliverAgent: model };
  const participants = { get: () => undefined, lastKnown: () => undefined, scheduleRefresh: vi.fn() };
  const router = new AgentMessageRouter(manager as unknown as RouterPorts[0], actors as unknown as RouterPorts[1],
    main as unknown as RouterPorts[2], participants, undefined, model);
  return { router, actors, actor, manager, main, participants, model, reply };
};

const fixture = async (controlIncarnation = INCARNATION) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "native-control-")); roots.push(root);
  const ledgerDir = path.join(root, "ledger");
  const ledger = await openBridgeNatsLedger({ dir: ledgerDir }); ledgers.push(ledger);
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 1_000); stores.push(mesh);
  await mesh.prepareLock();
  const file = new StoreBridgeSide(mesh, LINK);
  const presence = new FakePresence();
  const nativeIdentity: MeshIdentity = { id: ACTOR, name: "native", kind: "actor" };
  presence.natsEntries.set(ACTOR, route(nativeIdentity, BRIDGE_MACHINE, INCARNATION, { ownerHostId: OWNER, ownerIdentityId: OWNER }));
  const records: BridgeNatsPresenceRecord[] = [
    { id: fileLead.id, name: fileLead.name, kind: "main", rootId: fileLead.id, ownerHostId: fileLead.id, ownerIdentityId: fileLead.id, live: true, observedAt: Date.now() },
    { id: ACTOR, name: "native", kind: "actor", rootId: fileLead.id, ownerHostId: OWNER, ownerIdentityId: OWNER, ownerIncarnation: INCARNATION, live: true, observedAt: Date.now() },
  ];
  const allow = parseBridgeNatsAllowSection({
    file: [{ kind: "root", id: fileLead.id, send: [], receive: [] },
      { kind: "actor", rootId: fileLead.id, name: "native", id: ACTOR, send: [], receive: ["ask", "cancel"] }],
    nats: [natsLead, otherNats].map(sender => ({ kind: "root", id: sender.id, machine: POD_MACHINE, send: ["ask", "cancel"], receive: [] })),
  }, { link: LINK, natsMachine: BRIDGE_MACHINE });
  const identityGate = createBridgeNatsIdentityGate(allow, { pins: { seats: new Map(), bound: new Map() } });
  const { authorize } = natsLinkAuthorizers(identityGate, {
    file: () => ({ side: "file", records: id => records.filter(record => record.id === id).map(record => ({ ...record, observedAt: Date.now() })) }),
    nats: () => ({ side: "nats", records: id => {
      const entry = presence.nats(id);
      return entry ? [{ id, name: entry.identity.name, kind: entry.identity.kind, rootId: id,
        ownerHostId: entry.route.ownerHostId, ownerIdentityId: entry.route.ownerIdentityId,
        ownerIncarnation: entry.route.ownerIncarnation, machine: entry.route.machine, live: entry.route.live, observedAt: Date.now() }] : [];
    } }),
  });
  const carrier = new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger: natsCarrierLedger(ledger, LINK), authorize });
  const binding: NativeActorBinding = { actorId: ACTOR, name: "native", rootId: fileLead.id, scope: "session", handlerKey: "smarty-inbox.rpc.v1",
    residentModule: path.join(root, "not-loaded.mjs"), ingress: { link: LINK, ledgerDir } };
  const ingress = createNativeActorIngress({ bindings: [binding] });
  const a = adapter();
  type ProviderPorts = ConstructorParameters<typeof AgentsProvider>;
  const provider = new AgentsProvider({ ...a.manager, cwd: process.cwd(), config: DEFAULT_FABRIC_CONFIG.agents } as unknown as ProviderPorts[0],
    a.actors as unknown as ProviderPorts[1], {} as ProviderPorts[2], a.main as unknown as ProviderPorts[3],
    a.participants as unknown as ProviderPorts[4], undefined, {} as ProviderPorts[6]);
  const contexts: NativeActorContext[] = [];
  const admissionAttempts: MeshEvent[] = [];
  const recordedIngress: NativeActorIngress = {
    requiresNativeAdmission: id => ingress.requiresNativeAdmission(id),
    admitNativeControl: event => { admissionAttempts.push(event); return ingress.admitNativeControl(event); },
  };
  const received: Array<{ command: FabricControlCommand; admission?: NativeControlAdmission }> = [];
  let onAsk = async (_context: NativeActorContext) => a.reply;
  // Real private-handle validation, but still an adapter stub: NOT ActorManager's
  // serial queue/custody implementation and not an application-effect proof.
  a.actors.ask.mockImplementation(async (...args: unknown[]) => {
    const [actorId, message, data, signal, options] = args as [string, string, unknown, AbortSignal, { nativeAdmission?: NativeControlAdmission }];
    const request = { actorId, message, data };
    // C1's native API accepts no binding/overrides, not even synthesized binding:{}.
    if (Object.hasOwn(options, "binding") || Object.hasOwn(options, "overrides")) throw new Error("Native model binding reached enqueue");
    claimNativeAskAdmission(options.nativeAdmission!, request);
    const context = consumeNativeAskAdmission(options.nativeAdmission!, request, signal);
    contexts.push(context);
    return onAsk(context);
  });
  const own = new MeshStore(path.join(mesh.root, "control-seen", createHash("sha256").update(OWNER).digest("hex").slice(0, 32)), 256 * 1024, 1_000); stores.push(own);
  const start = (port: NativeActorIngress = recordedIngress) => {
    const owner = new FabricControlPlane(mesh, { id: OWNER, name: "resident", kind: "agent" }, {
      enabled: true, hostId: OWNER, ownerIncarnation: controlIncarnation, nativeActorIngress: port, pollMs: 20, acknowledgementTimeoutMs: 60_000,
    });
    planes.push(owner);
    owner.start((command, from, signal, verification, admission) => {
      received.push({ command, ...(admission ? { admission } : {}) });
      return command.targetId === "barrier" ? { accepted: true, messageId: command.commandId }
        : provider.acceptControl(command, from, signal, verification, admission);
    });
    return owner;
  };
  const owner = start();
  const acks = (id?: string) => mesh.read({ topic: "fabric.control.ack", limit: 1_000 })
    .filter(event => event.from.id === OWNER && (!id || body(event).commandId === id));
  let sequence = 0;
  const barrier = async () => {
    const id = `barrier-${++sequence}`;
    await mesh.publish({ topic: "fabric.control.command", kind: "steer", from: fileLead, to: OWNER,
      data: { version: 1, commandId: id, targetId: "barrier", operation: "steer", replyTo: fileLead.id,
        ownerIncarnation: controlIncarnation, message: id, requestedAt: Date.now(), deadlineAt: Date.now() + 60_000 } });
    await vi.waitFor(() => expect(acks(id)).toHaveLength(1));
  };
  const produce = async (changes: Record<string, unknown> = {}) => {
    const requestedAt = Date.now();
    const operation = changes.operation ?? "ask";
    return eventOf(await carrier.inbound({ sourceMachine: POD_MACHINE, sequence: ++sequence, envelope: {
      id: `source-event-${sequence}`, kind: "command", to: { machine: BRIDGE_MACHINE, hostId: OWNER }, targetId: ACTOR, operation, from: natsLead,
      body: { version: 1, commandId: "source-ask", targetId: ACTOR, operation, replyTo: natsLead.id, ownerIncarnation: INCARNATION,
        message: MESSAGE, data: { opaque: ["do not trim", { sid: "untrusted input" }] }, requestedAt, deadlineAt: requestedAt + 60_000, ...changes },
    } }));
  };
  const persist = async (raw: MeshEvent) => {
    // Real StoreBridgeSide reconstruction; this is the fixed reviewed toLocal
    // bridge stamp, not a new authority field or an invented admission handle.
    await file.publish({ topic: raw.topic, kind: raw.kind, from: raw.from, to: raw.to!,
      ...(raw.principal ? { principal: raw.principal } : {}), data: { ...body(raw), bridge: { from: LINK, id: raw.id } } });
    return mesh.read({ topic: "fabric.control.command", limit: 1_000 }).find(event => body(event).commandId === body(raw).commandId && event.dedupeKey !== undefined)!;
  };
  const forge = (changes: Record<string, unknown> = {}) => mesh.publish({ topic: "fabric.control.command", kind: String(changes.operation ?? "ask"),
    from: { ...natsLead, verified: "bridge" }, to: OWNER,
    data: { version: 1, commandId: mapped(), targetId: ACTOR, operation: "ask", replyTo: otherNats.id, ownerIncarnation: controlIncarnation,
      message: MESSAGE, data: { forged: true }, requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
      bridge: { from: LINK, id: "copied-label" }, nativeAdmission: {}, nativeContext: { sourceMachine: POD_MACHINE }, ...changes } });
  return { ...a, provider, binding, mesh, own, owner, start, ingress, contexts, admissionAttempts, received, produce, persist, forge, barrier, acks,
    setOnAsk: (fn: typeof onAsk) => { onAsk = fn; } };
};

describe.skipIf(!POSIX)("native FILE control admission before claims, outcomes and cancellation", () => {
  it("prevents public claim poisoning, then admits a genuine ask despite helper-synthesized binding:{}", async () => {
    const f = await fixture();
    await f.forge();
    await f.barrier();
    expect(f.mesh.get(seenKey(mapped()), { fresh: true })).toBeUndefined();
    expect(f.own.get(seenKey(mapped()), { fresh: true })).toBeUndefined();
    expect(f.acks(mapped())).toHaveLength(0);
    expect(f.actors.ask).not.toHaveBeenCalled();
    const raw = await f.produce();
    await f.barrier(); // Receipt existence alone dispatches nothing.
    expect(f.actors.ask).not.toHaveBeenCalled();
    const event = await f.persist(raw);
    await vi.waitFor(() => expect(f.acks(mapped())).toHaveLength(1));
    expect(f.contexts).toHaveLength(1);
    expect(f.contexts[0]).toMatchObject({ actorId: ACTOR, sender: { id: natsLead.id, kind: "main", sessionId: natsLead.id.slice(8) }, sourceMachine: POD_MACHINE });
    expect(f.actors.ask.mock.calls[0]?.[1]).toBe(MESSAGE);
    expect(f.actors.ask.mock.calls[0]?.[2]).toEqual(body(event).data);
    const nativeDelivery = f.received.find(entry => entry.command.targetId === ACTOR)!;
    expect(nativeDelivery.command).not.toHaveProperty("nativeAdmission");
    expect(controlActorBindingOptions(nativeDelivery.command, natsLead, fileLead.id, undefined)).toEqual({ binding: {} });
    expect(f.actors.ask.mock.calls[0]?.[4]).not.toHaveProperty("binding");
    expect(f.actors.ask.mock.calls[0]?.[4]).not.toHaveProperty("overrides");
    expect(nativeDelivery.admission).toBe(f.actors.ask.mock.calls[0]?.[4]?.nativeAdmission);
    expect(nativeDelivery.admission).toBeDefined();
    expect(f.acks(mapped())[0]).toMatchObject({ to: natsLead.id, data: { accepted: true, result: { data: f.reply.data } } });
    expect(f.model).not.toHaveBeenCalled();
  });

  it("rejects actual wire bindings, including an explicit empty binding, rather than silently dropping inputs", async () => {
    const f = await fixture();
    for (const [sourceId, binding] of [["source-model-binding", { model: "not-provider/not-model" }], ["source-empty-binding", {}]] as const) {
      await f.persist(await f.produce({ commandId: sourceId, binding }));
      await vi.waitFor(() => expect(f.acks(mapped(sourceId))).toHaveLength(1));
      expect(f.acks(mapped(sourceId))[0]).toMatchObject({ to: natsLead.id, data: { accepted: false, error: expect.stringContaining("overrides") } });
    }
    expect(f.contexts).toHaveLength(0);
    expect(f.actors.ask).not.toHaveBeenCalled();
    expect(f.model).not.toHaveBeenCalled();
  });

  it("revalidates before cached replay after restart; forgery gets no cached result while authentic input does", async () => {
    const f = await fixture();
    await f.persist(await f.produce());
    await vi.waitFor(() => expect(f.acks(mapped())).toHaveLength(1));
    await f.forge({ data: { changed: "replay phishing" }, replyTo: otherNats.id });
    await f.barrier();
    expect(f.acks(mapped())).toHaveLength(1);
    expect(f.contexts).toHaveLength(1);
    await f.owner.close();
    f.start(createNativeActorIngress({ bindings: [f.binding] }));
    await vi.waitFor(() => expect(f.acks(mapped())).toHaveLength(2));
    await f.barrier();
    expect(f.acks(mapped()).every(event => event.to === natsLead.id)).toBe(true);
    expect(f.acks(mapped())[1]?.data).toMatchObject({ accepted: true, result: { data: f.reply.data } });
    expect(f.contexts).toHaveLength(1); // Saved outcome, not a second callback.
  });

  it("authenticates before owned-command access/cancel: a forged withdrawal cannot abort, own genuine cancel can", async () => {
    const f = await fixture(), held = gate();
    f.setOnAsk(async () => { await held.promise; return f.reply; });
    await f.persist(await f.produce());
    await vi.waitFor(() => expect(f.contexts).toHaveLength(1));
    const signal = f.contexts[0]!.signal;
    // Same commandId hits the owned-map branch without the ingress-before-owned fence.
    await f.forge({ commandId: mapped(), replyTo: otherNats.id });
    // A cancellation needs its OWN receipt, not the original ask's labels/ID.
    await f.forge({ operation: "cancel", commandId: mapped("unproved-cancel"), cancelCommandId: mapped(), replyTo: natsLead.id });
    await f.barrier();
    expect(signal.aborted).toBe(false);
    expect(f.admissionAttempts.some(event => body(event).commandId === mapped() && body(event).replyTo === otherNats.id)).toBe(true);
    expect(f.admissionAttempts.some(event => body(event).commandId === mapped("unproved-cancel"))).toBe(true);
    expect(f.acks(mapped())).toHaveLength(0);
    expect(f.contexts).toHaveLength(1);
    await f.persist(await f.produce({ operation: "cancel", commandId: "source-cancel", cancelCommandId: "source-ask" }));
    await f.barrier();
    expect(signal.aborted).toBe(true);
    expect(f.contexts).toHaveLength(1);
    held.open();
    await vi.waitFor(() => expect(f.acks(mapped())).toHaveLength(1));
  });

  it("drops unauthenticated stale-owner traffic without ACK; authentic stale input retains the ordinary refusal", async () => {
    const f = await fixture("successor-incarnation");
    await f.forge({ ownerIncarnation: INCARNATION });
    await f.barrier();
    expect(f.acks(mapped())).toHaveLength(0);
    expect(f.mesh.get(seenKey(mapped()), { fresh: true })).toBeUndefined();
    await f.persist(await f.produce());
    await vi.waitFor(() => expect(f.acks(mapped())).toHaveLength(1));
    expect(f.acks(mapped())[0]).toMatchObject({ to: natsLead.id, data: { accepted: false, errorCode: CONTROL_STALE_INCARNATION } });
    expect(f.mesh.get(seenKey(mapped()), { fresh: true })).toBeUndefined();
    expect(f.actors.ask).not.toHaveBeenCalled();
  });

  it("never hydrates payload authority lookalikes into a generic handler or its fifth argument", async () => {
    const f = await fixture(), id = "generic-untrusted-fields";
    await f.mesh.publish({ topic: "fabric.control.command", kind: "steer", from: fileLead, to: OWNER,
      data: { version: 1, commandId: id, targetId: "barrier", operation: "steer", replyTo: fileLead.id,
        ownerIncarnation: INCARNATION, message: "ordinary", requestedAt: Date.now(), deadlineAt: Date.now() + 60_000,
        nativeAdmission: {}, nativeContext: { sender: natsLead }, sourceMachine: POD_MACHINE, sessionId: "forged", sender: { id: "forged" } } });
    await vi.waitFor(() => expect(f.acks(id)).toHaveLength(1));
    const delivery = f.received.find(entry => entry.command.commandId === id)!;
    expect(delivery.admission).toBeUndefined();
    for (const key of ["nativeAdmission", "nativeContext", "sourceMachine", "sessionId"]) expect(delivery.command).not.toHaveProperty(key);
    expect(delivery.command.sender).not.toEqual({ id: "forged" });
  });

  it("does not accept a handle-shaped fifth argument as a private capability", async () => {
    const f = await fixture();
    expect(await f.provider.acceptControl(command(), natsLead, new AbortController().signal, "bridge", {} as NativeControlAdmission))
      .toMatchObject({ accepted: false, error: expect.stringContaining("admission") });
    expect(f.contexts).toHaveLength(0);
    expect(f.model).not.toHaveBeenCalled();
  });

  it("guards reserved/quarantined targets even without a configured callback and leaves ordinary controls working", async () => {
    const f = await fixture();
    await f.owner.close();
    f.start(createNativeActorIngress({ bindings: [], isNativeActor: id => id === ACTOR }));
    await f.forge();
    await f.barrier(); // A non-native steer still receives its ordinary ACK.
    expect(f.mesh.get(seenKey(mapped()), { fresh: true })).toBeUndefined();
    expect(f.acks(mapped())).toHaveLength(0);
    expect(f.actors.ask).not.toHaveBeenCalled();
  });
});

const command = (changes: Partial<FabricControlCommand> = {}): FabricControlCommand => ({
  version: 1, commandId: "adapter-command", targetId: ACTOR, operation: "ask", replyTo: natsLead.id,
  message: MESSAGE, data: { opaque: [1, { anything: true }] }, requestedAt: Date.now(), ...changes,
});

describe("native router/provider adapter ordering (stubs, not capability proof)", () => {
  it("rejects a direct native ask or copied JSON proof before enqueue/model preflight", async () => {
    const a = adapter();
    const forged = { ...command(), nativeAdmission: {}, nativeContext: { sourceMachine: POD_MACHINE } };
    expect(await a.router.acceptControl(forged, natsLead, undefined, "bridge")).toMatchObject({ accepted: false, error: expect.stringContaining("admission") });
    expect(a.actors.ask).not.toHaveBeenCalled();
    expect(a.model).not.toHaveBeenCalled();
  });

  it("retains ordinary model-message trimming, but refuses native tell/steer before activation binding", async () => {
    const a = adapter();
    for (const operation of ["steer", "followUp"] as const) {
      expect(await a.router.acceptControl(command({ operation }), natsLead, undefined, "bridge")).toMatchObject({ accepted: false });
    }
    expect(a.model).not.toHaveBeenCalled();
    a.actor.runner = "pi";
    a.actors.isNativeActor = () => false;
    const binding = { model: "ordinary/model", thinking: "high" as const };
    expect(await a.router.acceptControl(command({ binding }), natsLead)).toMatchObject({ accepted: true, result: a.reply });
    expect(a.actors.ask.mock.calls[0]?.[1]).toBe(MESSAGE.trim());
    expect(a.actors.ask.mock.calls[0]?.[4]).toMatchObject({ binding });
    expect(a.actors.ask.mock.calls[0]?.[4]).not.toHaveProperty("nativeAdmission");
  });

  const providerFixture = () => {
    const a = adapter();
    const manager = { ...a.manager, cwd: process.cwd(), config: DEFAULT_FABRIC_CONFIG.agents,
      resolveKernel: a.model, assertModelAllowed: a.model, resolveModelReasonPolicy: a.model };
    const global = { resolve: vi.fn(), toRequest: vi.fn<(...args: unknown[]) => unknown>(() => a.model()), create: a.model, update: a.model };
    type ProviderPorts = ConstructorParameters<typeof AgentsProvider>;
    const provider = new AgentsProvider(manager as unknown as ProviderPorts[0], a.actors as unknown as ProviderPorts[1],
      global as unknown as ProviderPorts[2], a.main as unknown as ProviderPorts[3], a.participants as unknown as ProviderPorts[4],
      undefined, {} as ProviderPorts[6]);
    const context = { cwd: process.cwd(), parentToolCallId: "native-test", nestedToolCallId: "native-nested",
      extensionContext: { modelRegistry: { getAvailable: a.model, find: a.model } }, update() {} } as unknown as FabricInvocationContext;
    return { ...a, provider, global, context };
  };

  it("public local native asks and model/thinking overrides never touch model preparation or enqueue", async () => {
    const f = providerFixture();
    for (const overrides of [{}, { model: "no-provider/no-model" }, { thinking: "high" }, { routeClass: "status-groom" }]) {
      await expect(f.provider.invoke("ask", { id: ACTOR, message: MESSAGE, ...overrides }, f.context)).rejects.toThrow(/Native/);
    }
    for (const action of ["setModel", "setThinking"]) {
      await expect(f.provider.invoke(action, { id: ACTOR, model: "no-provider/no-model", thinking: "high" }, f.context)).rejects.toThrow(/Native/);
    }
    expect(f.actors.ask).not.toHaveBeenCalled();
    expect(f.model).not.toHaveBeenCalled();
  });

  it("public create/import reject execution markers before reconstruction, including own undefined/non-enumerable", async () => {
    const f = providerFixture();
    for (const action of ["create", "createActor", "import"]) {
      for (const marker of [{ execution: { kind: "native", version: 1 } }, { execution: undefined }, { runner: "native" },
        Object.defineProperty({}, "execution", { value: undefined, enumerable: false })]) {
        const args = { id: "template", name: "template", instructions: "ordinary" };
        Object.defineProperties(args, Object.getOwnPropertyDescriptors(marker));
        await expect(f.provider.invoke(action, args, f.context)).rejects.toThrow(/native actors or execution markers/);
      }
    }
    expect(f.global.resolve).not.toHaveBeenCalled();
    expect(f.global.toRequest).not.toHaveBeenCalled();
    expect(f.model).not.toHaveBeenCalled();
  });

  it("import rejects a marked resolved template rather than stripping it through toRequest", async () => {
    const f = providerFixture();
    f.global.resolve.mockReturnValue({ id: "template", name: "template", instructions: "ordinary", runner: "pi", execution: { kind: "unknown" } });
    await expect(f.provider.invoke("import", { id: "template" }, f.context)).rejects.toThrow(/execution markers/);
    expect(f.global.toRequest).not.toHaveBeenCalled();
    expect(f.model).not.toHaveBeenCalled();
  });

  it("checks the materialized import before a spread could erase its non-enumerable marker", async () => {
    const f = providerFixture();
    f.global.resolve.mockReturnValue({ id: "template", name: "template", instructions: "ordinary", runner: "pi" });
    f.global.toRequest.mockReturnValue(Object.defineProperty({ name: "template", instructions: "ordinary" }, "execution", { value: undefined }));
    await expect(f.provider.invoke("import", { id: "template" }, f.context)).rejects.toThrow(/execution markers/);
    expect(f.global.toRequest).toHaveBeenCalledTimes(1);
    expect(f.model).not.toHaveBeenCalled();
  });
});
