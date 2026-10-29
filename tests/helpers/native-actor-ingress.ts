// Keep: trust boundary + data safety. Actual carrier/gate/ledger/StoreBridgeSide/MeshStore
// produces the private admission. Presence is a test double, NOT a broker/native-owner proof.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { vi } from "vitest";
import { AgentManager } from "../../src/agents/manager.js";
import { ActorManager } from "../../src/actors/manager.js";
import type { NativeActorBinding, NativeActorHandler } from "../../src/actors/native-handler.js";
import { DEFAULT_FABRIC_CONFIG } from "../../src/config.js";
import { StoreBridgeSide, type BridgePublish } from "../../src/mesh/bridge.js";
import { NatsBridgeCarrier } from "../../src/mesh/bridge-nats-carrier.js";
import { createBridgeNatsIdentityGate, parseBridgeNatsAllowSection } from "../../src/mesh/bridge-nats-identity.js";
import { createNativeActorIngress } from "../../src/mesh/bridge-nats-ingress.js";
import { openBridgeNatsLedger } from "../../src/mesh/bridge-nats-ledger.js";
import { natsCarrierLedger, natsLinkAuthorizers } from "../../src/mesh/bridge-nats-link.js";
import type { BridgeNatsPresenceRecord, BridgeNatsPresenceView } from "../../src/mesh/bridge-nats-types.js";
import { MeshStore, type MeshIdentity } from "../../src/mesh/store.js";
import { residentHostId } from "../../src/residency/protocol.js";
import { BRIDGE_MACHINE, FakePresence, LINK, POD_MACHINE, fileLead, natsLead, route } from "./bridge-nats-l2-fakes.js";

export const NATIVE_ACTOR_ID = "34200000000000000000000000000001";
export const nativePosix = process.platform !== "win32" && !!fs.constants.O_NOFOLLOW && !!fs.constants.O_DIRECTORY && !!process.geteuid;
export const deferred = <T>() => {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};

export const createNativeActorFixture = async (options: {
  handler?: NativeActorHandler;
  ensure?: boolean;
  managerOptions?: ConstructorParameters<typeof ActorManager>[6];
} = {}) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "native-actor-core-"));
  const mesh = new MeshStore(path.join(root, "mesh"), 256 * 1024, 100);
  await mesh.prepareLock();
  const clock = { now: Date.now() };
  const ledger = await openBridgeNatsLedger({ dir: path.join(root, "ledger"), clock: () => clock.now });
  const binding: NativeActorBinding = { actorId: NATIVE_ACTOR_ID, rootId: fileLead.id, scope: "session", name: "native-inbox",
    handlerKey: "smarty-inbox.rpc.v1", residentModule: path.join(root, "application.mjs"), ingress: { link: LINK, ledgerDir: path.join(root, "ledger") } };
  const owner = residentHostId(binding.rootId);
  const identity: MeshIdentity = { id: owner, name: "resident-test", kind: "agent", sessionId: binding.rootId.slice(8) };
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  const handler = vi.fn<NativeActorHandler>(options.handler ?? (async request => ({ data: request.data, text: "native reply" })));
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, transport: "file" as const, actorScope: "session" as const, actorPollMs: 20 };
  const actorRoot = path.join(root, "actors");
  const managers: ActorManager[] = [];
  const deliveries: unknown[] = [];
  const managerOptions = { actorRoot, persistent: true, actorScope: "session" as const, claimResidency: "durable" as const,
    rootId: binding.rootId, nativeBindings: [binding], nativeHandlers: [{ binding, handler }], canConsumeMesh: () => true,
    closeGraceMs: 5, meshRetentionSweepPath: false as const, reapDeadSessionPresence: false as const };
  const openManager = (overrides: ConstructorParameters<typeof ActorManager>[6] = {}, managerIdentity = identity) => {
    const manager = new ActorManager(binding.rootId.slice(8), managerIdentity, mesh, meshConfig, agents,
      request => { deliveries.push(request); }, { ...managerOptions, ...overrides });
    managers.push(manager);
    return manager;
  };
  const actors = openManager(options.managerOptions);
  if (options.ensure !== false) await actors.ensureNativeActor(binding);
  const modelSpies = () => {
    const fail = (): never => { throw new Error("MODEL_PATH_MUST_NOT_RUN"); };
    return [vi.spyOn(agents, "defaultModel").mockImplementation(fail), vi.spyOn(agents, "resolveKernel").mockImplementation(fail),
      vi.spyOn(agents, "prepareModelWithReason").mockImplementation(fail), vi.spyOn(agents, "run").mockImplementation(fail)];
  };
  const presence = new FakePresence();
  presence.natsEntries.set(binding.actorId, route({ id: binding.actorId, name: binding.name, kind: "actor" }, BRIDGE_MACHINE,
    "native-owner-inc", { ownerHostId: owner, ownerIdentityId: owner }));
  const rootRecord: BridgeNatsPresenceRecord = { id: binding.rootId, rootId: binding.rootId, kind: "main", name: "owner-main",
    ownerHostId: binding.rootId, ownerIdentityId: binding.rootId, live: true, observedAt: clock.now };
  const actorRecord: BridgeNatsPresenceRecord = { id: binding.actorId, rootId: binding.rootId, kind: "actor", name: binding.name,
    ownerHostId: owner, ownerIdentityId: owner, ownerIncarnation: "native-owner-inc", live: true, observedAt: clock.now };
  const fileView = (): BridgeNatsPresenceView => ({ side: "file", records: id => [rootRecord, actorRecord]
    .filter(record => record.id === id).map(record => ({ ...record, observedAt: clock.now })) });
  const natsView = (): BridgeNatsPresenceView => ({ side: "nats", records: id => {
    const entry = presence.nats(id);
    return entry ? [{ id, name: entry.identity.name, kind: entry.identity.kind, rootId: id,
      ownerHostId: entry.route.ownerHostId, ownerIdentityId: entry.route.ownerIdentityId, ownerIncarnation: entry.route.ownerIncarnation,
      machine: entry.route.machine, live: entry.route.live, observedAt: clock.now }] : [];
  } });
  const allow = parseBridgeNatsAllowSection({
    file: [{ kind: "root", id: binding.rootId, send: [], receive: [] },
      { kind: "actor", rootId: binding.rootId, name: binding.name, id: binding.actorId, send: [], receive: ["ask", "cancel"] }],
    nats: [{ kind: "root", id: natsLead.id, machine: POD_MACHINE, send: ["ask", "cancel"], receive: [] }],
  }, { link: LINK, natsMachine: BRIDGE_MACHINE });
  const gate = createBridgeNatsIdentityGate(allow, { pins: { seats: new Map(), bound: new Map() }, clock: () => clock.now });
  const { authorize } = natsLinkAuthorizers(gate, { file: fileView, nats: natsView });
  const carrier = new NatsBridgeCarrier({ link: LINK, machine: BRIDGE_MACHINE, presence, ledger: natsCarrierLedger(ledger, LINK),
    authorize, now: () => clock.now });
  const file = new StoreBridgeSide(mesh, LINK, () => clock.now);
  const ingress = createNativeActorIngress({ bindings: [binding], isNativeActor: id => actors.isNativeActor(id), now: () => clock.now });
  const issue = async (message: string, data?: unknown, deadlineMs = 60_000) => {
    const sourceId = randomUUID();
    const plan = await carrier.inbound({ sourceMachine: POD_MACHINE, sequence: 1, envelope: {
      id: randomUUID(), kind: "command", to: { machine: BRIDGE_MACHINE, hostId: owner }, targetId: binding.actorId, operation: "ask", from: natsLead,
      body: { version: 1, commandId: sourceId, targetId: binding.actorId, operation: "ask", replyTo: natsLead.id,
        ownerIncarnation: "native-owner-inc", message, ...(data !== undefined ? { data } : {}),
        requestCreatedAt: clock.now, requestedAt: clock.now, deadlineAt: clock.now + deadlineMs },
    } });
    if (plan.kind !== "event") throw new Error(`Native test carrier refused: ${plan.kind}`);
    const raw = plan.event, body = raw.data as Record<string, unknown>;
    await file.publish({ topic: raw.topic, kind: raw.kind, from: raw.from, to: raw.to!,
      data: { ...body, bridge: { from: LINK, id: raw.id } } } as BridgePublish);
    const event = mesh.read({ after: 0, limit: 1000 }).find(event =>
      (event.data as Record<string, unknown> | undefined)?.commandId === body.commandId)!;
    if (!event) throw new Error("Native test final stored event missing");
    return { event, nativeAdmission: ingress.admitNativeControl(event) };
  };
  const ask = async (message: string, data?: unknown, signal?: AbortSignal, manager = actors) => {
    const proof = await issue(message, data);
    return manager.ask(binding.actorId, message, data, signal, { nativeAdmission: proof.nativeAdmission });
  };
  const rows = (): Array<Record<string, unknown>> => {
    try { return (JSON.parse(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8")) as { actors: Array<Record<string, unknown>> }).actors; }
    catch { return []; }
  };
  const writeRows = (rows: unknown[]) => fs.writeFileSync(path.join(actorRoot, "actors.json"), JSON.stringify({ format: 1, actors: rows }));
  const close = async () => {
    await Promise.all(managers.map(manager => manager.close()));
    await agents.close(); await ledger.close(); mesh.closeState();
    // This helper created and exclusively owns this directory.
    fs.rmSync(root, { recursive: true, force: true });
  };
  return { root, actorRoot, binding, identity, mesh, meshConfig, agents, actors, managers, managerOptions,
    openManager, handler, clock, ingress, issue, ask, modelSpies, deliveries, rows, writeRows, close };
};
export type NativeActorFixture = Awaited<ReturnType<typeof createNativeActorFixture>>;
