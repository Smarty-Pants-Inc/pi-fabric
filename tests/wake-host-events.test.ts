import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { saveCompletion } from "../src/agents/completion-journal.js";
import type { AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { fabricWakeCause, registerFabricWakeCapture, type FabricWakeCause } from "../src/fabric-provenance.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { lifecycleSourceIdentity, type FabricLifecycleEvent, type FabricLifecycleSubscription } from "../src/lifecycle/types.js";
import { MainAgentController } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentDeliveryPrefix, residentRoot, type ResidentHostConfig, type ResidentDeliveryRecord } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { installInProcessResidentFence } from "./helpers/in-process-resident-fence.js";

const cleanups: Array<() => void | Promise<unknown>> = [];
beforeEach(() => installInProcessResidentFence(true)); // No subprocesses, workers, inference, or Git writes.
afterEach(async () => { for (const cleanup of cleanups.splice(0).reverse()) await cleanup(); });
const tempRoot = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "wake-host-events-"));
  cleanups.push(() => fs.rmSync(root, { recursive: true, force: true }));
  return root;
};

type Message = Parameters<ExtensionAPI["sendMessage"]>[0];
type Handler = (event: any, ctx: ExtensionContext) => unknown;
// Recording Pi adapter: delivery/transport/journal are real; only native hook dispatch is emulated.
const main = (root: string, sessionId = "root", idle = true, journal = path.join(root, `${sessionId}-followups.json`)) => {
  const handlers = new Map<string, Handler[]>();
  const sent: Array<{ message: Message; options: Parameters<ExtensionAPI["sendMessage"]>[1] }> = [];
  const entries: unknown[] = [];
  const ctx = { isIdle: () => idle, hasPendingMessages: () => false, signal: new AbortController().signal,
    sessionManager: { getSessionId: () => sessionId, getEntries: () => entries, isPersisted: () => false } } as unknown as ExtensionContext;
  const appendEntry = vi.fn((customType: string, data: unknown) => entries.push({ type: "custom", customType, data }));
  const pi = { hostCapabilities: { turnProvenance: 1 },
    on: (name: string, handler: Handler) => {
      handlers.set(name, [...(handlers.get(name) ?? []), handler]);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter(fn => fn !== handler));
    },
    sendMessage: (message: Message, options: Parameters<ExtensionAPI["sendMessage"]>[1]) => sent.push({ message, options }),
    sendUserMessage: vi.fn(), appendEntry,
  } as unknown as ExtensionAPI;
  const emit = async (name: string, event: unknown = {}) => {
    for (const handler of handlers.get(name) ?? []) await handler(event, ctx);
  };
  registerFabricWakeCapture(pi);
  const controller = new MainAgentController(pi, `session:${sessionId}`, true, root, sessionId);
  controller.attachFollowUpDrain(ctx, 120_000, journal);
  cleanups.push(() => controller.closeFollowUpDrain());
  const consume = async (index = sent.length - 1) => {
    const message = { ...sent[index]!.message, role: "custom" };
    await emit("turn_start");
    await emit("message_start", { message });
    await emit("context", { messages: [message] });
    await emit("context", { messages: [message] });
  };
  return { controller, pi, sent, appendEntry, consume, journal, emit };
};

const identity = (id: string): MeshIdentity => ({ id, name: id, kind: "main" });
const directory = async (root: string, mesh: MeshStore, from: MeshIdentity) => {
  const participants = new ParticipantDirectory(mesh, { enabled: true, identity: from, hostId: from.id, rootId: from.id,
    heartbeatMs: 60_000, leaseMs: 120_000 });
  participants.registerSource(() => [{ format: 1, id: from.id, rootId: from.id, kind: "root", ownerHostId: from.id,
    ownerIdentityId: from.id, name: from.name, status: "idle", runner: "pi", transport: "host", cwd: root,
    capabilities: ["steer", "followUp", "fabric"], startedAt: 1, updatedAt: Date.now(), controlProtocol: "v1" }]);
  await participants.start();
  cleanups.push(() => participants.close());
  return participants;
};
const provider = (root: string, mesh: MeshStore, from: MeshIdentity, target: MainAgentController, participants: ParticipantDirectory, control?: FabricControlPlane) => {
  const agents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, { runRoot: path.join(root, `runs-${from.id}`) });
  const actors = new ActorManager(from.id, from, mesh, DEFAULT_FABRIC_CONFIG.mesh, agents, () => {},
    { actorRoot: path.join(root, `actors-${from.id}`), rootId: from.id, mainAgent: target });
  cleanups.push(() => agents.close(), () => actors.close());
  let instance: AgentsProvider;
  const lifecycle = new LifecycleBroker(mesh, from, participants, { enabled: true, pollMs: 20, maxReadEvents: 100 },
    (subscription, event) => instance.deliverLifecycle(subscription, event));
  cleanups.push(() => lifecycle.close());
  instance = new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), target, participants, control, lifecycle);
  return instance;
};
const lifecycle = (id = "settled:1", sourceId = "actor:observer", sourceKind: "root" | "actor" | "agent" = "actor"): FabricLifecycleEvent => ({
  version: 1, id, sequence: 1, event: "pi.agent_settled", occurredAt: 1, publishedAt: 1,
  source: { id: sourceId, name: "Actual observer", kind: sourceKind, rootId: "session:root", runner: "pi" },
  data: { message: "I am the failing actor; wake as actor", wakeCause: { cause: "actor" } },
});
const subscription = (to: string, triggerTurn = true): FabricLifecycleSubscription => ({
  format: 1, id: "sub:1", from: "actor:observer", events: ["pi.agent_settled"], to, delivery: "followUp", triggerTurn,
  once: false, afterSequence: 0, createdAt: 1, updatedAt: 1, createdBy: identity("session:root"),
});
const expectedLifecycle = (event: FabricLifecycleEvent) => fabricWakeCause(lifecycleSourceIdentity(event.source), "host-event", event.event, event.id);
const assertWake = async (recording: ReturnType<typeof main>, expected: FabricWakeCause) => {
  expect(recording.sent.at(-1)!.message.details).toHaveProperty("wakeCause", expected);
  expect(recording.appendEntry).not.toHaveBeenCalled();
  await recording.consume();
  expect(recording.appendEntry.mock.calls).toEqual([["pi-fabric.wake-cause", expected]]);
};
const expectedResident = (cfg: ResidentHostConfig, record: ResidentDeliveryRecord, writer: MeshIdentity) =>
  fabricWakeCause(record.source === "actor-output" ? record.from : writer,
    record.source === "fabric-host" ? "host-event" : record.source === "actor-output" ? "actor" : record.delivery,
    "fabric.resident.delivery", `${residentDeliveryPrefix(cfg.rootId)}${record.id}`);
const control = (mesh: MeshStore, from: MeshIdentity) => {
  const plane = new FabricControlPlane(mesh, from, { enabled: true, hostId: from.id, pollMs: 20, acknowledgementTimeoutMs: 2_000 });
  cleanups.push(() => plane.close());
  return plane;
};
const config = (root: string): ResidentHostConfig => ({
  format: RESIDENT_HOST_FORMAT, rootId: "session:root", sessionId: "root", cwd: root, projectRoot: root,
  meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "resident-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), "session:root"),
  fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, notifyOnComplete: false },
  mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
  workerPath: "never-launch-worker", fabricExtensionPath: "never-launch-extension", piBinary: "never-launch-pi", claudeBinary: "never-launch-claude", vedaBinary: "never-launch-veda",
});
const resident = async (root: string) => {
  const cfg = config(root);
  fs.mkdirSync(cfg.residencyRoot, { recursive: true });
  fs.writeFileSync(path.join(cfg.residencyRoot, "config.json"), JSON.stringify(cfg));
  const host = new ResidentHost(cfg, () => {});
  await host.start();
  cleanups.push(() => host.close());
  return { cfg, host };
};
const drain = async (cfg: ResidentHostConfig, mesh: MeshStore, participants: ParticipantDirectory, target: MainAgentController) => {
  const client = new ResidencyClient({ config: cfg, mesh, participants, mainAgent: target });
  cleanups.push(() => client.close());
  client.start();
  await vi.waitFor(() => expect(mesh.listAll(residentDeliveryPrefix(cfg.rootId), { fresh: true })).toHaveLength(0));
  await client.close();
};

describe("host-event attribution through lifecycle routing and durable resident delivery", () => {
  it("local lifecycle -> provider -> Main -> wake capture uses event source/topic/key, not payload text", async () => {
    const root = tempRoot();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const recording = main(root);
    const participants = await directory(root, mesh, identity("session:root"));
    const agents = provider(root, mesh, identity("session:root"), recording.controller, participants);
    const event = lifecycle();
    await agents.deliverLifecycle(subscription(recording.controller.id), event);
    await agents.flushLifecycleDeliveries();
    expect(recording.sent).toHaveLength(1);
    expect(recording.sent[0]!.options).toMatchObject({ triggerTurn: true,
      provenance: { sender: { id: event.source.id, kind: "actor", verified: "mesh" } } });
    await assertWake(recording, expectedLifecycle(event));
  });

  it("coalesced local lifecycle retains both event causes without changing batching", async () => {
    const root = tempRoot();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const recording = main(root);
    const participants = await directory(root, mesh, identity("session:root"));
    const agents = provider(root, mesh, identity("session:root"), recording.controller, participants);
    const first = lifecycle("settled:first");
    const last = { ...lifecycle("turn:last"), event: "pi.turn_end" as const };
    await agents.deliverLifecycle(subscription(recording.controller.id), first);
    await agents.deliverLifecycle(subscription(recording.controller.id), last);
    await agents.flushLifecycleDeliveries();
    expect(recording.sent).toHaveLength(1);
    expect(recording.sent[0]!.message.details).toMatchObject({ data: [first, last] });
    const causes = [first, last].map(event => expectedLifecycle(event));
    expect(recording.sent[0]!.message.details).toHaveProperty("wakeCauses", causes);
    await recording.consume();
    expect(recording.appendEntry.mock.calls).toEqual([["pi-fabric.wake-diagnostic", { cause: "multiple", exact: false,
      causes: causes.map(cause => ({ ...cause, exact: true })),
    }]]);
  });

  it("passive local lifecycle never stamps or records a diagnostic wake", async () => {
    const root = tempRoot();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const recording = main(root);
    const participants = await directory(root, mesh, identity("session:root"));
    const agents = provider(root, mesh, identity("session:root"), recording.controller, participants);
    await agents.deliverLifecycle(subscription(recording.controller.id, false), lifecycle());
    await agents.flushLifecycleDeliveries();
    expect(recording.sent[0]!.options).toMatchObject({ triggerTurn: false });
    expect(recording.sent[0]!.message.details).not.toHaveProperty("wakeCause");
    await recording.consume();
    expect(recording.appendEntry).not.toHaveBeenCalled();
  });

  it("remote lifecycle -> control admission -> Main replay names the authenticated command sender, not observed payload source", async () => {
    const root = tempRoot();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const local = main(root);
    const remote = main(root, "peer", false);
    const localIdentity = identity("session:root");
    const remoteIdentity = identity("session:peer");
    const localParticipants = await directory(root, mesh, localIdentity);
    const remoteParticipants = await directory(root, mesh, remoteIdentity);
    const sender = control(mesh, localIdentity);
    const receiver = control(mesh, remoteIdentity);
    const sourceProvider = provider(root, mesh, localIdentity, local.controller, localParticipants, sender);
    const ownerProvider = provider(root, mesh, remoteIdentity, remote.controller, remoteParticipants, receiver);
    const deliver = vi.spyOn(remote.controller, "deliverAgent");
    sender.start(() => ({ accepted: false }));
    receiver.start((command, from, signal, verification) => ownerProvider.acceptControl(command, from, signal, verification));
    const event = lifecycle("remote:settled", "session:actual-observer", "root");
    await sourceProvider.deliverLifecycle(subscription(remote.controller.id), event);
    await sourceProvider.flushLifecycleDeliveries();
    const command = mesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1)!;
    expect(command.from.id).toBe(localIdentity.id); // Principal/provenance are still command-envelope based.
    expect(command.data).not.toHaveProperty("wakeCause");
    expect(deliver).toHaveBeenCalledOnce();
    expect(deliver.mock.calls[0]![0]).not.toHaveProperty("admissionTopic");
    expect(mesh.read({ topic: "fabric.control.ack", limit: 100 }).at(-1)!.data).not.toHaveProperty("admissionTopic");
    const expected = fabricWakeCause(localIdentity, "followUp", "fabric.control.command", command.id);
    expect(remote.sent).toHaveLength(0);
    expect(JSON.parse(fs.readFileSync(remote.journal, "utf8")).items[0]).toMatchObject({
      from: localIdentity, wakeCause: expected, provenance: { sender: { id: localIdentity.id, verified: "mesh" } },
    });
    remote.controller.closeFollowUpDrain();
    const replay = main(root, "peer", true, remote.journal);
    expect(replay.sent).toHaveLength(1);
    expect(replay.sent[0]!.options).toMatchObject({ provenance: { via: "replay", sender: { id: localIdentity.id, verified: "mesh" } } });
    await assertWake(replay, expected);
  });

  it("malformed remote diagnostics do not reject authority-valid commands or fabricate a producer", async () => {
    const root = tempRoot();
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const recording = main(root);
    const owner = identity("session:root");
    const senderIdentity = identity("session:sender");
    const participants = await directory(root, mesh, owner);
    await directory(root, mesh, senderIdentity);
    const receiver = control(mesh, owner);
    const sender = control(mesh, senderIdentity);
    const agents = provider(root, mesh, owner, recording.controller, participants, receiver);
    receiver.start((command, from, signal, verification) => agents.acceptControl(command, from, signal, verification));
    sender.start(() => ({ accepted: false }));
    const malformed = { cause: "host-event", from: { id: "fake", kind: "invalid" } } as unknown as FabricWakeCause;
    await expect(sender.request(owner.id, owner.id, "followUp", { message: "normal input", wakeCause: malformed })).resolves.toMatchObject({ acknowledged: true });
    await assertWake(recording, fabricWakeCause(senderIdentity, "followUp", "fabric.control.command",
      mesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1)!.id));
    expect(recording.sent[0]!.options).toMatchObject({ provenance: { sender: { id: senderIdentity.id, verified: "mesh" } } });
    recording.appendEntry.mockClear();
    // A legacy/foreign writer bypasses the sender's diagnostic sanitizer. The reader
    // must still admit the command under its original envelope authority.
    const forged = await mesh.publish({ topic: "fabric.control.command", kind: "followUp", from: senderIdentity, to: owner.id,
      data: { version: 1, commandId: "raw:malformed-diagnosis", targetId: owner.id, operation: "followUp", replyTo: senderIdentity.id,
        requestedAt: Date.now(), deadlineAt: Date.now() + 2_000, message: "raw normal input", wakeCause: malformed } });
    await vi.waitFor(() => expect(recording.sent).toHaveLength(2));
    expect(recording.sent[1]!.options).toMatchObject({ provenance: { sender: { id: senderIdentity.id, verified: "mesh" } } });
    await assertWake(recording, fabricWakeCause(senderIdentity, "followUp", "fabric.control.command", forged.id));
  });

  it("resident lifecycle replay is unattributed when the original writer differs from the journalled sender", async () => {
    const root = tempRoot();
    const { cfg, host } = await resident(root);
    const participants = await directory(root, host.mesh, identity(cfg.rootId));
    const recording = main(root, "root", false);
    const event = lifecycle("resident:settled", "agent:actual-producer", "agent");
    await host.lifecycle.deliver(subscription(cfg.rootId), event);
    const record = host.mesh.listAll(residentDeliveryPrefix(cfg.rootId), { fresh: true })[0]!.value as ResidentDeliveryRecord;
    expect(record.wakeCause).toEqual(expectedLifecycle(event));
    await drain(cfg, host.mesh, participants, recording.controller);
    const held = JSON.parse(fs.readFileSync(recording.journal, "utf8")).items[0];
    const expected = expectedResident(cfg, record, host.identity);
    expect(held).toMatchObject({ from: lifecycleSourceIdentity(event.source), wakeCause: expected });
    recording.controller.closeFollowUpDrain();
    const replay = main(root, "root", true, recording.journal);
    // Live admission knew the storage writer, but the durable sender/provenance
    // identifies the observed producer. A serialized wakeCause cannot repair that.
    expect(replay.sent[0]!.message.details).not.toHaveProperty("wakeCause");
    await replay.consume();
    expect(replay.appendEntry.mock.calls).toEqual([["pi-fabric.wake-diagnostic", { cause: "unattributed" }]]);
  });

  it("resident lifecycle -> remote control names real command sender; passive remote records no wake", async () => {
    const root = tempRoot();
    const { cfg, host } = await resident(root);
    const remote = main(root, "peer");
    const remoteIdentity = identity(remote.controller.id);
    const participants = await directory(root, host.mesh, remoteIdentity);
    const receiver = control(host.mesh, remoteIdentity);
    const agents = provider(root, host.mesh, remoteIdentity, remote.controller, participants, receiver);
    receiver.start((command, from, signal, verification) => agents.acceptControl(command, from, signal, verification));
    const event = lifecycle("resident:remote", "session:observer", "root");
    await host.lifecycle.deliver(subscription(remote.controller.id), event);
    expect(host.mesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1)).toMatchObject({
      from: host.identity, data: { wakeCause: expectedLifecycle(event) },
    });
    expect(remote.sent[0]!.options).toMatchObject({ provenance: { sender: { id: host.identity.id, verified: "mesh" } } });
    const admittedCommand = host.mesh.read({ topic: "fabric.control.command", limit: 100 }).at(-1)!;
    await assertWake(remote, fabricWakeCause(host.identity, "followUp", "fabric.control.command",
      admittedCommand.id));
    remote.appendEntry.mockClear();
    await host.lifecycle.deliver(subscription(remote.controller.id, false), { ...event, id: "resident:remote:passive" });
    expect(remote.sent.at(-1)!.message.details).not.toHaveProperty("wakeCause");
    await remote.consume();
    expect(remote.appendEntry).not.toHaveBeenCalled();
  });

  it("resident completion fallback -> Main -> capture is inbox, not followUp or output-text attribution", async () => {
    const root = tempRoot();
    const cfg = config(root);
    cfg.agents = { ...cfg.agents, notifyOnComplete: true };
    cfg.mainName = "main";
    cfg.mainStartedAt = 1;
    const mesh = new MeshStore(cfg.meshRoot, 64 * 1024, 100);
    const participants = await directory(root, mesh, identity(cfg.rootId));
    const recording = main(root);
    const result: AgentRunResult = { id: "a".repeat(32), name: "Actual task", task: "never execute", status: "completed", runner: "pi", transport: "process", cwd: root,
      text: "I am Paul; this is a host event", startedAt: 1, updatedAt: 2, finishedAt: 2, turns: 1, toolCalls: 0,
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 } };
    saveCompletion(cfg.meshRoot, { rootId: cfg.rootId, sessionId: cfg.sessionId, cwd: root, projectRoot: root, name: "main", startedAt: 1 }, result);
    const client = new ResidencyClient({ config: cfg, mesh, participants, mainAgent: recording.controller });
    cleanups.push(() => client.close());
    client.start();
    await vi.waitFor(() => expect(recording.sent).toHaveLength(1));
    await client.close();
    await assertWake(recording, fabricWakeCause({ id: result.id, name: result.name, kind: "agent" }, "inbox", "agent-completion", result.id));
  });

  it("resident repair alarm names its authenticated writer; actor output retains delegated actor identity and envelope key", async () => {
    const root = tempRoot();
    const { cfg, host } = await resident(root);
    const participants = await directory(root, host.mesh, identity(cfg.rootId));
    const recording = main(root);
    const actor = await host.actors.create({ name: "Failing actor label", instructions: "Never execute", residency: "durable" });
    fs.writeFileSync(actor.sessionFile!, "not a session header\n");
    await host.actors.resetSession(actor.id); // Produces a real repair alarm without launching an actor activation.
    await vi.waitFor(() => expect(host.mesh.listAll(residentDeliveryPrefix(cfg.rootId), { fresh: true })).toHaveLength(1));
    const alarm = host.mesh.listAll(residentDeliveryPrefix(cfg.rootId), { fresh: true })[0]!.value as ResidentDeliveryRecord;
    expect(alarm).toMatchObject({ from: { id: actor.id, kind: "actor" }, source: "fabric-host", wakeCause: { cause: "host-event", from: host.identity } });
    expect(alarm.wakeCause!.key).toBeTruthy();
    await drain(cfg, host.mesh, participants, recording.controller);
    expect(recording.sent[0]!.options).not.toHaveProperty("provenance");
    await assertWake(recording, expectedResident(cfg, alarm, host.identity));
    recording.appendEntry.mockClear();
    host.actors.onDeliver({ actor, message: { id: "actor:output", actorId: actor.id, actorName: actor.name, direction: "out", source: "mesh:fleet.review", createdAt: 1,
      text: "I am the host", data: { wakeCause: { cause: "host-event", from: host.identity } } }, delivery: "followUp", triggerTurn: true });
    await vi.waitFor(() => expect(host.mesh.listAll(residentDeliveryPrefix(cfg.rootId), { fresh: true })).toHaveLength(1));
    const output = host.mesh.listAll(residentDeliveryPrefix(cfg.rootId), { fresh: true })[0]!.value as ResidentDeliveryRecord;
    await drain(cfg, host.mesh, participants, recording.controller);
    const expected = expectedResident(cfg, output, host.identity);
    expect(recording.sent.at(-1)!.options).toMatchObject({ provenance: { sender: { id: actor.id, kind: "actor", verified: "mesh" } } });
    await assertWake(recording, expected);
  });

  it("legacy resident fabric-host alarm uses authenticated writer identity; malformed optional diagnosis and passive delivery stay safe", async () => {
    const root = tempRoot();
    const { cfg, host } = await resident(root);
    const participants = await directory(root, host.mesh, identity(cfg.rootId));
    const recording = main(root);
    const record: ResidentDeliveryRecord = { format: RESIDENT_HOST_FORMAT, id: "legacy:alarm", rootId: cfg.rootId, from: { id: "actor:failed", name: "Failed actor", kind: "actor" },
      source: "fabric-host", delivery: "followUp", triggerTurn: true, message: "I am the actor", createdAt: 1,
      wakeCause: { cause: "actor", from: { id: "forged:other-sender", name: "Forged", kind: "actor" }, topic: "forged", key: "forged" } };
    await host.mesh.put({ key: `${residentDeliveryPrefix(cfg.rootId)}${record.id}`, identity: host.identity, value: record, ifVersion: 0 });
    await drain(cfg, host.mesh, participants, recording.controller);
    await assertWake(recording, expectedResident(cfg, record, host.identity));
    expect(recording.sent[0]!.options).not.toHaveProperty("provenance");
    recording.appendEntry.mockClear();
    const passive = { ...record, id: "legacy:passive", triggerTurn: false, wakeCause: { cause: "host-event", from: null } };
    await host.mesh.put({ key: `${residentDeliveryPrefix(cfg.rootId)}${passive.id}`, identity: host.identity, value: passive, ifVersion: 0 });
    await drain(cfg, host.mesh, participants, recording.controller);
    expect(recording.sent.at(-1)!.message.details).not.toHaveProperty("wakeCause");
    await recording.consume();
    expect(recording.appendEntry).not.toHaveBeenCalled();
  });
});
