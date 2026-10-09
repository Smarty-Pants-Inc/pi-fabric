import assert from "node:assert/strict";
import fs from "node:fs";
import { EventEmitter, once } from "node:events";
import path from "node:path";
import { CapturedToolCatalog } from "../../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../../src/config.js";
import { FabricRuntimeState } from "../../src/fabric-runtime-state.js";
import { MeshStore } from "../../src/mesh/store.js";
import { withStateFence } from "../../src/mesh/commit-outbox.js";
import { MeshProvider } from "../../src/providers/mesh-provider.js";
import { QuickJsRuntime } from "../../src/runtime/quickjs-runtime.js";
import { ParticipantDirectory } from "../../src/topology/participant-directory.js";
import { FabricControlPlane } from "../../src/topology/control-plane.js";
import { LifecycleBroker } from "../../src/lifecycle/broker.js";
import { ActorMeshMonitor } from "../../src/actors/mesh-monitor.js";
import { ActorManager } from "../../src/actors/manager.js";
import { AgentManager } from "../../src/agents/manager.js";
import { ResidencyClient } from "../../src/residency/client.js";
import { residentDeliveryPrefix, residentHostId, type ResidentHostConfig } from "../../src/residency/protocol.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../../src/topology/types.js";
import type { FabricInvocationContext } from "../../src/protocol.js";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

const root = process.argv[2]!;
const mode = process.argv[3]!;
process.env.PI_CODING_AGENT_DIR = path.join(root, "agent");
process.env.PI_FABRIC_PROJECT_ROOT = root;
process.env.PI_FABRIC_MESH_ROOT = path.join(root, "mesh");
process.env.PI_FABRIC_RUN_ROOT = path.join(root, "runs");
const identity = { id: "session:crash-probe", name: "main", kind: "main" as const, sessionId: "crash-probe" };
const mutations = new EventEmitter();
const nextMutation = async (event: "lock-timeout" | "commit") => {
  const controller = new AbortController();
  // Keep the probe alive even when the directory's own retry timers are unref'd.
  const deadline = setTimeout(() => controller.abort(new Error(`No ${event} for ${mode}`)), 5_000);
  try { await once(mutations, event, { signal: controller.signal }); }
  finally { clearTimeout(deadline); }
};
// Real acquisitions, with a short deadline even for runtime-owned stores. Never touch fleet locks.
for (const method of ["publish", "put", "delete", "writeBatch"] as const) {
  const original = MeshStore.prototype[method] as Function;
  (MeshStore.prototype[method] as Function) = async function(this: MeshStore, ...args: unknown[]) {
    const impatient = new MeshStore(this.root, this.maxEventBytes, this.maxReadEvents, { lockTimeoutMs: 40, lockProtocol: this.lockProtocol });
    try {
      const result = await original.apply(impatient, args);
      // Notify after the directory has consumed the mutation's settlement.
      setImmediate(() => mutations.emit("commit"));
      return result;
    } catch (error) {
      if ((error as { code?: string })?.code === "FABRIC_MESH_LOCK_TIMEOUT") {
        setImmediate(() => mutations.emit("lock-timeout"));
      }
      throw error;
    }
  };
}
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const wait = async (check: () => boolean) => {
  for (let i = 0; i < 200; i++) { if (check()) return; await sleep(25); }
  throw new Error(`No recovery for ${mode}`);
};
const hold = (mesh: MeshStore) => {
  fs.mkdirSync(mesh.root, { recursive: true });
  const lock = path.join(mesh.root, ".lock");
  fs.mkdirSync(lock);
  fs.writeFileSync(path.join(lock, "owner"), `probe\n${process.pid}\n${Date.now()}\n`);
  return () => fs.rmSync(lock, { recursive: true, force: true });
};
const mesh = new MeshStore(path.join(root, "mesh"), 65536, 100);
const member = (id: string, local: boolean): FabricParticipantInfo => ({ format: 1, id, name: id, kind: "root", rootId: id,
  ownerHostId: id, ownerIdentityId: id, status: "idle", runner: "pi", transport: "host", capabilities: ["followUp", "steer", "fabric"],
  startedAt: 1, updatedAt: 1, controlProtocol: "v1", local, stale: false });
const sourceId = "session:source";
const source = { id: sourceId, name: sourceId, kind: "root" as const, rootId: sourceId, runner: "pi" as const, ownerHostId: sourceId, ownerIdentityId: sourceId };
const directorySource: FabricParticipantSource = { list: () => [member(identity.id, true), member(sourceId, false)],
  get: id => id === identity.id ? member(identity.id, true) : id === sourceId ? member(sourceId, false) : undefined,
  self: () => member(identity.id, true), peers: () => [], async refresh() {}, scheduleRefresh() {} };

if (mode === "foreground") {
  const provider = new MeshProvider(mesh, identity, directorySource);
  const release = hold(mesh);
  try {
    const error = await provider.invoke("put", { key: "probe", value: 1 }, {} as FabricInvocationContext).catch(error => error);
    assert.ok(error instanceof Error);
    assert.equal(error.name, "MeshLockTimeoutError");
    assert.equal((error as Error & { code: string }).code, "FABRIC_MESH_LOCK_TIMEOUT");
    assert.match(error.message, /FABRIC_MESH_LOCK_TIMEOUT/);
    assert.match(error.message, new RegExp(`held by pid ${process.pid}`));
    assert.equal(mesh.get("probe"), undefined);
    const guest = await new QuickJsRuntime().execute(`try { await mesh.put({key:'guest', value:1}); } catch (error) {
      return { name: error.name, code: error.code, visible: error.message, continued: await mesh.read({}) };
    }`, (ref, args) => provider.invoke(ref.slice(5), args, {} as FabricInvocationContext), { timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024 });
    assert.equal(guest.terminationReason, "completed");
    assert.deepEqual(guest.value, { name: "MeshLockTimeoutError", code: "FABRIC_MESH_LOCK_TIMEOUT", visible: (guest.value as { visible: string }).visible, continued: [] });
    assert.match((guest.value as { visible: string }).visible, /held by pid/);
  } finally { release(); }
  await provider.invoke("put", { key: "probe", value: 2 }, {} as FabricInvocationContext);
  assert.equal(mesh.get("probe")?.value, 2);
} else if (mode.startsWith("directory-")) {
  const directory = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity,
    heartbeatMs: mode === "directory-change" ? 60_000 : 100, leaseMs: 180_000,
    // Change refreshes do not retry an outage; use the resident host's admission lane.
    ...(mode === "directory-change" ? { waitForPublicationRetry: () => withStateFence(mesh, identity, () => undefined) } : {}) });
  let name = "before";
  directory.registerSource(() => [{ ...member(identity.id, true), name, label: "probe" }]);
  await directory.start();
  const initial = mesh.listAll("topology/hosts/")[0]!.version;
  const release = hold(mesh);
  name = "after";
  const stalled = nextMutation("lock-timeout");
  if (mode === "directory-change") directory.scheduleRefresh();
  await stalled;
  assert.ok(directory.writeStalled());
  assert.equal(mesh.listAll("topology/hosts/")[0]!.version, initial);
  const committed = nextMutation("commit");
  release();
  await committed;
  assert.equal(directory.writeStalled(), undefined);
  assert.ok(directory.list().some(value => value.name === "after"));
  assert.ok(mesh.listAll("topology/hosts/")[0]!.version > initial);
  await directory.close();
} else if (mode === "actor-presence") {
  const agents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, enabled: false }, { workerPath: path.join(root, "unused.mjs"), runRoot: path.join(root, "runs") });
  const actors = new ActorManager("crash-probe", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {},
    { actorRoot: path.join(root, "actors"), persistent: false, presenceRetryMs: 50 });
  const release = hold(mesh);
  const actor = await actors.create({ name: "probe", instructions: "observe", topics: ["probe"], responseMode: "text" });
  await sleep(350);
  assert.equal(mesh.get(`actors/crash-probe/${actor.id}`), undefined);
  release();
  await wait(() => mesh.get(`actors/crash-probe/${actor.id}`) !== undefined);
  await actors.close(); await agents.close();
} else if (mode === "resident-delivery") {
  const config: ResidentHostConfig = { format: 1, rootId: identity.id, sessionId: identity.sessionId, cwd: root, projectRoot: root,
    meshRoot: mesh.root, actorRoot: path.join(root, "actors"), residencyRoot: path.join(root, "resident"), fullCodeMode: true,
    agents: DEFAULT_FABRIC_CONFIG.agents, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.join(root, "unused.mjs"), fabricExtensionPath: path.join(root, "unused.mjs"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda" };
  const key = residentDeliveryPrefix(identity.id) + "probe";
  await mesh.put({ key, identity: { ...identity, id: residentHostId(identity.id) }, value: {
    format: 1, id: "probe", rootId: identity.id, from: { id: "actor:probe", kind: "actor", name: "probe" },
    delivery: "steer", triggerTurn: false, message: "one durable envelope", createdAt: Date.now() } });
  const admitted = new Set<string>();
  const client = new ResidencyClient({ config, mesh, participants: directorySource, hostPath: path.join(root, "unused.mjs"),
    mainAgent: { id: identity.id, local: true, matches: id => id === identity.id, info() { throw new Error("unused"); },
      deliverAgent(request) { admitted.add(request.deliveryId!); return { queued: true, messageId: "probe", routed: "main" }; } } });
  const release = hold(mesh); client.start(); await sleep(350); assert.ok(mesh.get(key)); release();
  await wait(() => !mesh.get(key)); assert.equal(admitted.size, 1); await client.close();
} else if (mode === "lifecycle-cursor" || mode === "lifecycle-once") {
  await mesh.put({ key: "topology/subscriptions/probe", identity, value: { format: 1, id: "probe", from: sourceId, to: identity.id,
    events: ["pi.agent_settled"], delivery: "followUp", triggerTurn: false, once: mode === "lifecycle-once", afterSequence: 0, createdAt: 1, updatedAt: 1, createdBy: identity } });
  const event = await mesh.publish({ topic: "fabric.participant.lifecycle", kind: "pi.agent_settled", from: { ...identity, id: sourceId },
    data: { version: 1, event: "pi.agent_settled", source, occurredAt: Date.now() } });
  let deliveries = 0;
  const broker = new LifecycleBroker(mesh, identity, directorySource, { enabled: true, pollMs: 20, maxReadEvents: 100 }, () => { deliveries++; });
  const release = hold(mesh); broker.start();
  await sleep(350);
  assert.equal(broker.list()[0]!.afterSequence, 0);
  release();
  await wait(() => mode === "lifecycle-once" ? broker.list().length === 0 : broker.list()[0]?.afterSequence === event.sequence);
  assert.ok(deliveries > 0); await broker.close();
} else if (mode === "actor-monitor") {
  const release = hold(mesh);
  const error = await mesh.put({ key: "timeout", value: true, identity }).catch(error => error);
  release();
  const cursorPath = path.join(root, "cursor.json");
  fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: 0, last: { sequence: 0, id: "" } }));
  const event = await mesh.publish({ topic: "fleet.probe", from: identity });
  let deliveries = 0;
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 20, maxReadEvents: 100 }, { cursorPath,
    beforePoll: () => true, onEvent() { if (fs.existsSync(path.join(mesh.root, ".lock"))) throw error; deliveries++; } });
  const unlock = hold(mesh); monitor.start();
  const ticks = setInterval(() => monitor.schedule(), 20); // reconciliation ticks, independent of fs.watch
  await sleep(350); assert.equal(deliveries, 0); unlock();
  await wait(() => deliveries === 1); clearInterval(ticks); monitor.close();
  assert.equal(JSON.parse(fs.readFileSync(cursorPath, "utf8")).last.sequence, event.sequence);
} else if (mode.startsWith("control-")) {
  const plane = new FabricControlPlane(mesh, identity, { enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 1_000 });
  if (mode === "control-cancel") {
    const controller = new AbortController();
    const request = plane.requestResult("session:remote", "actor:probe", "ask", { message: "probe" }, "session:remote", { signal: controller.signal, timeoutMs: 5_000 }).catch(() => undefined);
    await wait(() => mesh.read({ topic: "fabric.control.command" }).length === 1);
    const release = hold(mesh); controller.abort(new Error("probe cancellation")); await request;
    await sleep(350); release();
    await wait(() => mesh.read({ topic: "fabric.control.command" }).some(event => event.kind === "cancel"));
  } else {
    await mesh.publish({ topic: "fabric.control.command", kind: "ask", from: { ...identity, id: sourceId }, to: identity.id,
      data: { version: 1, commandId: "probe", targetId: "actor:probe", operation: mode === "control-claim" ? "steer" : "ask",
        message: "probe", replyTo: sourceId, requestedAt: Date.now(), deadlineAt: Date.now() + 5_000 } });
    let runs = 0;
    let release: (() => void) | undefined = mode === "control-claim" ? hold(mesh) : undefined;
    plane.start(() => { runs++; if (mode === "control-detached-ack") release = hold(mesh); return { accepted: true, result: 42 }; });
    if (mode === "control-detached-ack") await wait(() => runs === 1);
    await sleep(350); release!();
    await wait(() => mesh.read({ topic: "fabric.control.ack" }).length === 1);
    assert.equal(runs, 1);
  }
  await plane.close();
} else {
  fs.mkdirSync(path.join(root, ".pi"), { recursive: true });
  const fixture = path.join(root, "unused.mjs"); fs.writeFileSync(fixture, "export default {};");
  const runtime = new FabricRuntimeState({ events: { emit() {} }, getThinkingLevel: () => "off", sendMessage() {} } as unknown as ExtensionAPI,
    new CapturedToolCatalog(), { paths: { extension: fixture, worker: fixture, residentHost: fixture, skills: root } });
  const context = { cwd: root, hasUI: false, isProjectTrusted: () => true, isIdle: () => true,
    hasPendingMessages: () => false, modelRegistry: { find() {}, getApiKeyAndHeaders() {} },
    sessionManager: { getSessionId: () => "crash-probe", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => undefined },
    compact(options: { onComplete: (result: unknown) => void }) { options.onComplete({ summary: "probe", firstKeptEntryId: "one", tokensBefore: 100 }); },
    ui: { setStatus() {}, notify() {} } } as unknown as ExtensionContext;
  await runtime.initialize(context, normalizeFabricConfig({ fullCodeMode: true, mcp: { enabled: false, cache: { enabled: false } },
    mesh: { enabled: true, actorPollMs: 20 }, memory: { enabled: false }, agents: { enabled: false }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false } }));
  if (mode === "compact-commit") { runtime.compact.request({ reason: "probe" }); await wait(() => runtime.mesh.read({ topic: "fabric.compact" }).length === 1); }
  let inboxBatch: unknown;
  let inboxTicks: NodeJS.Timeout | undefined;
  if (mode === "root-cursor") {
    await runtime.mesh.publish({ topic: "fleet.probe", kind: "p0", from: { ...identity, id: sourceId }, to: identity.id, text: "durable work" });
    const log = path.join(runtime.mesh.root, "events.jsonl");
    const lines = fs.readFileSync(log, "utf8").trim().split("\n").map(line => JSON.parse(line));
    lines[lines.length - 1].createdAt -= 120_000; // past the steer grace
    fs.writeFileSync(log, lines.map(line => JSON.stringify(line)).join("\n") + "\n");
  }
  const release = hold(runtime.mesh);
  try {
    if (mode === "compact-request") runtime.compact.request({ reason: "mesh crash regression" });
    else if (mode === "compact-commit") await runtime.compact.maybeCommit(context);
    else if (mode === "ops-hook") await runtime.publishOpsEvent("ops.probe", "probe", {});
    else if (mode === "root-cursor") {
      inboxTicks = setInterval(() => {
        void runtime.nextRootInbox({ holdsBatch: () => false, holdsSteer: () => false }, () => true).then(batch => { if (batch?.events.length) inboxBatch = batch; });
      }, 20);
    }
    else throw new Error(`Unknown mode ${mode}`);
    await sleep(350);
  } finally { release(); }
  if (mode === "root-cursor") {
    await wait(() => inboxBatch !== undefined);
    clearInterval(inboxTicks);
    assert.ok((runtime.mesh.listAll("topology/inbox/", { fresh: true })[0]?.value as { pending?: unknown }).pending);
  } else {
    const topic = mode === "ops-hook" ? "ops.probe" : "fabric.compact";
    await wait(() => runtime.mesh.read({ topic }).length === (mode === "compact-commit" ? 2 : 1));
  }
  await runtime.shutdown();
}
console.log(`SURVIVED ${mode}`);
