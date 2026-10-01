import * as childProcess from "node:child_process";
import { EventEmitter } from "node:events";
import { Duplex, PassThrough } from "node:stream";
import { createHash } from "node:crypto";

import fs from "node:fs";
import { deliveryRoot, projectOf } from "../src/topology/project-identity.js";
import os from "node:os";
import path from "node:path";
import { SessionManager, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import type { FabricActorDeliveryRequest, FabricActorInfo, FabricActorRequest } from "../src/actors/types.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type {
  FabricLifecycleEvent,
  FabricLifecycleSubscription,
} from "../src/lifecycle/types.js";
import {
  DEFAULT_FABRIC_CONFIG,
  type FabricAgentConfig,
  type FabricModelsConfig,
} from "../src/config.js";
import type {
  FabricMainAgentDeliveryRequest,
  FabricMainAgentTarget,
} from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshBridge, StoreBridgeSide } from "../src/mesh/bridge.js";
import type {
  FabricParticipantInfo,
  FabricParticipantSource,
  FabricPeerInfo,
} from "../src/topology/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentsProvider, collectAgentToolPreviewNodes } from "../src/providers/agents-provider.js";
import type { ResidencyClient } from "../src/residency/client.js";
import { ResidentOutcomeUnknownError } from "../src/residency/protocol.js";
import { snapshotHandoffSession } from "../src/agents/handoff.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import type { AgentHandleInfo } from "../src/agents/types.js";
import type { AgentRunRecord } from "../src/agents/types.js";
import { captureRuntimeDeadline } from "./helpers/early-runtime-deadline.js";
import { captureMontyTransport } from "./helpers/monty-transport.js";
import { executeAfterAdmission } from "./helpers/admission-clock.js";
import { createMainExecutionCeilingError } from "../src/async-settlement.js";

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

const roots: string[] = [];
const actorManagers: ActorManager[] = [];
const agentManagers: AgentManager[] = [];
const controlPlanes: FabricControlPlane[] = [];

const usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

const visiblePiModels = [
  { provider: "anthropic", id: "executor", name: "Executor" },
  { provider: "anthropic", id: "frontier", name: "Frontier" },
  { provider: "provider", id: "project" },
  { provider: "provider", id: "project-default" },
  { provider: "provider", id: "one-off" },
  { provider: "provider", id: "session" },
  { provider: "provider", id: "model-a" },
  { provider: "provider", id: "model-b" },
];

const visibleModelRegistry = {
  getAvailable: () => visiblePiModels,
  find: (provider: string, id: string) =>
    visiblePiModels.find((model) => model.provider === provider && model.id === id),
};

const context: FabricInvocationContext = {
  cwd: process.cwd(),
  signal: undefined,
  parentToolCallId: "test",
  nestedToolCallId: "nested",
  extensionContext: { modelRegistry: visibleModelRegistry } as unknown as ExtensionContext,
  update() {},
  activity() {},
};

const setup = (
  peers: FabricPeerInfo[] = [],
  members: FabricParticipantInfo[] = [],
  control?: FabricControlPlane,
  options?: {
    identity?: MeshIdentity;
    switchModel?: FabricMainAgentTarget["switchModel"];
    modelsConfig?: FabricModelsConfig;
    agentsConfig?: Partial<FabricAgentConfig>;
    workerPath?: string;
    writeStalled?: () => Error | undefined;
    onBackgroundComplete?: (result: import("../src/agents/types.js").AgentRunResult) => void;
    onResultConsumed?: (id: string) => void;
  },
) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-agents-provider-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(
    process.cwd(),
    { ...DEFAULT_FABRIC_CONFIG.agents, ...options?.agentsConfig },
    {
      workerPath: options?.workerPath ?? path.resolve("tests/fixtures/fake-worker.mjs"),
      claudeBinary: path.resolve("tests/fixtures/fake-claude.mjs"),
      vedaBinary: path.resolve("tests/fixtures/fake-veda.mjs"),
      runRoot: path.join(root, "runs"),
      ...(options?.onBackgroundComplete ? { onBackgroundComplete: options.onBackgroundComplete } : {}),
      ...(options?.onResultConsumed ? { onResultConsumed: options.onResultConsumed } : {}),
    },
  );
  agentManagers.push(agents);
  const identity: MeshIdentity = options?.identity ?? {
    id: "session:test",
    name: "main",
    kind: "main",
    sessionId: "test",
  };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const mainDeliveries: FabricMainAgentDeliveryRequest[] = [];
  const mainAgent = {
    id: identity.id,
    local: true,
    matches: (id: string) => id === "main" || id === identity.id,
    info: () => ({
      id: identity.id,
      name: "Main" as const,
      kind: "main" as const,
      status: "idle" as const,
      runner: "pi" as const,
      transport: "host" as const,
      cwd: process.cwd(),
      sessionId: "test",
      startedAt: 1,
      updatedAt: 1,
      pendingMessages: false,
      local: true,
    }),
    deliverAgent: (request: FabricMainAgentDeliveryRequest) => {
      mainDeliveries.push(request);
      return {
        queued: true as const,
        messageId: `main-message-${mainDeliveries.length}`,
        routed: "main" as const,
      };
    },
    ...(options?.switchModel ? { switchModel: options.switchModel } : {}),
    flushHeldAtNextBoundary: vi.fn(),
  };
  const actorDeliveries: FabricActorDeliveryRequest[] = [];
  const actors = new ActorManager("test", identity, mesh, meshConfig, agents, request => actorDeliveries.push(request), {
    actorRoot: path.join(root, "actors"),
    persistent: true,
    mainAgent,
  });
  actorManagers.push(actors);
  const globalActors = new GlobalActorRegistry(root, 64 * 1024);
  const participants: FabricParticipantSource = {
    list: (options = {}) =>
      members.filter(
        (participant) =>
          (!options.kinds || options.kinds.includes(participant.kind)) &&
          (options.scope !== "local" || participant.local) &&
          (options.scope !== "lineage" || participant.rootId === identity.id),
      ),
    get: (id) => members.find((participant) => participant.id === id),
    self: () => ({
      format: 1,
      id: identity.id,
      kind: "root",
      rootId: identity.id,
      ownerHostId: identity.id,
      ownerIdentityId: identity.id,
      name: "main",
      status: "idle",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "fabric"],
      cwd: process.cwd(),
      sessionId: "test",
      startedAt: 1,
      updatedAt: 1,
      pendingMessages: false,
      controlProtocol: "v1",
      local: true,
      stale: false,
    }),
    peers: () => peers,
    ...(options?.writeStalled ? { writeStalled: options.writeStalled } : {}),
    async refresh() {},
    scheduleRefresh() {},
  };
  let provider: AgentsProvider;
  const lifecycle = new LifecycleBroker(
    mesh,
    identity,
    participants,
    { enabled: true, pollMs: 20, maxReadEvents: 100 },
    async (subscription, event) => provider.deliverLifecycle(subscription, event),
  );
  provider = new AgentsProvider(
    agents,
    actors,
    globalActors,
    mainAgent,
    participants,
    control,
    lifecycle,
    undefined,
    undefined,
    undefined,
    () => options?.modelsConfig ?? DEFAULT_FABRIC_CONFIG.models,
  );
  return {
    root,
    mesh,
    identity,
    mainAgent,
    participants,
    control,
    lifecycle,
    actors,
    agents,
    globalActors,
    provider,
    mainDeliveries,
    actorDeliveries,
  };
};

describe("queued spawn handles (#2576)", () => {
  const fixture = (maxConcurrent = 2, maxPerExecution = 10) => {
    const state = setup([], [], undefined, {
      agentsConfig: { maxConcurrent, maxPerExecution, transport: "process", budgetUsd: 0 },
      workerPath: path.resolve("tests/fixtures/queued-worker.mjs"),
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.agents = state.agents.config;
    config.approvals.agent = "allow";
    config.approvals.read = "allow";
    config.executor.timeoutMs = 2_000;
    const registry = new ActionRegistry();
    registry.register(state.provider);
    registry.register({
      name: "probe", description: "Hold a calling program until its deadline",
      async list() { return [{ name: "hold", description: "Wait for caller abort", risk: "read" as const, inputSchema: { type: "object", properties: {} } }]; },
      async describe() { return (await this.list({}, context))[0]; },
      async invoke(_name, _args, invocation) {
        return new Promise((_resolve, reject) => {
          const abort = () => reject(new Error("calling program aborted"));
          invocation.signal?.addEventListener("abort", abort, { once: true });
          if (invocation.signal?.aborted) abort();
        });
      },
    });
    const service = new FabricExecutionService(registry, config);
    const run = (code: string) => service.execute({ code, signal: undefined,
      parentToolCallId: "queued-spawn-program", context: { ...context.extensionContext, cwd: process.cwd(), hasUI: false } as ExtensionContext,
      onPartial() {},
    });
    const gate = (index: number) => path.join(state.root, `release-${index}`);
    const request = (index: number) => ({ task: JSON.stringify({ gate: gate(index) }), name: `queued-${index}`, transport: "process", nice: 7 });
    const spawn = (index: number) => state.provider.invoke("spawn", request(index), context) as Promise<AgentHandleInfo>;
    const receipt = async (pending: Promise<AgentHandleInfo>) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      try {
        return await Promise.race([pending, new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error("spawn blocked on admission")), 1_000);
        })]);
      } finally { clearTimeout(timer); }
    };
    const release = (index: number) => fs.writeFileSync(gate(index), "go");
    return { ...state, run, spawn, receipt, release, request };
  };

  it("returns five handles from one program with two running and three queued, then completes all five", async () => {
    const state = fixture();
    const requests = Array.from({ length: 5 }, (_, i) => state.request(i));
    const result = await state.run(`return await Promise.all(${JSON.stringify(requests)}.map(request => agents.spawn(request)));`);
    expect(result.success, result.error).toBe(true);
    const handles = result.value as AgentHandleInfo[];
    expect(handles.map((handle) => handle.status)).toEqual(["running", "running", "queued", "queued", "queued"]);
    const listed = await state.provider.invoke("list", { scope: "local" }, context) as AgentHandleInfo[];
    expect(listed.map((handle) => handle.status)).toEqual(["running", "running", "queued", "queued", "queued"]);
    expect((await Promise.all(handles.slice(2).map((handle) => state.provider.invoke("status", { id: handle.id }, context))) as AgentHandleInfo[]).map((handle) => handle.queuePosition)).toEqual([1, 2, 3]);
    let finished = false;
    const waiting = (state.provider.invoke("join", { id: handles[4]!.id, timeoutMs: 5_000 }, context) as Promise<AgentRunRecord>).then((value) => { finished = true; return value; });
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(finished).toBe(false);
    for (let i = 0; i < 5; i++) state.release(i);
    expect((await waiting).status).toBe("completed");
    expect((await Promise.all(handles.map((handle) => state.agents.wait(handle.id)))).map((value) => value.status)).toEqual(Array(5).fill("completed"));
  });

  it("preserves queued spawns after the calling program's short deadline", async () => {
    const state = fixture(1);
    const requests = Array.from({ length: 3 }, (_, i) => state.request(i));
    const result = await state.run(`await Promise.all(${JSON.stringify(requests)}.map(request => agents.spawn(request))); return await tools.call({ref: "probe.hold", args: {}});`);
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/timed out/);
    const handles = state.agents.list();
    expect(handles).toHaveLength(3);
    expect(handles.map((handle) => handle.status)).toEqual(["running", "queued", "queued"]);
    for (let i = 0; i < 3; i++) state.release(i);
    expect((await Promise.all(handles.map((handle) => state.agents.wait(handle.id)))).map((value) => value.status)).toEqual(Array(3).fill("completed"));
  });

  it("cancels a queued agent without ever creating its process and updates queue positions", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const state = fixture(1);
    const first = await state.spawn(0);
    const cancelled = await state.receipt(state.spawn(1));
    const third = await state.receipt(state.spawn(2));
    expect(await state.provider.invoke("stop", { id: cancelled.id }, context)).toMatchObject({ status: "stopped" });
    expect(state.agents.status(third.id)).toMatchObject({ status: "queued", queuePosition: 1 });
    state.release(0); state.release(2);
    await Promise.all([state.agents.wait(first.id), state.agents.wait(third.id)]);
    expect(await state.agents.wait(cancelled.id)).toMatchObject({ status: "stopped" });
    expect(launch.mock.calls.map(([request]) => request.id)).toEqual([first.id, third.id]);
  });

  it("session shutdown cancels returned queued handles without launching them", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const state = fixture(1);
    await state.spawn(0);
    const queued = await state.receipt(state.spawn(1));
    await state.agents.close();
    expect(await state.agents.wait(queued.id)).toMatchObject({ status: "stopped" });
    expect(launch).toHaveBeenCalledTimes(1);
  });

  it("counts queued receipts against the per-execution budget and resets accounting for the next program", async () => {
    const state = fixture(1, 2);
    const requests = Array.from({ length: 3 }, (_, i) => state.request(i));
    const result = await state.run(`for (const request of ${JSON.stringify(requests)}) await agents.spawn(request);`);
    expect(result.success).toBe(false);
    expect(result.error).toContain("agent budget exhausted");
    expect(state.agents.list().map((handle) => handle.status)).toEqual(["running", "queued"]);
    const next = await state.run(`return await agents.spawn(${JSON.stringify(state.request(2))});`);
    expect(next.success, next.error).toBe(true);
    expect(next.value).toMatchObject({ status: "queued", queuePosition: 2 });
    const handles = state.agents.list();
    for (let i = 0; i < 3; i++) state.release(i);
    expect((await Promise.all(handles.map((handle) => state.agents.wait(handle.id)))).map((value) => value.status)).toEqual(Array(3).fill("completed"));
  });

  it("does not refund a cancelled queued receipt's execution budget", async () => {
    const state = fixture(1, 2);
    const result = await state.run(`
      await agents.spawn(${JSON.stringify(state.request(0))});
      const queued = await agents.spawn(${JSON.stringify(state.request(1))});
      await tools.call({ref: "agents.cancel", args: {id: queued.id}});
      return await agents.spawn(${JSON.stringify(state.request(2))});
    `);
    expect(result.success).toBe(false);
    expect(result.error).toContain("agent budget exhausted");
    expect(state.agents.list().map((handle) => handle.status)).toEqual(["running", "stopped"]);
  });

  it("admits queued workers in FIFO order while preserving concurrency and nice", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const state = fixture(1);
    const handles = [await state.spawn(0), await state.receipt(state.spawn(1)), await state.receipt(state.spawn(2))];
    for (let i = 0; i < 3; i++) {
      await vi.waitFor(() => expect(launch).toHaveBeenCalledTimes(i + 1), { timeout: 10_000 });
      expect(state.agents.list().filter((handle) => handle.status === "running")).toHaveLength(1);
      state.release(i);
      expect((await state.agents.wait(handles[i]!.id)).status).toBe("completed");
    }
    expect(launch.mock.calls.map(([request]) => request.id)).toEqual(handles.map((handle) => handle.id));
    for (const [request] of launch.mock.calls) {
      const index = request.workerArguments.indexOf("--nice");
      expect(request.workerArguments[index + 1]).toBe("7");
    }
  });
});

describe("#169 round 1 agents.remove cleanup routing", () => {
  it("discovers a resident cleanup-only marker without restart and routes its exact id using retained ownership", async () => {
    const state = setup();
    await state.actors.close();
    const actorRoots = { project: path.join(state.root, "project-actors"), session: path.join(state.root, "session-actors") };
    const owner = new ActorDirectory(["test", state.identity, state.mesh, DEFAULT_FABRIC_CONFIG.mesh, state.agents, () => {},
      { persistent: true, rootId: state.identity.id, claimResidency: "durable" }], actorRoots, "project");
    const passive = new ActorDirectory(["test", state.identity, state.mesh, DEFAULT_FABRIC_CONFIG.mesh, state.agents, () => {},
      { persistent: true, rootId: state.identity.id, canManageActor: () => false }], actorRoots, "project");
    actorManagers.push(owner, passive);
    const actor = await owner.create({ name: "resident obligation", instructions: "Work.", residency: "durable" });
    expect(passive.list().map((entry) => entry.id)).toContain(actor.id); // Main had the pre-removal view.
    const dir = path.join(actorRoots.project, actor.id);
    const rm = fs.rmSync.bind(fs);
    const failing = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (target === dir) throw new Error("cleanup unavailable");
      return rm(target, options);
    });
    try { await owner.remove(actor.id); } finally { failing.mockRestore(); }
    const removeActor = vi.fn((id: string) => owner.remove(id));
    const provider = new AgentsProvider(state.agents, passive, state.globalActors, state.mainAgent, state.participants,
      state.control, state.lifecycle, undefined, { removeActor } as unknown as ResidencyClient);
    await expect(provider.invoke("remove", { id: actor.id }, context)).resolves.toMatchObject({ removed: true });
    expect(removeActor).toHaveBeenCalledExactlyOnceWith(actor.id, context.signal);
    expect(fs.existsSync(dir)).toBe(false);
    expect(await provider.invoke("actors", {}, context)).not.toContainEqual(expect.objectContaining({ id: actor.id }));
  });

  it.each(["project", "session"] as const)("reports and retries a cleanup-only %s id through the provider, preserving its successor", async (scope) => {
    const state = setup();
    await state.actors.close();
    const actorRoots = { project: path.join(state.root, "project-actors"), session: path.join(state.root, "session-actors") };
    const directory = new ActorDirectory(["test", state.identity, state.mesh, DEFAULT_FABRIC_CONFIG.mesh, state.agents, () => {},
      { persistent: true, rootId: state.identity.id }], actorRoots, "project");
    actorManagers.push(directory);
    const provider = new AgentsProvider(state.agents, directory, state.globalActors, state.mainAgent, state.participants,
      state.control, state.lifecycle, undefined, undefined, undefined, () => DEFAULT_FABRIC_CONFIG.models);
    const actor = await directory.create({ scope, name: "retry worker", instructions: "Work." });
    const dir = path.join(actorRoots[scope], actor.id);
    const rm = fs.rmSync.bind(fs);
    const failing = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (target === dir) throw new Error("cleanup unavailable");
      return rm(target, options);
    });
    try {
      await expect(provider.invoke("remove", { id: actor.id }, context)).resolves.toMatchObject({ removed: true, cleaned: false });
      // No participant/presence entry remains. agents.actors must still expose the stopped obligation.
      expect(state.participants.get(actor.id)).toBeUndefined();
      expect(await provider.invoke("actors", {}, context)).toContainEqual(expect.objectContaining({
        id: actor.id, scope, status: "stopped", rootId: state.identity.id, removal: expect.objectContaining({ state: expect.stringContaining("cleanup failed") }),
      }));
      const successor = await directory.create({ scope, name: "retry worker", instructions: "Successor." });
      fs.writeFileSync(successor.sessionFile!, "successor history\n");
      await expect(provider.invoke("remove", { id: actor.id.slice(0, 12) }, context)).rejects.toThrow();
      failing.mockRestore();
      await expect(provider.invoke("remove", { id: actor.id }, context)).resolves.toMatchObject({ removed: true });
      expect(directory.pendingRemovals()).toEqual([]);
      expect(fs.existsSync(dir)).toBe(false);
      expect(fs.readFileSync(successor.sessionFile!, "utf8")).toBe("successor history\n");
      expect(directory.status("retry worker").id).toBe(successor.id);
      expect(await provider.invoke("actors", {}, context)).not.toContainEqual(expect.objectContaining({ id: actor.id }));
    } finally { failing.mockRestore(); }
  });
});
// smarty-dev#1439: agents.compact on an actor id pointed nowhere ("Unknown Fabric agent").
describe("Main remote ASK observation ownership", () => {
  it.each([
    ["native", false], ["native", true], ["mesh", false], ["mesh", true],
  ] as const)("preserves accepted %s owner work at the Main ceiling (queued=%s)", async (route, queued) => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const owner = setup([], [], undefined, { identity: { id: "session:remoteowner", name: "remote owner", kind: "main", sessionId: "remoteowner" } });
    const actor = await owner.actors.create({ name: "remote survivor", instructions: "Reply.", transport: "process", responseMode: "text", delivery: "followUp", triggerTurn: false });
    const ownerId = owner.identity.id;
    const senderIdentity: MeshIdentity = { id: "session:observer", name: "observer", kind: "main" };
    const senderMesh = route === "mesh" ? new MeshStore(path.join(owner.root, "observer-mesh"), 64 * 1024, 100) : owner.mesh;
    const ownerControl = new FabricControlPlane(owner.mesh, owner.identity, { enabled: true, hostId: ownerId, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    const senderControl = new FabricControlPlane(senderMesh, senderIdentity, {
      enabled: true, hostId: senderIdentity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000,
      ...(route === "mesh" ? { readMirroredOwner: () => ({ remoteHost: "remote-machine", expiresAt: Date.now() + 60_000 }) } : {}),
    });
    controlPlanes.push(ownerControl, senderControl);
    ownerControl.start((command, from, signal) => owner.provider.acceptControl(command, from, signal));
    senderControl.start(() => ({ accepted: false }));
    let bridge: MeshBridge | undefined;
    let pumping = true;
    let pump: Promise<void> | undefined;
    if (route === "mesh") {
      for (const [mesh, identity] of [[owner.mesh, owner.identity], [senderMesh, senderIdentity]] as const) {
        const now = Date.now();
        await mesh.put({ key: "topology/hosts/" + createHash("sha256").update(identity.id).digest("hex"), identity, value: { format: 1, id: identity.id, rootId: identity.id, identity, startedAt: now, updatedAt: now, expiresAt: now + 60_000 } });
        await mesh.put({ key: "topology/participants/" + createHash("sha256").update(identity.id).digest("hex"), identity, value: { format: 1, id: identity.id, rootId: identity.id, ownerHostId: identity.id, ownerIdentityId: identity.id, kind: "root", name: identity.name, sessionId: identity.id, startedAt: now, updatedAt: now } });
      }
      await owner.mesh.put({ key: "topology/participants/" + createHash("sha256").update(actor.id).digest("hex"), identity: owner.identity, value: { format: 1, id: actor.id, rootId: ownerId, ownerHostId: ownerId, ownerIdentityId: ownerId, kind: "actor", name: actor.name, startedAt: Date.now(), updatedAt: Date.now() } });
      bridge = new MeshBridge({ localName: "observer-machine", remoteName: "remote-machine", local: new StoreBridgeSide(senderMesh, "remote-machine"), remote: new StoreBridgeSide(owner.mesh, "observer-machine"), cursorPath: path.join(owner.root, "bridge.cursor.json"), presenceMs: 60_000 });
      await bridge.start(); await bridge.syncPresence();
      expect((bridge.options.local as StoreBridgeSide).holds(ownerId)).toBe(true);
      const connected = bridge;
      pump = (async () => { while (pumping) { await connected.step(); await new Promise(resolve => setTimeout(resolve, 20)); } })();
    }
    const member: FabricParticipantInfo = { format: 1, id: actor.id, name: actor.name, kind: "actor", rootId: owner.identity.id, ownerHostId: ownerId, ownerIdentityId: owner.identity.id, status: "idle", runner: "pi", transport: "host", capabilities: ["ask", "stop", "actor-bindings"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: false, stale: false, ...(route === "mesh" ? { remoteHost: "remote-machine" } : {}) };
    const sender = setup([], [member], senderControl);
    const stop = vi.spyOn(owner.agents, "stop");
    const run = vi.spyOn(owner.agents, "run");
    try {
      const first = queued ? owner.actors.ask(actor.id, "LIVE_WITHOUT_PROGRESS").catch(error => error) : undefined;
      if (queued) await waitFor(() => Boolean(owner.actors.status(actor.id).inFlightRun));
      const controller = new AbortController();
      const observation = sender.provider.invoke("ask", { id: actor.id, message: queued ? "accepted queued request" : "LIVE_WITHOUT_PROGRESS" }, {
        ...context, signal: controller.signal, extensionContext: { ...context.extensionContext, mode: "rpc", sessionManager: { getSessionId: () => "observer" } } as unknown as ExtensionContext,
      }).catch(error => error);
      await waitFor(() => queued ? owner.actors.status(actor.id).queued === 1 : Boolean(owner.actors.status(actor.id).inFlightRun));
      const ceiling = createMainExecutionCeilingError(700);
      controller.abort(ceiling);
      const rejection = await observation;
      await new Promise(resolve => setTimeout(resolve, 80)); // let any destructive cancel reach the real receiver
      expect(senderMesh.read({ topic: "fabric.control.command", limit: 50 }).filter(event => event.kind === "cancel")).toHaveLength(0);
      expect(rejection).toBe(ceiling);
      expect(stop).not.toHaveBeenCalled();
      if (queued) expect(owner.actors.status(actor.id).queued).toBe(1);
      else expect(owner.agents.list()[0]).toMatchObject({ status: "running", turns: 0, toolCalls: 0 });
      await first;
      await waitFor(() => owner.actorDeliveries.length === (queued ? 2 : 1) && owner.actors.status(actor.id).status === "idle", 5_000);
      expect(owner.actors.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(queued ? 2 : 1);
      expect(run).toHaveBeenCalledTimes(queued ? 2 : 1);
      expect(stop).not.toHaveBeenCalled();
      // Expiry did not revoke owner authority: explicit stop still cancels new accepted work.
      const stopObservation = new AbortController();
      const again = sender.provider.invoke("ask", { id: actor.id, message: "HANG" }, { ...context, signal: stopObservation.signal }).catch(error => error);
      await waitFor(() => Boolean(owner.actors.status(actor.id).inFlightRun));
      const activationId = owner.actors.status(actor.id).inFlightRun!.id;
      if (route === "mesh") {
        // v1 bridges bind ACK targets to Main roots, not actor ids. The command
        // still reaches the real owner; prove stop via authoritative activation
        // state, then bound the unsupported actor ACK observation separately.
        await senderControl.request(ownerId, actor.id, "stop", {}, ownerId, { timeoutMs: 500, routedRemoteHost: "remote-machine" }).catch(error => {
          expect(error.message).toContain("the outcome is unknown");
        });
      } else await sender.provider.stopParticipant(actor.id);
      stopObservation.abort(new Error("stop observation finished"));
      await again;
      expect(stop.mock.calls.filter(([id]) => id === activationId)).toHaveLength(1);
      expect(owner.actorDeliveries).toHaveLength(queued ? 2 : 1);
    } finally { pumping = false; await pump; await bridge?.stop(); vi.unstubAllEnvs(); }
  });
});

describe("terminal result observation receipts", () => {
  it.each([["wait", "publication"], ["join", "publication"], ["status", "publication"], ["wait", "progress"], ["join", "progress"], ["wait", "serialization"], ["join", "serialization"], ["status", "serialization"]] as const)("keeps exactly one completion when terminal %s %s crosses the ceiling", async (action, stage) => {
    const { AgentCompletionInbox } = await import("../src/agents/completion-inbox.js");
    const handlers = new Map<string, (...args: any[]) => unknown>();
    const sendMessage = vi.fn();
    const inboxContext = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, inboxContext);
    const consumed = vi.fn((id: string) => inbox.acknowledge(id));
    const completed = vi.fn((result: import("../src/agents/types.js").AgentRunResult) => inbox.enqueue(result));
    const h = setup([], [], undefined, { onBackgroundComplete: completed, onResultConsumed: consumed });
    const registry = new ActionRegistry();
    registry.register(h.provider);
    const handle = await h.agents.spawn({ task: "receipt test", transport: "process" });
    h.agents.detachSignal(handle.id);
    await waitFor(() => completed.mock.calls.length === 1, 5_000);
    let expired = false;
    const ceiling = createMainExecutionCeilingError(700);
    if (stage === "serialization") {
      const original = h.agents.status.bind(h.agents);
      vi.spyOn(h.agents, "status").mockImplementation(id => {
        const result = original(id);
        const plain = { ...result };
        Object.defineProperty(result, "toJSON", { value() { expired = true; return plain; } });
        return result;
      });
      if (action !== "status") {
        const originalWait = h.agents.wait.bind(h.agents);
        vi.spyOn(h.agents, "wait").mockImplementation(async (...args) => {
          const result = await originalWait(...args);
          const plain = { ...result };
          Object.defineProperty(result, "toJSON", { value() { expired = true; return plain; } });
          return result;
        });
      }
    }
    try {
      await expect(registry.invoke(`agents.${action}`, { id: handle.id }, {
        ...context, audits: [], maxResultChars: 100_000, async approve() {},
        checkExecutionBudget() { if (expired) throw ceiling; },
        activity(event) { if (stage === "progress" && event.type === "metrics") expired = true; },
        observeInvocation(event) { if (stage === "publication" && event.type === "call_end" && event.success) expired = true; },
      })).rejects.toBe(ceiling);
      expect(consumed).not.toHaveBeenCalled();
      const boundary = () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, inboxContext);
      boundary(); boundary();
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([handle.id]);
      expect(sendMessage.mock.calls[0]![0].content).toContain("fake worker complete");
    } finally { inbox.close(); await registry.close(); }
  });
});

describe("queued terminal observation receipts", () => {
  describe.each(["cpython", "node-process"] as const)("%s", backend => {
    it.skipIf(backend === "cpython" && process.platform === "win32").each(
      (["wait", "join", "status"] as const).flatMap(action =>
        (["write failure", "transport closure", "ceiling cancellation", "written then expired", "written then closed", "confirmed delivery"] as const).map(outcome => [action, outcome] as const)),
    )("keeps exactly one completion for terminal agents.%s after %s until confirmed delivery", async (action, outcome) => {
          vi.stubEnv("PI_FABRIC_PARENT_RUN", ""); vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
          const { AgentCompletionInbox } = await import("../src/agents/completion-inbox.js");
          const handlers = new Map<string, (...args: any[]) => unknown>();
          const sendMessage = vi.fn();
          const inboxContext = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
          const inbox = new AgentCompletionInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, inboxContext);
          const consumed = vi.fn((id: string) => inbox.acknowledge(id));
          const completed = vi.fn((result: import("../src/agents/types.js").AgentRunResult) => inbox.enqueue(result));
          const h = setup([], [], undefined, { onBackgroundComplete: completed, onResultConsumed: consumed });
          const registry = new ActionRegistry(); registry.register(h.provider);
          const handle = await h.agents.spawn({ task: "queued receipt", transport: "process" });
          h.agents.detachSignal(handle.id);
          await waitFor(() => completed.mock.calls.length === 1, 5_000);
          let deadlineAt = 0;
          const invoke = registry.invoke.bind(registry);
          const invocation = vi.spyOn(registry, "invoke").mockImplementation((ref, args, ctx) => {
            deadlineAt = ctx.mainDeadlineAt!;
            return invoke(ref, args, ctx);
          });
          const call = { type: "call", id: 1, ref: `agents.${action}`, args: { id: handle.id } };
          let confirm!: (error?: Error) => void;
          let queued!: (message: any) => void;
          const response = new Promise<any>(resolve => { queued = resolve; });
          const channel = new Duplex({
            read() {},
            write(chunk, _encoding, callback) {
              const message = JSON.parse(chunk.toString());
              if (message.type === "execute") {
                callback();
                queueMicrotask(() => channel.push(`${JSON.stringify(call)}\n`));
              } else {
                confirm = callback;
                queued(message);
              }
            },
          });
          const child = Object.assign(new EventEmitter(), {
            pid: undefined, stdout: new PassThrough(), stderr: new PassThrough(),
            stdio: [null, null, null, channel], connected: true, exitCode: null, signalCode: null,
            send: vi.fn((message: any, callback: (error?: Error) => void) => {
              if (message.type === "execute") {
                callback(); queueMicrotask(() => child.emit("message", call));
              } else { confirm = callback; queued(message); }
              return true;
            }),
            disconnect: vi.fn(() => { child.connected = false; }),
            kill: vi.fn(() => { queueMicrotask(() => child.emit("close", 1, null)); return true; }),
          });
          const spawn = vi.mocked(childProcess.spawn).mockReturnValueOnce(child as unknown as ReturnType<typeof childProcess.spawn>);
          const config = structuredClone(DEFAULT_FABRIC_CONFIG);
          if (backend === "cpython") { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
          else config.executor.runtime = backend;
          config.executor.mainMaxTimeoutMs = 60_000;
          const timer = captureRuntimeDeadline(backend);
          const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
          const execution = new FabricExecutionService(registry, config).execute({
            code: backend === "cpython" ? `return await agents.${action}(id=${JSON.stringify(handle.id)})` : `return await agents.${action}({id:${JSON.stringify(handle.id)}});`,
            context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "transport-main" } } as unknown as ExtensionContext,
            signal: undefined, parentToolCallId: `queued-${backend}-${action}`, onPartial() {},
          });
          try {
            const message = await Promise.race([response, execution.then(result => { throw new Error(`Ended before response queued: ${result.error}`); })]);
            expect(message.ok, JSON.stringify(message)).toBe(true);
            // Neither submission nor a successful native write is admission.
            expect(consumed, "queued receipt").not.toHaveBeenCalled();
            const admit = (ack: unknown) => {
              if (backend === "cpython") channel.push(`${JSON.stringify(ack)}\n`);
              else child.emit("message", ack);
            };
            const ack = { type: "response_ack", id: message.id, responseId: message.responseId };
            if (outcome === "write failure") {
              confirm(new Error("queued IPC write failed"));
              child.emit("exit", 1, null); child.emit("close", 1, null);
            } else if (outcome === "transport closure") {
              channel.destroy(); child.connected = false;
              child.emit("exit", 1, null); child.emit("close", 1, null);
              confirm(); // A stale successful callback must not revive delivery.
            } else if (outcome === "ceiling cancellation") {
              timer.fireAt(deadlineAt);
              confirm();
            } else if (outcome === "written then expired") {
              confirm();
              expect(consumed, "write is not admission").not.toHaveBeenCalled();
              timer.fireAt(deadlineAt);
              admit(ack); // The ack after the ceiling cannot revive the receipt.
            } else if (outcome === "written then closed") {
              confirm();
              expect(consumed, "write is not admission").not.toHaveBeenCalled();
              channel.destroy(); child.connected = false;
              child.emit("exit", 1, null); child.emit("close", 1, null);
              if (backend === "node-process") admit(ack);
            } else {
              confirm();
              expect(consumed, "write is not admission").not.toHaveBeenCalled();
              admit({ ...ack, id: message.id + 1 });
              admit({ ...ack, responseId: message.responseId + 1 });
              expect(consumed, "uncorrelated ack").not.toHaveBeenCalled();
              admit(ack);
              // CPython frames use a stream; let its data event run first.
              await new Promise<void>(resolve => setImmediate(resolve));
              admit(ack);
              expect(consumed).toHaveBeenCalledExactlyOnceWith(handle.id);
              const result = { type: "result", result: { terminationReason: "completed", value: message.value, logs: [] } };
              if (backend === "cpython") channel.push(`${JSON.stringify(result)}\n`);
              else child.emit("message", result);
            }
            const result = await execution;
            expect(result.success).toBe(outcome === "confirmed delivery");
            if (outcome === "ceiling cancellation" || outcome === "written then expired") expect(result.error).toMatch(/MainExecutionCeilingError/);
            const boundary = () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, inboxContext);
            boundary(); boundary();
            expect(sendMessage).toHaveBeenCalledTimes(outcome === "confirmed delivery" ? 0 : 1);
            if (outcome !== "confirmed delivery") {
              expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([handle.id]);
              expect(consumed).not.toHaveBeenCalled();
            }
          } finally {
            // Always settle a started execution, including a failing red assertion.
            child.emit("exit", 1, null); child.emit("close", 1, null);
            await execution;
            clock.mockRestore(); timer.restore(); invocation.mockRestore(); spawn.mockReset();
            const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
            spawn.mockImplementation(actual.spawn);
            channel.destroy(); child.stdout.destroy(); child.stderr.destroy();
            inbox.close(); await registry.close(); vi.unstubAllEnvs();
          }
      }, 45_000,
    );
  });
});

// Real transports: stop the receiver after it issued the observation, then let
// its native write succeed. A write callback while SIGSTOPped proves nothing
// about admission. These signal controls are Linux-only, not Windows mocks.
describe.skipIf(process.platform !== "linux")("native written-but-unadmitted observations", () => {
  it.each((["cpython", "node-process"] as const).flatMap(backend =>
    (["deadline", "transport close"] as const).map(outcome => [backend, outcome] as const)),
  )("retains exactly one completion after %s write success before guest admission and %s", async (backend, outcome) => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", ""); vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const { AgentCompletionInbox } = await import("../src/agents/completion-inbox.js");
    const handlers = new Map<string, (...args: any[]) => unknown>();
    const sendMessage = vi.fn();
    const inboxContext = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, inboxContext);
    const consumed = vi.fn((id: string) => inbox.acknowledge(id));
    const completed = vi.fn((result: import("../src/agents/types.js").AgentRunResult) => inbox.enqueue(result));
    const h = setup([], [], undefined, { onBackgroundComplete: completed, onResultConsumed: consumed });
    const registry = new ActionRegistry(); registry.register(h.provider);
    const handle = await h.agents.spawn({ task: "native admission receipt", transport: "process" });
    h.agents.detachSignal(handle.id);
    await waitFor(() => completed.mock.calls.length === 1, 5_000);
    const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
    let guest!: ReturnType<typeof childProcess.spawn>;
    let wrote!: () => void;
    const written = new Promise<void>(resolve => { wrote = resolve; });
    const spawn = vi.mocked(childProcess.spawn).mockImplementationOnce((...args: Parameters<typeof childProcess.spawn>) => {
      guest = actual.spawn(...args);
      if (backend === "node-process") {
        const send = guest.send.bind(guest);
        guest.send = ((message: any, callback: (error: Error | null) => void) => send(message, (error) => {
          callback(error);
          if (message.type === "response" && !error) wrote();
        })) as typeof guest.send;
      } else {
        const channel = guest.stdio[3] as Duplex;
        const write = channel.write.bind(channel);
        channel.write = ((frame: string, callback: (error?: Error | null) => void) => write(frame, (error) => {
          callback(error);
          if (JSON.parse(frame).type === "response" && !error) wrote();
        })) as typeof channel.write;
      }
      return guest;
    });
    let deadlineAt = 0;
    const invoke = registry.invoke.bind(registry);
    const invocation = vi.spyOn(registry, "invoke").mockImplementation(async (ref, args, ctx) => {
      deadlineAt = ctx.mainDeadlineAt!;
      process.kill(guest.pid!, "SIGSTOP");
      await waitFor(() => /\) T /.test(fs.readFileSync(`/proc/${guest.pid}/stat`, "utf8")), 5_000);
      return invoke(ref, args, ctx);
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    if (backend === "cpython") { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
    else config.executor.runtime = backend;
    config.executor.mainMaxTimeoutMs = 60_000;
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
    const timer = captureRuntimeDeadline(backend);
    const controller = new AbortController();
    const execution = new FabricExecutionService(registry, config).execute({
      code: backend === "cpython" ? `return await agents.wait(id=${JSON.stringify(handle.id)})` : `return await agents.wait({id:${JSON.stringify(handle.id)}});`,
      context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "native-admission-main" } } as unknown as ExtensionContext,
      signal: controller.signal, parentToolCallId: `native-${backend}-${outcome}`, onPartial() {},
    });
    try {
      await Promise.race([written, execution.then(result => { throw new Error(`Ended before native write: ${result.error}`); })]);
      expect(/\) T /.test(fs.readFileSync(`/proc/${guest.pid}/stat`, "utf8"))).toBe(true);
      expect(consumed, "successful write to a stopped receiver is not admission").not.toHaveBeenCalled();
      if (outcome === "deadline") timer.fireAt(deadlineAt);
      else {
        if (backend === "cpython") (guest.stdio[3] as Duplex).destroy();
        else guest.disconnect();
        guest.kill("SIGKILL");
      }
      const result = await execution;
      expect(result.success).toBe(false);
      if (outcome === "deadline") expect(result.error).toMatch(/MainExecutionCeilingError/);
      expect(consumed).not.toHaveBeenCalled();
      const boundary = () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, inboxContext);
      boundary(); boundary();
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([handle.id]);
    } finally {
      controller.abort();
      guest?.kill("SIGKILL");
      await execution;
      if (guest && guest.exitCode === null && guest.signalCode === null) await new Promise<void>(resolve => guest.once("exit", () => resolve()));
      clock.mockRestore(); timer.restore(); invocation.mockRestore(); spawn.mockImplementation(actual.spawn);
      inbox.close(); await registry.close(); vi.unstubAllEnvs();
    }
  }, 45_000);
});

describe.skipIf(process.platform !== "linux")("Monty returned-but-unadmitted agent observations", () => {
  it.each((["wait", "join", "status"] as const).flatMap(action =>
    (["expiry", "closure", "normal ack", "ack then expiry"] as const).map(outcome => [action, outcome] as const)),
  )("terminal agents.%s after Monty return before admission: %s", async (action, outcome) => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", ""); vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const { AgentCompletionInbox } = await import("../src/agents/completion-inbox.js");
    const handlers = new Map<string, (...args: any[]) => unknown>();
    const sendMessage = vi.fn();
    const inboxContext = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, inboxContext);
    const consumed = vi.fn((id: string) => inbox.acknowledge(id));
    const completed = vi.fn((result: import("../src/agents/types.js").AgentRunResult) => inbox.enqueue(result));
    const h = setup([], [], undefined, { onBackgroundComplete: completed, onResultConsumed: consumed });
    const registry = new ActionRegistry(); registry.register(h.provider);
    const handle = await h.agents.spawn({ task: "Monty admission receipt", transport: "process" });
    h.agents.detachSignal(handle.id);
    await waitFor(() => completed.mock.calls.length === 1, 5_000);
    let countAtReturn = -1;
    let deadlineAt = 0;
    let ackCount = 0;
    const timer = captureRuntimeDeadline("monty");
    const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
    const control = await captureMontyTransport(() => { countAtReturn = consumed.mock.calls.length; }, ack => {
      ackCount++;
      ack(1, 0); ack(0, 1);
      expect(consumed, "uncorrelated confirmations").not.toHaveBeenCalled();
      ack(); ack();
      expect(consumed).toHaveBeenCalledExactlyOnceWith(handle.id);
      if (outcome === "ack then expiry") timer.fireAt(deadlineAt);
    }, outcome === "expiry" || outcome === "closure");
    const invoke = registry.invoke.bind(registry);
    const invocation = vi.spyOn(registry, "invoke").mockImplementation(async (ref, args, ctx) => {
      deadlineAt = ctx.mainDeadlineAt!;
      return invoke(ref, args, ctx);
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.kernel = "python"; config.executor.pythonRuntime = "monty"; config.executor.mainMaxTimeoutMs = 60_000;
    const controller = new AbortController();
    const execution = new FabricExecutionService(registry, config).execute({
      code: `return await agents.${action}(id=${JSON.stringify(handle.id)})`,
      context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "monty-admission-main" } } as unknown as ExtensionContext,
      signal: controller.signal, parentToolCallId: `monty-${action}-${outcome}`, onPartial() {},
    });
    try {
      await Promise.race([control.responseReturned, execution.then(result => { throw new Error(`Ended before callback returned: ${result.error}`); })]);
      expect(countAtReturn, "callback return is not guest admission").toBe(0);
      if (outcome === "expiry") timer.fireAt(deadlineAt);
      if (outcome === "closure") control.closeReceiver();
      const result = await execution;
      expect(result.success).toBe(outcome === "normal ack");
      if (outcome === "expiry" || outcome === "ack then expiry") expect(result.error).toMatch(/MainExecutionCeilingError/);
      control.staleAck();
      const admitted = outcome === "normal ack" || outcome === "ack then expiry";
      expect(consumed).toHaveBeenCalledTimes(admitted ? 1 : 0);
      expect(ackCount).toBe(admitted ? 1 : 0);
      const boundary = () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, inboxContext);
      boundary(); boundary();
      expect(sendMessage).toHaveBeenCalledTimes(admitted ? 0 : 1);
      if (!admitted) expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([handle.id]);
    } finally {
      controller.abort(); control.closeReceiver(); await execution;
      invocation.mockRestore(); control.restore(); clock.mockRestore(); timer.restore();
      inbox.close(); await registry.close(); vi.unstubAllEnvs();
    }
  }, 45_000);
});

describe("runtime observation receipts", () => {
  it.each(["quickjs", "node-process", "monty", "cpython"] as const)("keeps an unread completion after %s rejects result encoding, but not after delivered guest continuation", async backend => {
    vi.stubEnv("PI_FABRIC_PARENT_RUN", ""); vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const started = performance.now();
    const steps: { atMs: number; step: string; details?: unknown }[] = [];
    const record = (step: string, details?: unknown) => {
      if (steps.length < 64) steps.push({ atMs: Math.round(performance.now() - started), step, details });
    };
    const { AgentCompletionInbox } = await import("../src/agents/completion-inbox.js");
    const handlers = new Map<string, (...args: any[]) => unknown>();
    const sendMessage = vi.fn();
    const inboxContext = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, inboxContext);
    const consumed = vi.fn((id: string) => { record("consumption receipt", { id }); inbox.acknowledge(id); });
    const completed = vi.fn((result: import("../src/agents/types.js").AgentRunResult) => { record("background completion", { id: result.id, status: result.status }); inbox.enqueue(result); });
    const h = setup([], [], undefined, { onBackgroundComplete: completed, onResultConsumed: consumed });
    const registry = new ActionRegistry(); registry.register(h.provider);
    let admitGuest: (() => void) | undefined;
    registry.register({
      name: "receipt_probe", description: "Receipt regression guest readiness",
      async list() { return [{ name: "ready", description: "Mark guest admission", risk: "read" as const, inputSchema: { type: "object", properties: {} } }]; },
      async describe() { return (await this.list({}, context))[0]; },
      async invoke() { admitGuest?.(); return null; },
    });
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.memoryLimitBytes = 128 * 1024 * 1024;
    const python = backend === "monty" || backend === "cpython";
    if (python) { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
    else config.executor.runtime = backend;
    config.executor.mainMaxTimeoutMs = 5_000;
    const service = new FabricExecutionService(registry, config);
    const mainContext = { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "receipt-main" } } as unknown as ExtensionContext;
    // Bound even a broken startup handshake, independently of the mocked budget
    // clock, and leave time for cancellation/cleanup before Vitest's 45s limit.
    const lifetime = new AbortController();
    const lifetimeGuard = setTimeout(() => {
      record("receipt test: startup/cleanup hang guard");
      lifetime.abort(new Error("Receipt regression exceeded its 35-second lifetime guard"));
    }, 35_000);
    const execute = async (parentToolCallId: string, code: string) => {
      record(`${parentToolCallId}: start`);
      const controller = new AbortController();
      let guard: ReturnType<typeof setTimeout> | undefined;
      // As with the admission-clock regressions, startup is not the tested
      // boundary. CPython starts a fresh interpreter/Windows IPC for every call;
      // a warmup cannot make later starts cheap. Arm the active-work guard only
      // when a real guest host call proves admission, not before cold startup.
      admitGuest = () => {
        if (guard !== undefined) return;
        record(`${parentToolCallId}: guest admitted`);
        guard = setTimeout(() => {
          record(`${parentToolCallId}: hang guard`);
          controller.abort(new Error("Receipt regression execution exceeded its 12-second admitted hang guard"));
        }, 12_000);
      };
      try {
        const ready = python ? 'await tools.call(ref="receipt_probe.ready", args={})\n' : 'await tools.call({ ref: "receipt_probe.ready", args: {} });\n';
        const result = await service.execute({ code: ready + code, context: mainContext, signal: AbortSignal.any([controller.signal, lifetime.signal]), parentToolCallId, onPartial() {} });
        record(`${parentToolCallId}: end`, { success: result.success, error: result.error, logs: result.logs, phases: result.phases, audits: result.audits });
        return result;
      } finally { clearTimeout(guard); admitGuest = undefined; }
    };
    const boundary = () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, inboxContext);
    try {
      // Startup is not the behavior under test. Both watchdogs rearm on early
      // firings; advance the wall clock only at the tested boundary. The old
      // real 5-second ceiling could expire before encode on a cold CI runner.
      const warmupClock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
      try {
        const warmup = await execute("receipt-warmup", python ? "return 1" : "return 1;");
        expect(warmup.success, warmup.error).toBe(true);
        expect(warmup.value).toBe(1);
      } finally { warmupClock.mockRestore(); }
      const handle = await h.agents.spawn({ task: "encoding receipt", transport: "process" });
      h.agents.detachSignal(handle.id);
      await waitFor(() => completed.mock.calls.length === 1, 5_000);
      const invoke = registry.invoke.bind(registry);
      const clock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
      const encode = vi.fn((deadlineAt: number) => {
        record("encoding seam", { deadlineAt });
        clock.mockReturnValue(deadlineAt + 1);
        return "rejected encoding";
      });
      const encoding = vi.spyOn(registry, "invoke").mockImplementation(async (ref, args, callContext) => {
        record("encoding host call", { ref });
        const value = await invoke(ref, args, callContext);
        record("encoding host result", { ref });
        if (ref !== "agents.wait") return value;
        // Registry admission precedes guest promise/frame publication.
        Object.defineProperty(value, "text", { enumerable: true, get: () => encode(callContext.mainDeadlineAt!) });
        return value;
      });
      let rejected: Awaited<ReturnType<typeof service.execute>>;
      try {
        rejected = await execute("receipt-encoding", python ? `return await agents.wait(id=${JSON.stringify(handle.id)})` : `return await agents.wait({ id: ${JSON.stringify(handle.id)} });`);
      } finally { encoding.mockRestore(); clock.mockRestore(); }
      expect(encode).toHaveBeenCalled();
      expect(rejected.error).toMatch(/MainExecutionCeilingError/);
      expect(consumed).not.toHaveBeenCalled();
      boundary(); boundary();
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([handle.id]);
      // A result admitted to the guest is consumed even if later guest code
      // spends the ceiling. Deferring all receipts until program success is wrong.
      const delivered = await h.agents.spawn({ task: "delivered receipt", transport: "process" });
      h.agents.detachSignal(delivered.id);
      await waitFor(() => completed.mock.calls.length === 2, 5_000);
      let continued!: (deadlineAt: number) => void;
      const continuation = new Promise<number>(resolve => { continued = resolve; });
      const guestContinuation = vi.spyOn(registry, "invoke").mockImplementation(async (ref, args, callContext) => {
        record("delivery host call", { ref });
        if (ref !== "agents.list") return invoke(ref, args, callContext);
        // This second guest call proves wait's response actually reached the
        // guest. Keep its continuation pending until the real watchdog aborts.
        const pending = new Promise<never>((_resolve, reject) => {
          callContext.signal!.addEventListener("abort", () => reject(callContext.signal!.reason), { once: true });
        });
        record("guest continuation", { deadlineAt: callContext.mainDeadlineAt });
        continued(callContext.mainDeadlineAt!);
        return pending;
      });
      const timer = captureRuntimeDeadline(backend);
      const deliveryClock = vi.spyOn(Date, "now").mockReturnValue(Date.now());
      const execution = execute("receipt-delivered", python ? `result = await agents.wait(id=${JSON.stringify(delivered.id)})\nreturn await agents.list()` : `const result = await agents.wait({ id: ${JSON.stringify(delivered.id)} }); return await agents.list();`);
      try {
        const deadlineAt = await Promise.race([continuation, execution.then(result => {
          throw new Error(`Guest ended before delivered continuation: ${result.error}`);
        })]);
        expect(consumed).toHaveBeenCalledExactlyOnceWith(delivered.id);
        deliveryClock.mockRestore();
        record("fire runtime deadline", { deadlineAt });
        timer.fireAt(deadlineAt);
        const observed = await execution;
        expect(observed.error).toMatch(/MainExecutionCeilingError/);
        expect(consumed).toHaveBeenCalledExactlyOnceWith(delivered.id);
        boundary(); boundary();
        expect(sendMessage).toHaveBeenCalledOnce();
      } finally { deliveryClock.mockRestore(); timer.restore(); guestContinuation.mockRestore(); await execution; }
    } catch (error) {
      // Monotonic failure-only telemetry survives the mocked wall clock. The
      // old assertion hid warmup errors and never identified the failing phase.
      console.error("runtime observation receipts diagnostics", JSON.stringify({
        backend, platform: process.platform, node: process.version, cpythonBinary: config.executor.cpython.binary,
        steps, consumptionCalls: consumed.mock.calls, completionIds: completed.mock.calls.map(([result]) => result.id),
      }, null, 2));
      throw error;
    } finally { clearTimeout(lifetimeGuard); lifetime.abort(); inbox.close(); await registry.close(); vi.unstubAllEnvs(); }
  }, 45_000);
});

describe("AgentsProvider actor session reset", () => {
  it("answers agents.compact on an actor id with a pointer to resetSession, and routes resetSession", async () => {
    const { provider, actors } = setup();
    const actor = await actors.create({ name: "reviewer", instructions: "Review." });
    await expect(provider.invoke("compact", { id: actor.id }, context)).rejects.toThrow(
      `reviewer (${actor.id}) is a Fabric actor, not a task agent: agents.compact compacts a running task agent. ` +
        "To start an actor on a fresh session, use agents.resetSession({ id }).",
    );
    await expect(provider.invoke("compact", { id: "reviewer" }, context)).rejects.toThrow(/use agents\.resetSession/);
    // Neither an agent nor an actor: the original error stands.
    await expect(provider.invoke("compact", { id: "nobody-here" }, context)).rejects.toThrow(/Unknown Fabric agent/);
    fs.mkdirSync(path.dirname(actor.sessionFile!), { recursive: true });
    fs.writeFileSync(actor.sessionFile!, "{}\n");
    await expect(provider.invoke("resetSession", { id: actor.id }, context)).resolves.toMatchObject({ id: actor.id });
    expect(JSON.parse(fs.readFileSync(actor.sessionFile!, "utf8").split("\n", 1)[0]!)).toMatchObject({ type: "session", version: 3 });
    expect(fs.readdirSync(path.dirname(actor.sessionFile!)).some((name) => /^session\.jsonl\..+\.bak$/.test(name))).toBe(true);
  });
});

describe("AgentsProvider runtime ownership lifecycle", () => {
  it("does not close shared runtime services when it is not the owner", async () => {
    const { agents, actors, globalActors, mainAgent, participants, control, lifecycle } = setup();
    const provider = new AgentsProvider(
      agents, actors, globalActors, mainAgent, participants, control, lifecycle,
      undefined, undefined, false,
    );
    const closeLifecycle = vi.spyOn(lifecycle, "close");
    const closeActors = vi.spyOn(actors, "close");
    const closeAgents = vi.spyOn(agents, "close");
    await provider.close();
    expect(closeLifecycle).not.toHaveBeenCalled();
    expect(closeActors).not.toHaveBeenCalled();
    expect(closeAgents).not.toHaveBeenCalled();
  });

  it("closes agents even if actor shutdown fails, after stopping lifecycle delivery", async () => {
    const { provider, agents, actors, lifecycle } = setup();
    const failure = new Error("actor shutdown failed");
    const order: string[] = [];
    vi.spyOn(lifecycle, "close").mockImplementationOnce(async () => { order.push("lifecycle"); });
    vi.spyOn(actors, "close").mockImplementationOnce(async () => {
      order.push("actors");
      throw failure;
    });
    vi.spyOn(agents, "close").mockImplementationOnce(async () => { order.push("agents"); });
    await expect(provider.close()).rejects.toBe(failure);
    expect(order).toEqual(["lifecycle", "actors", "agents"]);
  });
});

afterEach(async () => {
  await Promise.all(controlPlanes.splice(0).map((control) => control.close()));
  await Promise.all(actorManagers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentManagers.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

const waitFor = async (predicate: () => boolean, timeoutMs = 2_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for actor state");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
};

const createRequest = {
  name: "reviewer",
  instructions: "Review code for security defects and reply concisely.",
  events: ["turn_end"],
  delivery: "steer",
  responseMode: "directive",
  triggerTurn: false,
};

const lifecycleSubscription = (
    overrides: Partial<FabricLifecycleSubscription> = {},
  ): FabricLifecycleSubscription => ({
    format: 1,
    id: `sub-${Math.random().toString(36).slice(2, 8)}`,
    from: "session:source",
    events: ["run.completed"],
    to: "session:test",
    delivery: "followUp",
    triggerTurn: true,
    once: false,
    afterSequence: 0,
    createdAt: 1,
    updatedAt: 1,
    createdBy: { id: "session:source", name: "source", kind: "main" },
    ...overrides,
  });

  const lifecycleEvent = (
    overrides: Partial<FabricLifecycleEvent> = {},
  ): FabricLifecycleEvent => ({
    version: 1,
    id: `evt-${Math.random().toString(36).slice(2, 8)}`,
    sequence: Math.floor(Math.random() * 10_000),
    event: "run.completed",
    source: {
      id: "session:source",
      name: "Peer source",
      kind: "root",
      rootId: "session:source",
      runner: "pi",
      ownerHostId: "session:source",
      ownerIdentityId: "session:source",
    },
    occurredAt: Date.now(),
    publishedAt: Date.now(),
    ...overrides,
  });

  // smarty-dev#1826: tell to Main is a followUp, so a stalled queue fails it the same way.
  it("throws for tell and followUp to a Main whose held queue is stalled", async () => {
    const { provider, mainAgent, mainDeliveries } = setup();
    mainAgent.deliverAgent = (request) => {
      mainDeliveries.push(request);
      return { queued: true, messageId: "held", routed: "main", pendingFollowUps: 4, oldestAgeS: 20_520, stalled: true } as never;
    };
    for (const method of ["tell", "followUp"]) {
      await expect(provider.invoke(method, { id: "main", message: "still there?" }, context)).rejects.toThrow(
        "Fabric followUp to main was accepted but is not being delivered: target idle and its held queue stalled (yours: 4 held, oldest 20520 s). " +
          "The message is still held, not withdrawn.",
      );
    }
    expect(mainDeliveries.map((request) => request.delivery)).toEqual(["followUp", "followUp"]);
  });

  describe("AgentsProvider lifecycle coalescing", () => {
    it("coalesces a burst of followUp lifecycle events into one wake delivery", async () => {
      const { provider, mainDeliveries } = setup();
      for (let index = 0; index < 5; index += 1) {
        await provider.deliverLifecycle(
          lifecycleSubscription(),
          lifecycleEvent({ source: { ...lifecycleEvent().source, id: `agent-${index}`, name: `worker-${index}` } }),
        );
      }
      await provider.flushLifecycleDeliveries();
      expect(mainDeliveries.length).toBe(1);
      const delivery = mainDeliveries[0]!;
      expect(delivery.delivery).toBe("followUp");
      expect(delivery.triggerTurn).toBe(true);
      expect(delivery.message).toContain("Fabric lifecycle events (5)");
      expect(delivery.message).toContain("worker-0");
      expect(delivery.message).toContain("worker-4");
    });

    it("delivers steer lifecycle events immediately without coalescing", async () => {
      const { provider, mainDeliveries } = setup();
      await provider.deliverLifecycle(
        lifecycleSubscription({ delivery: "steer" }),
        lifecycleEvent(),
      );
      await provider.flushLifecycleDeliveries();
      expect(mainDeliveries.length).toBe(1);
      expect(mainDeliveries[0]!.delivery).toBe("steer");
    });

    it("keeps separate targets on separate wake deliveries", async () => {
      const { provider, mainDeliveries } = setup();
      await provider.deliverLifecycle(
        lifecycleSubscription({ to: "session:test" }),
        lifecycleEvent(),
      );
      await provider.deliverLifecycle(
        lifecycleSubscription({ to: "main" }),
        lifecycleEvent(),
      );
      await provider.flushLifecycleDeliveries();
      expect(mainDeliveries.length).toBe(2);
    });

    it("keeps single-event deliveries in the original message format", async () => {
      const { provider, mainDeliveries } = setup();
      await provider.deliverLifecycle(
        lifecycleSubscription(),
        lifecycleEvent({ runId: "run-12345678", status: "completed" }),
      );
      await provider.flushLifecycleDeliveries();
      expect(mainDeliveries.length).toBe(1);
      expect(mainDeliveries[0]!.message).toBe(
        "Fabric lifecycle run.completed from Peer source (session:source) (run run-1234) with status completed.",
      );
      expect(mainDeliveries[0]!.message).not.toContain("Fabric lifecycle events");
    });
  });

describe("AgentsProvider runner support", () => {
  it("exposes model-programmable residency on spawn and create", async () => {
    const { provider } = setup();
    const spawn = await provider.describe("spawn", context);
    const create = await provider.describe("create", context);
    const run = await provider.describe("run", context);
    const spawnProperties = (spawn?.inputSchema as { properties: Record<string, { enum?: string[] }> }).properties;
    const createProperties = (create?.inputSchema as { properties: Record<string, { enum?: string[] }> }).properties;
    const runProperties = (run?.inputSchema as { properties: Record<string, unknown> }).properties;

    expect(spawnProperties.residency?.enum).toEqual(["session", "durable"]);
    expect(createProperties.residency?.enum).toEqual(["session", "durable"]);
    expect(runProperties).not.toHaveProperty("residency");
  });

  it("exposes actor activation overrides and scoped binding setters", async () => {
    const { provider } = setup();
    const ask = await provider.describe("ask", context);
    const tell = await provider.describe("tell", context);
    const setModel = await provider.describe("setModel", context);
    const setThinking = await provider.describe("setThinking", context);
    const properties = (descriptor: typeof ask) =>
      (descriptor?.inputSchema as {
        properties: Record<string, { enum?: string[] }>;
      }).properties;

    expect(properties(ask)).toHaveProperty("model");
    expect(properties(ask).thinking?.enum).toContain("xhigh");
    expect(properties(tell)).toHaveProperty("model");
    expect(properties(setModel).scope?.enum).toEqual(["session", "project"]);
    expect(properties(setThinking).scope?.enum).toEqual(["session", "project"]);
  });
  it("exposes the Veda runner and per-run persona on run and spawn", async () => {
    const { provider } = setup();
    const run = await provider.describe("run", context);
    const spawn = await provider.describe("spawn", context);
    type RunnerProperty = { enum?: string[]; type?: string; description?: string };
    const runProperties = (run?.inputSchema as { properties: Record<string, RunnerProperty> }).properties;
    const spawnProperties = (spawn?.inputSchema as { properties: Record<string, RunnerProperty> }).properties;
    expect(runProperties.runner?.enum).toEqual(["pi", "claude", "veda"]);
    expect(spawnProperties.runner?.enum).toEqual(["pi", "claude", "veda"]);
    expect(runProperties.persona?.type).toBe("string");
    expect(runProperties.persona?.description).toContain("Veda persona");
    expect(spawnProperties.persona?.type).toBe("string");
    expect(spawnProperties.persona?.description).toContain("Veda persona");
    // Veda forwards any -m value to the backend, so model discovery is an
    // empty advisory list rather than a runtime enumeration.
    await expect(provider.invoke("models", { runner: "veda" }, context)).resolves.toEqual([]);
  });

  it("rejects unavailable durable actor residency before creating an actor", async () => {
    const { provider, actors } = setup();

    await expect(
      provider.invoke(
        "create",
        {
          name: "resident",
          instructions: "Remain active.",
          residency: "durable",
        },
        context,
      ),
    ).rejects.toThrow("trusted project");
    expect(actors.list()).toEqual([]);
  });

  it("routes durable creates and imports when the local registry is already owned", async () => {
    const state = setup();
    // Simulate the transferred registry: a live foreign actor over the shared
    // registry makes the local manager's create guard throw.
    const peerIdentity: MeshIdentity = {
      id: "session:foreign-owner",
      name: "main",
      kind: "main",
      sessionId: "foreign-owner",
    };
    const peer = new ActorManager(
      "foreign-owner",
      peerIdentity,
      state.mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        claimResidency: "session",
        rootId: peerIdentity.id,
      },
    );
    actorManagers.push(peer);
    const foreign = await peer.create({
      name: "first-durable",
      instructions: "Ceded to the resident host.",
      residency: "durable",
    });
    // A manager that sees every live actor as owned by another host, like
    // Main after the registry transferred to the resident host.
    const guardedActors = new ActorManager(
      "guarded-main",
      state.identity,
      state.mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      state.agents,
      () => {},
      {
        actorRoot: path.join(state.root, "actors"),
        persistent: true,
        claimResidency: "session",
        rootId: state.identity.id,
        canManageActor: () => false,
      },
    );
    actorManagers.push(guardedActors);
    await waitFor(() => guardedActors.list().some((entry) => entry.id === foreign.id));

    let sequence = 0;
    const createActor = vi.fn(async (request: FabricActorRequest): Promise<FabricActorInfo> => {
      const now = Date.now();
      sequence += 1;
      return {
        id: `resident-actor-${sequence}`,
        scope: request.scope ?? "project",
        name: request.name,
        status: "idle",
        runner: request.runner ?? "pi",
        events: request.events ?? [],
        topics: request.topics ?? [],
        delivery: request.delivery ?? "mailbox",
        responseMode: request.responseMode ?? "text",
        triggerTurn: request.triggerTurn ?? false,
        coalesce: request.coalesce ?? true,
        residency: "durable",
        queued: 0,
        messages: 0,
        createdAt: now,
        updatedAt: now,
      };
    });
    const residency = {
      ensureHost: vi.fn(async () => undefined),
      createActor,
    } as unknown as ResidencyClient;
    const provider = new AgentsProvider(
      state.agents,
      guardedActors,
      state.globalActors,
      state.mainAgent,
      state.participants,
      state.control,
      state.lifecycle,
      undefined,
      residency,
      undefined,
      () => DEFAULT_FABRIC_CONFIG.models,
    );

    const invocationContext = { ...context, signal: new AbortController().signal };
    const created = (await provider.invoke(
      "create",
      {
        name: "second-durable",
        instructions: "Created via the resident host.",
        residency: "durable",
      },
      invocationContext,
    )) as FabricActorInfo;
    state.globalActors.create({
      name: "durable-template",
      instructions: "Imported via the resident host.",
      residency: "durable",
    });
    const imported = (await provider.invoke(
      "import",
      { name: "durable-template" },
      invocationContext,
    )) as FabricActorInfo;

    expect(created).toMatchObject({ id: "resident-actor-1", name: "second-durable" });
    expect(imported).toMatchObject({ id: "resident-actor-2", name: "durable-template" });
    expect(createActor).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ name: "second-durable", residency: "durable" }),
      invocationContext.signal,
    );
    expect(createActor).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ name: "durable-template", residency: "durable" }),
      invocationContext.signal,
    );
  });

  it("never hides resident uncertainty in local activation compensation or reclaim", async () => {
    const state = setup();
    const activationError = new Error("publication failed after local creation");
    const unknown = new ResidentOutcomeUnknownError({
      format: 1, operation: "removeActor", requestId: "committed-removal", rootId: "session:main",
      id: "known-actor", createdAt: Date.now(),
    }, { state: "committed", requestId: "committed-removal", id: "known-actor", ownerHostId: "resident:test" }, activationError);
    const createActor = vi.fn().mockRejectedValue(unknown);
    const ensureActor = vi.fn().mockRejectedValue(activationError);
    const removeActor = vi.fn().mockRejectedValue(unknown);
    const localCreate = vi.spyOn(state.actors, "create");
    const reclaim = vi.spyOn(state.actors, "reclaim");
    const residency = {
      ensureHost: vi.fn(async () => undefined), ensureActor, removeActor, createActor,
    } as unknown as ResidencyClient;
    const provider = new AgentsProvider(
      state.agents, state.actors, state.globalActors, state.mainAgent, state.participants,
      state.control, state.lifecycle, undefined, residency, undefined, () => DEFAULT_FABRIC_CONFIG.models,
    );
    await expect(provider.invoke("create", {
      name: "activation-failure", instructions: "Do not reclaim an uncertain transfer.", residency: "durable",
    }, context)).rejects.toBe(unknown);
    expect(createActor).toHaveBeenCalledOnce();
    expect(localCreate).not.toHaveBeenCalled(); expect(ensureActor).not.toHaveBeenCalled();
    expect(removeActor).not.toHaveBeenCalled(); expect(reclaim).not.toHaveBeenCalled();
  });
  it("lists live peer sessions separately from Main", async () => {
    const peer: FabricPeerInfo = {
      id: "session:peer",
      name: "Peer peer",
      kind: "peer",
      status: "idle",
      runner: "pi",
      transport: "host",
      cwd: process.cwd(),
      sessionId: "peer",
      startedAt: 1,
      updatedAt: 2,
      pendingMessages: false,
      local: false,
    };
    const { provider } = setup([peer]);

    await expect(provider.invoke("peers", {}, context)).resolves.toEqual([peer]);
    expect((await provider.describe("peers", context))?.risk).toBe("read");
  });

  // smarty-dev#784: a worktree agent finds its project agent by role and project.
  it("returns this session's project agent", async () => {
    const project = projectOf(process.cwd());
    const base = {
      format: 1 as const, kind: "root" as const, name: "main", status: "idle", runner: "pi", transport: "host",
      capabilities: ["steer", "followUp", "fabric"] as FabricParticipantInfo["capabilities"],
      startedAt: 1, updatedAt: 2, controlProtocol: "v1" as const, local: false, stale: false,
    };
    const lead = {
      ...base, id: "session:lead", rootId: "session:lead", ownerHostId: "session:lead", ownerIdentityId: "session:lead",
      sessionId: "lead", role: "project-agent", project, cwd: project,
    } as FabricParticipantInfo;
    const other = {
      ...base, id: "session:other", rootId: "session:other", ownerHostId: "session:other", ownerIdentityId: "session:other",
      sessionId: "other", role: "project-agent", project: "/elsewhere", cwd: "/elsewhere",
    } as FabricParticipantInfo;
    const { provider } = setup([], [other, lead]);
    await expect(provider.invoke("projectAgent", {}, context)).resolves.toMatchObject({ id: "session:lead" });
    expect((await provider.describe("projectAgent", context))?.risk).toBe("read");
  });

  // smarty-dev#2045 (security review F1 on pi-fabric#132): a mirrored root's role and project are
  // the remote's own claims and never make it this project's leader.
  it("never returns a mirrored root as the project agent, even newer than the native one", async () => {
    const project = projectOf(process.cwd());
    const base = {
      format: 1 as const, kind: "root" as const, name: "main", status: "idle", runner: "pi", transport: "host",
      capabilities: ["steer", "followUp", "fabric"] as FabricParticipantInfo["capabilities"],
      updatedAt: 2, controlProtocol: "v1" as const, local: false, stale: false, role: "project-agent", project, cwd: project,
    };
    const lead = {
      ...base, id: "session:lead", rootId: "session:lead", ownerHostId: "session:lead", ownerIdentityId: "session:lead",
      sessionId: "lead", startedAt: 1,
    } as FabricParticipantInfo;
    const mirrored = {
      ...base, id: "session:remote", rootId: "session:remote", ownerHostId: "session:remote", ownerIdentityId: "session:remote",
      sessionId: "remote", startedAt: 99, remoteHost: "forge",
    } as FabricParticipantInfo;
    await expect(setup([], [lead, mirrored]).provider.invoke("projectAgent", {}, context))
      .resolves.toMatchObject({ id: "session:lead" });
    await expect(setup([], [mirrored]).provider.invoke("projectAgent", {}, context))
      .rejects.toThrow(`No live project agent for ${project}`);
    // The resident-actor fallback uses the same resolver: with the actor's root gone, no mirror.
    expect(deliveryRoot("session:gone", [lead, mirrored], project)).toBe("session:lead");
    expect(deliveryRoot("session:gone", [mirrored], project)).toBe("session:gone");
  });

  it("lists current and peer roots as symmetric session agents", async () => {
    const roots: FabricParticipantInfo[] = [
      {
        format: 1, id: "session:test", kind: "root", rootId: "session:test",
        ownerHostId: "session:test", ownerIdentityId: "session:test", name: "main",
        status: "idle", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp", "fabric"], sessionId: "test",
        startedAt: 1, updatedAt: 2, controlProtocol: "v1", local: true, stale: false,
      },
      {
        format: 1, id: "session:peer", kind: "root", rootId: "session:peer",
        ownerHostId: "session:peer", ownerIdentityId: "session:peer", name: "main",
        status: "running", runner: "pi", transport: "host",
        capabilities: ["steer", "followUp", "fabric"], sessionId: "peer",
        startedAt: 1, updatedAt: 2, controlProtocol: "v1", local: false, stale: false,
      },
    ];
    const { provider } = setup([], roots);

    await expect(provider.invoke("sessions", {}, context)).resolves.toEqual(roots);
    expect((await provider.describe("sessions", context))?.risk).toBe("read");
  });

  it("creates, lists, and removes source-qualified lifecycle subscriptions", async () => {
    const target: FabricParticipantInfo = {
      format: 1,
      id: "session:test",
      kind: "root",
      rootId: "session:test",
      ownerHostId: "session:test",
      ownerIdentityId: "session:test",
      name: "main",
      status: "idle",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "fabric"],
      cwd: process.cwd(),
      sessionId: "test",
      startedAt: 1,
      updatedAt: 1,
      controlProtocol: "v1",
      local: true,
      stale: false,
    };
    const source: FabricParticipantInfo = {
      ...target,
      id: "session:peer",
      rootId: "session:peer",
      ownerHostId: "session:peer",
      ownerIdentityId: "session:peer",
      name: "Peer peer",
      sessionId: "peer",
      local: false,
    };
    const { provider } = setup([], [target, source]);

    const subscription = await provider.invoke(
      "subscribe",
      {
        from: source.id,
        events: ["pi.agent_settled"],
        delivery: "followUp",
        triggerTurn: false,
        once: true,
      },
      context,
    ) as { id: string };

    await expect(provider.invoke("subscriptions", { to: "main" }, context)).resolves.toEqual([
      expect.objectContaining({
        id: subscription.id,
        from: source.id,
        to: target.id,
        events: ["pi.agent_settled"],
        triggerTurn: false,
        once: true,
      }),
    ]);
    await expect(
      provider.invoke("unsubscribe", { id: subscription.id }, context),
    ).resolves.toEqual({ removed: true });
    expect((await provider.describe("subscribe", context))?.risk).toBe("agent");
  });

  it("delivers lifecycle envelopes to Main with source identity and passive policy", async () => {
    const { provider, mainDeliveries } = setup();
    const subscription: FabricLifecycleSubscription = {
      format: 1,
      id: "subscription-1",
      from: "session:peer",
      events: ["pi.agent_settled"],
      to: "session:test",
      delivery: "followUp",
      triggerTurn: false,
      once: false,
      afterSequence: 0,
      createdAt: 1,
      updatedAt: 1,
      createdBy: { id: "session:test", name: "main", kind: "main" },
    };
    const event: FabricLifecycleEvent = {
      version: 1,
      id: "event-1",
      sequence: 1,
      event: "pi.agent_settled",
      source: {
        id: "session:peer",
        name: "Peer peer",
        kind: "root",
        rootId: "session:peer",
        runner: "pi",
      },
      occurredAt: 2,
      publishedAt: 3,
    };

    await provider.deliverLifecycle(subscription, event);
    await provider.flushLifecycleDeliveries();

    expect(mainDeliveries).toEqual([
      expect.objectContaining({
        from: { id: "session:peer", name: "Peer peer", kind: "main" },
        delivery: "followUp",
        triggerTurn: false,
        data: event,
      }),
    ]);
  });

  it("rejects remote Main delivery after its capabilities are withdrawn", async () => {
    const remoteRoot: FabricParticipantInfo = {
      format: 1,
      id: "session:test",
      kind: "root",
      rootId: "session:test",
      ownerHostId: "session:test",
      ownerIdentityId: "session:test",
      name: "main",
      status: "idle",
      runner: "pi",
      transport: "host",
      capabilities: [],
      cwd: process.cwd(),
      sessionId: "test",
      startedAt: 1,
      updatedAt: 2,
      controlProtocol: "v1",
      local: false,
      stale: false,
    };
    const { provider } = setup([], [remoteRoot]);
    (provider.mainAgent as { local: boolean }).local = false;

    await expect(provider.invoke("status", { id: "main" }, context)).resolves.toEqual(
      remoteRoot,
    );
    await expect(
      provider.routeMessage("main", "too late", undefined, "steer"),
    ).rejects.toThrow(
      "does not support steer",
    );
  });

  it("tells a sender that a quiesced Main is shutting down (smarty-dev#1113)", async () => {
    const stopping: FabricParticipantInfo = {
      format: 1, id: "session:test", kind: "root", rootId: "session:test", ownerHostId: "session:test",
      ownerIdentityId: "session:test", name: "main", status: "stopping", runner: "pi", transport: "host",
      capabilities: [], cwd: process.cwd(), sessionId: "test", startedAt: 1, updatedAt: 2, controlProtocol: "v1",
      local: false, stale: false,
    };
    const { provider } = setup([], [stopping]);
    (provider.mainAgent as { local: boolean }).local = false;
    await expect(provider.routeMessage("main", "hello", undefined, "steer")).rejects.toThrow(
      "is shutting down; its session will relaunch or end. Retry after it restarts.",
    );
  });

  it("projects remote agents through members, scoped list, and status", async () => {
    const remote: FabricParticipantInfo = {
      format: 1,
      id: "agent:remote",
      kind: "agent",
      rootId: "session:peer",
      ownerHostId: "session:peer",
      ownerIdentityId: "session:peer",
      parentId: "session:peer",
      name: "remote reviewer",
      status: "running",
      runner: "pi",
      transport: "process",
      capabilities: ["steer", "followUp", "stop"],
      cwd: process.cwd(),
      startedAt: 1,
      updatedAt: 2,
      controlProtocol: "v1",
      local: false,
      stale: false,
    };
    const { provider } = setup([], [remote]);

    await expect(
      provider.invoke("members", { scope: "project" }, context),
    ).resolves.toEqual([remote]);
    await expect(
      provider.invoke("list", { scope: "project" }, context),
    ).resolves.toEqual([remote]);
    await expect(
      provider.invoke("status", { id: remote.id }, context),
    ).resolves.toEqual(remote);
    await expect(
      provider.invoke("list", { scope: "lineage" }, context),
    ).resolves.toEqual([]);
  });

  // smarty-dev#2184: an actor's activation run (id = run id, name = actor name, no root) listed
  // as a standalone agent read as "the reviewer is now <run id>, with no root session".
  it("omits actor activation runs from agents.list; the actor is listed by agents.actors", async () => {
    const { provider, agents } = setup();
    const run = await agents.spawn({
      task: "actor activation",
      name: "playground-review-astra",
      actorId: "7a3e1e35-actor",
      actorName: "playground-review-astra",
    });
    const task = await agents.spawn({ task: "plain task", name: "worker" });
    for (const scope of [undefined, "local", "project", "lineage"]) {
      const listed = (await provider.invoke("list", scope ? { scope } : {}, context)) as Array<{ id: string }>;
      expect(listed.map((record) => record.id), String(scope)).not.toContain(run.id);
    }
    const local = (await provider.invoke("list", {}, context)) as Array<{ id: string }>;
    expect(local.map((record) => record.id)).toContain(task.id);
    // A direct lookup of the run id still answers, and names its actor.
    await expect(provider.invoke("status", { id: run.id }, context)).resolves.toMatchObject({
      id: run.id,
      actorId: "7a3e1e35-actor",
    });
  });

  // smarty-dev#266: during a mesh write stall, user-facing listings report it instead of [].
  it("reports a mesh write stall from user-facing listings but not from local ones", async () => {
    const stalled = new Error("Fabric mesh is write-stalled: Timed out waiting for the Fabric mesh lock");
    const { provider } = setup([], [], undefined, { writeStalled: () => stalled });

    for (const [action, args] of [
      ["sessions", {}], ["peers", {}], ["members", { scope: "project" }], ["list", { scope: "project" }],
      // lineage also reads the shared directory: descendants in other runtimes.
      ["members", { scope: "lineage", kinds: ["agent"] }], ["list", { scope: "lineage" }],
    ] as const) {
      await expect(provider.invoke(action, args, context), action).rejects.toThrow(stalled.message);
    }
    await expect(provider.invoke("members", { scope: "local" }, context)).resolves.toBeInstanceOf(Array);
    await expect(provider.invoke("members", { scope: "project", includeStale: true }, context)).resolves.toBeInstanceOf(Array);
    await expect(provider.invoke("members", { scope: "lineage", includeStale: true }, context)).resolves.toBeInstanceOf(Array);
    await expect(provider.invoke("list", {}, context)).resolves.toBeInstanceOf(Array);
  });

  it("defers explicit handoff until the finalized outer Fabric result", async () => {
    const { provider, root } = setup();
    const source = SessionManager.create(process.cwd(), path.join(root, "source-session"));
    source.appendMessage({
      role: "user",
      content: "Implement the rare token guard 43117",
      timestamp: 1,
    });
    source.appendMessage({
      role: "assistant",
      content: [
        { type: "text", text: "I found the guard and am completing the full program." },
        {
          type: "toolCall",
          id: context.parentToolCallId,
          name: "fabric_exec",
          arguments: {
            code: "await pi.read(...); await pi.edit(...); await pi.edit(...); return 'verified';",
          },
        },
      ],
      api: "anthropic",
      provider: "anthropic",
      model: "frontier",
      usage,
      stopReason: "toolUse",
      timestamp: 2,
    });
    const updates: string[] = [];
    let deferredRequest: Record<string, unknown> | undefined;
    const handoffContext: FabricInvocationContext = {
      ...context,
      extensionContext: {
        ...context.extensionContext,
        sessionManager: source,
        model: { provider: "anthropic", id: "frontier" },
      } as unknown as ExtensionContext,
      update(message) {
        updates.push(message);
      },
      deferHandoff(args) {
        deferredRequest = structuredClone(args);
        return {
          scheduled: true,
          status: "deferred",
          boundary: "fabric_exec_end",
        };
      },
    };
    const args = {
      model: "anthropic/executor",
      task: "Finish the implementation and verify it.",
      transport: "process",
    };

    await expect(provider.invoke("handoff", args, handoffContext)).resolves.toMatchObject({
      status: "deferred",
      boundary: "fabric_exec_end",
    });
    expect(deferredRequest).toEqual({ ...args, extensions: true, kernel: "typescript", pythonRuntime: "monty" });
    expect(fs.existsSync(path.join(root, "runs"))).toBe(false);

    const outerToolResult = {
      role: "toolResult" as const,
      toolCallId: context.parentToolCallId,
      toolName: "fabric_exec",
      content: [{ type: "text" as const, text: "verified after every nested call" }],
      details: { success: true },
      isError: false,
      timestamp: 3,
    };
    const seed = snapshotHandoffSession(
      source,
      { provider: "anthropic", id: "frontier" },
      outerToolResult,
      context.parentToolCallId,
    );
    const result = (await provider.executeHandoff(
      deferredRequest!,
      handoffContext,
      seed,
    )) as {
      handedOff: boolean;
      completed: boolean;
      status: string;
      implementation: string;
      agent: { id: string; model: string };
    };

    expect(result).toMatchObject({
      handedOff: true,
      completed: true,
      status: "completed",
      implementation: "fake worker complete",
      agent: { model: "anthropic/executor" },
    });
    expect(updates).toContainEqual(expect.stringContaining("caller is waiting"));
    expect(updates).toContainEqual(expect.stringContaining("completed implementation"));
    const task = fs.readFileSync(
      path.join(root, "runs", result.agent.id, "task.txt"),
      "utf8",
    );
    expect(task).toContain("inherited conversation trajectory");
    expect(task).toContain("End with a concise conclusion for the caller");
    expect(task).toContain("unfinished or blocked work");
    expect(task).toContain("artifact paths verbatim");
    expect(task).toContain("Finish the implementation and verify it.");
    expect(task).toContain("If the request is read-only");
    expect(task).not.toContain("caller has handed implementation to you");
    const handoffDirectory = path.join(root, "runs", result.agent.id, "handoff-session");
    const [sessionName] = fs.readdirSync(handoffDirectory);
    const seededSession = SessionManager.open(path.join(handoffDirectory, sessionName!));
    const seededMessages = seededSession.buildSessionContext().messages;
    expect(JSON.stringify(seededMessages)).toContain("Implement the rare token guard 43117");
    expect(seededMessages.map((message) => message.role)).toEqual([
      "user",
      "assistant",
      "toolResult",
    ]);
    expect(seededMessages[1]).toMatchObject({
      role: "assistant",
      content: expect.arrayContaining([
        expect.objectContaining({
          type: "toolCall",
          name: "fabric_exec",
          id: context.parentToolCallId,
        }),
      ]),
    });
    expect(seededMessages[2]).toEqual(outerToolResult);
    expect(seededSession.getEntries().some((entry) => entry.type === "custom_message")).toBe(false);
  });

  it("requires an explicit target model for handoff", async () => {
    const { provider, root } = setup();
    const source = SessionManager.inMemory(root);
    const handoffContext = {
      ...context,
      extensionContext: {
        ...context.extensionContext,
        sessionManager: source,
      } as unknown as ExtensionContext,
    };
    await expect(provider.invoke("handoff", {}, handoffContext)).rejects.toThrow(
      /requires an explicit Pi target model/,
    );
    const descriptor = await provider.describe("handoff", handoffContext);
    expect(descriptor?.risk).toBe("agent");
    const schema = descriptor?.inputSchema as {
      required: string[];
      properties: Record<string, unknown>;
    };
    expect(schema.required).toEqual(["model"]);
    expect(schema.properties).toHaveProperty("task");
    expect(schema.properties).not.toHaveProperty("when");
    expect(schema.properties).not.toHaveProperty("checkpoint");
  });

  it("exposes cwd on one-shot schemas but not handoff or actor definitions", async () => {
    const { provider } = setup();
    const run = await provider.describe("run", context);
    const spawn = await provider.describe("spawn", context);
    const handoff = await provider.describe("handoff", context);
    const create = await provider.describe("create", context);
    const properties = (descriptor: typeof run) =>
      (descriptor?.inputSchema as { properties: Record<string, unknown> }).properties;

    expect(properties(run)).toHaveProperty("cwd", expect.objectContaining({ type: "string" }));
    expect(properties(run)).toHaveProperty("recursive", { type: "boolean" });
    expect(properties(spawn)).toHaveProperty("cwd");
    expect(properties(handoff)).not.toHaveProperty("cwd");
    expect(properties(create)).not.toHaveProperty("cwd");
  });

  it("rejects invalid durable recursive cwd before the provider can transfer ownership", async () => {
    const { provider, root } = setup();

    await expect(
      provider.invoke(
        "spawn",
        { task: "must remain recursive", cwd: path.join(root, "missing"), recursive: true, residency: "durable" },
        context,
      ),
    ).rejects.toThrow(/Invalid Fabric agent cwd/);
    expect(fs.existsSync(path.join(root, "runs"))).toBe(false);
  });

  it("shows the effective cwd in run and spawn launch activity", async () => {
    const { provider } = setup();
    const updates: string[] = [];
    const invocationContext = { ...context, update: (message: string) => updates.push(message) };
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-agent-activity-"));
    roots.push(root);
    const target = path.join(root, "leaf");
    fs.mkdirSync(target);
    const requested = path.join(root, "leaf-link");
    fs.symlinkSync(target, requested, "dir");
    const canonical = fs.realpathSync(target);

    const runResult = await provider.invoke(
      "run",
      { task: "report the launch directory", cwd: requested, recursive: true },
      invocationContext,
    ) as { cwd: string };
    expect(runResult.cwd).toBe(canonical);
    expect(updates.some((message) => message.endsWith(`cwd ${canonical}`))).toBe(true);

    updates.length = 0;
    const handle = await provider.invoke(
      "spawn",
      { task: "report the launch directory", cwd: requested, recursive: true },
      invocationContext,
    ) as { id: string; cwd: string };
    expect(handle.cwd).toBe(canonical);
    expect(updates.some((message) => message.endsWith(`cwd ${canonical}`))).toBe(true);
    await provider.invoke("wait", { id: handle.id }, invocationContext);
    await provider.invoke("cleanup", { id: handle.id }, invocationContext);
  });

  it.skipIf(process.platform === "win32")("bounds and escapes control characters in cwd launch activity", async () => {
    const { provider } = setup();
    const updates: string[] = [];
    const invocationContext = { ...context, update: (message: string) => updates.push(message) };
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-agent-activity-"));
    roots.push(root);
    const longPart = "x".repeat(96);
    const target = path.join(root, longPart, longPart, `leaf-\u001b-${"y".repeat(96)}`);
    fs.mkdirSync(target, { recursive: true });
    const requested = path.join(root, "leaf-link");
    fs.symlinkSync(target, requested, "dir");
    const canonical = fs.realpathSync(target);
    const safe = canonical.replace(/[\u0000-\u001f\u007f]/g, (character) =>
      `\\u${character.codePointAt(0)!.toString(16).padStart(4, "0")}`,
    );
    const shown = safe.length <= 240 ? safe : `…${safe.slice(-239)}`;

    const runResult = await provider.invoke(
      "run",
      { task: "report the launch directory", cwd: requested, recursive: true },
      invocationContext,
    ) as { cwd: string };
    expect(runResult.cwd).toBe(canonical);
    expect(updates.some((message) => message.endsWith(`cwd ${shown}`))).toBe(true);

    updates.length = 0;
    const handle = await provider.invoke(
      "spawn",
      { task: "report the launch directory", cwd: requested, recursive: true },
      invocationContext,
    ) as { id: string; cwd: string };
    expect(handle.cwd).toBe(canonical);
    expect(updates.some((message) => message.endsWith(`cwd ${shown}`))).toBe(true);
    expect(updates.every((message) => !/[\u0000-\u001f\u007f]/.test(message))).toBe(true);
    expect(updates.every((message) => message.length < 512)).toBe(true);
    await provider.invoke("wait", { id: handle.id }, invocationContext);
    await provider.invoke("cleanup", { id: handle.id }, invocationContext);
  });

  // smarty-dev#2119: an interactive Main's wait is capped at 60 s and the bound is a normal result.
  it("returns the live status at the bound in an interactive Main; a task agent's wait still throws", async () => {
    const { provider, mainAgent } = setup();
    const mainContext = (mode: string) => ({
      ...context,
      extensionContext: { ...context.extensionContext, mode, sessionManager: { getSessionId: () => "test" } } as unknown as ExtensionContext,
    });
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    try {
      for (const mode of ["tui", "rpc"]) {
        const handle = await provider.invoke("spawn", { task: "LIVE_WITH_PROGRESS", transport: "process" }, mainContext(mode)) as { id: string };
        const started = Date.now();
        const result = await provider.invoke("wait", { id: handle.id, timeoutMs: 1_000 }, mainContext(mode)) as Record<string, unknown>;
        expect(Date.now() - started).toBeLessThan(1_400);
        expect(result).toMatchObject({ id: handle.id, status: "running", waitTimedOut: true });
        expect(result.note).toMatch(/Still running after 1 s; Main waits are capped at 60 s/);
        expect(mainAgent.flushHeldAtNextBoundary).toHaveBeenCalledTimes(mode === "tui" ? 1 : 2);
        await expect(provider.invoke("wait", { id: handle.id }, mainContext(mode))).resolves.toMatchObject({ status: "completed" });
      }
      // Print mode is a script, not an interactive Main: the #854 error is unchanged.
      const scripted = await provider.invoke("spawn", { task: "LIVE_WITH_PROGRESS", transport: "process" }, mainContext("print")) as { id: string };
      await expect(provider.invoke("wait", { id: scripted.id, timeoutMs: 1_000 }, mainContext("print"))).rejects.toThrow(/is still running after 1 s\. It continues/);
      // A task agent (PI_FABRIC_PARENT_RUN set) in RPC mode keeps the #854 error too.
      vi.stubEnv("PI_FABRIC_PARENT_RUN", "parent-run");
      const child = await provider.invoke("spawn", { task: "LIVE_WITH_PROGRESS", transport: "process" }, mainContext("rpc")) as { id: string };
      await expect(provider.invoke("wait", { id: child.id, timeoutMs: 1_000 }, mainContext("rpc"))).rejects.toThrow(/is still running after 1 s\. It continues/);
      expect(mainAgent.flushHeldAtNextBoundary).toHaveBeenCalledTimes(2);           // print and task-agent waits flush nothing
    } finally {
      vi.unstubAllEnvs();
    }
  }, 30_000);

  it.each([undefined, 86_400_000])("bounds Main agents.run at 60 s without stopping its child (request %s)", async timeoutMs => {
    const { provider, agents, mainAgent } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const controller = new AbortController();
    const mainContext = {
      ...context, signal: controller.signal,
      extensionContext: { ...context.extensionContext, mode: "tui", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
    };
    const realWait = agents.wait.bind(agents);
    let waitOptions: Parameters<AgentManager["wait"]>[1];
    const waiting = new Promise<void>(resolve => {
      vi.spyOn(agents, "wait").mockImplementation((id, options) => {
        waitOptions = options;
        vi.useFakeTimers();
        resolve();
        return realWait(id, options);
      });
    });
    const run = provider.invoke("run", { task: "HANG", transport: "process", timeoutMs }, mainContext);
    try {
      await waiting;
      let settled = false;
      void run.then(() => { settled = true; });
      await vi.advanceTimersByTimeAsync(59_999);
      expect(settled).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect(settled).toBe(true);
      const result = await run as Record<string, unknown>;
      expect(waitOptions?.timeoutMs).toBe(60_000);
      expect(result).toMatchObject({ status: "running", waitTimedOut: true });
      expect(result.note).toMatch(/continues.*completion message/);
      expect(mainAgent.flushHeldAtNextBoundary).toHaveBeenCalledOnce();
      controller.abort();
      expect(agents.status(String(result.id)).status).toBe("running");
    } finally {
      vi.useRealTimers();
      vi.unstubAllEnvs();
      // On the unfixed base, bound the test's otherwise unbounded run before closing.
      await Promise.all(agents.list().map(handle => agents.stop(handle.id)));
      await run;
    }
  });

  it("returns Main agents.run before a 60 s program ceiling and continues the guest", async () => {
    const { provider, agents, mainAgent } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.mainMaxTimeoutMs = 60_000;
    const service = new FabricExecutionService(registry, config);
    const options = {
      signal: undefined, parentToolCallId: "main-run-observation-budget", onPartial() {},
      context: { ...context.extensionContext, cwd: process.cwd(), mode: "tui", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
    };
    // Warm the optional runtime, then use a real local worker without clocking its process.
    await service.execute({ ...options, code: "return 1;" });
    const handle = await agents.spawn({ task: "HANG", transport: "process" });
    let launching!: () => void;
    const launched = new Promise<void>(resolve => { launching = resolve; });
    vi.spyOn(agents, "spawn").mockImplementationOnce(async () => {
      launching();
      await new Promise(resolve => setTimeout(resolve, 250));
      return handle;
    });
    const realWait = agents.wait.bind(agents);
    let waitOptions: Parameters<AgentManager["wait"]>[1];
    const waiting = new Promise<void>(resolve => {
      vi.spyOn(agents, "wait").mockImplementation((id, options) => {
        waitOptions = options;
        resolve();
        return realWait(id, options);
      });
    });
    vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
    const program = service.execute({
      ...options,
      code: `const result = await agents.run({ task: "HANG", transport: "process" });
const after = await agents.status({ id: result.id });
return { result, after, tail: "continued" };`,
    });
    try {
      await launched;
      await vi.advanceTimersByTimeAsync(250);
      await waiting;
      let settled = false;
      void program.then(() => { settled = true; });
      await vi.advanceTimersByTimeAsync(57_750);
      // Bound the red baseline too: its program ceiling wins instead of returning the run.
      if (!settled) await vi.advanceTimersByTimeAsync(2_000);
      const result = await program;
      expect(waitOptions?.timeoutMs).toBe(57_750);
      expect(result.success).toBe(true);
      expect(result.value).toMatchObject({
        result: { id: handle.id, status: "running", waitTimedOut: true },
        after: { id: handle.id, status: "running" }, tail: "continued",
      });
      expect(mainAgent.flushHeldAtNextBoundary).toHaveBeenCalledOnce();
      expect(agents.status(handle.id).status).toBe("running");
    } finally {
      // Also settle the unfixed baseline's deadline so a red test leaves no suspended VM.
      await vi.advanceTimersByTimeAsync(2_000);
      await program;
      vi.useRealTimers();
      vi.unstubAllEnvs();
    }
  });

  it.each(["wait", "join"] as const)(
    "budgets successive Main %s observations against the same absolute ceiling", async action => {
      const { provider, agents } = setup();
      vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
      vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
      const handle = await agents.spawn({ task: "HANG", transport: "process" });
      const registry = new ActionRegistry();
      registry.register(provider);
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      config.executor.mainMaxTimeoutMs = 60_000;
      const service = new FabricExecutionService(registry, config);
      const options = {
        signal: undefined, parentToolCallId: "main-successive-observations", onPartial() {},
        context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
      };
      await service.execute({ ...options, code: "return 1;" });
      const realWait = agents.wait.bind(agents);
      const wait = vi.spyOn(agents, "wait");
      let observing!: () => void;
      const observation = new Promise<void>(resolve => { observing = resolve; });
      wait.mockImplementation((id, options) => { observing(); return realWait(id, options); });
      vi.useFakeTimers({ toFake: ["Date", "setTimeout", "clearTimeout", "setInterval", "clearInterval"] });
      const program = service.execute({
        ...options,
        code: `const first = await agents.${action}({ id: ${JSON.stringify(handle.id)}, timeoutMs: 1800000 });
const second = await agents.${action}({ id: first.id, timeoutMs: 1800000 });
return { first, second, tail: "continued" };`,
      });
      try {
        await observation;
        let settled = false;
        void program.then(() => { settled = true; });
        await vi.advanceTimersByTimeAsync(59_000);
        if (!settled) await vi.advanceTimersByTimeAsync(1_000);
        const result = await program;
        expect(wait.mock.calls.map(([, options]) => options?.timeoutMs)).toEqual([58_000, 1_000]);
        expect(result.success).toBe(true);
        expect(result.value).toMatchObject({
          first: { status: "running", waitTimedOut: true },
          second: { status: "running", waitTimedOut: true }, tail: "continued",
        });
        expect(agents.status(handle.id).status).toBe("running");
      } finally {
        await vi.advanceTimersByTimeAsync(1_000);
        await program;
        vi.useRealTimers();
        vi.unstubAllEnvs();
      }
    },
  );

  it.each(["wait", "join"] as const)(
    "budgets a durable Main %s observation without consuming its running result", async action => {
      const state = setup();
      vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
      vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
      const durable = { id: "durable-child", name: "Durable child", status: "running" };
      const stopAgent = vi.fn();
      const acknowledgeCompletion = vi.fn();
      const residency = {
        hasAgent: () => true, statusAgent: () => durable, stopAgent, acknowledgeCompletion,
        waitAgent: vi.fn((_id: string, signal: AbortSignal) => new Promise((_, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        })),
      } as unknown as ResidencyClient;
      const provider = new AgentsProvider(state.agents, state.actors, state.globalActors, state.mainAgent,
        state.participants, state.control, state.lifecycle, undefined, residency, false);
      const registry = new ActionRegistry();
      registry.register(provider);
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      config.executor.mainMaxTimeoutMs = 3_200;
      try {
        const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
          code: `const result = await agents.${action}({ id: "durable-child" }); return { result, tail: "continued" };`,
          signal, parentToolCallId: "main-durable-observation-budget",
          context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
          onPartial() {},
        }), () => vi.mocked(residency.waitAgent).mock.calls.length === 1);
        expect(result.success).toBe(true);
        expect(result.value).toMatchObject({ result: { ...durable, waitTimedOut: true }, tail: "continued" });
        expect(residency.waitAgent).toHaveBeenCalledOnce();
        expect(stopAgent).not.toHaveBeenCalled();
        expect(acknowledgeCompletion).not.toHaveBeenCalled();
      } finally { vi.unstubAllEnvs(); }
    },
  );

  it.each(["quickjs", "node-process", "monty", "cpython"] as const)(
    "returns a live Main run and guest tail before a small ceiling through %s", async backend => {
      const { provider, agents } = setup();
      vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
      vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
      const registry = new ActionRegistry();
      registry.register(provider);
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      config.executor.mainMaxTimeoutMs = 3_500;
      config.executor.memoryLimitBytes = 128 * 1024 * 1024;
      const python = backend === "monty" || backend === "cpython";
      if (python) { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
      else config.executor.runtime = backend;
      const wait = vi.spyOn(agents, "wait");
      try {
        const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
          code: python
            ? 'result = await agents.run(task="HANG", transport="process")\nreturn {"result": result, "tail": "continued"}'
            : 'const result = await agents.run({ task: "HANG", transport: "process" }); return { result, tail: "continued" };',
          signal, parentToolCallId: "main-small-run-budget",
          context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
          onPartial() {},
        }), () => wait.mock.calls.length === 1);
        expect(result.success).toBe(true);
        expect(result.value).toMatchObject({ result: { status: "running", waitTimedOut: true }, tail: "continued" });
        expect(wait.mock.calls[0]?.[1]?.timeoutMs).toBeGreaterThanOrEqual(1_000);
        expect(wait.mock.calls[0]?.[1]?.timeoutMs).toBeLessThanOrEqual(1_500);
        expect(agents.list()).toHaveLength(1);
        expect(agents.list()[0]).toMatchObject({ status: "running" });
      } finally { vi.unstubAllEnvs(); }
    },
  );

  it.each([
    'return agents.run({ task: "HANG", transport: "process" });',
    'const child = await agents.spawn({ task: "HANG", transport: "process" }); while (true) await agents.wait({ id: child.id });',
  ])("detaches a Main child with no progress when its program hits the ceiling: %s", async code => {
    const { provider, agents } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.mainMaxTimeoutMs = 200;
    const wait = vi.spyOn(agents, "wait");
    try {
      const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
        code,
        signal, parentToolCallId: "main-run-ceiling",
        context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
        onPartial() {},
      }), () => wait.mock.calls.length > 0);
      expect(result.error).toMatch(/MainExecutionCeilingError.*Main ceiling hit/);
      expect(result.trace.outcome).toBe("timed_out");
      expect(agents.list()).toHaveLength(1);
      expect(agents.list()[0]).toMatchObject({ status: "running" });
    } finally { vi.unstubAllEnvs(); }
  });

  it.each([
    ["tui", true, "quickjs"],
    ["rpc", false, "quickjs"],
    ["rpc", false, "node-process"],
    ["rpc", false, "monty"],
    ["rpc", false, "cpython"],
  ] as const)(
    "detaches only a zero-progress local actor ASK observation at the Main ceiling (%s, fullCodeMode=%s, backend=%s)",
    async (mode, fullCodeMode, backend) => {
      const { provider, actors, agents, actorDeliveries } = setup();
      vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
      vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
      const actor = await actors.create({
        name: "late-advisor", instructions: "Advise.", responseMode: "text", delivery: "followUp", triggerTurn: false, transport: "process",
      });
      const registry = new ActionRegistry();
      registry.register(provider);
      const config = structuredClone(DEFAULT_FABRIC_CONFIG);
      config.fullCodeMode = fullCodeMode;
      config.executor.memoryLimitBytes = 128 * 1024 * 1024;
      const python = backend === "monty" || backend === "cpython";
      if (python) { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
      else config.executor.runtime = backend;
      config.executor.mainMaxTimeoutMs = 700;
      const stop = vi.spyOn(agents, "stop");
      const run = vi.spyOn(agents, "run");
      try {
        const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
          code: python
            ? `return await agents.ask(id=${JSON.stringify(actor.id)}, message="LIVE_WITHOUT_PROGRESS")`
            : `return agents.ask({ id: ${JSON.stringify(actor.id)}, message: "LIVE_WITHOUT_PROGRESS" });`,
          signal, parentToolCallId: "main-local-ask-ceiling",
          context: { ...context.extensionContext, cwd: process.cwd(), mode, sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
          onPartial() {},
        }), () => Boolean(actors.status(actor.id).inFlightRun) && Boolean(agents.list()[0] && "turns" in agents.list()[0]!));
        expect(result.error).toMatch(/MainExecutionCeilingError.*Main ceiling hit/);
        expect(result.trace.outcome).toBe("timed_out");
        expect(run).toHaveBeenCalledOnce();
        expect(actors.status(actor.id).inFlightRun).toBeDefined();
        expect(agents.list()).toHaveLength(1);
        expect(agents.list()[0]).toMatchObject({ status: "running", turns: 0, toolCalls: 0 });
        expect(stop).not.toHaveBeenCalled();
        expect(actorDeliveries).toEqual([]);
        // The actor, not the expired observer, owns result history and delivery.
        await waitFor(() => actorDeliveries.length === 1 && actors.status(actor.id).status === "idle", 3_000);
        expect(actorDeliveries[0]).toMatchObject({ delivery: "followUp", message: { actorId: actor.id, text: "live attempt 1 complete" } });
        expect(actors.messages(actor.id).filter(message => message.direction === "out")).toMatchObject([
          { actorId: actor.id, text: "live attempt 1 complete" },
        ]);
        expect(run).toHaveBeenCalledOnce();
        expect(stop).not.toHaveBeenCalled();
        expect(agents.list()).toHaveLength(0); // completed actor run was retained then cleaned
      } finally { vi.unstubAllEnvs(); }
    },
  );

  it.each(["Escape", "ordinary deadline", "explicit stop", "non-Main abort"] as const)(
    "still stops a zero-progress local actor ASK on %s",
    async cancellation => {
      const { provider, actors, agents, actorDeliveries } = setup();
      vi.stubEnv("PI_FABRIC_PARENT_RUN", cancellation === "non-Main abort" ? "parent-run" : "");
      vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
      const actor = await actors.create({ name: "stoppable-advisor", instructions: "Advise.", responseMode: "text", delivery: "followUp", triggerTurn: false, transport: "process" });
      const controller = new AbortController();
      const stop = vi.spyOn(agents, "stop");
      const observation = provider.invoke("ask", { id: actor.id, message: "HANG" }, {
        ...context, signal: controller.signal,
        extensionContext: { ...context.extensionContext, mode: "rpc", sessionManager: { getSessionId: () => "caller" } } as unknown as ExtensionContext,
      });
      const outcome = observation.catch(error => error);
      try {
        await waitFor(() => {
          const worker = agents.list()[0];
          return Boolean(actors.status(actor.id).inFlightRun) && worker?.status === "running" && "turns" in worker;
        });
        expect(agents.list()[0]).toMatchObject({ turns: 0, toolCalls: 0 });
        if (cancellation === "explicit stop") await provider.invoke("stop", { id: actor.id }, context);
        else controller.abort(cancellation === "non-Main abort"
          ? new Error("MainExecutionCeilingError: Main ceiling hit after 700ms (executor.mainMaxTimeoutMs).")
          : new Error(cancellation === "ordinary deadline" ? "Execution timed out" : "Escape"));
        expect(await outcome).toBeInstanceOf(Error);
        expect((await outcome as Error).message).toMatch(/Agent stopped|Operation aborted/);
        await waitFor(() => agents.list().every(run => run.status === "stopped"));
        expect(stop).toHaveBeenCalled();
        expect(actorDeliveries).toEqual([]);
        expect(actors.messages(actor.id).filter(message => message.direction === "out" && message.text)).toEqual([]);
      } finally { vi.unstubAllEnvs(); }
    },
  );

  it("keeps explicit stop effective after Main's ceiling detaches a local ASK", async () => {
    const { provider, actors, agents, actorDeliveries } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const actor = await actors.create({ name: "stop-after-ceiling", instructions: "Advise.", responseMode: "text", transport: "process" });
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.mainMaxTimeoutMs = 700;
    try {
      const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
        code: `return agents.ask({ id: ${JSON.stringify(actor.id)}, message: "HANG" });`,
        signal, parentToolCallId: "main-ask-stop-after-ceiling",
        context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
        onPartial() {},
      }), () => Boolean(actors.status(actor.id).inFlightRun) && Boolean(agents.list()[0] && "turns" in agents.list()[0]!));
      expect(result.error).toMatch(/MainExecutionCeilingError.*Main ceiling hit/);
      expect(agents.list()[0]).toMatchObject({ status: "running", turns: 0, toolCalls: 0 });
      await provider.invoke("stop", { id: actor.id }, context);
      await waitFor(() => agents.list().every(run => run.status === "stopped"));
      expect(actors.status(actor.id).status).toBe("stopped");
      expect(actorDeliveries).toEqual([]);
    } finally { vi.unstubAllEnvs(); }
  });

  it.each((["quickjs", "node-process", "monty", "cpython"] as const).flatMap(backend =>
    [false, true].map(queued => [backend, queued] as const)))(
    "rearms an early runtime Main deadline without cancelling accepted ASK work (%s, queued=%s)", async (backend, queued) => {
      // Exercise both a queued ASK and a zero-progress in-flight ASK with real workers.
        const { provider, actors, agents, actorDeliveries } = setup();
        vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
        vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
        const actor = await actors.create({ name: `early-${queued}`, instructions: "Advise.", responseMode: "text", delivery: "followUp", triggerTurn: false, transport: "process" });
        const registry = new ActionRegistry();
        registry.register(provider);
        const config = structuredClone(DEFAULT_FABRIC_CONFIG);
        config.executor.mainMaxTimeoutMs = 700;
        config.executor.memoryLimitBytes = 128 * 1024 * 1024;
        const python = backend === "monty" || backend === "cpython";
        if (python) { config.executor.kernel = "python"; config.executor.pythonRuntime = backend; }
        else config.executor.runtime = backend;
        const mainContext = { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext;
        const service = new FabricExecutionService(registry, config);
        await service.prewarm(mainContext);
        const run = vi.spyOn(agents, "run");
        const stop = vi.spyOn(agents, "stop");
        const first = queued ? actors.ask(actor.id, "LIVE_WITHOUT_PROGRESS").catch(error => error) : undefined;
        if (queued) await waitFor(() => Boolean(actors.status(actor.id).inFlightRun) && "turns" in agents.list()[0]!);
        const activation = actors.status(actor.id).inFlightRun;
        let mainDeadlineAt: number | undefined;
        const invoke = provider.invoke.bind(provider);
        vi.spyOn(provider, "invoke").mockImplementation((action, args, callContext) => {
          mainDeadlineAt = callContext.mainDeadlineAt;
          return invoke(action, args, callContext);
        });
        const timer = captureRuntimeDeadline(backend);
        const message = queued ? "queued advice" : "LIVE_WITHOUT_PROGRESS";
        const execution = executeAfterAdmission(signal => service.execute({
          code: python ? `return await agents.ask(id=${JSON.stringify(actor.id)}, message=${JSON.stringify(message)})`
            : `return agents.ask({ id: ${JSON.stringify(actor.id)}, message: ${JSON.stringify(message)} });`,
          context: mainContext, signal, parentToolCallId: "early-runtime-ask", onPartial() {},
        }), () => timer.ready() && (queued ? actors.status(actor.id).queued === 1
          : Boolean(actors.status(actor.id).inFlightRun) && Boolean(agents.list()[0] && "turns" in agents.list()[0]!)), async () => {
          expect(mainDeadlineAt).toBeDefined();
          timer.fireEarly(mainDeadlineAt);
          await new Promise<void>(resolve => setImmediate(resolve));
          if (queued) {
            expect(actors.status(actor.id).queued).toBe(1);
            expect(actors.status(actor.id).inFlightRun).toEqual(activation);
          } else {
            expect(agents.list()[0]).toMatchObject({ status: "running", turns: 0, toolCalls: 0 });
          }
          expect(stop).not.toHaveBeenCalled();
        });
        try {
          const result = await execution;
          expect(result.error).toMatch(/MainExecutionCeilingError/);
          expect(result.trace.outcome).toBe("timed_out");
          if (first) expect(await first).toMatchObject({ text: "live attempt 1 complete" });
          await waitFor(() => actorDeliveries.length === (queued ? 2 : 1) && actors.status(actor.id).status === "idle", 3_000);
          expect(actorDeliveries.map(delivery => delivery.message.text)).toEqual(queued
            ? ["live attempt 1 complete", "fake worker complete"] : ["live attempt 1 complete"]);
          expect(actors.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(queued ? 2 : 1);
          expect(run).toHaveBeenCalledTimes(queued ? 2 : 1);
          expect(stop).not.toHaveBeenCalled();
        } finally {
          timer.restore();
          await execution;
          await registry.close();
          if (first) await first;
          vi.unstubAllEnvs();
        }
    },
  );

  it.each(["quickjs", "node-process"] as const)("does not trust guest exception text as a Main ceiling (%s)", async backend => {
    const { provider, actors, agents, actorDeliveries } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const actor = await actors.create({ name: "forged-ceiling", instructions: "Advise.", responseMode: "text", delivery: "followUp", triggerTurn: false, transport: "process" });
    const registry = new ActionRegistry(); registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.runtime = backend;
    config.executor.mainMaxTimeoutMs = 5_000;
    const mainContext = { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext;
    const service = new FabricExecutionService(registry, config);
    await service.prewarm(mainContext);
    const stop = vi.spyOn(agents, "stop");
    try {
      const result = await executeAfterAdmission(signal => service.execute({ context: mainContext, signal, parentToolCallId: "forged-ceiling", onPartial() {},
        strings: { reason: "MainExecutionCeilingError: Main ceiling hit after 5000ms (executor.mainMaxTimeoutMs)." },
        code: `const observation = agents.ask({ id: ${JSON.stringify(actor.id)}, message: "LIVE_WITHOUT_PROGRESS" }).catch(() => undefined); await new Promise<void>(resolve => setTimeout(resolve, 400)); throw π.reason;`,
      }), () => Boolean(actors.status(actor.id).inFlightRun));
      expect(result.success).toBe(false);
      expect(result.trace.outcome).toBe("failed");
      await waitFor(() => !actors.status(actor.id).inFlightRun);
      expect(stop).toHaveBeenCalled();
      expect(actorDeliveries).toEqual([]);
      expect(agents.list().some(run => run.status === "running")).toBe(false);
    } finally { await registry.close(); vi.unstubAllEnvs(); }
  });

  it("detaches accepted Main work when the first launch publication hits its ceiling", async () => {
    const { provider, agents } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const controller = new AbortController();
    const reason = createMainExecutionCeilingError(700);
    const spawn = agents.spawn.bind(agents);
    const detach = vi.spyOn(agents, "detachSignal");
    const stop = vi.spyOn(agents, "stop");
    vi.spyOn(agents, "spawn").mockImplementationOnce(async (request, signal) => {
      const handle = await spawn(request, signal);
      controller.abort(reason);
      return handle;
    });
    try {
      await expect(provider.invoke("run", { task: "LIVE_WITHOUT_PROGRESS", transport: "process" }, {
        ...context, signal: controller.signal,
        extensionContext: { ...context.extensionContext, mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
        activity() { if (controller.signal.aborted) throw controller.signal.reason; },
      })).rejects.toBe(reason);
      const id = agents.list()[0]!.id;
      expect(detach).toHaveBeenCalledExactlyOnceWith(id);
      expect(stop).not.toHaveBeenCalled();
      await waitFor(() => agents.status(id).status === "completed", 3_000);
      expect(agents.status(id)).toMatchObject({ status: "completed", text: "live attempt 1 complete" });
    } finally { vi.unstubAllEnvs(); }
  });

  it.each(["rpc", "tui", "print"] as const)("keeps ordinary Escape cancellation for zero-progress agents.run (%s)", async mode => {
    const { provider, agents } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const controller = new AbortController();
    const stop = vi.spyOn(agents, "stop");
    const observation = provider.invoke("run", { task: "LIVE_WITHOUT_PROGRESS", transport: "process" }, {
      ...context, signal: controller.signal,
      extensionContext: { ...context.extensionContext, mode, sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
    }).catch(error => error);
    try {
      await waitFor(() => agents.list().length === 1 && "turns" in agents.list()[0]!);
      const id = agents.list()[0]!.id;
      expect(agents.status(id)).toMatchObject({ status: "running", turns: 0, toolCalls: 0 });
      controller.abort(new Error("Escape"));
      if (mode === "print") expect(await observation).toMatchObject({ status: "stopped" });
      else expect(await observation).toBeInstanceOf(Error);
      await waitFor(() => agents.status(id).status !== "running");
      expect(agents.status(id).status).toBe("stopped");
      expect(stop).toHaveBeenCalled();
    } finally { controller.abort(); await observation; vi.unstubAllEnvs(); }
  });

  it("preserves a queued local ASK and its late delivery at Main's ceiling", async () => {
    const { provider, actors, agents, actorDeliveries } = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const actor = await actors.create({ name: "queued-advisor", instructions: "Advise.", responseMode: "text", delivery: "followUp", triggerTurn: false, transport: "process" });
    const first = actors.ask(actor.id, "LIVE_WITHOUT_PROGRESS");
    const firstOutcome = first.catch(error => error);
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.mainMaxTimeoutMs = 100;
    try {
      await waitFor(() => Boolean(actors.status(actor.id).inFlightRun) && "turns" in agents.list()[0]!);
      const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
        code: `return agents.ask({ id: ${JSON.stringify(actor.id)}, message: "queued advice" });`,
        signal, parentToolCallId: "main-queued-ask-ceiling",
        context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
        onPartial() {},
      }), () => actors.status(actor.id).queued === 1);
      expect(result.error).toMatch(/MainExecutionCeilingError.*Main ceiling hit/);
      expect(actors.status(actor.id).queued).toBe(1);
      expect(await firstOutcome).toMatchObject({ text: "live attempt 1 complete" });
      await waitFor(() => actorDeliveries.length === 2 && actors.status(actor.id).status === "idle");
      expect(actorDeliveries.map(delivery => delivery.message.text)).toEqual(["live attempt 1 complete", "fake worker complete"]);
      expect(actors.messages(actor.id).filter(message => message.direction === "out")).toHaveLength(2);
    } finally { vi.unstubAllEnvs(); }
  });

  it("ends only the durable wait at the Main program ceiling, without stopping or acknowledging its run", async () => {
    const state = setup();
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "");
    vi.stubEnv("PI_FABRIC_ACTOR_ID", "");
    const durable = { id: "durable-child", name: "Durable child", status: "running" };
    const stopAgent = vi.fn();
    const acknowledgeCompletion = vi.fn();
    const residency = {
      hasAgent: () => true, statusAgent: () => durable, stopAgent, acknowledgeCompletion,
      waitAgent: vi.fn((_id: string, signal: AbortSignal) => new Promise((_, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
      })),
    } as unknown as ResidencyClient;
    const provider = new AgentsProvider(state.agents, state.actors, state.globalActors, state.mainAgent,
      state.participants, state.control, state.lifecycle, undefined, residency, false);
    const registry = new ActionRegistry();
    registry.register(provider);
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.executor.mainMaxTimeoutMs = 50;
    try {
      const result = await executeAfterAdmission(signal => new FabricExecutionService(registry, config).execute({
        code: 'return agents.wait({ id: "durable-child" });',
        signal, parentToolCallId: "main-durable-ceiling",
        context: { ...context.extensionContext, cwd: process.cwd(), mode: "rpc", sessionManager: { getSessionId: () => "main" } } as unknown as ExtensionContext,
        onPartial() {},
      }), () => vi.mocked(residency.waitAgent).mock.calls.length === 1);
      expect(result.error).toMatch(/MainExecutionCeilingError.*Main ceiling hit/);
      expect(residency.waitAgent).toHaveBeenCalledOnce();
      expect(durable.status).toBe("running");
      expect(stopAgent).not.toHaveBeenCalled();
      expect(acknowledgeCompletion).not.toHaveBeenCalled();
    } finally { vi.unstubAllEnvs(); }
  });

  it("exposes the compact option on handoff only and validates it before deferring", async () => {
    const { provider, root } = setup();
    const source = SessionManager.inMemory(root);
    const handoffContext = {
      ...context,
      extensionContext: {
        ...context.extensionContext,
        sessionManager: source,
      } as unknown as ExtensionContext,
    };
    const handoffDescriptor = await provider.describe("handoff", handoffContext);
    const handoffSchema = handoffDescriptor?.inputSchema as { properties: Record<string, unknown> };
    expect(handoffSchema.properties).toHaveProperty("compact");
    const runDescriptor = await provider.describe("run", handoffContext);
    expect(
      (runDescriptor?.inputSchema as { properties: Record<string, unknown> }).properties,
    ).not.toHaveProperty("compact");
    const spawnDescriptor = await provider.describe("spawn", handoffContext);
    expect(
      (spawnDescriptor?.inputSchema as { properties: Record<string, unknown> }).properties,
    ).not.toHaveProperty("compact");

    await expect(
      provider.invoke(
        "handoff",
        { model: "anthropic/executor", compact: "yes" },
        handoffContext,
      ),
    ).rejects.toThrow(/must be true or an object/);
    await expect(
      provider.invoke(
        "handoff",
        {
          model: "anthropic/executor",
          compact: { preserve: Array.from({ length: 17 }, (_, index) => String(index)) },
        },
        handoffContext,
      ),
    ).rejects.toThrow(/exceeds 16 items/);
  });

  it("compacts the handed-off trajectory when the caller requests it", async () => {
    const { provider, root } = setup();
    const source = SessionManager.create(process.cwd(), path.join(root, "source-session"));
    source.appendMessage({
      role: "user",
      content: "Implement the rare token guard 43117",
      timestamp: 1,
    });
    source.appendMessage({
      role: "assistant",
      content: [
        {
          type: "text",
          text: `Long scratch exploration of guard internals. ${"filler ".repeat(30)}SCRATCH_TAIL_99231`,
        },
      ],
      api: "anthropic",
      provider: "anthropic",
      model: "frontier",
      usage,
      stopReason: "stop",
      timestamp: 2,
    });
    source.appendMessage({ role: "user", content: "Proceed", timestamp: 3 });
    source.appendMessage({
      role: "assistant",
      content: [
        { type: "text", text: "Completing the full program at the boundary." },
        {
          type: "toolCall",
          id: context.parentToolCallId,
          name: "fabric_exec",
          arguments: { code: "await pi.edit(...); return 'verified';" },
        },
      ],
      api: "anthropic",
      provider: "anthropic",
      model: "frontier",
      usage,
      stopReason: "toolUse",
      timestamp: 4,
    });
    let deferredRequest: Record<string, unknown> | undefined;
    const handoffContext: FabricInvocationContext = {
      ...context,
      extensionContext: {
        ...context.extensionContext,
        sessionManager: source,
        model: { provider: "anthropic", id: "frontier" },
      } as unknown as ExtensionContext,
      deferHandoff(args) {
        deferredRequest = structuredClone(args);
        return {
          scheduled: true,
          status: "deferred",
          boundary: "fabric_exec_end",
        };
      },
    };
    const args = {
      model: "anthropic/executor",
      transport: "process",
      compact: { preserve: ["Guard threshold stays at 90 percent 5678"] },
    };

    await expect(provider.invoke("handoff", args, handoffContext)).resolves.toMatchObject({
      status: "deferred",
      boundary: "fabric_exec_end",
    });
    expect(deferredRequest).toEqual({ ...args, extensions: true, kernel: "typescript", pythonRuntime: "monty" });

    const outerToolResult = {
      role: "toolResult" as const,
      toolCallId: context.parentToolCallId,
      toolName: "fabric_exec",
      content: [{ type: "text" as const, text: "verified after every nested call" }],
      details: { success: true },
      isError: false,
      timestamp: 5,
    };
    const seed = snapshotHandoffSession(
      source,
      { provider: "anthropic", id: "frontier" },
      outerToolResult,
      context.parentToolCallId,
    );
    const result = (await provider.executeHandoff(
      deferredRequest!,
      handoffContext,
      seed,
    )) as { handedOff: boolean; completed: boolean; agent: { id: string } };
    expect(result).toMatchObject({ handedOff: true, completed: true });

    const handoffDirectory = path.join(root, "runs", result.agent.id, "handoff-session");
    const [sessionName] = fs.readdirSync(handoffDirectory);
    const seededSession = SessionManager.open(path.join(handoffDirectory, sessionName!));
    const seededMessages = seededSession.buildSessionContext().messages;
    expect(seededMessages.map((message) => message.role)).toEqual([
      "compactionSummary",
      "user",
      "assistant",
      "toolResult",
    ]);
    expect(JSON.stringify(seededMessages[0])).toContain("Guard threshold stays at 90 percent 5678");
    expect(JSON.stringify(seededMessages[0])).toContain("Implement the rare token guard 43117");
    expect(JSON.stringify(seededMessages[0])).toContain("Historical assistant response (not a verified outcome)");
    expect(JSON.stringify(seededMessages[0])).toContain("SCRATCH_TAIL_99231");
    expect(JSON.stringify(seededMessages.slice(1))).not.toContain("SCRATCH_TAIL_99231");
    expect(
      seededSession.getEntries().some((entry) => JSON.stringify(entry).includes("SCRATCH_TAIL_99231")),
    ).toBe(true);
    expect(
      seededSession.getEntries().some((entry) => entry.type === "compaction"),
    ).toBe(true);
    expect(seededSession.getEntries().at(-1)).toMatchObject({
      type: "custom",
      customType: "pi-fabric-handoff",
      data: { compaction: { applied: true } },
    });
  });

  it("attaches a structured child-tool preview to blocking agent runs", async () => {
    const { provider } = setup();
    const previews: unknown[] = [];
    const previewContext: FabricInvocationContext = {
      ...context,
      attachPreview(preview) {
        previews.push(preview);
      },
    };

    await provider.invoke(
      "run",
      { task: "return a short result", name: "preview-agent", transport: "process" },
      previewContext,
    );

    expect(previews.at(-1)).toMatchObject({
      kind: "fabric-agent-tools",
      name: "preview-agent",
      status: "completed",
      runner: "pi",
      owner: "agent",
      text: "fake worker complete",
      tools: expect.any(Array),
    });
  });

  it("refreshes bounded agent previews when only the transcript changes", async () => {
    const { provider } = setup();
    const previews: Array<Record<string, unknown>> = [];
    const previewContext: FabricInvocationContext = {
      ...context,
      attachPreview(preview) {
        previews.push(preview as Record<string, unknown>);
      },
    };

    await provider.invoke(
      "run",
      { task: "STREAM_PREVIEW", name: "stream-preview-agent", transport: "process" },
      previewContext,
    );

    const liveTools = previews
      .filter((preview) => preview.status === "running")
      .flatMap((preview) => preview.tools as Array<{ label?: string; toolName?: string }> ?? []);
    expect(liveTools.some((tool) => (tool.toolName ?? tool.label) === "read")).toBe(true);
    expect(liveTools.some((tool) => (tool.toolName ?? tool.label) === "bash")).toBe(true);
    expect(previews.length).toBeLessThanOrEqual(4);
  }, 10_000);

  it("acknowledges model-facing terminal status but not running status or UI polling", async () => {
    const { provider, agents } = setup();
    const acknowledge = vi.spyOn(agents, "markForeground");
    const handle = await provider.invoke("spawn", { task: "return a short result", transport: "process" }, context) as { id: string };
    const initial = await provider.invoke("status", { id: handle.id }, context) as AgentRunRecord;
    if (initial.status === "running") expect(acknowledge).not.toHaveBeenCalled();
    acknowledge.mockClear();
    await waitFor(() => agents.status(handle.id).status === "completed");
    agents.listForUi();
    expect(acknowledge).not.toHaveBeenCalled();
    await provider.invoke("status", { id: handle.id }, context);
    expect(acknowledge).toHaveBeenCalledExactlyOnceWith(handle.id);
  });

  it.each(["wait", "join"])("attaches previews and reports friendly names through %s for spawned agents", async (method) => {
    const { provider, agents } = setup();
    const wait = vi.spyOn(agents, "wait");
    const updates: string[] = [];
    const previews: Array<Record<string, unknown>> = [];
    const previewContext: FabricInvocationContext = {
      ...context,
      update(message) {
        updates.push(message);
      },
      attachPreview(preview) {
        previews.push(preview as Record<string, unknown>);
      },
    };
    const handle = await provider.invoke(
      "spawn",
      { task: "return a short result", name: "wait-preview-agent", transport: "process" },
      previewContext,
    ) as { id: string; name: string };

    await provider.invoke(method, { id: handle.id }, previewContext);
    expect(wait).toHaveBeenCalledExactlyOnceWith(handle.id, { timeoutMs: 5 * 60_000 });   // the default bound (smarty-dev#854)

    expect(updates.some((message) => message.startsWith("Agent wait-preview-agent:"))).toBe(true);
    expect(updates.join("\n")).not.toContain(handle.id.slice(0, 8));
    expect(previews.at(-1)).toMatchObject({
      kind: "fabric-agent-tools",
      id: handle.id,
      name: "wait-preview-agent",
      status: "completed",
      owner: "agent",
    });
  });

  it("attaches the final preview for actors that settle before the first poll", async () => {
    const { provider } = setup();
    const actor = (await provider.invoke("create", createRequest, context)) as { id: string };
    const previews: Array<Record<string, unknown>> = [];
    const previewContext: FabricInvocationContext = {
      ...context,
      attachPreview(preview) {
        previews.push(preview as Record<string, unknown>);
      },
    };

    await provider.invoke("ask", { id: actor.id, message: "inspect quickly" }, previewContext);

    expect(previews.at(-1)).toMatchObject({
      kind: "fabric-agent-tools",
      status: "completed",
      owner: "actor",
      tools: expect.any(Array),
    });
  });

  it("ignores actor timeout overrides below the configured default", async () => {
    // Pin the configured default below the 24-hour ceiling: that is the only
    // configuration where a per-actor or per-call timeout can raise a run.
    const { provider, actors } = setup([], [], undefined, {
      agentsConfig: { timeoutMs: 3_600_000 },
    });
    const inherited = (await provider.invoke(
      "create",
      { ...createRequest, name: "inherited-timeout", timeoutMs: 240_000 },
      context,
    )) as { id: string };
    const longer = (await provider.invoke(
      "create",
      { ...createRequest, name: "longer-timeout", timeoutMs: 7_200_000 },
      context,
    )) as { id: string };

    expect(actors.definition(inherited.id)).not.toHaveProperty("timeoutMs");
    expect(actors.definition(longer.id).timeoutMs).toBe(7_200_000);
  });

  it("enumerates Claude models and preserves runner on actors", async () => {
    const { provider } = setup();
    const models = (await provider.invoke("models", { runner: "claude" }, context)) as Array<{
      runner: string;
      key: string;
      resolvedModel: string;
    }>;
    expect(models).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          runner: "claude",
          key: "claude/haiku",
          resolvedModel: "claude-haiku-test",
        }),
      ]),
    );

    const actor = (await provider.invoke(
      "create",
      {
        name: "claude-reviewer",
        instructions: "Review messages.",
        runner: "claude",
      },
      context,
    )) as { runner: string };
    expect(actor.runner).toBe("claude");
  });
});

describe("AgentsProvider shared actor definitions", () => {
  it("exposes the shared definition, mailbox, and logs while keeping mutation owner-gated", async () => {
    const members: FabricParticipantInfo[] = [];
    const { provider, actors } = setup([], members);
    const actor = await actors.create(createRequest as FabricActorRequest);
    members.push({
      format: 1,
      id: actor.id,
      kind: "actor",
      rootId: "session:peer",
      ownerHostId: "session:peer",
      ownerIdentityId: "session:peer",
      parentId: "session:peer",
      name: actor.name,
      status: "idle",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "stop", "ask", "actor-bindings", "fabric"],
      startedAt: actor.createdAt,
      updatedAt: actor.updatedAt,
      controlProtocol: "v1",
      local: false,
      stale: false,
    });

    await expect(provider.invoke("actors", {}, context)).resolves.toHaveLength(1);
    await expect(provider.invoke("actorStatus", { id: actor.id }, context)).resolves.toMatchObject({
      id: actor.id,
      name: actor.name,
    });
    await expect(provider.invoke("messages", { id: actor.id }, context)).resolves.toEqual([]);
    await expect(provider.invoke("log", { id: actor.id }, context)).resolves.toMatchObject({
      actorId: actor.id,
    });
    const read = (await provider.invoke("instructions", { id: actor.id }, context)) as {
      instructions: string;
      instructionsDigest: string;
      instructionsLength: number;
    };
    expect(read).toMatchObject({ id: actor.id, name: actor.name, instructions: createRequest.instructions });
    expect(read.instructionsDigest).toBe(
      createHash("sha256").update(createRequest.instructions).digest("hex"),
    );
    expect(read.instructionsLength).toBe(createRequest.instructions.length);
    // A read of the actor must not write a global template (smarty-dev#918).
    await expect(provider.invoke("export", { id: actor.id }, context)).rejects.toThrow(
      /write: true.*agents\.instructions/,
    );
    await expect(provider.invoke("actors", { scope: "global" }, context)).resolves.toEqual([]);
    await expect(provider.invoke("export", { id: actor.id, write: true }, context)).resolves.toMatchObject({
      name: actor.name,
    });
  });

  it("routes passive-session ask and tell with the caller's pinned binding", async () => {
    const members: FabricParticipantInfo[] = [];
    const response = {
      id: "remote-response",
      actorId: "pending",
      actorName: "reviewer",
      direction: "out" as const,
      source: "direct",
      createdAt: Date.now(),
      text: "remote answer",
    };
    const requestResult = vi.fn().mockResolvedValue(response);
    const request = vi.fn().mockResolvedValue({
      queued: true,
      messageId: "remote-message",
      routed: "mesh",
      acknowledged: true,
    });
    const control = { requestResult, request } as unknown as FabricControlPlane;
    const { provider, actors } = setup([], members, control);
    const actor = await actors.create({
      ...createRequest,
      model: "provider/project",
      thinking: "medium",
      timeoutMs: 2 * 60 * 60 * 1_000,
    } as FabricActorRequest);
    response.actorId = actor.id;
    await actors.cede(actor.id);
    await actors.setModel(actor.id, "provider/session");
    await actors.setThinking(actor.id, "low");
    members.push({
      format: 1,
      id: actor.id,
      kind: "actor",
      rootId: "session:owner",
      ownerHostId: "host:owner",
      ownerIdentityId: "identity:owner",
      parentId: "session:owner",
      name: actor.name,
      status: "idle",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "stop", "ask", "actor-bindings", "fabric"],
      startedAt: actor.createdAt,
      updatedAt: actor.updatedAt,
      controlProtocol: "v1",
      local: false,
      stale: false,
    });

    await expect(provider.invoke("ask", {
      id: actor.name,
      message: "review",
      model: "provider/one-off",
      thinking: "xhigh",
    }, context)).resolves.toEqual(response);
    expect(requestResult).toHaveBeenCalledWith(
      "host:owner",
      actor.id,
      "ask",
      expect.objectContaining({
        message: "review",
        binding: { model: "provider/one-off", thinking: "xhigh" },
      }),
      "identity:owner",
      { timeoutMs: 2 * 60 * 60 * 1_000 + 30_000, routedRemoteHost: null, detachOnMainCeiling: false },
    );

    await expect(
      provider.invoke(
        "ask",
        { id: actor.name, message: "x".repeat(62 * 1_024) },
        context,
      ),
    ).rejects.toThrow("after reserving the Fabric envelope");
    expect(requestResult).toHaveBeenCalledTimes(1);

    await provider.invoke("tell", { id: actor.name, message: "queue" }, context);
    expect(request).toHaveBeenCalledWith(
      "host:owner",
      actor.id,
      "followUp",
      expect.objectContaining({
        message: "queue",
        binding: { model: "provider/session", thinking: "low" },
      }),
      "identity:owner",
      { routedRemoteHost: null },
    );
  });

  // smarty-dev#1323: a spawner could not steer its durable child ("Unknown Fabric participant").
  it("routes steer and followUp to a remote agent through its owner", async () => {
    const child: FabricParticipantInfo = {
      format: 1, id: "4d7629e5a6e54f8aa913fa099d379593", kind: "agent", rootId: "session:test",
      ownerHostId: "resident:5c7d5dcf0ec46f0d40d91176", ownerIdentityId: "identity:resident", parentId: "session:test",
      name: "handoff", status: "running", residency: "durable", runner: "pi", transport: "process",
      capabilities: ["steer", "followUp", "stop"], startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: false, stale: false,
    };
    const request = vi.fn().mockResolvedValue({ queued: true, messageId: "m", routed: "mesh", acknowledged: true });
    const { provider } = setup([], [child], { request } as unknown as FabricControlPlane);
    for (const kind of ["steer", "followUp"] as const) {
      await expect(provider.routeMessage(child.id, `correct it (${kind})`, { key: "k" }, kind)).resolves.toMatchObject({ acknowledged: true });
      expect(request).toHaveBeenLastCalledWith(child.ownerHostId, child.id, kind, { message: `correct it (${kind})`, data: { key: "k" } }, child.ownerIdentityId, { routedRemoteHost: null });
    }
    await expect(provider.stopParticipant(child.id)).resolves.toMatchObject({ acknowledged: true });
    expect(request).toHaveBeenLastCalledWith(child.ownerHostId, child.id, "stop", {}, child.ownerIdentityId, { routedRemoteHost: null });
    const unsteerable = setup([], [{ ...child, capabilities: ["stop"] }], { request } as unknown as FabricControlPlane).provider;
    await expect(unsteerable.routeMessage(child.id, "no", undefined, "steer")).rejects.toThrow("does not support steer");
  });

  it("routes ask and tell for a remote actor absent from the local registry", async () => {
    const participant: FabricParticipantInfo = {
      format: 1,
      id: "actor:resident-child",
      kind: "actor",
      rootId: "session:test",
      ownerHostId: "host:resident",
      ownerIdentityId: "identity:resident",
      parentId: "session:test",
      name: "resident child",
      status: "idle",
      residency: "durable",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "stop", "ask", "actor-bindings", "fabric"],
      startedAt: 1,
      updatedAt: 1,
      controlProtocol: "v1",
      local: false,
      stale: false,
    };
    const members = [participant];
    const response = {
      id: "remote-response",
      actorId: participant.id,
      actorName: participant.name,
      direction: "out" as const,
      source: "direct",
      createdAt: Date.now(),
      text: "PONG",
    };
    const requestResult = vi.fn().mockResolvedValue(response);
    const request = vi.fn().mockResolvedValue({
      queued: true,
      messageId: "remote-message",
      routed: "mesh",
      acknowledged: true,
    });
    const control = { requestResult, request } as unknown as FabricControlPlane;
    const { provider } = setup([], members, control);

    await expect(provider.invoke("ask", {
      id: participant.id,
      message: "PING",
    }, context)).resolves.toEqual(response);
    expect(requestResult).toHaveBeenCalledWith(
      "host:resident",
      participant.id,
      "ask",
      { message: "PING" },
      "identity:resident",
      { timeoutMs: DEFAULT_FABRIC_CONFIG.agents.timeoutMs + 30_000, routedRemoteHost: null, detachOnMainCeiling: false },
    );

    await provider.invoke("tell", {
      id: participant.id,
      message: "queue",
    }, context);
    expect(request).toHaveBeenCalledWith(
      "host:resident",
      participant.id,
      "followUp",
      expect.objectContaining({ message: "queue" }),
      "identity:resident",
      { routedRemoteHost: null },
    );
  });

  it("executes one shared actor with each caller's session binding", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-two-sessions-"));
    roots.push(root);
    const meshRoot = path.join(root, "mesh");
    const actorRoot = path.join(root, "actors");
    const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const ownerIdentity: MeshIdentity = {
      id: "session:a",
      name: "Main A",
      kind: "main",
      sessionId: "a",
    };
    const peerIdentity: MeshIdentity = {
      id: "session:b",
      name: "Main B",
      kind: "main",
      sessionId: "b",
    };
    const ownerMesh = new MeshStore(meshRoot, 64 * 1_024, 1_000);
    const peerMesh = new MeshStore(meshRoot, 64 * 1_024, 1_000);
    const workerPath = path.resolve("tests/fixtures/fake-worker.mjs");
    const ownerAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath,
      runRoot: path.join(root, "owner-runs"),
    });
    const peerAgents = new AgentManager(process.cwd(), DEFAULT_FABRIC_CONFIG.agents, {
      workerPath,
      runRoot: path.join(root, "peer-runs"),
    });
    agentManagers.push(ownerAgents, peerAgents);
    const makeMain = (identity: MeshIdentity): FabricMainAgentTarget => ({
      id: identity.id,
      local: true,
      matches: (id) => id === "main" || id === identity.id,
      info: () => ({
        id: identity.id,
        name: "Main",
        kind: "main",
        status: "idle",
        runner: "pi",
        transport: "host",
        cwd: process.cwd(),
        sessionId: identity.sessionId ?? identity.id,
        startedAt: 1,
        updatedAt: 1,
        pendingMessages: false,
        local: true,
      }),
      deliverAgent: () => ({ queued: true, messageId: "main-message", routed: "main" }),
    });
    const ownerActors = new ActorManager(
      "a",
      ownerIdentity,
      ownerMesh,
      meshConfig,
      ownerAgents,
      () => {},
      {
        actorRoot,
        persistent: true,
        canManageActor: () => true,
        mainAgent: makeMain(ownerIdentity),
      },
    );
    actorManagers.push(ownerActors);
    const actor = await ownerActors.create({
      name: "shared reviewer",
      instructions: "Review each direct request.",
      model: "provider/project-default",
      thinking: "medium",
    });
    const peerActors = new ActorManager(
      "b",
      peerIdentity,
      peerMesh,
      meshConfig,
      peerAgents,
      () => {},
      {
        actorRoot,
        persistent: true,
        canManageActor: () => false,
        mainAgent: makeMain(peerIdentity),
      },
    );
    actorManagers.push(peerActors);

    const participantFor = (local: boolean): FabricParticipantInfo => ({
      format: 1,
      id: actor.id,
      kind: "actor",
      rootId: ownerIdentity.id,
      ownerHostId: "host:a",
      ownerIdentityId: ownerIdentity.id,
      parentId: ownerIdentity.id,
      name: actor.name,
      status: "idle",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "stop", "ask", "actor-bindings", "fabric"],
      startedAt: actor.createdAt,
      updatedAt: actor.updatedAt,
      controlProtocol: "v1",
      local,
      stale: false,
    });
    const sourceFor = (
      identity: MeshIdentity,
      hostId: string,
      local: boolean,
    ): FabricParticipantSource => ({
      list: () => [participantFor(local)],
      get: (id) => id === actor.id ? participantFor(local) : undefined,
      self: () => ({
        format: 1,
        id: identity.id,
        kind: "root",
        rootId: identity.id,
        ownerHostId: hostId,
        ownerIdentityId: identity.id,
        name: identity.name,
        status: "idle",
        runner: "pi",
        transport: "host",
        capabilities: ["steer", "followUp", "fabric"],
        sessionId: identity.sessionId ?? identity.id,
        startedAt: 1,
        updatedAt: 1,
        pendingMessages: false,
        controlProtocol: "v1",
        local: true,
        stale: false,
      }),
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    });
    const ownerSource = sourceFor(ownerIdentity, "host:a", true);
    const peerSource = sourceFor(peerIdentity, "host:b", false);
    const ownerControl = new FabricControlPlane(ownerMesh, ownerIdentity, {
      enabled: true,
      hostId: "host:a",
      pollMs: 20,
      acknowledgementTimeoutMs: 1_000,
    });
    const peerControl = new FabricControlPlane(peerMesh, peerIdentity, {
      enabled: true,
      hostId: "host:b",
      pollMs: 20,
      acknowledgementTimeoutMs: 1_000,
    });
    controlPlanes.push(ownerControl, peerControl);
    const ownerLifecycle = new LifecycleBroker(
      ownerMesh,
      ownerIdentity,
      ownerSource,
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      async () => {},
    );
    const peerLifecycle = new LifecycleBroker(
      peerMesh,
      peerIdentity,
      peerSource,
      { enabled: true, pollMs: 20, maxReadEvents: 100 },
      async () => {},
    );
    const globals = new GlobalActorRegistry(root, 64 * 1_024);
    const ownerProvider = new AgentsProvider(
      ownerAgents,
      ownerActors,
      globals,
      makeMain(ownerIdentity),
      ownerSource,
      ownerControl,
      ownerLifecycle,
      () => false,
      undefined,
      false,
    );
    const peerProvider = new AgentsProvider(
      peerAgents,
      peerActors,
      globals,
      makeMain(peerIdentity),
      peerSource,
      peerControl,
      peerLifecycle,
      () => false,
      undefined,
      false,
    );
    ownerControl.start((command, from, signal) =>
      ownerProvider.acceptControl(command, from, signal));
    peerControl.start((command, from, signal) =>
      peerProvider.acceptControl(command, from, signal));
    const run = vi.spyOn(ownerAgents, "run");

    await ownerProvider.invoke("setModel", { id: actor.id, model: "provider/model-a" }, context);
    await peerProvider.invoke("setModel", { id: actor.id, model: "provider/model-b" }, context);
    await ownerProvider.invoke("ask", { id: actor.id, message: "Review from A" }, context);
    await peerProvider.invoke("ask", { id: actor.id, message: "Review from B" }, context);

    expect(run.mock.calls.map(([request]) => request.model)).toEqual([
      "provider/model-a",
      "provider/model-b",
    ]);
    expect(ownerActors.status(actor.id)).toMatchObject({
      id: actor.id,
      model: "provider/model-a",
      binding: { model: "provider/model-a", sessionId: "a" },
      projectDefaults: { model: "provider/project-default" },
    });
    expect(peerActors.status(actor.id)).toMatchObject({
      id: actor.id,
      model: "provider/model-b",
      binding: { model: "provider/model-b", sessionId: "b" },
      projectDefaults: { model: "provider/project-default" },
    });
    const registry = JSON.parse(
      fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string; model?: string }> };
    expect(registry.actors).toContainEqual(
      expect.objectContaining({ id: actor.id, model: "provider/project-default" }),
    );
  });

});

describe("AgentsProvider global actors", () => {
  it("creates a global template and lists it separately from project actors", async () => {
    const { provider, actors, globalActors } = setup();
    const template = await provider.invoke("create", { ...createRequest, scope: "global" }, context);
    expect((template as { name: string }).name).toBe("reviewer");
    expect(globalActors.list()).toHaveLength(1);
    // project scope (default) lists live actors, not templates
    expect(await provider.invoke("actors", {}, context)).toEqual([]);
    expect(await provider.invoke("actors", { scope: "global" }, context)).toHaveLength(1);
    expect(actors.list()).toEqual([]);
  });

  it("imports a global template as a fresh live actor without history", async () => {
    const { provider, actors } = setup();
    await provider.invoke("create", { ...createRequest, scope: "global" }, context);
    const actor = (await provider.invoke("import", { name: "reviewer" }, context)) as {
      id: string;
      name: string;
      messages: number;
    };
    expect(actor.name).toBe("reviewer");
    expect(actors.list()).toHaveLength(1);
    // fresh actor starts with no mailbox history
    expect(actor.messages).toBe(0);
    expect(actors.instructions(actor.id)).toBe(createRequest.instructions);
  });

  it("exports a project actor to a global template without its history", async () => {
    const { provider, actors, globalActors } = setup();
    const actor = (await provider.invoke(
      "create",
      { ...createRequest, extensions: false, tools: ["read"] },
      context,
    )) as { id: string };
    // build some mailbox history so we can prove it is not exported
    await provider.invoke("ask", { id: actor.id, message: "inspect auth" }, context);
    await waitFor(() => actors.status(actor.id).status === "idle");
    expect(actors.status(actor.id).messages).toBeGreaterThan(0);

    const template = (await provider.invoke("export", { id: actor.id, write: true }, context)) as {
      name: string;
      instructions: string;
    };
    expect(template.name).toBe("reviewer");
    expect(template.instructions).toBe(createRequest.instructions);
    expect(globalActors.list()).toHaveLength(1);
    // a template carries no history at all
    const stored = globalActors.resolve("reviewer")!;
    expect(stored).not.toHaveProperty("messages");
    expect(stored).not.toHaveProperty("sessionFile");
    expect(stored.extensions).toBe(false);

    // re-importing yields a fresh actor with no inherited history
    const fresh = (await provider.invoke("import", { name: "reviewer", as: "reviewer-2" }, context)) as {
      messages: number;
      extensions?: boolean;
    };
    expect(fresh.messages).toBe(0);
    expect(fresh.extensions).toBe(false);
  });

  it("export collides without overwrite and replaces with it", async () => {
    const { provider } = setup();
    await provider.invoke("create", { ...createRequest, scope: "global" }, context);
    const actor = (await provider.invoke("create", createRequest, context)) as { id: string };
    await expect(provider.invoke("export", { id: actor.id, write: true }, context)).rejects.toThrow(/already exists/);
    const replaced = (await provider.invoke("export", { id: actor.id, write: true, overwrite: true }, context)) as {
      name: string;
    };
    expect(replaced.name).toBe("reviewer");
  });

  it("migrates a persistent actor model and thinking without replacing its session", async () => {
    const { provider, actors } = setup();
    const actor = (await provider.invoke("create", createRequest, context)) as {
      id: string;
      sessionFile?: string;
    };
    const sessionFile = actors.status(actor.id).sessionFile;

    await provider.invoke(
      "setModel",
      { id: actor.id, model: "anthropic/executor" },
      context,
    );
    await provider.invoke("setThinking", { id: actor.id, thinking: "low" }, context);
    expect(actors.status(actor.id)).toMatchObject({
      model: "anthropic/executor",
      thinking: "low",
      sessionFile,
    });

    await provider.invoke("setModel", { id: actor.id }, context);
    await provider.invoke("setThinking", { id: actor.id }, context);
    expect(actors.status(actor.id)).not.toHaveProperty("model");
    expect(actors.status(actor.id)).not.toHaveProperty("thinking");
    expect(actors.status(actor.id).sessionFile).toBe(sessionFile);
  });

  it("exposes inference context create/update/readback and history-free export/import", async () => {
    const { provider, actors } = setup();
    const descriptor = await provider.describe("setInferenceContext", context);
    expect(descriptor?.inputSchema).toMatchObject({ required: ["id", "inferenceContext"], properties: { inferenceContext: { enum: ["full-history", "activation"] } } });
    const actor = await provider.invoke("create", { ...createRequest, inferenceContext: "activation", extensions: false, tools: [] }, context) as { id: string; sessionFile: string };
    expect(actors.status(actor.id)).toMatchObject({ inferenceContext: "activation", extensions: false, tools: [] });
    await provider.invoke("setInferenceContext", { id: actor.id, inferenceContext: "full-history" }, context);
    expect(await provider.invoke("actorStatus", { id: actor.id }, context)).toMatchObject({ id: actor.id, sessionFile: actor.sessionFile, inferenceContext: "full-history" });
    const template = await provider.invoke("export", { id: actor.id, write: true }, context) as { id: string };
    await provider.invoke("setInferenceContext", { id: template.id, inferenceContext: "activation", scope: "global" }, context);
    const imported = await provider.invoke("import", { id: template.id, as: "window-copy" }, context) as { id: string; sessionFile: string; messages: number; inferenceContext: string };
    expect(imported).toMatchObject({ inferenceContext: "activation", messages: 0 });
    expect(imported.id).not.toBe(actor.id);
    expect(imported.sessionFile).not.toBe(actor.sessionFile);
    await expect(provider.invoke("setInferenceContext", { id: actor.id, inferenceContext: "invalid" }, context)).rejects.toThrow();
    await expect(provider.invoke("create", { ...createRequest, name: "bad-window", inferenceContext: "invalid" }, context)).rejects.toThrow();
  });

  it("updates tool allowlists for project actors and global templates", async () => {
    const { provider, actors, globalActors } = setup();
    const actor = (await provider.invoke("create", createRequest, context)) as { id: string };
    await provider.invoke("setTools", { id: actor.id, tools: ["read", "grep"] }, context);
    expect(actors.status(actor.id).tools).toEqual(["read", "grep"]);

    await provider.invoke("create", { ...createRequest, name: "templar", scope: "global" }, context);
    const templateId = globalActors.resolve("templar")!.id;
    await provider.invoke(
      "setTools",
      { id: templateId, tools: [], scope: "global" },
      context,
    );
    expect(globalActors.resolve("templar")!.tools).toEqual([]);
  });


  it("accepts the complete host-event catalog through create and setEvents", async () => {
    const { provider, actors } = setup();
    const actor = (await provider.invoke(
      "create",
      {
        ...createRequest,
        events: ["before_agent_start", "tool_call", "tool_result", "message_update"],
      },
      context,
    )) as { id: string };
    expect(actors.status(actor.id).events).toEqual([
      "before_agent_start",
      "tool_call",
      "tool_result",
      "message_update",
    ]);

    await provider.invoke(
      "setEvents",
      { id: actor.id, events: ["context", "before_provider_request", "session_tree"] },
      context,
    );
    expect(actors.status(actor.id).events).toEqual([
      "context",
      "before_provider_request",
      "session_tree",
    ]);
  });

  it("validates and updates delivery policies for project actors and global templates", async () => {
    const { provider, actors, globalActors } = setup();
    const { triggerTurn: _triggerTurn, ...ambiguous } = createRequest;
    await expect(provider.invoke("create", ambiguous, context)).rejects.toThrow(
      /requires explicit triggerTurn/,
    );

    const actor = (await provider.invoke("create", createRequest, context)) as { id: string };
    await provider.invoke(
      "setDeliveryPolicy",
      { id: actor.id, delivery: "steer", triggerTurn: true },
      context,
    );
    expect(actors.status(actor.id)).toMatchObject({ delivery: "steer", triggerTurn: true });

    await provider.invoke(
      "create",
      { ...createRequest, name: "templar", scope: "global" },
      context,
    );
    const templateId = globalActors.resolve("templar")!.id;
    await provider.invoke(
      "setDeliveryPolicy",
      { id: templateId, delivery: "followUp", triggerTurn: true, scope: "global" },
      context,
    );
    expect(globalActors.resolve(templateId)).toMatchObject({
      delivery: "followUp",
      triggerTurn: true,
    });
  });

  it("edits instructions for project and global scopes", async () => {
    const { provider, actors, globalActors } = setup();
    const actor = (await provider.invoke("create", createRequest, context)) as { id: string };
    await provider.invoke("setInstructions", { id: actor.id, instructions: "Be brief.", replace: true }, context);
    expect(actors.instructions(actor.id)).toBe("Be brief.");

    await provider.invoke("create", { ...createRequest, name: "templar", scope: "global" }, context);
    const globalId = globalActors.resolve("templar")!.id;
    await provider.invoke("setInstructions", { id: globalId, instructions: "Template brief.", scope: "global", replace: true }, context);
    expect(globalActors.resolve("templar")!.instructions).toBe("Template brief.");
  });

  // smarty-dev#2340: a >80% shrink is refused unless replace: true.
  it("guards setInstructions against a >80% shrink in project and global scopes", async () => {
    const { provider, actors, globalActors } = setup();
    const long = "x".repeat(100);
    const actor = (await provider.invoke("create", createRequest, context)) as { id: string };
    await provider.invoke("create", { ...createRequest, name: "templar", scope: "global" }, context);
    const globalId = globalActors.resolve("templar")!.id;
    const read = {
      project: () => actors.instructions(actor.id),
      global: () => globalActors.resolve("templar")!.instructions,
    };
    for (const scope of ["project", "global"] as const) {
      const id = scope === "global" ? globalId : actor.id;
      const set = (instructions: string, extra: Record<string, unknown> = {}) =>
        provider.invoke("setInstructions", { id, instructions, scope, ...extra }, context);
      await set(long, { replace: true });
      expect(read[scope]()).toBe(long);
      // Refused: 19 chars is more than 80% shorter than 100.
      const error = await set("y".repeat(19)).then(() => undefined, (e: Error) => e);
      expect(error?.message).toMatch(/19/);
      expect(error?.message).toMatch(/100/);
      expect(error?.message).toMatch(/replace: true/);
      expect(read[scope]()).toBe(long);
      // Exact 80% boundary (20 of 100) is allowed.
      await set("z".repeat(20));
      expect(read[scope]()).toBe("z".repeat(20));
      // Normal edit allowed.
      await set("z".repeat(18) + "ab");
      expect(read[scope]()).toBe("z".repeat(18) + "ab");
      // Explicit replace allows a large shrink.
      await set(long, { replace: true });
      await set("tiny", { replace: true });
      expect(read[scope]()).toBe("tiny");
    }
  });

  it("removes a global template via scoped remove", async () => {
    const { provider, globalActors } = setup();
    const template = (await provider.invoke(
      "create",
      { ...createRequest, scope: "global" },
      context,
    )) as { id: string };
    await provider.invoke("remove", { id: template.id, scope: "global" }, context);
    expect(globalActors.list()).toEqual([]);
  });

  // smarty-dev#918: remove({ id }) on a template answered only "Unknown Fabric actor".
  it("points an unscoped remove of a global template to the global scope", async () => {
    const { provider, globalActors } = setup();
    const template = (await provider.invoke("create", { ...createRequest, scope: "global" }, context)) as { id: string };
    await expect(provider.invoke("remove", { id: template.id }, context))
      .rejects.toThrow(/is a global template: remove it with agents\.remove\(\{ id, scope: "global" \}\)/);
    expect(globalActors.list()).toHaveLength(1);                        // nothing removed
  });
});

describe("AgentsProvider steering", () => {
  const readSteerFile = (root: string, id: string): Array<Record<string, unknown>> => {
    const file = path.join(root, "runs", id, "steer.jsonl");
    if (!fs.existsSync(file)) return [];
    return fs
      .readFileSync(file, "utf8")
      .split("\n")
      .filter((line) => line.trim())
      .map((line) => JSON.parse(line) as Record<string, unknown>);
  };

  it("discovers and addresses the root Main agent through its stable alias", async () => {
    const { provider, mainDeliveries } = setup();

    await expect(provider.invoke("main", {}, context)).resolves.toMatchObject({
      id: "session:test",
      name: "Main",
      kind: "main",
      local: true,
    });
    await expect(
      provider.invoke("status", { id: "main" }, context),
    ).resolves.toMatchObject({ id: "session:test", name: "Main" });

    const steer = await provider.invoke(
      "steer",
      { id: "main", message: "prioritize the failing test", data: { source: "supervisor" } },
      context,
    );
    const followUp = await provider.invoke(
      "followUp",
      { id: "session:test", message: "then summarize the fix" },
      context,
    );

    expect(steer).toEqual({
      queued: true,
      messageId: "main-message-1",
      routed: "main",
    });
    expect(followUp).toEqual({
      queued: true,
      messageId: "main-message-2",
      routed: "main",
    });
    expect(mainDeliveries).toMatchObject([
      {
        from: { id: "session:test", kind: "main" },
        message: "prioritize the failing test",
        delivery: "steer",
        data: { source: "supervisor" },
      },
      {
        message: "then summarize the fix",
        delivery: "followUp",
      },
    ]);
  });

  it("steer routes to a local running agent and queues a steer command", async () => {
    const { provider, root } = setup();
    const handle = (await provider.invoke(
      "spawn",
      { task: "HANG", transport: "process" },
      context,
    )) as { id: string };
    const result = (await provider.invoke(
      "steer",
      { id: handle.id, message: "focus on refresh tokens" },
      context,
    )) as { queued: boolean; messageId: string; routed: string };
    expect(result).toEqual({ queued: true, messageId: expect.any(String), routed: "local" });
    const entries = readSteerFile(root, handle.id);
    expect(entries[0]).toMatchObject({ type: "steer", message: "focus on refresh tokens" });
    await provider.invoke("stop", { id: handle.id }, context);
  });

  it("accepts an owner-addressed control command for a local agent", async () => {
    const { provider, root } = setup();
    const handle = (await provider.invoke(
      "spawn",
      { task: "HANG", transport: "process" },
      context,
    )) as { id: string };
    const acceptance = await provider.acceptControl(
      {
        version: 1,
        commandId: "command-1",
        targetId: handle.id,
        operation: "followUp",
        replyTo: "session:peer",
        message: "summarize after the current turn",
        requestedAt: Date.now(),
      },
      { id: "session:peer", name: "peer", kind: "main", sessionId: "peer" },
    );

    expect(acceptance).toMatchObject({ accepted: true, messageId: expect.any(String) });
    expect(readSteerFile(root, handle.id)[0]).toMatchObject({
      type: "follow_up",
      message: "summarize after the current turn",
    });
    await provider.invoke("stop", { id: handle.id }, context);
  });

  it("steer routes to a local actor as a mailbox message", async () => {
    const { provider } = setup();
    const actor = (await provider.invoke(
      "create",
      { name: "steered", instructions: "reply", responseMode: "text" },
      context,
    )) as { id: string };
    const result = (await provider.invoke(
      "steer",
      { id: actor.id, message: "check session expiry" },
      context,
    )) as { routed: string };
    expect(result.routed).toBe("local");
    const messages = (await provider.invoke("messages", { id: actor.id }, context)) as Array<{
      direction: string;
      data?: { message?: string };
    }>;
    expect(
      messages.some(
        (message) => message.direction === "in" && message.data?.message === "check session expiry",
      ),
    ).toBe(true);
  });

  // smarty-dev#447: status and stop said only "Unknown Fabric participant", without the reason.
  it("says why status and stop cannot resolve an id", async () => {
    const { provider } = setup();
    for (const action of ["status", "stop"] as const) {
      await expect(provider.invoke(action, { id: "session:never" }, context))
        .rejects.toThrow("Unknown Fabric participant: session:never (no record on this mesh root");
    }
  });

  // smarty-dev#1882: after its host exits, a finished durable agent has no participant; stop
  // returns its terminal record. A durable agent that still runs keeps the participant route.
  it("stops a finished durable agent with its terminal record, not Unknown participant", async () => {
    const { agents, actors, globalActors, mainAgent, participants, control, lifecycle } = setup();
    const id = "b".repeat(32);
    let settled: unknown = { id, name: "durable", status: "completed", text: "done", residency: "durable" };
    const acknowledged: string[] = [];
    const residency = {
      hasAgent: (candidate: string) => candidate === id,
      settledAgent: (candidate: string) => candidate === id ? settled : undefined,
      statusAgent: () => ({ id, name: "durable", status: "stopped", residency: "durable" }),
      acknowledgeCompletion: (candidate: string) => acknowledged.push(candidate),
    } as unknown as ResidencyClient;
    const provider = new AgentsProvider(
      agents, actors, globalActors, mainAgent, participants, control, lifecycle,
      undefined, residency, false,
    );
    await expect(provider.invoke("stop", { id }, context)).resolves.toMatchObject({ status: "completed", text: "done" });
    expect(acknowledged).toEqual([id]);
    // review/astra on #136: a terminal-looking attempt that a live host may resume is not settled;
    // stop takes the participant route and acknowledges nothing.
    settled = undefined;
    await expect(provider.invoke("stop", { id }, context)).rejects.toThrow("Unknown Fabric participant");
    expect(acknowledged).toEqual([id]);
  });

  // review/astra on #57: the remote-Main branch of status said only "Unknown Fabric Main participant".
  it("says why status cannot resolve a remote Main, by alias and by id, after a write stall check", async () => {
    let stalled: Error | undefined;
    const { provider } = setup([], [], undefined, { writeStalled: () => stalled });
    (provider.mainAgent as { local: boolean }).local = false;
    for (const id of ["main", provider.mainAgent.id]) {
      await expect(provider.invoke("status", { id }, context))
        .rejects.toThrow(`Unknown Fabric Main participant: ${provider.mainAgent.id} (no record on this mesh root`);
    }
    stalled = new Error("Fabric mesh is write-stalled: Timed out waiting for the Fabric mesh lock");
    await expect(provider.invoke("status", { id: "main" }, context)).rejects.toThrow(stalled.message);
  });

  it("rejects an unknown remote id instead of broadcasting an unverified steer", async () => {
    const { provider } = setup();
    await expect(
      provider.invoke(
        "steer",
        { id: "not-a-local-id", message: "from elsewhere" },
        context,
      ),
    ).rejects.toThrow("Unknown Fabric participant");
  });

  // smarty-dev#705: the queue key for mesh events, set at creation or later.
  it("creates an actor with a coalesceKey and sets, clears and validates it", async () => {
    const { provider } = setup();
    const actor = (await provider.invoke("create", {
      name: "reviewer", instructions: "Review.", topics: ["github.demo.pulls"], coalesce: false, coalesceKey: "payload.number",
    }, context)) as { id: string; coalesceKey?: string };
    expect(actor.coalesceKey).toBe("payload.number");
    await expect(provider.invoke("setCoalesceKey", { id: actor.id, coalesceKey: null }, context)).resolves.not.toHaveProperty("coalesceKey");
    await expect(provider.invoke("setCoalesceKey", { id: actor.id, coalesceKey: "payload.pull_request.number" }, context))
      .resolves.toMatchObject({ coalesceKey: "payload.pull_request.number" });
    await expect(provider.invoke("setCoalesceKey", { id: actor.id, coalesceKey: "not a path" }, context)).rejects.toThrow("Invalid actor coalesceKey");
    await expect(provider.invoke("setCoalesceKey", { id: actor.id }, context)).rejects.toThrow("coalesceKey is required");
    await expect(provider.invoke("create", { name: "bad", instructions: "x", coalesceKey: "" }, context)).rejects.toThrow("Invalid actor coalesceKey");
  });

  // smarty-dev#1579: the skip-only activation filter, set at creation or later, project or global.
  it("creates an actor with an activationFilter and sets, clears and validates it", async () => {
    const { provider } = setup();
    const actor = (await provider.invoke("create", {
      name: "supervisor", instructions: "Supervise.", topics: ["github.demo"], activationFilter: ["hold", "never-message-events"],
    }, context)) as { id: string; activationFilter?: unknown };
    expect(actor.activationFilter).toEqual(["hold", "never-message-events"]);
    await expect(provider.invoke("setActivationFilter", { id: actor.id, activationFilter: null }, context)).resolves.not.toHaveProperty("activationFilter");
    await expect(provider.invoke("setActivationFilter", { id: actor.id, activationFilter: ["hold"] }, context))
      .resolves.toMatchObject({ activationFilter: ["hold"] });
    await expect(provider.invoke("setActivationFilter", { id: actor.id, activationFilter: ["holds"] }, context)).rejects.toThrow("Unknown activationFilter preset");
    await expect(provider.invoke("setActivationFilter", { id: actor.id }, context)).rejects.toThrow("activationFilter is required");
    await expect(provider.invoke("create", { name: "bad", instructions: "x", activationFilter: [{ id: "all" }] }, context)).rejects.toThrow("name a source, topic, kind or where");
    const template = (await provider.invoke("create", {
      name: "template", instructions: "Supervise.", scope: "global", activationFilter: ["never-message-events"],
    }, context)) as { id: string; activationFilter?: unknown };
    expect(template.activationFilter).toEqual(["never-message-events"]);
    await expect(provider.invoke("setActivationFilter", { id: template.id, activationFilter: ["hold"], scope: "global" }, context))
      .resolves.toMatchObject({ activationFilter: ["hold"] });
    await expect(provider.invoke("setActivationFilter", { id: template.id, activationFilter: null, scope: "global" }, context))
      .resolves.not.toHaveProperty("activationFilter");
    await expect(provider.invoke("setActivationFilter", { id: template.id, activationFilter: ["x"], scope: "global" }, context)).rejects.toThrow("Unknown activationFilter preset");
  });

  it("setSteeringMode routes to a local agent", async () => {
    const { provider, root } = setup();
    const handle = (await provider.invoke(
      "spawn",
      { task: "HANG", transport: "process" },
      context,
    )) as { id: string };
    await provider.invoke("setSteeringMode", { id: handle.id, mode: "all" }, context);
    const entries = readSteerFile(root, handle.id);
    expect(entries[0]).toMatchObject({ type: "set_steering_mode", mode: "all" });
    await provider.invoke("stop", { id: handle.id }, context);
  });

  it("setSteeringMode throws for a non-local id (no mesh fallback)", async () => {
    const { provider } = setup();
    await expect(
      provider.invoke("setSteeringMode", { id: "unknown-id", mode: "all" }, context),
    ).rejects.toThrow(/Unknown Fabric agent/);
  });

  it("setSteeringMode rejects an invalid mode", async () => {
    const { provider } = setup();
    const handle = (await provider.invoke(
      "spawn",
      { task: "HANG", transport: "process" },
      context,
    )) as { id: string };
    await expect(
      provider.invoke("setSteeringMode", { id: handle.id, mode: "always" }, context),
    ).rejects.toThrow(/Invalid steering mode/);
    await provider.invoke("stop", { id: handle.id }, context);
  });

  it("compact enqueues a compact entry for a running pi child", async () => {
    const { provider, root } = setup();
    const handle = (await provider.invoke(
      "spawn",
      { task: "HANG", transport: "process" },
      context,
    )) as { id: string };
    const result = (await provider.invoke(
      "compact",
      { id: handle.id, instructions: "Keep the test plan" },
      context,
    )) as { queued: true; messageId: string };
    expect(result.queued).toBe(true);
    expect(typeof result.messageId).toBe("string");
    const entries = readSteerFile(root, handle.id);
    expect(entries[0]).toMatchObject({ type: "compact", instructions: "Keep the test plan" });
    await provider.invoke("stop", { id: handle.id }, context);
  });

  it("compact descriptor is agent-risk with required id", async () => {
    const { provider } = setup();
    const descriptor = await provider.describe("compact", context);
    expect(descriptor?.risk).toBe("agent");
    const schema = descriptor?.inputSchema as {
      properties: Record<string, unknown>;
      required: string[];
      additionalProperties: boolean;
    };
    expect(schema.required).toEqual(["id"]);
    expect(schema.properties).toHaveProperty("instructions");
    expect(schema.additionalProperties).toBe(false);
  });

  it("compact rejects an unknown id", async () => {
    const { provider } = setup();
    await expect(
      provider.invoke("compact", { id: "not-a-real-id" }, context),
    ).rejects.toThrow(/Unknown Fabric agent/);
  });
});

describe("collectAgentToolPreviewNodes", () => {
  const previewRecord = (overrides: Record<string, unknown>): AgentRunRecord =>
    ({
      id: "id",
      name: "agent",
      task: "task",
      status: "running",
      runner: "pi",
      transport: "process",
      cwd: "/tmp",
      startedAt: 0,
      updatedAt: 0,
      turns: 0,
      toolCalls: 0,
      text: "",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
      ...overrides,
    }) as AgentRunRecord;

  const toolEntry = (record: AgentRunRecord) => ({
    id: `tool-${record.id}`,
    kind: "tool" as const,
    label: "read",
  });

  it("maps a nested run tree onto recursive preview nodes", () => {
    const nodes = collectAgentToolPreviewNodes(
      [
        previewRecord({
          id: "parent",
          name: "parent",
          nestedAgents: [
            previewRecord({
              id: "child",
              name: "child",
              currentTool: "grep",
              nestedAgents: [previewRecord({ id: "grand", name: "grand" })],
            }),
          ],
        }),
      ],
      { tools: (record) => [toolEntry(record)] },
    );

    expect(nodes).toHaveLength(1);
    expect(nodes[0]?.tools[0]?.id).toBe("tool-parent");
    const child = nodes[0]?.agents?.[0];
    expect(child).toMatchObject({ id: "child", name: "child", currentTool: "grep", owner: "agent" });
    expect(child?.tools[0]?.id).toBe("tool-child");
    expect(child?.agents?.[0]).toMatchObject({ id: "grand" });
    expect(child?.agentsTruncated).toBeUndefined();
  });

  it("marks nodes whose descendants exceed the depth budget", () => {
    const nodes = collectAgentToolPreviewNodes(
      [
        previewRecord({
          id: "parent",
          nestedAgents: [
            previewRecord({ id: "child", nestedAgents: [previewRecord({ id: "grand" })] }),
          ],
        }),
      ],
      { tools: () => [], maxDepth: 2 },
    );

    const child = nodes[0]?.agents?.[0];
    expect(child?.agents).toBeUndefined();
    expect(child?.agentsTruncated).toBe(true);
  });

  it("caps the total node count across the breadth of the tree", () => {
    const nodes = collectAgentToolPreviewNodes(
      [
        previewRecord({ id: "first" }),
        previewRecord({ id: "second", nestedAgents: [previewRecord({ id: "third" })] }),
        previewRecord({ id: "fourth" }),
      ],
      { tools: () => [], maxNodes: 2 },
    );

    expect(nodes.map((node) => node.id)).toEqual(["first", "second"]);
    expect(nodes[1]?.agents).toBeUndefined();
    expect(nodes[1]?.agentsTruncated).toBe(true);
  });

  it("labels actor-runs with the actor owner kind", () => {
    const nodes = collectAgentToolPreviewNodes(
      [previewRecord({ id: "run", actorId: "actor-1", actorName: "mailbox-bot" })],
      { tools: () => [] },
    );

    expect(nodes[0]).toMatchObject({ owner: "actor", name: "mailbox-bot" });
  });
});

describe("AgentsProvider switchModel", () => {
  const registryModels = [
    { provider: "anthropic", id: "claude-opus-4-5", name: "Claude Opus 4.5" },
    { provider: "google", id: "gemini-2.5-flash", name: "Gemini 2.5 Flash" },
    { provider: "google", id: "gemini-2.5-pro", name: "Gemini 2.5 Pro" },
  ];

  const modelContext = (current?: { provider: string; id: string }): FabricInvocationContext => ({
    ...context,
    extensionContext: {
      modelRegistry: { getAvailable: () => registryModels },
      ...(current ? { model: current } : {}),
    } as unknown as ExtensionContext,
  });

  it("describes the action with a required model selector", async () => {
    const { provider } = setup();
    const descriptor = await provider.describe("switchModel", context);
    expect(descriptor?.inputSchema).toMatchObject({ required: ["model"] });
    expect(descriptor?.risk).toBe("agent");
  });

  it("switches an exact provider/id and reports the previous model", async () => {
    const switchModel = vi.fn(async () => ({ ok: true }));
    const { provider } = setup([], [], undefined, {
      switchModel: switchModel as FabricMainAgentTarget["switchModel"],
    });
    const invocation = modelContext({ provider: "anthropic", id: "claude-opus-4-5" });
    const result = await provider.invoke(
      "switchModel",
      { model: "google/gemini-2.5-flash" },
      invocation,
    );
    expect(result).toEqual({
      switched: true,
      model: "google/gemini-2.5-flash",
      name: "Gemini 2.5 Flash",
      previous: "anthropic/claude-opus-4-5",
    });
    expect(switchModel).toHaveBeenCalledWith(
      { provider: "google", id: "gemini-2.5-flash" },
      invocation.extensionContext,
    );
  });

  it("resolves aliases configured in models.aliases with fallback chains", async () => {
    const switchModel = vi.fn(async () => ({ ok: true }));
    const { provider } = setup([], [], undefined, {
      switchModel: switchModel as FabricMainAgentTarget["switchModel"],
      modelsConfig: { aliases: { budget: { targets: ["cohere/command-r", "google/gemini-2.5-pro"] } } },
    });
    const result = await provider.invoke(
      "switchModel",
      { model: "Budget" },
      modelContext(),
    );
    expect(result).toMatchObject({
      switched: true,
      model: "google/gemini-2.5-pro",
      alias: "budget",
    });
  });

  it("keeps the current model when the selector is already active", async () => {
    const switchModel = vi.fn(async () => ({ ok: true }));
    const { provider } = setup([], [], undefined, {
      switchModel: switchModel as FabricMainAgentTarget["switchModel"],
    });
    const result = await provider.invoke(
      "switchModel",
      { model: "claude-opus" },
      modelContext({ provider: "anthropic", id: "claude-opus-4-5" }),
    );
    expect(result).toEqual({
      switched: false,
      reason: "already-active",
      model: "anthropic/claude-opus-4-5",
      name: "Claude Opus 4.5",
    });
    expect(switchModel).not.toHaveBeenCalled();
  });

  it("resolves inexact selectors to the closest match and reports the pick", async () => {
    const switchModel = vi.fn(async () => ({ ok: true }));
    const { provider } = setup([], [], undefined, {
      switchModel: switchModel as FabricMainAgentTarget["switchModel"],
    });
    const result = await provider.invoke(
      "switchModel",
      { model: "gemini" },
      modelContext(),
    );
    expect(result).toMatchObject({
      switched: true,
      model: "google/gemini-2.5-pro",
      via: "closest",
    });
    expect(switchModel).toHaveBeenCalledWith(
      { provider: "google", id: "gemini-2.5-pro" },
      expect.anything(),
    );
  });

  it("resolves visible exact, fuzzy, and alias run models before spawning", async () => {
    const { provider, agents } = setup([], [], undefined, {
      modelsConfig: {
        aliases: { fast: { targets: ["opencode/hidden", "google/gemini-2.5-flash"] } },
      },
    });
    const spawn = vi.spyOn(agents, "spawn");
    const selectors = [
      ["google/gemini-2.5-flash", "google/gemini-2.5-flash"],
      ["gemini", "google/gemini-2.5-pro"],
      ["google/gemni-2.5-flash", "google/gemini-2.5-flash"],
      ["fast", "google/gemini-2.5-flash"],
    ] as const;

    for (const [model, expected] of selectors) {
      await provider.invoke(
        "run",
        { task: `run ${model}`, model },
        modelContext(),
      );
      expect(spawn).toHaveBeenLastCalledWith(
        expect.objectContaining({ model: expected }),
        undefined,
      );
    }
  });

  it.each(["run", "spawn"] as const)(
    "rejects unavailable exact models for agents.%s",
    async (action) => {
      const { provider, agents } = setup();
      const spawn = vi.spyOn(agents, "spawn");

      await expect(
        provider.invoke(
          action,
          { task: "do not launch", model: "opencode/ox-alpha" },
          modelContext(),
        ),
      ).rejects.toThrow(/not available to this Pi session/);
      expect(spawn).not.toHaveBeenCalled();
    },
  );

  it("launches near-miss models canonically while isolating unrelated batch failures", async () => {
    const { provider, agents } = setup();
    const spawn = vi.spyOn(agents, "spawn");
    const invocation: FabricInvocationContext = {
      ...context,
      extensionContext: {
        modelRegistry: { getAvailable: () => [
          { provider: "openai-codex", id: "gpt-6-astra" },
          { provider: "openai-codex", id: "gpt-5.6-sol" },
        ] },
      } as unknown as ExtensionContext,
    };
    const results = await Promise.allSettled([
      provider.invoke("spawn", { task: "Astra", model: "openai-codex/gpt-6-astra" }, invocation),
      provider.invoke("spawn", { task: "Sol", model: "openai-codex/gpt-6-sol" }, invocation),
      provider.invoke("spawn", { task: "Unrelated", model: "openai-codex/zzzz" }, invocation),
    ]);
    expect(results[0]).toMatchObject({ status: "fulfilled", value: { model: "openai-codex/gpt-6-astra" } });
    expect(results[1]).toMatchObject({ status: "fulfilled", value: { model: "openai-codex/gpt-5.6-sol" } });
    expect(results[2]).toMatchObject({ status: "rejected", reason: expect.any(Error) });
    expect(spawn).toHaveBeenCalledTimes(2);
    expect(spawn).toHaveBeenCalledWith(expect.objectContaining({ model: "openai-codex/gpt-5.6-sol" }), undefined);
    for (const result of results) {
      if (result.status !== "fulfilled") continue;
      const handle = result.value as { id: string; model: string };
      await expect(agents.wait(handle.id)).resolves.toMatchObject({ status: "completed", model: handle.model });
    }
  });

  it("rejects exhausted Pi model aliases instead of forwarding them", async () => {
    const { provider } = setup([], [], undefined, {
      modelsConfig: {
        aliases: { retired: { targets: ["opencode/old", "opencode/older"] } },
      },
    });

    await expect(
      provider.invoke(
        "run",
        { task: "do not launch", model: "retired" },
        modelContext(),
      ),
    ).rejects.toThrow(/not available to this Pi session.*opencode\/old, opencode\/older/);
  });

  it("rejects unavailable Pi models for handoff and actor creation", async () => {
    const { provider, actors } = setup();
    const deferHandoff = vi.fn(() => ({
      scheduled: true as const,
      status: "deferred" as const,
      boundary: "fabric_exec_end" as const,
    }));
    const invocation = { ...modelContext(), deferHandoff };

    await expect(
      provider.invoke("handoff", { model: "opencode/ox-alpha" }, invocation),
    ).rejects.toThrow(/not available to this Pi session/);
    expect(deferHandoff).not.toHaveBeenCalled();
    await expect(
      provider.invoke(
        "create",
        { name: "hidden actor", instructions: "Do not create.", model: "opencode/ox-alpha" },
        modelContext(),
      ),
    ).rejects.toThrow(/not available to this Pi session/);
    expect(actors.list()).toEqual([]);
  });

  it("rejects unavailable actor setModel and activation overrides", async () => {
    const { provider } = setup();
    const actor = await provider.invoke(
      "create",
      {
        name: "visible actor",
        instructions: "Use only visible models.",
        model: "google/gemini-2.5-flash",
      },
      modelContext(),
    ) as FabricActorInfo;

    await expect(
      provider.invoke("setModel", { id: actor.id, model: "opencode/ox-alpha" }, modelContext()),
    ).rejects.toThrow(/not available to this Pi session/);
    await expect(
      provider.invoke(
        "ask",
        { id: actor.id, message: "Do not run", model: "opencode/ox-alpha" },
        modelContext(),
      ),
    ).rejects.toThrow(/not available to this Pi session/);
    await expect(
      provider.invoke(
        "tell",
        { id: actor.id, message: "Do not queue", model: "opencode/ox-alpha" },
        modelContext(),
      ),
    ).rejects.toThrow(/not available to this Pi session/);
  });

  it("passes Claude and Veda model strings through unchanged", async () => {
    const { provider, agents } = setup();
    const spawn = vi.spyOn(agents, "spawn");

    await provider.invoke(
      "spawn",
      { task: "Claude pass-through", runner: "claude", model: "private/claude-model" },
      modelContext(),
    );
    await provider.invoke(
      "spawn",
      { task: "Veda pass-through", runner: "veda", model: "private/veda-model" },
      modelContext(),
    );

    expect(spawn.mock.calls[0]?.[0]).toMatchObject({
      runner: "claude",
      model: "private/claude-model",
    });
    expect(spawn.mock.calls[1]?.[0]).toMatchObject({
      runner: "veda",
      model: "private/veda-model",
    });
  });

  it("rejects unknown selectors and exhausted alias chains", async () => {
    const { provider } = setup([], [], undefined, {
      switchModel: vi.fn(async () => ({ ok: true })) as FabricMainAgentTarget["switchModel"],
      modelsConfig: { aliases: { budget: { targets: ["cohere/command-r", "mistral/mistral-large"] } } },
    });
    await expect(
      provider.invoke("switchModel", { model: "cohere/command-r" }, modelContext()),
    ).rejects.toThrow(/no available model matching "cohere\/command-r"/);
    await expect(
      provider.invoke("switchModel", { model: "budget" }, modelContext()),
    ).rejects.toThrow(/Tried: cohere\/command-r, mistral\/mistral-large/);
  });

  it("surfaces host switch failures as errors", async () => {
    const { provider } = setup([], [], undefined, {
      switchModel: vi.fn(async () => ({
        ok: false,
        error: "No authentication configured for model: google/gemini-2.5-flash",
      })) as FabricMainAgentTarget["switchModel"],
    });
    await expect(
      provider.invoke("switchModel", { model: "google/gemini-2.5-flash" }, modelContext()),
    ).rejects.toThrow(/No authentication configured/);
  });

  it("rejects when Main is not a local host with model control", async () => {
    const { provider } = setup();
    await expect(
      provider.invoke("switchModel", { model: "google/gemini-2.5-flash" }, modelContext()),
    ).rejects.toThrow(/requires a local Main session/);
  });

  it("requires a non-empty model selector", async () => {
    const { provider } = setup([], [], undefined, {
      switchModel: vi.fn(async () => ({ ok: true })) as FabricMainAgentTarget["switchModel"],
    });
    await expect(
      provider.invoke("switchModel", { model: "  " }, modelContext()),
    ).rejects.toThrow(/requires a model selector/);
  });
});

