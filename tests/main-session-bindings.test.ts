import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../src/main-agent.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";

const roots: string[] = [];
const fixtures: Array<{ control: FabricControlPlane; main: MainAgentController; provider: AgentsProvider }> = [];
const session = () => `session:${randomUUID()}`;
const members = new Map<string, FabricParticipantInfo>();
const models = ["a", "b", "denied"].map(id => ({ provider: "probe", id, name: id }));
const fixture = (meshRoot: string, id = session(), kind: MeshIdentity["kind"] = "main") => {
  const cwd = path.join(meshRoot, encodeURIComponent(id)); fs.mkdirSync(cwd, { recursive: true });
  const identity: MeshIdentity = { id, kind, name: "Main", sessionId: id.slice(8) };
  let model = models[0]!;
  let thinking = "low";
  const entries: unknown[] = [];
  const pi = {
    on: vi.fn(() => () => {}), sendMessage: vi.fn(), sendUserMessage: vi.fn(),
    getThinkingLevel: () => thinking,
    setThinkingLevel: vi.fn((level: string) => { thinking = level; }),
    setModel: vi.fn(async (next: typeof model) => { model = next; return true; }),
    appendEntry: vi.fn((customType: string, data: unknown) => entries.push({ customType, data })),
  } as unknown as ExtensionAPI;
  const context = {
    cwd, get model() { return model; }, isIdle: () => false, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => id.slice(8), getSessionFile: () => undefined, getEntries: () => [] },
    modelRegistry: { find: (provider: string, id: string) => models.find(m => m.provider === provider && m.id === id), getAvailable: () => models },
  } as unknown as ExtensionContext;
  const main = new MainAgentController(pi, id, kind === "main", cwd, id.slice(8));
  main.attachFollowUpDrain(context, 0);
  const mesh = new MeshStore(meshRoot, 64 * 1024, 1000);
  const manager = new AgentManager(cwd, { ...DEFAULT_FABRIC_CONFIG.agents, deniedModels: ["probe/denied"] }, { runRoot: path.join(cwd, "runs") });
  const actors = new ActorDirectory([id.slice(8), identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, () => {}, { rootId: id }],
    { project: path.join(cwd, "actors"), session: path.join(cwd, "session-actors") }, "project");
  const self = { format: 1, id, rootId: id, kind: kind === "main" ? "root" : "agent", ownerHostId: id, ownerIdentityId: id,
    name: "Main", status: "running", runner: "pi", transport: "host", capabilities: ["fabric"], mainBindings: true,
    controlProtocol: "v1", local: true, stale: false, startedAt: 1, updatedAt: 1 } as FabricParticipantInfo;
  members.set(id, self);
  const participants: FabricParticipantSource = {
    self: () => self, get: id => members.get(id), list: () => [...members.values()], peers: () => [],
    refresh: async () => {}, scheduleRefresh: vi.fn(),
  };
  const control = new FabricControlPlane(mesh, identity, { enabled: true, hostId: id, pollMs: 20, acknowledgementTimeoutMs: 1000 });
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
  const provider = new AgentsProvider(manager, actors, new GlobalActorRegistry(cwd, 64 * 1024), main, participants, control, lifecycle);
  control.start((command, from, signal, verification) => provider.acceptControl(command, from, signal, verification));
  const invocation = { extensionContext: context, signal: new AbortController().signal, callId: "binding-test", audits: [] } as unknown as FabricInvocationContext;
  const value = { provider, main, pi, context, invocation, control, manager, actors, self, entries };
  fixtures.push(value); return value;
};
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "main-bindings-")); roots.push(dir); return dir; };
afterEach(async () => {
  for (const f of fixtures.splice(0)) { await f.control.close(); f.main.closeFollowUpDrain(); await f.provider.close(); }
  members.clear(); vi.unstubAllEnvs();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe("live Main binding setters (smarty-dev#3626)", () => {
  it("sets its own Main thinking with native before/after readback and caller audit", async () => {
    const f = fixture(root());
    const effort = await f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, f.invocation);
    expect(effort).toMatchObject({ id: f.main.id, kind: "main", thinking: "high", caller: f.main.id, previous: { model: "probe/a", thinking: "low" } });
    expect(f.pi.setThinkingLevel).toHaveBeenCalledWith("high");
    expect(f.pi.setModel).not.toHaveBeenCalled();
    expect(f.entries).toEqual([
      { customType: "pi-fabric.main-binding-change", data: expect.objectContaining({ action: "agents.setThinking", caller: f.main.id, before: { model: "probe/a", thinking: "low" }, after: { model: "probe/a", thinking: "high" } }) },
    ]);
  });

  it.each([undefined, "probe/b", "probe/denied", "probe/missing"])("defers own Main setModel (%s) before registry/auth/native entry", async model => {
    const f = fixture(root());
    const available = vi.spyOn(f.context.modelRegistry, "getAvailable");
    const find = vi.spyOn(f.context.modelRegistry, "find");
    const nativeAuth = vi.fn(async () => { throw new Error("native auth must never start"); });
    vi.mocked(f.pi.setModel).mockImplementation(nativeAuth);
    const commit = vi.spyOn(f.main, "setBinding");
    await expect(f.provider.invoke("setModel", { id: f.main.id, model }, f.invocation))
      .rejects.toThrow("Main setModel is not supported yet (own or remote); see smarty-dev#4153");
    expect(available).not.toHaveBeenCalled(); expect(find).not.toHaveBeenCalled();
    expect(commit).not.toHaveBeenCalled(); expect(nativeAuth).not.toHaveBeenCalled();
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.entries).toHaveLength(0);
    expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "low" });
  });

  it("defers direct controller model changes as defense in depth", async () => {
    const f = fixture(root()); const fence = vi.fn();
    await expect(f.main.setBinding({ operation: "setModel", model: { provider: "probe", id: "b" } }, f.main.id, f.context, fence))
      .rejects.toThrow(/Main setModel.*smarty-dev#4153/);
    expect(fence).not.toHaveBeenCalled(); expect(f.pi.setModel).not.toHaveBeenCalled();
    expect(f.entries).toHaveLength(0); expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "low" });
  });

  it.each([["setModel", undefined], ["setThinking", undefined], ["setModel", "another-machine"], ["setThinking", "another-machine"]] as const)("refuses a remote Main %s (%s) without publication or mutation, even with launch enrollment", async (operation, remoteHost) => {
    const mesh = root(), callerId = session();
    vi.stubEnv("SMARTY_LEAD_SESSION", callerId);
    vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", JSON.stringify([callerId]));
    const target = fixture(mesh), caller = fixture(mesh, callerId);
    target.self.mainBindings = true;
    if (remoteHost) target.self.remoteHost = remoteHost;
    const send = vi.spyOn(caller.control, "requestResult");
    await expect(caller.provider.invoke(operation, { id: target.main.id, model: "probe/b", thinking: "high" }, caller.invocation))
      .rejects.toThrow(operation === "setModel" ? "Main setModel is not supported yet (own or remote); see smarty-dev#4153" : "remote Main model changes are not supported yet; see smarty-dev#4153");
    expect(send).not.toHaveBeenCalled();
    expect(caller.control.mesh.read({ topic: "fabric.control.command" })).toHaveLength(0);
    for (const f of [target, caller]) {
      expect(f.pi.setModel).not.toHaveBeenCalled(); expect(f.pi.setThinkingLevel).not.toHaveBeenCalled();
      expect(f.entries).toHaveLength(0); expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "low" });
    }
  });

  it.each(["setModel", "setThinking"] as const)("refuses incoming legacy %s control commands even with verified Main authority", async operation => {
    const f = fixture(root());
    for (const verification of [undefined, "mesh", "bridge"] as const) {
      const command = { version: 1 as const, commandId: "cmd", targetId: f.main.id, operation, replyTo: f.main.id, requestedAt: Date.now(), binding: { model: "probe/b", thinking: "high" as const } };
      await expect(f.provider.acceptControl(command, { id: f.main.id, kind: "main", name: "Main" }, undefined, verification))
        .resolves.toMatchObject({ accepted: false, error: "remote Main model changes are not supported yet; see smarty-dev#4153" });
    }
    expect(f.pi.setModel).not.toHaveBeenCalled(); expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.entries).toHaveLength(0);
  });

  it("refuses own-session thinking during reload/shutdown instead of queueing", async () => {
    const f = fixture(root()); f.main.prepareReload();
    await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, f.invocation)).rejects.toThrow(/not live/);
    f.main.closeFollowUpDrain();
    await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, f.invocation)).rejects.toThrow(/not live/);
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled();
  });

  it("refuses invalid, expired or cancelled own-session thinking", async () => {
    const f = fixture(root());
    await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, { ...f.invocation, checkExecutionBudget: () => { throw new Error("Execution expired"); } })).rejects.toThrow();
    const abort = new AbortController(); abort.abort();
    await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, { ...f.invocation, signal: abort.signal })).rejects.toThrow();
    for (const thinking of [undefined, "bogus"]) await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking }, f.invocation)).rejects.toThrow(/Main thinking level/);
    for (const scope of ["project", "global"]) await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high", scope }, f.invocation)).rejects.toThrow(/Main bindings support only session scope/);
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.pi.setModel).not.toHaveBeenCalled(); expect(f.entries).toHaveLength(0);
  });

  it("reports Pi's clamped effort rather than pretending the requested effort was applied", async () => {
    const f = fixture(root()); vi.mocked(f.pi.setThinkingLevel).mockImplementation(() => {});
    const result = await f.provider.invoke("setThinking", { id: f.main.id, thinking: "max" }, f.invocation);
    expect(f.pi.setThinkingLevel).toHaveBeenCalledWith("max");
    expect(result).toMatchObject({ thinking: "low", previous: { thinking: "low" } });
    expect(f.entries[0]).toMatchObject({ data: { after: { thinking: "low" } } });
  });

  it.each(["reload", "cancel", "deadline", "authority-loss"])("fences the thinking queue before synchronous native commit after %s", async loss => {
    const f = fixture(root()); const abort = new AbortController(); let expired = false;
    const pending = f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, { ...f.invocation, signal: abort.signal,
      checkExecutionBudget: () => { if (expired) throw new Error("Execution expired"); } });
    // The controller admitted the call, but its serialized Promise callback has not run.
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled();
    if (loss === "reload") f.main.prepareReload();
    if (loss === "cancel") abort.abort();
    if (loss === "deadline") expired = true;
    if (loss === "authority-loss") f.self.stale = true;
    await expect(pending).rejects.toThrow();
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.pi.setModel).not.toHaveBeenCalled();
    expect(f.entries).toHaveLength(0); expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "low" });
    if (loss !== "reload") {
      f.self.stale = false;
      await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "medium" }, f.invocation)).resolves.toMatchObject({ thinking: "medium" });
    }
  });

  it("public fabric_exec cancellation after thinking admission cannot cross the queued commit fence", async () => {
    const f = fixture(root()); const abort = new AbortController();
    const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.approvals.agent = "allow";
    const registry = new ActionRegistry(); registry.register(f.provider);
    const service = new FabricExecutionService(registry, config);
    const original = f.main.setBinding.bind(f.main);
    const admitted = vi.spyOn(f.main, "setBinding").mockImplementation((...args) => {
      const queued = original(...args);
      // Cancel after all provider checks and queue admission, before the native callback.
      abort.abort(new Error("Escape"));
      return queued;
    });
    const result = await service.execute({
      code: `return await agents.setThinking({ id: ${JSON.stringify(f.main.id)}, thinking: "high" });`,
      signal: abort.signal, parentToolCallId: "main-thinking-queued-cancel",
      context: { ...f.context, hasUI: false } as ExtensionContext, onPartial() {},
    });
    expect(admitted).toHaveBeenCalledOnce(); expect(result.success).toBe(false);
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.pi.setModel).not.toHaveBeenCalled();
    expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "low" });
    expect(f.entries).toHaveLength(0);
  });

  it("public fabric_exec Main setModel reports deferral with no native mutation", async () => {
    const f = fixture(root());
    const config = structuredClone(DEFAULT_FABRIC_CONFIG); config.approvals.agent = "allow";
    const registry = new ActionRegistry(); registry.register(f.provider);
    const service = new FabricExecutionService(registry, config);
    const result = await service.execute({
      code: `return await agents.setModel({ id: ${JSON.stringify(f.main.id)}, model: "probe/b" });`,
      signal: f.invocation.signal, parentToolCallId: "main-model-deferred", context: { ...f.context, hasUI: false } as ExtensionContext, onPartial() {},
    });
    expect(result.success).toBe(false); expect(JSON.stringify(result)).toContain("smarty-dev#4153");
    expect(f.pi.setModel).not.toHaveBeenCalled(); expect(f.pi.setThinkingLevel).not.toHaveBeenCalled();
    expect(f.entries).toHaveLength(0); expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "low" });
  });

  it("retains a thinking change already synchronously committed before cancellation", async () => {
    const f = fixture(root()); const abort = new AbortController();
    const result = await f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, { ...f.invocation, signal: abort.signal });
    abort.abort(); expect(result).toMatchObject({ thinking: "high" });
    expect(f.main.info(f.context)).toMatchObject({ model: "probe/a", thinking: "high" }); expect(f.entries).toHaveLength(1);
  });

  it.each(["agent", "actor"] as const)("denies %s lineage own-Main thinking authority", async kind => {
    const f = fixture(root(), session(), kind);
    // A child in a local host still cannot claim the Main's binding authority.
    Object.defineProperty(f.main, "local", { value: true });
    await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, f.invocation)).rejects.toThrow(/lineage is not Main authority/);
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.entries).toHaveLength(0);
  });

  it("keeps actor IDs on the existing ActorDirectory binding path", async () => {
    const f = fixture(root());
    const actor = await f.actors.create({ name: "main", instructions: "test", runner: "pi", model: "probe/a", responseMode: "text" });
    expect(await f.provider.invoke("setThinking", { id: actor.id, thinking: "high" }, f.invocation)).toMatchObject({ id: actor.id, thinking: "high" });
    expect(await f.provider.invoke("setModel", { id: actor.id, model: "probe/b" }, f.invocation)).toMatchObject({ id: actor.id, model: "probe/b" });
    expect(await f.provider.invoke("setThinking", { id: "main", thinking: "low" }, f.invocation)).toMatchObject({ id: actor.id, thinking: "low" });
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.pi.setModel).not.toHaveBeenCalled();
  });
});
