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
  it("sets its own Main through the public provider with native before/after readback and caller audit", async () => {
    const f = fixture(root());
    const effort = await f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, f.invocation);
    expect(effort).toMatchObject({ id: f.main.id, kind: "main", thinking: "high", caller: f.main.id, previous: { model: "probe/a", thinking: "low" } });
    expect(f.pi.setThinkingLevel).toHaveBeenCalledWith("high");
    const switched = await f.provider.invoke("setModel", { id: f.main.id, model: "probe/b" }, f.invocation);
    expect(switched).toMatchObject({ model: "probe/b", thinking: "high", previous: { model: "probe/a", thinking: "high" } });
    expect(f.pi.setModel).toHaveBeenCalledWith(models[1]);
    expect(f.entries).toEqual([
      { customType: "pi-fabric.main-binding-change", data: expect.objectContaining({ action: "agents.setThinking", caller: f.main.id, before: { model: "probe/a", thinking: "low" }, after: { model: "probe/a", thinking: "high" } }) },
      { customType: "pi-fabric.main-binding-change", data: expect.objectContaining({ action: "agents.setModel", caller: f.main.id, before: { model: "probe/a", thinking: "high" }, after: { model: "probe/b", thinking: "high" } }) },
    ]);
  });

  it.each(["lead", "org", "product-owner"])("lets a registered %s Main control another live Main over real mesh IPC", async registration => {
    const mesh = root(), callerId = session();
    if (registration === "lead") vi.stubEnv("SMARTY_LEAD_SESSION", callerId);
    else vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", JSON.stringify([callerId]));
    const target = fixture(mesh);
    // Enrollment belongs to the receiver and is immutable after initialization.
    vi.stubEnv("SMARTY_LEAD_SESSION", ""); vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", "[]");
    const caller = fixture(mesh, callerId);
    const effort = await caller.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, caller.invocation);
    expect(effort).toMatchObject({ id: target.main.id, thinking: "high", caller: callerId, previous: { thinking: "low" } });
    const result = await caller.provider.invoke("setModel", { id: target.main.id, model: "probe/b" }, caller.invocation);
    expect(result).toMatchObject({ id: target.main.id, model: "probe/b", caller: callerId, previous: { model: "probe/a" } });
    expect(target.pi.setThinkingLevel).toHaveBeenCalledOnce(); expect(target.pi.setModel).toHaveBeenCalledOnce();
    expect(target.entries).toHaveLength(2); expect(caller.entries).toHaveLength(0);
  });

  it("refuses an unauthorized Main, even one claiming an org/owner role or principal in request data", async () => {
    const mesh = root(), target = fixture(mesh), caller = fixture(mesh);
    caller.self.role = "product-owner";
    for (const action of ["setThinking", "setModel"]) {
      await expect(caller.provider.invoke(action, { id: target.main.id, thinking: "high", model: "probe/b", principal: { binding: "org-agent", id: "owner" } }, caller.invocation))
        .rejects.toThrow(/Unauthorized Main binding change/);
    }
    expect(target.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(target.pi.setModel).not.toHaveBeenCalled(); expect(target.entries).toHaveLength(0);
  });

  it("does not promote a registered task or actor into a Main controller", async () => {
    const mesh = root(), childId = session(); vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", JSON.stringify([childId]));
    const target = fixture(mesh); const child = fixture(mesh, childId, "agent");
    await expect(child.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, child.invocation)).rejects.toThrow(/Unauthorized Main binding change/);
    expect(target.pi.setThinkingLevel).not.toHaveBeenCalled();
  });

  it.each(["missing", "stale", "legacy", "no-capability", "disabled-capability", "malformed-capability"])("refuses a Main without a live control path (%s), before publication", async reason => {
    const mesh = root(), target = fixture(mesh), caller = fixture(mesh);
    if (reason === "missing") members.delete(target.main.id);
    if (reason === "stale") target.self.stale = true;
    if (reason === "legacy") target.self.controlProtocol = "legacy";
    if (reason === "no-capability") delete target.self.mainBindings;
    if (reason === "disabled-capability") target.self.mainBindings = false;
    if (reason === "malformed-capability") Object.assign(target.self, { mainBindings: "true" });
    const send = vi.spyOn(caller.control, "requestResult");
    await expect(caller.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, caller.invocation)).rejects.toThrow(/no live Main binding control path/);
    expect(send).not.toHaveBeenCalled(); expect(target.pi.setThinkingLevel).not.toHaveBeenCalled();
  });

  it("rechecks receiver liveness after lookup and refuses reload/shutdown instead of queueing", async () => {
    const mesh = root(), callerId = session(); vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", JSON.stringify([callerId]));
    const target = fixture(mesh), caller = fixture(mesh, callerId);
    target.main.prepareReload();
    await expect(caller.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, caller.invocation)).rejects.toThrow(/not live/);
    target.main.closeFollowUpDrain();
    await expect(target.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, target.invocation)).rejects.toThrow(/not live/);
    expect(target.pi.setThinkingLevel).not.toHaveBeenCalled();
  });

  it("refuses unverified control envelopes and invalid, expired or cancelled changes", async () => {
    const f = fixture(root());
    const command = { version: 1 as const, commandId: "cmd", targetId: f.main.id, operation: "setThinking" as const, replyTo: f.main.id, requestedAt: Date.now(), binding: { thinking: "high" as const } };
    const reply = await f.provider.acceptControl(command, { id: f.main.id, kind: "main", name: "Main" });
    expect(reply).toMatchObject({ accepted: false, error: expect.stringMatching(/Unauthorized Main binding change/) });
    const expired = await f.provider.acceptControl({ ...command, deadlineAt: Date.now() - 1 }, { id: f.main.id, kind: "main", name: "Main" }, undefined, "mesh");
    expect(expired).toMatchObject({ accepted: false, error: expect.stringMatching(/expired/) });
    const abort = new AbortController(); abort.abort();
    await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high" }, { ...f.invocation, signal: abort.signal })).rejects.toThrow();
    for (const thinking of [undefined, "bogus"]) await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking }, f.invocation)).rejects.toThrow(/Main thinking level/);
    for (const scope of ["project", "global"]) await expect(f.provider.invoke("setThinking", { id: f.main.id, thinking: "high", scope }, f.invocation)).rejects.toThrow(/Main bindings support only session scope/);
    await expect(f.provider.invoke("setModel", { id: f.main.id }, f.invocation)).rejects.toThrow(/Main model is required/);
    await expect(f.provider.invoke("setModel", { id: f.main.id, model: "probe/denied" }, f.invocation)).rejects.toMatchObject({ code: "FABRIC_MODEL_DENIED" });
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(f.pi.setModel).not.toHaveBeenCalled();
  });

  it("reports Pi's clamped effort rather than pretending the requested effort was applied", async () => {
    const f = fixture(root());
    vi.mocked(f.pi.setThinkingLevel).mockImplementation(() => {}); // Native capability clamp retains low.
    const result = await f.provider.invoke("setThinking", { id: f.main.id, thinking: "max" }, f.invocation);
    expect(f.pi.setThinkingLevel).toHaveBeenCalledWith("max");
    expect(result).toMatchObject({ thinking: "low", previous: { thinking: "low" } });
    expect(f.entries[0]).toMatchObject({ data: { after: { thinking: "low" } } });
  });

  it("refuses an unavailable or unauthenticated model without claiming a successful change", async () => {
    const f = fixture(root());
    await expect(f.provider.invoke("setModel", { id: f.main.id, model: "probe/missing" }, f.invocation)).rejects.toThrow(/not available/);
    expect(f.pi.setModel).not.toHaveBeenCalled();
    vi.mocked(f.pi.setModel).mockResolvedValue(false);
    await expect(f.provider.invoke("setModel", { id: f.main.id, model: "probe/b" }, f.invocation)).rejects.toThrow(/No authentication configured/);
    expect(f.main.info(f.context).model).toBe("probe/a"); expect(f.entries).toHaveLength(0);
  });

  it.each(["reload", "cancel", "authority-loss"])("fences a model refresh before native mutation after %s", async loss => {
    const f = fixture(root());
    let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    const refresh = vi.fn(() => barrier);
    Object.assign(f.context.modelRegistry, { refresh });
    vi.spyOn(f.context.modelRegistry, "getAvailable").mockImplementationOnce(() => []);
    const abort = new AbortController();
    const pending = f.provider.invoke("setModel", { id: f.main.id, model: "probe/b" }, { ...f.invocation, signal: abort.signal });
    await vi.waitFor(() => expect(refresh).toHaveBeenCalledOnce());
    if (loss === "reload") f.main.prepareReload();
    if (loss === "cancel") abort.abort();
    if (loss === "authority-loss") f.self.stale = true;
    release();
    await expect(pending).rejects.toThrow();
    expect(f.pi.setModel).not.toHaveBeenCalled(); expect(f.entries).toHaveLength(0);
  });

  it.each(["abort", "timeout"] as const)("delivers remote caller %s to the model-refresh commit fence before resolution resumes", async cause => {
    const mesh = root(), callerId = session(); vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", JSON.stringify([callerId]));
    const target = fixture(mesh), caller = fixture(mesh, callerId);
    let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    const refresh = vi.fn(() => barrier);
    Object.assign(target.context.modelRegistry, { refresh });
    vi.spyOn(target.context.modelRegistry, "getAvailable").mockImplementationOnce(() => []);
    let receiverSignal: AbortSignal | undefined;
    const accept = target.provider.acceptControl.bind(target.provider);
    vi.spyOn(target.provider, "acceptControl").mockImplementation((command, from, signal, verification) => {
      if (command.operation === "setModel") receiverSignal = signal;
      return accept(command, from, signal, verification);
    });
    const abort = new AbortController();
    const signal = cause === "timeout" ? AbortSignal.timeout(500) : abort.signal;
    const pending = caller.provider.invoke("setModel", { id: target.main.id, model: "probe/b" }, { ...caller.invocation, signal }).catch(error => error);
    let cancelledBeforeRelease = false;
    try {
      await vi.waitFor(() => expect(refresh).toHaveBeenCalledOnce());
      if (cause === "abort") abort.abort();
      expect(await pending).toMatchObject({ message: expect.stringMatching(/cancelled/) });
      // The wire deadline is still live (5 s); only consuming sender cancellation can pass.
      await vi.waitFor(() => { cancelledBeforeRelease = receiverSignal?.aborted === true; expect(cancelledBeforeRelease).toBe(true); }, { timeout: 700, interval: 10 }).catch(() => {});
    } finally { release(); }
    const acks = () => caller.control.mesh.read({ topic: "fabric.control.ack" }).filter(event => event.to === callerId);
    await vi.waitFor(() => expect(acks()).toHaveLength(1));
    expect(target.pi.setModel).not.toHaveBeenCalled();
    expect(target.entries).toHaveLength(0);
    expect(cancelledBeforeRelease).toBe(true);
    expect(acks()[0]!.data).toMatchObject({ accepted: false, error: expect.stringMatching(/expired or cancelled/) });
    expect(target.main.info(target.context).model).toBe("probe/a");
  });

  it("cancels a queued remote thinking change while an already committed model change completes exactly once", async () => {
    const mesh = root(), callerId = session(); vi.stubEnv("PI_FABRIC_MAIN_CONTROLLERS", JSON.stringify([callerId]));
    const target = fixture(mesh), caller = fixture(mesh, callerId);
    let release!: () => void;
    const barrier = new Promise<void>(resolve => { release = resolve; });
    const native = vi.mocked(target.pi.setModel).getMockImplementation()!;
    vi.mocked(target.pi.setModel).mockImplementation(async model => {
      await native(model); // Point of no return: committed native state, await its completion.
      await barrier; return true;
    });
    let thinkingSignal: AbortSignal | undefined;
    const accept = target.provider.acceptControl.bind(target.provider);
    vi.spyOn(target.provider, "acceptControl").mockImplementation((command, from, signal, verification) => {
      if (command.operation === "setThinking") thinkingSignal = signal;
      return accept(command, from, signal, verification);
    });
    const model = caller.provider.invoke("setModel", { id: target.main.id, model: "probe/b" }, caller.invocation);
    void model.catch(() => {});
    const abort = new AbortController();
    let thinking: Promise<unknown> | undefined;
    try {
      await vi.waitFor(() => expect(target.pi.setModel).toHaveBeenCalledOnce());
      thinking = caller.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, { ...caller.invocation, signal: abort.signal }).catch(error => error);
      await vi.waitFor(() => expect(thinkingSignal).toBeDefined());
      abort.abort();
      expect(await thinking).toMatchObject({ message: expect.stringMatching(/cancelled/) });
      await vi.waitFor(() => expect(thinkingSignal!.aborted).toBe(true));
    } finally { abort.abort(); release(); await thinking; }
    expect(await model).toMatchObject({ model: "probe/b", caller: callerId });
    const acks = () => caller.control.mesh.read({ topic: "fabric.control.ack" }).filter(event => event.to === callerId);
    await vi.waitFor(() => expect(acks()).toHaveLength(2));
    expect(target.pi.setThinkingLevel).not.toHaveBeenCalled(); expect(target.entries).toHaveLength(1);
    const command = caller.control.mesh.read({ topic: "fabric.control.command" }).find(event => event.kind === "setModel")!;
    await caller.control.mesh.publish({ topic: command.topic, kind: command.kind, from: command.from, to: target.main.id, data: command.data });
    await vi.waitFor(() => expect(acks()).toHaveLength(3));
    expect(acks().filter(event => (event.data as { accepted: boolean }).accepted)).toHaveLength(2);
    expect(target.pi.setModel).toHaveBeenCalledOnce(); expect(target.entries).toHaveLength(1);
  });

  it("refuses another-host Main when its advertised bridge has no control path, without publishing", async () => {
    const mesh = root(), target = fixture(mesh), caller = fixture(mesh);
    target.self.remoteHost = "unreachable";
    await expect(caller.provider.invoke("setThinking", { id: target.main.id, thinking: "high" }, caller.invocation)).rejects.toThrow(/remote host unreachable is unavailable/);
    expect(caller.control.mesh.read({ topic: "fabric.control.command" })).toHaveLength(0);
    expect(target.pi.setThinkingLevel).not.toHaveBeenCalled();
  });

  it("keeps actor IDs on the existing ActorDirectory binding path", async () => {
    const f = fixture(root());
    const actor = await f.actors.create({ name: "main", instructions: "test", runner: "pi", model: "probe/a", responseMode: "text" });
    const changed = await f.provider.invoke("setThinking", { id: actor.id, thinking: "high" }, f.invocation);
    expect(changed).toMatchObject({ id: actor.id, thinking: "high" });
    // "main" remains a valid actor name here; only session:<id> takes the new path.
    expect(await f.provider.invoke("setThinking", { id: "main", thinking: "low" }, f.invocation)).toMatchObject({ id: actor.id, thinking: "low" });
    expect(f.pi.setThinkingLevel).not.toHaveBeenCalled();
  });
});
