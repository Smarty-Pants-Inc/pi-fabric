import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { PassThrough } from "node:stream";
import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { AgentCompletionInbox, AGENT_COMPLETION_MESSAGE_TYPE } from "../src/agents/completion-inbox.js";
import { consumeCompletion, saveCompletion } from "../src/agents/completion-journal.js";
import { RemoteTaskClient, RemoteTaskOwner, assertRemoteTaskLead, remoteTaskRequest } from "../src/agents/remote-tasks.js";
import type { AgentHandleInfo, AgentRunRecord, AgentRunResult } from "../src/agents/types.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { MeshBridge, StoreBridgeSide, RemoteBridgeSide, serveBridgeAgent } from "../src/mesh/bridge.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidentHost } from "../src/residency/host.js";
import { residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { REMOTE_TASK_ERROR_CODES, RemoteTaskError } from "../src/agents/remote-task-errors.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { createAgentServiceClient } from "../src/agents/service-provider.js";
import { AGENTS_ACTION_DESCRIPTORS } from "../src/providers/agents-actions.js";

const hash = (id: string): string => createHash("sha256").update(id).digest("hex");
const cleanups: Array<() => Promise<unknown>> = [];
afterEach(async () => { for (const close of cleanups.splice(0).reverse()) await close(); vi.restoreAllMocks(); });
const scratch = (): string => fs.mkdtempSync(path.join(os.tmpdir(), "fabric-remote-capability-"));

const fixture = async () => {
  const root = scratch();
  const a = path.join(root, "work-host"), b = path.join(root, "capability-host");
  fs.mkdirSync(a); fs.mkdirSync(b);
  fs.writeFileSync(path.join(b, "corpus.txt"), "answer-only-on-root-B");
  const identity: MeshIdentity = { id: "session:work-lead", sessionId: "work-lead", name: "work lead", kind: "main" };
  const hubIdentity: MeshIdentity = { id: "session:hub-lead", sessionId: "hub-lead", name: "hub lead", kind: "main" };
  const meshA = new MeshStore(path.join(a, "mesh"), 64 * 1024, 1000), meshB = new MeshStore(path.join(b, "mesh"), 64 * 1024, 1000);
  const directory = new ParticipantDirectory(meshA, { enabled: true, hostId: identity.id, rootId: identity.id, identity, reapDeadHosts: false });
  let role = "project-agent";
  directory.registerSource(() => [{ format: 1, id: identity.id, rootId: identity.id, kind: "root", ownerHostId: identity.id,
    ownerIdentityId: identity.id, name: identity.name, role, interactive: true, status: "idle", runner: "pi", transport: "host",
    capabilities: ["fabric", "followUp", "steer"], sessionId: identity.sessionId!, cwd: a, projectRoot: a, startedAt: 1,
    updatedAt: Date.now(), controlProtocol: "v1" }]);
  await directory.start();
  cleanups.push(() => directory.close());
  const hubDirectory = new ParticipantDirectory(meshB, { enabled: true, hostId: hubIdentity.id, rootId: hubIdentity.id, identity: hubIdentity, reapDeadHosts: false });
  hubDirectory.registerSource(() => [{ format: 1, id: hubIdentity.id, rootId: hubIdentity.id, kind: "root", ownerHostId: hubIdentity.id,
    ownerIdentityId: hubIdentity.id, name: hubIdentity.name, role: "project-agent", interactive: true, status: "idle", runner: "pi", transport: "host",
    capabilities: ["fabric", "followUp", "steer"], sessionId: hubIdentity.sessionId!, cwd: b, projectRoot: b, startedAt: 1,
    updatedAt: Date.now(), controlProtocol: "v1" }]);
  await hubDirectory.start();
  cleanups.push(() => hubDirectory.close());
  const config: ResidentHostConfig = {
    format: 1, rootId: hubIdentity.id, sessionId: hubIdentity.sessionId!, cwd: b, projectRoot: b, meshRoot: meshB.root,
    actorRoot: path.join(b, "actors"), sessionActorRoot: path.join(b, "actors", "hub"), residencyRoot: residentRoot(meshB.root, hubIdentity.id),
    fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, transport: "process", budgetUsd: 0, retainRuns: true,
      hostCapabilities: ["corpus", "github"], defaultTools: ["read"], deniedModels: ["fixture/denied"],
      remoteSpawnPolicy: { writableRoots: [], shell: "deny", tools: ["read"] } },
    mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, retention: { ...DEFAULT_FABRIC_CONFIG.retention },
    workerPath: path.resolve("tests/fixtures/remote-task-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "fixture", id: "visible" }, { provider: "fixture", id: "denied" }], aliases: {}, defaultModel: "fixture/visible" },
  };
  let host = new ResidentHost(config);
  await host.start();
  cleanups.push(async () => { await host.close(); });
  const control = new FabricControlPlane(meshA, identity, { enabled: true, hostId: identity.id, ownerIncarnation: directory.ownerIncarnation,
    pollMs: 20, readMirroredOwner: (owner, sender, target) => directory.mirroredControlOwner(owner, sender, target) });
  const manager = new AgentManager(a, { ...DEFAULT_FABRIC_CONFIG.agents, transport: "process", budgetUsd: 0, retainRuns: true,
    defaultTools: ["read"], hostCapabilities: [] }, { workerPath: config.workerPath, runRoot: path.join(a, "runs"),
    mainAgentId: identity.id, identityId: identity.id, hostId: identity.id });
  const actors = new ActorManager("work-lead", identity, meshA, config.mesh, manager, () => {}, { persistent: false });
  const lifecycle = new LifecycleBroker(meshA, identity, directory, { enabled: false, pollMs: 20, maxReadEvents: 1000 }, async () => {});
  const main = { id: identity.id, local: true, interactive: true, matches: (id: string) => id === identity.id } as FabricMainAgentTarget;
  const provider = new AgentsProvider(manager, actors, new GlobalActorRegistry(path.join(a, "globals"), 64 * 1024), main, directory, control, lifecycle);
  const ctx = { mode: "tui", cwd: a, hasUI: false, isIdle: () => true, hasPendingMessages: () => false,
    sessionManager: { getSessionId: () => "work-lead", getEntries: () => [] },
    modelRegistry: { getAvailable: () => [{ provider: "fixture", id: "visible" }], find: (provider: string, id: string) => ({ provider, id }) },
  } as unknown as ExtensionContext;
  const context: FabricInvocationContext = { cwd: a, extensionContext: ctx, signal: new AbortController().signal, parentToolCallId: "proof", nestedToolCallId: "proof", update() {} };
  const messages: Array<{ customType: string; content: string; details: { ids: string[] } }> = [];
  let delivered!: () => void;
  const completion = new Promise<void>(resolve => { delivered = resolve; });
  const api = { on: () => undefined, sendMessage: (message: typeof messages[number]) => { messages.push(message); delivered(); } } as unknown as ExtensionAPI;
  const inbox = new AgentCompletionInbox(api, ctx);
  provider.onRemoteComplete = (result, recipient) => { saveCompletion(meshA.root, recipient, result); inbox.enqueue(result); };
  provider.onRemoteConsumed = id => { consumeCompletion(meshA.root, id, "work-lead"); inbox.acknowledge(id); };
  control.start((command, from, signal, verification) => provider.acceptControl(command, from, signal, verification));
  cleanups.push(async () => { inbox.close(); await control.close(); await actors.close(); await manager.close(); await lifecycle.close(); });
  // The real stdio bridge protocol, with B as the hub and A as a work host.
  const toAgent = new PassThrough(), fromAgent = new PassThrough();
  const agent = serveBridgeAgent(new StoreBridgeSide(meshA, "ryzen1"), toAgent, fromAgent);
  const remote = new RemoteBridgeSide(fromAgent, toAgent);
  const bridge = new MeshBridge({ localName: "ryzen1", remoteName: "work-host", local: new StoreBridgeSide(meshB, "work-host"), remote,
    cursorPath: path.join(root, "bridge-cursor.json"), presenceMs: 0 });
  await bridge.start();
  const running = bridge.run();
  cleanups.push(async () => { await bridge.stop(); await running; toAgent.end(); await agent; });
  const spawn = (args: Record<string, unknown> = {}) => provider.invoke("spawn", { task: "{}", transport: "process", needs: ["corpus"], model: "fixture/visible", ...args }, context) as Promise<AgentHandleInfo>;
  const wire = (request: Record<string, unknown>, overrides: Record<string, unknown> = {}, key = Math.random().toString()) => control.requestResult(host.hostId, host.hostId, "spawn", {
    ownerIncarnation: host.participants.ownerIncarnation, data: { key, returnTo: identity.id, sessionId: identity.sessionId,
      request: { task: "{}", runner: "pi", model: "fixture/visible", cwd: b, needs: ["corpus"], ...request }, ...overrides },
  }, host.identity.id, { routedRemoteHost: "ryzen1", idempotencyKey: key, timeoutMs: 5000 });
  return { root, a, b, meshA, meshB, directory, hubDirectory, config, host: () => host, bridge, control, provider, context, manager, messages, completion, spawn, wire,
    setRole: async (value: string) => { role = value; await directory.refresh(); await bridge.syncPresence(); },
    restartOwner: async () => { await host.close(); host = new ResidentHost(config); await host.start(); await bridge.syncPresence(); },
  };
};

describe("remote capability process tasks over two real mesh roots", () => {
  it("a work-host lead spawns needs corpus on B and receives its native answer in A's completion inbox", async () => {
    const f = await fixture();
    const handle = await f.spawn({ cwd: f.b, requires: [path.join(f.b, "corpus.txt")], idempotencyKey: "proof-one" });
    await f.completion;
    const result = await f.provider.invoke("result", { id: handle.id }, f.context) as AgentRunResult;
    expect(result.status).toBe("completed");
    expect(result.value).toMatchObject({ cwd: f.b, corpus: "answer-only-on-root-B", spawner: "session:work-lead", spawnerSession: "work-lead",
      spawnerArgument: "session:work-lead", returnAddress: { spawnerId: "session:work-lead", spawnerSessionId: "work-lead", ancestors: ["session:work-lead"] } });
    expect(f.manager.list()).toEqual([]);
    expect(f.host().agents.list().map(run => run.id)).toEqual([handle.id]);
    expect(f.messages).toHaveLength(1);
    expect(f.messages[0]).toMatchObject({ customType: AGENT_COMPLETION_MESSAGE_TYPE, details: { ids: [handle.id] } });
    expect(f.messages[0]!.content).toContain("answer-only-on-root-B");
    expect(f.meshA.read({ topic: "fabric.control.command", limit: 1000 }).some(event => event.kind === "spawn" && event.to === f.host().hostId)).toBe(true);
    expect(f.meshA.read({ topic: "fabric.control.command", limit: 1000 }).some(event => event.kind === "taskComplete" && event.to === "session:work-lead" && event.verification === "bridge")).toBe(true);
    expect(await f.spawn({ cwd: f.b, requires: [path.join(f.b, "corpus.txt")], idempotencyKey: "proof-one" })).toMatchObject({ id: handle.id });
    if (process.env.FABRIC_REMOTE_TASK_PROOF_OUTPUT) fs.writeFileSync(process.env.FABRIC_REMOTE_TASK_PROOF_OUTPUT, JSON.stringify({ pass: true,
      rootA: f.meshA.root, rootB: f.meshB.root, remoteAgent: handle.id, status: result.status, nativeValue: result.value, inbox: f.messages[0] }, null, 2));
  });

  it("status/wait/result use the proxy; an observation timeout leaves the native task running and stop joins its exit", async () => {
    const f = await fixture();
    const handle = await f.spawn({ cwd: f.b, task: '{"delayMs":5000}' });
    const status = await f.provider.invoke("status", { id: handle.id }, f.context) as AgentRunRecord;
    expect(status.status).toBe("running");
    const timeout = await f.provider.invoke("wait", { id: handle.id, timeoutMs: 1000 }, f.context) as { status: string; waitTimedOut: boolean };
    expect(timeout).toMatchObject({ status: "running", waitTimedOut: true });
    expect(f.host().agents.status(handle.id).status).toBe("running");
    const stopped = await f.provider.invoke("stop", { id: handle.id }, f.context) as { stopped: boolean; status: string };
    expect(stopped).toMatchObject({ stopped: true, status: "stopped" });
    expect(f.host().agents.isSettled(handle.id)).toBe(true);
    const terminal = await f.provider.invoke("result", { id: handle.id }, f.context) as AgentRunResult;
    expect(terminal.status).toBe("stopped");
  });

  it("persists the caller proxy across restart, deduplicates unchanged keys, and retains terminal results after owner restart", async () => {
    const f = await fixture();
    const handle = await f.spawn({ cwd: f.b, idempotencyKey: "restart" });
    const result = await f.provider.invoke("wait", { id: handle.id }, f.context) as AgentRunResult;
    expect(result.status).toBe("completed");
    const restarted = new RemoteTaskClient(f.meshA.root, f.directory.options.identity, f.directory.options.hostId, f.directory, f.control);
    expect(restarted.has(handle.id)).toBe(true);
    expect(await f.spawn({ cwd: f.b, idempotencyKey: "restart" })).toMatchObject({ id: handle.id });
    await expect(f.spawn({ task: '{"changed":true}', cwd: f.b, idempotencyKey: "restart" })).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_POLICY_REFUSED" });
    await f.restartOwner();
    expect(await restarted.observe(handle.id, "taskResult", 1000)).toMatchObject({ id: handle.id, status: "completed", value: result.value });
    expect(f.host().agents.list()).toEqual([]); // terminal receipt, not re-execution
  });

  it("retains owner-side deduplication beyond control claims, and refuses interrupted admission after restart", async () => {
    const f = await fixture();
    const handle = await f.spawn({ cwd: f.b, idempotencyKey: "durable-owner-key" });
    await f.provider.invoke("wait", { id: handle.id }, f.context);
    const callerIntent = path.join(f.meshA.root, "remote-tasks", "callers", hash("session:work-lead"), `intent-${hash("durable-owner-key")}.json`);
    const request = JSON.parse(fs.readFileSync(callerIntent, "utf8")).request;
    await f.restartOwner();
    expect(await f.wire(request, { key: "durable-owner-key" }, "fresh-control-key")).toMatchObject({ id: handle.id });
    expect(f.host().agents.list()).toEqual([]);
    const intent = path.join(f.config.residencyRoot, "remote-tasks", "intents", `${hash("session:work-lead\0durable-owner-key")}.json`);
    const pending = JSON.parse(fs.readFileSync(intent, "utf8"));
    delete pending.handle; // recorded-call crash cut: no native launch or destructive filesystem test
    fs.writeFileSync(intent, JSON.stringify(pending));
    await f.restartOwner();
    await expect(f.wire(request, { key: "durable-owner-key" }, "interrupted-control-key")).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_LOST" });
    expect(f.host().agents.list()).toEqual([]);
  });

  it("retains an unknown completion ACK and rekeys delivery to the same caller's restarted activation", async () => {
    const f = await fixture();
    const handle = await f.spawn({ cwd: f.b });
    await f.completion;
    await f.provider.invoke("wait", { id: handle.id }, f.context);
    const original = JSON.parse(fs.readFileSync(path.join(f.config.residencyRoot, "remote-tasks", `${hash(handle.id)}.json`), "utf8"));
    // Recorded-call delivery probe: preserve the real run outcome, simulate only the ACK loss.
    const replayRoot = path.join(f.b, "delivery-replay");
    fs.mkdirSync(path.join(replayRoot, "remote-tasks"), { recursive: true });
    fs.writeFileSync(path.join(replayRoot, "remote-tasks", `${hash(handle.id)}.json`), JSON.stringify({ ...original, delivered: false }));
    const caller = f.host().participants.get("session:work-lead", undefined, { fresh: true })!;
    let incarnation = caller.ownerIncarnation;
    const keys: string[] = [];
    const requestResult = vi.fn(async (_owner, target, operation, input, _identity, options) => {
      expect(target).toBe("session:work-lead");
      expect(operation).toBe("taskComplete");
      expect(input.data.result.id).toBe(handle.id);
      keys.push(options.idempotencyKey);
      if (keys.length === 1) throw new RemoteTaskError("FABRIC_REMOTE_TASK_BRIDGE_STALE", "recorded unknown ACK");
      return true;
    });
    const owner = new RemoteTaskOwner(replayRoot, f.host().hostId, f.host().agents,
      { get: () => ({ ...caller, ownerIncarnation: incarnation }) } as unknown as ParticipantDirectory,
      { requestResult } as unknown as FabricControlPlane, f.config.agents);
    await expect(owner.flushCompletions()).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_BRIDGE_STALE" });
    expect(owner.pendingCompletions).toBe(true);
    incarnation = "fixture:restarted-caller";
    await owner.flushCompletions();
    expect(keys).toEqual([`remote-completion:${handle.id}:${caller.ownerIncarnation}`, `remote-completion:${handle.id}:fixture:restarted-caller`]);
    expect(owner.pendingCompletions).toBe(false);
  });

  it("allows host-authorized write and shell settings to narrow, and defaults an omitted cwd to execution-host TMPDIR", async () => {
    const f = await fixture();
    f.config.agents.remoteSpawnPolicy = { writableRoots: [f.b], shell: "unconfined", tools: ["read", "bash"] };
    const handle = await f.spawn({ cwd: f.b, readOnly: false, writableRoots: [f.b], shell: "deny", tools: ["read"] });
    const result = await f.provider.invoke("wait", { id: handle.id }, f.context) as AgentRunResult;
    expect(result.value).toMatchObject({ writePolicy: { readOnly: false, writableRoots: [fs.realpathSync(f.b)], shell: "deny" } });
    f.config.agents.remoteSpawnPolicy = { writableRoots: [], shell: "deny", tools: ["read"] };
    const tmp = await f.spawn();
    expect(tmp.cwd).not.toBe(f.a);
    expect(tmp.cwd).not.toBe(f.b);
    expect(path.dirname(tmp.cwd)).toBe(fs.realpathSync(os.tmpdir()));
    expect(path.basename(tmp.cwd)).toMatch(/^fabric-remote-task-/);
    await f.provider.invoke("wait", { id: tmp.id }, f.context);
  });

  it.each([
    ["return address", {}, { returnTo: "session:hub-lead" }],
    ["return session", {}, { sessionId: "hub-lead" }],
    ["shell widening", { shell: "unconfined" }, {}],
    ["write widening", { readOnly: false, writableRoots: ["REPLACE_CWD"] }, {}],
    ["tool widening", { tools: ["bash"] }, {}],
    ["denied model", { model: "fixture/denied" }, {}],
    ["relative cwd", { cwd: "relative" }, {}],
    ["internal launch fields", { residentStartupProbe: true }, {}],
    ["unsupported needs", { needs: ["other"] }, {}],
    ["missing selected-host input", { requires: ["REPLACE_MISSING"] }, {}],
  ])("refuses %s at the authenticated receiver without starting a worker", async (_name, request, overrides) => {
    const f = await fixture();
    const fixed = { ...request };
    if ("writableRoots" in fixed) fixed.writableRoots = [f.b];
    if ("requires" in fixed) fixed.requires = [path.join(f.b, "missing")];
    await expect(f.wire(fixed, overrides)).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_POLICY_REFUSED" });
    expect(f.host().agents.list()).toEqual([]);
  });

  it.each(["worktree-agent", "task-agent", "unknown"])("a mirrored %s Main cannot add a remote hop", async role => {
    const f = await fixture();
    await f.setRole(role);
    await expect(f.wire({})).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_POLICY_REFUSED" });
    expect(f.host().agents.list()).toEqual([]);
  });

  it("refuses unknown and stale owners without publishing, and keeps local-capable/reserved-local tasks on A", async () => {
    const f = await fixture();
    await expect(f.spawn({ needs: ["missing"] })).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_NO_OWNER" });
    const selected = f.directory.taskOwners()[0]!;
    const source = { ...f.directory, self: () => f.directory.self(), taskOwners: () => [{ ...selected, stale: true }] } as unknown as ParticipantDirectory;
    const client = new RemoteTaskClient(f.meshA.root, f.directory.options.identity, f.directory.options.hostId, source, f.control);
    await expect(client.spawn({ task: "{}", needs: ["corpus"] })).rejects.toMatchObject({ code: "FABRIC_REMOTE_TASK_BRIDGE_STALE" });
    const local = await f.spawn({ needs: ["LOCAL", "corpus"] });
    expect(local.cwd).toBe(f.a);
    await f.manager.wait(local.id);
    f.manager.config.hostCapabilities = ["corpus"];
    const capable = await f.spawn();
    expect(capable.cwd).toBe(f.a);
    await f.manager.wait(capable.id);
    expect(f.host().agents.list()).toEqual([]);
  });
});

describe("remote capability authority and host-only configuration", () => {
  it("registers and dispatches the public result alias in both guest and hosted clients", async () => {
    expect(AGENTS_ACTION_DESCRIPTORS.some(action => action.name === "result")).toBe(true);
    const calls: string[] = [];
    const client = createAgentServiceClient(async action => { calls.push(action); return { status: "completed", text: "done" }; });
    expect(await client.result("remote-id")).toMatchObject({ status: "completed", text: "done" });
    const result = await new QuickJsRuntime().execute('return await agents.result({id:"remote-id"});',
      async ref => { calls.push(ref); return { status: "completed", text: "done" }; }, { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(result.value).toMatchObject({ status: "completed", text: "done" });
    expect(calls).toEqual(["result", "agents.result"]);
  });
  it.each(REMOTE_TASK_ERROR_CODES)("preserves typed %s through the Main's QuickJS host-call boundary", async code => {
    const result = await new QuickJsRuntime().execute('try { await agents.spawn({task:"work",needs:["corpus"]}); } catch (error) { return {code:error.code,name:error.name}; }',
      async () => { throw new RemoteTaskError(code, "fixture refusal"); }, { timeoutMs: 5000, memoryLimitBytes: 32 * 1024 * 1024 });
    expect(result.value).toEqual({ code, name: "RemoteTaskError" });
  });
  it("defaults empty; workspace cannot advertise capabilities or relax host remote policy", () => {
    expect(normalizeFabricConfig({}).agents.hostCapabilities).toEqual([]);
    const root = scratch(), agentDir = path.join(root, "profile"), cwd = path.join(root, "workspace");
    fs.mkdirSync(agentDir); fs.mkdirSync(path.join(cwd, ".pi"), { recursive: true });
    const policy = { writableRoots: [], shell: "deny", tools: ["read"] };
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { hostCapabilities: ["CORPUS", "github"], remoteSpawnPolicy: policy } }));
    fs.writeFileSync(path.join(cwd, ".pi", "fabric.json"), JSON.stringify({ agents: { hostCapabilities: ["other"], remoteSpawnPolicy: { writableRoots: [root], shell: "unconfined", tools: ["bash"] } } }));
    expect(loadFabricConfig({ cwd, agentDir, projectTrusted: true }).agents).toMatchObject({ hostCapabilities: ["corpus", "github"], remoteSpawnPolicy: policy });
    expect(() => normalizeFabricConfig({ agents: { hostCapabilities: ["local"] } })).toThrow("reserved");
  });
  it("requires an authenticated lead root, and refuses inherited/private run authority", () => {
    const f = { id: "session:lead", sessionId: "lead", name: "lead", kind: "main" as const };
    const caller = { id: f.id, rootId: f.id, sessionId: "lead", ownerIdentityId: f.id, kind: "root", role: "project-agent", stale: false } as ReturnType<ParticipantDirectory["self"]>;
    expect(assertRemoteTaskLead(f, caller)).toBe(caller);
    for (const kind of ["agent", "actor"] as const) expect(() => assertRemoteTaskLead({ ...f, kind }, caller)).toThrow("lead Main");
    expect(() => assertRemoteTaskLead(f, { ...caller, rootId: "session:other" })).toThrow("lead Main");
    for (const field of ["recursive", "worktree", "sessionSeed", "forkSeed", "scope", "inheritedScope", "residentStartupProbe"])
      expect(() => remoteTaskRequest({ task: "work", [field]: true })).toThrow("unseeded");
  });
});
