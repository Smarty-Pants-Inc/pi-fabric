import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ActorManager } from "../src/actors/manager.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";
import type { FabricActorValidWhileSource } from "../src/actors/types.js";

const roots: string[] = [];
const cleanups: Array<() => Promise<unknown>> = [];
const fixture = path.resolve("tests/fixtures/fake-worker.mjs");
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  // Windows can briefly retain an exited worker's cwd after close. Keep teardown
  // bounded, but retry EBUSY/EPERM/ENOTEMPTY rather than failing passed assertions.
  for (const root of roots.splice(0)) {
    await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

// Real provider -> AgentManager -> process worker, with the environment an actor's
// activation worker supplies. Both session (Main owned) and durable (resident owned)
// actors must keep their child results within the actor, not its lineage Main.
const setup = async (residency: "session" | "durable", notifyOnComplete = true, validWhile?: FabricActorValidWhileSource, responseMode: "text" | "directive" = "text", ownerMaxConcurrent = DEFAULT_FABRIC_CONFIG.agents.maxConcurrent) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-child-"));
  roots.push(root);
  const rootId = "session:root-main";
  const meshRoot = path.join(root, "mesh");
  const actorRoot = path.join(meshRoot, "actors");
  const actorScopeRoot = residency === "durable" ? actorRoot : path.join(actorRoot, "root-main");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const ownerAgents = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: ownerMaxConcurrent }, {
    workerPath: fixture, runRoot: path.join(root, "owner-runs"), mainAgentId: rootId,
  });
  cleanups.push(() => ownerAgents.close());
  const rootDeliveries = vi.fn();
  const resolveModel = vi.fn(async (model: string) => model);
  const makeOwner = () => new ActorManager("root-main", {
    id: residency === "durable" ? "resident:root-main" : rootId,
    name: "owner", kind: "main", sessionId: "root-main",
  }, mesh, meshConfig, ownerAgents, rootDeliveries, {
    persistent: true, actorRoot: actorScopeRoot, claimResidency: residency, rootId, resolvePiModel: resolveModel,
  });
  const owner = makeOwner();
  cleanups.push(() => owner.close());
  const actor = await owner.create({
    name: "independent-review", instructions: "Review without leaking your sub-results.",
    residency, delivery: "mailbox", responseMode, transport: "process", model: "fixture/review",
    ...(validWhile ? { validWhile } : {}),
  });
  const actorRunId = "a".repeat(32);
  let endActivation!: () => void;
  const activation = new Promise<void>((resolve) => { endActivation = resolve; });
  vi.spyOn(ownerAgents, "run").mockImplementationOnce(async (request, _signal, onStart) => {
    onStart?.({ id: actorRunId, name: actor.name, status: "running", runner: "pi", transport: "process", cwd: root });
    await activation;
    return {
      id: actorRunId, name: actor.name, task: request.task, status: "completed", runner: "pi",
      transport: "process", cwd: root, startedAt: Date.now(), updatedAt: Date.now(), finishedAt: Date.now(),
      turns: 1, toolCalls: 0, text: "", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
  });
  owner.tell(actor.id, "review activation");
  await vi.waitFor(() => expect(owner.status(actor.id).inFlightRun?.id).toBe(actorRunId));
  cleanups.push(async () => { endActivation(); });
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", rootId);
  vi.stubEnv("PI_FABRIC_SESSION_ID", "root-main");
  vi.stubEnv("PI_FABRIC_ACTOR_ID", actor.id);
  vi.stubEnv("PI_FABRIC_ACTOR_NAME", actor.name);
  vi.stubEnv("PI_FABRIC_PARENT_RUN", actorRunId);
  vi.stubEnv("PI_FABRIC_GRANTED_RISKS", "agent");
  vi.stubEnv("PI_FABRIC_ACTOR_SESSION_FILE", actor.sessionFile);
  vi.stubEnv("PI_FABRIC_RUN_ROOT", path.join(root, "child-runs"));
  const handlers = new Map<string, Array<(event: any, ctx: ExtensionContext) => unknown>>();
  const sendMessage = vi.fn();
  const pi = {
    on: (name: string, handler: (event: any, ctx: ExtensionContext) => unknown) => {
      const entries = handlers.get(name) ?? [];
      entries.push(handler);
      handlers.set(name, entries);
      return () => handlers.set(name, (handlers.get(name) ?? []).filter((entry) => entry !== handler));
    }, events: { emit() {} }, sendMessage, appendEntry: vi.fn(), getThinkingLevel: () => "off",
  } as unknown as ExtensionAPI;
  const context = {
    cwd: root, mode: "rpc", hasUI: false, isIdle: () => false,
    isProjectTrusted: () => true, hasPendingMessages: () => false,
    modelRegistry: {
      getAvailable: () => [{ provider: "fixture", id: "review" }],
      find: () => ({ provider: "fixture", id: "review" }),
      getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "synthetic-fixture" }),
    }, sessionManager: {
      getSessionId: () => "actor-worker-session", getSessionFile: () => actor.sessionFile,
      getBranch: () => [], getEntries: () => [], getLeafId: () => undefined,
    }, ui: { notify() {}, setStatus() {} },
  } as unknown as ExtensionContext;
  const invocation = {
    cwd: root, signal: undefined, parentToolCallId: "actor-task", nestedToolCallId: "child-task",
    extensionContext: context, update() {}, approve: async () => {}, audits: [], maxResultChars: 32768,
  };
  const runtime = new FabricRuntimeState(pi, new CapturedToolCatalog(), {
    paths: { worker: fixture, extension: fixture, residentHost: fixture, skills: root },
  });
  const config = normalizeFabricConfig({
    fullCodeMode: true, mesh: { enabled: true, actorPollMs: 20 },
    mcp: { enabled: false, cache: { enabled: false } }, memory: { enabled: false },
    agents: { notifyOnComplete }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  });
  const initialize = () => runtime.initialize(context, config);
  await initialize();
  cleanups.push(() => runtime.shutdown());
  const boundary = () => {
    for (const handler of handlers.get("turn_end") ?? []) handler({ message: { role: "assistant", stopReason: "stop" } }, context);
  };
  const spawn = (task = "private review sub-result") => runtime.registry.invoke("agents.spawn", {
    task, name: "review-subtask", transport: "process", model: "fixture/review",
  }, invocation) as Promise<AgentHandleInfo>;
  return { actor, actorRunId, owner, ownerAgents, mesh, runtime, invocation, rootDeliveries, sendMessage, boundary, spawn, endActivation, makeOwner, resolveModel, initialize };
};

describe.each(["session", "durable"] as const)("%s actor process children", (residency) => {
  it("records the actor and activation run as spawner, never the root Main", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id).status).toBe("completed"), { timeout: 5000 });
    expect(h.runtime.agents.status(child.id)).toMatchObject({
      spawner: { id: h.actor.id, kind: "actor", runId: h.actorRunId },
    });
    await vi.waitFor(() => { h.boundary(); expect(h.sendMessage).toHaveBeenCalledOnce(); }, { timeout: 5000 });
    expect(h.sendMessage.mock.calls[0]![0].content).toContain("fake worker complete");
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["shutdown", "reload"] as const)("%s preserves unread full archives through owner restart, without replaying consumed outcomes", async (reason) => {
    const h = await setup(residency);
    const unread = await h.spawn("LARGE_RESULT");
    await h.runtime.agents.join(unread.id);
    const consumed = await h.spawn("LARGE_RESULT");
    await h.runtime.registry.invoke("agents.wait", { id: consumed.id }, h.invocation);
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(store.pending().map(({ result }) => result.id)).toEqual([unread.id]);
    expect(store.received(unread.id)).toBe(false);
    expect(store.received(consumed.id)).toBe(true);
    // Close must not turn execution exit into a durable consumption receipt.
    await h.runtime.shutdown(reason);
    if (reason === "reload") { await h.initialize(); await h.runtime.shutdown(); }
    expect(store.received(unread.id)).toBe(false);
    expect(store.pending().map(({ result }) => result.id)).toEqual([unread.id]);
    const fullResult = () => JSON.parse(fs.readFileSync(store.resultFile(unread.id), "utf8")) as AgentRunResult;
    expect(fullResult()).toMatchObject({ text: "x".repeat(100000), value: { output: "x".repeat(100000) } });
    expect(fs.existsSync(store.resultFile(consumed.id))).toBe(false);
    // End the activation only after its owner stops polling, so the unread
    // outcome must survive an actual owner restart before delivery is possible.
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const inference = vi.fn();
    vi.mocked(h.ownerAgents.run).mockRestore();
    const run = h.ownerAgents.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      if (args[0].task.includes(JSON.stringify(store.resultFile(unread.id)))) inference(fullResult());
      return run(...args);
    });
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "next activation after owner restart");
    await vi.waitFor(() => expect(inference).toHaveBeenCalledOnce(), { timeout: 5000 });
    expect(inference.mock.calls[0]![0].value).toEqual({ output: "x".repeat(100000) });
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter((m) => m.id === unread.id && m.direction === "in")).toHaveLength(1);
    expect(restarted.messages(h.actor.id).filter((m) => m.id === consumed.id)).toEqual([]);
    expect(fs.existsSync(store.resultFile(unread.id))).toBe(false);
    await restarted.close();
    const again = h.makeOwner();
    cleanups.push(() => again.close());
    await again.ask(h.actor.id, "unrelated activation after consumption");
    await vi.waitFor(() => expect(again.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(again.messages(h.actor.id).filter((m) => m.id === unread.id && m.direction === "in")).toHaveLength(1);
    expect(again.messages(h.actor.id).filter((m) => m.id === consumed.id)).toEqual([]);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["shutdown", "reload"] as const)("%s leaves muted full text/value archived across owner restart and unrelated work", async (reason) => {
    const h = await setup(residency, false);
    const child = await h.spawn("LARGE_RESULT");
    await h.runtime.agents.join(child.id);
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await h.runtime.shutdown(reason);
    if (reason === "reload") { await h.initialize(); await h.runtime.shutdown(); }
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    expect(store.received(child.id)).toBe(false);
    expect(store.pending()).toEqual([]);
    await restarted.ask(h.actor.id, "unrelated activation must not consume muted work");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8"))).toMatchObject({
      status: "completed", text: "x".repeat(100000), value: { output: "x".repeat(100000) },
    });
    expect(store.received(child.id)).toBe(false);
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("hands an unread completion to the actor mailbox when its worker run ends", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id).status).toBe("completed"), { timeout: 5000 });
    await vi.waitFor(() => expect(fs.existsSync(path.join(path.dirname(h.actor.sessionFile!), "child-completions", `${child.id}.json`))).toBe(true));
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.messages(h.actor.id).some((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toBe(true), { timeout: 5000 });
    expect(h.owner.messages(h.actor.id).filter((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toHaveLength(1);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("keeps the complete text and structured value accessible from the next-run mailbox", async () => {
    const h = await setup(residency);
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    let consumeHandoff!: () => void;
    const handoff = new Promise<void>((resolve) => { consumeHandoff = resolve; });
    cleanups.push(async () => { consumeHandoff(); });
    const run = h.ownerAgents.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementationOnce(async (...args) => { await handoff; return run(...args); });
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(store.received(child.id)).toBe(true), { timeout: 5000 });
    const incoming = h.owner.messages(h.actor.id).find((message) => message.id === child.id && message.direction === "in");
    expect(incoming!.data).toMatchObject({ data: { resultFile: store.resultFile(child.id) } });
    const saved = JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8")) as AgentRunResult;
    expect(saved.text).toHaveLength(100000);
    expect(saved.value).toEqual({ output: "x".repeat(100000) });
    expect(saved.spawner).toEqual({ id: h.actor.id, kind: "actor", runId: h.actorRunId });
    expect(store.pending()).toEqual([]);
    consumeHandoff();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("does not repeat a live-delivered result in the next activation or after replay", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    await vi.waitFor(() => { h.boundary(); expect(h.sendMessage).toHaveBeenCalledOnce(); }, { timeout: 5000 });
    const result = h.runtime.agents.status(child.id) as AgentRunResult;
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"));
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    store.enqueue(result, { id: h.actor.id, kind: "actor", runId: h.actorRunId });
    expect(store.pending()).toEqual([]);
    expect(h.owner.messages(h.actor.id).filter((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("archives muted completed children with full text/value but no activation", async () => {
    const h = await setup(residency, false);
    const child = await h.spawn("LARGE_RESULT");
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id).status).toBe("completed"), { timeout: 5000 });
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(fs.existsSync(store.resultFile(child.id))).toBe(true));
    h.boundary();
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"));
    const saved = JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8"));
    expect(saved).toMatchObject({ status: "completed", spawner: { id: h.actor.id, runId: h.actorRunId } });
    expect(saved.text).toHaveLength(100000);
    expect(saved.value).toEqual({ output: "x".repeat(100000) });
    expect(store.pending()).toEqual([]);
    expect(h.owner.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("does not replay a live notice after acknowledge I/O failure and owner restart", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    const acknowledge = ActorChildCompletionStore.prototype.acknowledge;
    const failed = vi.spyOn(ActorChildCompletionStore.prototype, "acknowledge").mockImplementation(function(this: ActorChildCompletionStore, id: string) {
      if (id === child.id) throw new Error("live receipt I/O failure");
      acknowledge.call(this, id);
    });
    try {
      h.boundary();
      expect(h.sendMessage).toHaveBeenCalledOnce();
      expect(failed).toHaveBeenCalledWith(child.id);
      expect(store.received(child.id)).toBe(true);
    } finally { failed.mockRestore(); }
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"));
    await h.owner.close();
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    expect(store.pending()).toEqual([]);
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("reconciles committed native live completion ids before a mailbox activation", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    fs.appendFileSync(h.actor.sessionFile!, JSON.stringify({ type: "custom_message",
      customType: "pi-fabric-agent-complete", details: { ids: [child.id] },
    }) + "\n");
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"));
    expect(store.received(child.id)).toBe(true);
    expect(store.pending()).toEqual([]);
    expect(h.owner.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
  });

  it("wait retracts a pending result and prevents next-run mailbox duplication", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const result = await h.runtime.registry.invoke("agents.wait", { id: child.id }, h.invocation) as AgentRunResult;
    expect(result.text).toBe("fake worker complete");
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown();
    h.endActivation();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(store.pending()).toEqual([]);
    expect(fs.readdirSync(store.directory)).toEqual([`${child.id}.receipt`]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("terminal agents.status consumption deletes the full result and envelope", async () => {
    const h = await setup(residency);
    const child = await h.spawn("LARGE_RESULT");
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id).status).toBe("completed"), { timeout: 5000 });
    const result = await h.runtime.registry.invoke("agents.status", { id: child.id }, { ...h.invocation, maxResultChars: 300000 }) as AgentRunResult;
    expect(result).toMatchObject({ id: child.id, status: "completed" });
    h.boundary();
    await h.runtime.shutdown(); // Wait for any late settle event, not just terminal status.json.
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(fs.readdirSync(store.directory)).toEqual([`${child.id}.receipt`]);
    expect(h.sendMessage).not.toHaveBeenCalled();
  });

  it("owner polls do not read the actor session while its spawning activation is in flight", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(fs.existsSync(path.join(store.directory, `${child.id}.json`))).toBe(true), { timeout: 5000 });
    const pending = vi.spyOn(ActorChildCompletionStore.prototype, "pending");
    const read = vi.spyOn(fs, "readFileSync");
    try {
      for (let i = 0; i < 20; i++) {
        const before = pending.mock.calls.length;
        // Wake the filesystem watcher too: Unix reconciles on a slower idle timer.
        await h.mesh.publish({ topic: "test.in-flight-poll", from: { id: "fixture", name: "fixture", kind: "main" }, text: "poll" });
        await vi.waitFor(() => expect(pending.mock.calls.length).toBeGreaterThan(before), { timeout: 1000 });
      }
      expect(read.mock.calls.filter(([file]) => file === h.actor.sessionFile)).toHaveLength(0);
      expect(h.owner.status(h.actor.id).inFlightRun?.id).toBe(h.actorRunId);
    } finally { read.mockRestore(); pending.mockRestore(); }
  });

  it("a foreground cleanup failure does not replay after shutdown and owner restart", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const discard = vi.spyOn(ActorChildCompletionStore.prototype, "discard").mockImplementation(() => { throw new Error("cleanup I/O failure"); });
    try {
      const result = await h.runtime.registry.invoke("agents.wait", { id: child.id }, h.invocation) as AgentRunResult;
      expect(result).toMatchObject({ id: child.id, status: "completed", text: "fake worker complete" });
      h.boundary();
      expect(h.sendMessage).not.toHaveBeenCalled();
    } finally { discard.mockRestore(); }
    await h.runtime.shutdown();
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(store.received(child.id)).toBe(true);
    expect(store.pending()).toEqual([]);
    restarted.tell(h.actor.id, "next unrelated activation");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
  });

  it("mid-batch preparation failure preserves every outcome through shutdown and owner restart", async () => {
    const h = await setup(residency);
    const a = await h.spawn("LARGE_RESULT");
    const b = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(2), { timeout: 5000 });
    const prepare = ActorChildCompletionStore.prototype.prepareLive;
    let prepared = 0;
    const failed = vi.spyOn(ActorChildCompletionStore.prototype, "prepareLive").mockImplementation(function(this: ActorChildCompletionStore, id: string) {
      if (++prepared === 2) throw new Error("second preparation failed");
      prepare.call(this, id);
    });
    h.boundary();
    expect(prepared).toBe(2);
    expect(h.sendMessage).not.toHaveBeenCalled();
    for (const child of [a, b]) {
      expect(store.received(child.id)).toBe(false);
      expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8")).text).toHaveLength(100000);
    }
    await h.runtime.shutdown();
    failed.mockRestore();
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "next activation");
    await vi.waitFor(() => expect(restarted.messages(h.actor.id).filter((m) =>
      m.source === "child-completion" && m.direction === "out" && !m.error)).toHaveLength(2), { timeout: 5000 });
    for (const child of [a, b]) {
      expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id && m.direction === "in")).toHaveLength(1);
      // Outgoing messages precede asynchronous run-log retention and cleanup.
      await vi.waitFor(() => expect(fs.existsSync(store.resultFile(child.id))).toBe(false), { timeout: 5000 });
    }
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["agents.wait", "agents.status"])("%s retries a foreground receipt before returning and never replays after restart", async (action) => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    const consume = ActorChildCompletionStore.prototype.consume;
    let attempts = 0;
    const receipt = vi.spyOn(ActorChildCompletionStore.prototype, "consume").mockImplementation(function(this: ActorChildCompletionStore, id, options) {
      if (id === child.id && ++attempts === 1) throw new Error("transient foreground receipt failure");
      consume.call(this, id, options);
    });
    const execution = await h.runtime.execution.execute({
      code: `return await ${action}({id:${JSON.stringify(child.id)}});`,
      context: h.invocation.extensionContext, signal: undefined, parentToolCallId: "receipt-fence", onPartial() {},
    });
    expect(execution.success, execution.error).toBe(true);
    expect(execution.value).toMatchObject({ id: child.id, status: "completed" });
    expect(attempts).toBeGreaterThanOrEqual(2);
    expect(store.received(child.id)).toBe(true);
    receipt.mockRestore();
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown();
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "next unrelated activation");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(store.pending()).toEqual([]);
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("persistent foreground receipt failure cannot return a value to the actor program", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    const receipt = vi.spyOn(ActorChildCompletionStore.prototype, "consume").mockImplementation(() => { throw new Error("persistent receipt failure"); });
    const execution = await h.runtime.execution.execute({
      code: `try { await agents.wait({id:${JSON.stringify(child.id)}}); return "RETURNED-UNRECORDED"; } catch { return "NO-RESULT-RETURNED"; }`,
      context: h.invocation.extensionContext, signal: undefined, parentToolCallId: "receipt-fence-failure", onPartial() {},
    });
    expect(execution.success, execution.error).toBe(true);
    expect(execution.value).toBe("NO-RESULT-RETURNED");
    expect(store.received(child.id)).toBe(false);
    receipt.mockRestore();
    await h.runtime.registry.invoke("agents.wait", { id: child.id }, h.invocation);
    expect(store.received(child.id)).toBe(true);
  });

  it.each(["worker-start", "model-resolution", "error-only-turn", "validWhile-refusal", "directive-error-only-turn"])("%s before inference keeps the full handoff for exactly one consumption after restart", async (failure) => {
    const h = await setup(residency, true, failure === "validWhile-refusal" ? {
      version: 1, source: `({activation,current}) => activation.source !== "child-completion" || current.latestActivationSequence > activation.sequence`,
    } : undefined, failure === "directive-error-only-turn" ? "directive" : "text");
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    const failedRun = vi.spyOn(h.ownerAgents, "run");
    if (failure === "worker-start") failedRun.mockRejectedValueOnce(new Error("forced worker start failure"));
    if (failure === "model-resolution") h.resolveModel.mockRejectedValue(new Error("forced model resolution failure"));
    if (failure.endsWith("error-only-turn")) failedRun.mockImplementationOnce(async (request) => ({
      ...JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8")),
      id: "e".repeat(32), task: request.task, status: "failed", error: "model error before first inference",
      text: "", value: undefined, turns: 1, toolCalls: 0, inferenceStarted: false,
    }));
    h.endActivation();
    await vi.waitFor(() => {
      expect(store.received(child.id)).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
      expect(h.owner.status(h.actor.id).queued).toBe(0);
      expect(h.owner.status(h.actor.id).inFlightRun).toBeUndefined();
    }, { timeout: 5000 });
    expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8"))).toMatchObject({
      text: "x".repeat(100000), value: { output: "x".repeat(100000) },
    });
    await h.owner.close();
    h.resolveModel.mockImplementation(async (model) => model);
    failedRun.mockRestore();
    let consumed = 0;
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      if (args[0].task.includes(JSON.stringify(store.resultFile(child.id)))) {
        ++consumed;
        expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8")).value).toEqual({ output: "x".repeat(100000) });
      }
      return run(...args);
    });
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "following activation may consume the unread handoff");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(consumed).toBe(1);
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id && m.direction === "in")).toHaveLength(1);
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("preserves bound spawner metadata and the foreground receipt fence on a stopped queued child", async () => {
    const h = await setup(residency);
    const fence = vi.fn();
    const consumed = vi.fn();
    const agents = new AgentManager(h.invocation.cwd, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: fixture, runRoot: path.join(h.invocation.cwd, "queued-child-runs"),
      onBeforeResultReturned: fence, onResultConsumed: consumed,
    });
    cleanups.push(() => agents.close());
    const blocker = await agents.spawn({ task: "HANG", transport: "process" });
    const queued = await agents.spawn({ task: "queued actor child", transport: "process" });
    const spawner = { id: h.actor.id, kind: "actor", runId: h.actorRunId };
    expect(queued).toMatchObject({ status: "queued", queuePosition: 1, spawner });
    expect(agents.status(queued.id)).toMatchObject({ status: "queued", spawner });
    await agents.stop(queued.id);
    let commit!: () => void;
    expect(await agents.wait(queued.id, { deferConsumption: (consume) => { commit = consume; } })).toMatchObject({
      status: "stopped", spawner,
    });
    expect(fence).toHaveBeenCalledWith(queued.id);
    expect(consumed).not.toHaveBeenCalled();
    commit();
    expect(consumed).toHaveBeenCalledExactlyOnceWith(queued.id);
    await agents.stop(blocker.id);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("revokes a queued activation without consuming its deferred child handoff, then consumes it once after resume", async () => {
    const h = await setup(residency, true, undefined, "text", 1);
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    h.resolveModel.mockRejectedValue(new Error("defer before inference"));
    h.endActivation();
    await vi.waitFor(() => {
      expect(store.received(child.id)).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
      expect(h.owner.status(h.actor.id).lastError).toContain("defer before inference");
    }, { timeout: 5000 });
    h.resolveModel.mockImplementation(async (model) => model);
    const tasks: string[] = [];
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      tasks.push(args[0].task);
      return run(...args);
    });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const blocker = await h.ownerAgents.spawn({ task: "HANG", transport: "process" });
    h.owner.tell(h.actor.id, "activation revoked before inference");
    await vi.waitFor(() => expect(h.ownerAgents.list().some((r) => r.actorId === h.actor.id && r.status === "queued")).toBe(true));
    const queued = h.ownerAgents.list().find((r) => r.actorId === h.actor.id && r.status === "queued")!;
    expect(tasks).toHaveLength(1);
    expect(tasks[0]).toContain(JSON.stringify(store.resultFile(child.id)));
    expect(h.owner.haltAll().halted).toBe(1);
    expect(await h.ownerAgents.wait(queued.id)).toMatchObject({ status: "stopped" });
    await vi.waitFor(() => expect(h.owner.inFlightCount()).toBe(0));
    await h.ownerAgents.stop(blocker.id);
    expect(launch.mock.calls.map(([request]) => request.id)).toEqual([blocker.id]);
    expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8"))).toMatchObject({
      text: "x".repeat(100000), value: { output: "x".repeat(100000) },
    });
    const queueFile = path.join(path.dirname(h.actor.sessionFile!),
      fs.readdirSync(path.dirname(h.actor.sessionFile!)).find((file) => file.startsWith("queue-"))!);
    expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).items).toEqual([
      expect.objectContaining({ id: child.id, deferredHandoff: true }),
    ]);
    h.owner.dispatchHostEvent("input", { source: "user" });
    await h.owner.ask(h.actor.id, "new authorized activation consumes retained context");
    // ask resolves on the outgoing message, before the drain's receipt/cleanup finally.
    await vi.waitFor(() => expect(h.owner.inFlightCount()).toBe(0));
    expect(tasks).toHaveLength(2);
    expect(tasks[1]).toContain(JSON.stringify(store.resultFile(child.id)));
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    await h.owner.ask(h.actor.id, "following activation must not repeat context");
    await vi.waitFor(() => expect(h.owner.inFlightCount()).toBe(0));
    expect(tasks).toHaveLength(3);
    expect(tasks[2]).not.toContain(JSON.stringify(store.resultFile(child.id)));
    expect(launch.mock.calls.some(([request]) => request.id === queued.id)).toBe(false);
    expect(h.owner.messages(h.actor.id).filter((m) => m.id === child.id && m.direction === "in")).toHaveLength(1);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each([false, true])("a stale latest-sequence handoff never blocks newer mailbox work and is consumed once (restart=%s)", async (restart) => {
    const h = await setup(residency, true, {
      version: 1, source: "({activation,current}) => activation.sequence === current.latestActivationSequence",
    });
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    h.resolveModel.mockRejectedValue(new Error("transient pre-inference model failure"));
    h.endActivation();
    await vi.waitFor(() => {
      expect(store.received(child.id)).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
      expect(h.owner.status(h.actor.id).lastError).toContain("transient pre-inference");
    }, { timeout: 5000 });
    const queueFile = () => path.join(path.dirname(h.actor.sessionFile!),
      fs.readdirSync(path.dirname(h.actor.sessionFile!)).find((file) => file.startsWith("queue-"))!);
    const deferred = JSON.parse(fs.readFileSync(queueFile(), "utf8")).items;
    expect(deferred).toHaveLength(1);
    expect(deferred[0]).toMatchObject({ id: child.id, deferredHandoff: true });
    const originalActivation = deferred[0].activation;
    expect(h.owner.status(h.actor.id).queued).toBe(0);
    expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8")).value).toEqual({ output: "x".repeat(100000) });
    let owner = h.owner;
    if (restart) {
      await owner.close();
      owner = h.makeOwner();
      cleanups.push(() => owner.close());
    }
    h.resolveModel.mockImplementation(async (model) => model);
    const tasks: string[] = [];
    let release!: () => void;
    const nextRun = new Promise<void>((resolve) => { release = resolve; });
    cleanups.push(async () => { release(); });
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      tasks.push(args[0].task);
      if (tasks.length === 1) await nextRun;
      return run(...args);
    });
    owner.tell(h.actor.id, "newer mailbox event 1");
    await vi.waitFor(() => expect(tasks).toHaveLength(1), { timeout: 5000 });
    expect(tasks[0]).toContain("newer mailbox event 1");
    expect(tasks[0]).toContain("context only, not current activation facts");
    expect(tasks[0]).toContain(JSON.stringify(store.resultFile(child.id)));
    const context = JSON.parse(tasks[0]!.split("context only, not current activation facts):\n\n")[1]!);
    expect(context[0].activation).toEqual(originalActivation);
    expect(JSON.parse(fs.readFileSync(queueFile(), "utf8")).latestActivationSequence).toBeGreaterThan(originalActivation.sequence);
    expect(fs.existsSync(store.resultFile(child.id))).toBe(true); // No inference yet.
    owner.tell(h.actor.id, "newer mailbox event 2");
    release();
    await vi.waitFor(() => expect(owner.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(tasks).toHaveLength(2);
    expect(tasks[1]).toContain("newer mailbox event 2");
    expect(tasks.filter((task) => task.includes(JSON.stringify(store.resultFile(child.id))))).toHaveLength(1);
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    expect(owner.messages(h.actor.id).filter((m) => m.id === child.id && m.direction === "in")).toHaveLength(1);
    await owner.close();
    const after = h.makeOwner();
    cleanups.push(() => after.close());
    after.tell(h.actor.id, "post-consumption mailbox event");
    await vi.waitFor(() => expect(after.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(tasks).toHaveLength(3);
    expect(tasks[2]).not.toContain(JSON.stringify(store.resultFile(child.id)));
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("moves a freshness-refused handoff into the next valid activation's context", async () => {
    const h = await setup(residency, true, {
      version: 1, source: "({activation,current}) => activation.sequence === current.latestActivationSequence",
    });
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    cleanups.push(async () => { release(); });
    let blocked = false;
    const put = h.mesh.put.bind(h.mesh);
    vi.spyOn(h.mesh, "put").mockImplementation(async (...args) => {
      if (!blocked && (args[0].value as { status?: string }).status === "preparing" && store.received(child.id)) {
        blocked = true;
        await gate; // Supersede the handoff before its freshness check, not after inference.
      }
      return put(...args);
    });
    const tasks: string[] = [];
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      tasks.push(args[0].task);
      return run(...args);
    });
    h.endActivation();
    await vi.waitFor(() => expect(blocked).toBe(true), { timeout: 5000 });
    h.owner.tell(h.actor.id, "newest authorized mailbox work");
    release();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]).toContain("newest authorized mailbox work");
    expect(tasks[0]).toContain(JSON.stringify(store.resultFile(child.id)));
    expect(h.owner.messages(h.actor.id).some((m) => m.source === "child-completion" && m.stale)).toBe(true);
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("expires deferred handoffs with the archive TTL instead of retaining stale work forever", async () => {
    const h = await setup(residency, true, { version: 1, source: '({activation}) => activation.source !== "child-completion"' });
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => {
      expect(store.received(child.id)).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
    }, { timeout: 5000 });
    expect(fs.existsSync(store.resultFile(child.id))).toBe(true);
    await h.owner.close();
    const old = new Date(Date.now() - DEFAULT_FABRIC_CONFIG.retention.actorRunArchiveMs - 1000);
    for (const file of fs.readdirSync(store.directory)) fs.utimesSync(path.join(store.directory, file), old, old);
    const restarted = h.makeOwner(); // Maintenance yields; activation must still reject expired context.
    cleanups.push(() => restarted.close());
    const tasks: string[] = [];
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      tasks.push(args[0].task);
      return run(...args);
    });
    restarted.tell(h.actor.id, "new work after expiry"); // Do not await the startup sweep first.
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]).not.toContain(JSON.stringify(store.resultFile(child.id)));
    await vi.waitFor(() => {
      expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
      expect(fs.readdirSync(path.dirname(h.actor.sessionFile!)).filter((file) => file.startsWith("queue-"))).toEqual([]);
    }, { timeout: 5000 });
  });

  it("keeps a stopped actor's unread completion stored instead of falling back to Main", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    h.endActivation();
    await h.owner.stop(h.actor.id);
    await h.runtime.shutdown();
    expect(store.pending()).toEqual([expect.objectContaining({ result: expect.objectContaining({ id: child.id }) })]);
    expect(h.owner.messages(h.actor.id).filter((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("delivers a host-stopped running child to the actor's next activation", async () => {
    const h = await setup(residency);
    const child = await h.spawn("HANG_WITH_PROGRESS");
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id)).toMatchObject({ turns: 3 }), { timeout: 5000 });
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.messages(h.actor.id).some((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toBe(true), { timeout: 5000 });
    const incoming = h.owner.messages(h.actor.id).find((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id));
    expect(incoming!.data).toMatchObject({ data: { spawner: { id: h.actor.id, runId: h.actorRunId }, result: { id: child.id, status: "stopped" } } });
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("preserves but does not activate on shutdown results when completion notices are disabled", async () => {
    const h = await setup(residency, false);
    const child = await h.spawn("HANG_WITH_PROGRESS");
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id)).toMatchObject({ turns: 3 }), { timeout: 5000 });
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(store.pending()).toEqual([]);
    expect(h.owner.messages(h.actor.id).filter((message) => message.id === child.id)).toEqual([]);
    expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8")).status).toBe("stopped");
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("does not deliver a mailbox handoff twice after an owner restart", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.messages(h.actor.id).some((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toBe(true), { timeout: 5000 });
    await vi.waitFor(() => expect(h.owner.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    await h.owner.close();
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    expect(store.pending()).toEqual([]);
    expect(restarted.messages(h.actor.id).filter((message) =>
      message.direction === "in" && JSON.stringify(message.data).includes(child.id))).toHaveLength(1);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("recovers the same mailbox item after receipt I/O failure and an owner restart", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    const acknowledge = ActorChildCompletionStore.prototype.acknowledge;
    const failedReceipt = vi.spyOn(ActorChildCompletionStore.prototype, "acknowledge").mockImplementation(function(this: ActorChildCompletionStore, id: string) {
      if (id === child.id) throw new Error("simulated receipt I/O failure");
      return acknowledge.call(this, id);
    });
    try {
      h.endActivation();
      await vi.waitFor(() => expect(h.owner.messages(h.actor.id).some((message) =>
        message.id === child.id && message.direction === "in")).toBe(true), { timeout: 5000 });
      h.owner.tell(h.actor.id, "unrelated later work must not bypass the receipt gate");
      expect(store.received(child.id)).toBe(false);
      expect(h.owner.status(h.actor.id).inFlightRun).toBeUndefined();
      expect(h.owner.messages(h.actor.id).filter((message) =>
        message.source === "child-completion" && message.direction === "out")).toEqual([]);
      await h.owner.close();
    } finally { failedReceipt.mockRestore(); }
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    await vi.waitFor(() => expect(store.received(child.id)).toBe(true), { timeout: 5000 });
    await vi.waitFor(() => expect(restarted.messages(h.actor.id).filter((message) =>
      message.source === "child-completion" && message.direction === "out")).toHaveLength(1), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter((message) =>
      message.id === child.id && message.direction === "in")).toHaveLength(1);
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each([true, false])("synthetic mitigation: consumed foreground agents.run leaves no archive (notify=%s)", async (notify) => {
    const h = await setup(residency, notify);
    const execution = await h.runtime.execution.execute({
      code: `return await agents.run({task:"private review sub-result",transport:"process",model:"fixture/review"});`,
      context: h.invocation.extensionContext, signal: undefined, parentToolCallId: "review-mitigation", onPartial() {},
    });
    expect(execution.success, execution.error ?? JSON.stringify(execution.typeErrors)).toBe(true);
    const result = execution.value as AgentRunResult;
    expect(result).toMatchObject({ status: "completed", text: "fake worker complete" });
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(fs.readdirSync(store.directory)).toEqual([`${result.id}.receipt`]);
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });
});
