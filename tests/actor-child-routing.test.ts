import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import { ActorManager } from "../src/actors/manager.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { MeshStore } from "../src/mesh/store.js";
import type { AgentHandleInfo, AgentRunResult } from "../src/agents/types.js";

const roots: string[] = [];
const cleanups: Array<() => Promise<unknown>> = [];
const fixture = path.resolve("tests/fixtures/fake-worker.mjs");
afterEach(async () => {
  for (const close of cleanups.splice(0).reverse()) await close();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

// Real provider -> AgentManager -> process worker, with the environment an actor's
// activation worker supplies. Both session (Main owned) and durable (resident owned)
// actors must keep their child results within the actor, not its lineage Main.
const setup = async (residency: "session" | "durable", notifyOnComplete = true) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-actor-child-"));
  roots.push(root);
  const rootId = "session:root-main";
  const meshRoot = path.join(root, "mesh");
  const actorRoot = path.join(meshRoot, "actors");
  const actorScopeRoot = residency === "durable" ? actorRoot : path.join(actorRoot, "root-main");
  const mesh = new MeshStore(meshRoot, 64 * 1024, 100);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const ownerAgents = new AgentManager(root, DEFAULT_FABRIC_CONFIG.agents, {
    workerPath: fixture, runRoot: path.join(root, "owner-runs"), mainAgentId: rootId,
  });
  cleanups.push(() => ownerAgents.close());
  const rootDeliveries = vi.fn();
  const makeOwner = () => new ActorManager("root-main", {
    id: residency === "durable" ? "resident:root-main" : rootId,
    name: "owner", kind: "main", sessionId: "root-main",
  }, mesh, meshConfig, ownerAgents, rootDeliveries, {
    persistent: true, actorRoot: actorScopeRoot, claimResidency: residency, rootId,
  });
  const owner = makeOwner();
  cleanups.push(() => owner.close());
  const actor = await owner.create({
    name: "independent-review", instructions: "Review without leaking your sub-results.",
    residency, delivery: "mailbox", responseMode: "text", transport: "process",
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
  await runtime.initialize(context, normalizeFabricConfig({
    fullCodeMode: true, mesh: { enabled: true, actorPollMs: 20 },
    mcp: { enabled: false, cache: { enabled: false } }, memory: { enabled: false },
    agents: { notifyOnComplete }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  }));
  cleanups.push(() => runtime.shutdown());
  const boundary = () => {
    for (const handler of handlers.get("turn_end") ?? []) handler({ message: { role: "assistant", stopReason: "stop" } }, context);
  };
  const spawn = (task = "private review sub-result") => runtime.registry.invoke("agents.spawn", {
    task, name: "review-subtask", transport: "process", model: "fixture/review",
  }, invocation) as Promise<AgentHandleInfo>;
  return { actor, actorRunId, owner, runtime, invocation, rootDeliveries, sendMessage, boundary, spawn, endActivation, makeOwner };
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

  it("wait retracts a pending result and prevents next-run mailbox duplication", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const result = await h.runtime.registry.invoke("agents.wait", { id: child.id }, h.invocation) as AgentRunResult;
    expect(result.text).toBe("fake worker complete");
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown();
    h.endActivation();
    expect(new ActorChildCompletionStore(h.actor.sessionFile!).pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
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

  it("verifies the meanwhile mitigation: await agents.run in the same program", async () => {
    const h = await setup(residency);
    const execution = await h.runtime.execution.execute({
      code: `return await agents.run({task:"private review sub-result",transport:"process",model:"fixture/review"});`,
      context: h.invocation.extensionContext, signal: undefined, parentToolCallId: "review-mitigation", onPartial() {},
    });
    expect(execution.success, execution.error ?? JSON.stringify(execution.typeErrors)).toBe(true);
    const result = execution.value as AgentRunResult;
    expect(result).toMatchObject({ status: "completed", text: "fake worker complete" });
    h.boundary();
    expect(h.sendMessage).not.toHaveBeenCalled();
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });
});
