import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { ActorChildCompletionStore } from "../src/actors/child-completions.js";
import * as atomicWrites from "../src/core/atomic-write.js";
import { runTreeExitVeto } from "../src/storage/retention.js";
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
const setup = async (residency: "session" | "durable", notifyOnComplete = true, validWhile?: FabricActorValidWhileSource, responseMode: "text" | "directive" = "text", ownerMaxConcurrent = DEFAULT_FABRIC_CONFIG.agents.maxConcurrent, childMaxConcurrent = DEFAULT_FABRIC_CONFIG.agents.maxConcurrent) => {
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
  await runtime.initialize(context, normalizeFabricConfig({
    fullCodeMode: true, mesh: { enabled: true, actorPollMs: 20 },
    mcp: { enabled: false, cache: { enabled: false } }, memory: { enabled: false },
    agents: { notifyOnComplete, maxConcurrent: childMaxConcurrent }, residency: { enabled: false }, prewalk: { enabled: false, alwaysRearm: false },
  }));
  cleanups.push(() => runtime.shutdown());
  const boundary = () => {
    for (const handler of handlers.get("turn_end") ?? []) handler({ message: { role: "assistant", stopReason: "stop" } }, context);
  };
  const spawn = (task = "private review sub-result") => runtime.registry.invoke("agents.spawn", {
    task, name: "review-subtask", transport: "process", model: "fixture/review",
  }, invocation) as Promise<AgentHandleInfo>;
  return { actor, actorRunId, owner, ownerAgents, mesh, runtime, invocation, rootDeliveries, sendMessage, boundary, spawn, endActivation, makeOwner, resolveModel };
};

// Windows omits unsupported directory fsync. Exercise the same uncertain writer
// rejection there, while Unix probes keep their physical post-rename fsync fault.
const failPostRenameOnWindows = (target: (file: string) => boolean, fail: () => void) => {
  if (process.platform !== "win32") return;
  const write = atomicWrites.writeJsonAtomic;
  return vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, value, options) => {
    write(file, value, options);
    if (target(file)) fail();
  });
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

  it("binds task slices to the actor spawner even when launch identity names the lineage Main", async () => {
    const h = await setup(residency);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const manager = new AgentManager(h.invocation.cwd, DEFAULT_FABRIC_CONFIG.agents, {
      workerPath: fixture, runRoot: path.join(h.invocation.cwd, "bound-child-runs"),
      identityId: "session:root-main", mainAgentId: "session:root-main",
    });
    cleanups.push(() => manager.close());
    const result = await manager.run({ task: "actor-owned return", transport: "process", model: "fixture/review" });
    const args = launch.mock.calls[0]![0].workerArguments;
    const address = JSON.parse(args[args.indexOf("--task-return-address") + 1]!);
    expect(address.spawnerId).toBe(h.actor.id);
    expect(args[args.indexOf("--spawner-id") + 1]).toBe(h.actor.id);
    expect(args[args.indexOf("--main-agent-id") + 1]).toBe("session:root-main");
    expect(result.spawner).toMatchObject({ id: h.actor.id, kind: "actor", runId: h.actorRunId });
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("keeps agents.main bound to the owning Main during an actor turn", async () => {
    const h = await setup(residency);
    const main = await h.runtime.registry.invoke("agents.main", {}, h.invocation) as { id: string };
    expect(main.id).toBe("session:root-main");
    expect(main.id).not.toBe(h.actor.id);
    const child = await h.spawn();
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id).status).toBe("completed"));
    expect(h.runtime.agents.status(child.id).spawner).toMatchObject({ id: h.actor.id, kind: "actor" });
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("retains a failed child archive through ancestor cleanup and recovers it after both custodians close", async () => {
    const h = await setup(residency);
    const enqueue = ActorChildCompletionStore.prototype.enqueue;
    const refused = vi.spyOn(ActorChildCompletionStore.prototype, "enqueue").mockImplementation(function(this: ActorChildCompletionStore, result, ...args) {
      if (result.name === "review-subtask") throw new Error("archive storage unavailable");
      return enqueue.call(this, result, ...args);
    });
    const child = await h.spawn("HANG_WITH_PROGRESS");
    await vi.waitFor(() => expect(h.runtime.agents.status(child.id)).toMatchObject({ turns: 3 }), { timeout: 5000 });
    const source = h.runtime.agents.runDirectory(child.id)!;
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await h.runtime.shutdown();
    const closing = h.owner.close(); h.endActivation(); await closing;
    expect(fs.existsSync(path.join(source, "archive-pending.json"))).toBe(true);
    const ancestor = path.join(path.dirname(source), "ancestor");
    const moved = path.join(ancestor, "nested", child.id);
    fs.mkdirSync(path.dirname(moved), { recursive: true });
    fs.writeFileSync(path.join(ancestor, "status.json"), JSON.stringify({ status: "completed" }));
    fs.renameSync(source, moved);
    store.trackArchiveSource(child.id, moved);
    expect(runTreeExitVeto(ancestor)).toMatch(/archive is pending/);
    refused.mockRestore();
    const recovered = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(recovered.pending({ actorId: h.actor.id })).toMatchObject([{ result: { id: child.id, status: "stopped" } }]);
    expect(runTreeExitVeto(ancestor)).toBeUndefined();
    expect(JSON.parse(fs.readFileSync(recovered.resultFile(child.id), "utf8"))).toMatchObject({ id: child.id, turns: 3, spawner: { id: h.actor.id } });
    const restarted = h.makeOwner(); cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "resume after source recovery");
    await vi.waitFor(() => expect(restarted.messages(h.actor.id).filter(m => m.id === child.id && m.direction === "in")).toHaveLength(1), { timeout: 5000 });
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
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

  it.each(["active", "queued"] as const)("guest stop consumes the %s child's result without a mailbox replay after restart", async (mode) => {
    const h = await setup(residency, true, undefined, "text", undefined, mode === "queued" ? 1 : undefined);
    let blocker: AgentHandleInfo | undefined;
    if (mode === "queued") {
      blocker = await h.spawn("HANG_WITH_PROGRESS");
      await vi.waitFor(() => expect(h.runtime.agents.status(blocker!.id)).toMatchObject({ turns: 3 }), { timeout: 5000 });
    }
    const child = await h.spawn("HANG_WITH_PROGRESS");
    if (mode === "active") await vi.waitFor(() => expect(h.runtime.agents.status(child.id)).toMatchObject({ turns: 3 }), { timeout: 5000 });
    else expect(child.status).toBe("queued");
    const execution = await h.runtime.execution.execute({
      code: `return await agents.stop({id:${JSON.stringify(child.id)}});`,
      context: h.invocation.extensionContext, signal: undefined, parentToolCallId: "guest-stop", onPartial() {},
    });
    expect(execution.success, execution.error).toBe(true);
    const result = execution.value as AgentRunResult;
    expect(result).toMatchObject({ id: child.id, status: "stopped", spawner: { id: h.actor.id, runId: h.actorRunId } });
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(store.received(child.id)).toBe(true);
    if (blocker) await h.runtime.agents.stop(blocker.id);
    await h.runtime.shutdown();
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "next unrelated activation");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["live", "mailbox"] as const)("an overlapping old activation and replacement owner deliver once when %s wins the claim", async (winner) => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    const acknowledge = ActorChildCompletionStore.prototype.acknowledge;
    let overlap = false;
    const race = vi.spyOn(ActorChildCompletionStore.prototype, "acknowledge").mockImplementation(function(this: ActorChildCompletionStore, id, options) {
      if (id !== child.id || !options?.handoff) return acknowledge.call(this, id, options);
      overlap = true;
      if (winner === "live") h.boundary();
      acknowledge.call(this, id, options);
      if (winner === "mailbox") h.boundary();
    });
    h.endActivation(); // The worker is still live while its replacement owner reconciles.
    await vi.waitFor(() => {
      expect(overlap).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
      expect(h.owner.inFlightCount()).toBe(0);
    }, { timeout: 5000 });
    race.mockRestore();
    const mailbox = h.owner.messages(h.actor.id).filter((m) => m.source === "child-completion" && m.direction === "out" && !m.error);
    expect(h.sendMessage.mock.calls.length + mailbox.length).toBe(1);
    expect(h.sendMessage).toHaveBeenCalledTimes(winner === "live" ? 1 : 0);
    expect(mailbox).toHaveLength(winner === "mailbox" ? 1 : 0);
    await h.runtime.shutdown();
    await h.owner.close();
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "next unrelated activation");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["agents.wait", "agents.status", "agents.stop"])("%s restores unread ownership when publication is cancelled after the foreground fence", async (action) => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    const abort = new AbortController();
    const atomicWrite = atomicWrites.writeJsonAtomic;
    let rollbackAttempts = 0;
    const rollbackFailure = vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file === path.join(store.directory, `${child.id}.receipt`) && (value as { unread?: boolean }).unread && ++rollbackAttempts === 1) {
        throw new Error("transient abandonment write failure");
      }
      return atomicWrite(file, value, options);
    });
    const consume = ActorChildCompletionStore.prototype.consume;
    const fence = vi.spyOn(ActorChildCompletionStore.prototype, "consume").mockImplementation(function(this: ActorChildCompletionStore, id, options) {
      consume.call(this, id, options);
      if (id === child.id) abort.abort(new Error("discard host result after fence"));
    });
    await expect(h.runtime.registry.invoke(action, { id: child.id }, { ...h.invocation, signal: abort.signal })).rejects.toThrow("discard host result after fence");
    fence.mockRestore();
    expect(rollbackAttempts).toBeGreaterThanOrEqual(2);
    rollbackFailure.mockRestore();
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown(); // Close before any replacement live notice is delivered.
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "next unrelated activation");
    await vi.waitFor(() => expect(restarted.messages(h.actor.id).filter((m) =>
      m.id === child.id && m.direction === "in")).toHaveLength(1), { timeout: 5000 });
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter((m) => m.source === "child-completion" && m.direction === "out" && !m.error)).toHaveLength(1);
    await vi.waitFor(() => expect(fs.existsSync(store.resultFile(child.id))).toBe(false), { timeout: 5000 });
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["agents.wait", "agents.status", "agents.stop"])("%s preserves archive custody through failed archival, cancelled publication and failed abandonment until restart", async (action) => {
    const h = await setup(residency);
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    const atomicWrite = atomicWrites.writeJsonAtomic;
    let archiveAttempts = 0;
    let rollbackAttempts = 0;
    const failedStorage = vi.spyOn(atomicWrites, "writeJsonAtomic").mockImplementation((file, value, options) => {
      if (file.startsWith(store.directory + path.sep) && file.endsWith(".result.json")) {
        archiveAttempts++;
        throw new Error("full-result archive unavailable");
      }
      if (file.startsWith(store.directory + path.sep) && file.endsWith(".receipt") && (value as { unread?: boolean }).unread) {
        rollbackAttempts++;
        throw new Error("unread receipt rewrite unavailable through close");
      }
      return atomicWrite(file, value, options);
    });
    const child = await h.spawn("LARGE_RESULT");
    await vi.waitFor(() => {
      expect(h.runtime.agents.status(child.id).status).toBe("completed");
      expect(archiveAttempts).toBeGreaterThan(0);
    }, { timeout: 5000 });
    const source = h.runtime.agents.runDirectory(child.id)!;
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    expect(fs.existsSync(path.join(store.directory, `${child.id}.json`))).toBe(false);
    expect(runTreeExitVeto(source)).toMatch(/archive is pending/);
    const abort = new AbortController();
    const consume = ActorChildCompletionStore.prototype.consume;
    const fence = vi.spyOn(ActorChildCompletionStore.prototype, "consume").mockImplementation(function(this: ActorChildCompletionStore, id, options) {
      consume.call(this, id, options);
      if (id === child.id && options?.publication) abort.abort(new Error("cancel unpublished archive observation"));
    });
    await expect(h.runtime.registry.invoke(action, { id: child.id }, { ...h.invocation, signal: abort.signal }))
      .rejects.toThrow("cancel unpublished archive observation");
    fence.mockRestore();
    expect(rollbackAttempts).toBeGreaterThanOrEqual(3);
    expect(JSON.parse(fs.readFileSync(path.join(store.directory, `${child.id}.receipt`), "utf8")).publication).toBeTruthy();
    expect(fs.existsSync(path.join(store.directory, `${child.id}.abandon`))).toBe(true);
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown();
    const closing = h.owner.close(); h.endActivation(); await closing;
    expect(rollbackAttempts).toBeGreaterThanOrEqual(6);
    expect(fs.existsSync(source)).toBe(true);
    expect(fs.existsSync(path.join(source, "archive-pending.json"))).toBe(true);
    expect(runTreeExitVeto(source)).toMatch(/archive is pending/);
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    failedStorage.mockRestore();
    const recovered = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(recovered.pending({ actorId: h.actor.id })).toMatchObject([{ result: { id: child.id, status: "completed" } }]);
    const saved = JSON.parse(fs.readFileSync(recovered.resultFile(child.id), "utf8"));
    expect(saved.text).toHaveLength(100000);
    expect(saved.value).toEqual({ output: "x".repeat(100000) });
    expect(saved.spawner).toEqual({ id: h.actor.id, kind: "actor", runId: h.actorRunId });
    expect(fs.existsSync(path.join(source, "archive-pending.json"))).toBe(false);
    expect(runTreeExitVeto(source)).toBeUndefined();
    expect(fs.existsSync(path.join(store.directory, `${child.id}.abandon`))).toBe(false);
    const restarted = h.makeOwner(); cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "recover combined storage failure");
    await vi.waitFor(() => expect(restarted.messages(h.actor.id).filter(m => m.id === child.id && m.direction === "in")).toHaveLength(1), { timeout: 5000 });
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter(m => m.source === "child-completion" && m.direction === "out" && !m.error)).toHaveLength(1);
    restarted.tell(h.actor.id, "unrelated activation must not replay");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter(m => m.id === child.id && m.direction === "in")).toHaveLength(1);
    expect(recovered.pending()).toEqual([]);
    expect(fs.existsSync(recovered.resultFile(child.id))).toBe(false);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("does not replay consumed deferred context after a failed queue commit and owner restart", async () => {
    const h = await setup(residency);
    const child = await h.spawn("LARGE_RESULT");
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    await h.runtime.shutdown();
    h.resolveModel.mockRejectedValue(new Error("defer before inference"));
    h.endActivation();
    await vi.waitFor(() => {
      expect(store.received(child.id)).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
    }, { timeout: 5000 });
    const directory = path.dirname(h.actor.sessionFile!);
    const queueFile = path.join(directory, fs.readdirSync(directory).find((file) => file.startsWith("queue-"))!);
    const staleQueue = fs.readFileSync(queueFile, "utf8");
    expect(JSON.parse(staleQueue).items).toEqual([expect.objectContaining({ id: child.id, deferredHandoff: true })]);
    h.resolveModel.mockImplementation(async (model) => model);
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    let inferred = false;
    const tasks: string[] = [];
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      tasks.push(args[0].task);
      const result = await run(...args);
      inferred = true;
      return result;
    });
    const rename = fs.renameSync;
    const remove = fs.rmSync;
    const failedWrite = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      if (inferred && to === queueFile) throw new Error("post-inference queue commit failed");
      rename(from, to);
    });
    const failedRemoval = vi.spyOn(fs, "rmSync").mockImplementation((file, options) => {
      if (inferred && file === queueFile) throw new Error("post-inference queue removal failed");
      remove(file, options);
    });
    await h.owner.ask(h.actor.id, "consume deferred context once");
    await vi.waitFor(() => expect(h.owner.inFlightCount()).toBe(0), { timeout: 5000 });
    expect(tasks[0]).toContain(JSON.stringify(store.resultFile(child.id)));
    expect(JSON.parse(fs.readFileSync(queueFile, "utf8")).items.some((item: { id: string }) => item.id === child.id)).toBe(true);
    await h.owner.close(); // No later successful queue checkpoint before restart.
    failedWrite.mockRestore();
    failedRemoval.mockRestore();
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "unrelated following activation");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(tasks.slice(1).every((task) => !task.includes(JSON.stringify(store.resultFile(child.id))))).toBe(true);
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it.each(["count", "bytes", "oversized"] as const)("bounds deferred handoff snapshots by %s, consuming only each snapshot", async (budget) => {
    const h = await setup(residency, true, { version: 1, source: '({activation}) => activation.source !== "child-completion"' });
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    const ids = Array.from({ length: budget === "oversized" ? 2 : 40 }, (_, i) => (i + 1).toString(16).padStart(32, "0"));
    const now = Date.now();
    for (const id of ids) store.enqueue({
      id, name: budget === "oversized" ? "界".repeat(12000) : "large deferred child", task: "child", status: "completed",
      text: budget === "bytes" ? "界".repeat(4000) : "short result",
      startedAt: now, updatedAt: now, finishedAt: now, value: { output: id },
    } as AgentRunResult, { id: h.actor.id, kind: "actor", runId: h.actorRunId });
    await h.runtime.shutdown();
    h.endActivation();
    await vi.waitFor(() => {
      expect(ids.every((id) => store.received(id))).toBe(true);
      expect(h.owner.status(h.actor.id).status).toBe("idle");
      expect(h.owner.status(h.actor.id).queued).toBe(0);
    }, { timeout: 10000 });
    h.resolveModel.mockImplementation(async (model) => model);
    const tasks: string[] = [];
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => { tasks.push(args[0].task); return run(...args); });
    const seen = new Set<string>();
    for (let activation = 0; seen.size < ids.length && activation < ids.length; activation++) {
      await h.owner.ask(h.actor.id, `fresh activation ${activation}`);
      await vi.waitFor(() => expect(h.owner.inFlightCount()).toBe(0), { timeout: 5000 });
      const json = tasks.at(-1)!.split("context only, not current activation facts):\n\n")[1]!;
      const context = JSON.parse(json) as Array<{ id: string }>;
      expect(context.length).toBeGreaterThan(0);
      expect(context.length).toBeLessThanOrEqual(16);
      expect(Buffer.byteLength(json, "utf8")).toBeLessThanOrEqual(32768);
      for (const item of context) { expect(seen.has(item.id)).toBe(false); seen.add(item.id); }
      for (const id of ids) expect(fs.existsSync(store.resultFile(id))).toBe(!seen.has(id));
    }
    expect(seen.size).toBe(ids.length);
    await h.owner.ask(h.actor.id, "no context remains");
    expect(tasks.at(-1)).not.toContain("context only, not current activation facts");
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  }, 30000);

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

  it("Q4 recovers every unsent live notice exactly once after a post-rename barrier failure and worker close", async () => {
    const h = await setup(residency);
    const children = [await h.spawn("LARGE_RESULT"), await h.spawn("LARGE_RESULT")];
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(2), { timeout: 5000 });
    const directory = fs.statSync(store.directory);
    const rename = fs.renameSync;
    const sync = fs.fsyncSync;
    let markerRenamed = false;
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to).endsWith(".live-receipt")) markerRenamed = true;
    });
    const failed = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const stat = fs.fstatSync(fd);
      if (markerRenamed && stat.dev === directory.dev && stat.ino === directory.ino) throw new Error("live barrier after rename");
      sync(fd);
    });
    const windowsFailure = failPostRenameOnWindows((file) => file.endsWith(".live-receipt"), () => { throw new Error("live barrier after rename"); });
    h.boundary();
    expect(markerRenamed).toBe(true);
    expect(h.sendMessage).not.toHaveBeenCalled();
    await h.runtime.shutdown();
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    failed.mockRestore();
    renamed.mockRestore();
    windowsFailure?.mockRestore();
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "recover unsent notices");
    await vi.waitFor(() => expect(restarted.messages(h.actor.id).filter((m) => m.source === "child-completion" && m.direction === "out" && !m.error)).toHaveLength(2), { timeout: 5000 });
    for (const child of children) {
      expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id && m.direction === "in")).toHaveLength(1);
      await vi.waitFor(() => expect(fs.existsSync(store.resultFile(child.id))).toBe(false));
    }
    expect(store.pending()).toEqual([]);
    expect(h.rootDeliveries).not.toHaveBeenCalled();
  });

  it("Q6 returns no foreground value until post-rename barriers complete and never replays after recovery", async () => {
    const h = await setup(residency);
    const child = await h.spawn();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    await vi.waitFor(() => expect(store.pending()).toHaveLength(1), { timeout: 5000 });
    const directory = fs.statSync(store.directory);
    const rename = fs.renameSync;
    const sync = fs.fsyncSync;
    let receiptRenamed = false;
    let blocked = true;
    let failures = 0;
    const renamed = vi.spyOn(fs, "renameSync").mockImplementation((from, to) => {
      rename(from, to);
      if (String(to) === path.join(store.directory, `${child.id}.receipt`)) receiptRenamed = true;
    });
    const failed = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
      const stat = fs.fstatSync(fd);
      if (receiptRenamed && blocked && stat.dev === directory.dev && stat.ino === directory.ino) {
        failures++;
        throw new Error("foreground barrier after rename");
      }
      sync(fd);
    });
    failPostRenameOnWindows((file) => file === path.join(store.directory, `${child.id}.receipt`), () => {
      if (blocked) { failures++; throw new Error("foreground barrier after rename"); }
    });
    const execution = await h.runtime.execution.execute({
      code: `try { return await agents.wait({id:${JSON.stringify(child.id)}}); } catch { return "NO-FOREGROUND-VALUE"; }`,
      context: h.invocation.extensionContext, signal: undefined, parentToolCallId: "barrier-fence", onPartial() {},
    });
    expect(execution.success, execution.error).toBe(true);
    expect(execution.value).toBe("NO-FOREGROUND-VALUE");
    expect(failures).toBeGreaterThanOrEqual(2);
    blocked = false;
    expect(await h.runtime.registry.invoke("agents.wait", { id: child.id }, h.invocation)).toMatchObject({ id: child.id, status: "completed" });
    failed.mockRestore();
    renamed.mockRestore();
    await h.runtime.shutdown();
    const closing = h.owner.close();
    h.endActivation();
    await closing;
    const restarted = h.makeOwner();
    cleanups.push(() => restarted.close());
    restarted.tell(h.actor.id, "after barrier recovery");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(restarted.messages(h.actor.id).filter((m) => m.id === child.id)).toEqual([]);
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
    const restarted = h.makeOwner(); // Startup runs the normal archive retention sweep.
    cleanups.push(() => restarted.close());
    expect(fs.existsSync(store.resultFile(child.id))).toBe(false);
    expect(fs.readdirSync(path.dirname(h.actor.sessionFile!)).filter((file) => file.startsWith("queue-"))).toEqual([]);
    const tasks: string[] = [];
    const run = AgentManager.prototype.run.bind(h.ownerAgents);
    vi.spyOn(h.ownerAgents, "run").mockImplementation(async (...args) => {
      tasks.push(args[0].task);
      return run(...args);
    });
    restarted.tell(h.actor.id, "new work after expiry");
    await vi.waitFor(() => expect(restarted.status(h.actor.id).status).toBe("idle"), { timeout: 5000 });
    expect(tasks).toHaveLength(1);
    expect(tasks[0]).not.toContain(JSON.stringify(store.resultFile(child.id)));
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

  it("retries shutdown archival per outcome without skipping later children", async () => {
    const h = await setup(residency);
    const a = await h.spawn("HANG_WITH_PROGRESS");
    const b = await h.spawn("HANG_WITH_PROGRESS");
    await vi.waitFor(() => expect(h.runtime.agents.status(b.id)).toMatchObject({ turns: 3 }), { timeout: 5000 });
    const enqueue = ActorChildCompletionStore.prototype.enqueue;
    const attempts = new Map<string, number>();
    const fail = vi.spyOn(ActorChildCompletionStore.prototype, "enqueue").mockImplementation(function(this: ActorChildCompletionStore, result, spawner, notify) {
      attempts.set(result.id, (attempts.get(result.id) ?? 0) + 1);
      // Fail managed settlement and the first shutdown attempt for only A.
      if (result.id === a.id && attempts.get(a.id)! <= 2) throw new Error("first shutdown archive unavailable");
      enqueue.call(this, result, spawner, notify);
    });
    await h.runtime.shutdown();
    fail.mockRestore();
    const store = new ActorChildCompletionStore(h.actor.sessionFile!);
    expect(attempts.get(a.id)).toBeGreaterThanOrEqual(3);
    expect(attempts.get(b.id)).toBeGreaterThanOrEqual(2); // Settlement plus shutdown, despite A's enqueue fault.
    expect(store.pending().map(({ result }) => result.id).sort()).toEqual([a.id, b.id].sort());
    for (const child of [a, b]) expect(JSON.parse(fs.readFileSync(store.resultFile(child.id), "utf8"))).toMatchObject({ id: child.id, status: "stopped", turns: 3, toolCalls: 1, usage: { input: 30, output: 10 } });
    h.endActivation();
    await vi.waitFor(() => expect(h.owner.messages(h.actor.id).filter((m) => m.source === "child-completion" && m.direction === "out" && !m.error)).toHaveLength(2), { timeout: 5000 });
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
