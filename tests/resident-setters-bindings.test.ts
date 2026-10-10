import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import type { AgentRunRequest, AgentRunResult } from "../src/agents/types.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { controlActorBindingOptions, FabricControlPlane } from "../src/topology/control-plane.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";
import type { FabricActorInfo, FabricActorRunBinding } from "../src/actors/types.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricInvocationContext } from "../src/protocol.js";

const roots: string[] = [];
const managers: ActorManager[] = [];
const agentsList: AgentManager[] = [];
const releases: Array<() => void> = [];
const controls: FabricControlPlane[] = [];
const identity = { id: "session:root", name: "Main", kind: "main" as const, sessionId: "root" };
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for binding probe");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
};
const terminal = (request: AgentRunRequest, id: number): AgentRunResult => ({
  id: `probe-${id}`, name: request.name ?? "probe", task: request.task, status: "completed",
  runner: "pi", transport: "process", cwd: process.cwd(), startedAt: Date.now(), updatedAt: Date.now(),
  turns: 1, toolCalls: 0, text: "done", usage: {
    input: 0, output: 0, cacheRead: 0, cacheWrite: 0,
    cost: 0,
  },
});
const setup = (queueLimit = 2, realRuns = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-binding-drain-")); roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, model: "provider/config", thinking: "low" }, {
    runRoot: path.join(root, "runs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
  });
  agentsList.push(agents);
  const launches: AgentRunRequest[] = [];
  const results: AgentRunResult[] = [];
  const actualRun = agents.run.bind(agents);
  let release = () => {};
  const held = new Promise<void>((resolve) => { release = resolve; });
  releases.push(release);
  let nextHold: Promise<void> | undefined;
  const holdNext = () => {
    let resume = () => {};
    nextHold = new Promise<void>((resolve) => { resume = resolve; });
    releases.push(resume); return resume;
  };
  vi.spyOn(agents, "run").mockImplementation(async (request, signal, onSpawned, ...callbacks) => {
    const pause = nextHold; nextHold = undefined;
    launches.push(structuredClone(request));
    // Non-process fixtures explicitly announce their simulated launched worker.
    if (!realRuns) onSpawned?.({ ...terminal(request, launches.length), status: "running" });
    if (launches.length === 1) await held;
    if (pause) await pause;
    const result = realRuns ? await actualRun(request, signal, onSpawned, ...callbacks) : terminal(request, launches.length);
    results.push(result); return result;
  });
  const actorRoot = path.join(root, "actors");
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20, actorQueueLimit: queueLimit };
  const open = (options: ConstructorParameters<typeof ActorManager>[6] = {}) => {
    const manager = new ActorManager("root", identity, mesh, meshConfig, agents, () => {}, { actorRoot, persistent: true, ...options });
    managers.push(manager); return manager;
  };
  return { root, mesh, agents, launches, results, release, open, actorRoot, holdNext };
};
// Exercise the public foreign Main provider and the real owner control dispatch, not
// just its outgoing command. A passive Main shares the registry but not session pins.
const routedProvider = (state: ReturnType<typeof setup>, actors: ActorManager, actor: FabricActorInfo, ownRoot: boolean) => {
  const caller = ownRoot ? identity : { id: "session:foreign", name: "Foreign Main", kind: "main" as const, sessionId: "foreign" };
  const callerActors = new ActorManager(caller.sessionId, caller, state.mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, state.agents, () => {}, {
    // A passive Main must not share the durable owner's mailbox writer lineage.
    actorRoot: state.actorRoot, persistent: true, claimResidency: "session", canManageActor: () => false,
  });
  managers.push(callerActors);
  const mainFor = (who: typeof identity): FabricMainAgentTarget => ({
    id: who.id, local: true, matches: (id) => id === who.id,
    info: () => ({ id: who.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", cwd: process.cwd(), sessionId: who.sessionId, startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
    deliverAgent: () => ({ queued: true, messageId: "delivery", routed: "main" }),
  });
  const source = (local: boolean): FabricParticipantSource => {
    const member: FabricParticipantInfo = {
      format: 1, id: actor.id, name: actor.name, kind: "actor", rootId: identity.id, ownerHostId: "host:owner", ownerIdentityId: identity.id,
      // An ASK can share the owner's startup millisecond; advertise its actual epoch.
      ownerIncarnation: ownerControl.incarnation,
      status: "idle", runner: "pi", transport: "host", capabilities: ["ask", "steer", "followUp", "actor-bindings"],
      startedAt: 1, updatedAt: 1, controlProtocol: "v1", local, stale: false,
    };
    return { list: () => [member], get: (id) => id === actor.id ? member : undefined, self: () => member, peers: () => [], async refresh() {}, scheduleRefresh() {} };
  };
  const ownerControl = new FabricControlPlane(state.mesh, identity, { enabled: true, hostId: "host:owner", pollMs: 20, acknowledgementTimeoutMs: 5_000 });
  const callerControl = new FabricControlPlane(state.mesh, caller, { enabled: true, hostId: "host:caller", pollMs: 20, acknowledgementTimeoutMs: 5_000 });
  controls.push(ownerControl, callerControl);
  const make = (manager: ActorManager, who: typeof identity, participants: FabricParticipantSource, control: FabricControlPlane) => new AgentsProvider(
    state.agents, manager, new GlobalActorRegistry(state.root, 64 * 1024), mainFor(who), participants, control,
    new LifecycleBroker(state.mesh, who, participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {}), () => false,
  );
  const owner = make(actors, identity, source(true), ownerControl);
  const provider = make(callerActors, caller, source(false), callerControl);
  ownerControl.start((command, from, signal) => owner.acceptControl(command, from, signal));
  callerControl.start(() => ({ accepted: false }));
  const context: FabricInvocationContext = { cwd: process.cwd(), signal: undefined, extensionContext: {} as ExtensionContext, parentToolCallId: "binding-regression", nestedToolCallId: "binding-regression", update() {} };
  return { provider, context };
};
afterEach(async () => {
  await Promise.all(controls.splice(0).map((control) => control.close()));
  for (const release of releases.splice(0)) release();
  await Promise.all(managers.splice(0).map((manager) => manager.close()));
  await Promise.all(agentsList.splice(0).map((manager) => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("resident queued binding defaults", () => {
  it.each(["session", "durable"] as const)("%s: rebinds N queued and overflow items, pins only requested fields, and leaves an in-flight launch unchanged", async (residency) => {
    const state = setup(); const actors = state.open();
    const actor = await actors.create({ name: "serial", instructions: "Watch", model: "provider/old", thinking: "low", coalesce: false, residency });
    actors.tell(actor.id, "in flight"); await waitFor(() => state.launches.length === 1);
    for (let index = 0; index < 6; index++) actors.tell(actor.id, `default-${index}`);
    actors.tell(actor.id, "both explicit", undefined, { overrides: { model: "provider/pinned", thinking: "high" } });
    actors.tell(actor.id, "model explicit", undefined, { overrides: { model: "provider/pinned" } });
    actors.tell(actor.id, "thinking explicit", undefined, { overrides: { thinking: "max" } });
    await actors.setInstructions(actor.id, "New instructions marker");
    await actors.setModel(actor.id, "provider/new"); await actors.setThinking(actor.id, "xhigh");
    const file = fs.readdirSync(path.join(state.actorRoot, actor.id)).find((name) => name.startsWith("queue-"))!;
    const saved = JSON.parse(fs.readFileSync(path.join(state.actorRoot, actor.id, file), "utf8"));
    expect(saved.items.slice(0, 7).map((item: { binding: unknown }) => item.binding)).toEqual(Array.from({ length: 7 }, () => ({})));
    state.release(); await waitFor(() => state.launches.length === 10 && actors.status(actor.id).status === "idle");
    expect(state.launches[0]).toMatchObject({ model: "provider/old", thinking: "low" });
    expect(state.launches[0]!.systemPrompt).toContain("Watch");
    expect(state.launches[0]!.systemPrompt).not.toContain("New instructions marker");
    expect(state.launches.slice(1).every((request) => request.systemPrompt?.includes("New instructions marker"))).toBe(true);
    expect(state.launches.slice(1, 7)).toEqual(Array.from({ length: 6 }, () => expect.objectContaining({ model: "provider/new", thinking: "xhigh" })));
    expect(state.launches[7]).toMatchObject({ model: "provider/pinned", thinking: "high" });
    expect(state.launches[8]).toMatchObject({ model: "provider/pinned", thinking: "xhigh" });
    expect(state.launches[9]).toMatchObject({ model: "provider/new", thinking: "max" });
  });

  it.each([
    ["model only", { model: "provider/foreign" }],
    ["thinking only", { thinking: "medium" as const }],
    ["neither field", {}],
  ] as Array<[string, FabricActorRunBinding]>)("foreign Main %s stays resolved before and after queued owner-session changes", async (_label, binding) => {
    const state = setup(10); const actors = state.open();
    const actor = await actors.create({ name: "foreign queued", instructions: "Watch", residency: "durable" });
    await actors.setModel(actor.id, "provider/private-before"); await actors.setThinking(actor.id, "xhigh");
    const { provider, context } = routedProvider(state, actors, actor, false);
    state.release();
    await provider.invoke("ask", { id: actor.id, message: "before change", ...binding }, context);
    // Hold the next own activation so the foreign ASK and TELL really wait in the mailbox.
    const release = state.holdNext();
    actors.tell(actor.id, "hold owner"); await waitFor(() => actors.status(actor.id).status === "running");
    const answer = provider.invoke("ask", { id: actor.id, message: "queued ask", ...binding }, context);
    await waitFor(() => actors.status(actor.id).queued === 1);
    await provider.invoke("tell", { id: actor.id, message: "queued tell", ...binding }, context);
    await actors.setModel(actor.id, "provider/private-after"); await actors.setThinking(actor.id, "max");
    release(); await answer; await waitFor(() => state.launches.length === 4 && actors.status(actor.id).status === "idle");
    for (const request of [state.launches[0]!, ...state.launches.slice(2)]) {
      expect({ model: request.model, thinking: request.thinking }).toEqual({ model: binding.model, thinking: binding.thinking });
    }
  });

  it.each([
    ["model only", { model: "provider/foreign" }],
    ["thinking only", { thinking: "medium" as const }],
    ["neither field", {}],
  ] as Array<[string, FabricActorRunBinding]>)("foreign Main %s remains resolved after mailbox persistence and restore", async (_label, binding) => {
    const state = setup(10); const actors = state.open();
    const actor = await actors.create({ name: "foreign restored", instructions: "Watch", residency: "durable" });
    await actors.setModel(actor.id, "provider/private-before"); await actors.setThinking(actor.id, "xhigh");
    actors.tell(actor.id, "in flight"); await waitFor(() => state.launches.length === 1);
    const { provider, context } = routedProvider(state, actors, actor, false);
    await provider.invoke("tell", { id: actor.id, message: "restore foreign", ...binding }, context);
    await actors.setModel(actor.id, "provider/private-after"); await actors.setThinking(actor.id, "max");
    // close suspends the held activation and queued callerless work; the on-disk
    // mailbox is replayed by a fresh owner with the same private session binding.
    const closing = actors.close(); state.release(); await closing;
    const reloaded = state.open(); await waitFor(() => state.launches.length === 3 && reloaded.status(actor.id).status === "idle");
    expect(reloaded.status(actor.id)).toMatchObject({ model: "provider/private-after", thinking: "max" });
    expect({ model: state.launches[2]!.model, thinking: state.launches[2]!.thinking }).toEqual({ model: binding.model, thinking: binding.thinking });
  });

  it.each([
    ["model only", { model: "provider/foreign" }],
    ["thinking only", { thinking: "medium" as const }],
    ["neither field", {}],
  ] as Array<[string, FabricActorRunBinding]>)("foreign Main %s reaches actual runner configuration fallback, not private session values", async (_label, binding) => {
    const state = setup(10, true); const actors = state.open(); state.release();
    const actor = await actors.create({ name: "foreign fallback", instructions: "Watch", residency: "durable", extensions: false, responseMode: "text" });
    await actors.setModel(actor.id, "provider/private"); await actors.setThinking(actor.id, "xhigh");
    const { provider, context } = routedProvider(state, actors, actor, false);
    await provider.invoke("ask", { id: actor.id, message: "prove config fallback", ...binding }, context);
    expect(state.results[0]).toMatchObject({ status: "completed", model: binding.model ?? "provider/config", thinking: binding.thinking ?? "low" });
  });

  it("routes a foreign Main ASK in the owner startup millisecond using its advertised incarnation", async () => {
    const state = setup(10); const actors = state.open(); state.release();
    const actor = await actors.create({ name: "foreign startup", instructions: "Watch", residency: "durable" });
    await actors.setModel(actor.id, "provider/private"); await actors.setThinking(actor.id, "xhigh");
    const at = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(at);
    try {
      const { provider, context } = routedProvider(state, actors, actor, false);
      await provider.invoke("ask", { id: actor.id, message: "same-millisecond ask", model: "provider/foreign" }, context);
    } finally {
      clock.mockRestore();
    }
    expect(state.results[0]).toMatchObject({ status: "completed" });
    expect({ model: state.launches[0]!.model, thinking: state.launches[0]!.thinking }).toEqual({ model: "provider/foreign", thinking: undefined });
  });

  it("own-root omitted fields still follow current session defaults after mailbox restore", async () => {
    const state = setup(10); const actors = state.open();
    const actor = await actors.create({ name: "own restored", instructions: "Watch", residency: "durable" });
    await actors.setModel(actor.id, "provider/old"); await actors.setThinking(actor.id, "low");
    actors.tell(actor.id, "in flight"); await waitFor(() => state.launches.length === 1);
    const { provider, context } = routedProvider(state, actors, actor, true);
    await provider.invoke("tell", { id: actor.id, message: "restore own" }, context);
    await actors.setModel(actor.id, "provider/new"); await actors.setThinking(actor.id, "max");
    const closing = actors.close(); state.release(); await closing;
    const reloaded = state.open(); await waitFor(() => state.launches.length === 3 && reloaded.status(actor.id).status === "idle");
    expect(state.launches[2]).toMatchObject({ model: "provider/new", thinking: "max" });
  });

  it("keeps coalesced mesh defaults dynamic, including when the replaced item sits in overflow", async () => {
    const state = setup(1); const actors = state.open();
    const actor = await actors.create({ name: "coalesced", instructions: "Watch", model: "provider/old", thinking: "low", topics: ["work.items"], coalesceKey: "key" });
    actors.tell(actor.id, "in flight"); await waitFor(() => state.launches.length === 1);
    actors.tell(actor.id, "queue slot");
    await state.mesh.publish({ topic: "work.items", from: identity, data: { key: "same", value: "older" } });
    await waitFor(() => actors.status(actor.id).queued === 2);
    await state.mesh.publish({ topic: "work.items", from: identity, data: { key: "same", value: "newer" } });
    await waitFor(() => {
      const file = fs.readdirSync(path.join(state.actorRoot, actor.id)).find((name) => name.startsWith("queue-"))!;
      return fs.readFileSync(path.join(state.actorRoot, actor.id, file), "utf8").includes("newer");
    });
    await actors.setModel(actor.id, "provider/new", "project"); await actors.setThinking(actor.id, "max", "project");
    state.release(); await waitFor(() => state.launches.length === 3 && actors.status(actor.id).status === "idle");
    expect(state.launches[2]).toMatchObject({ model: "provider/new", thinking: "max" });
    expect(state.launches[2]!.task).toContain("newer"); expect(state.launches[2]!.task).not.toContain("older");
  });

  it("persists project model/thinking through the existing publishPresence save path", async () => {
    const state = setup(); const actors = state.open(); state.release();
    const actor = await actors.create({ name: "persisted", instructions: "Watch", model: "provider/old", thinking: "low" });
    await actors.setModel(actor.id, "provider/new", "project"); await actors.setThinking(actor.id, "max", "project");
    await actors.close(); const reloaded = state.open();
    expect(reloaded.status(actor.id)).toMatchObject({ model: "provider/new", thinking: "max", projectDefaults: { model: "provider/new", thinking: "max" } });
  });

  it("migrates legacy mesh/host defaults but conservatively preserves legacy direct bindings on reload", async () => {
    const state = setup(10); const actors = state.open(); state.release();
    const actor = await actors.create({ name: "reload", instructions: "Watch", model: "provider/old", thinking: "low" });
    await actors.close();
    const dir = path.join(state.actorRoot, actor.id);
    const key = (await import("node:crypto")).createHash("sha256").update([identity.id, "session"].join("\0")).digest("hex").slice(0, 16);
    const registry = JSON.parse(fs.readFileSync(path.join(state.actorRoot, "actors.json"), "utf8"));
    delete registry.actors[0].projectDefaults; // Exercise the pre-resolved-binding registry format.
    registry.actors[0].model = "provider/new"; registry.actors[0].thinking = "max";
    fs.writeFileSync(path.join(state.actorRoot, "actors.json"), JSON.stringify(registry));
    const records = ["mesh:work.items", "host:input", "direct", "direct"].map((source, index) => ({
      id: `saved-${index}`, source, payload: { message: `saved-${index}` }, createdAt: Date.now(),
      activation: { kind: "direct", id: `saved-${index}`, source, sequence: index + 1, createdAt: Date.now() },
      binding: index === 3 ? { model: "provider/pinned" } : { model: "provider/old", thinking: "low" },
      ...(index === 3 ? { bindingVersion: 2 } : {}),
    }));
    fs.writeFileSync(path.join(dir, `queue-${key}.json`), JSON.stringify({ format: 1, items: records }));
    const reloaded = state.open(); await waitFor(() => state.launches.length === 4 && reloaded.status(actor.id).status === "idle");
    expect(state.launches.slice(0, 2)).toEqual(Array.from({ length: 2 }, () => expect.objectContaining({ model: "provider/new", thinking: "max" })));
    expect(state.launches[2]).toMatchObject({ model: "provider/old", thinking: "low" });
    expect(state.launches[3]).toMatchObject({ model: "provider/pinned", thinking: "max" });
  });
});

describe("owner-default control provenance", () => {
  it("accepts raw explicit fields from root or validated own-root actors, and rejects foreign promotion", () => {
    const command = { binding: { model: "provider/pinned" }, bindingProvenance: { kind: "owner-defaults" as const, rootId: identity.id } };
    expect(controlActorBindingOptions(command, identity, identity.id, undefined)).toEqual({ overrides: { model: "provider/pinned" } });
    const sender = { id: "actor-a", name: "actor", kind: "actor" as const };
    expect(controlActorBindingOptions(command, sender, identity.id, identity.id)).toEqual({ overrides: command.binding });
    expect(() => controlActorBindingOptions(command, sender, identity.id, "session:foreign")).toThrow("Invalid actor owner-default binding provenance");
    expect(() => controlActorBindingOptions(command, identity, "session:foreign", identity.id)).toThrow("Invalid actor owner-default binding provenance");
    expect(controlActorBindingOptions({ binding: command.binding }, sender, identity.id, "session:foreign")).toEqual({ binding: command.binding });
  });
});
