import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { MeshStore } from "../src/mesh/store.js";
import { AgentManager } from "../src/agents/manager.js";
import { assertAgentSpawnAllowed, AgentSpawnForbiddenError, snapshotAgentSpawnEnvironment } from "../src/agents/spawn-authority.js";
import { createAgentServiceClient, createAgentsProvider } from "../src/agents/service-provider.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

// Coverage for the provider trust boundary: a task worker is not an interactive
// caller, even if it asks for an ordinary child, actor, global template, alias,
// fork, or handoff. The manager spy proves refusal precedes launch side effects.
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
});

const context: FabricInvocationContext = {
  cwd: process.cwd(), signal: undefined, parentToolCallId: "task-spawn-unit", nestedToolCallId: "nested",
  extensionContext: {} as FabricInvocationContext["extensionContext"], update() {}, activity() {},
};

const harness = (kind: "main" | "actor" | "agent", actorIdentityId?: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-spawn-authority-")); roots.push(root);
  const identity = { id: actorIdentityId ?? `${kind}:task-spawn-fixture`, name: "fixture", kind, sessionId: "task-spawn-fixture" } as const;
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi" }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), piBinary: path.resolve("tests/fixtures/fake-pi.mjs"),
    runRoot: path.join(root, "runs"),
  }); managers.push(manager);
  const main = {
    id: kind === "main" ? identity.id : "session:root-main", local: kind === "main",
    matches: (id: string) => id === "main" || id === "session:root-main" || id === identity.id,
    info: () => ({ id: "session:root-main", name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host", cwd: root, sessionId: "fixture", startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
    deliverAgent: () => ({ queued: true, messageId: "fixture", routed: "main" }), flushHeldAtNextBoundary() {},
  } as unknown as ConstructorParameters<typeof AgentsProvider>[3];
  const participants: FabricParticipantSource = {
    list: () => [], get: () => undefined,
    self: () => ({ ownerIncarnation: "fixture", format: 1, id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "fixture", status: "idle", runner: "pi", transport: "host",
      capabilities: ["fabric"], cwd: root, sessionId: "fixture", startedAt: 1, updatedAt: 1, pendingMessages: false,
      controlProtocol: "v1", local: true, stale: false }),
    peers: () => [], async refresh() {}, scheduleRefresh() {},
  } as unknown as FabricParticipantSource;
  const actors = new ActorManager("fixture", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh }, manager,
    () => undefined, { actorRoot: path.join(root, "actors"), persistent: true, mainAgent: main, rootId: "session:root-main" });
  const globalActors = new GlobalActorRegistry(root, 64 * 1024);
  let provider!: AgentsProvider;
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: true, pollMs: 20, maxReadEvents: 100 },
    async (subscription, event) => provider.deliverLifecycle(subscription, event));
  provider = new AgentsProvider(manager, actors, globalActors, main, participants, undefined, lifecycle,
    undefined, undefined, true);
  return { root, manager, provider, actors, globalActors };
};

const forbidden = (error: unknown) => {
  expect(error).toBeInstanceOf(AgentSpawnForbiddenError);
  expect(error).toMatchObject({ name: "AgentSpawnForbiddenError", code: "FABRIC_AGENT_SPAWN_FORBIDDEN", launchOutcome: "unlaunched" });
};

describe("task-agent spawn authority", () => {
  const workerBase = (): NodeJS.ProcessEnv => ({
    SMARTY_ROLE: "task-agent", PI_FABRIC_PARENT_RUN: "a".repeat(32),
    PI_FABRIC_DEPTH: "0", PI_FABRIC_LINEAGE: JSON.stringify({ version: 1, rootSessionId: "root",
      runId: "a".repeat(32), depth: 0, childIndex: 0, worker: true }),
  });

  it.each([
    ["empty parent", { PI_FABRIC_PARENT_RUN: "" }],
    ["malformed parent", { PI_FABRIC_PARENT_RUN: "not-a-run" }],
    ["empty lineage", { PI_FABRIC_LINEAGE: "" }],
    ["malformed lineage", { PI_FABRIC_LINEAGE: "{" }],
    ["nonzero depth", { PI_FABRIC_DEPTH: "1" }],
    ["malformed depth", { PI_FABRIC_DEPTH: "wat" }],
  ] as const)("refuses a Main-shaped caller when task environment has %s", (_label, overrides) => {
    const environment = { ...workerBase(), ...overrides };
    try { assertAgentSpawnAllowed({ kind: "main" }, environment); }
    catch (error) {
      expect(error).toBeInstanceOf(AgentSpawnForbiddenError);
      expect(error).toMatchObject({ code: "FABRIC_AGENT_SPAWN_FORBIDDEN", launchOutcome: "unlaunched" });
      return;
    }
    throw new Error("malformed/worker context must fail closed");
  });

  it.each([
    { PI_FABRIC_PARENT_RUN: "" },
    { PI_FABRIC_ACTOR_ID: "" },
    { PI_FABRIC_LINEAGE: "{" },
    { PI_FABRIC_DEPTH: "" },
    { PI_FABRIC_DEPTH: " " },
    { PI_FABRIC_DEPTH: "1" },
    { PI_FABRIC_TASK_PROCESS_CHILD: "" },
    { PI_FABRIC_AGENT_RUN_DIR: "" },
    { PI_FABRIC_AGENT_NAME: "" },
    { PI_FABRIC_SPAWNER_KIND: "" },
    { SMARTY_ROLE: "task-agent@0123456789ab" },
  ])("fails closed on an isolated worker marker %j", environment => {
    expect(() => assertAgentSpawnAllowed({ kind: "main" }, environment)).toThrow(AgentSpawnForbiddenError);
  });

  it("refuses unknown caller kinds and permits only an explicitly bound actor", () => {
    const task = workerBase();
    // A bound agent identity is sufficient task evidence even if its worker
    // environment has been scrubbed; ambient metadata cannot erase lineage.
    expect(() => assertAgentSpawnAllowed({ kind: "agent" }, {})).toThrow(AgentSpawnForbiddenError);
    for (const kind of ["unknown-principal", "", "main-shaped-but-unknown"]) {
      expect(() => assertAgentSpawnAllowed({ kind }, task)).toThrow(AgentSpawnForbiddenError);
      expect(() => assertAgentSpawnAllowed({ kind }, { ...task, PI_FABRIC_PARENT_RUN: "", PI_FABRIC_DEPTH: "bad" }))
        .toThrow(AgentSpawnForbiddenError);
    }
    expect(() => assertAgentSpawnAllowed(undefined as never, task)).toThrow(AgentSpawnForbiddenError);
    expect(() => assertAgentSpawnAllowed(null as never, task)).toThrow(AgentSpawnForbiddenError);
    const actorId = "b".repeat(32);
    const actorEnvironment = { ...workerBase(), PI_FABRIC_ACTOR_ID: actorId };
    expect(() => assertAgentSpawnAllowed({ kind: "actor", id: actorId }, actorEnvironment)).not.toThrow();
    expect(() => assertAgentSpawnAllowed({ kind: "actor", id: "c".repeat(32) }, actorEnvironment)).toThrow(AgentSpawnForbiddenError);
    expect(() => assertAgentSpawnAllowed({ kind: "agent", id: actorId }, actorEnvironment)).toThrow(AgentSpawnForbiddenError);
  });

  it.each([
    ["run", { task: "must not dispatch" }],
    ["spawn", { task: "must not dispatch" }],
    ["create", { name: "must not create" }],
    ["resume", { id: "existing-session", task: "must not resume" }],
  ] as const)("hosted provider refuses task-worker %s before dispatch", async (action, args) => {
    const dispatch = vi.fn(async () => ({ id: "unexpected-hosted-dispatch" }));
    vi.stubEnv("SMARTY_ROLE", "task-agent");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "e".repeat(32));
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "1");
    const provider = createAgentsProvider(createAgentServiceClient(dispatch, { topology: true, resume: true }));
    // The factory snapshots identity/environment once; later host environment
    // changes must not turn a task-worker provider into a Main caller.
    vi.stubEnv("SMARTY_ROLE", "main");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    let error: unknown;
    try { await provider.invoke(action, args, context); } catch (caught) { error = caught; }
    forbidden(error);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("leaves Main logical hosted service sessions authorized", async () => {
    const dispatch = vi.fn(async () => ({ id: "main-hosted-child" }));
    vi.stubEnv("SMARTY_ROLE", "main");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined);
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    vi.stubEnv("PI_FABRIC_DEPTH", "0");
    const provider = createAgentsProvider(createAgentServiceClient(dispatch, { topology: true, resume: true }));
    await expect(provider.invoke("spawn", { task: "Main hosted child" }, context)).resolves.toEqual({ id: "main-hosted-child" });
    expect(dispatch).toHaveBeenCalledOnce();
  });

  it("binds the decision to a captured environment snapshot", () => {
    const environment = workerBase();
    const snapshot = snapshotAgentSpawnEnvironment(environment);
    environment.SMARTY_ROLE = "main";
    environment.PI_FABRIC_PARENT_RUN = undefined;
    expect(() => assertAgentSpawnAllowed({ kind: "main" }, snapshot)).toThrow(AgentSpawnForbiddenError);
  });
  it.each(["run", "spawn", "create", "createActor", "import"])("refuses %s before provider effects", async action => {
    vi.stubEnv("SMARTY_ROLE", "task-agent");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "a".repeat(32));
    vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    const h = harness("agent");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const args: Record<string, unknown> = action === "run" || action === "spawn"
      ? { task: "must not launch", transport: "process", ...(action === "spawn" ? { residency: "durable", forkSeed: { mode: "branch" } } : {}) }
      : action === "create" || action === "createActor"
        ? { name: "must-not-create", instructions: "must not persist", scope: "global" }
        : { id: "global-template", as: "must-not-import" };
    try {
      let error: unknown;
      try { await h.provider.invoke(action, args, context); } catch (caught) { error = caught; }
      forbidden(error);
      expect(launch).not.toHaveBeenCalled();
      expect(h.manager.list()).toEqual([]);
      expect(h.actors.list()).toEqual([]);
      expect(h.globalActors.list()).toEqual([]);
    } finally { launch.mockRestore(); }
  });

  it("fails closed for malformed worker metadata and an actor-origin task child", async () => {
    for (const actorId of ["not-an-actor-id", "d".repeat(32)]) {
      vi.stubEnv("SMARTY_ROLE", "task-agent");
      vi.stubEnv("PI_FABRIC_PARENT_RUN", "malformed");
      vi.stubEnv("PI_FABRIC_ACTOR_ID", actorId);
      const h = harness("agent");
      let error: unknown;
      try { await h.provider.invoke("spawn", { task: "must not launch", transport: "process" }, context); }
      catch (caught) { error = caught; }
      forbidden(error);
      expect(h.manager.list()).toEqual([]);
    }
  });

  it("refuses executeHandoff itself, not only the public deferred handoff action", async () => {
    vi.stubEnv("SMARTY_ROLE", "task-agent"); vi.stubEnv("PI_FABRIC_PARENT_RUN", "b".repeat(32));
    const h = harness("agent");
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    let error: unknown;
    try {
      await h.provider.executeHandoff({ task: "no", model: "provider/model" }, context, { messages: [] } as never);
    } catch (caught) { error = caught; }
    forbidden(error);
    expect(launch).not.toHaveBeenCalled();
  });

  it("keeps the Main and a valid actor caller authorized", async () => {
    vi.stubEnv("SMARTY_ROLE", undefined); vi.stubEnv("PI_FABRIC_PARENT_RUN", undefined); vi.stubEnv("PI_FABRIC_ACTOR_ID", undefined);
    const main = harness("main");
    const mainRun = await main.provider.invoke("spawn", { task: "main may launch", transport: "process" }, context) as { id: string };
    expect(await main.manager.wait(mainRun.id)).toMatchObject({ status: "completed" });

    // Derive the authorized actor identity from an explicit actor creation
    // request; do not synthesize PI_FABRIC_ACTOR_ID as the trust evidence.
    const actorDefinition = await main.actors.create({ name: "explicit-actor-request", instructions: "A real fixture actor." });
    vi.stubEnv("SMARTY_ROLE", "task-agent");
    vi.stubEnv("PI_FABRIC_PARENT_RUN", "c".repeat(32));
    vi.stubEnv("PI_FABRIC_ACTOR_ID", actorDefinition.id);
    const actor = harness("actor", actorDefinition.id);
    const actorRun = await actor.provider.invoke("spawn", { task: "actor may launch", transport: "process" }, context) as { id: string };
    expect(await actor.manager.wait(actorRun.id)).toMatchObject({ status: "completed" });
  });
});
