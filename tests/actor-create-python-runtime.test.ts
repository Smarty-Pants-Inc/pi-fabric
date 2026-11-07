import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, type FabricPythonRuntime } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { actorRestoreRequest } from "../src/main-drain/actor-restore.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";

const cleanups: Array<() => Promise<void>> = [];
beforeEach(() => {
  // This fixture is a standalone owning Main, even when Vitest is launched
  // from a factory task process. Do not inherit that launcher's worker fence.
  for (const key of [
    "PI_FABRIC_PARENT_RUN", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_LINEAGE", "PI_FABRIC_DEPTH",
    "PI_FABRIC_TASK_PROCESS_CHILD", "PI_FABRIC_AGENT_RUN_DIR", "PI_FABRIC_AGENT_NAME",
    "PI_FABRIC_SPAWNER_KIND", "SMARTY_ROLE", "PI_FABRIC_BUDGET_FILE", "PI_FABRIC_BUDGET",
  ]) vi.stubEnv(key, undefined);
});
afterEach(async () => {
  try {
    for (const cleanup of cleanups.splice(0)) await cleanup();
  } finally {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  }
});
const unused = (): never => { throw new Error("Unexpected metadata collaborator call"); };

// Snapshot bytes and directory membership, not just actor count: validation must
// not materialize a session, change a registry, or publish an actor to the mesh.
const treeSnapshot = (root: string): unknown => {
  if (!fs.existsSync(root)) return null;
  return fs.readdirSync(root, { withFileTypes: true })
    .sort((a, b) => a.name.localeCompare(b.name))
    .map(entry => ({
      name: entry.name,
      value: entry.isDirectory()
        ? treeSnapshot(path.join(root, entry.name))
        : fs.readFileSync(path.join(root, entry.name)).toString("hex"),
    }));
};

// Reuses the actor-kernel persistence fixture and fabric-exec-null-arguments
// public executor pattern. The create method and registry are never mocked.
const fixture = (inheritedRuntime: FabricPythonRuntime = "monty") => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-create-python-runtime-"));
  const actorRoot = path.join(root, "actors");
  const agents = new AgentManager(root, structuredClone(DEFAULT_FABRIC_CONFIG.agents), {
    runRoot: path.join(root, "runs"), kernel: () => "typescript", pythonRuntime: () => inheritedRuntime,
  });
  // Fail closed if this create-only regression accidentally activates anything.
  const run = vi.spyOn(agents, "run").mockImplementation(async () => { throw new Error("Actor activation is forbidden in this test"); });
  const spawn = vi.spyOn(agents, "spawn").mockImplementation(async () => { throw new Error("Worker spawn is forbidden in this test"); });
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
  const identity: MeshIdentity = {
    id: "session:create-python-runtime", name: "main", kind: "main", sessionId: "create-python-runtime",
  };
  const main: FabricMainAgentTarget = { id: identity.id, local: true, matches: () => false, info: unused, deliverAgent: unused };
  const refresh = vi.fn();
  const participants: FabricParticipantSource = {
    list: () => [], get: () => undefined, self: unused, peers: () => [],
    async refresh() {}, scheduleRefresh: refresh,
  };
  const actors = new ActorManager("create-python-runtime", identity, mesh, {
    ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true,
  }, agents, unused, {
    actorRoot, persistent: true, mainAgent: main, canConsumeMesh: () => false, reapDeadSessionPresence: false,
  });
  const lifecycle = new LifecycleBroker(mesh, identity, participants, {
    enabled: false, pollMs: 20, maxReadEvents: 100,
  }, async () => {});
  const globalRegistry = new GlobalActorRegistry(root, 64 * 1024);
  const provider = new AgentsProvider(agents, actors, globalRegistry, main, participants, undefined, lifecycle);
  const invoke = vi.spyOn(provider, "invoke");
  const create = vi.spyOn(actors, "create"); // Call-through only: real admission and persistence.
  const registry = new ActionRegistry();
  registry.register(provider);
  cleanups.push(async () => {
    try { await registry.close(); } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
  const config = structuredClone(DEFAULT_FABRIC_CONFIG);
  config.fullCodeMode = false;
  config.executor.kernel = "typescript";
  config.executor.runtime = "quickjs";
  config.approvals.agent = "allow";
  const service = new FabricExecutionService(registry, config);
  const executeAction = (ref: string, args: object) => service.execute({
    // Generic tools.call deliberately reaches authoritative registry validation
    // rather than stopping at the guest's typed agents facade.
    code: `return tools.call({ ref: ${JSON.stringify(ref)}, args: ${JSON.stringify(args)} });`,
    signal: undefined, parentToolCallId: "create-python-runtime",
    context: { cwd: root, hasUI: false } as ExtensionContext, onPartial() {},
  });
  const executeCreate = (args: object) => executeAction("agents.create", args);
  return { root, actorRoot, actors, globalRegistry, run, spawn, refresh, invoke, create, executeAction, executeCreate };
};

describe("public agents.create Python runtime", () => {
  it.each(["monty", "cpython"] as const)("restores and persists %s through tools.call and the real actor registry without activation", async (pythonRuntime) => {
    // Opposite live defaults prove that both explicit selectors survive public
    // argument validation and provider projection, rather than being re-inherited.
    const { actorRoot, actors, run, spawn, create, executeCreate } = fixture(pythonRuntime === "monty" ? "cpython" : "monty");
    const request = actorRestoreRequest({
      id: "a".repeat(32), rootId: "session:old-main", status: "stopped",
      name: `restored-${pythonRuntime}`, instructions: "Idle actor; do not execute Python.",
      residency: "session", runner: "pi", extensions: true, kernel: "python", pythonRuntime,
    });
    expect(request).not.toHaveProperty("id");
    expect(request).not.toHaveProperty("rootId");
    expect(request).toMatchObject({ kernel: "python", pythonRuntime });

    const result = await executeCreate(request);
    expect(result.success, result.error).toBe(true);
    expect(result.value).toMatchObject({ id: expect.any(String), name: request.name, status: "idle", kernel: "python", pythonRuntime });
    const actor = actors.list().find(row => row.name === request.name)!;
    expect(actor).toBeDefined();
    expect(actors.status(actor.id)).toMatchObject({ kernel: "python", pythonRuntime });
    expect(actors.definition(actor.id)).toMatchObject({ kernel: "python", pythonRuntime });
    expect(new ActorRegistryStore(actorRoot).records()).toEqual([
      expect.objectContaining({ id: actor.id, kernel: "python", pythonRuntime }),
    ]);
    // Independent disk readback: not just the manager/store's warm in-memory view.
    const saved = JSON.parse(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8"));
    expect(saved.actors).toEqual([expect.objectContaining({ id: actor.id, kernel: "python", pythonRuntime })]);
    expect(create).toHaveBeenCalledTimes(1);
    expect(run).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  });

  it.each(["cpython", "monty"] as const)("roundtrips global %s templates through public create and import without activation", async (pythonRuntime) => {
    const { root, actorRoot, actors, globalRegistry, run, spawn, create, executeAction, executeCreate } = fixture(pythonRuntime === "monty" ? "cpython" : "monty");
    const name = `global-${pythonRuntime}`;
    const result = await executeCreate({
      name, instructions: "Idle global template; do not execute Python.", scope: "global",
      residency: "session", runner: "pi", extensions: true, kernel: "python", pythonRuntime,
    });
    expect(result.success, result.error).toBe(true);
    expect(result.value).toMatchObject({ id: expect.any(String), name, kernel: "python", pythonRuntime });
    const template = globalRegistry.resolve(name)!;
    expect(template).toMatchObject({ name, kernel: "python", pythonRuntime });
    expect(actors.list()).toEqual([]);
    expect(create).not.toHaveBeenCalled();

    // A new registry instance must recover the selector from the temporary disk,
    // and its real import projection must preserve it against the opposite default.
    const reloaded = new GlobalActorRegistry(root, 64 * 1024);
    const persisted = reloaded.resolve(template.id)!;
    expect(persisted).toMatchObject({ id: template.id, kernel: "python", pythonRuntime });
    expect(reloaded.toRequest(persisted)).toMatchObject({ name, kernel: "python", pythonRuntime });
    const savedTemplate = JSON.parse(fs.readFileSync(path.join(root, "fabric", "actors", "global-actors.json"), "utf8"));
    expect(savedTemplate.actors).toEqual([expect.objectContaining({ id: template.id, kernel: "python", pythonRuntime })]);

    const importedName = `imported-${pythonRuntime}`;
    const imported = await executeAction("agents.import", { id: persisted.id, as: importedName });
    expect(imported.success, imported.error).toBe(true);
    expect(imported.value).toMatchObject({ id: expect.any(String), name: importedName, status: "idle", kernel: "python", pythonRuntime });
    const actor = actors.list().find(row => row.name === importedName)!;
    expect(actor).toBeDefined();
    expect(actor.id).not.toBe(template.id);
    expect(actors.status(actor.id)).toMatchObject({ kernel: "python", pythonRuntime });
    expect(actors.definition(actor.id)).toMatchObject({ kernel: "python", pythonRuntime });
    expect(new ActorRegistryStore(actorRoot).records()).toEqual([
      expect.objectContaining({ id: actor.id, kernel: "python", pythonRuntime }),
    ]);
    const savedActor = JSON.parse(fs.readFileSync(path.join(actorRoot, "actors.json"), "utf8"));
    expect(savedActor.actors).toEqual([expect.objectContaining({ id: actor.id, kernel: "python", pythonRuntime })]);
    expect(create).toHaveBeenCalledTimes(1);
    expect(run).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  });

  it.each([
    ["unknown backend", "pypy"],
    ["empty backend", ""],
    ["null", null],
    ["boolean", false],
    ["number", 1],
    ["object", { backend: "monty" }],
    ["array", ["monty"]],
  ])("rejects %s before actor creation with exactly no persisted effects", async (_label, pythonRuntime) => {
    const { root, actors, run, spawn, refresh, invoke, create, executeCreate } = fixture();
    const before = treeSnapshot(root);
    const result = await executeCreate({ name: "rejected", instructions: "Must not be created.", kernel: "python", pythonRuntime });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/(?:Invalid arguments for agents\.create:.*pythonRuntime|Invalid inherited Fabric Python runtime)/s);
    expect(invoke).not.toHaveBeenCalled();
    expect(create).not.toHaveBeenCalled();
    expect(actors.list()).toEqual([]);
    expect(treeSnapshot(root)).toEqual(before);
    expect(refresh).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  });

  it("keeps the unknown-property refusal path strict when pythonRuntime is supplied", async () => {
    const { root, actors, run, spawn, refresh, invoke, create, executeCreate } = fixture();
    const before = treeSnapshot(root);
    const result = await executeCreate({
      name: "unknown-property", instructions: "Must not be created.", kernel: "python", pythonRuntime: "monty", unknown: true,
    });
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/Invalid arguments for agents\.create:.*unknown/s);
    expect(invoke).not.toHaveBeenCalled();
    expect(create).not.toHaveBeenCalled();
    expect(actors.list()).toEqual([]);
    expect(treeSnapshot(root)).toEqual(before);
    expect(refresh).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
    expect(spawn).not.toHaveBeenCalled();
  });
});
