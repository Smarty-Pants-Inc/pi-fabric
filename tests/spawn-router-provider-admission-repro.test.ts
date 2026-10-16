// Keep: trust-boundary/data-safety regression for router admission receipts (#2890).
// This is a local-safe provider-to-native path: the worker is the checked-in fake
// worker, the router command is intentionally empty (fail-open default), and all
// state is confined to an isolated private temporary directory.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import type { MeshIdentity } from "../src/mesh/store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const actors: ActorManager[] = [];

const context: FabricInvocationContext = {
  cwd: process.cwd(), signal: undefined, parentToolCallId: "repro", nestedToolCallId: "nested",
  extensionContext: { modelRegistry: { getAvailable: () => [
    { provider: "provider", id: "model-a" }, { provider: "provider", id: "model-b" },
  ] } } as never,
  update() {}, activity() {},
};

const ledger = (meshRoot: string): Array<Record<string, unknown>> =>
  fs.readFileSync(path.join(meshRoot, "router", "decisions.jsonl"), "utf8")
    .trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>);

const setup = () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-router-admission-repro-"));
  roots.push(root);
  const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100, { stateBackend: "file" });
  const identity: MeshIdentity = { id: "session:repro", name: "repro", kind: "main", sessionId: "repro" };
  const mainAgent = {
    id: identity.id, local: true, matches: (id: string) => id === identity.id,
    info: () => ({ id: identity.id, name: "Main", kind: "main", status: "idle", runner: "pi", transport: "host",
      cwd: process.cwd(), sessionId: "repro", startedAt: 1, updatedAt: 1, pendingMessages: false, local: true }),
    deliverAgent: () => ({ queued: true, messageId: "repro-message", routed: "main" }),
  } as unknown as FabricMainAgentTarget;
  const manager = new AgentManager(process.cwd(), {
    ...DEFAULT_FABRIC_CONFIG.agents,
    model: "provider/model-a", thinking: "medium", budgetUsd: 0, maxConcurrent: 1,
    transport: "process", extensions: false,
    router: { command: [], mode: "shadow", timeoutMs: 400 },
  }, {
    runRoot: path.join(root, "runs"),
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"),
    hostId: identity.id, mainAgentId: identity.id, identityId: identity.id,
  });
  managers.push(manager);
  const actorManager = new ActorManager("repro", identity, mesh,
    { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, mainAgent,
      rootId: identity.id,
    });
  actors.push(actorManager);
  const participants: FabricParticipantSource = {
    list: () => [], get: () => undefined,
    self: () => ({ id: identity.id, kind: "root", rootId: identity.id,
      ownerHostId: identity.id, ownerIdentityId: identity.id, name: "repro", status: "idle",
      runner: "pi", transport: "host", capabilities: [], cwd: process.cwd(), sessionId: "repro",
      startedAt: 1, updatedAt: 1, pendingMessages: false, controlProtocol: "v1", local: true,
      stale: false, format: 1 }),
    peers: () => [], refresh: async () => {}, scheduleRefresh: () => {},
  };
  const provider = new AgentsProvider(manager, actorManager, new GlobalActorRegistry(root, 64 * 1024),
    mainAgent, participants, undefined, {} as never);
  return { root, mesh, manager, provider, actorManager, mainAgent };
};

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(actors.splice(0).map(actor => actor.close()));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

describe("provider router admission receipt repro", () => {
  it("records the native start in the authenticated admission mesh", async () => {
    const foreignMeshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-router-foreign-mesh-"));
    roots.push(foreignMeshRoot);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", foreignMeshRoot);
    const state = setup();
    const child = await state.provider.invoke("spawn", { task: "private normal flow" }, context) as { id: string };
    await expect(state.manager.wait(child.id)).resolves.toMatchObject({ status: "completed" });
    const entries = ledger(state.mesh.root);
    expect(entries).toHaveLength(2);
    expect(entries[0]).toMatchObject({ kind: "decision", spawnKind: "spawn" });
    expect(entries[1]).toMatchObject({ kind: "start", decisionId: entries[0]!.decisionId, agentId: child.id });
    expect(state.mesh.get(`decisions/spawn-router/admissions/${entries[0]!.decisionId}`, { fresh: true })?.value)
      .toMatchObject({ state: "consumed" });
  });

  it("records one native start-failed outcome in the authenticated admission mesh", async () => {
    const foreignMeshRoot = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-router-foreign-mesh-"));
    roots.push(foreignMeshRoot);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", foreignMeshRoot);
    const state = setup();
    const task = "private failure must not enter the ledger";
    await expect(state.provider.invoke("spawn", { task, transport: "tmux" }, context))
      .rejects.toThrow("Fabric agent transport tmux is disabled");
    const entries = ledger(state.mesh.root);
    expect(entries).toHaveLength(2);
    expect(entries[1]).toMatchObject({ kind: "start-failed", decisionId: entries[0]!.decisionId, error: "start-failed" });
    expect(JSON.stringify(entries)).not.toContain(task);
  });

  it("keeps ordinary durable child forwarding independent of participants.self", async () => {
    const state = setup();
    delete state.manager.config.router;
    const createActor = vi.fn(async (request: Record<string, unknown>) => ({ ...request, id: "resident-actor" }));
    vi.spyOn(ResidentActorClient, "fromEnv").mockReturnValue({ createActor } as unknown as ResidentActorClient);
    const withoutSelf = { scheduleRefresh() {}, refresh: async () => {} } as never;
    const childProvider = new AgentsProvider(state.manager, state.actorManager,
      new GlobalActorRegistry(state.root, 64 * 1024), state.mainAgent, withoutSelf,
      undefined, {} as never);
    const signal = new AbortController().signal;
    await expect(childProvider.invoke("create", { name: "ordinary", instructions: "Work.", residency: "durable" },
      { ...context, signal })).resolves.toMatchObject({ id: "resident-actor" });
    expect(createActor).toHaveBeenCalledWith(expect.objectContaining({ name: "ordinary", residency: "durable" }), signal);
  });
});
