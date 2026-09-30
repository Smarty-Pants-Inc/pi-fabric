import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { QuickJsRuntime } from "../src/runtime/quickjs-runtime.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { abandonResidentRequest, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
};
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const waitFor = async (predicate: () => boolean) => {
  const deadline = Date.now() + 8_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("Timed out waiting for real resident host exchange");
    await delay(10);
  }
};
const names = (directory: string) => {
  try { return fs.readdirSync(directory); } catch { return []; }
};
const entries = (root: string, directory: string) => names(path.join(root, directory)).filter((file) => file.endsWith(".json"));
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });

/** No fake host/response: real pickup, managers, model refresh, worker launch and ownership. */
const harness = async (beforeCommit: boolean, seed?: (config: ResidentHostConfig) => void, commandTimeoutMs = 500) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-commit-fence-"));
  const meshRoot = path.join(root, "mesh");
  const rootId = `session:fence:${path.basename(root)}`;
  const residencyRoot = residentRoot(meshRoot, rootId);
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const mesh = new MeshStore(meshRoot, meshConfig.maxEventBytes, meshConfig.maxReadEvents);
  const identity = { id: rootId, name: "main", kind: "main" as const };
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: rootId, rootId, identity });
  participants.registerSource(() => [{
    format: 1, id: rootId, kind: "root", rootId, ownerHostId: rootId, ownerIdentityId: rootId,
    name: "main", status: "idle", residency: "session", runner: "pi", transport: "host",
    capabilities: ["fabric"], cwd: root, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1",
  }]);
  await participants.start();
  const config: ResidentHostConfig = {
    format: 1, rootId, sessionId: "fence", cwd: root, projectRoot: root, meshRoot,
    actorRoot: path.join(meshRoot, "actors"), sessionActorRoot: path.join(meshRoot, "actors", "fence"),
    residencyRoot, fullCodeMode: false, agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000 },
    mesh: meshConfig, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
    piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available: [{ provider: "test", id: "visible" }], aliases: {}, defaultModel: "test/visible" },
  };
  fs.mkdirSync(residencyRoot, { recursive: true });
  const configPath = path.join(residencyRoot, "config.json");
  fs.writeFileSync(configPath, JSON.stringify(config));
  seed?.(config);
  const entered = deferred();
  const release = deferred();
  let refreshed = false;
  const registry = {
    getAvailable: () => refreshed ? [{ provider: "test", id: "slow" }] : [],
    refresh: async () => { entered.resolve(); await release.promise; refreshed = true; },
  };
  const shutdown = new AbortController();
  const signalListeners = new Map(["SIGTERM", "SIGINT"].map((name) => [name, new Set(process.listeners(name))]));
  const host = runResidentHostFromConfigPath(configPath, shutdown.signal, registry);
  await waitFor(() => fs.existsSync(path.join(residencyRoot, "owner.json")));
  const client = new ResidencyClient({
    config, mesh, participants, commandTimeoutMs,
    mainAgent: { id: rootId, local: true, matches: (id) => id === rootId, info: () => { throw new Error("unused"); },
      deliverAgent: () => ({ queued: true, messageId: "unused", routed: "main" }) },
  });
  const nested = new ResidentActorClient(meshRoot, rootId, 500);
  const model = beforeCommit ? "test/slow" : "test/visible";
  return {
    root, residencyRoot, config, participants, client, nested, entered, release, model,
    close: async () => {
      release.resolve();
      shutdown.abort();
      await host;
      await client.close();
      await participants.close();
      for (const [name, previous] of signalListeners) {
        for (const listener of process.listeners(name)) if (!previous.has(listener)) process.removeListener(name, listener);
      }
      fs.rmSync(root, { recursive: true, force: true });
    },
  };
};

const kinds = ["main spawn", "main create", "nested create"] as const;
const endings = ["timeout", "abort"] as const;
const send = (state: Awaited<ReturnType<typeof harness>>, kind: typeof kinds[number], signal: AbortSignal) => {
  const actor = { name: "fenced", instructions: "do not become a double writer", residency: "durable" as const, model: state.model };
  return kind === "main spawn"
    ? state.client.spawnAgent({ task: "no silent late launch", residency: "durable", model: state.model }, signal)
    : kind === "main create" ? state.client.createActor(actor, signal) : state.nested.createActor(actor, signal);
};

describe("resident commit vs abandonment: real client -> pickup -> preparation -> mutation", () => {
  for (const kind of kinds) for (const ending of endings) {
    it(`${kind} ${ending} before commit tombstones picked-up work; no actor/worker ever starts`, { timeout: 40_000 }, async () => {
      const state = await harness(true);
      const controller = new AbortController();
      try {
        const outcome = send(state, kind, controller.signal).catch((error: Error) => error);
        await state.entered.promise; // Real model-refresh await, AFTER the host renamed to processing/.
        const processing = entries(state.residencyRoot, "processing");
        expect(processing).toHaveLength(1);
        const requestId = processing[0]!.slice(0, -5);
        if (ending === "abort") controller.abort();
        const error = await outcome;
        state.release.resolve();
        await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).message).toMatch(ending === "abort" ? /aborted/ : /Timed out/);
        expect(entries(state.residencyRoot, "agents")).toEqual([]);
        expect(names(path.join(state.residencyRoot, "runs"))).toEqual([]);
        expect(state.participants.list({ scope: "lineage" }).filter((p) => p.kind === "actor" || p.kind === "agent")).toEqual([]);
        expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
        expect(new ActorRegistryStore(state.config.sessionActorRoot!).records()).toEqual([]);
        expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", `${requestId}.json`), "utf8")))
          .toMatchObject({ requestId, state: "abandoned" });
        expect(entries(state.residencyRoot, "requests")).toEqual([]);
        expect(entries(state.residencyRoot, "responses")).toEqual([]);
      } finally { await state.close(); }
    });

    it(`${kind} ${ending} after commit reports unknown with known ID; late registration is owned exactly once`, { timeout: 40_000 }, async () => {
      const state = await harness(false);
      const controller = new AbortController();
      let knownId = "";
      let creations = 0;
      if (kind === "main spawn") {
        const original = AgentManager.prototype.spawn;
        vi.spyOn(AgentManager.prototype, "spawn").mockImplementation(async function (this: AgentManager, ...args) {
          const handle = await original.apply(this, args); // Real fence, files, transport and manager registration.
          knownId = handle.id; creations++;
          state.entered.resolve(); await state.release.promise;
          return handle;
        });
      } else {
        const original = ActorDirectory.prototype.create;
        vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
          const actor = await original.apply(this, args); // Real fence, registry and owned presence.
          knownId = actor.id; creations++;
          state.entered.resolve(); await state.release.promise;
          return actor;
        });
      }
      try {
        const outcome = send(state, kind, controller.signal).catch((error: Error) => error);
        await state.entered.promise;
        const processing = entries(state.residencyRoot, "processing");
        const requestId = processing[0]!.slice(0, -5);
        const originalCommand = fs.readFileSync(path.join(state.residencyRoot, "processing", processing[0]!), "utf8");
        if (ending === "abort") controller.abort();
        const error = await outcome;
        state.release.resolve();
        await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", requestId, id: knownId,
          operation: kind === "main spawn" ? "spawn" : "createActor", ownerHostId: residentHostId(state.config.rootId) });
        expect((error as Error).message).toContain(knownId);
        expect((error as Error).message).toMatch(/Do not retry or reassign.*status|actorStatus/);
        expect((error as Error).message).toContain("agents.stop");
        expect(creations).toBe(1);
        await waitFor(() => state.participants.get(knownId)?.ownerHostId === residentHostId(state.config.rootId));
        expect(state.participants.get(knownId)).toMatchObject({ id: knownId, residency: "durable", stale: false });
        if (kind === "main spawn") {
          expect(state.client.listAgents()).toHaveLength(1);
          expect(state.client.statusAgent(knownId)).toMatchObject({ id: knownId, residency: "durable" });
        }
        expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", `${requestId}.json`), "utf8")))
          .toMatchObject({ state: "committed", id: knownId });
        const responsePath = path.join(state.residencyRoot, "responses", `${requestId}.json`);
        expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: true, requestId });
        // Replay the actual exchange envelope: the immutable commit fence rejects a second mutation.
        fs.rmSync(responsePath);
        fs.writeFileSync(path.join(state.residencyRoot, "requests", `${requestId}.json`), originalCommand);
        await waitFor(() => fs.existsSync(responsePath));
        expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: false, requestId });
        expect(creations).toBe(1);
        if (kind === "main spawn") expect(state.client.listAgents()).toHaveLength(1);
        else expect(new ActorRegistryStore(state.config.actorRoot).records().length +
          new ActorRegistryStore(state.config.sessionActorRoot!).records().length).toBe(1);
      } finally { await state.close(); }
    });
  }

  it("cleanup that times out behind a real wait is fenced before file/worktree mutation", { timeout: 10_000 }, async () => {
    const state = await harness(false);
    const original = AgentManager.prototype.join;
    const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
    try {
      const handle = await state.client.spawnAgent({ task: "settle before cleanup", model: state.model });
      vi.spyOn(AgentManager.prototype, "join").mockImplementation(async function (this: AgentManager, ...args) {
        const result = await original.apply(this, args);
        state.entered.resolve(); await state.release.promise;
        return result;
      });
      const outcome = state.client.cleanupAgent(handle.id).catch((error: Error) => error);
      await state.entered.promise;
      const error = await outcome;
      state.release.resolve();
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect((error as Error).message).toContain("Timed out");
      expect(cleanup).not.toHaveBeenCalled();
      expect(state.client.hasAgent(handle.id)).toBe(true);
      expect(state.client.statusAgent(handle.id).id).toBe(handle.id);
      // Local read-only status/list do not create decision records or become unknown outcomes.
      const decisions = entries(state.residencyRoot, "decisions");
      state.client.statusAgent(handle.id); state.client.listAgents();
      expect(entries(state.residencyRoot, "decisions")).toEqual(decisions);
    } finally { await state.close(); }
  });

  it.each(["removeActor", "foreground", "cleanup"] as const)("host refuses a picked-up, already-abandoned %s envelope", async (operation) => {
    const state = await harness(false);
    const mutator = operation === "removeActor" ? vi.spyOn(ActorDirectory.prototype, "remove")
      : operation === "foreground" ? vi.spyOn(AgentManager.prototype, "markForeground")
        : vi.spyOn(AgentManager.prototype, "cleanup");
    try {
      const requestId = "abandoned-mutator";
      abandonResidentRequest(path.join(state.residencyRoot, "requests"), path.join(state.residencyRoot, "responses"), requestId);
      fs.writeFileSync(path.join(state.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
        format: 1, rootId: state.config.rootId, requestId, operation, id: "known", deleteBranch: false, createdAt: Date.now(),
      }));
      await waitFor(() => entries(state.residencyRoot, "requests").length === 0 && entries(state.residencyRoot, "processing").length === 0);
      expect(mutator).not.toHaveBeenCalled();
      expect(entries(state.residencyRoot, "responses")).toEqual([]);
    } finally { await state.close(); }
  });

  it("restart cleans abandoned processing without replay and retains committed IDs for unknown recovery", async () => {
    const state = await harness(false, (config) => {
      for (const directory of ["decisions", "processing"]) fs.mkdirSync(path.join(config.residencyRoot, directory));
      for (const status of ["abandoned", "committed"]) {
        const requestId = `restart-${status}`;
        fs.writeFileSync(path.join(config.residencyRoot, "decisions", `${requestId}.json`), JSON.stringify({
          requestId, state: status, ...(status === "committed" ? { id: "known", operation: "spawn", ownerHostId: residentHostId(config.rootId) } : {}),
        }));
        fs.writeFileSync(path.join(config.residencyRoot, "processing", `${requestId}.json`), JSON.stringify({
          format: 1, requestId, rootId: config.rootId, operation: "spawn", request: { task: "never replay" }, createdAt: 1,
        }));
      }
    });
    try {
      expect(entries(state.residencyRoot, "processing")).toEqual([]);
      expect(entries(state.residencyRoot, "responses")).toEqual(["restart-committed.json"]);
      expect(entries(state.residencyRoot, "agents")).toEqual([]);
      expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", "restart-committed.json"), "utf8")))
        .toMatchObject({ state: "committed", id: "known" });
    } finally { await state.close(); }
  });

  it("abandoned successor create does not remove its stopped predecessor before the fence", async () => {
    const state = await harness(false);
    let owner!: ActorDirectory;
    const original = ActorDirectory.prototype.create;
    vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
      owner = this;
      return original.apply(this, args);
    });
    try {
      const predecessor = await state.nested.createActor({ name: "fenced", instructions: "predecessor", residency: "durable", model: state.model });
      await owner.stop(predecessor.id);
      const outcome = state.nested.createActor({ name: "fenced", instructions: "successor", residency: "durable", model: "test/slow" })
        .catch((error: Error) => error);
      await state.entered.promise;
      const error = await outcome;
      state.release.resolve();
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect((error as Error).message).toContain("Timed out");
      expect(owner.list()).toMatchObject([{ id: predecessor.id, status: "stopped" }]);
    } finally { await state.close(); }
  });

  it("never dispatches mutations to an already-running pre-fence host", async () => {
    const state = await harness(false);
    try {
      const ownerPath = path.join(state.residencyRoot, "owner.json");
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      delete owner.requestFence;
      fs.writeFileSync(ownerPath, JSON.stringify(owner));
      await expect(state.client.spawnAgent({ task: "must not dispatch" })).rejects.toThrow(/lacks the abandonment fence.*No request was dispatched/);
      await expect(state.nested.createActor({ name: "legacy", instructions: "must not dispatch", residency: "durable" }))
        .rejects.toThrow(/lacks the abandonment fence.*No request was dispatched/);
      expect(entries(state.residencyRoot, "requests")).toEqual([]);
      expect(entries(state.residencyRoot, "decisions")).toEqual([]);
    } finally { await state.close(); }
  });

  it.each(["main spawn", "nested create"] as const)("%s reports unknown rather than rejection if atomic abandonment cannot be established", async (kind) => {
    const state = await harness(false);
    vi.spyOn(fs, "linkSync").mockImplementation(() => { throw Object.assign(new Error("fence unavailable"), { code: "EPERM" }); });
    try {
      const error = await send(state, kind, new AbortController().signal).catch((error: Error) => error);
      expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", requestId: expect.any(String) });
      expect((error as Error).message).toContain("fence unavailable");
      expect(entries(state.residencyRoot, "agents")).toEqual([]);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
    } finally { vi.restoreAllMocks(); await state.close(); }
  });

  it.each(["main spawn", "nested create"] as const)("%s fences a request publication that succeeds before reporting a write error", async (kind) => {
    const state = await harness(false);
    const rename = fs.renameSync;
    vi.spyOn(fs, "renameSync").mockImplementation((source, target) => {
      rename(source, target);
      if (path.dirname(String(target)) === path.join(state.residencyRoot, "requests")) throw new Error("publication succeeded but write reported failure");
    });
    try {
      const error = await send(state, kind, new AbortController().signal).catch((error: Error) => error);
      expect((error as Error).message).toContain("publication succeeded but write reported failure");
      const decisions = entries(state.residencyRoot, "decisions");
      expect(decisions).toHaveLength(1);
      expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", decisions[0]!), "utf8"))).toMatchObject({ state: "abandoned" });
      expect(entries(state.residencyRoot, "requests")).toEqual([]);
      expect(entries(state.residencyRoot, "agents")).toEqual([]);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
    } finally { vi.restoreAllMocks(); await state.close(); }
  });

  it.each(["main spawn", "main create"] as const)("%s aborted during participant publication retains the confirmed ID", async (kind) => {
    const state = await harness(false);
    const original = state.participants.get.bind(state.participants);
    const get = vi.spyOn(state.participants, "get").mockImplementation((id) => id === state.config.rootId ? original(id) : undefined);
    const controller = new AbortController();
    try {
      const outcome = send(state, kind, controller.signal).catch((error: Error) => error);
      await waitFor(() => entries(state.residencyRoot, "decisions").length === 1 &&
        entries(state.residencyRoot, "processing").length === 0 && entries(state.residencyRoot, "responses").length === 0);
      controller.abort();
      const error = await outcome;
      expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", requestId: expect.any(String), id: expect.stringMatching(/^[0-9a-f]{32}$/) });
      get.mockRestore();
      const id = (error as Error & { id: string }).id;
      await waitFor(() => state.participants.get(id)?.ownerHostId === residentHostId(state.config.rootId));
    } finally { await state.close(); }
  });

  it("a committed cleanup failure remains unknown and cannot fall through to offline cleanup", async () => {
    const state = await harness(false);
    try {
      const handle = await state.client.spawnAgent({ task: "settle before cleanup", model: state.model });
      vi.spyOn(AgentManager.prototype, "cleanup").mockRejectedValue(new Error("Unknown Fabric agent after cleanup commit"));
      const error = await state.client.cleanupAgent(handle.id).catch((error: Error) => error);
      expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: handle.id, operation: "cleanup" });
      expect(state.client.hasAgent(handle.id)).toBe(true);
    } finally { await state.close(); }
  });

  it.each(["spawn", "create"] as const)("public provider %s abort reaches the real resident-host fence", async (operation) => {
    const state = await harness(true);
    const localAgents = new AgentManager(state.root, state.config.agents, { runRoot: path.join(state.root, "local-runs") });
    const identity = { id: state.config.rootId, name: "main", kind: "main" as const };
    const localActors = new ActorManager("caller", identity, state.client.options.mesh, state.config.mesh, localAgents, () => {});
    const lifecycle = new LifecycleBroker(state.client.options.mesh, identity, state.participants,
      { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(localAgents, localActors, new GlobalActorRegistry(state.root, 64 * 1024),
      state.client.options.mainAgent, state.participants, undefined, lifecycle, undefined,
      state.client, false);
    const controller = new AbortController();
    const context: FabricInvocationContext = {
      cwd: state.root, signal: controller.signal, parentToolCallId: "fence", nestedToolCallId: "fence-nested",
      extensionContext: { modelRegistry: { getAvailable: () => [{ provider: "test", id: "slow" }] } } as unknown as FabricInvocationContext["extensionContext"],
      update: () => {},
    };
    if (operation === "create") {
      vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", state.config.rootId);
      vi.stubEnv("PI_FABRIC_MESH_ROOT", state.config.meshRoot);
    }
    try {
      const outcome = provider.invoke(operation, { residency: "durable", model: state.model,
        ...(operation === "spawn" ? { task: "must not launch" } : { name: "provider-fenced", instructions: "must not register" }) }, context)
        .catch((error: Error) => error);
      await state.entered.promise;
      controller.abort();
      const error = await outcome;
      state.release.resolve();
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect((error as Error).message).toContain("aborted");
      expect(entries(state.residencyRoot, "agents")).toEqual([]);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
      expect(entries(state.residencyRoot, "decisions")).toHaveLength(1);
    } finally {
      state.release.resolve();
      await provider.close(); await localActors.close(); await localAgents.close(); await lifecycle.close();
      await state.close();
    }
  });
});

const mainProvider = (state: Awaited<ReturnType<typeof harness>>) => {
  const manager = new AgentManager(state.root, state.config.agents, { runRoot: path.join(state.root, "local-runs") });
  const identity = { id: state.config.rootId, name: "main", kind: "main" as const };
  const actors = new ActorDirectory(["caller", identity, state.client.options.mesh, state.config.mesh, manager, () => {}, {
    persistent: true, rootId: state.config.rootId, claimResidency: "session",
    canManageActor: (id) => {
      const participant = state.participants.get(id);
      return participant ? participant.ownerHostId === state.config.rootId : undefined;
    },
    resolvePiModel: async (model) => {
      if (model === "test/slow") { state.entered.resolve(); await state.release.promise; }
      return model;
    },
  }], { project: state.config.actorRoot, session: state.config.sessionActorRoot! }, "project");
  const control = new FabricControlPlane(state.client.options.mesh, identity, {
    enabled: true, hostId: identity.id, pollMs: 20, acknowledgementTimeoutMs: 3_000,
  });
  control.start(() => ({ accepted: false }));
  const lifecycle = new LifecycleBroker(state.client.options.mesh, identity, state.participants,
    { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
  const global = new GlobalActorRegistry(state.root, 64 * 1024);
  const provider = new AgentsProvider(manager, actors, global, state.client.options.mainAgent,
    state.participants, control, lifecycle, undefined, state.client, false);
  const registry = new ActionRegistry();
  registry.register(provider);
  const context: FabricInvocationContext = {
    cwd: state.root, signal: undefined, parentToolCallId: "round1", nestedToolCallId: "round1-nested", update() {},
    extensionContext: { modelRegistry: { getAvailable: () => [{ provider: "test", id: "slow" }, { provider: "test", id: "visible" }] } } as unknown as FabricInvocationContext["extensionContext"],
  };
  return { provider, registry, actors, global, context,
    invoke: (ref: string, args: Record<string, unknown>, signal?: AbortSignal) => registry.invoke(ref, args,
      { ...context, signal, audits: [], maxResultChars: 100_000, approve: async () => {} }),
    close: async () => { await registry.close(); await control.close(); await actors.close(); await manager.close(); await lifecycle.close(); },
  };
};

const requestArgs = (state: Awaited<ReturnType<typeof harness>>, operation: "spawn" | "create") => ({
  residency: "durable", model: state.model,
  ...(operation === "spawn" ? { task: "round1 never silently reassign" } : { name: "round1", instructions: "round1 never silently reassign" }),
});

describe("round 1 public cancellation contract", () => {
  it("QuickJS teardown surfaces a committed unawaited resident call instead of successful guest output", async () => {
    const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
    const original = ActorDirectory.prototype.create;
    vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
      const actor = await original.apply(this, args); state.entered.resolve(); await state.release.promise; return actor;
    });
    try {
      const outcome = new QuickJsRuntime().execute('void agents.create(JSON.parse(π.args)); await new Promise(resolve => setTimeout(resolve, 500)); return "guest ended";',
        (ref, args, signal) => main.invoke(ref, args, signal), {
          timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024, strings: { args: JSON.stringify(requestArgs(state, "create")) },
        });
      await state.entered.promise;
      const result = await outcome;
      expect(result.terminationReason).toBe("runtime_error"); expect(result.value).toBeUndefined();
      const file = entries(state.residencyRoot, "decisions")[0]!;
      const decision = JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", file), "utf8"));
      expect(result.error).toContain("ResidentOutcomeUnknownError");
      expect(result.error).toContain(decision.requestId); expect(result.error).toContain(decision.id);
      state.release.resolve(); await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
    } finally { state.release.resolve(); await main.close(); await state.close(); }
  });

  it("QuickJS outer cancellation preserves all committed IDs, including an earlier successful call", async () => {
    const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
    const controller = new AbortController(); const original = ActorDirectory.prototype.create; let created = 0;
    vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
      const actor = await original.apply(this, args);
      if (++created === 2) { state.entered.resolve(); await state.release.promise; }
      return actor;
    });
    try {
      const outcome = new QuickJsRuntime().execute('const args = JSON.parse(π.args); await agents.create({...args, name:"receipt-one"}); return await agents.create({...args, name:"receipt-two"});',
        (ref, args, signal) => main.invoke(ref, args, signal), {
          timeoutMs: 5_000, memoryLimitBytes: 32 * 1024 * 1024, signal: controller.signal,
          strings: { args: JSON.stringify(requestArgs(state, "create")) },
        });
      await state.entered.promise; controller.abort(); const result = await outcome;
      expect(result.terminationReason).toBe("aborted"); expect(result.error).toContain("ResidentOutcomeUnknownError");
      const decisions = entries(state.residencyRoot, "decisions").map(file => JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", file), "utf8")));
      expect(decisions).toHaveLength(2);
      for (const decision of decisions) {
        expect(decision.state).toBe("committed"); expect(result.error).toContain(decision.requestId); expect(result.error).toContain(decision.id);
      }
      state.release.resolve(); await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect(created).toBe(2); expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(2);
    } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
  });

  it("Main publication failure preserves committed actor uncertainty without compensation or reclaim", async () => {
    const state = await harness(false, undefined, 10_000);
    const main = mainProvider(state);
    const cede = vi.spyOn(main.actors, "cede");
    const reclaim = vi.spyOn(main.actors, "reclaim");
    const remove = vi.spyOn(state.client, "removeActor");
    const original = ActorDirectory.prototype.create;
    const publicationFailure = new Error("injected publication failure after resident commit");
    vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
      const actor = await original.apply(this, args);
      if (this !== main.actors) throw publicationFailure;
      return actor;
    });
    try {
      const error = await main.invoke("agents.create", requestArgs(state, "create")).catch((error: Error) => error);
      expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", operation: "createActor" });
      expect((error as Error).message).toContain(publicationFailure.message);
      const decisions = entries(state.residencyRoot, "decisions");
      expect(decisions).toHaveLength(1);
      const decision = JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", decisions[0]!), "utf8"));
      expect(decision).toMatchObject({ state: "committed", id: (error as { id: string }).id });
      expect((error as Error).message).toContain(decision.requestId);
      expect((error as Error).message).toContain(decision.id);
      expect(cede).not.toHaveBeenCalled(); expect(remove).not.toHaveBeenCalled(); expect(reclaim).not.toHaveBeenCalled();
      await waitFor(() => state.participants.get(decision.id)?.ownerHostId === residentHostId(state.config.rootId));
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
      expect(await main.invoke("agents.actorStatus", { id: decision.id })).toMatchObject({ id: decision.id });
      await main.invoke("agents.stop", { id: decision.id });
    } finally { await main.close(); await state.close(); }
  });

  it("abandoned public cleanup preserves real worker background completion exactly once", { timeout: 15_000 }, async () => {
    const state = await harness(false, undefined, 10_000);
    const main = mainProvider(state);
    const controller = new AbortController();
    const delivered = vi.spyOn(state.client.options.mainAgent, "deliverAgent");
    const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
    state.client.start();
    try {
      const handle = await state.client.spawnAgent({ task: "LIVE_WITH_PROGRESS cleanup notification", model: state.model });
      const outcome = main.invoke("agents.cleanup", { id: handle.id }, controller.signal).catch((error: Error) => error);
      await waitFor(() => entries(state.residencyRoot, "processing").length > 0);
      const requestId = entries(state.residencyRoot, "processing")[0]!.slice(0, -5);
      controller.abort(); expect(await outcome).toBeInstanceOf(Error);
      expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", `${requestId}.json`), "utf8")))
        .toMatchObject({ state: "abandoned" });
      await waitFor(() => state.client.settledAgent(handle.id) !== undefined);
      await waitFor(() => delivered.mock.calls.some(([delivery]) => (delivery.data as { id?: string })?.id === handle.id));
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      await delay(100);
      expect(delivered.mock.calls.filter(([delivery]) => (delivery.data as { id?: string })?.id === handle.id)).toHaveLength(1);
      expect(cleanup).not.toHaveBeenCalled(); expect(state.client.hasAgent(handle.id)).toBe(true);
    } finally { controller.abort(); await main.close(); await state.close(); }
  });

  it("abandoned cleanup join preserves completion even when the client timeout writes the fence", { timeout: 15_000 }, async () => {
    const state = await harness(false, undefined, 200);
    const delivered = vi.spyOn(state.client.options.mainAgent, "deliverAgent");
    const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
    state.client.start();
    try {
      const handle = await state.client.spawnAgent({ task: "LIVE_WITH_PROGRESS join-only completion", model: state.model });
      const outcome = state.client.cleanupAgent(handle.id).catch((error: Error) => error);
      await waitFor(() => entries(state.residencyRoot, "processing").length > 0);
      const requestId = entries(state.residencyRoot, "processing")[0]!.slice(0, -5);
      expect((await outcome as Error).message).toContain("Timed out");
      expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", `${requestId}.json`), "utf8")))
        .toMatchObject({ state: "abandoned" });
      await waitFor(() => state.client.settledAgent(handle.id) !== undefined);
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      // Delivery polling is independent of host settlement. This bound also lets
      // the unsafe base complete normally, so the assertion exposes lost delivery.
      await delay(1_000);
      expect(delivered.mock.calls.filter(([delivery]) => (delivery.data as { id?: string })?.id === handle.id)).toHaveLength(1);
      expect(cleanup).not.toHaveBeenCalled(); expect(state.client.hasAgent(handle.id)).toBe(true);
    } finally { await state.close(); }
  });

  it("durable create never enters activation compensation when committed removal would be unknown", async () => {
    const state = await harness(false, undefined, 200); const main = mainProvider(state);
    const activationFailure = new Error("injected activation failure");
    const ensure = vi.spyOn(state.client, "ensureActor").mockImplementation(async (id) => {
      await waitFor(() => state.participants.get(id)?.ownerHostId === residentHostId(state.config.rootId));
      throw activationFailure;
    });
    let removalError: unknown;
    const originalClientRemove = state.client.removeActor.bind(state.client);
    const remove = vi.spyOn(state.client, "removeActor").mockImplementation(async (id) => {
      try { return await originalClientRemove(id); }
      catch (error) { removalError = error; throw error; }
    });
    const reclaim = vi.spyOn(main.actors, "reclaim");
    // The old public path compensates by asking the real host to remove. Pause
    // that handler after its committed fence; timing out is not safe to reclaim.
    const originalRemove = ActorDirectory.prototype.remove;
    vi.spyOn(ActorDirectory.prototype, "remove").mockImplementation(async function (this: ActorDirectory, ...args) {
      if (this !== main.actors) await state.release.promise;
      return originalRemove.apply(this, args);
    });
    try {
      const result = await main.invoke("agents.create", requestArgs(state, "create")).catch((error: Error) => error);
      if (remove.mock.calls.length > 0) {
        // Bind the base failure to the exact adverse condition, not merely an
        // ordinary ownership refusal: removal committed, IDs reached the client,
        // then the old provider concealed them and reclaimed anyway.
        const removed = entries(state.residencyRoot, "decisions").map(file => JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", file), "utf8")))
          .find(decision => decision.operation === "removeActor");
        expect(removed).toMatchObject({ state: "committed" });
        expect(removalError).toMatchObject({ name: "ResidentOutcomeUnknownError", requestId: removed.requestId, id: removed.id });
        expect(result).toBe(activationFailure);
      }
      expect(reclaim).not.toHaveBeenCalled(); expect(remove).not.toHaveBeenCalled(); expect(ensure).not.toHaveBeenCalled();
      expect(result).toMatchObject({ residency: "durable" });
      const decisions = entries(state.residencyRoot, "decisions").map(file => JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", file), "utf8")));
      expect(decisions).toEqual([expect.objectContaining({ operation: "createActor", state: "committed", id: (result as { id: string }).id })]);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
    } finally { state.release.resolve(); await main.close(); await state.close(); }
  });

  it("durable host still validates capability requirements before its fence", async () => {
    const state = await harness(false);
    const main = mainProvider(state);
    try {
      await expect(main.invoke("agents.create", { ...requestArgs(state, "create"), requires: ["memory.get"] }))
        .rejects.toThrow("cannot commit actor capability requirements");
      expect(entries(state.residencyRoot, "decisions").map((file) => JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", file), "utf8"))))
        .not.toContainEqual(expect.objectContaining({ state: "committed" }));
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
    } finally { await main.close(); await state.close(); }
  });
  it("Main cancellation during empty-registry preparation cannot publish a late actor after rejection", async () => {
    const state = await harness(true, undefined, 10_000); const main = mainProvider(state);
    const controller = new AbortController();
    try {
      const outcome = main.invoke("agents.create", requestArgs(state, "create"), controller.signal).catch((error: Error) => error);
      await state.entered.promise; controller.abort(); expect(await outcome).toBeInstanceOf(Error);
      state.release.resolve(); await delay(200);
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
      expect(new ActorRegistryStore(state.config.sessionActorRoot!).records()).toEqual([]);
      expect(entries(state.residencyRoot, "decisions")).toHaveLength(1);
      expect(entries(state.residencyRoot, "agents")).toEqual([]);
    } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
  });
  it("Main empty-registry create is exclusively fenced before preparation; import uses the same path", async () => {
    const state = await harness(true, undefined, 10_000);
    const main = mainProvider(state);
    const localCreate = vi.spyOn(main.actors, "create");
    const controller = new AbortController();
    try {
      let finished = false;
      const outcome = main.invoke("agents.create", requestArgs(state, "create"), controller.signal)
        .catch((error: Error) => error).finally(() => { finished = true; });
      await waitFor(() => finished || entries(state.residencyRoot, "processing").length > 0);
      expect(localCreate).not.toHaveBeenCalled();
      await state.entered.promise;
      controller.abort();
      expect((await outcome as Error).message).toMatch(/abort/i);
      const requestId = entries(state.residencyRoot, "decisions")[0]!;
      expect(JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", requestId), "utf8"))).toMatchObject({ state: "abandoned" });
      state.release.resolve();
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
      expect(entries(state.residencyRoot, "agents")).toEqual([]);
      const template = main.global.create({ name: "import-round1", instructions: "no local durable import", model: "test/visible", residency: "durable" });
      const imported = await main.invoke("agents.import", { id: template.id });
      expect(imported).toMatchObject({ residency: "durable" });
      expect(localCreate).not.toHaveBeenCalled();
    } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
  });

  it("Main empty-registry create refuses a legacy host before any local or remote mutation", async () => {
    const state = await harness(false);
    const main = mainProvider(state);
    const localCreate = vi.spyOn(main.actors, "create");
    try {
      const ownerPath = path.join(state.residencyRoot, "owner.json");
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
      delete owner.requestFence; fs.writeFileSync(ownerPath, JSON.stringify(owner));
      await expect(main.invoke("agents.create", requestArgs(state, "create"))).rejects.toThrow(/lacks the abandonment fence.*No request was dispatched/);
      expect(localCreate).not.toHaveBeenCalled();
      expect(entries(state.residencyRoot, "decisions")).toEqual([]);
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
    } finally { await main.close(); await state.close(); }
  });

  for (const operation of ["spawn", "create"] as const) for (const ending of ["abort", "deadline"] as const) for (const before of [true, false]) {
    it(`QuickJS -> registry -> Main ${operation} ${ending} ${before ? "before" : "after"} commit retains the fence outcome`, { timeout: 20_000 }, async () => {
      const state = await harness(before, undefined, 10_000);
      const main = mainProvider(state);
      const controller = new AbortController();
      let knownId = "";
      let creations = 0;
      if (!before) {
        if (operation === "spawn") {
          const original = AgentManager.prototype.spawn;
          vi.spyOn(AgentManager.prototype, "spawn").mockImplementation(async function (this: AgentManager, ...args) {
            const handle = await original.apply(this, args); knownId = handle.id; creations++;
            state.entered.resolve(); await state.release.promise; return handle;
          });
        } else {
          const original = ActorDirectory.prototype.create;
          vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
            const actor = await original.apply(this, args); knownId = actor.id; creations++;
            state.entered.resolve(); await state.release.promise; return actor;
          });
        }
      }
      try {
        const outcome = new QuickJsRuntime().execute(`return await agents.${operation}(JSON.parse(π.args));`,
          (ref, args, signal) => main.invoke(ref, args, signal), {
            timeoutMs: 1_000, memoryLimitBytes: 32 * 1024 * 1024, signal: controller.signal,
            strings: { args: JSON.stringify(requestArgs(state, operation)) },
          });
        // π values are strings: use JSON.parse explicitly, as in public programs.
        await state.entered.promise;
        const requestId = entries(state.residencyRoot, "processing")[0]!.slice(0, -5);
        if (ending === "abort") controller.abort();
        const result = await outcome;
        expect(result.terminationReason).toBe(ending === "abort" ? "aborted" : "timed_out");
        const decision = JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", `${requestId}.json`), "utf8"));
        expect(decision.state).toBe(before ? "abandoned" : "committed");
        if (!before) {
          expect(result.error).toContain("ResidentOutcomeUnknownError");
          expect(result.error).toContain(requestId); expect(result.error).toContain(knownId);
          expect(result.error).toContain("Do not retry or reassign"); expect(result.error).toContain("agents.stop");
        }
        state.release.resolve();
        await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        if (before) {
          expect(creations).toBe(0); expect(names(path.join(state.residencyRoot, "runs"))).toEqual([]);
          expect(entries(state.residencyRoot, "agents")).toEqual([]);
          expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
        } else {
          expect(creations).toBe(1);
          await waitFor(() => state.participants.get(knownId)?.ownerHostId === residentHostId(state.config.rootId));
          expect(state.participants.list({ scope: "lineage" }).filter((p) => p.id === knownId)).toHaveLength(1);
          expect(await main.invoke(operation === "spawn" ? "agents.status" : "agents.actorStatus", { id: knownId })).toMatchObject({ id: knownId });
          await main.invoke("agents.stop", { id: knownId });
        }
      } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
    });
  }

  it("registry public cleanup abort fences a real host wait before destructive mutation", async () => {
    const state = await harness(false, undefined, 10_000);
    const main = mainProvider(state);
    const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
    const original = AgentManager.prototype.join;
    const controller = new AbortController();
    try {
      const handle = await state.client.spawnAgent({ task: "cleanup round1", model: state.model });
      vi.spyOn(AgentManager.prototype, "join").mockImplementation(async function (this: AgentManager, ...args) {
        const result = await original.apply(this, args); state.entered.resolve(); await state.release.promise; return result;
      });
      const outcome = main.invoke("agents.cleanup", { id: handle.id }, controller.signal).catch((error: Error) => error);
      await state.entered.promise; controller.abort();
      expect(await outcome).toBeInstanceOf(Error);
      const requestId = entries(state.residencyRoot, "processing")[0]!.slice(0, -5);
      const decisionPath = path.join(state.residencyRoot, "decisions", `${requestId}.json`);
      expect(fs.existsSync(decisionPath)).toBe(true);
      expect(JSON.parse(fs.readFileSync(decisionPath, "utf8"))).toMatchObject({ state: "abandoned" });
      state.release.resolve(); await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      expect(cleanup).not.toHaveBeenCalled(); expect(state.client.hasAgent(handle.id)).toBe(true);
    } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
  });
});
