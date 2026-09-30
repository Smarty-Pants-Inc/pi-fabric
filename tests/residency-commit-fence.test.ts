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
const harness = async (beforeCommit: boolean, seed?: (config: ResidentHostConfig) => void) => {
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
    config, mesh, participants, commandTimeoutMs: 500,
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
    const original = AgentManager.prototype.wait;
    const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
    try {
      const handle = await state.client.spawnAgent({ task: "settle before cleanup", model: state.model });
      vi.spyOn(AgentManager.prototype, "wait").mockImplementation(async function (this: AgentManager, ...args) {
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
      operation === "spawn" ? state.client : undefined, false);
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
