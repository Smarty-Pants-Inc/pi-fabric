import * as childProcess from "node:child_process";
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
import { CPythonRuntime } from "../src/runtime/cpython-runtime.js";
import { MontyRuntime } from "../src/runtime/monty-runtime.js";
import { NodeProcessRuntime } from "../src/runtime/node-process-runtime.js";
import type { FabricHostCall, FabricSandboxOptions } from "../src/runtime/kernel.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { FabricExecutionService } from "../src/execution-service.js";
import type { ExtensionAPI, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { FabricState } from "../src/fabric-state.js";
import piFabric from "../src/index.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { ResidencyClient } from "../src/residency/client.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { abandonResidentRequest, residentHostId, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";

import { captureDurableExecutionTrace } from "./helpers/durable-execution-trace.js";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, type AgentSession } from "@earendil-works/pi-coding-agent";
import { PiToolsProvider } from "../src/providers/pi-tools-provider.js";
import { claimFabricHandoff, runFabricHandoffAtBoundary } from "../src/prewalk/handoff.js";

vi.mock("node:child_process", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:child_process")>();
  return { ...actual, spawn: vi.fn(actual.spawn) };
});

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
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); vi.mocked(childProcess.spawn).mockReset(); });

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
      // Host shutdown confirms worker exit, and CPython controls also await guest
      // close. Windows can still transiently retain a cwd/directory in the OS;
      // opt into Node's bounded recursive-rm retry rather than masking EBUSY.
      fs.rmSync(root, { recursive: true, force: true, maxRetries: 5, retryDelay: 25 });
    },
  };
};

describe("resident fence harness teardown", () => {
  it("retries a transient Windows EBUSY after the resident host has closed", async () => {
    const state = await harness(false);
    const rm = fs.rmSync.bind(fs);
    let attempts = 0;
    let cleanupOptions: fs.RmOptions | undefined;
    const busy = Object.assign(new Error("Windows still holds the removed cwd"), { code: "EBUSY" });
    const cleanup = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) !== state.root) return rm(target, options);
      cleanupOptions = options;
      expect(fs.existsSync(path.join(state.residencyRoot, "owner.json"))).toBe(false);
      // Model Node's documented recursive rm retry contract on Linux: the first
      // rmdir is busy, then the OS releases it. Native Windows exercises the real
      // implementation; maxRetries defaults to zero without the harness opt-in.
      for (let retry = 0; ; retry++) {
        attempts++;
        if (attempts > 1) return rm(target, options);
        if (retry >= (options?.maxRetries ?? 0)) throw busy;
      }
    });
    try {
      await expect(state.close()).resolves.toBeUndefined();
      expect(attempts).toBe(2);
      expect(cleanupOptions).toMatchObject({ recursive: true, force: true, maxRetries: 5, retryDelay: 25 });
      expect(fs.existsSync(state.root)).toBe(false);
    } finally {
      cleanup.mockRestore();
      rm(state.root, { recursive: true, force: true });
    }
  });
});

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

const mainProvider = (state: Awaited<ReturnType<typeof harness>>, caller: "main" | "nested" = "main") => {
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
    state.participants, control, lifecycle, undefined, caller === "main" ? state.client : undefined, false);
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

const engines = ["quickjs", "cpython", "monty", "node", "bun"] as const;
type ReceiptEngine = typeof engines[number];
const publicExecution = (state: Awaited<ReturnType<typeof harness>>, main: ReturnType<typeof mainProvider>, engine: ReceiptEngine, timeoutMs = 1_500, interactiveMain = false) => {
  const python = engine === "cpython" || engine === "monty";
  const config = normalizeFabricConfig({
    fullCodeMode: true, executor: { kernel: python ? "python" : "typescript", pythonRuntime: python ? engine : "monty",
      runtime: engine === "node" ? "node-process" : engine === "bun" ? "bun-process" : "quickjs", timeoutMs, memoryLimitBytes: 256 * 1024 * 1024 },
    agents: { timeoutMs },
  });
  // Unit-scale Main budgets bypass the production minimum without weakening it.
  if (interactiveMain) config.executor.mainMaxTimeoutMs = timeoutMs;
  const service = new FabricExecutionService(main.registry, config);
  const context = { ...main.context.extensionContext, cwd: state.root, hasUI: false,
    ...(interactiveMain ? { mode: "rpc" } : {}),
    sessionManager: { getSessionId: () => "round2", getSessionFile: () => undefined },
  } as unknown as FabricInvocationContext["extensionContext"];
  let sequence = 0;
  return (code: string, signal?: AbortSignal) => service.execute({ code, signal, context,
    parentToolCallId: `round2-${engine}-${++sequence}`, onPartial() {},
  });
};
const publicCall = (engine: ReceiptEngine, operation: "spawn" | "create", args: Record<string, unknown>) =>
  engine === "cpython" || engine === "monty"
    ? `await agents.${operation}(**${JSON.stringify(args)})`
    : `await agents.${operation}(${JSON.stringify(args)})`;
const decisionsFor = (state: Awaited<ReturnType<typeof harness>>) => entries(state.residencyRoot, "decisions")
  .map(file => JSON.parse(fs.readFileSync(path.join(state.residencyRoot, "decisions", file), "utf8")));
const assertReceipts = (error: string | undefined, decisions: ReturnType<typeof decisionsFor>) => {
  expect(error).toContain("ResidentOutcomeUnknownError");
  expect(error).toContain("Do not retry or reassign");
  for (const decision of decisions) {
    expect(decision.state).toBe("committed");
    for (const field of ["requestId", "id", "ownerHostId"]) expect(error).toContain(decision[field]);
  }
};

// Capture the tool actually registered by src/index.ts, including execute's
// real formatter and production shell decorator. Only unrelated session/bootstrap
// hooks are stubbed; registry, runtime, provider, clients and mutations are real.
const registeredExecution = async (state: Awaited<ReturnType<typeof harness>>, main: ReturnType<typeof mainProvider>, timeoutMs = 5_000, engine: ReceiptEngine = "quickjs", mainTimeoutMs?: number) => {
  const python = engine === "cpython" || engine === "monty";
  const config = normalizeFabricConfig({ fullCodeMode: true,
    executor: { kernel: python ? "python" : "typescript", pythonRuntime: python ? engine : "monty",
      runtime: engine === "node" ? "node-process" : engine === "bun" ? "bun-process" : "quickjs",
      resultFormat: "json", timeoutMs, maxOutputChars: 50_000, memoryLimitBytes: 256 * 1024 * 1024 },
    agents: { timeoutMs },
  });
  if (mainTimeoutMs !== undefined) config.executor.mainMaxTimeoutMs = mainTimeoutMs;
  const execution = new FabricExecutionService(main.registry, config);
  vi.spyOn(FabricState.prototype, "bootstrapped", "get").mockReturnValue(true);
  vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(config);
  vi.spyOn(FabricState.prototype, "execution", "get").mockReturnValue(execution);
  vi.spyOn(FabricState.prototype, "ensure").mockResolvedValue(undefined);
  vi.spyOn(FabricState.prototype, "claimHandoff").mockResolvedValue(undefined);
  const registered = new Map<string, ToolDefinition<any, any, any>>();
  const api = {
    events: { emit: vi.fn(), on: vi.fn(() => () => {}) },
    getActiveTools: vi.fn(() => ["fabric_exec"]), getAllTools: vi.fn(() => []), on: vi.fn(),
    registerCommand: vi.fn(), registerMessageRenderer: vi.fn(), setActiveTools: vi.fn(),
    registerTool: vi.fn((tool: ToolDefinition<any, any, any>) => registered.set(tool.name, tool)),
  };
  await piFabric(api as unknown as ExtensionAPI);
  expect(api.registerTool).toHaveBeenCalledWith(expect.objectContaining({ name: "fabric_exec" }));
  const tool = registered.get("fabric_exec")!;
  expect(tool.name).toBe("fabric_exec");
  const context = { ...main.context.extensionContext, cwd: state.root, hasUI: false,
    sessionManager: { getSessionId: () => "round4", getSessionFile: () => undefined },
    ...(mainTimeoutMs !== undefined ? { mode: "rpc" } : {}),
  } as unknown as FabricInvocationContext["extensionContext"];
  let sequence = 0;
  return async (code: string, signal?: AbortSignal) => {
    const result = await tool.execute(`round4-${++sequence}`, { code }, signal, undefined, context!);
    // Pi's ToolDefinition return type omits the runtime-supported isError flag.
    return result as typeof result & { isError?: boolean };
  };
};
const visibleText = (result: Awaited<ReturnType<Awaited<ReturnType<typeof registeredExecution>>>>) =>
  result.content.filter(block => block.type === "text").map(block => block.text).join("\n");

describe("round 7 resident receipts at actual Pi message_end", { timeout: 30_000 }, () => {
  const cases = [
    ["handled uncertainty", "in-place", true], ["handled uncertainty", "in-place", false],
    ["terminal failure", "in-place", true], ["terminal failure", "in-place", false],
    ["terminal failure", "trajectory", true], ["terminal failure", "explicit", true],
    ["terminal failure", "none", true], ["ordinary success", "in-place", true],
  ] as const;
  it.each(cases)("preserves %s through %s boundary (switch=%s) in persisted and next model context", async (ending, mode, switchSucceeds) => {
    const state = await harness(false, undefined, ending === "handled uncertainty" ? 250 : 10_000);
    const main = mainProvider(state);
    main.registry.register(new PiToolsProvider(state.root));
    const config = normalizeFabricConfig({ fullCodeMode: true,
      executor: { resultFormat: "json", timeoutMs: 8_000, maxOutputChars: 1_400, memoryLimitBytes: 256 * 1024 * 1024 },
      prewalk: { compactOnReturn: false }, entropy: { compile: false } });
    const execution = new FabricExecutionService(main.registry, config);
    const faux = fauxProvider({ provider: "test", models: [{ id: "visible" }, { id: "executor" }] });
    const runtime = await ModelRuntime.create({ modelsPath: null, refreshOnCreate: false, authPath: path.join(state.root, "unused-auth.json") });
    runtime.registerNativeProvider(faux.provider);
    const manager = SessionManager.create(state.root, path.join(state.root, "sessions"));
    let session: AgentSession | undefined;
    let originalToolText = "";
    let nextModelText = "";
    let pendingCount = 0;
    const claimed = vi.spyOn(FabricState.prototype, "claimHandoff").mockImplementation(async function (this: FabricState, result, sessionId, format) {
      if (mode === "none") return undefined;
      const pending = claimFabricHandoff(this.prewalk, result, sessionId, format);
      if (pending?.kind === "prewalk-plan") throw new Error("Plan was not ready");
      if (pending) { pendingCount++; originalToolText = result.error ?? ""; }
      return pending;
    });
    vi.spyOn(FabricState.prototype, "bootstrapped", "get").mockReturnValue(true);
    vi.spyOn(FabricState.prototype, "config", "get").mockReturnValue(config);
    vi.spyOn(FabricState.prototype, "execution", "get").mockReturnValue(execution);
    vi.spyOn(FabricState.prototype, "bootstrap").mockResolvedValue(undefined);
    vi.spyOn(FabricState.prototype, "ensure").mockImplementation(async function (this: FabricState, ctx) {
      if (mode !== "none" && this.prewalk.status().state === "idle") {
        this.prewalk.arm({ model: `${faux.getModel().provider}/executor`, mode: mode === "trajectory" ? "trajectory" : "in-place",
          sessionId: ctx.sessionManager.getSessionId(), requirePlan: true, task: "Reconcile the committed writer; never replace it" });
        expect(this.prewalk.submitPlan(ctx.sessionManager.getSessionId(), {
          outcome: "Keep one committed writer", steps: ["mutate once", "reconcile IDs"], verification: ["status and stop"], risks: "no replacement writer",
        })).toBe(true);
      }
    });
    let boundaryApi: ExtensionAPI;
    vi.spyOn(FabricState.prototype, "runHandoffAtBoundary").mockImplementation(async function (this: FabricState, pending, result, ctx) {
      return runFabricHandoffAtBoundary(this.prewalk, { executeHandoff: async () => ({ completed: switchSucceeds, status: switchSucceeds ? "completed" : "failed", implementation: "boundary prose ".repeat(200) }) },
        { ...boundaryApi, setModel: async model => switchSucceeds && await boundaryApi.setModel(model) }, pending, result, ctx);
    });
    if (ending === "handled uncertainty") {
      const create = ActorDirectory.prototype.create;
      vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
        const actor = await create.apply(this, args); // Commit REAL host ownership, then delay its response beyond the client wait.
        await delay(900); return actor;
      });
    }
    const args = requestArgs(state, "create");
    const mutation = `await pi.write({path:${JSON.stringify(path.join(state.root, "trigger.txt"))},text:"triggered"});`;
    const requests = `await agents.create(${JSON.stringify({ ...args, name: "boundary-one" })});` +
      (ending === "terminal failure" ? `await agents.create(${JSON.stringify({ ...args, name: "boundary-two" })});` : "");
    const body = mutation + (ending === "handled uncertainty" ? `try { ${requests} } catch {} return "guest handled uncertainty";` : requests +
      (mode === "explicit" ? `await agents.handoff({model:${JSON.stringify(`${faux.getModel().provider}/executor`)},task:"reconcile"});` : "") +
      (ending === "terminal failure" ? `throw new Error("terminal boundary cause");` : `return "ordinary handle delivered";`));
    const loader = new DefaultResourceLoader({ cwd: state.root, agentDir: path.join(state.root, "agent"),
      noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
      extensionFactories: [{ name: path.resolve("src/index.ts"), factory: async api => {
        boundaryApi = api;
        await piFabric(api);
        api.on("context", event => {
          const result = event.messages.find(message => message.role === "toolResult" && message.toolName === "fabric_exec");
          if (result?.role === "toolResult") nextModelText = result.content.filter(block => block.type === "text").map(block => block.text).join("\n");
        });
      } }],
    });
    try {
      await loader.reload();
      // Inline factories get a synthetic path; give this registered source its
      // real identity so production tool_result ownership repair also runs.
      loader.getExtensions().extensions[0]!.sourceInfo.path = path.resolve("src/index.ts");
      ({ session } = await createAgentSession({ cwd: state.root, agentDir: path.join(state.root, "agent"), modelRuntime: runtime,
        model: faux.getModel(), resourceLoader: loader, sessionManager: manager, tools: ["fabric_exec"] }));
      await session.bindExtensions({});
      faux.setResponses([fauxAssistantMessage(fauxToolCall("fabric_exec", { code: body }), { stopReason: "toolUse" }),
        fauxAssistantMessage("reconcile only, no replacement"), fauxAssistantMessage("boundary follow-up acknowledged")]);
      await session.prompt("Run the bounded committed writer test");
      const result = session.messages.find(message => message.role === "toolResult" && message.toolName === "fabric_exec");
      expect(result?.role).toBe("toolResult");
      if (result?.role !== "toolResult") throw new Error("Missing native result");
      const text = result.content.filter(block => block.type === "text").map(block => block.text).join("\n");
      const decisions = decisionsFor(state);
      if (!decisions.length) throw new Error(`No real commitment: ${text}`);
      const persisted = SessionManager.open(manager.getSessionFile()!).getBranch().find(entry => entry.type === "message" && entry.message.role === "toolResult");
      expect(persisted?.type === "message" && persisted.message).toEqual(result);
      // The uncertainty probe intentionally holds the host's create response for
      // 900 ms with a 250 ms client wait. Resident actorStatus now uses that same
      // serial exchange, so reconcile only after the held request finishes.
      await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
      // Reconcile live entities before assertions that deliberately fail on the old head.
      for (const decision of decisions) {
        await waitFor(() => state.participants.get(decision.id)?.ownerHostId === residentHostId(state.config.rootId));
        expect((await main.invoke("agents.actorStatus", { id: decision.id })) as object).toMatchObject({ id: decision.id });
        expect(await main.invoke("agents.stop", { id: decision.id })).toMatchObject({ acknowledged: true });
      }
      expect(decisions).toHaveLength(ending === "terminal failure" ? 2 : 1);
      expect(fs.readFileSync(path.join(state.root, "trigger.txt"), "utf8")).toBe("triggered");
      expect(pendingCount).toBe(mode === "none" ? 0 : 1);
      expect(claimed).toHaveBeenCalled();
      if (ending === "ordinary success") {
        expect(result.isError).toBe(false); expect(text).not.toContain("ResidentOutcomeUnknownError"); expect(text).toContain('"continued": true');
      } else {
        expect(result.isError).toBe(true);
        expect(result.details).toMatchObject({ success: false });
        assertReceipts(text, decisions);
        expect(text.startsWith("ResidentOutcomeUnknownError")).toBe(true);
        if (ending === "terminal failure" && mode !== "none") expect(text).toContain("terminal boundary cause");
        if (nextModelText) assertReceipts(nextModelText, decisions);
        if (mode === "in-place" && switchSucceeds) expect(nextModelText).toBe(text);
      }
      if (process.env.PI_FABRIC_BOUNDARY_EVIDENCE) fs.writeFileSync(`${process.env.PI_FABRIC_BOUNDARY_EVIDENCE}-${ending.replaceAll(" ", "-")}-${mode}-${switchSucceeds}.json`, JSON.stringify({ ending, mode, switchSucceeds, result, decisions, nextModelText, originalToolText }, null, 2));
    } finally { await session?.abort(); session?.dispose(); await main.close(); await state.close(); }
  });
});

describe("round 4 registered fabric_exec committed-output priority", { timeout: 25_000 }, () => {
  it("keeps every committed receipt FIRST despite large guest logs and a long terminal error; reconciles through the registered tool", async () => {
    const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
    let artifactPath: string | undefined;
    try {
      const run = await registeredExecution(state, main);
      const args = requestArgs(state, "create");
      const result = await run(`await agents.create(${JSON.stringify({ ...args, name: "volume-one" })});
        await agents.create(${JSON.stringify({ ...args, name: "volume-two" })});
        console.log("guest-logs-start" + "log line; ".repeat(2_600) + "guest-logs-end");
        throw new Error("terminal-cause-start" + "cause detail! ".repeat(2_000) + "terminal-cause-end");`);
      const text = visibleText(result);
      const decisions = decisionsFor(state); expect(decisions).toHaveLength(2);
      artifactPath = /saved to: ([^\n]+)\]/.exec(text)?.[1];
      // Reconciliation precedes the output assertions so baseline evidence also
      // establishes the live, committed entities behind the misleading failure.
      const reconciled = [];
      for (const { id } of decisions) {
        await waitFor(() => state.participants.get(id)?.ownerHostId === residentHostId(state.config.rootId));
        const reconciliation = await run(`return {status:await agents.actorStatus({id:"${id}"}),stop:await agents.stop({id:"${id}"})};`);
        expect(reconciliation.isError).not.toBe(true);
        const record = JSON.parse(visibleText(reconciliation));
        expect(record.status.id).toBe(id); expect(record.stop).toMatchObject({ queued: true, acknowledged: true });
        await waitFor(() => state.participants.get(id)?.status === "stopped");
        const stopped = await run(`return await agents.actorStatus({id:"${id}"});`);
        expect(stopped.isError).not.toBe(true);
        expect(JSON.parse(visibleText(stopped))).toMatchObject({ id, status: "stopped" });
        reconciled.push({ ...record, stopped: JSON.parse(visibleText(stopped)) });
      }
      if (process.env.PI_FABRIC_TEST_OUTPUT_EVIDENCE) {
        const prefix = process.env.PI_FABRIC_TEST_OUTPUT_EVIDENCE;
        fs.writeFileSync(`${prefix}.visible.txt`, text);
        fs.writeFileSync(`${prefix}.json`, JSON.stringify({ visibleChars: text.length, isError: result.isError, decisions, reconciled }, null, 2));
        if (artifactPath) fs.copyFileSync(artifactPath, `${prefix}.full-output.txt`);
      }
      expect(result.isError).toBe(true);
      expect(text.length).toBeLessThanOrEqual(20_000);
      assertReceipts(text, decisions);
      const firstLog = text.indexOf("guest-logs-start");
      expect(firstLog).toBeGreaterThan(0);
      expect(text.indexOf("Do not retry or reassign")).toBeLessThan(firstLog);
      for (const decision of decisions) for (const field of ["requestId", "id", "ownerHostId"]) {
        expect(text.indexOf(decision[field])).toBeLessThan(firstLog);
      }
      expect(text).toContain("terminal-cause-start"); expect(text).toContain("terminal-cause-end");
      expect(artifactPath).toBeDefined();
      const full = fs.readFileSync(artifactPath!, "utf8");
      expect(full).toContain("log line; ".repeat(2_600)); expect(full).toContain("cause detail! ".repeat(2_000));
      assertReceipts(full, decisions);
    } finally {
      if (artifactPath) fs.rmSync(path.dirname(artifactPath), { recursive: true, force: true });
      await main.close(); await state.close();
    }
  });

  it("ordinary durable success still returns its handle without uncertainty", async () => {
    const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
    try {
      const run = await registeredExecution(state, main);
      const result = await run(`return await agents.create(${JSON.stringify(requestArgs(state, "create"))});`);
      expect(result.isError).not.toBe(true);
      expect(visibleText(result)).not.toContain("ResidentOutcomeUnknownError");
      expect(JSON.parse(visibleText(result))).toMatchObject({ id: decisionsFor(state)[0].id });
    } finally { await main.close(); await state.close(); }
  });
});

describe("round 6 registered fabric_exec handled resident uncertainty", { timeout: 25_000 }, () => {
  it.each(engines)("%s collects handled client-deadline receipts on normal completion, delivers priority output and reconciles without duplicates", async (engine) => {
    const state = await harness(false, undefined, 700); const main = mainProvider(state);
    const original = ActorDirectory.prototype.create;
    vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
      const actor = await original.apply(this, args);
      // Real commitment wins, but the client's own exchange deadline expires
      // while the executor still has time. No outer abort or terminal guest error.
      if (actor.name.startsWith("handled-unknown")) await delay(1_000);
      return actor;
    });
    const descriptor = { name: "drain", description: "Wait for resident publication", risk: "read" as const,
      inputSchema: { type: "object", properties: {}, additionalProperties: false } };
    main.registry.register({ name: "probe", description: "resident exchange synchronization",
      async list() { return [descriptor]; }, async describe() { return descriptor; },
      async invoke() { await waitFor(() => entries(state.residencyRoot, "processing").length === 0); return true; },
    });
    let artifactPath: string | undefined;
    try {
      const run = await registeredExecution(state, main, 10_000, engine);
      const executed = vi.spyOn(FabricExecutionService.prototype, "execute");
      const python = engine === "cpython" || engine === "monty";
      const calls = ["handled-success", "handled-unknown-one", "handled-unknown-two"].map(name =>
        publicCall(engine, "create", { ...requestArgs(state, "create"), name }));
      const code = python
        ? `mapped = []\n${calls.map(call => `try:\n    handle = ${call}\n    mapped.append({"ok": True, "handle": handle})\nexcept Exception as error:\n    mapped.append({"ok": False, "error": str(error)})\nawait tools.call(ref="probe.drain", args={})`).join("\n")}\nprint("guest-logs-start" + "log line; " * 3000 + "guest-logs-end")\nreturn {"mapped": mapped, "supplement": "result-start" + "result detail! " * 2000 + "result-end"}`
        : `const mapped = []; ${calls.map(call => `mapped.push(...(await Promise.allSettled([${call.replace(/^await /, "")}])).map(result => result.status === "fulfilled" ? {ok:true,handle:result.value} : {ok:false,error:String(result.reason)})); await tools.call({ref:"probe.drain",args:{}});`).join("\n")}
          console.log("guest-logs-start" + "log line; ".repeat(3000) + "guest-logs-end");
          return {mapped,supplement:"result-start" + "result detail! ".repeat(2000) + "result-end"};`;
      const result = await run(code);
      const collected = await executed.mock.results[0]!.value;
      const text = visibleText(result);
      artifactPath = /saved to: ([^\n]+)\]/.exec(text)?.[1];
      const decisions = decisionsFor(state); expect(decisions).toHaveLength(3);
      const mapped = collected.value.mapped;
      expect(collected.success, collected.error).toBe(true);
      expect(collected.trace.outcome).toBe("succeeded");
      expect(result.isError).not.toBe(true);
      expect(mapped).toEqual([expect.objectContaining({ ok: true, handle: expect.objectContaining({ id: expect.any(String) }) }),
        expect.objectContaining({ ok: false, error: expect.stringContaining("ResidentOutcomeUnknownError") }),
        expect.objectContaining({ ok: false, error: expect.stringContaining("ResidentOutcomeUnknownError") })]);
      const successful = decisions.find(decision => decision.id === mapped[0].handle.id)!;
      const uncertain = decisions.filter(decision => decision !== successful);
      const reconciled = [];
      for (const { id } of decisions) {
        await waitFor(() => state.participants.get(id)?.ownerHostId === residentHostId(state.config.rootId));
        expect(state.participants.list({ scope: "lineage" }).filter(participant => participant.id === id)).toHaveLength(1);
        const reconciliation = await run(python
          ? `return {"status": await agents.actorStatus(id="${id}"), "stop": await agents.stop(id="${id}")}`
          : `return {status:await agents.actorStatus({id:"${id}"}),stop:await agents.stop({id:"${id}"})};`);
        expect(reconciliation.isError).not.toBe(true);
        const record = JSON.parse(visibleText(reconciliation));
        expect(record.status.id).toBe(id); expect(record.stop).toMatchObject({ queued: true, acknowledged: true });
        await waitFor(() => state.participants.get(id)?.status === "stopped");
        const stopped = await run(python ? `return await agents.actorStatus(id="${id}")` : `return await agents.actorStatus({id:"${id}"});`);
        expect(JSON.parse(visibleText(stopped))).toMatchObject({ id, status: "stopped" });
        reconciled.push(record);
      }
      expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(3);
      if (process.env.FABRIC_RESIDENT_OUTPUT_EVIDENCE) {
        const prefix = `${process.env.FABRIC_RESIDENT_OUTPUT_EVIDENCE}.handled-${engine}`;
        fs.writeFileSync(`${prefix}.visible.txt`, text);
        fs.writeFileSync(`${prefix}.json`, JSON.stringify({ visibleChars: text.length, success: collected.success,
          receipts: collected.residentOutcomes, mapped, decisions, reconciled }, null, 2));
        if (artifactPath) fs.copyFileSync(artifactPath, `${prefix}.full-output.txt`);
      }
      expect(collected.residentOutcomes).toHaveLength(2);
      for (const decision of uncertain) expect(collected.residentOutcomes).toContainEqual(expect.objectContaining({
        requestId: decision.requestId, id: decision.id, ownerHostId: decision.ownerHostId, state: "committed",
      }));
      expect(text.length).toBeLessThanOrEqual(50_000);
      expect(text.startsWith("ResidentOutcomeUnknownError:")).toBe(true);
      const firstLog = text.indexOf("Guest logs:"); expect(firstLog).toBeGreaterThan(0);
      const priority = text.slice(0, firstLog);
      assertReceipts(priority, uncertain);
      expect(priority).toContain("Resident receipts (2)");
      expect(priority).not.toContain(successful.id);
      for (const decision of uncertain) for (const field of ["requestId", "id"]) {
        expect(priority.split(decision[field])).toHaveLength(2);
      }
      expect(artifactPath).toBeDefined();
      const full = fs.readFileSync(artifactPath!, "utf8");
      expect(full).toContain("log line; ".repeat(3000)); expect(full).toContain("result detail! ".repeat(2000));
    } finally {
      if (artifactPath) fs.rmSync(path.dirname(artifactPath), { recursive: true, force: true });
      await main.close(); await state.close();
    }
  });
});
describe("round 6 registered fabric_exec post-completion deadlines", { timeout: 25_000 }, () => {
  const runtimes = { quickjs: QuickJsRuntime, cpython: CPythonRuntime, monty: MontyRuntime, node: NodeProcessRuntime, bun: NodeProcessRuntime };
  for (const engine of engines) for (const tail of ["runtime-return", "finalization"] as const) for (const ending of ["on-time", "Main", "ordinary"] as const) {
    it(`${engine} ${ending} ${tail} preserves confirmed handles or committed receipts without reassignment`, async () => {
      const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
      const startedAt = Date.now(); const clock = vi.spyOn(Date, "now").mockReturnValue(startedAt);
      let finalizedOutcome: string | undefined;
      main.registry.register({ name: "finalization-probe", description: "hold normal invocation finalization",
        async list() { return []; }, async describe() { return undefined; }, async invoke() { throw new Error("unused"); },
        async invocationEnded(_id, outcome) {
          finalizedOutcome = outcome;
          if (tail === "finalization" && ending !== "on-time") { await delay(20); clock.mockReturnValue(startedAt + 60_000); }
        },
      });
      try {
        const run = await registeredExecution(state, main, 5_000, engine, ending === "Main" ? 5_000 : 10_000);
        const execute = runtimes[engine].prototype.execute;
        const guest = vi.spyOn(runtimes[engine].prototype, "execute").mockImplementation(async function (this: QuickJsRuntime | CPythonRuntime | MontyRuntime | NodeProcessRuntime, code: string, hostCall: FabricHostCall, options: FabricSandboxOptions) {
          const completed = await execute.call(this, code, hostCall, options);
          // Guest and real native runtime have completed with all calls settled;
          // hold the remaining runtime->service publication boundary separately.
          if (tail === "runtime-return" && ending !== "on-time") { await delay(20); clock.mockReturnValue(startedAt + 60_000); }
          return completed;
        });
        const executed = vi.spyOn(FabricExecutionService.prototype, "execute");
        const result = await run(`return ${publicCall(engine, "create", requestArgs(state, "create"))}${engine === "cpython" || engine === "monty" ? "" : ";"}`);
        clock.mockRestore();
        const completed = await guest.mock.results[0]!.value;
        const collected = await executed.mock.results[0]!.value;
        const decisions = decisionsFor(state); expect(decisions).toHaveLength(1);
        const decision = decisions[0]!; const text = visibleText(result);
        expect(finalizedOutcome).toBe(tail === "runtime-return" && ending !== "on-time" ? "failed" : "succeeded");
        expect(completed.terminationReason).toBe("completed");
        expect(completed.value.id).toBe(decision.id);
        expect(completed.residentOutcomes).toBeUndefined();
        if (ending === "on-time") {
          expect(collected.success).toBe(true); expect(result.isError).not.toBe(true);
          expect(JSON.parse(text)).toMatchObject({ id: decision.id });
          expect(collected.residentOutcomes).toBeUndefined(); expect(text).not.toContain("Do not retry or reassign");
        } else {
          expect(collected.success).toBe(false); expect(collected.value).toBeUndefined();
          expect(collected.trace.outcome).toBe("timed_out"); expect(result.isError).toBe(true);
          expect(collected.residentOutcomes).toEqual([expect.objectContaining({ requestId: decision.requestId,
            id: decision.id, ownerHostId: decision.ownerHostId, state: "committed" })]);
          expect(text.startsWith("ResidentOutcomeUnknownError:")).toBe(true);
          assertReceipts(text, decisions); expect(text.length).toBeLessThanOrEqual(50_000);
          expect(text).toContain(ending === "Main" ? "MainExecutionCeilingError" : "Execution timed out after 5000ms");
          if (ending === "ordinary") expect(text).not.toContain("MainExecutionCeilingError");
        }
        // The caller reconciles the original writer, never launches a replacement.
        const python = engine === "cpython" || engine === "monty";
        const reconciled = await run(python
          ? `return {"status": await agents.actorStatus(id="${decision.id}"), "stop": await agents.stop(id="${decision.id}")}`
          : `return {status:await agents.actorStatus({id:"${decision.id}"}),stop:await agents.stop({id:"${decision.id}"})};`);
        expect(reconciled.isError).not.toBe(true);
        expect(JSON.parse(visibleText(reconciled))).toMatchObject({ status: { id: decision.id }, stop: { acknowledged: true } });
        await waitFor(() => state.participants.get(decision.id)?.status === "stopped");
        expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
        if (process.env.FABRIC_RESIDENT_OUTPUT_EVIDENCE) {
          const prefix = `${process.env.FABRIC_RESIDENT_OUTPUT_EVIDENCE}.${tail}-${engine}-${ending}`;
          fs.writeFileSync(`${prefix}.visible.txt`, text);
          fs.writeFileSync(`${prefix}.json`, JSON.stringify({ success: collected.success, receipts: collected.residentOutcomes,
            guestCompleted: completed.terminationReason, finalizedOutcome, decisions, reconciliation: JSON.parse(visibleText(reconciled)) }, null, 2));
        }
      } finally { clock.mockRestore(); await main.close(); await state.close(); }
    });
  }

  for (const engine of ["cpython", "monty"] as const) for (const ending of ["Main", "ordinary"] as const) {
    it(`${engine} ${ending} native teardown after confirmed guest completion settles every committed receipt`, async () => {
      const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
      const startedAt = Date.now(); const clock = vi.spyOn(Date, "now").mockReturnValue(startedAt);
      let crossed = false; const closed: Promise<void>[] = [];
      const crossDeadline = () => { crossed = true; clock.mockReturnValue(startedAt + 60_000); };
      if (engine === "cpython") {
        const actual = await vi.importActual<typeof import("node:child_process")>("node:child_process");
        vi.mocked(childProcess.spawn).mockImplementation(((...args: Parameters<typeof actual.spawn>) => {
          const child = actual.spawn(...args);
          if (Array.isArray(args[1]) && args[1].includes("-I")) {
            closed.push(new Promise<void>(resolve => child.once("close", () => resolve())));
            const kill = child.kill.bind(child);
            vi.spyOn(child, "kill").mockImplementation(signal => {
              // finish() has accepted the successful result and run its initial
              // receipt pass; only process teardown now crosses the deadline.
              const killed = kill(signal); crossDeadline(); return killed;
            });
          }
          return child;
        }) as typeof actual.spawn);
      } else {
        const native = await import("@pydantic/monty/node"); const create = native.Monty.create.bind(native.Monty);
        vi.spyOn(native.Monty, "create").mockImplementation(async options => {
          const pool = await create(options); const close = pool.close.bind(pool);
          vi.spyOn(pool, "close").mockImplementation(async () => { await close(); crossDeadline(); });
          return pool;
        });
      }
      try {
        const run = await registeredExecution(state, main, 5_000, engine, ending === "Main" ? 5_000 : 10_000);
        const executed = vi.spyOn(FabricExecutionService.prototype, "execute");
        const result = await run(`handle = ${publicCall(engine, "create", requestArgs(state, "create"))}\nprint("guest completed with " + handle["id"])\nreturn handle`);
        clock.mockRestore();
        await Promise.all(closed);
        const collected = await executed.mock.results[0]!.value;
        const decisions = decisionsFor(state); expect(decisions).toHaveLength(1);
        const decision = decisions[0]!; const text = visibleText(result);
        expect(crossed).toBe(true); expect(collected.logs.join("\n")).toContain(`guest completed with ${decision.id}`);
        expect(collected.success).toBe(false); expect(collected.value).toBeUndefined();
        expect(collected.trace.outcome).toBe("timed_out"); expect(result.isError).toBe(true);
        expect(collected.residentOutcomes).toEqual([expect.objectContaining({ requestId: decision.requestId,
          id: decision.id, ownerHostId: decision.ownerHostId, state: "committed" })]);
        expect(text.startsWith("ResidentOutcomeUnknownError:")).toBe(true); assertReceipts(text, decisions);
        expect(text).toContain(ending === "Main" ? "MainExecutionCeilingError" : "Execution timed out after 5000ms");
        if (ending === "ordinary") expect(text).not.toContain("MainExecutionCeilingError");
        // Stop injecting the crossed clock into later reconciliation invocations.
        vi.restoreAllMocks(); vi.mocked(childProcess.spawn).mockReset();
        const reconcile = await registeredExecution(state, main, 5_000, engine);
        const reconciled = await reconcile(`return {"status": await agents.actorStatus(id="${decision.id}"), "stop": await agents.stop(id="${decision.id}")}`);
        expect(reconciled.isError).not.toBe(true);
        expect(JSON.parse(visibleText(reconciled))).toMatchObject({ status: { id: decision.id }, stop: { acknowledged: true } });
        await waitFor(() => state.participants.get(decision.id)?.status === "stopped");
        expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
        if (process.env.FABRIC_RESIDENT_OUTPUT_EVIDENCE) {
          const prefix = `${process.env.FABRIC_RESIDENT_OUTPUT_EVIDENCE}.native-tail-${engine}-${ending}`;
          fs.writeFileSync(`${prefix}.visible.txt`, text);
          fs.writeFileSync(`${prefix}.json`, JSON.stringify({ success: collected.success, receipts: collected.residentOutcomes,
            logs: collected.logs, crossed, decisions, reconciliation: JSON.parse(visibleText(reconciled)) }, null, 2));
        }
      } finally { clock.mockRestore(); await Promise.all(closed); await main.close(); await state.close(); }
    });
  }
});
describe("round 4 public cleanup-obligation retry cancellation", { timeout: 25_000 }, () => {
  for (const caller of ["main", "nested"] as const) for (const before of [true, false]) {
    it(`${caller} retry cancellation ${before ? "before" : "after"} commitment preserves the accepted removal obligation`, async () => {
      const state = await harness(false, undefined, 10_000); const main = mainProvider(state, caller);
      if (caller === "nested") {
        vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", state.config.rootId);
        vi.stubEnv("PI_FABRIC_MESH_ROOT", state.config.meshRoot);
      }
      const controller = new AbortController();
      let failing: ReturnType<typeof vi.spyOn> | undefined;
      try {
        const actor = await state.client.createActor({ ...requestArgs(state, "create"), name: "cleanup-retry", instructions: "Work.", residency: "durable" });
        const directory = path.join(state.config.actorRoot, actor.id);
        const rm = fs.rmSync.bind(fs);
        failing = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
          if (target === directory) throw new Error("accepted cleanup remains unavailable");
          return rm(target, options);
        });
        await expect(state.client.removeActor(actor.id)).resolves.toMatchObject({ removed: true, cleaned: false });
        const accepted = decisionsFor(state).find(d => d.operation === "removeActor")!;
        expect(accepted.state).toBe("committed");
        expect(main.actors.cleanupObligation(actor.id)).toMatchObject({ id: actor.id, residency: "durable" });
        expect(main.actors.owns(actor.id)).toBe(false);
        if (before) {
          vi.stubEnv("PI_FABRIC_TEST_RESIDENT_DELAY_STAGE", "before_commit");
          vi.stubEnv("PI_FABRIC_TEST_RESIDENT_DELAY_MS", "500");
        } else {
          const original = ActorDirectory.prototype.remove;
          vi.spyOn(ActorDirectory.prototype, "remove").mockImplementation(async function (this: ActorDirectory, ...args) {
            state.entered.resolve(); await state.release.promise; return original.apply(this, args);
          });
        }
        const outcome = main.invoke("agents.remove", { id: actor.id }, controller.signal).catch((error: Error) => error);
        if (before) await waitFor(() => entries(state.residencyRoot, "processing").length === 1);
        else await state.entered.promise;
        const requestId = entries(state.residencyRoot, "processing")[0]!.slice(0, -5);
        controller.abort(new Error("cleanup retry cancelled"));
        const error = await outcome;
        state.release.resolve();
        await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        const retry = decisionsFor(state).find(d => d.requestId === requestId);
        if (process.env.PI_FABRIC_TEST_OUTPUT_EVIDENCE) {
          fs.writeFileSync(`${process.env.PI_FABRIC_TEST_OUTPUT_EVIDENCE}.p2-${caller}-${before ? "before" : "after"}-commit.json`, JSON.stringify({
            caller, cancellationStage: before ? "before_commit" : "after_commit", accepted, retry,
            error: { name: (error as Error).name, message: (error as Error).message },
            removalMarkerRetained: fs.existsSync(path.join(state.config.actorRoot, `removal-${actor.id}.json`)),
          }, null, 2));
        }
        expect(retry).toMatchObject({ state: before ? "abandoned" : "committed" });
        expect(decisionsFor(state).find(d => d.requestId === accepted.requestId)).toEqual(accepted);
        // Cancellation of a retry must never undo the earlier durable revocation.
        expect(main.actors.cleanupObligation(actor.id)).toMatchObject({ id: actor.id, status: "stopped" });
        expect(fs.existsSync(directory)).toBe(true);
        expect(fs.existsSync(path.join(state.config.actorRoot, `removal-${actor.id}.json`))).toBe(true);
        if (before) {
          expect((error as Error).message).toContain("cleanup retry cancelled");
          expect((error as Error).message).not.toContain("ResidentOutcomeUnknownError");
        } else assertReceipts((error as Error).message, [retry]);
        failing.mockRestore(); failing = undefined;
        vi.stubEnv("PI_FABRIC_TEST_RESIDENT_DELAY_STAGE", undefined);
        vi.stubEnv("PI_FABRIC_TEST_RESIDENT_DELAY_MS", undefined);
        await expect(main.invoke("agents.remove", { id: actor.id })).resolves.toMatchObject({ removed: true });
        expect(fs.existsSync(directory)).toBe(false);
        // Assert authoritative cleanup, not the passive directory's cached row.
        expect(fs.existsSync(path.join(state.config.actorRoot, `removal-${actor.id}.json`))).toBe(false);
      } finally {
        controller.abort(); state.release.resolve(); failing?.mockRestore(); vi.unstubAllEnvs();
        await main.close(); await state.close();
      }
    });
  }
});

describe("round 2 public execution receipt contract", { timeout: 25_000 }, () => {
  for (const engine of engines) {
    it(`${engine} public Main ceiling retains committed resident receipts through normalized provider signals`, async () => {
      const state = await harness(false, undefined, 10_000);
      const main = mainProvider(state);
      const controller = new AbortController();
      const original = ActorDirectory.prototype.create;
      vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
        const actor = await original.apply(this, args);
        state.entered.resolve(); await state.release.promise; return actor;
      });
      try {
        const run = publicExecution(state, main, engine, 1_500, true);
        const outcome = run(`return ${publicCall(engine, "create", requestArgs(state, "create"))}`, controller.signal);
        await state.entered.promise;
        const result = await outcome;
        expect(result.success).toBe(false);
        expect(result.trace.outcome).toBe("timed_out");
        expect(result.error).toContain("MainExecutionCeilingError");
        const decisions = decisionsFor(state);
        expect(decisions).toHaveLength(1);
        assertReceipts(result.error, decisions);
        expect(result.residentOutcomes).toEqual([expect.objectContaining({
          requestId: decisions[0].requestId, id: decisions[0].id, state: "committed",
        })]);
        state.release.resolve();
        await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
      } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
    });
  }
  for (const engine of engines) for (const operation of ["spawn", "create"] as const) {
    it(`${engine} normal durable ${operation} returns its handle without false uncertainty`, async () => {
      const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
      const trace = await captureDurableExecutionTrace();
      trace.record("harness ready");
      let execution: unknown;
      const report = () => { try { trace.report(engine, operation, {
        execution, decisions: decisionsFor(state),
        files: Object.fromEntries(["requests", "processing", "responses", "agents", "runs"].map(directory => [directory, names(path.join(state.residencyRoot, directory))])),
        participants: state.participants.list({ scope: "lineage" }).map(({ id, kind, ownerHostId, status, stale }) => ({ id, kind, ownerHostId, status, stale })),
      }); } catch (error) { trace.report(engine, operation, { execution, snapshotError: String(error) }); } };
      try {
        // Keep the original 5 s deadline. On CI failure the phase trace, not a
        // larger timeout, distinguishes Python/IPC startup from a resident stall.
        const run = publicExecution(state, main, engine, 5_000);
        trace.record("public execution entered");
        const result = await run(`return ${publicCall(engine, operation, requestArgs(state, operation))}`);
        execution = result; trace.record("public execution returned", { success: result.success, error: result.error });
        expect(result.success, result.error).toBe(true); expect(result.error).toBeUndefined();
        const decisions = decisionsFor(state); expect(decisions).toHaveLength(1);
        expect(result.value).toMatchObject({ id: decisions[0].id });
      } catch (error) {
        trace.record("control failed", { error: String(error) });
        await trace.waitForGuests(); // Include the final exit status and stderr in failure-only output.
        report(); throw error;
      } finally {
        await trace.waitForGuests(); // Release the guest's cwd before Windows rmdir.
        await main.close(); await state.close();
      }
    });
  }
  for (const engine of engines) for (const operation of ["spawn", "create"] as const)
    for (const ending of ["abort", "deadline"] as const) for (const before of [true, false]) {
    it(`${engine} ${operation} ${ending} ${before ? "before" : "after"} commit`, async () => {
      const state = await harness(before, undefined, 10_000); const main = mainProvider(state);
      const controller = new AbortController();
      if (!before) {
        if (operation === "spawn") {
          const original = AgentManager.prototype.spawn;
          vi.spyOn(AgentManager.prototype, "spawn").mockImplementation(async function (this: AgentManager, ...args) {
            const handle = await original.apply(this, args); state.entered.resolve(); await state.release.promise; return handle;
          });
        } else {
          const original = ActorDirectory.prototype.create;
          vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
            const actor = await original.apply(this, args); state.entered.resolve(); await state.release.promise; return actor;
          });
        }
      }
      try {
        const run = publicExecution(state, main, engine);
        const outcome = run(`return ${publicCall(engine, operation, requestArgs(state, operation))}`, controller.signal);
        await state.entered.promise;
        const requestId = entries(state.residencyRoot, "processing")[0]!.slice(0, -5);
        if (ending === "abort") controller.abort();
        const result = await outcome;
        expect(result.success).toBe(false);
        const decisions = decisionsFor(state);
        expect(decisions).toHaveLength(1);
        expect(decisions[0]).toMatchObject({ requestId, state: before ? "abandoned" : "committed" });
        if (before) {
          expect(result.error).toContain(ending === "abort" ? "Execution cancelled" : "Execution timed out");
          expect(result.error).not.toContain("ResidentOutcomeUnknownError");
        } else assertReceipts(result.error, decisions);
        state.release.resolve(); await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        if (before) {
          expect(entries(state.residencyRoot, "agents")).toEqual([]);
          expect(names(path.join(state.residencyRoot, "runs"))).toEqual([]);
          expect(new ActorRegistryStore(state.config.actorRoot).records()).toEqual([]);
        } else {
          const id = decisions[0].id;
          await waitFor(() => state.participants.get(id)?.ownerHostId === residentHostId(state.config.rootId));
          expect(state.participants.list({ scope: "lineage" }).filter(p => p.id === id)).toHaveLength(1);
          // Reconciliation uses the same public service, not a direct client invocation.
          const reconcile = engine === "cpython" || engine === "monty"
            ? `return {"status": await agents.${operation === "spawn" ? "status" : "actorStatus"}(id="${id}"), "stop": await agents.stop(id="${id}")}`
            : `return {status:await agents.${operation === "spawn" ? "status" : "actorStatus"}({id:"${id}"}),stop:await agents.stop({id:"${id}"})}`;
          expect(await run(reconcile)).toMatchObject({ success: true, value: { status: { id } } });
        }
      } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
    });
  }
  for (const engine of engines) for (const settlesDuringGrace of [false, true]) {
    it(`${engine} teardown exposes a committed unawaited mutation ${settlesDuringGrace ? "that settles during grace" : "still pending after grace"} rather than false success`, async () => {
      const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
      const original = ActorDirectory.prototype.create;
      vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
        const actor = await original.apply(this, args); state.entered.resolve(); await state.release.promise; return actor;
      });
      let releaseTimer: NodeJS.Timeout | undefined;
      const descriptor = { name: "ready", description: "Wait for the real resident commit", risk: "read" as const,
        inputSchema: { type: "object", properties: {}, additionalProperties: false } };
      main.registry.register({ name: "probe", description: "public execution synchronization",
        async list() { return [descriptor]; }, async describe() { return descriptor; },
        async invoke() {
          await state.entered.promise;
          if (settlesDuringGrace) releaseTimer = setTimeout(state.release.resolve, 100);
          return true;
        },
      });
      try {
        const run = publicExecution(state, main, engine, 5_000);
        const call = publicCall(engine, "create", requestArgs(state, "create"));
        const code = engine === "cpython"
          ? `asyncio.create_task(${call.replace(/^await /, "")})\nawait tools.call(ref="probe.ready", args={})\nreturn "guest ended"`
          : engine === "monty"
            ? `${call.replace(/^await /, "")}\nawait tools.call(ref="probe.ready", args={})\nreturn "guest ended"`
            : `void ${call.replace(/^await /, "")}; await tools.call({ref:"probe.ready",args:{}}); return "guest ended";`;
        const result = await run(code); expect(result.success).toBe(false); expect(result.value).toBeUndefined();
        const decisions = decisionsFor(state); expect(decisions).toHaveLength(1); assertReceipts(result.error, decisions);
        state.release.resolve(); await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(1);
      } finally { clearTimeout(releaseTimer); state.release.resolve(); await main.close(); await state.close(); }
    });
  }
  for (const engine of engines) for (const ending of ["abort", "deadline", "failure"] as const) {
    it(`${engine} retains every previously successful mutation not returned to caller on ${ending}`, async () => {
      const state = await harness(false, undefined, 10_000); const main = mainProvider(state);
      const controller = new AbortController(); const original = ActorDirectory.prototype.create; let created = 0;
      vi.spyOn(ActorDirectory.prototype, "create").mockImplementation(async function (this: ActorDirectory, ...args) {
        const actor = await original.apply(this, args);
        if (++created === 2 && ending !== "failure") { state.entered.resolve(); await state.release.promise; }
        return actor;
      });
      try {
        const run = publicExecution(state, main, engine);
        const first = publicCall(engine, "create", { ...requestArgs(state, "create"), name: "receipt-one" });
        const second = publicCall(engine, "create", { ...requestArgs(state, "create"), name: "receipt-two" });
        const python = engine === "cpython" || engine === "monty";
        const code = ending === "failure" ? `${first}${python ? '\nraise ValueError("guest failed")' : '; throw new Error("guest failed");'}`
          : `${first}${python ? '\nreturn ' : '; return '}${second}`;
        const outcome = run(code, controller.signal);
        if (ending !== "failure") { await state.entered.promise; if (ending === "abort") controller.abort(); }
        const result = await outcome; expect(result.success).toBe(false);
        const decisions = decisionsFor(state); expect(decisions).toHaveLength(ending === "failure" ? 1 : 2);
        assertReceipts(result.error, decisions);
        state.release.resolve(); await waitFor(() => entries(state.residencyRoot, "processing").length === 0);
        expect(new ActorRegistryStore(state.config.actorRoot).records()).toHaveLength(decisions.length);
      } finally { controller.abort(); state.release.resolve(); await main.close(); await state.close(); }
    });
  }
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
