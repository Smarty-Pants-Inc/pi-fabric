import { execFileSync, spawn, spawnSync } from "node:child_process";
import { hasUnresolvedWorker, markUnresolvedWorker } from "../src/storage/retention.js";
import { retainedProcessStates, retainedProcessWorker } from "./helpers/retained-process-worker.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { runResidentHostFromConfigPath } from "../src/residency/host.js";
import { AgentManager } from "../src/agents/manager.js";
import { AgentCompletionInbox } from "../src/agents/completion-inbox.js";
import { ActorRegistryStore } from "../src/actors/registry-store.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { createMainExecutionCeilingError } from "../src/async-settlement.js";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricMainAgentDeliveryRequest, FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { ResidencyClient } from "../src/residency/client.js";
import { ResidentHost, RESIDENT_RUN_RETENTION_MS } from "../src/residency/host.js";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import { processStartTime } from "../src/residency/process-identity.js";
import { projectOf } from "../src/topology/project-identity.js";
import { ResidentActorClient } from "../src/residency/actor-client.js";
import {
  RESIDENT_HOST_FORMAT,
  residentDeliveryPrefix,
  residentHostId,
  residentResultPath,
  residentRoot,
  type ResidentHostConfig,
  type ResidentHostOwner,
} from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { launchLog, same, startedAtMs } from "./helpers/owned-processes.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { actorParticipantRecord } from "../src/topology/records.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import type { FabricActorMessage } from "../src/actors/types.js";

const repo = process.cwd();
const hostPath = path.resolve("dist/residency/launcher.js");
const fakeWorker = path.resolve("tests/fixtures/fake-worker.mjs");
const hasResidentHost = fs.existsSync(hostPath);
const roots: string[] = [];

const delay = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });

// Git normalizes worktree paths its own way (forward slashes, Windows 8.3
// short names resolved), while Node never expands the short form from
// os.tmpdir(), so path text can never match there. Assert the worktree's
// branch registration instead — that is what these checks mean.
const worktreeBranches = (repository: string): string[] =>
  git(repository, "worktree", "list", "--porcelain")
    .split("\n")
    .filter((line) => line.startsWith("branch refs/heads/"))
    .map((line) => line.slice("branch refs/heads/".length));

const initRepository = (directory: string): void => {
  fs.mkdirSync(directory, { recursive: true });
  git(directory, "init", "-q");
  git(directory, "config", "user.email", "pi-fabric-tests@example.invalid");
  git(directory, "config", "user.name", "Pi Fabric tests");
  fs.writeFileSync(path.join(directory, "README.md"), "test repository\n");
  git(directory, "add", ".");
  git(directory, "commit", "-qm", "initial");
};

const waitFor = async (predicate: () => boolean, timeoutMs = 7_000): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error("Timed out waiting for durable residency state");
    await delay(25);
  }
};

const mainTarget = (
  identity: MeshIdentity,
  deliveries: FabricMainAgentDeliveryRequest[],
): FabricMainAgentTarget => ({
  id: identity.id,
  local: true,
  matches: (id) => id === "main" || id === identity.id,
  info: () => {
    throw new Error("not used by residency tests");
  },
  deliverAgent: (request) => {
    deliveries.push(request);
    return { queued: true, messageId: randomId(), routed: "main" };
  },
});

const randomId = (): string => Math.random().toString(16).slice(2);

interface RootHarness {
  root: string;
  mesh: MeshStore;
  meshConfig: typeof DEFAULT_FABRIC_CONFIG.mesh;
  identity: MeshIdentity;
  participants: ParticipantDirectory;
  mainAgent: FabricMainAgentTarget;
  deliveries: FabricMainAgentDeliveryRequest[];
  config: ResidentHostConfig;
}

const rootHarness = async (name: string): Promise<RootHarness> => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `pi-fabric-${name}-`));
  roots.push(root);
  const meshRoot = path.join(root, "mesh");
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const mesh = new MeshStore(meshRoot, meshConfig.maxEventBytes, meshConfig.maxReadEvents);
  const identity: MeshIdentity = {
    id: `session:${name}:${randomId()}`,
    name: "main",
    kind: "main",
    sessionId: name,
  };
  const participants = new ParticipantDirectory(mesh, {
    enabled: true,
    hostId: identity.id,
    rootId: identity.id,
    identity,
    heartbeatMs: 50,
    leaseMs: 300,
  });
  participants.registerSource(() => [{
    format: 1,
    id: identity.id,
    kind: "root",
    rootId: identity.id,
    ownerHostId: identity.id,
    ownerIdentityId: identity.id,
    name: "main",
    status: "idle",
    residency: "session",
    runner: "pi",
    transport: "host",
    capabilities: ["steer", "followUp", "fabric"],
    cwd: repo,
    sessionId: name,
    startedAt: Date.now(),
    updatedAt: Date.now(),
    controlProtocol: "v1",
  }]);
  await participants.start();
  const residencyRoot = residentRoot(meshRoot, identity.id);
  const deliveries: FabricMainAgentDeliveryRequest[] = [];
  return {
    root,
    mesh,
    meshConfig,
    identity,
    participants,
    mainAgent: mainTarget(identity, deliveries),
    deliveries,
    config: {
      format: RESIDENT_HOST_FORMAT,
      rootId: identity.id,
      sessionId: name,
      cwd: repo,
      projectRoot: repo,
      meshRoot,
      actorRoot: path.join(meshRoot, "actors"),
      sessionActorRoot: path.join(meshRoot, "actors", name),
      residencyRoot,
      fullCodeMode: true,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 10_000 },
      mesh: meshConfig,
      retention: DEFAULT_FABRIC_CONFIG.retention,
      workerPath: fakeWorker,
      fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi",
      claudeBinary: "claude",
      vedaBinary: "veda",
      piModels: {
        available: [
          { provider: "provider", id: "visible", name: "Visible" },
          { provider: "deepseek", id: "deepseek-chat", name: "DeepSeek Chat" },
        ],
        aliases: {},
        defaultModel: "provider/visible",
      },
    },
  };
};

const stopResident = async (config: ResidentHostConfig): Promise<void> => {
  const ownerPath = path.join(config.residencyRoot, "owner.json");
  const owner = (() => {
    try {
      return JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
    } catch {
      return undefined;
    }
  })();
  if (owner?.pid) {
    const alive = (): boolean => {
      try {
        process.kill(owner.pid, 0);
        return true;
      } catch {
        return false;
      }
    };
    try {
      process.kill(owner.pid, "SIGTERM");
    } catch {
      // Process already exited.
    }
    // The host still writes under mesh/ after it removes owner.json: wait for the process
    // itself to exit, not only its marker, before the root is removed.
    await waitFor(() => !alive(), 20_000).catch(() => {
      try { process.kill(owner.pid, "SIGKILL"); } catch { /* exited */ }
    });
  }
};

afterEach(async () => {
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) {
    const residencyDirectory = path.join(root, "mesh", "residency");
    try {
      for (const entry of fs.readdirSync(residencyDirectory)) {
        const config = JSON.parse(
          fs.readFileSync(path.join(residencyDirectory, entry, "config.json"), "utf8"),
        ) as ResidentHostConfig;
        await stopResident(config);
      }
    } catch {
      // No resident host was created.
    }
    // A host that already removed owner.json (idle exit) may still be finishing its last mesh
    // writes: retry ENOTEMPTY briefly instead of failing the suite on a loaded runner.
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
  }
});

describe("durable cwd validation", () => {
  it("rejects invalid recursive cwd before creating a resident host or request", async () => {
    const state = await rootHarness("resident-cwd-rejection");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    try {
      await expect(
        client.spawnAgent({
          task: "must remain recursive",
          cwd: path.join(state.root, "missing"),
          recursive: true,
          residency: "durable",
        }),
      ).rejects.toThrow(/Invalid Fabric agent cwd.*ENOENT.*call spawn in the next message/);
      expect(fs.existsSync(path.join(state.config.residencyRoot, "owner.json"))).toBe(false);
      expect(fs.existsSync(path.join(state.config.residencyRoot, "requests"))).toBe(false);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });
});

// smarty-dev#883: a timed-out start left its launcher (and Pi child) running
// with no owner. The client must end the launcher it spawned.
describe.skipIf(process.platform === "win32")("resident host start timeout", () => {
  it("ends the launcher and its child when the start budget runs out", async () => {
    const state = await rootHarness("resident-start-timeout");
    const client = new ResidencyClient({
      config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent,
      hostPath: path.resolve("tests/fixtures/stalled-launcher.mjs"), startupTimeoutMs: 300,
    });
    try {
      await expect(client.ensureHost()).rejects.toThrow(/Timed out after \d+ms starting Fabric resident host/);
      const log = fs.readFileSync(path.join(state.config.residencyRoot, "launcher.log"), "utf8");
      const launcher = (JSON.parse(log.trim().split("\n")[0]!) as { pid: number }).pid;
      const child = Number(fs.readFileSync(path.join(state.config.residencyRoot, "stalled-child.pid"), "utf8"));
      const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
      // SIGTERM went to the whole group before the rejection; wait for both exits.
      await waitFor(() => !alive(launcher) && !alive(child), 30_000);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });
});

// Known Windows limitation: cross-spawn of the pi binary through bun's
// node_modules shims hangs before the child starts, so the launcher never
// reaches its spawn trace. Durable residency E2E stays POSIX-only until that
// spawn path is resolved; the launcher logic tests below run everywhere.
describe("resident setter Main authorization", () => {
  it.each(["self", "sibling"] as const)("refuses read-only Fabric child setTools escalation to %s and preserves next activation tools", { timeout: 20_000 }, async (target) => {
    const state = await rootHarness(`setter-child-${target}`);
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    const configPath = path.join(state.config.residencyRoot, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(state.config));
    const controller = new AbortController();
    // Start the unrestricted owner before constructing the restricted child.
    const running = runResidentHostFromConfigPath(configPath, controller.signal);
    const mainClient = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    await waitFor(() => fs.existsSync(path.join(state.config.residencyRoot, "owner.json")));
    const child = await mainClient.createActor({ name: "read-only Fabric child", instructions: "Read only.", residency: "durable", extensions: true, tools: ["read"], model: "provider/visible" });
    const sibling = await mainClient.createActor({ name: "read-only sibling", instructions: "Read only.", residency: "durable", extensions: true, tools: ["read"], model: "provider/visible" });
    vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", state.identity.id);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", state.config.meshRoot);
    vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", '["read","fabric_exec"]');
    const childIdentity: MeshIdentity = { id: child.id, name: child.name, kind: "actor", sessionId: "child-activation" };
    const agents = new AgentManager(repo, state.config.agents, { workerPath: fakeWorker, runRoot: path.join(state.root, "child-runs") });
    const passive = new ActorDirectory(["child-activation", childIdentity, state.mesh, state.meshConfig, agents, () => {},
      { persistent: true, rootId: state.identity.id, canManageActor: () => false }],
      { project: state.config.actorRoot, session: state.config.sessionActorRoot! }, "project");
    const participants = new ParticipantDirectory(state.mesh, { enabled: true, hostId: "runtime:child-activation", rootId: state.identity.id, identity: childIdentity });
    const lifecycle = new LifecycleBroker(state.mesh, childIdentity, participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const childMain = { ...state.mainAgent, local: false };
    const provider = new AgentsProvider(agents, passive, new GlobalActorRegistry(state.root, 64 * 1024), childMain, participants, undefined, lifecycle);
    const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "child", nestedToolCallId: "setTools", extensionContext: {} as FabricInvocationContext["extensionContext"], update() {}, activity() {} };
    const control = new FabricControlPlane(state.mesh, state.identity, { enabled: true, hostId: state.identity.id, pollMs: 20 });
    control.start(() => ({ accepted: false }));
    try {
      const id = target === "self" ? child.id : sibling.id;
      await expect(provider.invoke("setTools", { id, tools: ["read", "write", "bash"] }, context)).rejects.toMatchObject({ name: "ResidentActorAuthorizationError", code: "RESIDENT_ACTOR_FORBIDDEN" });
      // Even same-ceiling setters require the actual owning Main.
      await expect(provider.invoke("setTools", { id, tools: ["read"] }, context)).rejects.toMatchObject({ name: "ResidentActorAuthorizationError", code: "RESIDENT_ACTOR_FORBIDDEN" });
      expect((await mainClient.actorStatus(id)).tools).toEqual(["read"]);
      const reply = await control.requestResult<FabricActorMessage>(residentHostId(state.identity.id), id, "ask", { message: "ECHO_MODEL security next activation" });
      const runFile = path.join((target === "self" ? child : sibling).logDir!, reply.runId!, "status.json");
      await waitFor(() => fs.existsSync(runFile));
      expect(JSON.parse(fs.readFileSync(runFile, "utf8"))).toMatchObject({ tools: ["read", "fabric_exec"] });
      expect((await mainClient.actorStatus(id)).tools).toEqual(["read"]);
    } finally {
      vi.unstubAllEnvs();
      controller.abort(); await running;
      await Promise.all([control.close(), mainClient.close(), passive.close(), lifecycle.close(), participants.close(), state.participants.close()]);
      await agents.close();
    }
  });

  it("host rejects missing, child, laundered and foreign control identities and above-ceiling tools independently of provider", { timeout: 20_000 }, async () => {
    const state = await rootHarness("setter-host-auth");
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    const configPath = path.join(state.config.residencyRoot, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(state.config));
    const controller = new AbortController();
    const running = runResidentHostFromConfigPath(configPath, controller.signal);
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      await waitFor(() => fs.existsSync(path.join(state.config.residencyRoot, "owner.json")));
      const actor = await client.createActor({ name: "host auth target", instructions: "Read only.", residency: "durable", tools: ["read"] });
      const rootCaller = { identity: state.identity, hostId: state.identity.id };
      const callers = [undefined,
        { identity: { id: actor.id, name: "child", kind: "actor", sessionId: "child" }, hostId: "runtime:child" },
        { identity: { ...state.identity, sessionId: "child" }, hostId: state.identity.id },
        { identity: state.identity, hostId: "runtime:child" },
        { identity: { ...state.identity, id: "session:foreign" }, hostId: "session:foreign" },
        { ...rootCaller, toolCeiling: ["read"] }];
      for (const [index, caller] of callers.entries()) {
        const requestId = `host-auth-${index}`;
        fs.writeFileSync(path.join(state.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
          format: RESIDENT_HOST_FORMAT, rootId: state.identity.id, requestId, createdAt: Date.now(), operation: "setTools", id: actor.id, tools: ["read", "bash"], caller,
        }));
        const responsePath = path.join(state.config.residencyRoot, "responses", `${requestId}.json`);
        await waitFor(() => fs.existsSync(responsePath));
        expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: false, errorCode: "RESIDENT_ACTOR_FORBIDDEN" });
        expect((await client.actorStatus(actor.id)).tools).toEqual(["read"]);
      }
      // Typed host rejection must survive the concrete proxy, not only JSON.
      await expect(new ResidentActorClient(state.config.meshRoot, state.identity.id).setActor({ operation: "setTools", id: actor.id, tools: ["bash"] })).rejects.toMatchObject({ name: "ResidentActorAuthorizationError", code: "RESIDENT_ACTOR_FORBIDDEN" });
      await expect(client.setActor({ operation: "setTools", id: actor.id, tools: ["read"] })).resolves.toMatchObject({ tools: ["read"] });
    } finally {
      controller.abort(); await running;
      await client.close(); await state.participants.close();
    }
  });
});

describe("saturated durable spawn receipt consistency (#181 F2)", () => {
  it("revokes accepted queued work before reporting failure through the provider", { timeout: 15_000 }, async () => {
    const state = await rootHarness("resident-saturated");
    state.config.agents = { ...state.config.agents, maxConcurrent: 1 };
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    const configPath = path.join(state.config.residencyRoot, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(state.config));
    const controller = new AbortController();
    const running = runResidentHostFromConfigPath(configPath, controller.signal);
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    const agents = new AgentManager(repo, state.config.agents, { workerPath: fakeWorker, runRoot: path.join(state.root, "passive-runs") });
    const passive = new ActorDirectory([state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {},
      { persistent: true, rootId: state.identity.id, canManageActor: () => false }],
      { project: state.config.actorRoot, session: state.config.sessionActorRoot! }, "project");
    const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants,
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, passive, new GlobalActorRegistry(state.root, 64 * 1024),
      state.mainAgent, state.participants, undefined, lifecycle, undefined, client);
    const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "test", nestedToolCallId: "spawn",
      extensionContext: {} as FabricInvocationContext["extensionContext"], update() {}, activity() {} };
    const spawned = vi.spyOn(AgentManager.prototype, "spawn");
    try {
      await waitFor(() => fs.existsSync(path.join(state.config.residencyRoot, "owner.json")));
      const blocker = await client.spawnAgent({ task: "HANG", residency: "durable", transport: "process" });
      await expect(provider.invoke("spawn", { task: "rejected durable activation", residency: "durable", transport: "process" }, context))
        .rejects.toThrow(/no run directory|cannot queue durable spawns/);
      const queued = await spawned.mock.results[1]!.value;
      const hostManager = spawned.mock.contexts[1] as AgentManager;
      expect(queued.status).toBe("queued");
      expect(JSON.parse(fs.readFileSync(residentResultPath(state.config.residencyRoot, queued.id), "utf8")))
        .toMatchObject({ status: "stopped" });
      expect(() => hostManager.status(queued.id)).toThrow(/Unknown Fabric agent/);
      expect(client.hasAgent(queued.id)).toBe(false);
      expect(client.listAgents().map((run) => run.id)).toEqual([blocker.id]);
      await hostManager.stop(blocker.id);
      const successor = await client.spawnAgent({ task: "accepted after pool release", residency: "durable", transport: "process" });
      await expect(client.waitAgent(successor.id)).resolves.toMatchObject({ status: "completed" });
      expect(hostManager.runDirectory(queued.id)).toBeUndefined();
      expect(fs.existsSync(path.join(state.config.residencyRoot, "runs", queued.id))).toBe(false);
      expect(hostManager.list().some((run) => run.id === queued.id)).toBe(false);
    } finally {
      spawned.mockRestore();
      controller.abort(); await running;
      await Promise.all([client.close(), passive.close(), lifecycle.close(), state.participants.close()]);
      await agents.close();
    }
  });
});

describe("#169 round 2 public cleanup outcome", () => {
  it.each(["main", "nested"] as const)("carries failed cleanup and exact-id retry through the real %s client and provider", { timeout: 15_000 }, async (caller) => {
    const state = await rootHarness(`public-cleanup-${caller}`);
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    const configPath = path.join(state.config.residencyRoot, "config.json");
    fs.writeFileSync(configPath, JSON.stringify(state.config));
    const controller = new AbortController();
    const running = runResidentHostFromConfigPath(configPath, controller.signal);
    const mainClient = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    const nestedClient = new ResidentActorClient(state.config.meshRoot, state.identity.id);
    const agents = new AgentManager(repo, state.config.agents, { workerPath: fakeWorker, runRoot: path.join(state.root, "passive-runs") });
    const passive = new ActorDirectory([state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {},
      { persistent: true, rootId: state.identity.id, canManageActor: () => false }],
      { project: state.config.actorRoot, session: state.config.sessionActorRoot! }, "project");
    const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants,
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    vi.stubEnv("PI_FABRIC_MAIN_AGENT_ID", state.identity.id);
    vi.stubEnv("PI_FABRIC_MESH_ROOT", state.config.meshRoot);
    const provider = new AgentsProvider(agents, passive, new GlobalActorRegistry(state.root, 64 * 1024),
      state.mainAgent, state.participants, undefined, lifecycle, undefined, caller === "main" ? mainClient : undefined);
    const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "test", nestedToolCallId: "remove",
      extensionContext: {} as FabricInvocationContext["extensionContext"], update() {}, activity() {} };
    let failing: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await waitFor(() => fs.existsSync(path.join(state.config.residencyRoot, "owner.json")));
      const client = caller === "main" ? mainClient : nestedClient;
      const actor = await client.createActor({ name: "public cleanup", instructions: "Work.", residency: "durable" });
      const dir = path.join(state.config.actorRoot, actor.id);
      const marker = path.join(state.config.actorRoot, `removal-${actor.id}.json`);
      const rm = fs.rmSync.bind(fs);
      failing = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
        if (target === dir) throw new Error("public cleanup unavailable");
        return rm(target, options);
      });
      await expect(provider.invoke("remove", { id: actor.id }, context)).resolves.toMatchObject({
        removed: true, cleaned: false, pending: expect.stringContaining("cleanup failed"),
      });
      // Exercise the concrete proxy directly too, not only provider routing.
      await expect(client.removeActor(actor.id)).resolves.toMatchObject({
        removed: true, cleaned: false, pending: expect.stringContaining("cleanup failed"),
      });
      expect(fs.existsSync(marker)).toBe(true);
      failing.mockRestore();
      await expect(provider.invoke("remove", { id: actor.id }, context)).resolves.toEqual({ removed: true });
      expect(fs.existsSync(dir)).toBe(false);
      expect(fs.existsSync(marker)).toBe(false);
    } finally {
      failing?.mockRestore();
      controller.abort(); await running;
      await Promise.all([mainClient.close(), passive.close(), lifecycle.close(), state.participants.close()]);
      await agents.close();
    }
  });
});

describe("#169 round 1 resident cleanup routing", () => {
  it.each(["project", "session"] as const)("retries and reports an exact cleanup-only %s id through the resident removeActor request", { timeout: 15_000 }, async (scope) => {
    const state = await rootHarness(`cleanup-host-${scope}`);
    const configPath = path.join(state.config.residencyRoot, "config.json");
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    fs.writeFileSync(configPath, JSON.stringify(state.config));
    const controller = new AbortController();
    const running = runResidentHostFromConfigPath(configPath, controller.signal);
    const request = async (command: Record<string, unknown>) => {
      const requestId = randomId();
      fs.writeFileSync(path.join(state.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
        format: RESIDENT_HOST_FORMAT, rootId: state.identity.id, requestId, createdAt: Date.now(), ...command,
      }));
      const responsePath = path.join(state.config.residencyRoot, "responses", `${requestId}.json`);
      await waitFor(() => fs.existsSync(responsePath));
      return JSON.parse(fs.readFileSync(responsePath, "utf8"));
    };
    let failing: ReturnType<typeof vi.spyOn> | undefined;
    try {
      await waitFor(() => fs.existsSync(path.join(state.config.residencyRoot, "requests")));
      const created = await request({ operation: "createActor", request: { scope, name: "resident retry", instructions: "Work.", residency: "durable" } });
      expect(created.ok).toBe(true);
      const actor = created.actor;
      const dir = path.join(scope === "project" ? state.config.actorRoot : state.config.sessionActorRoot!, actor.id);
      const marker = path.join(path.dirname(dir), `removal-${actor.id}.json`);
      const rm = fs.rmSync.bind(fs);
      failing = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
        if (target === dir) throw new Error("resident cleanup failure");
        return rm(target, options);
      });
      const first = await request({ operation: "removeActor", id: actor.id });
      expect(first).toMatchObject({ ok: true, pending: expect.stringContaining("cleanup failed") });
      expect(fs.existsSync(marker)).toBe(true);
      const removalsPath = path.join(state.config.residencyRoot, "removals.json");
      expect(JSON.parse(fs.readFileSync(removalsPath, "utf8")).removals).toContainEqual(expect.objectContaining({ id: actor.id }));
      const successor = (await request({ operation: "createActor", request: { scope, name: "resident retry", instructions: "Successor.", residency: "durable" } })).actor;
      fs.writeFileSync(successor.sessionFile, "successor history\n");
      // Refresh the participant view: the revoked actor must not need a live presence row.
      await new Promise((resolve) => setTimeout(resolve, 100));
      failing.mockRestore();
      const retried = await request({ operation: "removeActor", id: actor.id });
      expect(retried).toMatchObject({ ok: true });
      expect(first.cleaned).toBe(false);
      expect(retried.pending).toBeUndefined();
      expect(fs.existsSync(dir)).toBe(false);
      expect(fs.existsSync(marker)).toBe(false);
      expect(fs.existsSync(removalsPath)).toBe(false);
      expect(fs.readFileSync(successor.sessionFile, "utf8")).toBe("successor history\n");
    } finally {
      failing?.mockRestore();
      controller.abort();
      await running;
      await state.participants.close();
    }
  });
});
describe("durable completion receipts", () => {
  const seedCompletion = async (state: RootHarness, status = "completed") => {
    const id = "a".repeat(32);
    const runDirectory = path.join(state.config.residencyRoot, "runs", id);
    const agentsPath = path.join(state.config.residencyRoot, "agents");
    fs.mkdirSync(runDirectory, { recursive: true });
    fs.mkdirSync(agentsPath, { recursive: true });
    const result = {
      id, name: "durable worker", status, text: "authoritative full result", task: "work",
      runner: "pi", transport: "process", sessionId: "2147483647", cwd: state.root, startedAt: 1, updatedAt: 2, finishedAt: 2,
      turns: 1, toolCalls: 0, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    };
    fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify(result));
    const metadataPath = path.join(agentsPath, `${id}.json`);
    fs.writeFileSync(metadataPath, JSON.stringify({
      format: RESIDENT_HOST_FORMAT, rootId: state.identity.id, id, runDirectory, handle: result, createdAt: 1, updatedAt: 2,
    }));
    const key = `${residentDeliveryPrefix(state.identity.id)}receipt-test`;
    await state.mesh.put({
      key, identity: { id: residentHostId(state.identity.id), name: "resident", kind: "main" }, ifVersion: 0,
      value: {
        format: RESIDENT_HOST_FORMAT, id: "receipt-test", rootId: state.identity.id,
        agentCompletionId: id, from: { id, name: result.name, kind: "agent" },
        delivery: "followUp", triggerTurn: true, message: "truncated legacy summary",
        data: { fabricTruncated: true }, createdAt: 2,
      },
    });
    return { id, result, runDirectory, metadataPath, key };
  };

  it.each(["wait", "join", "status"])("keeps a durable completion unread when terminal %s publication is rejected", async action => {
    const state = await rootHarness(`rejected-durable-${action}`);
    const seeded = await seedCompletion(state);
    const handlers = new Map<string, (...args: any[]) => unknown>();
    const sendMessage = vi.fn();
    const context = { isIdle: () => false, hasPendingMessages: () => false, hasUI: false } as ExtensionContext;
    const inbox = new AgentCompletionInbox({ on: (name: string, handler: (...args: any[]) => unknown) => { handlers.set(name, handler); }, sendMessage } as any, context);
    const consumed = vi.fn((id: string) => inbox.acknowledge(id));
    const completed = vi.fn((result, delivered) => inbox.enqueue(result, delivered));
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete: completed, onResultConsumed: consumed });
    const agents = new AgentManager(repo, state.config.agents, { workerPath: fakeWorker, runRoot: path.join(state.root, "session-runs") });
    const actors = new ActorManager(state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {}, { actorRoot: path.join(state.root, "session-actors"), persistent: true });
    const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(state.root, 64 * 1024), state.mainAgent, state.participants, undefined, lifecycle, () => false, client);
    const registry = new ActionRegistry(); registry.register(provider);
    let expired = false;
    const ceiling = createMainExecutionCeilingError(700);
    try {
      client.start();
      await waitFor(() => completed.mock.calls.length === 1);
      await expect(registry.invoke(`agents.${action}`, { id: seeded.id }, {
        cwd: repo, signal: undefined, parentToolCallId: "receipt", nestedToolCallId: "receipt", extensionContext: context,
        update() {}, audits: [], maxResultChars: 100_000, async approve() {},
        checkExecutionBudget() { if (expired) throw ceiling; },
        observeInvocation(event) { if (event.type === "call_end" && event.success) expired = true; },
      })).rejects.toBe(ceiling);
      expect(consumed).not.toHaveBeenCalled();
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
      expect(state.mesh.get(seeded.key)).toBeDefined();
      const boundary = () => handlers.get("turn_end")?.({ message: { role: "assistant", stopReason: "stop" } }, context);
      boundary(); boundary();
      expect(sendMessage).toHaveBeenCalledOnce();
      expect(sendMessage.mock.calls[0]![0].details.ids).toEqual([seeded.id]);
      expect(sendMessage.mock.calls[0]![0].content).toContain("authoritative full result");
      await waitFor(() => state.mesh.get(seeded.key) === undefined);
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeGreaterThan(0);
    } finally { inbox.close(); await registry.close(); await client.close(); await lifecycle.close(); await actors.close(); await agents.close(); await state.participants.close(); }
  });

  // smarty-dev#878: a resident host whose root is gone sends its actors' messages to the project's
  // project agent. That Main accepts an actor message from another root's resident host, and
  // still nothing from a writer that is not a resident host.
  it("delivers an actor message that another root's resident host addressed to this root", { timeout: 30_000 }, async () => {
    const state = await rootHarness("retargeted-delivery");
    const put = (id: string, writer: string, message: string) => state.mesh.put({
      key: `${residentDeliveryPrefix(state.identity.id)}${id}`,
      identity: { id: writer, name: "writer", kind: "agent" }, ifVersion: 0,
      value: {
        format: RESIDENT_HOST_FORMAT, id, rootId: state.identity.id,
        from: { id: "actor-1", name: "supervisor", kind: "actor" },
        delivery: "steer", triggerTurn: true, message, createdAt: 1,
      },
    });
    await put("from-resident", residentHostId("session:departed-root"), "steer from the departed root's actor");
    await put("from-intruder", "session:intruder", "steer from a writer that is not a resident host");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      client.start();
      await waitFor(() => state.deliveries.length >= 1);
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(state.deliveries.map((delivery) => delivery.message)).toEqual(["steer from the departed root's actor"]);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // smarty-dev#2236: one text reply from a resident actor (lucky-asc-router on gpt-6-sol, run
  // 5b660655: one run, one outgoing message, one fabric.actor.output) reached Main twice, 1.5 s
  // apart, because the client delivered a record before deleting it: any drain that saw the record
  // again delivered it again. A record is delivered only by the drainer that claims it.
  describe("a resident actor's delivery record", () => {
    const actorReply = async (state: RootHarness, policy: { delivery: "steer" | "followUp"; triggerTurn: boolean } = { delivery: "followUp", triggerTurn: true }) => {
      const id = "sol-text-reply";
      const key = `${residentDeliveryPrefix(state.identity.id)}${id}`;
      await state.mesh.put({
        key, identity: { id: residentHostId(state.identity.id), name: "resident", kind: "main" }, ifVersion: 0,
        value: {
          format: RESIDENT_HOST_FORMAT, id, rootId: state.identity.id,
          from: { id: "a50e8177eef74cf3a5adb16c068961f7", name: "lucky-asc-router", kind: "actor" },
          ...policy, message: "asc-router: <redacted one-line relay>", createdAt: 1,
        },
      });
      return key;
    };

    // A real Main whose Pi only queues what it is sent (prompt preflight, a settle): nothing is in
    // the session until the test appends it. Review round 2 on pi-fabric#160 (finding 1, S1).
    type Sent = { message: { details: Record<string, any> }; options?: { deliverAs?: string; triggerTurn?: boolean } | undefined };
    const realMain = async (state: RootHarness, entries: unknown[], modules?: { MainAgentController: typeof import("../src/main-agent.js").MainAgentController }, idle = true) => {
      const { MainAgentController } = modules ?? await import("../src/main-agent.js");
      const handlers = new Map<string, Array<(event: unknown, ctx: unknown) => void>>();
      const sent: Sent[] = [];
      const pi = {
        on: (name: string, fn: (event: unknown, ctx: unknown) => void) => { handlers.set(name, [...(handlers.get(name) ?? []), fn]); },
        sendMessage: (message: Sent["message"], options: Sent["options"]) => { sent.push({ message, options }); },
        getThinkingLevel: () => "off",
      };
      const ctx = { isIdle: () => idle, hasPendingMessages: () => false, signal: { aborted: false }, sessionManager: { getEntries: () => entries } };
      const main = new MainAgentController(pi as never, state.identity.id, true, repo, state.identity.sessionId);
      main.attachFollowUpDrain(ctx as never, 60_000, path.join(state.config.meshRoot, "main-followups", "root.json"));
      const emit = (name: string, event: unknown) => { for (const fn of handlers.get(name) ?? []) fn(event, ctx); };
      const append = (item: Sent) => entries.push({ type: "custom_message", customType: "pi-fabric-agent-message", details: item.message.details });
      return { main, sent, emit, append };
    };
    const settle = { outcome: "completed", context: { pendingMessages: [] } };

    it.skipIf(process.platform === "win32").each(["held", "direct", "consumed"])("#180 S3 / #177 S1 refuses uncertain %s replay after a fresh Main/residency restart", { timeout: 20_000 }, async (mode) => {
      const state = await rootHarness(`replay-post-rename-${mode}`);
      const key = await actorReply(state, mode === "direct"
        ? { delivery: "steer", triggerTurn: true }
        : { delivery: "followUp", triggerTurn: true });
      const configPath = path.join(state.root, "fixture-config.json");
      fs.writeFileSync(configPath, JSON.stringify(state.config));
      const run = (phase: string) => {
        const child = spawnSync("bun", [path.resolve("tests/fixtures/main-residency-replay.ts"), configPath, key, mode, phase], { encoding: "utf8", timeout: 8_000 });
        expect(child.error).toBeUndefined();
        expect(child.status, child.stderr).toBe(0);
        return JSON.parse(child.stdout) as { pid: number; refused: { sourceSurvives: boolean; acknowledgments: number; deletes: number }; recovered: { acknowledgments: number; deletes: number; delivered: number } };
      };
      try {
        const before = run("prepare");
        expect(state.mesh.get(key)).toBeDefined();
        const after = run("recover");
        expect(after.pid).not.toBe(before.pid); // No module/controller state survives.
        expect(after.refused).toEqual({ sourceSurvives: true, acknowledgments: 0, deletes: 0 });
        expect(after.recovered).toEqual({ acknowledgments: 1, deletes: 1, delivered: mode === "consumed" ? 0 : 1 });
      } finally { await state.participants.close(); }
    });

    it.each(process.platform === "win32" ? ["file"] : ["file", "directory"])("#169 round 3 keeps the resident source when Main's %s journal barrier fails", { timeout: 15_000 }, async (barrier) => {
      const state = await rootHarness(`delivery-barrier-${barrier}`);
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const sync = fs.fsyncSync.bind(fs);
      let fail = true;
      const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
        if (fail && fs.fstatSync(fd).isDirectory() === (barrier === "directory")) throw new Error("journal barrier unavailable");
        sync(fd);
      });
      const deliver = vi.spyOn(main.main, "deliverAgent");
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        client.start();
        await waitFor(() => deliver.mock.results.length > 0);
        expect(deliver.mock.results[0]).toMatchObject({ type: "throw", value: expect.objectContaining({ message: expect.stringContaining("journal barrier unavailable") }) });
        expect(state.mesh.get(key)).toBeDefined();
        expect(main.sent).toHaveLength(0);
        fail = false;
        await waitFor(() => state.mesh.get(key) === undefined);
        expect(main.sent).toHaveLength(1);
      } finally { fail = false; synced.mockRestore(); await client.close(); main.main.closeFollowUpDrain(); await state.participants.close(); }
    });

    it.skipIf(process.platform === "win32")("#169 security S1 retains the source until a retry completes the containing-directory barrier", { timeout: 15_000 }, async () => {
      const state = await rootHarness("delivery-owed-directory-barrier");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const containingDirectory = state.config.meshRoot;
      const leaf = path.join(containingDirectory, "main-followups");
      const descriptors = new Map<number, string>();
      const events: string[] = [];
      const open = fs.openSync.bind(fs);
      const sync = fs.fsyncSync.bind(fs);
      let fail = true;
      const opened = vi.spyOn(fs, "openSync").mockImplementation((file, flags, mode) => {
        const fd = open(file, flags, mode);
        descriptors.set(fd, String(file));
        return fd;
      });
      const synced = vi.spyOn(fs, "fsyncSync").mockImplementation((fd) => {
        if (descriptors.get(fd) === containingDirectory) {
          events.push("containing-directory");
          if (fail) throw new Error("directory link barrier unavailable");
        }
        sync(fd);
      });
      const deliver = vi.spyOn(main.main, "deliverAgent");
      const removeSource = state.mesh.delete.bind(state.mesh);
      const deleted = vi.spyOn(state.mesh, "delete").mockImplementation(async (request) => {
        if (request.key === key) events.push("source-delete");
        return removeSource(request);
      });
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        client.start();
        await waitFor(() => deliver.mock.results.length >= 2);
        expect(fs.statSync(leaf).isDirectory()).toBe(true); // Retry sees the failed attempt's existing leaf.
        expect(deliver.mock.results.every((result) => result.type === "throw")).toBe(true);
        expect(events.filter((event) => event === "containing-directory").length).toBeGreaterThanOrEqual(2);
        expect(events).not.toContain("source-delete");
        expect(state.mesh.get(key)).toBeDefined();
        expect(main.sent).toHaveLength(0);
        events.length = 0;
        fail = false;
        await waitFor(() => state.mesh.get(key) === undefined);
        expect(events.indexOf("containing-directory")).toBeGreaterThanOrEqual(0);
        expect(events.indexOf("source-delete")).toBeGreaterThan(events.indexOf("containing-directory"));
        expect(main.sent).toHaveLength(1);
      } finally {
        fail = false;
        deleted.mockRestore(); synced.mockRestore(); opened.mockRestore();
        await client.close(); main.main.closeFollowUpDrain(); await state.participants.close();
      }
    });

    it("#169 round 2 isolates a refused first source while delivering a steer and another sender, then retries after a boundary", { timeout: 15_000 }, async () => {
      // Barrier order and failures are covered by tests/atomic-write-durable.test.ts and the barrier regressions.
      const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {});
      try {
        const state = await rootHarness("fair-ancestry-drain");
        const main = await realMain(state, [], undefined, false);
        const from = { id: "bounded-actor", name: "actor", kind: "actor" as const };
        for (let index = 0; index <= 1024; index++) {
          main.main.deliverAgent({ from, message: "tiny", delivery: "followUp", triggerTurn: true, data: { coalesceKey: "state" }, deliveryId: `prior-${index}` });
        }
        const prefix = residentDeliveryPrefix(state.identity.id);
        const seed = async (id: string, sender: typeof from, delivery: "steer" | "followUp", data?: unknown) => {
          await state.mesh.put({ key: `${prefix}${id}`, identity: { id: residentHostId(state.identity.id), name: "resident", kind: "main" }, value: {
            format: RESIDENT_HOST_FORMAT, id, rootId: state.identity.id, from: sender,
            message: id, delivery, triggerTurn: true, ...(data ? { data } : {}), createdAt: Date.now(),
          } });
        };
        await seed("a-refused", from, "followUp", { coalesceKey: "state" });
        await seed("b-steer", { ...from, id: "steerer" }, "steer");
        await seed("c-other", { ...from, id: "another-sender" }, "followUp");
        const deliver = vi.spyOn(main.main, "deliverAgent");
        const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
        try {
          client.start();
          await waitFor(() => deliver.mock.calls.length >= 1);
          await new Promise((resolve) => setTimeout(resolve, 200));
          expect(deliver.mock.calls.map(([request]) => request.message)).toContain("b-steer");
          expect(deliver.mock.calls.map(([request]) => request.message)).toContain("c-other");
          expect(state.mesh.get(`${prefix}a-refused`)).toBeDefined();
          expect(state.mesh.get(`${prefix}b-steer`)).toBeUndefined();
          expect(state.mesh.get(`${prefix}c-other`)).toBeUndefined();
          expect(main.sent.some((item) => item.options?.deliverAs === "steer")).toBe(true);
          // Release and durably acknowledge the old carrier at a real Main boundary.
          main.main.flushHeldAtNextBoundary();
          main.emit("turn_end", { context: { pendingMessages: [] } });
          for (const item of main.sent) main.append(item);
          main.emit("turn_end", { context: { pendingMessages: [] } });
          await waitFor(() => state.mesh.get(`${prefix}a-refused`) === undefined);
          expect(deliver.mock.results.some((result) => result.type === "throw")).toBe(true);
          expect(deliver.mock.calls.filter(([request]) => request.message === "a-refused").length).toBeGreaterThan(1);
        } finally { await client.close(); await state.participants.close(); }
      } finally { synced.mockRestore(); }
    });

    it("#169 round 1 leaves an over-bound replacement source in the resident mesh", { timeout: 15_000 }, async () => {
      // Barrier order and failures are covered by tests/atomic-write-durable.test.ts and the barrier regressions.
      const synced = vi.spyOn(fs, "fsyncSync").mockImplementation(() => {});
      try {
        const state = await rootHarness("ancestry-backpressure");
        const main = await realMain(state, [], undefined, false);
        const from = { id: "bounded-actor", name: "actor", kind: "actor" as const };
        const maxAncestry = 1024;
        for (let index = 0; index <= maxAncestry; index++) {
          main.main.deliverAgent({ from, message: "tiny", delivery: "followUp", triggerTurn: true, data: { coalesceKey: "state" }, deliveryId: `prior-${index}` });
        }
        const key = `${residentDeliveryPrefix(state.identity.id)}refused-source`;
        await state.mesh.put({ key, identity: { id: residentHostId(state.identity.id), name: "resident", kind: "agent" }, value: {
          format: RESIDENT_HOST_FORMAT, id: "refused-source", rootId: state.identity.id, from,
          message: "tiny", delivery: "followUp", triggerTurn: true, data: { coalesceKey: "state" }, createdAt: Date.now(),
        } });
        const deliver = vi.spyOn(main.main, "deliverAgent");
        const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
        try {
          client.start();
          await waitFor(() => deliver.mock.calls.length >= 1);
          await new Promise((resolve) => setTimeout(resolve, 100));
          expect(state.mesh.get(key)).toBeDefined();
          expect(deliver.mock.results.every((result) => result.type === "throw" && /queue is full/.test(String(result.value)))).toBe(true);
          expect(main.main.queueDepth().pendingFollowUps).toBe(1);
          const saved = JSON.parse(fs.readFileSync(path.join(state.config.meshRoot, "main-followups", "root.json"), "utf8"));
          expect(saved.items[0].supersedes).toHaveLength(maxAncestry);
          expect(saved.items[0].deliveryId).toBe(`prior-${maxAncestry}`);
        } finally { await client.close(); await state.participants.close(); }
      } finally { synced.mockRestore(); }
    });
    it("is delivered once when its delete fails after it was read", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-delete-fails");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const original = state.mesh.delete.bind(state.mesh);
      let failures = 1;
      vi.spyOn(state.mesh, "delete").mockImplementation(async (input) => {
        if (failures-- > 0) throw new Error("Timed out waiting for the Fabric mesh lock");
        return original(input);
      });
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        client.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
        expect(main.sent[0]!.message.details.deliveryId).toBe(`resident:${state.identity.id}:sol-text-reply`);
      } finally {
        await client.close();
        await state.participants.close();
      }
    });

    it("is delivered once when a second drainer read it before the first deleted it", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-stale-drainer");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const stale = state.mesh.listAll(residentDeliveryPrefix(state.identity.id));
      const first = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      // A second store on the same mesh whose recent-parse cache still holds the record.
      const staleMesh = new MeshStore(state.config.meshRoot, state.meshConfig.maxEventBytes, state.meshConfig.maxReadEvents);
      vi.spyOn(staleMesh, "listAll").mockImplementation(() => structuredClone(stale));
      const second = new ResidencyClient({ config: state.config, mesh: staleMesh, participants: state.participants, mainAgent: main.main });
      try {
        first.start();
        await waitFor(() => state.mesh.get(key) === undefined && main.sent.length > 0);
        await first.close();
        second.start();
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
      } finally {
        await first.close();
        await second.close();
        await state.participants.close();
      }
    });

    // Review round 1 on pi-fabric#160: a Main that dies before it took the reply must find it after
    // its restart (a delete-first claim lost it).
    it("survives a Main that dies before taking it, and reaches the restarted Main once", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-main-dies");
      const key = await actorReply(state);
      const dying = { ...state.mainAgent, deliverAgent: () => { throw new Error("Main process exited"); } };
      const before = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: dying });
      try {
        before.start();
        await new Promise((resolve) => setTimeout(resolve, 300));
      } finally {
        await before.close();
      }
      expect(state.mesh.get(key)).toBeDefined();
      const main = await realMain(state, []);
      const after = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        after.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
      } finally {
        await after.close();
        await state.participants.close();
      }
    });

    // Review round 2 on pi-fabric#160: deliverAgent returned while the reply sat only in Pi's
    // volatile queue behind a prompt preflight; the record was deleted, then Main died.
    it("is journalled by Main before its record goes, and a restarted Main replays it exactly once", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-preflight-dies");
      const key = await actorReply(state);
      const entries: unknown[] = [];
      const dying = await realMain(state, entries);
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: dying.main });
      try {
        client.start();
        await waitFor(() => state.mesh.get(key) === undefined);
      } finally {
        await client.close();
      }
      expect(dying.sent).toHaveLength(1);                                           // queued in Pi, never in the session
      // Main dies (no close); a new controller on the same journal and session.
      const restarted = await realMain(state, entries);
      restarted.emit("agent_before_settle", settle);
      expect(restarted.sent.map((item) => item.message.details.id)).toEqual([dying.sent[0]!.message.details.id]);
      restarted.append(restarted.sent[0]!);
      restarted.emit("agent_settled", { outcome: "completed" });
      restarted.emit("agent_before_settle", settle);
      expect(restarted.sent).toHaveLength(1);
      await state.participants.close();
    });

    // Review round 2 on pi-fabric#160: a module-local set of delivered ids does not survive a
    // release reload, which loads a fresh copy of every module.
    it("is not delivered twice after a failed delete and a release reload", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-release-reload");
      const key = await actorReply(state);
      const entries: unknown[] = [];
      const old = await realMain(state, entries);
      const original = state.mesh.delete.bind(state.mesh);
      const failing = vi.spyOn(state.mesh, "delete").mockRejectedValue(new Error("Timed out waiting for the Fabric mesh lock"));
      const before = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: old.main });
      try {
        before.start();
        await waitFor(() => old.sent.length > 0 && failing.mock.calls.length > 0);
      } finally {
        await before.close();
      }
      old.append(old.sent[0]!);                                                     // the session holds it
      old.emit("agent_settled", { outcome: "completed" });
      failing.mockImplementation(original);
      vi.resetModules();                                                            // a new release generation
      const [{ ResidencyClient: FreshClient }, freshMain] = await Promise.all([
        import("../src/residency/client.js"),
        import("../src/main-agent.js"),
      ]);
      const fresh = await realMain(state, entries, freshMain);
      const after = new FreshClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: fresh.main });
      try {
        after.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(fresh.sent).toHaveLength(0);
      } finally {
        await after.close();
        await state.participants.close();
      }
    });

    // Security round 3 on pi-fabric#160 (S2): a passive reply (a non-triggering followUp; a nextTurn
    // delivery reaches Main as one, src/residency/host.ts) keeps its policy across a restart and a
    // release reload: appended once, triggerTurn false, and no Main turn of its own.
    it("keeps a passive reply passive when a restarted, reloaded Main replays it", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-passive-replay");
      const key = await actorReply(state, { delivery: "followUp", triggerTurn: false });
      const entries: unknown[] = [];
      const dying = await realMain(state, entries, undefined, false);             // Main is streaming
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: dying.main });
      try {
        client.start();
        await waitFor(() => state.mesh.get(key) === undefined);
      } finally {
        await client.close();
      }
      expect(dying.sent.map((item) => item.options)).toEqual([{ deliverAs: "followUp", triggerTurn: false }]);
      // Main dies before Pi appends it; the restart loads a new release generation.
      vi.resetModules();
      const fresh = await realMain(state, entries, await import("../src/main-agent.js"), false);
      fresh.emit("agent_before_settle", settle);
      expect(fresh.sent.map((item) => item.message.details.id)).toEqual([dying.sent[0]!.message.details.id]);
      expect(fresh.sent[0]!.options).toEqual({ deliverAs: "followUp", triggerTurn: false });
      expect(fresh.sent[0]!.message.details.triggerTurn).toBe(false);
      fresh.append(fresh.sent[0]!);
      fresh.emit("agent_settled", { outcome: "completed" });
      fresh.emit("agent_before_settle", settle);
      expect(fresh.sent).toHaveLength(1);                                           // once, no triggering release
      expect(fs.existsSync(path.join(state.config.meshRoot, "main-followups", "root.json"))).toBe(false);
      await state.participants.close();
    });

    // Astra round 3 finding 2 on pi-fabric#160: shutdown closes Main's journal first; a drain in
    // that window must keep the record, and the next Main gets it once.
    it("is kept when Main's journal is already closed, and the next Main takes it once", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-journal-closed");
      const key = await actorReply(state);
      const closing = await realMain(state, []);
      closing.main.closeFollowUpDrain();
      const during = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: closing.main });
      try {
        during.start();
        await new Promise((resolve) => setTimeout(resolve, 300));
      } finally {
        await during.close();
      }
      expect(closing.sent).toHaveLength(0);
      expect(state.mesh.get(key)).toBeDefined();
      const next = await realMain(state, []);
      const after = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: next.main });
      try {
        after.start();
        await waitFor(() => state.mesh.get(key) === undefined);
        expect(next.sent).toHaveLength(1);
      } finally {
        await after.close();
        await state.participants.close();
      }
    });

    it("is kept for a later drain when Main refuses it", { timeout: 30_000 }, async () => {
      const state = await rootHarness("delivery-refused");
      const key = await actorReply(state);
      const main = await realMain(state, []);
      const deliver = main.main.deliverAgent.bind(main.main);
      let refusals = 1;
      let keptAtRefusal = false;
      vi.spyOn(main.main, "deliverAgent").mockImplementation((request) => {
        // Review round 1 on pi-fabric#160: until Main took it, the record is the only copy.
        if (refusals-- > 0) {
          keptAtRefusal = state.mesh.get(key) !== undefined;
          throw new Error("Main's followUp queue is full");
        }
        return deliver(request);
      });
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: main.main });
      try {
        client.start();
        await waitFor(() => main.sent.length > 0 && state.mesh.get(key) === undefined);
        expect(keptAtRefusal).toBe(true);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(main.sent).toHaveLength(1);
      } finally {
        await client.close();
        await state.participants.close();
      }
    });
  });

  it("offline cleanup respects a pre-aborted public invocation before any deletion", async () => {
    const state = await rootHarness("offline-cleanup-abort");
    const seeded = await seedCompletion(state, "completed");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    const controller = new AbortController(); controller.abort(new Error("owned cleanup cancellation"));
    try {
      await expect(client.cleanupAgent(seeded.id, false, controller.signal)).rejects.toThrow("owned cleanup cancellation");
      expect(fs.existsSync(seeded.runDirectory)).toBe(true); expect(fs.existsSync(seeded.metadataPath)).toBe(true);
      expect(fs.existsSync(path.join(state.config.residencyRoot, "decisions"))).toBe(false);
    } finally { await client.close(); await state.participants.close(); }
  });

  it("offline cleanup filesystem failure after its fence retains known-ID uncertainty", async () => {
    const state = await rootHarness("offline-cleanup-failure");
    const seeded = await seedCompletion(state, "completed");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    const original = fs.rmSync;
    const failure = new Error("injected cleanup filesystem failure");
    const remove = vi.spyOn(fs, "rmSync").mockImplementation((target, options) => {
      if (String(target) === seeded.runDirectory) throw failure;
      return original(target, options);
    });
    try {
      const error = await client.cleanupAgent(seeded.id).catch((error: Error) => error);
      expect(error).toMatchObject({ name: "ResidentOutcomeUnknownError", id: seeded.id, operation: "cleanup", cause: failure });
      expect((error as Error).message).toContain("Do not retry or reassign");
      expect(fs.existsSync(seeded.metadataPath)).toBe(true);
      const decisions = fs.readdirSync(path.join(state.config.residencyRoot, "decisions"));
      expect(decisions).toHaveLength(1);
      expect(JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "decisions", decisions[0]!), "utf8")))
        .toMatchObject({ state: "committed", id: seeded.id });
    } finally { remove.mockRestore(); await client.close(); await state.participants.close(); }
  });

  // review/astra on 3257dba, D1: the durable fallback cleanup keeps a possibly live worker's files.
  it("refuses the fallback cleanup of a durable run marked with an unresolved worker", async () => {
    const state = await rootHarness("unresolved-cleanup");
    const seeded = await seedCompletion(state, "failed");
    markUnresolvedWorker(seeded.runDirectory, "the Herdr server has been unreachable for 300 s");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      await expect(client.cleanupAgent(seeded.id)).rejects.toThrow(/may still be running/);
      expect(fs.existsSync(seeded.runDirectory)).toBe(true);
      expect(fs.existsSync(seeded.metadataPath)).toBe(true);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it.each([['tmux', 'closed'], ['screen', 'closed'], ['tmux', 'unknown'], ['screen', 'unknown']] as const)("vetoes public durable cleanup of terminal live %s after host %s without an unresolved marker", async (kind, route) => {
    const state = await rootHarness(`external-cleanup-${kind}-${route}`);
    const source = path.join(state.root, 'source'); initRepository(source);
    const seeded = await seedCompletion(state, 'failed');
    const branch = `pi-fabric/external-${kind}-${seeded.id.slice(0, 8)}`;
    const worktree = path.join(source, '.pi', 'fabric', 'worktrees', seeded.id);
    fs.mkdirSync(path.dirname(worktree), { recursive: true });
    git(source, 'worktree', 'add', '-q', '-b', branch, worktree, 'HEAD');
    const record = { ...seeded.result, transport: kind, sessionId: 'external-pane', cwd: worktree };
    fs.writeFileSync(path.join(seeded.runDirectory, 'status.json'), JSON.stringify(record));
    const metadata = JSON.parse(fs.readFileSync(seeded.metadataPath, 'utf8'));
    fs.writeFileSync(seeded.metadataPath, JSON.stringify({ ...metadata, handle: { ...record, worktree, branch }, worktreeGitRoot: source }));
    const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
    fs.mkdirSync(path.dirname(resultPath), { recursive: true }); fs.writeFileSync(resultPath, JSON.stringify(record));
    // A real live cwd holder substitutes for the external pane, without requiring tmux/screen on CI.
    const worker = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { cwd: worktree, stdio: 'ignore' });
    const exited = new Promise<void>((resolve, reject) => { worker.once('error', reject); worker.once('close', () => resolve()); });
    const host = new ResidentHost(state.config);
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    const agents = new AgentManager(repo, { ...state.config.agents, budgetUsd: 0, nice: 19 }, { workerPath: fakeWorker, runRoot: path.join(state.root, "session-runs") });
    const actors = new ActorManager(state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {}, { actorRoot: path.join(state.root, "session-actors"), persistent: true });
    const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(state.root, 64 * 1024), state.mainAgent, state.participants, undefined, lifecycle, () => false, client);
    const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "public-external-cleanup", nestedToolCallId: "cleanup", extensionContext: {} as ExtensionContext, update() {} };
    const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
    const join = vi.spyOn(AgentManager.prototype, "join");
    try {
      await host.start();
      if (route === 'closed') await host.close();
      expect(fs.existsSync(path.join(seeded.runDirectory, 'unresolved-worker.json'))).toBe(false);
      await expect(provider.invoke("cleanup", { id: seeded.id, deleteBranch: true }, context)).rejects.toThrow(/checked.*exit|exit.*unconfirmed/);
      expect(join).toHaveBeenCalledTimes(route === "unknown" ? 1 : 0);
      expect(worker.exitCode).toBeNull(); expect(worker.signalCode).toBeNull();
      expect(cleanup).not.toHaveBeenCalled();
      for (const file of [seeded.runDirectory, seeded.metadataPath, resultPath, worktree]) expect(fs.existsSync(file)).toBe(true);
      expect(worktreeBranches(source)).toContain(branch);
      expect(git(source, 'branch', '--list', branch)).toContain(branch);
      const decisions = path.join(state.config.residencyRoot, 'decisions');
      const committed = fs.existsSync(decisions) ? fs.readdirSync(decisions).map(name => JSON.parse(fs.readFileSync(path.join(decisions, name), 'utf8'))).filter(decision => decision.state === 'committed') : [];
      expect(committed).toEqual([]);
    } finally {
      worker.kill("SIGTERM"); await exited; cleanup.mockRestore(); join.mockRestore();
      await client.close(); await host.close(); await actors.close(); await agents.close(); await lifecycle.close(); await state.participants.close();
    }
  }, 15_000);

  // smarty-dev#3148: public fallback must inspect process descendants, not only the parent.
  it.each(retainedProcessStates.flatMap(state => (["closed", "unknown"] as const).map(route => ({ state, route }))))(
    "vetoes public nested process $state cleanup after host $route until exit is confirmed", async ({ state: childState, route }) => {
      const state = await rootHarness(`nested-process-cleanup-${childState}-${route}`);
      const source = path.join(state.root, "source"); initRepository(source);
      const seeded = await seedCompletion(state, "completed");
      const branch = `pi-fabric/nested-process-${seeded.id.slice(0, 8)}`;
      const worktree = path.join(source, ".pi", "fabric", "worktrees", seeded.id);
      fs.mkdirSync(path.dirname(worktree), { recursive: true });
      git(source, "worktree", "add", "-q", "-b", branch, worktree, "HEAD");
      const record = { ...seeded.result, cwd: worktree };
      fs.writeFileSync(path.join(seeded.runDirectory, "status.json"), JSON.stringify(record));
      const metadata = JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8"));
      fs.writeFileSync(seeded.metadataPath, JSON.stringify({ ...metadata, handle: { ...record, worktree, branch }, worktreeGitRoot: source }));
      const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
      fs.mkdirSync(path.dirname(resultPath), { recursive: true }); fs.writeFileSync(resultPath, JSON.stringify(record));
      const host = new ResidentHost(state.config);
      const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
      const agents = new AgentManager(repo, { ...state.config.agents, budgetUsd: 0, nice: 19 }, { workerPath: fakeWorker, runRoot: path.join(state.root, "session-runs") });
      const actors = new ActorManager(state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {}, { actorRoot: path.join(state.root, "session-actors"), persistent: true });
      const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
      const provider = new AgentsProvider(agents, actors, new GlobalActorRegistry(state.root, 64 * 1024), state.mainAgent, state.participants, undefined, lifecycle, () => false, client);
      const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "public-nested-process-cleanup", nestedToolCallId: "cleanup", extensionContext: {} as ExtensionContext, update() {} };
      const cleanup = vi.spyOn(AgentManager.prototype, "cleanup");
      const join = vi.spyOn(AgentManager.prototype, "join");
      let child: Awaited<ReturnType<typeof retainedProcessWorker>> | undefined;
      const invoke = () => provider.invoke("cleanup", { id: seeded.id, deleteBranch: true }, context);
      try {
        await host.start();
        if (route === "closed") await host.close();
        child = await retainedProcessWorker(path.join(seeded.runDirectory, "nested", "process-child"), worktree, childState);
        if (childState === "missing-identity") {
          const published = JSON.parse(fs.readFileSync(child.statusFile, "utf8"));
          expect(published).toMatchObject({ status: "completed", transport: "process" });
          expect(published).not.toHaveProperty("sessionId");
        }
        expect(hasUnresolvedWorker(seeded.runDirectory)).toBe(false);
        await expect(invoke()).rejects.toThrow(/exit.*unconfirmed/);
        expect(join).toHaveBeenCalledTimes(route === "unknown" ? 1 : 0);
        expect(cleanup).not.toHaveBeenCalled();
        expect(child.worker.exitCode).toBeNull(); expect(child.worker.signalCode).toBeNull();
        for (const file of [seeded.runDirectory, child.taskFile, child.statusFile, seeded.metadataPath, resultPath, worktree]) expect(fs.existsSync(file)).toBe(true);
        expect(worktreeBranches(source)).toContain(branch);
        const decisions = path.join(state.config.residencyRoot, "decisions");
        const committed = fs.existsSync(decisions) ? fs.readdirSync(decisions).map(name => JSON.parse(fs.readFileSync(path.join(decisions, name), "utf8"))).filter(decision => decision.state === "committed") : [];
        expect(committed).toEqual([]);
        await child.stop();
        if (childState !== "terminal-live") await expect(invoke()).rejects.toThrow(/exit.*unconfirmed/);
        child.confirmExit();
        expect(await invoke()).toEqual({ cleaned: true });
        for (const file of [seeded.runDirectory, seeded.metadataPath, resultPath, worktree]) expect(fs.existsSync(file)).toBe(false);
        expect(worktreeBranches(source)).not.toContain(branch);
        expect(git(source, "branch", "--list", branch)).toBe("");
      } finally {
        await child?.stop(); cleanup.mockRestore(); join.mockRestore();
        await client.close(); await host.close(); await actors.close(); await agents.close(); await lifecycle.close(); await state.participants.close();
      }
    }, 20_000,
  );

  it("refuses the fallback cleanup of a completed durable run whose nested child is marked", async () => {
    const state = await rootHarness("unresolved-nested-cleanup");
    const seeded = await seedCompletion(state, "completed");
    markUnresolvedWorker(path.join(seeded.runDirectory, "nested", "child"), "the Herdr server has been unreachable for 300 s");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      await expect(client.cleanupAgent(seeded.id)).rejects.toThrow(/may still be running/);
      expect(fs.existsSync(path.join(seeded.runDirectory, "nested", "child"))).toBe(true);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("retracts an already queued completion on late wait and persists the receipt across reconnects", async () => {
    const state = await rootHarness("late-completion-wait");
    const seeded = await seedCompletion(state);
    const onBackgroundComplete = vi.fn();
    const onResultConsumed = vi.fn();
    const options = { config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete, onResultConsumed };
    const client = new ResidencyClient(options);
    const reconnect = new ResidencyClient(options);
    try {
      client.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      expect(onBackgroundComplete.mock.calls[0]![0].text).toBe("authoritative full result");
      expect(state.deliveries).toHaveLength(0);
      expect(state.mesh.listAll(residentDeliveryPrefix(state.identity.id))).toHaveLength(1);
      expect((await client.waitAgent(seeded.id)).status).toBe("completed");
      expect(onResultConsumed).toHaveBeenCalledWith(seeded.id);
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeGreaterThan(0);
      await client.close();
      onBackgroundComplete.mockClear();
      reconnect.start();
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id)).length === 0);
      expect(onBackgroundComplete).not.toHaveBeenCalled();
      expect(state.deliveries).toHaveLength(0);
    } finally {
      await client.close();
      await reconnect.close();
      await state.participants.close();
    }
  });

  it("retains an unread envelope across disconnect and acknowledges only actual inbox delivery", async () => {
    const state = await rootHarness("unread-completion-resume");
    const seeded = await seedCompletion(state);
    const onBackgroundComplete = vi.fn();
    const options = { config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete };
    const client = new ResidencyClient(options);
    const reconnect = new ResidencyClient(options);
    try {
      client.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      await client.close();
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
      onBackgroundComplete.mockClear();
      reconnect.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      onBackgroundComplete.mock.calls[0]![1]();
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id)).length === 0);
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeGreaterThan(0);
    } finally {
      await client.close();
      await reconnect.close();
      await state.participants.close();
    }
  });

  it("honors disabled completion notifications for envelopes from an older resident host", async () => {
    const state = await rootHarness("disabled-completion-resume");
    await seedCompletion(state);
    state.config.agents.notifyOnComplete = false;
    const onBackgroundComplete = vi.fn();
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete });
    try {
      client.start();
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id)).length === 0);
      expect(onBackgroundComplete).not.toHaveBeenCalled();
      expect(state.deliveries).toHaveLength(0);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("F1 preserves original completion through recovery sweep and a second host restart without a saved result", async () => {
    const state = await rootHarness("f1-recovery-completion");
    const seeded = await seedCompletion(state);
    const metadata = JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8"));
    // The host died while the spawn handle still said running; only the worker's record advanced.
    fs.writeFileSync(seeded.metadataPath, JSON.stringify({ ...metadata, handle: { ...metadata.handle, status: "running", text: "", residency: "durable" } }));
    await state.mesh.delete({ key: seeded.key });
    const expiredAt = Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000;
    fs.utimesSync(seeded.runDirectory, expiredAt / 1_000, expiredAt / 1_000);
    const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
    const expected = { id: seeded.id, status: "completed", text: seeded.result.text, residency: "durable" };
    let host: ResidentHost | undefined;
    let client: ResidencyClient | undefined;
    try {
      expect(fs.existsSync(resultPath)).toBe(false);
      for (let restart = 1; restart <= 2; restart++) {
        host = new ResidentHost(state.config);
        await host.start(); // The real fenced startup sweep runs before manager construction.
        client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
        expect(client.statusAgent(seeded.id), `restart ${restart}`).toMatchObject(expected);
        expect(await client.waitAgent(seeded.id, AbortSignal.timeout(2_000))).toMatchObject(expected);
        expect(fs.existsSync(seeded.runDirectory)).toBe(true);
        expect(fs.existsSync(resultPath)).toBe(false);
        await client.close();
        await host.close();
      }
    } finally {
      await client?.close();
      await host?.close();
      await state.participants.close();
    }
  });

  // smarty-dev#1882: an idle resident host removes runs/; status must not fall back to the spawn handle.
  it("reads the saved terminal record after the host removed the run directory", async () => {
    const state = await rootHarness("saved-terminal-record");
    const seeded = await seedCompletion(state);
    const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
    fs.mkdirSync(path.dirname(resultPath), { recursive: true });
    fs.writeFileSync(resultPath, JSON.stringify(seeded.result));
    fs.rmSync(path.join(state.config.residencyRoot, "runs"), { recursive: true, force: true });
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      expect(client.statusAgent(seeded.id)).toMatchObject({ status: "completed", text: "authoritative full result", residency: "durable" });
      expect(await client.waitAgent(seeded.id, AbortSignal.timeout(2_000))).toMatchObject({ status: "completed", text: "authoritative full result" });
      await expect(client.cleanupAgent(seeded.id)).resolves.toEqual({ cleaned: true });
      expect(fs.existsSync(resultPath)).toBe(false);
      expect(client.hasAgent(seeded.id)).toBe(false);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("reports a run with no record, no run directory and no live host as failed, and a live run as running", async () => {
    const state = await rootHarness("lost-terminal-record");
    const seeded = await seedCompletion(state, "running");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      fs.rmSync(path.join(seeded.runDirectory, "status.json"));
      expect(client.statusAgent(seeded.id).status).toBe("running");
      fs.rmSync(path.join(state.config.residencyRoot, "runs"), { recursive: true, force: true });
      expect(client.statusAgent(seeded.id)).toMatchObject({ status: "failed", error: expect.stringMatching(/record lost/) });
      expect((await client.waitAgent(seeded.id, AbortSignal.timeout(2_000))).status).toBe("failed");
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // review/astra on pi-fabric#136: while a host lives, a stopped attempt may still resume or retry.
  it("treats a terminal status.json as settled only once no host owns the run or the host saved it", async () => {
    const state = await rootHarness("settled-terminal-record");
    const seeded = await seedCompletion(state, "stopped");
    const ownerPath = path.join(state.config.residencyRoot, "owner.json");
    fs.writeFileSync(ownerPath, JSON.stringify({
      format: RESIDENT_HOST_FORMAT, hostId: residentHostId(state.identity.id), pid: process.pid, token: "t", startedAt: 1, readyAt: 1,
    }));
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent });
    try {
      expect(client.settledAgent(seeded.id)).toBeUndefined();
      const resultPath = residentResultPath(state.config.residencyRoot, seeded.id);
      fs.mkdirSync(path.dirname(resultPath), { recursive: true });
      fs.writeFileSync(resultPath, JSON.stringify({ ...seeded.result, status: "completed" }));
      expect(client.settledAgent(seeded.id)).toMatchObject({ status: "completed" });
      fs.rmSync(resultPath);
      fs.rmSync(ownerPath);
      expect(client.settledAgent(seeded.id)).toMatchObject({ status: "stopped" });
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  it("does not consume a result when a durable wait is aborted", async () => {
    const state = await rootHarness("aborted-completion-wait");
    const seeded = await seedCompletion(state, "running");
    const onBackgroundComplete = vi.fn();
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, onBackgroundComplete });
    try {
      await expect(client.waitAgent(seeded.id, AbortSignal.abort())).rejects.toThrow("aborted");
      expect(JSON.parse(fs.readFileSync(seeded.metadataPath, "utf8")).completionConsumedAt).toBeUndefined();
      fs.writeFileSync(path.join(seeded.runDirectory, "status.json"), JSON.stringify({ ...seeded.result, status: "completed" }));
      client.start();
      await waitFor(() => onBackgroundComplete.mock.calls.length > 0);
      expect(state.deliveries).toHaveLength(0);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });
});

describe.skipIf(!hasResidentHost || process.platform === "win32")("durable participant residency", () => {
  // Compiled launcher + real resident host/control transport; inference is deliberately fake.
  // Main supplies the separate native Pi actor acceptance proof for #2726.
  it("#2726 exposes a busy resident-owned actor and two queued tells through both provider reads", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-live-read");
    const releasePath = path.join(state.root, "release-first-worker");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath });
    const control = new FabricControlPlane(state.mesh, state.identity, { enabled: true, hostId: state.identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const agents = new AgentManager(repo, state.config.agents, { workerPath: fakeWorker, runRoot: path.join(state.root, "passive-runs") });
    const passive = new ActorDirectory([state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {},
      { persistent: true, rootId: state.identity.id, canManageActor: () => false }],
      { project: state.config.actorRoot, session: state.config.sessionActorRoot! }, "project");
    const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants, { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, passive, new GlobalActorRegistry(state.root, 64 * 1024), state.mainAgent, state.participants, control, lifecycle, undefined, client);
    const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "test", nestedToolCallId: "live-read",
      extensionContext: {} as FabricInvocationContext["extensionContext"], update() {}, activity() {} };
    try {
      client.start();
      const actor = await client.createActor({ name: "resident live review", instructions: "Reply.", residency: "durable", transport: "process", responseMode: "text", delivery: "mailbox", coalesce: false });
      const participant = () => state.participants.get(actor.id, undefined, { fresh: true });
      await control.request(client.hostId, actor.id, "followUp", { message: "settled baseline" }, client.hostId);
      await waitFor(() => participant()?.status === "idle" && Boolean(passive.status(actor.id).lastRunId));
      const baseline = passive.status(actor.id);
      expect(passive.owns(actor.id)).toBe(false);
      const definition = passive.definition(actor.id);
      await control.request(client.hostId, actor.id, "followUp", { message: "LIVE_WITH_PROGRESS", data: { fakeWorkerReleasePath: releasePath } }, client.hostId);
      await waitFor(() => participant()?.status === "running" && participant()?.actorRun !== undefined);
      const running = participant()!;
      expect(await provider.invoke("actorStatus", { id: actor.id }, context)).toMatchObject({
        status: "running", queued: 0, inFlightRun: { id: running.actorRun!.id }, lastRunId: baseline.lastRunId,
      });
      expect(await provider.invoke("actors", {}, context)).toContainEqual(expect.objectContaining({
        id: actor.id, status: "running", queued: 0, inFlightRun: expect.objectContaining({ id: running.actorRun!.id }),
      }));
      await control.request(client.hostId, actor.id, "followUp", { message: "queued review one" }, client.hostId);
      await control.request(client.hostId, actor.id, "followUp", { message: "queued review two" }, client.hostId);
      await waitFor(() => participant()?.actorQueued === 2);
      const live = participant()!;
      expect(live).toMatchObject({ status: "running", ownerHostId: client.hostId, actorQueued: 2,
        actorRun: { id: running.actorRun!.id } });
      expect(live.actorMessages).toBeGreaterThan(baseline.messages);
      expect(passive.status(actor.id)).toMatchObject({ status: "idle", queued: 0, lastRunId: baseline.lastRunId });
      const single = await provider.invoke("actorStatus", { id: actor.id }, context);
      const listed = (await provider.invoke("actors", {}, context) as Array<{ id: string }>).find(row => row.id === actor.id);
      for (const view of [single, listed]) {
        expect(view).toMatchObject({ id: actor.id, name: actor.name, status: "running", queued: 2,
          messages: live.actorMessages, lastRunId: baseline.lastRunId, inFlightRun: { id: live.actorRun!.id } });
      }
      expect(passive.definition(actor.id)).toEqual(definition);
      // The real worker cannot finish FIRST until both provider reads above agree.
      fs.writeFileSync(releasePath, "release\n");
      await waitFor(() => participant()?.status === "idle" && participant()?.actorQueued === 0 && participant()?.actorRun === undefined);
      const idleSingle = await provider.invoke("actorStatus", { id: actor.id }, context) as { inFlightRun?: unknown; lastRunId?: string };
      const idleListed = (await provider.invoke("actors", {}, context) as Array<{ id: string; inFlightRun?: unknown }>).find(row => row.id === actor.id)!;
      for (const view of [idleSingle, idleListed]) {
        expect(view).toMatchObject({ status: "idle", queued: 0, messages: participant()!.actorMessages });
        expect(view.inFlightRun).toBeUndefined();
      }
      expect(idleSingle.lastRunId).not.toBe(baseline.lastRunId);
      expect(idleSingle.lastRunId).not.toBe(running.actorRun!.id);
      const persisted = new ActorRegistryStore(state.config.actorRoot).records().find(record => record.id === actor.id)!;
      expect((persisted.messages as FabricActorMessage[]).filter(message => message.direction === "out").map(message => message.text)).toEqual([
        "fake worker complete", "live attempt 1 complete", "fake worker complete", "fake worker complete",
      ]);
      await client.removeActor(actor.id);
    } finally {
      // Also release after an assertion failure; host shutdown and root cleanup stay bounded.
      fs.writeFileSync(releasePath, "release\n");
      await control.close(); await client.close(); await passive.close(); await agents.close(); await lifecycle.close();
      await state.participants.close(); await stopResident(state.config);
      fs.rmSync(releasePath, { force: true });
    }
  });

  it.skipIf(process.platform !== "linux")("F1 preserves original completion when the compiled resident host's result save fails through graceful close and two restarts", { timeout: 60_000 }, async () => {
    // Real compiled launcher -> real Pi -> compiled resident host; only model inference is fake.
    // STREAM_PREVIEW supplies its own running/terminal records and full text, without fixture edits.
    const state = await rootHarness("f1-compiled-save-failure");
    const launches = launchLog(state.root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const options = { config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath };
    const client = new ResidencyClient(options);
    let reconnect: ResidencyClient | undefined;
    const ownerPath = path.join(state.config.residencyRoot, "owner.json");
    const closeHost = async () => {
      const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
      const owned = launches.owned().find((entry) => entry.pid === owner.pid)!;
      expect(owned).toBeDefined();
      expect(owned.argv).toContain(path.resolve("dist/residency/pi-entry.js"));
      expect(same(owned)).toBe(true);
      process.kill(owner.pid, "SIGTERM"); // Exercise graceful tracked-run cleanup, not crash recovery.
      await waitFor(() => !same(owned), 20_000);
      expect(fs.existsSync(ownerPath)).toBe(false);
    };
    try {
      expect(state.config.agents.retainRuns).toBe(false); // Keep the public default.
      const handle = await client.spawnAgent({ task: "STREAM_PREVIEW", transport: "process", residency: "durable" });
      expect(launches.owned().some(({ argv }) => argv[0] === hostPath)).toBe(true);
      const run = path.join(state.config.residencyRoot, "runs", handle.id);
      const statusPath = path.join(run, "status.json");
      const resultPath = residentResultPath(state.config.residencyRoot, handle.id);
      const record = () => JSON.parse(fs.readFileSync(statusPath, "utf8"));
      // Inject a genuine filesystem fault after spawn but before the worker finishes:
      // atomic result publication cannot rename a regular file over this directory.
      fs.mkdirSync(resultPath, { recursive: true });
      await waitFor(() => fs.existsSync(statusPath));
      expect(record().status).toBe("running");
      const worker = launches.owned().find(({ argv }) => argv[0] === fakeWorker && argv[argv.indexOf("--id") + 1] === handle.id)!;
      expect(worker).toBeDefined();
      await waitFor(() => record().status === "completed" && !same(worker), 10_000);
      const original = record();
      expect(original.text).toBe("stream preview complete");
      // Host completion publication follows onSettled, proving the failed save was attempted.
      // Do not start the client drainer: the envelope must not become an alternate result store.
      await waitFor(() => state.mesh.listAll(residentDeliveryPrefix(state.identity.id))
        .some((entry) => (entry.value as { agentCompletionId?: string }).agentCompletionId === handle.id));
      expect(fs.statSync(resultPath).isDirectory()).toBe(true);
      const metadataPath = path.join(state.config.residencyRoot, "agents", `${handle.id}.json`);
      expect(JSON.parse(fs.readFileSync(metadataPath, "utf8")).handle.status).toBe("running");
      await client.close();
      await closeHost();
      // Remove the notification too: recovery must use the worker record, not a queued summary.
      for (const entry of state.mesh.listAll(residentDeliveryPrefix(state.identity.id))) {
        await state.mesh.delete({ key: entry.key });
      }
      expect(fs.existsSync(statusPath), "failed save must veto tracked close deleting the last result").toBe(true);
      const expected = { id: handle.id, status: original.status, text: original.text, residency: "durable" };
      for (let restart = 1; restart <= 2; restart++) {
        reconnect = new ResidencyClient(options);
        await reconnect.ensureHost();
        expect(reconnect.statusAgent(handle.id), `restart ${restart}`).toMatchObject(expected);
        expect(await reconnect.waitAgent(handle.id, AbortSignal.timeout(2_000))).toMatchObject(expected);
        expect(record()).toEqual(original);
        expect(fs.statSync(resultPath).isDirectory()).toBe(true);
        await reconnect.close();
        await closeHost();
      }
      // Counterexample: a fully valid saved copy permits the expired run's startup collection.
      // This is a test-owned authoritative copy, not a claim that host save retries succeeded.
      fs.rmdirSync(resultPath);
      fs.writeFileSync(resultPath, JSON.stringify(original));
      const expiredAt = Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000;
      fs.utimesSync(run, expiredAt / 1_000, expiredAt / 1_000);
      reconnect = new ResidencyClient(options);
      await reconnect.ensureHost();
      expect(fs.existsSync(run)).toBe(false);
      expect(reconnect.statusAgent(handle.id)).toMatchObject(expected);
      expect(await reconnect.waitAgent(handle.id, AbortSignal.timeout(2_000))).toMatchObject(expected);
      await reconnect.close();
      await closeHost();
    } finally {
      await reconnect?.close();
      await client.close();
      await stopResident(state.config);
      for (const owned of launches.owned()) if (same(owned)) {
        try { process.kill(owned.pid, "SIGKILL"); } catch { /* already exited */ }
      }
      await waitFor(() => launches.owned().every((owned) => !same(owned)), 10_000);
      await state.participants.close();
    }
  });

  it.skipIf(process.platform !== "linux")("F1 retains a detached worker completion after SIGKILL, expiry, recovery and a second restart", { timeout: 60_000 }, async () => {
    const state = await rootHarness("f1-detached-completion");
    const launches = launchLog(state.root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const options = { config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath };
    const client = new ResidencyClient(options);
    let reconnect: ResidencyClient | undefined;
    const killOwner = async () => {
      const owner = JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner;
      const owned = launches.owned().find((entry) => entry.pid === owner.pid)!;
      expect(owned).toBeDefined();
      expect(same(owned)).toBe(true);
      process.kill(owner.pid, "SIGKILL"); // Kill only the host, not its detached process worker.
      await waitFor(() => !same(owned), 20_000);
    };
    try {
      const handle = await client.spawnAgent({ task: "STREAM_PREVIEW", transport: "process", residency: "durable" });
      const run = path.join(state.config.residencyRoot, "runs", handle.id);
      const statusPath = path.join(run, "status.json");
      const resultPath = residentResultPath(state.config.residencyRoot, handle.id);
      const record = () => JSON.parse(fs.readFileSync(statusPath, "utf8"));
      await waitFor(() => fs.existsSync(statusPath));
      expect(record().status).toBe("running");
      const worker = launches.owned().find(({ argv }) => argv[0] === fakeWorker && argv[argv.indexOf("--id") + 1] === handle.id)!;
      expect(worker).toBeDefined();
      await killOwner();
      expect(same(worker)).toBe(true);
      await waitFor(() => record().status === "completed" && !same(worker), 10_000);
      const original = record();
      expect(original.text).toBe("stream preview complete");
      expect(fs.existsSync(resultPath)).toBe(false);
      const metadataPath = path.join(state.config.residencyRoot, "agents", `${handle.id}.json`);
      expect(JSON.parse(fs.readFileSync(metadataPath, "utf8")).handle.status).toBe("running");
      // Only advance retention age, never rewrite the worker's terminal record.
      const expiredAt = Date.now() - RESIDENT_RUN_RETENTION_MS - 60_000;
      fs.utimesSync(run, expiredAt / 1_000, expiredAt / 1_000);
      await client.close();
      for (let restart = 1; restart <= 2; restart++) {
        reconnect = new ResidencyClient(options);
        await reconnect.ensureHost();
        const expected = { id: handle.id, status: original.status, text: original.text, residency: "durable" };
        expect(reconnect.statusAgent(handle.id), `restart ${restart}`).toMatchObject(expected);
        expect(await reconnect.waitAgent(handle.id, AbortSignal.timeout(2_000))).toMatchObject(expected);
        expect(record()).toEqual(original);
        expect(fs.existsSync(resultPath)).toBe(false);
        await reconnect.close();
        await killOwner();
      }
    } finally {
      await reconnect?.close();
      await client.close();
      await stopResident(state.config);
      // Reap only processes recorded by this fixture if a pre-completion assertion failed.
      for (const owned of launches.owned()) if (same(owned)) {
        try { process.kill(owned.pid, "SIGKILL"); } catch { /* already exited */ }
      }
      await waitFor(() => launches.owned().every((owned) => !same(owned)), 10_000);
      await state.participants.close();
    }
  });

  it.skipIf(process.platform !== "linux")("replaces owner and host lock whose live PID has a different start time", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-reused-pid");
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh,
      participants: state.participants, mainAgent: state.mainAgent, hostPath });
    fs.mkdirSync(state.config.residencyRoot, { recursive: true });
    const stale = { format: RESIDENT_HOST_FORMAT, hostId: client.hostId, pid: process.pid,
      processStartTime: "0", token: "stale", startedAt: 0, readyAt: 0 };
    for (const file of ["owner.json", "host.lock"]) fs.writeFileSync(path.join(state.config.residencyRoot, file), JSON.stringify(stale));
    try {
      const actor = await client.createActor({ name: "PID survivor", instructions: "Keep mailbox.", residency: "durable" });
      const owner = await client.ensureHost();
      expect(owner.pid).not.toBe(process.pid);
      expect(owner.processStartTime).toBe(processStartTime(owner.pid));
      await client.removeActor(actor.id);
    } finally {
      await client.close();
      // The fail-on-base probe leaves the planted owner in place; never signal it.
      const ownerPath = path.join(state.config.residencyRoot, "owner.json");
      if (JSON.parse(fs.readFileSync(ownerPath, "utf8")).pid === process.pid) fs.rmSync(ownerPath);
      await stopResident(state.config); await state.participants.close();
    }
  });

  it.each([
    ["SIGTERM", false, false], ["SIGKILL", false, false], ["SIGKILL", true, false], ["SIGKILL", false, true],
    ["SIGKILL", "ownerless", false],
  ] as const)("relaunches a dead durable host on ordinary tell after %s (fresh dead mesh lock: %s) (fresh dead registry lock: %s)", { timeout: 100_000 }, async (signal, deadMeshLock, deadRegistryLock) => {
    const state = await rootHarness(`resident-relaunch-${signal}`);
    const launches = launchLog(state.root);
    for (const [key, value] of Object.entries(launches.env)) vi.stubEnv(key, value);
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker, runRoot: path.join(state.root, "parent-runs"),
      mainAgentId: state.identity.id, meshRoot: state.config.meshRoot,
      projectRoot: repo, hostId: state.identity.id, identityId: state.identity.id,
    });
    const actors = new ActorManager(state.config.sessionId, state.identity, state.mesh,
      state.meshConfig, agents, () => {}, {
        actorRoot: state.config.actorRoot, persistent: true, claimResidency: "session", rootId: state.identity.id,
        canManageActor: (id) => state.participants.get(id)?.ownerHostId === state.identity.id,
      });
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh,
      participants: state.participants, mainAgent: state.mainAgent, hostPath });
    // Deliberately do not start the watchdog: this is the ordinary message path.
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true, hostId: state.identity.id, pollMs: 20, acknowledgementTimeoutMs: 3_000,
    });
    control.start(() => ({ accepted: false }));
    const router = new AgentMessageRouter(agents, actors, state.mainAgent, state.participants,
      control, (binding) => binding, client);
    const ownerPath = path.join(state.config.residencyRoot, "owner.json");
    const owner = () => JSON.parse(fs.readFileSync(ownerPath, "utf8")) as ResidentHostOwner;
    const registry = () => JSON.parse(fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8")) as
      { actors: Array<{ id: string; messages: Array<{ text?: string; data?: { message?: string } }>; queue: unknown[] }> };
    let senderRestart: Promise<void> | undefined;
    let ownerlessLockCreatedAt: number | undefined;
    try {
      const actor = await client.createActor({ name: "restart survivor", instructions: "Keep id and mailbox.", residency: "durable", coalesce: false });
      await router.routeMessage(actor.id, "before death", undefined, "followUp");
      await waitFor(() => (registry().actors.find((item) => item.id === actor.id)?.messages.length ?? 0) >= 2);
      const before = registry().actors.find((item) => item.id === actor.id)!;
      const killed = owner();
      // Never signal a PID that outlived our launch identity.
      const recorded = launches.owned().find((entry) => entry.pid === killed.pid)!;
      expect(recorded).toBeDefined();
      expect(same(recorded)).toBe(true);
      if (killed.processStartTime) expect(killed.processStartTime).toBe(recorded.started);
      process.kill(killed.pid, signal);
      await waitFor(() => !same(recorded), 20_000);
      // Send immediately: complete dead identities must be recoverable without waiting for
      // the 30 s window reserved for missing/corrupt owner records.
      if (deadMeshLock) {
        // Deterministically model SIGKILL inside a mesh write, without changing MeshStore's fence.
        // Pause our own mesh publishers so they cannot race this owner-file publication.
        await state.participants.close();
        await control.close();
        const meshLock = path.join(state.config.meshRoot, ".lock");
        await waitFor(() => !fs.existsSync(meshLock), 35_000);
        fs.mkdirSync(meshLock);
        // SIGKILL between mkdir and owner publication (or during legacy release) leaves no owner.
        if (deadMeshLock !== "ownerless") {
          fs.writeFileSync(path.join(meshLock, "owner"), `dead-host\n${killed.pid}\n${Date.now()}\n`);
        } else {
          expect(fs.existsSync(path.join(meshLock, "owner"))).toBe(false);
          ownerlessLockCreatedAt = fs.statSync(meshLock).mtimeMs;
        }
        // Resume the sender after publishing the dead identity or fresh ownerless directory.
        senderRestart = state.participants.start().catch(() => undefined);
        control.start(() => ({ accepted: false }));
      }
      if (deadRegistryLock) {
        // SIGKILL after registry rename can leave its lock younger than acquisition's 5 s
        // deadline. Use the verified dead launch identity, never an arbitrary/live PID.
        const registryLock = path.join(state.config.actorRoot, "actors.json.lock");
        fs.mkdirSync(registryLock, { recursive: true });
        fs.writeFileSync(path.join(registryLock, "owner"), `dead-host\n${killed.pid}\n${Date.now()}\n${recorded.started}\n`);
      }
      const started = Date.now();
      const result = await router.routeMessage(actor.id, `after ${signal}`, undefined, "followUp");
      expect(result.queued).toBe(true);
      expect(Date.now() - started).toBeLessThan(30_000 + 15_000);
      if (ownerlessLockCreatedAt !== undefined) {
        expect(Date.now() - ownerlessLockCreatedAt).toBeGreaterThan(30_000);
        const fences = fs.readdirSync(state.config.meshRoot).filter(name => name.startsWith(".lock.dead."));
        expect(fences.some(name => fs.existsSync(path.join(state.config.meshRoot, name, ".recovery-fence")))).toBe(true);
      }
      await senderRestart;
      expect(owner().pid).not.toBe(killed.pid);
      await router.routeMessage(actor.id, "queued successor", undefined, "followUp");
      await waitFor(() => (registry().actors.find((item) => item.id === actor.id)?.messages.length ?? 0) >= before.messages.length + 4);
      const after = registry().actors.find((item) => item.id === actor.id)!;
      expect(after.id).toBe(actor.id);
      expect(after.messages.slice(0, before.messages.length)).toEqual(before.messages);
      expect(after.messages.map((item) => item.data?.message)).toEqual(expect.arrayContaining([`after ${signal}`, "queued successor"]));
      await client.removeActor(actor.id);
    } finally {
      await senderRestart;
      await client.close();
      await stopResident(state.config);
      await control.close();
      await actors.close();
      await agents.close();
      await state.participants.close();
    }
  });

  it.each([false, true])("preserves a resident-host ASK activation at the Main ceiling (queued=%s)", { timeout: 45_000 }, async queued => {
    const state = await rootHarness(`resident-main-ceiling-${queued}`);
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath });
    const control = new FabricControlPlane(state.mesh, state.identity, { enabled: true, hostId: state.identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000 });
    control.start(() => ({ accepted: false }));
    const controller = new AbortController();
    let first: Promise<unknown> | undefined;
    try {
      client.start();
      const actor = await client.createActor({ name: "durable survivor", instructions: "Reply.", residency: "durable", transport: "process", responseMode: "text", delivery: "followUp", triggerTurn: false });
      const participant = () => state.participants.get(actor.id, undefined, { fresh: true });
      const current = () => state.mesh.get(`actors/${state.config.sessionId}/${actor.id}`, { fresh: true })?.value as import("../src/actors/types.js").FabricActorInfo | undefined;
      if (queued) {
        first = control.requestResult(client.hostId, actor.id, "ask", { message: "LIVE_WITHOUT_PROGRESS" }, client.hostId, { timeoutMs: 10_000 }).catch(error => error);
        await waitFor(() => participant()?.actorRun !== undefined);
      }
      const observation = control.requestResult(client.hostId, actor.id, "ask", { message: queued ? "accepted durable queue item" : "LIVE_WITHOUT_PROGRESS" }, client.hostId, { timeoutMs: 10_000, signal: controller.signal, detachOnMainCeiling: true }).catch(error => error);
      await waitFor(() => queued ? current()?.queued === 1 : current()?.inFlightRun !== undefined);
      const runId = current()!.inFlightRun!.id;
      const ceiling = createMainExecutionCeilingError(700);
      controller.abort(ceiling);
      const rejection = await observation;
      await delay(100);
      expect(state.mesh.read({ topic: "fabric.control.command", limit: 50 }).filter(event => event.kind === "cancel")).toHaveLength(0);
      expect(rejection).toBe(ceiling);
      expect(current()!.inFlightRun!.id).toBe(runId);
      if (queued) expect(current()!.queued).toBe(1);
      const worker = JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "runs", runId, "status.json"), "utf8"));
      expect(worker).toMatchObject({ status: "running", turns: 0, toolCalls: 0 });
      await first;
      await waitFor(() => state.deliveries.length === (queued ? 2 : 1) && participant()?.actorRun === undefined);
      const persisted = new ActorRegistryStore(state.config.actorRoot).records().find(record => record.id === actor.id)!;
      expect((persisted.messages as FabricActorMessage[]).filter(message => message.direction === "out")).toHaveLength(queued ? 2 : 1);
      await delay(200);
      expect(state.deliveries).toHaveLength(queued ? 2 : 1);
      // An explicit remote stop still owns activation lifetime after observation expiry.
      const again = control.requestResult(client.hostId, actor.id, "ask", { message: "HANG" }, client.hostId, { timeoutMs: 10_000 }).catch(error => error);
      await waitFor(() => participant()?.actorRun !== undefined);
      await control.request(client.hostId, actor.id, "stop", {}, client.hostId);
      await again;
      await waitFor(() => participant()?.actorRun === undefined);
      expect(state.deliveries).toHaveLength(queued ? 2 : 1);
      await client.removeActor(actor.id);
    } finally { controller.abort(); await first; await control.close(); await client.close(); await state.participants.close(); await stopResident(state.config); }
  });
  it("keeps a durable actor responsive after its originating Main closes", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-actor");
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(state.root, "parent-runs"),
      mainAgentId: state.identity.id,
      meshRoot: state.config.meshRoot,
      projectRoot: repo,
      hostId: state.identity.id,
      identityId: state.identity.id,
    });
    const canManage = (id: string): boolean | undefined => {
      const participant = state.participants.get(id);
      return participant ? participant.ownerHostId === state.identity.id : undefined;
    };
    const actors = new ActorManager(
      state.config.sessionId,
      state.identity,
      state.mesh,
      state.meshConfig,
      agents,
      () => {},
      {
        actorRoot: state.config.actorRoot,
        persistent: true,
        canManageActor: canManage,
        claimResidency: "session",
        rootId: state.identity.id,
      },
    );
    state.participants.registerSource(() =>
      actors.listOwned().map((actor) =>
        actorParticipantRecord(
          actor,
          state.identity.id,
          state.identity.id,
          state.identity.id,
          state.identity.id,
        ),
      ),
    );
    actors.subscribe(() => state.participants.scheduleRefresh());
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const actor = await actors.create({
      name: "resident actor",
      instructions: "Persist across Main shutdown.",
      residency: "durable",
      delivery: "mailbox",
    });
    await actors.cede(actor.id);
    await state.participants.refresh();
    await client.ensureActor(actor.id);

    const messageCount = (): number => {
      const registry = JSON.parse(
        fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8"),
      ) as { actors: Array<{ id: string; messages?: unknown[] }> };
      return registry.actors.find((candidate) => candidate.id === actor.id)?.messages?.length ?? 0;
    };
    const originalControl = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    originalControl.start(() => ({ accepted: false }));
    await originalControl.request(
      client.hostId,
      actor.id,
      "followUp",
      { message: "before Main shutdown" },
      client.hostId,
    );
    await waitFor(() => messageCount() >= 2);
    await originalControl.close();

    await actors.close();
    await agents.close();
    await state.participants.close();
    await client.close();

    const peerIdentity: MeshIdentity = {
      id: `session:peer:${randomId()}`,
      name: "peer",
      kind: "main",
      sessionId: "peer",
    };
    const peerControl = new FabricControlPlane(state.mesh, peerIdentity, {
      enabled: true,
      hostId: peerIdentity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    peerControl.start(() => ({ accepted: false }));
    const before = messageCount();
    await peerControl.request(
      residentHostId(state.identity.id),
      actor.id,
      "followUp",
      { message: "after Main shutdown" },
      residentHostId(state.identity.id),
    );
    await waitFor(() => messageCount() >= before + 2);
    await peerControl.request(
      residentHostId(state.identity.id),
      actor.id,
      "stop",
      {},
      residentHostId(state.identity.id),
    );
    await peerControl.close();

    expect(messageCount()).toBeGreaterThanOrEqual(before + 2);
    const detachedParticipants: FabricParticipantSource = {
      list: () => [],
      get: () => undefined,
      self: () => {
        throw new Error("not used by detached residency client");
      },
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const reconnect = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: detachedParticipants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    await expect(reconnect.removeActor(actor.id)).resolves.toEqual({ removed: true });
    // review/astra on pi-fabric#136: actor activations leave no saved durable task result.
    const resultsDir = path.join(state.config.residencyRoot, "results");
    expect(fs.existsSync(resultsDir) ? fs.readdirSync(resultsDir) : []).toEqual([]);
    const registry = JSON.parse(
      fs.readFileSync(path.join(state.config.actorRoot, "actors.json"), "utf8"),
    ) as { actors: Array<{ id: string }> };
    expect(registry.actors.some((candidate) => candidate.id === actor.id)).toBe(false);
    await reconnect.close();
  });

  // smarty-dev#2184 item 8: a removal waited for the actor's in-flight run and blocked every
  // later request to the host (clients timed out; a create right after it was dropped).
  it("returns a removal behind an in-flight run at once and keeps serving requests", { timeout: 60_000 }, async () => {
    const testStarted = Date.now();
    const state = await rootHarness("resident-remove-pending");
    state.config.agents = { ...state.config.agents, timeoutMs: 120_000 };
    // The resident host and its workers inherit this and record themselves at launch: the worker
    // to kill is found there, never by a host-wide command-line match (pi-fabric#160 S4).
    const launches = launchLog(state.root);
    const savedEnv = Object.fromEntries(Object.keys(launches.env).map((key) => [key, process.env[key]]));
    Object.assign(process.env, launches.env);
    const client = new ResidencyClient({
      config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath,
    });
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true, hostId: state.identity.id, pollMs: 20, acknowledgementTimeoutMs: 5_000,
    });
    control.start(() => ({ accepted: false }));
    try {
      const request = { name: "hung reviewer", instructions: "Review.", residency: "durable" as const, delivery: "mailbox" as const };
      const actor = await client.createActor(request);
      await control.request(client.hostId, actor.id, "followUp", { message: "HANG_WITH_PROGRESS" }, client.hostId);
      await waitFor(() => state.participants.get(actor.id, undefined, { fresh: true })?.actorRun !== undefined, 30_000);
      const runId = state.participants.get(actor.id, undefined, { fresh: true })!.actorRun!.id;

      const started = Date.now();
      const removed = await client.removeActor(actor.id);
      expect(Date.now() - started).toBeLessThan(2_000);
      expect(removed.pending).toContain(`pending behind its in-flight run ${runId}`);
      expect(client.hostStateNote()).toContain(`removal of hung reviewer (${actor.id}) is pending behind its in-flight run ${runId}`);

      // The queue is free: a same-name create right after the removal succeeds.
      const successor = await client.createActor(request);
      expect(successor.id).not.toBe(actor.id);

      // The old run ends (its worker is killed); the removal then finishes and cleans up.
      // Of the processes this fixture launched, the one started as the worker of this run.
      const workers = launches.owned().filter(({ argv }) =>
        argv[0] === fakeWorker && argv[argv.indexOf("--id") + 1] === runId);
      expect(workers).toHaveLength(1);
      const worker = workers[0]!;
      expect(startedAtMs(worker.started)).toBeGreaterThanOrEqual(testStarted - 1_000);
      // Revalidated just before the signal: the pid still names the recorded process.
      expect(same(worker)).toBe(true);
      process.kill(worker.pid, "SIGKILL");
      const registryIds = (): string[] => [state.config.actorRoot, state.config.sessionActorRoot!].flatMap((root) => {
        try {
          return (JSON.parse(fs.readFileSync(path.join(root, "actors.json"), "utf8")) as { actors: Array<{ id: string }> })
            .actors.map((entry) => entry.id);
        } catch { return []; }
      });
      expect(registryIds()).toContain(actor.id);
      await waitFor(() => !registryIds().includes(actor.id), 30_000);
      expect(registryIds()).toContain(successor.id);
      await waitFor(() => client.hostStateNote() === "", 5_000);
      await client.removeActor(successor.id);
    } finally {
      await control.close();
      await client.close();
      await stopResident(state.config);
      await state.participants.close();
      for (const [key, value] of Object.entries(savedEnv)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });

  it("queues passive actor delivery until Main resumes", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-delivery");
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(state.root, "parent-delivery-runs"),
      mainAgentId: state.identity.id,
      meshRoot: state.config.meshRoot,
      projectRoot: repo,
      hostId: state.identity.id,
      identityId: state.identity.id,
    });
    const actors = new ActorManager(
      state.config.sessionId,
      state.identity,
      state.mesh,
      state.meshConfig,
      agents,
      () => {},
      {
        actorRoot: state.config.actorRoot,
        persistent: true,
        claimResidency: "session",
        rootId: state.identity.id,
      },
    );
    state.participants.registerSource(() =>
      actors.listOwned().map((actor) =>
        actorParticipantRecord(
          actor,
          state.identity.id,
          state.identity.id,
          state.identity.id,
          state.identity.id,
        ),
      ),
    );
    actors.subscribe(() => state.participants.scheduleRefresh());
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const actor = await actors.create({
      name: "resident delivery",
      instructions: "Reply to every message.",
      residency: "durable",
      delivery: "followUp",
      triggerTurn: false,
    });
    await actors.cede(actor.id);
    await state.participants.refresh();
    await client.ensureActor(actor.id);
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    control.start(() => ({ accepted: false }));
    await control.request(client.hostId, actor.id, "followUp", { message: "respond" }, client.hostId);
    const prefix = residentDeliveryPrefix(state.identity.id);
    await waitFor(() => state.mesh.listAll(prefix).length === 1);
    expect(state.deliveries).toEqual([]);

    client.start();
    await waitFor(() => state.deliveries.length === 1);
    expect(state.deliveries[0]).toMatchObject({
      from: { id: actor.id, kind: "actor" },
      delivery: "followUp",
      triggerTurn: false,
      message: "fake worker complete",
    });
    // Delivery lands before MeshStore.delete completes its locked write, so
    // poll the queue drain instead of asserting the removal synchronously.
    await waitFor(() => state.mesh.listAll(prefix).length === 0);
    expect(state.mesh.listAll(prefix)).toEqual([]);

    await client.removeActor(actor.id);
    await control.close();
    await client.close();
    await actors.close();
    await agents.close();
    await state.participants.close();
  });

  it("creates durable actors through root and nested owner channels while Main owns the registry", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-recruitment");
    const agents = new AgentManager(repo, state.config.agents, {
      workerPath: fakeWorker,
      runRoot: path.join(state.root, "recruitment-runs"),
      mainAgentId: state.identity.id,
      meshRoot: state.config.meshRoot,
      projectRoot: repo,
      hostId: state.identity.id,
      identityId: state.identity.id,
    });
    const canManage = (id: string): boolean | undefined => {
      const participant = state.participants.get(id);
      return participant ? participant.ownerHostId === state.identity.id : undefined;
    };
    const actors = new ActorManager(
      state.config.sessionId,
      state.identity,
      state.mesh,
      state.meshConfig,
      agents,
      () => {},
      {
        actorRoot: state.config.actorRoot,
        persistent: true,
        canManageActor: canManage,
        claimResidency: "session",
        rootId: state.identity.id,
      },
    );
    state.participants.registerSource(() =>
      actors.listOwned().map((actor) =>
        actorParticipantRecord(
          actor,
          state.identity.id,
          state.identity.id,
          state.identity.id,
          state.identity.id,
        ),
      ),
    );
    actors.subscribe(() => state.participants.scheduleRefresh());
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    try {
      const mainOwned = await actors.create({
        name: "main registry owner",
        instructions: "Keep the local registry guarded.",
        residency: "session",
      });
      await state.participants.refresh();
      expect(state.participants.get(mainOwned.id)?.ownerHostId).toBe(state.identity.id);

      const first = await client.createActor({
        name: "recruited architect",
        instructions: "Design the bounded change.",
        residency: "durable",
      });
      expect(state.participants.get(first.id)).toMatchObject({
        ownerHostId: client.hostId,
        residency: "durable",
      });

      const recruitment = new ResidentActorClient(
        state.config.meshRoot,
        state.identity.id,
      );
      const second = await recruitment.createActor({
        scope: "session",
        name: "recruited advisor",
        instructions: "Challenge the design.",
        residency: "durable",
      });
      await waitFor(() => state.participants.get(second.id)?.ownerHostId === client.hostId);

      const residentActors = [first, second];
      expect(residentActors.map((actor) => actor.name).sort()).toEqual([
        "recruited advisor",
        "recruited architect",
      ]);
      expect(residentActors.every((actor) => actor.residency === "durable")).toBe(true);
      expect(first.scope).toBe("project");
      expect(second.scope).toBe("session");
      // smarty-dev#878: resident-side creation records the creating project too.
      expect(residentActors.map((actor) => actor.project)).toEqual([projectOf(state.config.cwd), projectOf(state.config.cwd)]);
      expect(second.sessionFile).toContain(path.join(state.config.sessionActorRoot!, second.id));
      expect(new Set(residentActors.map((actor) => actor.sessionFile)).size).toBe(2);

      for (const actor of residentActors) await client.removeActor(actor.id);
      await actors.remove(mainOwned.id);
    } finally {
      await client.close();
      await actors.close();
      await agents.close();
      await state.participants.close();
    }
  });

  it.each(["session", "project"] as const)("routes root Main durable setters (%s/default), reads instructions back, and persists across host restart", { timeout: 45_000 }, async (scope) => {
    const state = await rootHarness(`resident-setters-${scope}`);
    const client = new ResidencyClient({ config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath });
    const control = new FabricControlPlane(state.mesh, state.identity, { enabled: true, hostId: state.identity.id, pollMs: 20 });
    control.start(() => ({ accepted: false }));
    const agents = new AgentManager(repo, state.config.agents, { workerPath: fakeWorker, runRoot: path.join(state.root, "passive-runs") });
    const passive = new ActorDirectory([state.config.sessionId, state.identity, state.mesh, state.meshConfig, agents, () => {},
      { persistent: true, rootId: state.identity.id, canManageActor: () => false }],
      { project: state.config.actorRoot, session: state.config.sessionActorRoot! }, "project");
    const lifecycle = new LifecycleBroker(state.mesh, state.identity, state.participants,
      { enabled: true, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const provider = new AgentsProvider(agents, passive, new GlobalActorRegistry(state.root, 64 * 1024),
      state.mainAgent, state.participants, control, lifecycle, undefined, client);
    const context: FabricInvocationContext = { cwd: repo, signal: undefined, parentToolCallId: "test", nestedToolCallId: "setter",
      // Main now admits the selector before routing to the resident owner.
      // Give this root session its actual visible registry, not an empty context.
      extensionContext: { modelRegistry: { getAvailable: () => state.config.piModels?.available ?? [] } } as unknown as FabricInvocationContext["extensionContext"], update() {}, activity() {} };
    try {
      const actor = await client.createActor({ name: "effective durable", instructions: "Before", residency: "durable", model: "provider/visible", thinking: "low" });
      expect(passive.owns(actor.id)).toBe(false);
      const updated = await provider.invoke("setInstructions", { id: actor.id, instructions: "After" }, context) as import("../src/actors/types.js").FabricActorInfo;
      const readback = await provider.invoke("instructions", { id: actor.id }, context);
      expect(readback).toMatchObject({ instructions: "After", instructionsDigest: updated.instructionsDigest, instructionsLength: 5 });
      const bindingScope = scope === "project" ? { scope } : {}; // Default scope must route too.
      await provider.invoke("setModel", { id: actor.id, model: "deepseek/deepseek-chat", ...bindingScope }, context);
      await provider.invoke("setThinking", { id: actor.id, thinking: "max", ...bindingScope }, context);
      await provider.invoke("setTools", { id: actor.id, tools: ["read"] }, context);
      const defaults = scope === "project" ? { model: "deepseek/deepseek-chat", thinking: "max" } : { model: "provider/visible", thinking: "low" };
      await expect(provider.invoke("actorStatus", { id: actor.id }, context)).resolves.toMatchObject({ model: "deepseek/deepseek-chat", thinking: "max", instructionsDigest: updated.instructionsDigest, tools: ["read"], projectDefaults: defaults });
      // A foreign Main must not promote its caller-local overlay into this host's registry.
      const foreignMain = { ...state.mainAgent, id: "session:foreign" };
      const foreign = new AgentsProvider(agents, passive, new GlobalActorRegistry(state.root, 64 * 1024),
        foreignMain, state.participants, control, lifecycle, undefined, client);
      for (const [operation, args] of [["setInstructions", { instructions: "bad" }], ["setModel", { model: "provider/visible", scope: "project" }],
        ["setThinking", { thinking: "low", scope: "project" }], ["setTools", { tools: ["bash"] }]] as const) {
        await expect(foreign.invoke(operation, { id: actor.id, ...args }, context)).rejects.toThrow("owned by another host");
      }
      // The request handler itself also rejects a mismatched root, independent of provider routing.
      const requestId = `foreign-${scope}`;
      fs.writeFileSync(path.join(state.config.residencyRoot, "requests", `${requestId}.json`), JSON.stringify({
        format: RESIDENT_HOST_FORMAT, requestId, rootId: foreignMain.id, operation: "setInstructions", id: actor.id, instructions: "bad", createdAt: Date.now(),
      }));
      const responsePath = path.join(state.config.residencyRoot, "responses", `${requestId}.json`);
      await waitFor(() => fs.existsSync(responsePath));
      expect(JSON.parse(fs.readFileSync(responsePath, "utf8"))).toMatchObject({ ok: false, error: "Invalid Fabric residency request" });
      const activate = async (message: string) => {
        const asked = await provider.invoke("ask", { id: actor.id, message: `ECHO_MODEL ${message}` }, context) as FabricActorMessage;
        expect(asked.text).toBe("model deepseek/deepseek-chat");
        const runFile = path.join(actor.logDir!, asked.runId!, "status.json");
        await waitFor(() => fs.existsSync(runFile));
        expect(JSON.parse(fs.readFileSync(runFile, "utf8"))).toMatchObject({ model: "deepseek/deepseek-chat", thinking: "max", systemPrompt: expect.stringContaining("After") });
      };
      await activate("effective");
      await stopResident(state.config); await client.ensureHost();
      await expect(provider.invoke("instructions", { id: actor.id }, context)).resolves.toMatchObject({ instructions: "After", instructionsDigest: updated.instructionsDigest });
      await expect(provider.invoke("actorStatus", { id: actor.id }, context)).resolves.toMatchObject({ model: "deepseek/deepseek-chat", thinking: "max", tools: ["read"], projectDefaults: defaults });
      await activate("persisted");
      await client.removeActor(actor.id);
    } finally {
      await control.close(); await client.close(); await passive.close(); await agents.close(); await lifecycle.close(); await state.participants.close();
    }
  });

  it("rejects a durable actor model that became hidden at the resident owner", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-hidden-model");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 3_000,
    });
    control.start(() => ({ accepted: false }));

    try {
      const actor = await client.createActor({
        name: "visibility witness",
        instructions: "Run only while the bound model remains visible.",
        residency: "durable",
        runner: "pi",
        model: "provider/visible",
      });
      state.config.piModels = {
        available: [],
        aliases: {},
        defaultModel: "provider/visible",
      };
      await client.ensureHost();

      await expect(
        control.request(
          client.hostId,
          actor.id,
          "followUp",
          { message: "Do not launch the hidden binding" },
          client.hostId,
        ),
      ).rejects.toThrow(/not available to this Pi session/);
      await client.removeActor(actor.id);
    } finally {
      await control.close();
      await client.close();
      await state.participants.close();
      await stopResident(state.config);
    }
  });

  it("resolves a model added to models.json after the resident host started, with no restart", { timeout: 60_000 }, async () => {
    // pi-fabric#138: the resident host runs in its own Pi process; its registry, not the
    // session's snapshot from host start, must discover the new exact id on ask and tell.
    const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-resident-models-"));
    roots.push(agentDir);
    const modelsPath = path.join(agentDir, "models.json");
    const writeModels = (ids: string[]) => fs.writeFileSync(modelsPath, JSON.stringify({
      providers: {
        "probe-late": {
          baseUrl: "http://127.0.0.1:9/v1",
          api: "openai-completions",
          apiKey: "probe",
          models: ids.map((id) => ({ id })),
        },
      },
    }));
    writeModels(["claude-late-5-5"]);
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    const state = await rootHarness("resident-late-model");
    state.config.piModels = {
      available: [{ provider: "probe-late", id: "claude-late-5-5" }],
      aliases: {},
      defaultModel: "probe-late/claude-late-5-5",
    };
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const control = new FabricControlPlane(state.mesh, state.identity, {
      enabled: true,
      hostId: state.identity.id,
      pollMs: 20,
      acknowledgementTimeoutMs: 5_000,
    });
    control.start(() => ({ accepted: false }));
    const ownerPid = () =>
      (JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner).pid;
    try {
      const actor = await client.createActor({
        name: "late model witness",
        instructions: "Reply with the model of each run.",
        residency: "durable",
        runner: "pi",
        model: "probe-late/claude-late-5-5",
      });
      const pid = ownerPid();
      writeModels(["claude-late-5-5", "claude-late-5-6"]);

      const asked = await control.requestResult<FabricActorMessage>(
        client.hostId,
        actor.id,
        "ask",
        { message: "ECHO_MODEL ping", binding: { model: "probe-late/claude-late-5-6" } },
        client.hostId,
        { timeoutMs: 20_000 },
      );
      expect(asked).toMatchObject({ text: "model probe-late/claude-late-5-6" });

      writeModels(["claude-late-5-5", "claude-late-5-6", "claude-late-5-7"]);
      await new Promise((resolve) => setTimeout(resolve, 10_500)); // past the shared refresh throttle
      await control.request(
        client.hostId,
        actor.id,
        "followUp",
        { message: "ECHO_MODEL pong", binding: { model: "probe-late/claude-late-5-7" } },
        client.hostId,
      );
      await waitFor(() => state.mesh.read({ topic: "fabric.actor.output", limit: 50 })
        .some((event) => event.text?.includes("model probe-late/claude-late-5-7")), 15_000);

      // A model that is still not configured fails after the refresh, not with a fuzzy stand-in.
      await expect(control.request(
        client.hostId,
        actor.id,
        "followUp",
        { message: "nope", binding: { model: "probe-missing/claude-late-5-9" } },
        client.hostId,
      )).rejects.toThrow(/not available to this Pi session/);
      expect(ownerPid()).toBe(pid);
      await client.removeActor(actor.id);
    } finally {
      await control.close();
      await client.close();
      await state.participants.close();
      await stopResident(state.config);
    }
  });

  it("applies live model guidance snapshots to durable participants", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-guidance");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const guidance = (content: string) => [{
      componentId: "deepseek-guidance",
      component: "deepseek-guidance",
      revision: 1,
      label: "deepseek",
      models: ["deepseek/*"],
      targets: ["participant" as const],
      placement: "append" as const,
      content,
    }];

    client.updateModelGuidance(guidance("First durable guidance"));
    const first = await client.spawnAgent({
      task: "First guided durable agent",
      transport: "process",
      residency: "durable",
      model: "deepseek/deepseek-chat",
    });
    const firstResult = await client.waitAgent(first.id);
    expect((firstResult as typeof firstResult & { systemPrompt?: string }).systemPrompt).toBe(
      "First durable guidance",
    );

    client.updateModelGuidance(guidance("Revised durable guidance"));
    const second = await client.spawnAgent({
      task: "Second guided durable agent",
      transport: "process",
      residency: "durable",
      model: "deepseek/deepseek-chat",
    });
    const secondResult = await client.waitAgent(second.id);
    expect((secondResult as typeof secondResult & { systemPrompt?: string }).systemPrompt).toBe(
      "Revised durable guidance",
    );

    await client.cleanupAgent(first.id);
    await client.cleanupAgent(second.id);
    await client.close();
    await state.participants.close();
  });

  it("completes and cleans a durable agent after its originating Main closes", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-agent");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const handle = await client.spawnAgent({
      task: "STREAM_PREVIEW",
      transport: "process",
      residency: "durable",
    });
    expect(handle.residency).toBe("durable");

    await client.close();
    await state.participants.close();

    const detachedParticipants: FabricParticipantSource = {
      list: () => [],
      get: () => undefined,
      self: () => {
        throw new Error("not used by detached residency client");
      },
      peers: () => [],
      async refresh() {},
      scheduleRefresh() {},
    };
    const reconnect = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: detachedParticipants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const result = await reconnect.waitAgent(handle.id);
    expect(result).toMatchObject({
      id: handle.id,
      status: "completed",
      residency: "durable",
      text: "stream preview complete",
    });
    await expect(reconnect.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    expect(reconnect.hasAgent(handle.id)).toBe(false);
    await reconnect.close();
  });

  // smarty-dev#883: on SIGTERM, Pi exited before the host's close stopped its
  // durable workers, which ran on as orphans and kept writing.
  it("stops its durable workers before a SIGTERM'd resident host exits", { timeout: 60_000 }, async () => {
    const state = await rootHarness("resident-sigterm");
    const client = new ResidencyClient({
      config: state.config, mesh: state.mesh, participants: state.participants, mainAgent: state.mainAgent, hostPath,
    });
    try {
      const handle = await client.spawnAgent({ task: "HANG", transport: "process", residency: "durable" });
      const workerPid = (): number | undefined => execFileSync("ps", ["-eo", "pid=,args="], { encoding: "utf8" })
        .split("\n").filter((line) => line.includes(handle.id) && line.includes(fakeWorker))
        .map((line) => Number(line.trim().split(/\s+/)[0]))[0];
      await waitFor(() => workerPid() !== undefined, 30_000);
      const worker = workerPid()!;
      const host = (JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "owner.json"), "utf8")) as ResidentHostOwner).pid;
      const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
      process.kill(host, "SIGTERM");
      await waitFor(() => !alive(host), 30_000);
      // The worker's exit is part of the host's close, so it is already gone.
      expect(alive(worker)).toBe(false);
    } finally {
      await client.close();
      await state.participants.close();
    }
  });

  // smarty-dev#1882: the host's exit removes runs/; a later session still reads the result.
  it("returns a completed durable agent's result after its resident host exits", { timeout: 90_000 }, async () => {
    const state = await rootHarness("resident-exit-status");
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });
    const handle = await client.spawnAgent({ task: "STREAM_PREVIEW", transport: "process", residency: "durable" });
    expect((await client.waitAgent(handle.id)).status).toBe("completed");
    await client.close();
    await state.participants.close();
    // The real path: the host exits after 30 s idle and its close removes runs/.
    const runsDir = path.join(state.config.residencyRoot, "runs");
    await waitFor(() => !fs.existsSync(path.join(state.config.residencyRoot, "owner.json")), 60_000);
    // owner.json goes last: nothing may write under mesh/ once it is gone, or a new host
    // started in that window, and every cleanup that trusts the marker, races the old one.
    const snapshot = (): Map<string, string> => {
      const files = new Map<string, string>();
      const walk = (directory: string): void => {
        for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
          const file = path.join(directory, entry.name);
          if (entry.isDirectory()) walk(file);
          else {
            try {
              const stat = fs.statSync(file);
              files.set(file, `${stat.size}:${stat.mtimeMs}`);
            } catch { /* removed meanwhile */ }
          }
        }
      };
      walk(path.join(state.root, "mesh"));
      return files;
    };
    const released = snapshot();
    await delay(2_000);
    const late = [...snapshot()].filter(([file, stamp]) => released.get(file) !== stamp)
      .map(([file]) => path.relative(state.root, file));
    expect(late).toEqual([]);
    expect(fs.existsSync(path.join(runsDir, handle.id))).toBe(false);

    const reconnect = new ResidencyClient({ ...client.options, hostPath });
    expect(reconnect.statusAgent(handle.id)).toMatchObject({
      id: handle.id,
      status: "completed",
      residency: "durable",
      text: "stream preview complete",
    });
    expect(await reconnect.waitAgent(handle.id)).toMatchObject({ status: "completed", text: "stream preview complete" });
    await expect(reconnect.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    expect(fs.existsSync(residentResultPath(state.config.residencyRoot, handle.id))).toBe(false);
    await reconnect.close();
  });

  it("rejects tampered durable worktree metadata before destructive cleanup", { timeout: 45_000 }, async () => {
    const state = await rootHarness("resident-worktree-tamper");
    const source = path.join(state.root, "source");
    const unrelated = path.join(state.root, "unrelated");
    initRepository(source);
    initRepository(unrelated);
    const id = randomId().padEnd(32, "0").slice(0, 32);
    const branch = `pi-fabric/tampered-${id.slice(0, 8)}`;
    const worktree = path.join(source, ".pi", "fabric", "worktrees", id);
    fs.mkdirSync(path.dirname(worktree), { recursive: true });
    git(source, "worktree", "add", "-q", "-b", branch, worktree, "HEAD");
    const runDirectory = path.join(state.config.residencyRoot, "runs", id);
    fs.mkdirSync(runDirectory, { recursive: true });
    fs.writeFileSync(path.join(runDirectory, "status.json"), JSON.stringify({
      id,
      name: "tampered worktree",
      task: "test",
      status: "completed",
      runner: "pi",
      transport: "process",
      sessionId: "2147483647", // Confirmed-absent parent: exercise the worktree fence, not the exit veto.
      cwd: worktree,
      startedAt: 1,
      updatedAt: 1,
      finishedAt: 1,
      turns: 0,
      toolCalls: 0,
      text: "complete",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0 },
    }));
    const metadataPath = path.join(state.config.residencyRoot, "agents", `${id}.json`);
    const metadata = {
      format: RESIDENT_HOST_FORMAT,
      rootId: state.identity.id,
      id,
      runDirectory,
      handle: {
        id,
        name: "tampered worktree",
        status: "completed",
        runner: "pi",
        transport: "process",
        cwd: worktree,
        residency: "durable",
        branch,
        worktree,
      },
      worktreeGitRoot: unrelated,
      createdAt: 1,
      updatedAt: 1,
    };
    fs.mkdirSync(path.dirname(metadataPath), { recursive: true });
    fs.writeFileSync(metadataPath, JSON.stringify(metadata));
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    try {
      await expect(client.cleanupAgent(id, true)).rejects.toThrow(/not registered/);
      expect(worktreeBranches(source)).toContain(branch);
      expect(git(source, "branch", "--list", branch)).toContain(branch);
      expect(fs.existsSync(runDirectory)).toBe(true);

      fs.writeFileSync(metadataPath, JSON.stringify({ ...metadata, worktreeGitRoot: source }));
      const controller = new AbortController();
      const originalRealpath = fs.realpathSync.native;
      const validate = vi.spyOn(fs.realpathSync, "native").mockImplementation((...args) => {
        const resolved = originalRealpath(...args);
        controller.abort(new Error("cleanup aborted during worktree validation"));
        return resolved;
      });
      try {
        await expect(client.cleanupAgent(id, true, controller.signal)).rejects.toThrow("cleanup aborted during worktree validation");
      } finally { validate.mockRestore(); }
      expect(worktreeBranches(source)).toContain(branch);
      expect(git(source, "branch", "--list", branch)).toContain(branch);
      expect(fs.existsSync(runDirectory)).toBe(true); expect(fs.existsSync(metadataPath)).toBe(true);
      await expect(client.cleanupAgent(id, true)).resolves.toEqual({ cleaned: true });
      expect(worktreeBranches(source)).not.toContain(branch);
      expect(git(source, "branch", "--list", branch)).toBe("");
      expect(fs.existsSync(runDirectory)).toBe(false);
    } finally {
      try {
        git(source, "worktree", "remove", "--force", worktree);
      } catch {
        // A failed assertion may follow an implementation that already removed it.
      }
      try {
        git(source, "branch", "-D", branch);
      } catch {
        // The worktree removal may already have removed its branch.
      }
      await client.close();
      await state.participants.close();
    }
  });

  it.each([false, true])("forwards and reports a canonical durable agent cwd (recursive=%s)", { timeout: 45_000 }, async (recursive) => {
    const state = await rootHarness("resident-agent-cwd");
    state.config.cwd = state.root;
    state.config.projectRoot = state.root;
    const target = path.join(state.root, "child");
    fs.mkdirSync(target);
    if (recursive) vi.stubEnv("PI_FABRIC_TOOL_ALLOWLIST", '["read","fabric_exec"]');
    const client = new ResidencyClient({
      config: state.config,
      mesh: state.mesh,
      participants: state.participants,
      mainAgent: state.mainAgent,
      hostPath,
    });

    const handle = await client.spawnAgent({
      task: "REPORT_RECURSIVE_CWD",
      kernel: "python",
      cwd: "child",
      tools: ["read", "bash", "write"],
      recursive,
      transport: "process",
      residency: "durable",
    });
    const canonical = fs.realpathSync(target);
    expect(handle.cwd).toBe(canonical);
    expect(handle.kernel).toBe("python");

    const result = await client.waitAgent(handle.id);
    expect(result.cwd).toBe(canonical);
    if (recursive) {
      expect(result).toMatchObject({ recursive: true, tools: ["read", "fabric_exec"], grantedRisks: ["agent"] });
    }
    expect(client.statusAgent(handle.id)).toMatchObject({ cwd: canonical });
    expect(client.readAgentLog(handle.id).status?.cwd).toBe(canonical);
    const reopened = new ResidencyClient(client.options);
    expect(reopened.statusAgent(handle.id)).toMatchObject({ cwd: canonical, kernel: "python", ...(recursive ? { recursive: true } : {}) });
    await reopened.close();
    await expect(client.cleanupAgent(handle.id)).resolves.toEqual({ cleaned: true });
    await client.close();
    await state.participants.close();
  });
});
