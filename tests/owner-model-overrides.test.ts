import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import type { FabricActorInfo } from "../src/actors/types.js";
import { AgentManager } from "../src/agents/manager.js";
import { CapturedToolCatalog } from "../src/capture/catalog.js";
import { normalizeFabricConfig } from "../src/config.js";
import { FabricRuntimeState } from "../src/fabric-runtime-state.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { FabricControlPlane } from "../src/topology/control-plane.js";
import { liveControlOwnerIncarnation } from "./helpers/live-control-owner.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../src/topology/types.js";

const owners = ["persistent session", "transient session", "resident"] as const;
type OwnerKind = typeof owners[number];
const exact = "dest/gpt-6.1-sol";
const refused = /not an exact model id or configured alias.*Candidates: (?=.*dest\/gpt-6-sol\b)(?=.*dest\/gpt-6\.1-sol\b)/;

// No control receipts or owner resolvers are mocked: the foreign provider sends raw
// overrides through the real control plane into FabricRuntimeState / ResidentHost.
const withOwner = async (
  kind: OwnerKind,
  run: (state: {
    provider: AgentsProvider; invocation: FabricInvocationContext; actor: FabricActorInfo;
    agents: AgentManager; actors: ActorManager; getAvailable: ReturnType<typeof vi.fn>;
    senderAvailable: ReturnType<typeof vi.fn>; refresh: ReturnType<typeof vi.fn>;
  }) => Promise<void>,
  aliases: Record<string, string> = {},
  added: string[] = [],
) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-owner-model-"));
  const meshRoot = path.join(root, "mesh");
  vi.stubEnv("PI_FABRIC_PROJECT_ROOT", root);
  vi.stubEnv("PI_FABRIC_MESH_ROOT", meshRoot);
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
  for (const name of ["PI_FABRIC_MAIN_AGENT_ID", "PI_FABRIC_ACTOR_ID", "PI_FABRIC_PARENT_RUN", "PI_FABRIC_SESSION_ID"]) vi.stubEnv(name, "");
  const models = ["gpt-6-sol", "gpt-6.1-sol"].map(id => ({ provider: "dest", id, name: id, contextWindow: 48_000 }));
  const getAvailable = vi.fn(() => [...models]);
  const refresh = vi.fn(async () => {
    for (const id of added) models.push({ provider: "dest", id, name: id, contextWindow: 48_000 });
  });
  const modelRegistry = {
    getAvailable, refresh,
    find: (provider: string, id: string) => models.find(model => model.provider === provider && model.id === id),
    getApiKeyAndHeaders: async () => ({ ok: true, apiKey: "test" }),
  };
  const config = normalizeFabricConfig({
    fullCodeMode: false, agents: { enabled: true, budgetUsd: 0, timeoutMs: 10_000 },
    mcp: { enabled: false }, memory: { enabled: false }, residency: { enabled: false },
    models: { aliases }, mesh: { enabled: true, actorPollMs: 20 },
    prewalk: { enabled: false, alwaysRearm: false },
  });
  const fixture = path.resolve("tests/fixtures/fake-worker.mjs");
  const context = {
    cwd: root, hasUI: false, isProjectTrusted: () => kind === "persistent session",
    isIdle: () => true, hasPendingMessages: () => false, model: models[1], modelRegistry,
    sessionManager: { getSessionId: () => "owner", getSessionFile: () => undefined, getBranch: () => [], getLeafId: () => null },
    ui: { setStatus: vi.fn(), notify: vi.fn() },
  } as unknown as ExtensionContext;
  const pi = { events: { emit: vi.fn() }, getThinkingLevel: () => "off", sendMessage: vi.fn() } as unknown as ExtensionAPI;
  const owner = kind === "resident"
    ? new ResidentHost({
        format: RESIDENT_HOST_FORMAT, rootId: "session:owner", sessionId: "owner",
        cwd: root, projectRoot: root, meshRoot, actorRoot: path.join(meshRoot, "actors"),
        residencyRoot: residentRoot(meshRoot, "session:owner"), fullCodeMode: false,
        agents: config.agents, mesh: config.mesh, retention: config.retention,
        workerPath: fixture, fabricExtensionPath: fixture,
        piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
        piModels: { available: [...models], aliases: config.models.aliases, defaultModel: exact },
      } satisfies ResidentHostConfig, undefined, modelRegistry)
    : new FabricRuntimeState(pi, new CapturedToolCatalog(), { paths: {
        extension: fixture, worker: fixture, residentHost: fixture, skills: root,
      } });
  let callerActors: ActorManager | undefined;
  let callerAgents: AgentManager | undefined;
  let control: FabricControlPlane | undefined;
  let lifecycle: LifecycleBroker | undefined;
  try {
    if (owner instanceof ResidentHost) await owner.start();
    else await owner.initialize(context, config);
    const actor = await owner.actors.create({
      name: "strict-owner", instructions: "Reply.", model: exact,
      residency: kind === "resident" ? "durable" : "session",
      tools: [], extensions: false, transport: "process", delivery: "mailbox", responseMode: "text",
    });
    const caller = { id: "session:foreign", sessionId: "foreign", name: "Foreign Main", kind: "main" as const };
    callerAgents = new AgentManager(root, config.agents, { workerPath: fixture, runRoot: path.join(root, "caller-runs") });
    callerActors = new ActorManager("foreign", caller, owner.mesh, config.mesh, callerAgents, () => {}, {
      actorRoot: path.join(root, "caller-actors"), canManageActor: () => false,
    });
    const member: FabricParticipantInfo = {
      format: 1, id: actor.id, name: actor.name, kind: "actor", rootId: "session:owner",
      ownerHostId: owner instanceof ResidentHost ? owner.hostId : "session:owner",
      ownerIdentityId: owner instanceof ResidentHost ? owner.identity.id : "session:owner",
      ownerIncarnation: await liveControlOwnerIncarnation(owner instanceof ResidentHost ? owner.participants : {
        get: id => owner.participantInfos({ fresh: true }).find(info => info.id === id),
      }, actor.id),
      status: "idle", residency: kind === "resident" ? "durable" : "session", runner: "pi", transport: "host",
      capabilities: ["ask", "steer", "followUp", "actor-bindings"],
      startedAt: 1, updatedAt: 1, controlProtocol: "v1", local: false, stale: false,
    };
    const participants: FabricParticipantSource = {
      list: () => [member], get: id => id === actor.id ? member : undefined,
      self: () => member, peers: () => [], async refresh() {}, scheduleRefresh() {},
    };
    control = new FabricControlPlane(owner.mesh, caller, {
      enabled: true, hostId: caller.id, pollMs: 20, acknowledgementTimeoutMs: 5_000,
    });
    control.start(() => ({ accepted: false }));
    lifecycle = new LifecycleBroker(owner.mesh, caller, participants, {
      enabled: false, pollMs: 20, maxReadEvents: 100,
    }, async () => {});
    const main = { id: caller.id, local: true, matches: (id: string) => id === caller.id } as FabricMainAgentTarget;
    const provider = new AgentsProvider(callerAgents, callerActors, new GlobalActorRegistry(root, 64 * 1024),
      main, participants, control, lifecycle, () => false);
    const senderAvailable = vi.fn(() => { throw new Error("Sender model registry must not resolve owner overrides"); });
    const invocation: FabricInvocationContext = {
      cwd: root, signal: undefined, parentToolCallId: "owner-model", nestedToolCallId: "owner-model",
      extensionContext: { modelRegistry: { getAvailable: senderAvailable } } as unknown as ExtensionContext,
      update() {},
    };
    getAvailable.mockClear(); refresh.mockClear();
    await run({ provider, invocation, actor, agents: owner.agents, actors: owner.actors,
      getAvailable, senderAvailable, refresh });
  } finally {
    await control?.close();
    await lifecycle?.close();
    await callerActors?.close();
    await callerAgents?.close();
    if (owner instanceof ResidentHost) await owner.close();
    else await owner.shutdown();
    vi.unstubAllEnvs();
    fs.rmSync(root, { recursive: true, force: true });
  }
};

for (const kind of owners) describe(`${kind} execution-owner activation models (#3592)`, () => {
  it.each(["ask", "tell"] as const)("refuses ranked agents.%s overrides at the owner without launching", async action => {
    await withOwner(kind, async state => {
      const launch = vi.spyOn(state.agents, "run");
      await expect(state.provider.invoke(action, { id: state.actor.id, message: "Do not activate", model: "sol" }, state.invocation))
        .rejects.toThrow(refused);
      expect(state.getAvailable).toHaveBeenCalled();
      expect(state.senderAvailable).not.toHaveBeenCalled();
      expect(launch).not.toHaveBeenCalled();
      expect(state.actors.status(state.actor.id)).toMatchObject({ model: exact, queued: 0 });
    });
  });

  it.each(["ask", "tell"] as const)("keeps exact ids and owner-only aliases for agents.%s without changing defaults", async action => {
    await withOwner(kind, async state => {
      const launch = vi.spyOn(state.agents, "run");
      for (const model of ["dest/gpt-6-sol", "gpt-6-sol", "sol"]) {
        const previous = launch.mock.calls.length;
        await state.provider.invoke(action, { id: state.actor.id, message: "PING", model }, state.invocation);
        await vi.waitFor(() => {
          expect(launch).toHaveBeenCalledTimes(previous + 1);
          expect(state.actors.status(state.actor.id)).toMatchObject({ status: "idle", queued: 0 });
        });
        expect(launch.mock.calls.at(-1)?.[0]).toMatchObject({ model: "dest/gpt-6-sol" });
        expect(state.actors.status(state.actor.id).model).toBe(exact);
      }
      expect(state.getAvailable).toHaveBeenCalled();
      expect(state.senderAvailable).not.toHaveBeenCalled();
    }, { sol: "dest/gpt-6-sol" });
  });

  it.each(["ask", "tell"] as const)("refreshes once for an exact just-added agents.%s override", async action => {
    await withOwner(kind, async state => {
      const launch = vi.spyOn(state.agents, "run");
      await state.provider.invoke(action, { id: state.actor.id, message: "PING", model: "dest/gpt-6.2-sol" }, state.invocation);
      await vi.waitFor(() => {
        expect(launch).toHaveBeenCalledOnce();
        expect(state.actors.status(state.actor.id)).toMatchObject({ status: "idle", queued: 0 });
      });
      expect(launch.mock.calls[0]?.[0]).toMatchObject({ model: "dest/gpt-6.2-sol" });
      expect(state.refresh).toHaveBeenCalledOnce();
      expect(state.senderAvailable).not.toHaveBeenCalled();
    }, {}, ["gpt-6.2-sol"]);
  });

  it("preserves task-run closest matching outside the strict actor boundary", async () => {
    await withOwner(kind, async state => {
      const handle = await state.agents.spawn({ task: "HANG", model: "sol", transport: "process" });
      try {
        expect(handle.model).toMatch(/^dest\/gpt-6(?:\.1)?-sol$/);
      } finally { await state.agents.stop(handle.id); }
    });
  });
});
