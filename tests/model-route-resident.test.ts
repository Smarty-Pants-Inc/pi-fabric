import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorDirectory } from "../src/actors/directory.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import { ResidentHost } from "../src/residency/host.js";
import { ResidencyClient } from "../src/residency/client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { JevClient } from "../src/jev/client.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricActorInfo, FabricActorMessage } from "../src/actors/types.js";

const pin = { model: "test/sol", effort: "high" as const };
const cheap = { model: "test/luna", effort: "medium" as const };
const available = [{ provider: "test", id: "sol" }, { provider: "test", id: "luna" }];
const answer = () => ({ model: "jev", answers: { route: { type: "choice" as const, choice: "candidate-1", confidence: .95,
  probabilities: { "candidate-0": .05, "candidate-1": .95 } } }, usage: { input_tokens: 1, output_tokens: 1 } });
let root: string;
const closers: Array<() => Promise<void>> = [];
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "model-route-resident-"));
  vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "profile"));
  vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
});
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true });
});
const rows = () => fs.readFileSync(path.join(root, "profile/fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
const fixture = async () => {
  const identity = { id: "session:resident-route", sessionId: "resident-route", kind: "main" as const, name: "Main" };
  const meshConfig = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
  const mesh = new MeshStore(path.join(root, "mesh"), meshConfig.maxEventBytes, meshConfig.maxReadEvents);
  const participants = new ParticipantDirectory(mesh, { enabled: true, hostId: identity.id, rootId: identity.id, identity, heartbeatMs: 50, leaseMs: 5000 });
  participants.registerSource(() => [{ format: 1, id: identity.id, kind: "root", rootId: identity.id, ownerHostId: identity.id,
    ownerIdentityId: identity.id, name: "Main", status: "idle", residency: "session", runner: "pi", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], cwd: root, sessionId: identity.sessionId, startedAt: Date.now(), updatedAt: Date.now(), controlProtocol: "v1" }]);
  await participants.start(); closers.push(() => participants.close());
  const modelRouting = { pinModel: pin.model, pinThinking: pin.effort, shadowCandidates: [cheap], liveClasses: ["status-groom", "task:exact-checks"] };
  const config: ResidentHostConfig = { format: RESIDENT_HOST_FORMAT, rootId: identity.id, sessionId: identity.sessionId,
    cwd: root, projectRoot: root, meshRoot: mesh.root, actorRoot: path.join(root, "actors"), sessionActorRoot: path.join(root, "session-actors"),
    residencyRoot: residentRoot(mesh.root, identity.id), fullCodeMode: true,
    agents: { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: false, budgetUsd: 0, modelRouting }, mesh: meshConfig, retention: DEFAULT_FABRIC_CONFIG.retention,
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"), piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda",
    piModels: { available, aliases: {}, defaultModel: pin.model },
    shadowRouting: { jev: { ...DEFAULT_FABRIC_CONFIG.jev, credentialCommand: [] }, networkAllowed: true, schemaEnforced: false } };
  fs.mkdirSync(config.residencyRoot, { recursive: true, mode: 0o700 });
  const configFile = path.join(config.residencyRoot, "config.json"); fs.writeFileSync(configFile, JSON.stringify(config));
  const host = new ResidentHost(structuredClone(config), () => {}, { getAvailable: () => available });
  await host.start(); closers.push(() => host.close());
  const mainAgent = { id: identity.id, local: true, ownsActor: () => false } as unknown as FabricMainAgentTarget;
  const client = new ResidencyClient({ config, mesh, participants, mainAgent, commandTimeoutMs: 5000 }); closers.push(() => client.close());
  const agents = new AgentManager(root, config.agents, { workerPath: config.workerPath, runRoot: path.join(root, "main-runs"), preparePiModel: async model => model });
  closers.push(() => agents.close());
  const passive = new ActorDirectory([identity.sessionId, identity, mesh, meshConfig, agents, () => {},
    { persistent: true, rootId: identity.id, canManageActor: () => false }], { project: config.actorRoot, session: config.sessionActorRoot! }, "project");
  closers.push(() => passive.close());
  const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {}); closers.push(() => lifecycle.close());
  const makeProvider = (main = mainAgent) => new AgentsProvider(agents, passive, new GlobalActorRegistry(path.join(root, "global"), 65536), main, participants,
    undefined, lifecycle, undefined, client, true, undefined, () => pin.effort, async () => answer());
  const provider = makeProvider();
  const context: FabricInvocationContext = { cwd: root, signal: undefined, parentToolCallId: "test", nestedToolCallId: "route",
    extensionContext: { modelRegistry: { getAvailable: () => available } } as unknown as FabricInvocationContext["extensionContext"], update() {} };
  const actor = await provider.invoke("create", { name: "status-groom", instructions: "Bounded status only", residency: "durable", runner: "pi", transport: "process",
    extensions: false, model: pin.model, thinking: pin.effort, routeClass: "status-groom", protected: false }, context) as FabricActorInfo;
  const activate = async () => {
    const result = await host.actors.ask(actor.id, "ECHO_MODEL", "test");
    await vi.waitFor(() => expect(host.agents.runDirectory(result.runId!)).toBeUndefined());
    return result;
  };
  return { identity, config, configFile, host, client, actor, activate, provider, context, makeProvider, mainAgent };
};

describe("resident LIVE custody and manual config rollback", () => {
  it("retains A through B and C when its archive fails, and retries before cleanup", async () => {
    const retain = ActorLogStore.prototype.retainRun;
    let failedRun: string | undefined;
    const archive = vi.spyOn(ActorLogStore.prototype, "retainRun").mockImplementation(async function (this: ActorLogStore, actor, runId, directory) {
      failedRun ??= runId;
      if (runId === failedRun) throw new Error("Injected A receipt archive failure");
      return retain.call(this, actor, runId, directory);
    });
    const state = await fixture(); const first = await state.host.actors.ask(state.actor.id, "ECHO_MODEL", "test");
    await vi.waitFor(() => expect(state.host.actors.status(state.actor.id).status).toBe("idle"));
    expect(state.host.agents.runDirectory(first.runId!)).toBeDefined();
    await state.activate(); await state.activate();
    expect(archive.mock.calls.filter(([, runId]) => runId === first.runId)).toHaveLength(3);
    const source = state.host.agents.runDirectory(first.runId!)!;
    expect(fs.existsSync(path.join(source, "route-dispatch-receipt.json"))).toBe(true);
    expect((await state.activate()).text).toContain(cheap.model);
    expect(state.host.agents.runDirectory(first.runId!)).toBe(source);
    archive.mockRestore(); expect((await state.activate()).text).toContain(cheap.model);
    expect(state.host.agents.runDirectory(first.runId!)).toBeUndefined();
    expect(fs.existsSync(path.join(state.actor.logDir!, first.runId!, "route-dispatch-receipt.json"))).toBe(true);
  }, 20000);

  it("keeps denied A archive custody through normal host close/restart and repairs before source collection", async () => {
    const retain = ActorLogStore.prototype.retainRun;
    let failedRun: string | undefined, denied = true;
    vi.spyOn(ActorLogStore.prototype, "retainRun").mockImplementation(async function (this: ActorLogStore, actor, runId, directory) {
      failedRun ??= runId;
      if (denied && runId === failedRun) throw new Error("Denied A archive before receipt copy");
      return retain.call(this, actor, runId, directory);
    });
    const state = await fixture();
    const a = await state.host.actors.ask(state.actor.id, "ECHO_MODEL", "test");
    await vi.waitFor(() => expect(state.host.actors.status(state.actor.id).status).toBe("idle"));
    const source = state.host.agents.runDirectory(a.runId!)!;
    const marker = path.join(source, "actor-run-archive-pending.json");
    expect(fs.existsSync(marker)).toBe(true);
    await state.activate(); // B succeeds; A's only receipt remains in its source.
    await state.host.close();
    expect(fs.existsSync(path.join(source, "route-dispatch-receipt.json"))).toBe(true);
    expect(fs.existsSync(marker)).toBe(true);
    expect(fs.existsSync(path.join(state.actor.logDir!, a.runId!, "route-dispatch-receipt.json"))).toBe(false);
    const restored = new ResidentHost(state.config, () => {}, { getAvailable: () => available });
    closers.push(() => restored.close()); await restored.start();
    expect(restored.agents.runDirectory(a.runId!)).toBeUndefined(); // No old live handle/map.
    expect(restored.agents.actorArchiveSources(state.actor.id, state.actor.sessionFile!).get(a.runId!)).toBe(source);
    denied = false;
    const next = await restored.actors.ask(state.actor.id, "ECHO_MODEL", "test"); expect(next.text).toContain(cheap.model);
    // #752 reviewed contract: docs/residency-runtime.md:64-69, 87-92 at 2f6838c3.
    // Once repair discharges archive custody, eligibility commits dormancy immediately.
    await vi.waitFor(() => expect(restored.actors.status(state.actor.id).status).toBe("dormant"));
    expect(fs.existsSync(path.join(state.actor.logDir!, a.runId!, "route-dispatch-receipt.json"))).toBe(true);
    expect(fs.existsSync(source)).toBe(false);
    expect(rows().filter(row => row.type === "decision").at(-1)).toMatchObject({ mode: "live", reasonCode: "live-choice" });
  }, 20000);

  it("manual empty liveClasses stops LIVE on Main and a running resident host, without replacing its owner", async () => {
    const state = await fixture();
    const ownerPath = path.join(state.config.residencyRoot, "owner.json");
    const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
    const mainRun = async (routeClass = "status-groom") => {
      const h = await state.provider.invoke("spawn", { task: "ECHO_MODEL", model: "auto", routeClass, protected: false }, state.context) as { id: string };
      return state.provider.invoke("wait", { id: h.id }, state.context);
    };
    expect((await state.activate()).text).toContain(cheap.model);
    expect(await mainRun()).toMatchObject({ model: cheap.model });
    const policy = state.config.agents.modelRouting!;
    state.config.agents.modelRouting = { ...policy, liveClasses: [] };
    await state.client.ensureHost(); // Normal same-release config refresh reuses ready owner.
    expect((await state.activate()).text).toContain(pin.model);
    expect(await mainRun()).toMatchObject({ model: pin.model });
    expect(rows().filter(row => row.type === "decision").at(-1)).toMatchObject({ mode: "shadow", reasonCode: "shadow-choice" });
    // Manual class-scoped rollback leaves unrelated task classes opted in.
    state.config.agents.modelRouting = { ...policy, liveClasses: ["task:exact-checks"] };
    await state.client.ensureHost();
    expect((await state.activate()).text).toContain(pin.model);
    expect(await mainRun("task:exact-checks")).toMatchObject({ model: cheap.model });
    // Explicit trusted re-enable/reset propagates to the same running host.
    state.config.agents.modelRouting = { ...policy, revertReset: { "status-groom": "manual-r2" } };
    await state.client.ensureHost();
    expect((await state.activate()).text).toContain(cheap.model);
    expect(await mainRun()).toMatchObject({ model: cheap.model });
    expect(rows().filter(row => row.type === "decision").at(-1)).toMatchObject({ mode: "live", revertReset: "manual-r2" });
    // Foreign overlay has no authority: the original trusted LIVE policy applies.
    fs.writeFileSync(state.configFile, JSON.stringify({ ...state.config, sessionId: "foreign-generation", agents: { ...state.config.agents, modelRouting: { ...policy, liveClasses: [] } } }));
    expect((await state.activate()).text).toContain(cheap.model);
    expect(JSON.parse(fs.readFileSync(ownerPath, "utf8"))).toMatchObject({ pid: owner.pid, token: owner.token });
  }, 20000);
});
