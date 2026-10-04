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
import { ResidentActorClient } from "../src/residency/actor-client.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { MeshStore } from "../src/mesh/store.js";
import { ParticipantDirectory } from "../src/topology/participant-directory.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import { JevClient } from "../src/jev/client.js";
import { isRouteClassReverted } from "../src/agents/model-route.js";
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

describe("public resident route feedback and config rollback", () => {
  it("authorizes an owning Main quality fail after completed durable activation cleanup and leaves other classes live", async () => {
    const state = await fixture();
    const first = await state.activate(); expect(first.text).toContain(cheap.model);
    expect(state.host.agents.runDirectory(first.runId!)).toBeUndefined();
    await expect(state.makeProvider({ ...state.mainAgent, id: "session:foreign" }).invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context))
      .rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
    await expect(new ResidentActorClient(state.config.meshRoot, state.identity.id).setActor({ operation: "routeQuality", id: first.runId!, routeQuality: "fail" }))
      .rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
    await expect(state.provider.invoke("routeOutcome", { id: "a".repeat(32), routeQuality: "fail" }, state.context)).rejects.toMatchObject({ code: "RESIDENT_ACTOR_FORBIDDEN" });
    await expect(state.provider.invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context)).resolves.toEqual({ id: first.runId, routeQuality: "fail" });
    const qualityDecisions = fs.readdirSync(path.join(state.config.residencyRoot, "decisions")).map(file =>
      JSON.parse(fs.readFileSync(path.join(state.config.residencyRoot, "decisions", file), "utf8")));
    expect(qualityDecisions.find(row => row.operation === "routeQuality" && row.state === "committed")).toMatchObject({ id: state.actor.id });
    const next = await state.activate(); expect(next.text).toContain(pin.model);
    const decisions = rows().filter(row => row.type === "decision");
    expect(decisions[0]).toMatchObject({ actorId: state.actor.id, mode: "live", reasonCode: "live-choice" });
    expect(decisions[1]).toMatchObject({ actorId: state.actor.id, mode: "shadow", reasonCode: "class-reverted" });
    expect(rows().find(row => row.type === "quality")).toMatchObject({ decisionId: decisions[0].decisionId, runId: first.runId, routeQuality: "fail" });
    const other = await state.provider.invoke("spawn", { task: "ECHO_MODEL", model: "auto", routeClass: "task:exact-checks", protected: false }, state.context) as { id: string; routeDecision: { mode: string } };
    expect(other.routeDecision.mode).toBe("live"); await state.provider.invoke("wait", { id: other.id }, state.context);
    await state.host.close(); // Recovered host reads archived receipt, even with retainRuns:false.
    const restored = new ResidentHost(state.config, () => {}, { getAvailable: () => available }); closers.push(() => restored.close()); await restored.start();
    await expect(state.provider.invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context)).resolves.toEqual({ id: first.runId, routeQuality: "fail" });
  }, 20000);

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("uses the same pending/committed safety fence for resident quality feedback", async () => {
    const state = await fixture(); const first = await state.activate();
    const file = path.join(root, "profile/fabric/model-routing-state.jsonl"); fs.chmodSync(file, 0o400);
    await expect(state.provider.invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context)).rejects.toThrow();
    expect((await state.activate()).text).toContain(pin.model);
    fs.chmodSync(file, 0o600); expect(isRouteClassReverted("status-groom")).toBe(true);
    const journal = fs.readFileSync(path.join(root, "profile/fabric/model-routing-quality.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(journal.map(row => row.type)).toEqual(["pending", "committed"]);
  }, 20000);

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("fences the first resident quality-intent write for both Main and resident dispatch", async () => {
    const state = await fixture(); const first = await state.activate();
    const journal = path.join(root, "profile/fabric/model-routing-quality.jsonl");
    fs.writeFileSync(journal, "", { mode: 0o400 }); fs.chmodSync(journal, 0o400);
    await expect(state.provider.invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context)).rejects.toThrow();
    expect(fs.readFileSync(journal, "utf8")).toBe("");
    expect((await state.activate()).text).toContain(pin.model);
    const main = await state.provider.invoke("spawn", { task: "ECHO_MODEL", model: "auto", routeClass: "status-groom", protected: false }, state.context) as { id: string };
    expect(await state.provider.invoke("wait", { id: main.id }, state.context)).toMatchObject({ model: pin.model });
    fs.chmodSync(journal, 0o600); expect(isRouteClassReverted("status-groom")).toBe(true);
    expect(rows().find(row => row.type === "quality")).toMatchObject({ runId: first.runId, routeQuality: "fail" });
  }, 20000);

  it("retains A through B and C when its archive fails, accepts owning-Main feedback, and retries before cleanup", async () => {
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
    expect(fs.existsSync(path.join(source, "route-quality-receipt.json"))).toBe(true);
    await expect(state.provider.invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context)).resolves.toEqual({ id: first.runId, routeQuality: "fail" });
    expect((await state.activate()).text).toContain(pin.model);
    expect(state.host.agents.runDirectory(first.runId!)).toBe(source);
    archive.mockRestore(); expect((await state.activate()).text).toContain(pin.model);
    expect(state.host.agents.runDirectory(first.runId!)).toBeUndefined();
    expect(fs.existsSync(path.join(state.actor.logDir!, first.runId!, "route-quality-receipt.json"))).toBe(true);
    expect(rows().find(row => row.type === "quality")).toMatchObject({ runId: first.runId, routeQuality: "fail" });
  }, 20000);

  it("rereads same-generation empty allowlist and fresh per-class reset on the next activation without replacing its host", async () => {
    const state = await fixture(); const ownerPath = path.join(state.config.residencyRoot, "owner.json"); const owner = JSON.parse(fs.readFileSync(ownerPath, "utf8"));
    const first = await state.activate(); expect(first.text).toContain(cheap.model);
    const policy = state.config.agents.modelRouting!;
    state.config.agents.modelRouting = { ...policy, liveClasses: [] };
    await state.client.ensureHost(); // Normal same-release refresh writes config and reuses ready owner.
    expect((await state.activate()).text).toContain(pin.model);
    expect(rows().filter(row => row.type === "decision").at(-1)).toMatchObject({ mode: "shadow", reasonCode: "shadow-choice" });
    await state.provider.invoke("routeOutcome", { id: first.runId, routeQuality: "fail" }, state.context);
    state.config.agents.modelRouting = policy; await state.client.ensureHost(); expect((await state.activate()).text).toContain(pin.model);
    const reset = { ...state.config, agents: { ...state.config.agents, modelRouting: { ...policy, revertReset: { "status-groom": "approved-r2" } } } };
    fs.writeFileSync(state.configFile, JSON.stringify({ ...reset, sessionId: "foreign-generation" }));
    expect((await state.activate()).text).toContain(pin.model); // No foreign overlay authority.
    state.config.agents = reset.agents; await state.client.ensureHost(); expect((await state.activate()).text).toContain(cheap.model);
    expect(rows().filter(row => row.type === "decision").at(-1)).toMatchObject({ mode: "live", revertReset: "approved-r2" });
    expect(JSON.parse(fs.readFileSync(ownerPath, "utf8"))).toMatchObject({ pid: owner.pid, token: owner.token });
  }, 20000);
});
