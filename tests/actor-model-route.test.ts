import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager, type ActorModelRouteInput } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { prepareModelRoute } from "../src/agents/model-route-prepare.js";
import { ShadowRouteOwner, type ShadowRoutePolicy } from "../src/agents/model-route-owner.js";
import { DEFAULT_FABRIC_CONFIG, normalizeFabricConfig } from "../src/config.js";
import { resolvePiRoutePin } from "../src/core/model-refresh.js";
import { JevClient } from "../src/jev/client.js";
import type { JevResponse } from "../src/jev/types.js";
import type { FabricLifecyclePublishRequest } from "../src/lifecycle/types.js";
import { MeshStore } from "../src/mesh/store.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, type ResidentHostConfig } from "../src/residency/protocol.js";

const pin = { model: "test/sol", effort: "high" as const };
const cheap = { model: "test/luna", effort: "medium" as const };
const registry = { getAvailable: () => [{ provider: "test", id: "sol" }, { provider: "test", id: "luna" }] };
const config = { shadowCandidates: [cheap] };
const policy = (): ShadowRoutePolicy => ({ jev: { ...DEFAULT_FABRIC_CONFIG.jev, credentialCommand: [] }, networkAllowed: true, schemaEnforced: false });
const answer = (): JevResponse => ({ model: "jev", answers: { route: { type: "choice", choice: "candidate-1",
  confidence: .95, probabilities: { "candidate-0": .05, "candidate-1": .95 } } }, usage: { input_tokens: 1, output_tokens: 1 } });
const request = { routeClass: "status-groom", protected: false, pinModel: pin.model, pinThinking: pin.effort,
  parentSessionId: "owner", actorId: "actor", activationId: "activation", registry, aliases: {}, config,
  assertModelAllowed() {}, evaluate: async () => answer() };
const roots: string[] = [];
const closers: Array<() => Promise<void>> = [];
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "actor-route-test-")); roots.push(dir); return dir; };
const records = (): Array<Record<string, any>> => fs.readFileSync(path.join(process.env.PI_CODING_AGENT_DIR!, "fabric/model-routing.jsonl"), "utf8")
  .trim().split("\n").map(line => JSON.parse(line));
beforeEach(() => vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root(), "agent")));
afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
const actorSpec = { name: "supervisor", instructions: "Status only or no-op.", residency: "durable" as const,
  runner: "pi" as const, transport: "process" as const, extensions: false, model: pin.model, thinking: pin.effort,
  routeClass: "status-groom" as const, protected: false };
const setup = (routePolicy = policy(), routeConfig = { ...config, liveClasses: [] as string[] }) => {
  const dir = root();
  const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
  const lifecycle: FabricLifecyclePublishRequest[] = [];
  const agents = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, modelRouting: routeConfig, retainRuns: true }, {
    workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    onLifecycle: event => lifecycle.push(event),
    preparePiModel: async (model, requiredPin) => {
      if (!requiredPin) return model;
      const exact = await resolvePiRoutePin({ selector: model ?? "", registry, aliases: {} });
      return `${exact.provider}/${exact.id}`;
    },
  });
  closers.push(() => agents.close());
  const owner = new ShadowRouteOwner(() => routePolicy); closers.push(() => owner.close());
  const options = { actorRoot: path.join(dir, "actors"), persistent: true, claimResidency: "durable" as const,
    resolvePiModel: async (model: string, requiredPin = false) => {
      if (!requiredPin) return model;
      const exact = await resolvePiRoutePin({ selector: model, registry, aliases: {} });
      return `${exact.provider}/${exact.id}`;
    },
    prepareModelRoute: (input: ActorModelRouteInput, signal: AbortSignal) =>
      prepareModelRoute({ ...input, signal, registry, aliases: {}, config: routeConfig, assertModelAllowed: model => agents.assertModelAllowed(model, "pi"),
        evaluate: (input, routeSignal) => owner.evaluate(input, routeSignal) }),
  };
  const identity = { id: "session:owner", name: "main", kind: "main" as const, sessionId: "owner" };
  const actors = new ActorManager("owner", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, options);
  closers.push(() => actors.close());
  return { actors, agents, dir, mesh, options, identity, lifecycle };
};
const flag = (args: string[], key: string) => args[args.indexOf(key) + 1];

describe("actor status-groom shadow routing", () => {
  it("shares finite Choice preparation and carries only bounded class/protection, not actor text", async () => {
    const evaluate = vi.fn(async () => answer());
    expect(await prepareModelRoute({ ...request, evaluate })).toMatchObject({ ...cheap, pin, actorId: "actor", activationId: "activation", mode: "shadow", reasonCode: "shadow-choice" });
    expect(evaluate).toHaveBeenCalledTimes(1);
    const sent = evaluate.mock.calls[0] as unknown as [Record<string, any>];
    expect(sent[0].state).toEqual({ routeClass: "status-groom", mode: "shadow", protection: "clear" });
    expect(sent[0].questions.route.criteria).toEqual({ "candidate-0": pin, "candidate-1": cheap });
    expect(JSON.stringify(sent[0])).not.toContain("activation");
  });
  it.each([undefined, "sol", "test/missing", "auto"])("refuses absent/non-exact pins before Choice: %s", async pinModel => {
    const evaluate = vi.fn(async () => answer());
    await expect(prepareModelRoute({ ...request, pinModel, evaluate })).rejects.toMatchObject({ code: "MODEL_ROUTE_PIN_UNAVAILABLE" });
    expect(evaluate).not.toHaveBeenCalled();
  });
  it("refuses missing effort and records invalid candidates without Jev", async () => {
    await expect(prepareModelRoute({ ...request, pinThinking: undefined })).rejects.toMatchObject({ code: "MODEL_ROUTE_PIN_UNAVAILABLE" });
    const evaluate = vi.fn(async () => answer());
    expect(await prepareModelRoute({ ...request, config: { shadowCandidates: [{ model: "test/unavailable", effort: "medium" }] }, evaluate }))
      .toMatchObject({ ...pin, reasonCode: "invalid-candidates" });
    expect(evaluate).not.toHaveBeenCalled();
  });
  it("has a default-OFF live reservation and refuses enabling it", () => {
    expect(normalizeFabricConfig({ agents: { modelRouting: {} } }).agents.modelRouting?.live).toBe(false);
    expect(() => normalizeFabricConfig({ agents: { modelRouting: { live: true } } })).toThrow(/measured parity.*2236/);
  });
  it("makes a fresh decision for each real durable ActorManager activation, launches pins and preserves the native session", async () => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const { actors, dir } = setup();
    const actor = await actors.create(actorSpec);
    const first = await actors.ask(actor.id, "ECHO_MODEL", "test");
    const session = actors.status(actor.id).sessionFile!;
    const header = fs.readFileSync(session, "utf8").split("\n")[0];
    const second = await actors.ask(actor.id, "ECHO_MODEL", "test");
    expect(evaluate).toHaveBeenCalledTimes(2);
    expect(first.text).toContain(pin.model); expect(second.text).toContain(pin.model);
    expect(fs.readFileSync(session, "utf8").split("\n")[0]).toBe(header);
    for (const [dispatch] of launch.mock.calls) {
      expect(flag(dispatch.workerArguments, "--model")).toBe(pin.model);
      expect(flag(dispatch.workerArguments, "--thinking")).toBe(pin.effort);
      expect(flag(dispatch.workerArguments, "--session-file")).toBe(session);
    }
    const decisions = records().filter(row => row.type === "decision");
    expect(decisions).toHaveLength(2);
    expect(new Set(decisions.map(row => row.decisionId)).size).toBe(2);
    expect(new Set(decisions.map(row => row.activationId)).size).toBe(2);
    for (const decision of decisions) {
      expect(decision).toMatchObject({ actorId: actor.id, model: cheap.model, effort: cheap.effort, pin, reasonCode: "shadow-choice", childSessionId: null });
      expect(records().find(row => row.type === "outcome" && row.decisionId === decision.decisionId)).toMatchObject({
        actorId: actor.id, runId: decision.runId, activationId: decision.activationId, status: "completed", admittedModel: pin.model, admittedEffort: pin.effort,
        tokens: { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, cost: 0 },
      });
      expect(fs.existsSync(path.join(dir, "runs", decision.runId, "route-session.jsonl"))).toBe(false);
    }
    for (const decision of decisions) {
      const archive = path.join(dir, "actors", actor.id, "runs", decision.runId, "status.json");
      await vi.waitFor(() => expect(fs.existsSync(archive)).toBe(true));
      expect(JSON.parse(fs.readFileSync(archive, "utf8")))
        .toMatchObject({ routeClass: "status-groom", routeClassSource: "explicit", protected: false });
    }
    expect(actors.definition(actor.id)).toMatchObject({ routeClass: "status-groom", protected: false });
    expect(actors.status(actor.id)).toMatchObject({ routeClass: "status-groom", protected: false });
  });
  it("launches the live finite candidate per activation, preserves session and joins actual outcome", async () => {
    vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const { actors } = setup(policy(), { ...config, liveClasses: ["status-groom"] });
    const actor = await actors.create(actorSpec);
    const first = await actors.ask(actor.id, "ECHO_MODEL", "test");
    const session = actors.status(actor.id).sessionFile!;
    const header = fs.readFileSync(session, "utf8").split("\n")[0];
    const second = await actors.ask(actor.id, "ECHO_MODEL", "test");
    expect(first.text).toContain(cheap.model); expect(second.text).toContain(cheap.model);
    expect(fs.readFileSync(session, "utf8").split("\n")[0]).toBe(header);
    for (const [dispatch] of launch.mock.calls) {
      expect(flag(dispatch.workerArguments, "--model")).toBe(cheap.model);
      expect(flag(dispatch.workerArguments, "--thinking")).toBe(cheap.effort);
      expect(flag(dispatch.workerArguments, "--session-file")).toBe(session);
      expect(flag(dispatch.workerArguments, "--route-header")).toContain("live-choice:");
    }
    for (const decision of records().filter(row => row.type === "decision")) {
      expect(decision).toMatchObject({ mode: "live", actorId: actor.id });
      expect(records().find(row => row.type === "outcome" && row.decisionId === decision.decisionId)).toMatchObject({ admittedModel: cheap.model, admittedEffort: cheap.effort });
    }
  });
  it("has no actor quality reporter and leaves subsequent opted-in activations live", async () => {
    vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors } = setup(policy(), { ...config, liveClasses: ["status-groom"] });
    const actor = await actors.create(actorSpec);
    expect((await actors.ask(actor.id, "ECHO_MODEL", "test")).text).toContain(cheap.model);
    expect("reportRouteQuality" in actors).toBe(false);
    expect("routeQualityTarget" in actors).toBe(false);
    expect((await actors.ask(actor.id, "ECHO_MODEL", "test")).text).toContain(cheap.model);
  });
  it.each([[false, false], [true, false], [false, true], [true, true]])("preserves the effective exception in routed activation records and run.spawned (session override: %s, live: %s)", async (sessionOverride, live) => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors, agents, dir, lifecycle } = setup(policy(), { ...config, liveClasses: live ? ["status-groom"] : [] });
    const creationReason = "  Named creation exception  ";
    const sessionReason = "  Named session binding exception  ";
    const actor = await actors.create({ ...actorSpec, modelReason: creationReason });
    if (sessionOverride) await actors.setModel(actor.id, cheap.model, "session", undefined, sessionReason);
    const modelReason = sessionOverride ? sessionReason : creationReason;
    const pinModel = sessionOverride ? cheap.model : pin.model;
    const model = live ? cheap.model : pinModel;
    await actors.ask(actor.id, "ECHO_MODEL", "test");
    const decision = records().find(row => row.type === "decision")!;
    expect(decision).toMatchObject({ modelReason, mode: live ? "live" : "shadow", pin: { model: pinModel, effort: pin.effort } });
    expect(records().find(row => row.type === "outcome")).toMatchObject({ modelReason });
    expect(agents.status(decision.runId)).toMatchObject({ model, modelReason, status: "completed" });
    expect(lifecycle.find(event => event.event === "run.spawned")).toMatchObject({
      runId: decision.runId, data: { model, modelReason },
    });
    for (const file of [path.join(dir, "runs", decision.runId, "status.json"),
      path.join(dir, "actors", actor.id, "runs", decision.runId, "status.json")]) {
      await vi.waitFor(() => expect(fs.existsSync(file)).toBe(true));
      expect(JSON.parse(fs.readFileSync(file, "utf8"))).toMatchObject({ model, modelReason });
    }
    expect(actors.definition(actor.id).modelReason).toBe(creationReason);
    expect(JSON.stringify(evaluate.mock.calls)).not.toContain(modelReason.trim());

  });
  it.each([true, undefined])("does not route protected or unknown actor state: %s", async protectedFlag => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors } = setup();
    const { protected: _clear, ...spec } = actorSpec;
    const actor = await actors.create({ ...spec, ...(protectedFlag === true ? { protected: true } : {}) });
    expect((await actors.ask(actor.id, "ECHO_MODEL", "test")).text).toContain(pin.model);
    expect(evaluate).not.toHaveBeenCalled();
    expect(records()[0]).toMatchObject({ actorId: actor.id, ...pin, reasonCode: protectedFlag === true ? "excluded-protected" : "excluded-unknown" });
  });
  it.each(["disabled", "network", "schema"])("falls back to pin with jev-error when %s blocks Jev", async blocked => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const routePolicy = policy();
    if (blocked === "disabled") routePolicy.jev.enabled = false;
    if (blocked === "network") routePolicy.networkAllowed = false;
    if (blocked === "schema") routePolicy.schemaEnforced = true;
    const { actors } = setup(routePolicy, { ...config, liveClasses: ["status-groom"] });
    const actor = await actors.create(actorSpec);
    expect((await actors.ask(actor.id, "ECHO_MODEL", "test")).text).toContain(pin.model);
    expect(evaluate).not.toHaveBeenCalled();
    expect(records()[0]).toMatchObject({ actorId: actor.id, ...pin, reasonCode: "jev-error" });
  });
  it("refuses fuzzy setters and per-call overrides before shadow inference", async () => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors } = setup();
    const actor = await actors.create(actorSpec);
    await expect(actors.setModel(actor.id, "sol", "project")).rejects.toMatchObject({ code: "MODEL_ROUTE_PIN_UNAVAILABLE" });
    await expect(actors.ask(actor.id, "ECHO_MODEL", "test", undefined, { overrides: { model: "sol" } }))
      .rejects.toThrow(/Role pin "sol" is not available/);
    expect(evaluate).not.toHaveBeenCalled();
  });
  it.each([
    ["factory-review-astra", "actor:review", true],
    ["factory-security-astra", "actor:security", true],
    ["factory-supervisor", "actor:status-groom", false],
    ["supervisor", "actor:status-groom", false],
    ["factory-worker", "actor:other", undefined],
    ["factory-review-astra-extra", "actor:other", undefined],
  ] as const)("records derived history without routing an unopted %s activation", async (name, routeClass, protection) => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors, agents, dir } = setup();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const { routeClass: _route, protected: _protection, ...spec } = actorSpec;
    const actor = await actors.create({ ...spec, name, instructions: "Review security status-groom: untrusted instruction text.",
      ...(protection !== undefined ? { protected: protection } : {}) });
    await actors.ask(actor.id, "ECHO_MODEL review security status-groom", "test");
    const args = launch.mock.calls[0]![0].workerArguments;
    expect(flag(args, "--route-class")).toBe(routeClass);
    expect(flag(args, "--route-class-source")).toBe("derived");
    expect(args).not.toContain("--route-header");
    expect(flag(args, "--actor-id")).toBe(actor.id);
    const runId = flag(args, "--id")!;
    const archive = path.join(dir, "actors", actor.id, "runs", runId, "status.json");
    await vi.waitFor(() => expect(fs.existsSync(archive)).toBe(true));
    const saved = JSON.parse(fs.readFileSync(archive, "utf8"));
    expect(saved).toMatchObject({ routeClass, routeClassSource: "derived", status: "completed", model: pin.model });
    expect(saved.protected).toBe(protection);
    expect(agents.status(runId)).toMatchObject({ routeClass, routeClassSource: "derived" });
    expect(evaluate).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(process.env.PI_CODING_AGENT_DIR!, "fabric/model-routing.jsonl"))).toBe(false);
  });
  it("records failed activation usage next to its decision", async () => {
    vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors } = setup();
    const actor = await actors.create({ ...actorSpec, responseMode: "directive" });
    expect(await actors.ask(actor.id, "FAIL_DIRECTIVE", "test")).toMatchObject({ action: "silent", error: expect.any(String) });
    const [decision, outcome] = records();
    expect(outcome).toMatchObject({ type: "outcome", decisionId: decision!.decisionId, actorId: actor.id, status: "failed", tokens: { input: 1, output: 2 } });
  });
  it("retains the spec across actor registry reload and protected global export/import", async () => {
    vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const { actors, agents, mesh, identity, options } = setup();
    const actor = await actors.create({ ...actorSpec, protected: true });
    await actors.close();
    const restored = new ActorManager("owner", identity, mesh, { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, agents, () => {}, options);
    closers.push(() => restored.close());
    expect(restored.definition(actor.id)).toMatchObject({ routeClass: "status-groom", protected: true });
    await restored.ask(actor.id, "ECHO_MODEL", "test");
    expect(records()[0]).toMatchObject({ actorId: actor.id, reasonCode: "excluded-protected" });
    const globalDir = path.join(root(), "agent");
    const global = new GlobalActorRegistry(globalDir, 64 * 1024);
    const template = global.create(restored.definition(actor.id));
    const reloaded = new GlobalActorRegistry(globalDir, 64 * 1024);
    // Pure template read/write, no history or ephemeral activation pin is exported.
    expect(global.toRequest(global.update(template.id, { instructions: "Still protected." }))).toMatchObject({ routeClass: "status-groom", protected: true });
    expect(template.routeClass).toBe("status-groom");
    expect(reloaded.toRequest(reloaded.resolve(template.id)!)).toMatchObject({ routeClass: "status-groom", protected: true });
  });
  it.each([
    ["registered resident model", registry],
    ["Main extension model absent from the bare resident registry", { getAvailable: () => [] }],
  ])("wires the same Choice and pin through a resident owner: %s", async (_label, residentRegistry) => {
    const evaluate = vi.spyOn(JevClient.prototype, "evaluate").mockImplementation(async () => answer());
    const dir = root();
    const hostConfig: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: "session:resident", sessionId: "resident", cwd: process.cwd(), projectRoot: process.cwd(),
      meshRoot: path.join(dir, "mesh"), actorRoot: path.join(dir, "actors"), residencyRoot: path.join(dir, "resident"), fullCodeMode: false,
      agents: { ...DEFAULT_FABRIC_CONFIG.agents, modelRouting: config }, mesh: { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 },
      retention: DEFAULT_FABRIC_CONFIG.retention, workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), fabricExtensionPath: path.resolve("dist/index.js"),
      piBinary: "pi", claudeBinary: "claude", vedaBinary: "veda", piModels: { available: [...registry.getAvailable()], aliases: {}, defaultModel: pin.model },
      shadowRouting: policy(),
    };
    const host = new ResidentHost(hostConfig, () => {}, residentRegistry); closers.push(() => host.close());
    await host.start();
    // The synced catalog admits exact extension pins/candidates, never fuzzy or
    // absent pins, even when the native resident registry cannot load extensions.
    for (const model of ["sol", "test/missing"]) {
      await expect(host.actors.create({ ...actorSpec, model })).rejects.toMatchObject({ code: "MODEL_ROUTE_PIN_UNAVAILABLE" });
    }
    expect(evaluate).not.toHaveBeenCalled();
    const actor = await host.actors.create(actorSpec);
    expect((await host.actors.ask(actor.id, "ECHO_MODEL", "test")).text).toContain(pin.model);
    expect(evaluate).toHaveBeenCalledTimes(1);
    expect(records()[0]).toMatchObject({ actorId: actor.id, parentSessionId: "resident", ...cheap, pin, reasonCode: "shadow-choice" });
    expect(records()[1]).toMatchObject({ actorId: actor.id, status: "completed", admittedModel: pin.model, admittedEffort: pin.effort });
  });
});
