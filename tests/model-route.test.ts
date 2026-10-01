import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { decideModelRoute, prepareRouteDispatch, ROUTE_DEADLINE_MS, type RouteEvaluate } from "../src/agents/model-route.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { normalizeAgentRunRequest } from "../src/agents/request.js";
import { normalizeFabricConfig, DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { JevResponse } from "../src/jev/types.js";

const pin = { model: "test/sol", effort: "high" as const };
const cheap = { model: "test/luna", effort: "medium" as const };
const input = { routeClass: "bounded-lookup", protected: false, pin, candidates: [cheap], parentSessionId: "parent" };
const response = (confidence = .95, probability = .95): JevResponse => ({ model: "jev", answers: {
  route: { type: "choice", choice: "candidate-1", confidence, probabilities: { "candidate-0": 1 - probability, "candidate-1": probability } },
}, usage: { input_tokens: 1, output_tokens: 1 } });
const roots: string[] = [];
const managers: AgentManager[] = [];
const root = () => { const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-")); roots.push(dir); return dir; };
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks();
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

describe("shadow model routing", () => {
  it.each([true, undefined, null, "review", "security", "audit", "needs-security-pass", "unknown", "false"])("excludes protected or unknown before Jev: %s", async protectedFlag => {
    const evaluate = vi.fn(async () => response());
    const result = await decideModelRoute({ ...input, protected: protectedFlag }, evaluate);
    expect(evaluate).not.toHaveBeenCalled();
    expect(result).toMatchObject({ ...pin, shadowChoice: pin, reasonCode: protectedFlag === true ? "excluded-protected" : "excluded-unknown" });
  });
  it("excludes unknown classes before Jev", async () => {
    const evaluate = vi.fn(async () => response());
    expect((await decideModelRoute({ ...input, routeClass: "unknown" }, evaluate)).reasonCode).toBe("excluded-class");
    expect(evaluate).not.toHaveBeenCalled();
  });
  it("excludes unavailable candidates before Jev", async () => {
    const evaluate = vi.fn(async () => response());
    expect((await decideModelRoute({ ...input, candidatesValid: false }, evaluate)).reasonCode).toBe("invalid-candidates");
    expect(evaluate).not.toHaveBeenCalled();
  });
  it.each([[.89, .99], [.99, .89], [0, 1]])("falls back when either confidence/probability is below .90: %s/%s", async (confidence, probability) => {
    const result = await decideModelRoute(input, async () => response(confidence, probability));
    expect(result).toMatchObject({ ...pin, confidence, probability, shadowChoice: cheap, reasonCode: "low-confidence" });
  });
  it("accepts both thresholds at .90, exactly one Choice and no task text", async () => {
    const evaluate = vi.fn<RouteEvaluate>(async () => response(.9, .9));
    const result = await decideModelRoute(input, evaluate);
    expect(result).toMatchObject({ ...cheap, mode: "shadow", reasonCode: "shadow-choice" });
    expect(evaluate).toHaveBeenCalledTimes(1);
    const request = evaluate.mock.calls[0]?.[0];
    expect(Object.keys((request as unknown as { questions: object }).questions)).toEqual(["route"]);
    expect(JSON.stringify(request)).not.toContain("task");
  });
  it("deduplicates finite candidates and defaults to pin only", async () => {
    const evaluate: RouteEvaluate = async request => {
      expect(request.questions.route).toMatchObject({ criteria: { "candidate-0": pin } });
      return { ...response(), answers: { route: { type: "choice", choice: "candidate-0", confidence: 1, probabilities: { "candidate-0": 1 } } } };
    };
    expect((await decideModelRoute({ ...input, candidates: [pin] }, evaluate)).shadowChoice).toEqual(pin);
  });
  it("records Jev errors with no retry", async () => {
    const evaluate = vi.fn(async () => { throw new Error("network unavailable"); });
    expect(await decideModelRoute(input, evaluate)).toMatchObject({ ...pin, reasonCode: "jev-error" });
    expect(evaluate).toHaveBeenCalledTimes(1);
  });
  it("enforces 2.5 second deadline even on a non-cooperative evaluator", async () => {
    vi.useFakeTimers();
    const evaluate = vi.fn(() => new Promise<JevResponse>(() => {}));
    const pending = decideModelRoute(input, evaluate);
    await vi.advanceTimersByTimeAsync(ROUTE_DEADLINE_MS);
    expect(await pending).toMatchObject({ ...pin, reasonCode: "jev-timeout" });
    expect(evaluate).toHaveBeenCalledTimes(1);
  });
  it("records shorter client timeouts as timeout, not a generic error", async () => {
    expect((await decideModelRoute(input, async () => { throw new Error("Jev request cancelled or timed out"); })).reasonCode).toBe("jev-timeout");
  });
  it("propagates caller cancellation rather than dispatching a fallback", async () => {
    const controller = new AbortController();
    const pending = decideModelRoute(input, () => new Promise<JevResponse>(() => {}), controller.signal);
    controller.abort(new Error("caller cancelled"));
    await expect(pending).rejects.toThrow("caller cancelled");
  });
  it.each([
    {}, { answers: { route: { type: "noul", noul: 1 } } },
    { answers: { route: { ...response().answers.route, choice: "arbitrary-model" } } },
    { answers: { route: { ...response().answers.route, confidence: NaN } } },
    { answers: { route: { ...response().answers.route, confidence: 1.1 } } },
    { answers: { route: { ...response().answers.route, probabilities: { "candidate-1": .95 } } } },
    { answers: { route: { ...response().answers.route, probabilities: { "candidate-0": .95, "candidate-1": .95 } } } },
    { answers: { route: { ...response().answers.route, probabilities: { "candidate-0": -.1, "candidate-1": 1.1 } } } },
  ])("rejects malformed or out-of-list choices: %#", async malformed => {
    expect(await decideModelRoute(input, async () => malformed as unknown as JevResponse)).toMatchObject({ ...pin, reasonCode: "malformed" });
  });
  it("classifies client typed-response validation failures as malformed", async () => {
    expect((await decideModelRoute(input, async () => { throw new Error("TypeSafe returned an invalid or oversized typed response"); })).reasonCode).toBe("malformed");
  });
});

describe("durable route dispatch", () => {
  it("writes and fsyncs decision before launch, seeds child identity and appends terminal outcome once", async () => {
    const dir = root();
    const file = path.join(dir, ".pi", "fabric", "model-routing.jsonl");
    const decision = await decideModelRoute(input, async () => response());
    const launch = ProcessTransport.prototype.launch;
    const calls: string[][] = [];
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
      const rows = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ type: "decision", decisionId: decision.decisionId, shadowChoice: cheap, pin });
      const argv = request.workerArguments;
      calls.push(argv);
      expect(argv[argv.indexOf("--model") + 1]).toBe(pin.model);
      expect(argv[argv.indexOf("--thinking") + 1]).toBe(pin.effort);
      expect(argv[argv.indexOf("--route-header") + 1]).toContain("test%2Fluna-medium/shadow-choice:");
      const sessionFile = argv[argv.indexOf("--session-file") + 1]!;
      expect(SessionManager.open(sessionFile).getSessionId()).toBe(rows[0].childSessionId);
      return launch.call(this, request);
    });
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    // Even an internal caller accidentally forwarding the shadow model cannot change the pin.
    const handle = await manager.spawn({ task: "harmless lookup", model: cheap.model, thinking: cheap.effort, transport: "process", routeDecision: decision });
    const outcome = await manager.wait(handle.id);
    expect(outcome).toMatchObject({ status: "completed", model: pin.model, thinking: pin.effort });
    expect(calls).toHaveLength(1);
    const rows = fs.readFileSync(file, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({ type: "outcome", decisionId: decision.decisionId, status: "completed", admittedModel: pin.model, admittedEffort: pin.effort, tokens: { input: 1, output: 2 } });
  });
  it("dispatches pin and marks record-failed when durable state cannot be written", async () => {
    const dir = root();
    fs.writeFileSync(path.join(dir, ".pi"), "not a directory");
    const decision = await decideModelRoute(input, async () => response());
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    const result = await manager.run({ task: "harmless lookup", transport: "process", routeDecision: decision });
    expect(result).toMatchObject({ status: "completed", model: pin.model, thinking: pin.effort });
    expect(decision).toMatchObject({ ...pin, reasonCode: "record-failed", shadowChoice: cheap });
  });
  it("records a queued decision and its stopped outcome even when it never dispatches", async () => {
    const dir = root();
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, maxConcurrent: 1 }, {
      workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    const blocker = await manager.spawn({ task: "HANG", transport: "process" });
    const decision = await decideModelRoute(input, async () => response());
    const queued = await manager.spawn({ task: "harmless lookup", transport: "process", routeDecision: decision });
    expect(queued).toMatchObject({ status: "queued", model: pin.model, thinking: pin.effort });
    await manager.stop(queued.id); await manager.stop(blocker.id);
    const rows = fs.readFileSync(path.join(dir, ".pi/fabric/model-routing.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(2);
    expect(rows[1]).toMatchObject({ type: "outcome", decisionId: decision.decisionId, status: "stopped", admittedModel: null, admittedEffort: null });
  });
  it("never passes a partially written seed session after record failure", async () => {
    const dir = root();
    const decision = await decideModelRoute(input, async () => response());
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, data, options) => {
      if (typeof file === "string" && file.endsWith("route-session.jsonl")) {
        write(file, "partial"); throw new Error("disk full");
      }
      return write(file, data, options);
    });
    const prepared = prepareRouteDispatch(decision, dir, path.join(dir, "run"), "child");
    expect(prepared.sessionFile).toBeUndefined();
    expect(prepared.header).toContain("record-failed:");
    expect(decision).toMatchObject({ ...pin, reasonCode: "record-failed" });
  });
  it("supports late outcome joins after a failed record, and idempotent terminal append", async () => {
    const dir = root();
    const runDir = path.join(dir, "run"); fs.mkdirSync(runDir);
    const decision = await decideModelRoute(input, async () => response());
    const prepared = prepareRouteDispatch(decision, dir, runDir, "child-id");
    prepared.outcome({ status: "failed" }); prepared.outcome({ status: "failed" });
    const rows = fs.readFileSync(path.join(dir, ".pi/fabric/model-routing.jsonl"), "utf8").trim().split("\n");
    expect(rows).toHaveLength(2);
    expect(JSON.parse(rows[1]!)).toMatchObject({ admittedModel: null, tokens: null });
  });
  it("rejects unresolved auto before manager inheritance", async () => {
    const manager = new AgentManager(root(), DEFAULT_FABRIC_CONFIG.agents); managers.push(manager);
    await expect(manager.spawn({ task: "test", model: "auto" })).rejects.toThrow("Unresolved model");
    expect(() => normalizeAgentRunRequest({ task: "test", model: "auto" }, { runner: "pi", timeoutMs: 1000 })).toThrow("only by agents.spawn");
  });
  it("preserves explicit routing configuration and never creates a default role pin", () => {
    expect(normalizeFabricConfig({}).agents.modelRouting).toBeUndefined();
    expect(normalizeFabricConfig({ agents: { modelRouting: { pinModel: pin.model, pinThinking: pin.effort, shadowCandidates: [cheap] } } }).agents.modelRouting)
      .toEqual({ pinModel: pin.model, pinThinking: pin.effort, shadowCandidates: [cheap] });
    expect(() => normalizeFabricConfig({ agents: { modelRouting: { shadowCandidates: [{ model: "bad", effort: "bogus" }] } } })).toThrow("Invalid agents.modelRouting");
  });
});
