import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { decideModelRoute } from "../src/agents/model-route.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const workerPath = path.resolve("dist/worker.js");
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});
describe.skipIf(!fs.existsSync(workerPath))("shadow routing in real built worker", () => {
  const run = async (scenario: string, route = true) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-route-worker-")); roots.push(root);
    const scenarioFile = path.join(root, "scenario"); fs.writeFileSync(scenarioFile, scenario);
    vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent"));
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenarioFile);
    vi.stubEnv("PI_FABRIC_ROUTE_HEADER", "parent-attribution-must-not-leak");
    const pin = { model: "openai-codex/gpt-5.6-sol", effort: "high" as const };
    const decision = await decideModelRoute({ routeClass: "bounded-lookup", protected: false, pin,
      candidates: [{ model: "test/luna", effort: "medium" }], parentSessionId: "parent" }, async () => ({ model: "jev", answers: {
      route: { type: "choice", choice: "candidate-1", confidence: .95, probabilities: { "candidate-0": .05, "candidate-1": .95 } },
    }, usage: { input_tokens: 1, output_tokens: 1 } }));
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 5000, retainRuns: true }, {
      workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-route.mjs"), runRoot: path.join(root, "runs"),
    }); managers.push(manager);
    const result = await manager.run({ task: "harmless lookup", model: pin.model, thinking: pin.effort,
      transport: "process", extensions: false, ...(route ? { routeDecision: decision } : {}) });
    const events = fs.readFileSync(path.join(root,"runs",result.id,"events.jsonl"),"utf8").trim().split("\n").map(line => JSON.parse(line));
    const launch = events.find(event => event.type === "fake_route_launch");
    const rows = route ? fs.readFileSync(path.join(root,"agent/fabric/model-routing.jsonl"),"utf8").trim().split("\n").map(line => JSON.parse(line)) : [];
    return { result, rows, launch, events, pin };
  };
  it("loads attribution hook even when extensions are disabled and records verified admission", async () => {
    const { result, rows, launch, pin } = await run("success");
    expect(result).toMatchObject({ status: "completed", model: pin.model, thinking: pin.effort, admittedModel: pin.model, admittedThinking: pin.effort });
    expect(launch.argv).toContain("--no-extensions");
    expect(launch.argv.some((arg: string) => arg.endsWith("/guards/model-route-hook.js"))).toBe(true);
    expect(launch.header).toContain("bounded-lookup/test%2Fluna-medium/shadow-choice:");
    expect(rows[1]).toMatchObject({ admittedModel: pin.model, admittedEffort: pin.effort, status: "completed", tokens: { input: 2, output: 3 } });
  });
  it("does not claim an admitted pin when Pi rejects model selection before prompt", async () => {
    const { result, rows, events } = await run("reject");
    expect(result.status).toBe("failed");
    expect(events.filter(event => event.type === "fake_received").some(event => event.frame.type === "prompt")).toBe(false);
    expect(rows[1]).toMatchObject({ status: "failed", admittedModel: null, admittedEffort: null });
  });
  it("clears parent route metadata for unrelated explicit-model tasks", async () => {
    const { result, launch } = await run("success", false);
    expect(result.status).toBe("completed");
    expect(launch.header).toBeNull();
    expect(launch.argv.some((arg: string) => arg.endsWith("/guards/model-route-hook.js"))).toBe(false);
  });
});
