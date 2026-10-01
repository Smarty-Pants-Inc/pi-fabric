import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { judge, type JudgeDependencies } from "../src/judge/runtime.js";
import { runJudgmentAgent } from "../src/judge/agent.js";
import { VERDICTS } from "../src/judge/contract.js";

const input = () => ({ questionClass: "item-stalled", itemRef: "org/repo#1", evidenceRefs: [{ url: "https://example.test/evidence", revision: "v1", observedAt: "2026-10-01T00:00:00Z" }], evidence: { facts: { complete: true }, excerpts: ["test"] }, allowedVerdicts: [...VERDICTS], timeboxMs: 5000, budget: { maxEvaluations: 1, maxAgents: 1, maxTokens: 1000 }, requestKey: "bounded-worker-probe" });
let root: string;
let deps: JudgeDependencies;
const managers: AgentManager[] = [];
beforeEach(() => {
  root = fs.mkdtempSync(path.join(os.tmpdir(), "judge-worker-test-"));
  vi.stubEnv("PI_FABRIC_DEPTH", "0");
  deps = { policy: { version: "probe-v1", role: "Sol", pin: { model: "openai-codex/gpt-5.6-sol", effort: "max" } }, ledger: path.join(root, "ledger/route.jsonl"),
    evaluate: async () => ({ model: "fixture", answers: { verdict: { type: "choice", choice: "stalled", confidence: .6, probabilities: { moving: .4, stalled: .6, dependency: 0, agent_decision: 0, human_decision: 0, unknown: 0 } } }, usage: { input_tokens: 1, output_tokens: 1 } }),
    agent: async (request, limits, signal) => {
      const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: limits.timeoutMs, maxTokensPerChild: limits.maxTokens, retainRuns: true, sessionExport: false }, { workerPath: path.resolve("dist/worker.js"), piBinary: path.resolve("tests/fixtures/fake-pi-route.mjs"), runRoot: path.join(root, "runs"), fullCodeMode: false });
      managers.push(manager);
      try { const handle = await manager.spawn(request, signal); return await manager.wait(handle.id, { signal }); }
      finally { await manager.close(); }
    },
  };
});
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  vi.restoreAllMocks(); vi.unstubAllEnvs();
  fs.rmSync(root, { recursive: true, force: true });
});
const events = () => {
  const runs = path.join(root, "runs");
  const id = fs.readdirSync(runs).find(id => fs.existsSync(path.join(runs, id, "events.jsonl")))!;
  return fs.readFileSync(path.join(runs, id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
};
describe.skipIf(!fs.existsSync("dist/worker.js"))("built judgment worker isolation", () => {
  it("has only reply/route hooks, no ambient authority, no full-code and no schema-free text result", async () => {
    const result = await judge(input(), deps);
    expect(result).toMatchObject({ verdict: "unknown", reasonCode: "invalid_schema" });
    const launch = events().find(e => e.type === "fake_route_launch");
    for (const flag of ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-approve", "--no-auto-compaction", "--system-prompt"]) expect(launch.argv).toContain(flag);
    expect(launch.argv[launch.argv.indexOf("--tools") + 1]).toBe("fabric_reply");
    expect(launch.argv.filter((_: string, i: number) => launch.argv[i - 1] === "-e").map((p: string) => path.basename(p))).toEqual(["reply-tool.js", "model-route-hook.js"]);
    expect(launch.header).toContain(`judgment-agent:${result.decisionId}`);
  });
  it("refuses an effort downgrade before sending any task prompt", async () => {
    const scenario = path.join(root, "scenario"); fs.writeFileSync(scenario, "effort-downgrade"); vi.stubEnv("FAKE_MODEL_SCENARIO", scenario);
    expect(await judge(input(), deps)).toMatchObject({ verdict: "unknown", reasonCode: "agent_failed" });
    expect(events().filter(e => e.type === "fake_received").some(e => e.frame.type === "prompt")).toBe(false);
    const ledger = fs.readFileSync(deps.ledger, "utf8"); expect(ledger).toContain('"admittedModel":null');
  });
  it("reports a refusal as unknown without launching any alternate model", async () => {
    const scenario = path.join(root, "scenario"); fs.writeFileSync(scenario, "refusal"); vi.stubEnv("FAKE_MODEL_SCENARIO", scenario);
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    expect(await judge(input(), deps)).toMatchObject({ verdict: "unknown", reasonCode: "refusal" });
    expect(launch).toHaveBeenCalledTimes(1);
  });
});
describe("owned cancellation, deadlines and no retry", () => {
  it.each(["RESUME_AFTER_STOP", "RESUME_AFTER_CRASH"])("never resumes or retries a spent judgment worker: %s", async text => {
    const request = input(); request.evidence.excerpts = [text];
    deps.agent = async (r, limits, signal) => {
      const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: limits.timeoutMs, maxTokensPerChild: limits.maxTokens, retainRuns: false, sessionExport: false }, { workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, "runs"), fullCodeMode: false });
      managers.push(manager);
      try { return await manager.run(r, signal); } finally { await manager.close(); }
    };
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    expect((await judge(request, deps)).verdict).toBe("unknown");
    expect(launch).toHaveBeenCalledTimes(1);
  });
  it.each(["cancel", "timeout"])("%s waits until its owned process has exited (waiting alone does not stop it)", async scenario => {
    const request = input(); request.evidence.excerpts = ["HANG_WITH_PROGRESS"];
    if (scenario === "timeout") request.timeboxMs = 1500;
    const abort = new AbortController();
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    let worker: Awaited<ReturnType<ProcessTransport["launch"]>> | undefined;
    let canceller: ReturnType<typeof setTimeout> | undefined;
    deps.agent = async (r, limits, signal) => {
      const pending = runJudgmentAgent(r, limits, signal, { piBinary: path.resolve("tests/fixtures/fake-pi-route.mjs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
      // A lifecycle-free fake worker is enough to expose a detached wait bug.
      await vi.waitFor(() => expect(launch.mock.results[0]?.type).toBe("return"));
      worker = await launch.mock.results[0]!.value;
      if (scenario === "cancel") canceller = setTimeout(() => abort.abort(), 150);
      return pending;
    };
    try {
      expect(await judge(request, deps, abort.signal)).toMatchObject({ verdict: "unknown", reasonCode: scenario === "cancel" ? "cancelled" : "timeout" });
      expect(await worker!.isAlive()).toBe(false);
      expect(launch).toHaveBeenCalledTimes(1);
    } finally { clearTimeout(canceller); }
  });
});
