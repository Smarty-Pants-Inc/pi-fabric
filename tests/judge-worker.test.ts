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
const ownedRoots: string[] = [];
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
  for (const owned of ownedRoots.splice(0)) fs.rmSync(owned, { recursive: true, force: true });
});
const events = () => {
  const runs = path.join(root, "runs");
  const id = fs.readdirSync(runs).find(id => fs.existsSync(path.join(runs, id, "events.jsonl")))!;
  return fs.readFileSync(path.join(runs, id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
};
describe.skipIf(!fs.existsSync("dist/worker.js"))("built judgment worker isolation", () => {
  it("has only host delivery/session/reply/route hooks, no ambient authority, no full-code and no schema-free text result", async () => {
    const result = await judge(input(), deps);
    expect(result).toMatchObject({ verdict: "unknown", reasonCode: "invalid_schema" });
    const launch = events().find(e => e.type === "fake_route_launch");
    for (const flag of ["--no-extensions", "--no-skills", "--no-prompt-templates", "--no-context-files", "--no-themes", "--no-approve", "--no-auto-compaction", "--system-prompt"]) expect(launch.argv).toContain(flag);
    expect(launch.argv[launch.argv.indexOf("--tools") + 1]).toBe("fabric_reply");
    // Delivery is a host command consuming private worker metadata, not a model tool.
    expect(launch.argv.filter((_: string, i: number) => launch.argv[i - 1] === "-e").map((p: string) => path.basename(p))).toEqual(["principal-delivery.js", "session-id.js", "reply-tool.js", "model-route-hook.js"]);
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
describe("retained owned judgment receipts", () => {
  it("preserves a persistently unsaved child outcome through close, fails closed, and permits recovery", async () => {
    let owner!: AgentManager;
    const spawn = AgentManager.prototype.spawn;
    vi.spyOn(AgentManager.prototype, "spawn").mockImplementation(function (this: AgentManager, ...args) { owner = this; return spawn.apply(this, args); });
    const write = fs.writeFileSync;
    const fault = vi.spyOn(fs, "writeFileSync").mockImplementation((file: any, data: any, opts: any) => {
      if (typeof file === "number" && typeof data === "string" && data.startsWith('{"type":"outcome"')) throw new Error("persistent child outcome failure");
      return write(file, data, opts);
    });
    deps.agent = async (r, limits, signal) => {
      const result = await runJudgmentAgent(r, limits, signal, { piBinary: path.resolve("tests/fixtures/fake-pi-route.mjs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
      return { ...result, replyVia: "tool", value: { verdict: "dependency", confidence: .8, evidenceLinks: [input().evidenceRefs[0]!.url], nextAction: { kind: "wait_dependency", owner: "dev-lead", targetRef: input().itemRef } } };
    };
    const result = await judge(input(), deps);
    const rows = fs.readFileSync(deps.ledger, "utf8").trim().split("\n").map(line => JSON.parse(line));
    const attempt = rows.find(row => row.backend === "pi-process");
    const run = owner.runDirectory(attempt.childAgentId)!;
    ownedRoots.push(path.dirname(path.dirname(run)));
    expect(result).toMatchObject({ verdict: "unknown", reasonCode: "agent_cleanup_unresolved", cost: { tokens: 5 } });
    expect(attempt.error).toContain(ownedRoots[0]);
    expect(fs.existsSync(path.join(run, "route-session.jsonl"))).toBe(true);
    expect(JSON.parse(fs.readFileSync(path.join(run, "status.json"), "utf8"))).toMatchObject({ usage: { input: 1, output: 2 } });
    expect(JSON.parse(fs.readFileSync(path.join(run, "pending-route-outcome.json"), "utf8"))).toMatchObject({ ledger: deps.ledger, record: { decisionId: result.decisionId, status: "completed", tokens: { input: 1, output: 2 } } });
    fault.mockRestore();
    await owner.cleanup(attempt.childAgentId);
    expect(fs.existsSync(run)).toBe(false);
    const recovered = fs.readFileSync(deps.ledger, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(recovered.filter(row => row.type === "outcome")).toEqual([expect.objectContaining({ decisionId: result.decisionId, tokens: expect.objectContaining({ input: 1, output: 2 }) })]);
  });
  it("preserves an unconfirmed exit even if writing its unresolved-worker marker fails", async () => {
    let owner!: AgentManager;
    const spawn = AgentManager.prototype.spawn;
    vi.spyOn(AgentManager.prototype, "spawn").mockImplementation(function (this: AgentManager, ...args) { owner = this; return spawn.apply(this, args); });
    const launch = ProcessTransport.prototype.launch;
    vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, args) {
      const handle = await launch.call(this, args);
      return { ...handle, lostContact: () => "probe: exit confirmation unavailable" };
    });
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file: any, data: any, opts: any) => {
      if (typeof file === "string" && file.includes("unresolved-worker")) throw new Error("marker write failed");
      return write(file, data, opts);
    });
    deps.agent = (r, limits, signal) => runJudgmentAgent(r, limits, signal, { piBinary: path.resolve("tests/fixtures/fake-pi-route.mjs"), workerPath: path.resolve("tests/fixtures/fake-worker.mjs") });
    const result = await judge(input(), deps);
    const attempt = fs.readFileSync(deps.ledger, "utf8").trim().split("\n").map(line => JSON.parse(line)).find(row => row.backend === "pi-process");
    const run = owner.runDirectory(attempt.childAgentId)!;
    ownedRoots.push(path.dirname(path.dirname(run)));
    expect(fs.existsSync(path.join(run, "unresolved-worker.json"))).toBe(false);
    expect(result).toMatchObject({ verdict: "unknown", reasonCode: "agent_cleanup_unresolved" });
    expect(fs.existsSync(path.join(run, "status.json"))).toBe(true);
    expect(attempt.error).toContain(ownedRoots[0]);
  });
});

describe.skipIf(!fs.existsSync("dist/worker.js"))("built judgment token cause", () => {
  it("classifies the real worker's timed_out token overshoot as token_budget with reported usage", async () => {
    const request = input(); request.budget.maxTokens = 6;
    const result = await judge(request, deps);
    expect(result).toMatchObject({ verdict: "unknown", reasonCode: "token_budget", cost: { tokens: 7 } });
    const rows = fs.readFileSync(deps.ledger, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows.find(row => row.type === "outcome")).toMatchObject({ status: "timed_out", tokens: { input: 2, output: 3 } });
    expect(rows.at(-1)).toMatchObject({ type: "judgment-outcome", reasonCode: "token_budget", cost: { tokens: 7 } });
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
    const request = input(); request.evidence.excerpts = ["HANG_WITH_PROGRESS_CACHE"];
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
      await vi.waitFor(() => {
        const statusFlag = launch.mock.calls[0]![0].workerArguments.indexOf("--status-file");
        const status = launch.mock.calls[0]![0].workerArguments[statusFlag + 1]!;
        expect(JSON.parse(fs.readFileSync(status, "utf8")).usage.input).toBe(30);
      });
      if (scenario === "cancel") canceller = setTimeout(() => abort.abort(), 10);
      return pending;
    };
    try {
      const result = await judge(request, deps, abort.signal);
      expect(result).toMatchObject({ verdict: "unknown", reasonCode: scenario === "cancel" ? "cancelled" : "timeout", cost: { tokens: 54 } });
      const rows = fs.readFileSync(deps.ledger, "utf8").trim().split("\n").map(line => JSON.parse(line));
      expect(rows.find(row => row.backend === "pi-process")).toMatchObject({ decisionId: result.decisionId, usage: { input: 30, output: 10, cacheRead: 5, cacheWrite: 7 } });
      expect(rows.find(row => row.type === "outcome")).toMatchObject({ decisionId: result.decisionId, tokens: { input: 30, output: 10, cacheRead: 5, cacheWrite: 7 } });
      expect(rows.at(-1)).toMatchObject({ type: "judgment-outcome", decisionId: result.decisionId, cost: { tokens: 54 } });
      expect(await worker!.isAlive()).toBe(false);
      expect(launch).toHaveBeenCalledTimes(1);
    } finally { clearTimeout(canceller); }
  });
});
