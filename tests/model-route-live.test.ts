import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { prepareModelRoute } from "../src/agents/model-route-prepare.js";
import { decideModelRoute, isRouteClassReverted, prepareRouteDispatch, ROUTE_DEADLINE_MS, routeLaunchCandidate, type ModelRoutingConfig } from "../src/agents/model-route.js";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG, loadFabricConfig, normalizeFabricConfig } from "../src/config.js";
import type { JevResponse } from "../src/jev/types.js";

const pin = { model: "test/sol", effort: "high" as const };
const candidate = { model: "test/luna", effort: "max" as const };
const registry = { getAvailable: () => [{ provider: "test", id: "sol" }, { provider: "test", id: "luna" }] };
const config: ModelRoutingConfig = { live: false, liveClasses: ["status-groom"], shadowCandidates: [candidate] };
const answer = (confidence = .95): JevResponse => ({ model: "jev", answers: { route: { type: "choice", choice: "candidate-1", confidence,
  probabilities: { "candidate-0": .05, "candidate-1": .95 } } }, usage: { input_tokens: 1, output_tokens: 1 } });
const input = { routeClass: "status-groom", protected: false, pinModel: pin.model, pinThinking: pin.effort,
  registry, aliases: {}, config, assertModelAllowed() {}, parentSessionId: "main", evaluate: async () => answer() };
let root: string;
const managers: AgentManager[] = [];
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "route-live-")); vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent")); });
afterEach(async () => { vi.useRealTimers(); await Promise.all(managers.splice(0).map(manager => manager.close())); vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); });
const ledger = () => path.join(process.env.PI_CODING_AGENT_DIR!, "fabric/model-routing.jsonl");
const state = () => path.join(process.env.PI_CODING_AGENT_DIR!, "fabric/model-routing-state.jsonl");
const rows = () => fs.readFileSync(ledger(), "utf8").trim().split("\n").map(line => JSON.parse(line));
const manager = () => { const m = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, retainRuns: true }, {
  workerPath: path.resolve("tests/fixtures/fake-worker.mjs"), runRoot: path.join(root, `runs-${managers.length}`), preparePiModel: async model => model,
}); managers.push(m); return m; };
const dispatch = async () => { const decision = await prepareModelRoute(input); return { decision,
  dispatch: prepareRouteDispatch(decision, undefined, path.join(root, decision.decisionId), decision.decisionId) }; };

// This is a fresh process with no module/cache state: the host journal alone restores the switch.
const restartedSwitch = () => {
  const probe = spawnSync("bun", ["-e", `import { isRouteClassReverted } from ${JSON.stringify(path.resolve("src/agents/model-route.ts"))};
    import { prepareModelRoute } from ${JSON.stringify(path.resolve("src/agents/model-route-prepare.ts"))};
    const reverted = isRouteClassReverted("status-groom");
    const decision = await prepareModelRoute({routeClass:"status-groom",protected:false,pinModel:"test/sol",pinThinking:"high",parentSessionId:"fresh-main",
      registry:{getAvailable:()=>[{provider:"test",id:"sol"},{provider:"test",id:"luna"}]},aliases:{},config:${JSON.stringify(config)},assertModelAllowed(){},evaluate:async()=>(${JSON.stringify(answer())})});
    console.log(JSON.stringify({reverted,decision}));`], { encoding: "utf8", env: process.env });
  expect(probe.status, probe.stderr).toBe(0);
  expect(JSON.parse(probe.stdout.trim())).toMatchObject({ reverted: true, decision: { ...pin, mode: "shadow", reasonCode: "class-reverted" } });
};

describe("live model routing", () => {
  it("defaults to empty liveClasses and accepts only trusted string lists", () => {
    expect(normalizeFabricConfig({ agents: { modelRouting: {} } }).agents.modelRouting).toMatchObject({ live: false, liveClasses: [] });
    for (const liveClasses of [true, "status-groom", [false], ["bad/header"]]) expect(() => normalizeFabricConfig({ agents: { modelRouting: { liveClasses } } })).toThrow("liveClasses");
    expect(() => normalizeFabricConfig({ agents: { modelRouting: { live: true } } })).toThrow();
  });
  it("loads liveClasses only from host or trusted project config, never an untrusted project", () => {
    const agentDir = process.env.PI_CODING_AGENT_DIR!; fs.mkdirSync(agentDir, { recursive: true }); fs.mkdirSync(path.join(root, ".pi"));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ agents: { modelRouting: { liveClasses: ["status-groom"] } } }));
    fs.writeFileSync(path.join(root, ".pi/fabric.json"), JSON.stringify({ agents: { modelRouting: { liveClasses: ["task:exact-checks"] } } }));
    expect(loadFabricConfig({ cwd: root, agentDir, projectTrusted: false }).agents.modelRouting?.liveClasses).toEqual(["status-groom"]);
    expect(loadFabricConfig({ cwd: root, agentDir, projectTrusted: true }).agents.modelRouting?.liveClasses).toEqual(["task:exact-checks"]);
  });
  it("launches candidate model+effort and joins worker header, decision and outcome", async () => {
    const decision = await prepareModelRoute(input);
    expect(decision).toMatchObject({ ...candidate, mode: "live", reasonCode: "live-choice", pin });
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const m = manager(); const handle = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision });
    expect(handle).toMatchObject({ model: candidate.model, thinking: candidate.effort });
    expect(await m.wait(handle.id)).toMatchObject({ status: "completed", model: candidate.model, thinking: candidate.effort });
    const args = launch.mock.calls[0]![0].workerArguments;
    expect(args).toEqual(expect.arrayContaining(["--model", candidate.model, "--thinking", candidate.effort]));
    expect(args[args.indexOf("--route-header") + 1]).toBe(`status-groom/test%2Fluna-max/live-choice:${decision.decisionId}`);
    expect(rows().find(row => row.type === "outcome")).toMatchObject({ decisionId: decision.decisionId, admittedModel: candidate.model, admittedEffort: candidate.effort });
  });
  it("freezes the live candidate across real worker resume", async () => {
    const launch = vi.spyOn(ProcessTransport.prototype, "launch");
    const decision = await prepareModelRoute(input); const m = manager();
    const handle = await m.spawn({ task: "RESUME_AFTER_CRASH", routeDecision: decision });
    expect(await m.wait(handle.id)).toMatchObject({ status: "completed", model: candidate.model, thinking: candidate.effort });
    expect(launch.mock.calls.length).toBeGreaterThan(1);
    for (const [request] of launch.mock.calls) expect(request.workerArguments).toEqual(expect.arrayContaining(["--model", candidate.model, "--thinking", candidate.effort]));
    expect(rows().filter(row => row.type === "outcome")).toHaveLength(1);
  }, 30000);
  it("launches the pin for an unlisted class even when Jev prefers the candidate", async () => {
    const decision = await prepareModelRoute({ ...input, config: { ...config, liveClasses: [] } });
    expect(routeLaunchCandidate(decision)).toEqual(pin); expect(decision.mode).toBe("shadow");
    const m = manager(); const handle = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision });
    expect(await m.wait(handle.id)).toMatchObject({ model: pin.model, thinking: pin.effort });
  });
  it.each(["error", "malformed", "confidence", "probability", "timeout"])("falls back to the pin on %s", async reason => {
    const evaluate = async (): Promise<JevResponse> => {
      if (reason === "error") throw new Error("backend unavailable");
      if (reason === "timeout") throw new Error("timeout");
      if (reason === "malformed") return { ...answer(), answers: {} };
      if (reason === "probability") return { ...answer(), answers: { route: { type: "choice", choice: "candidate-1", confidence: 1, probabilities: { "candidate-0": .2, "candidate-1": .8 } } } };
      return answer(.89);
    };
    const decision = await prepareModelRoute({ ...input, evaluate });
    expect(routeLaunchCandidate(decision)).toEqual(pin);
    expect(decision.reasonCode).toBe(({ error: "jev-error", malformed: "malformed", timeout: "jev-timeout" } as Record<string, string>)[reason] ?? "low-confidence");
    const m = manager(); const handle = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision });
    expect(await m.wait(handle.id)).toMatchObject({ model: pin.model, thinking: pin.effort });
  });
  it("keeps the absolute deadline in live mode", async () => {
    vi.useFakeTimers(); const pending = decideModelRoute({ routeClass: input.routeClass, protected: false, pin, candidates: [candidate], parentSessionId: "main", live: true }, () => new Promise(() => {}));
    await vi.advanceTimersByTimeAsync(ROUTE_DEADLINE_MS);
    expect(await pending).toMatchObject({ ...pin, reasonCode: "jev-timeout" });
  });
  it("falls back before admission when durable decision storage fails", async () => {
    fs.mkdirSync(path.dirname(ledger()), { recursive: true }); fs.mkdirSync(ledger());
    const decision = await prepareModelRoute(input); const m = manager();
    const handle = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision });
    expect(handle).toMatchObject({ model: pin.model, thinking: pin.effort });
    fs.rmdirSync(ledger());
    expect(await m.wait(handle.id)).toMatchObject({ status: "completed", model: pin.model });
    expect(decision).toMatchObject({ mode: "shadow", reasonCode: "record-failed" });
  });
  it.each(["review", "security", "audit", "actor:review", "task:security", "task:pi:process"])("never enables protected/derived class %s even if listed and protected:false", async routeClass => {
    const evaluate = vi.fn(async () => answer());
    const decision = await prepareModelRoute({ ...input, routeClass, config: { ...config, liveClasses: [routeClass] }, evaluate });
    expect(decision).toMatchObject({ ...pin, mode: "shadow", reasonCode: "excluded-class" }); expect(evaluate).not.toHaveBeenCalled();
  });
  it.each([true, undefined])("never enables a listed class with protection %s", async protection => {
    const evaluate = vi.fn(async () => answer());
    expect(await prepareModelRoute({ ...input, protected: protection, evaluate })).toMatchObject({ ...pin, mode: "shadow" }); expect(evaluate).not.toHaveBeenCalled();
  });
  it.each(["task:merge-additive", "task:ci-test-fixture", "task:exact-checks"])("requires opt-in for explicit %s", async routeClass => {
    expect((await prepareModelRoute({ ...input, routeClass })).mode).toBe("shadow");
    expect(await prepareModelRoute({ ...input, routeClass, config: { ...config, liveClasses: [routeClass] } })).toMatchObject({ ...candidate, mode: "live" });
  });
  it("refuses denied and unsupported effort candidates before Jev", async () => {
    const evaluate = vi.fn(async () => answer());
    expect(await prepareModelRoute({ ...input, evaluate, assertModelAllowed(model) { if (model === candidate.model) throw new Error("denied"); } })).toMatchObject({ ...pin, reasonCode: "invalid-candidates" });
    const noReasoning = { getAvailable: () => [{ provider: "test", id: "sol" }, { provider: "test", id: "luna", reasoning: false }] };
    expect(await prepareModelRoute({ ...input, evaluate, registry: noReasoning })).toMatchObject({ ...pin, reasonCode: "invalid-candidates" });
    expect(evaluate).not.toHaveBeenCalled();
  });
  it("reverts immediately on outcome quality fail and survives restart", async () => {
    const run = await dispatch(); run.dispatch.outcome({ status: "completed", routeQuality: "fail" });
    expect(isRouteClassReverted("status-groom")).toBe(true); restartedSwitch();
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
    expect(rows().find(row => row.type === "revert")).toMatchObject({ routeClass: "status-groom", reason: "quality-fail", decisionId: run.decision.decisionId });
  });
  it("lets the caller report a review FAIL after settlement through owned run identity", async () => {
    const m = manager(); const decision = await prepareModelRoute(input); const run = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision });
    await m.wait(run.id); m.reportRouteQuality(run.id, "fail");
    expect(await prepareModelRoute(input)).toMatchObject({ mode: "shadow", reasonCode: "class-reverted" });
    expect(rows().find(row => row.type === "quality")).toMatchObject({ decisionId: decision.decisionId, runId: run.id, routeQuality: "fail" });
    expect(() => m.reportRouteQuality("foreign-run", "fail")).toThrow();
  });
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("fences a quality fail with readable unwritable state across restart and retries without losing its join", async () => {
    const run = await dispatch();
    fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(fs.readFileSync(state(), "utf8")).toBe("");
    expect(() => run.dispatch.reportQuality("fail")).toThrow();
    expect(rows().find(row => row.type === "quality")).toMatchObject({ decisionId: run.decision.decisionId, routeQuality: "fail" });
    const journal = path.join(path.dirname(state()), "model-routing-quality.jsonl");
    const receipts = () => fs.readFileSync(journal, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(receipts().map(row => row.type)).toEqual(["pending"]);
    restartedSwitch(); // Fresh process still sees pending receipt, not the empty safety state.
    const next = await prepareModelRoute(input);
    expect(next).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
    const m = manager(); const handle = await m.spawn({ task: "ECHO_MODEL", routeDecision: next });
    expect(await m.wait(handle.id)).toMatchObject({ model: pin.model, thinking: pin.effort });
    expect(fs.readFileSync(state(), "utf8")).toBe("");
    fs.chmodSync(state(), 0o600);
    expect(isRouteClassReverted("status-groom")).toBe(true);
    expect(receipts().map(row => row.type)).toEqual(["pending", "committed"]);
    expect(rows().filter(row => row.type === "quality")).toHaveLength(1);
    expect(JSON.parse(fs.readFileSync(state(), "utf8").trim())).toMatchObject({ decisionId: run.decision.decisionId, routeQuality: "fail" });
    restartedSwitch();
  });
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("fences the FIRST quality intent failure through the public report in a fresh process", async () => {
    const m = manager(); const decision = await prepareModelRoute(input);
    const run = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision }); await m.wait(run.id);
    const journal = path.join(path.dirname(state()), "model-routing-quality.jsonl");
    fs.writeFileSync(journal, "", { mode: 0o400 }); fs.chmodSync(journal, 0o400);
    expect(() => m.reportRouteQuality(run.id, "fail")).toThrow();
    expect(fs.readFileSync(journal, "utf8")).toBe("");
    const safety = fs.readFileSync(path.join(path.dirname(state()), "model-routing-pending.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(safety.at(-1)).toMatchObject({ type: "pending", decision: { decisionId: decision.decisionId }, result: { routeQuality: "fail" } });
    restartedSwitch();
    const next = await prepareModelRoute(input);
    expect(next).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
    const pinned = await m.spawn({ task: "ECHO_MODEL", routeDecision: next });
    expect(await m.wait(pinned.id)).toMatchObject({ model: pin.model, thinking: pin.effort });
    expect(await prepareModelRoute({ ...input, routeClass: "bounded-lookup", config: { ...config, liveClasses: ["bounded-lookup"] } })).toMatchObject({ mode: "live" });
    fs.chmodSync(journal, 0o600); expect(isRouteClassReverted("status-groom")).toBe(true); restartedSwitch();
    expect(rows().filter(row => row.type === "quality")).toHaveLength(1);
    expect(rows().find(row => row.type === "quality")).toMatchObject({ decisionId: decision.decisionId, runId: run.id, routeQuality: "fail" });
  });
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("fences failed/stopped terminal saves across restart and repairs the original streak exactly once", async () => {
    // Both runs were admitted before the first terminal save failed.
    const first = await dispatch(), second = await dispatch();
    fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(() => first.dispatch.outcome({ status: "failed" })).toThrow();
    expect(() => second.dispatch.outcome({ status: "stopped" })).toThrow();
    restartedSwitch();
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
    expect(fs.readFileSync(state(), "utf8")).toBe("");
    fs.chmodSync(state(), 0o600); expect(isRouteClassReverted("status-groom")).toBe(true);
    first.dispatch.outcome({ status: "failed" }); second.dispatch.outcome({ status: "stopped" });
    const saved = fs.readFileSync(state(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(saved).toHaveLength(2);
    expect(saved.map(row => [row.decisionId, row.status])).toEqual([[first.decision.decisionId, "failed"], [second.decision.decisionId, "stopped"]]);
    expect(rows().filter(row => row.type === "outcome")).toHaveLength(2);
    expect(rows().filter(row => row.type === "revert")).toHaveLength(1);
    expect(rows().find(row => row.type === "revert")).toMatchObject({ reason: "consecutive-failures", decisionId: second.decision.decisionId });
    restartedSwitch();
  });
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("pins even one pending successful terminal save and resumes only after repair", async () => {
    const run = await dispatch(); fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(() => run.dispatch.outcome({ status: "completed" })).toThrow(); restartedSwitch();
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, reasonCode: "class-reverted" });
    fs.chmodSync(state(), 0o600); expect(isRouteClassReverted("status-groom")).toBe(false);
    expect(await prepareModelRoute(input)).toMatchObject({ mode: "live", reasonCode: "live-choice" });
    run.dispatch.outcome({ status: "completed" }); expect(rows().filter(row => row.type === "outcome")).toHaveLength(1);
  });
  it.each([0, 128])("fails closed across fresh owners when the writable safety journal has only %s bytes left", async remaining => {
    const decision = await prepareModelRoute(input), m = manager();
    const run = await m.spawn({ task: "ECHO_MODEL", routeDecision: decision }); await m.wait(run.id);
    const journal = path.join(path.dirname(state()), "model-routing-pending.jsonl");
    const prefix = JSON.stringify({ type: "committed", receiptId: "capacity-fixture", at: 1, padding: "" });
    const row = prefix.slice(0, -2) + "x".repeat(64 * 1024 - Buffer.byteLength(prefix) - 1) + '"}\n';
    expect(Buffer.byteLength(row)).toBe(64 * 1024);
    // Safe committed rows; append-open/fsync succeeds, an actual intent cannot.
    fs.writeFileSync(journal, row.repeat(1024));
    if (remaining) fs.truncateSync(journal, 64 * 1024 * 1024 - 64 * 1024);
    if (remaining) {
      const tail = JSON.stringify({ type: "committed", receiptId: "tail", at: 1, padding: "" });
      fs.appendFileSync(journal, tail.slice(0, -2) + "x".repeat(64 * 1024 - remaining - Buffer.byteLength(tail) - 1) + '"}\n');
    }
    const fd = fs.openSync(journal, "a"); fs.fsyncSync(fd); fs.closeSync(fd);
    expect(fs.statSync(journal).size).toBe(64 * 1024 * 1024 - remaining);
    expect(() => m.reportRouteQuality(run.id, "fail")).toThrow(/oversized/);
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "revert-state-error" });
    const fresh = spawnSync("bun", ["-e", `import {prepareModelRoute} from ${JSON.stringify(path.resolve("src/agents/model-route-prepare.ts"))};
      console.log(JSON.stringify(await prepareModelRoute({routeClass:"status-groom",protected:false,pinModel:"test/sol",pinThinking:"high",parentSessionId:"fresh-main",
        registry:{getAvailable:()=>[{provider:"test",id:"sol"},{provider:"test",id:"luna"}]},aliases:{},config:${JSON.stringify(config)},assertModelAllowed(){},evaluate:async()=>(${JSON.stringify(answer())})})));`], { encoding: "utf8", env: process.env });
    expect(fresh.status, fresh.stderr).toBe(0);
    expect(JSON.parse(fresh.stdout.trim())).toMatchObject({ ...pin, mode: "shadow", reasonCode: "revert-state-error" });
    const refused = path.join(path.dirname(state()), "model-routing-refused.jsonl");
    expect(JSON.parse(fs.readFileSync(refused, "utf8").trim())).toMatchObject({ decision: { decisionId: decision.decisionId }, result: { routeQuality: "fail" } });
    // Explicit reconciliation retires ONLY these committed capacity-fixture rows.
    // The refused assertion must replay from disk in a fresh owner, not be erased.
    fs.truncateSync(journal, 0); restartedSwitch();
    expect(rows().filter(row => row.type === "quality")).toHaveLength(1);
    expect(rows().find(row => row.type === "quality")).toMatchObject({ runId: run.id, routeQuality: "fail" });
    expect(isRouteClassReverted("status-groom")).toBe(true); restartedSwitch();
    expect(rows().filter(row => row.type === "quality")).toHaveLength(1);
  }, 30000);

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("orders repaired A-success before B-failure and C-stop from three pre-admitted runs", async () => {
    const a = await dispatch(), b = await dispatch(), c = await dispatch();
    for (const run of [a, b, c]) expect(run.decision.mode).toBe("live");
    fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(() => a.dispatch.outcome({ status: "completed" })).toThrow();
    fs.chmodSync(state(), 0o600); b.dispatch.outcome({ status: "failed" });
    fs.chmodSync(state(), 0o400); expect(() => c.dispatch.outcome({ status: "stopped" })).toThrow();
    restartedSwitch();
    fs.chmodSync(state(), 0o600); restartedSwitch(); // Fresh owner repairs, in physical B/A/C order.
    const saved = fs.readFileSync(state(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(saved.map(row => row.decisionId)).toEqual([b.decision.decisionId, a.decision.decisionId, c.decision.decisionId]);
    expect([...saved].sort((x, y) => x.at - y.at).map(row => row.status)).toEqual(["completed", "failed", "stopped"]);
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
    a.dispatch.outcome({ status: "completed" }); b.dispatch.outcome({ status: "failed" }); c.dispatch.outcome({ status: "stopped" });
    restartedSwitch();
    expect(fs.readFileSync(state(), "utf8").trim().split("\n")).toHaveLength(3);
    expect(rows().filter(row => row.type === "outcome")).toHaveLength(3);
    expect(rows().filter(row => row.type === "revert")).toHaveLength(1);
  });

  it("keeps non-live quality feedback audit-only without poisoning live admission", async () => {
    const shadow = await prepareModelRoute({ ...input, routeClass: "critical-read", protected: true });
    const report = prepareRouteDispatch(shadow, undefined, path.join(root, shadow.decisionId), shadow.decisionId);
    report.reportQuality("fail");
    expect(rows().find(row => row.type === "quality")).toMatchObject({ routeClass: "critical-read", routeQuality: "fail" });
    expect(await prepareModelRoute(input)).toMatchObject({ mode: "live", reasonCode: "live-choice" });
  });
  it("reverts after two consecutive failed/aborted runs, not after one or duplicate settlement", async () => {
    const first = await dispatch(); first.dispatch.outcome({ status: "failed" }); first.dispatch.outcome({ status: "failed" });
    expect(isRouteClassReverted("status-groom")).toBe(false);
    const second = await dispatch(); second.dispatch.outcome({ status: "stopped" });
    restartedSwitch(); expect((await prepareModelRoute(input)).reasonCode).toBe("class-reverted");
    expect(rows().filter(row => row.type === "revert")).toHaveLength(1);
    expect(rows().find(row => row.type === "revert")).toMatchObject({ reason: "consecutive-failures", decisionId: second.decision.decisionId });
  });
  it("resets the failure streak on success, isolates classes, and reverses with a new config generation", async () => {
    for (const status of ["failed", "completed", "timed_out"] as const) (await dispatch()).dispatch.outcome({ status });
    expect(isRouteClassReverted("status-groom")).toBe(false);
    const old = await dispatch(); old.dispatch.outcome({ status: "failed" });
    const resetConfig = { ...config, revertReset: { "status-groom": "approved-retry-2" } };
    expect(await prepareModelRoute({ ...input, config: resetConfig })).toMatchObject({ mode: "live", revertReset: "approved-retry-2" });
    old.dispatch.reportQuality("fail");
    expect((await prepareModelRoute({ ...input, config: resetConfig })).mode).toBe("live");
    expect((await prepareModelRoute({ ...input, routeClass: "bounded-lookup", config: { ...config, liveClasses: ["bounded-lookup"] } })).mode).toBe("live");
  });
  it("fails closed on corrupt, linked or unsafe durable state", async () => {
    fs.mkdirSync(path.dirname(state()), { recursive: true, mode: 0o700 });
    fs.writeFileSync(state(), "not-json\n", { mode: 0o600 });
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "revert-state-error" });
    fs.writeFileSync(state(), "{}\n");
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "revert-state-error" });
    fs.unlinkSync(state()); const target = path.join(root, "state-target"); fs.writeFileSync(target, "", { mode: 0o600 }); fs.symlinkSync(target, state());
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "revert-state-error" });
  });
});
