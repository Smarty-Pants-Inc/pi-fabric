import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { prepareModelRoute } from "../src/agents/model-route-prepare.js";
import { decideModelRoute, isRouteAdmissionBlocked, prepareRouteDispatch, ROUTE_DEADLINE_MS, routeLaunchCandidate, type ModelRoutingConfig } from "../src/agents/model-route.js";
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

// This is a fresh process with no module/cache state: the host journal alone restores admission safety.
const restartedSwitch = (blocked = true) => {
  const probe = spawnSync("bun", ["-e", `import { isRouteAdmissionBlocked } from ${JSON.stringify(path.resolve("src/agents/model-route.ts"))};
    import { prepareModelRoute } from ${JSON.stringify(path.resolve("src/agents/model-route-prepare.ts"))};
    const reverted = isRouteAdmissionBlocked("status-groom");
    const decision = await prepareModelRoute({routeClass:"status-groom",protected:false,pinModel:"test/sol",pinThinking:"high",parentSessionId:"fresh-main",
      registry:{getAvailable:()=>[{provider:"test",id:"sol"},{provider:"test",id:"luna"}]},aliases:{},config:${JSON.stringify(config)},assertModelAllowed(){},evaluate:async()=>(${JSON.stringify(answer())})});
    console.log(JSON.stringify({reverted,decision}));`], { encoding: "utf8", env: process.env });
  expect(probe.status, probe.stderr).toBe(0);
  expect(JSON.parse(probe.stdout.trim())).toMatchObject({ reverted: blocked, decision: blocked ? { ...pin, mode: "shadow", reasonCode: "admission-blocked" } : { ...candidate, mode: "live", reasonCode: "live-choice" } });
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
  it("reverts manually with an empty allowlist and keeps other opted-in classes live", async () => {
    const m = manager();
    const live = await m.spawn({ task: "ECHO_MODEL", routeDecision: await prepareModelRoute(input) });
    expect(await m.wait(live.id)).toMatchObject({ model: candidate.model, thinking: candidate.effort });
    const reverted = await prepareModelRoute({ ...input, config: { ...config, liveClasses: [] } });
    expect(reverted.mode).toBe("shadow");
    const pinned = await m.spawn({ task: "ECHO_MODEL", routeDecision: reverted });
    expect(await m.wait(pinned.id)).toMatchObject({ model: pin.model, thinking: pin.effort });
    expect((await prepareModelRoute({ ...input, routeClass: "bounded-lookup", config: { ...config, liveClasses: ["bounded-lookup"] } })).mode).toBe("live");
  });
  it("never automatically reverts after failed, stopped or timed-out runs or untyped quality assertions", async () => {
    for (const status of ["failed", "stopped", "timed_out", "completed"] as const) {
      const run = await dispatch();
      run.dispatch.outcome({ status, ...{ routeQuality: "fail" } });
      run.dispatch.outcome({ status });
      expect(isRouteAdmissionBlocked("status-groom")).toBe(false);
      expect(await prepareModelRoute(input)).toMatchObject({ mode: "live", reasonCode: "live-choice" });
    }
    restartedSwitch(false);
    expect(rows().filter(row => row.type === "outcome")).toHaveLength(4);
    expect(rows().some(row => row.type === "revert" || row.type === "quality")).toBe(false);
    expect(rows().filter(row => row.type === "outcome").every(row => !("routeQuality" in row))).toBe(true);
    expect(fs.existsSync(path.join(path.dirname(state()), "model-routing-quality.jsonl"))).toBe(false);
  });
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("fences failed/stopped terminal saves across restart and repairs the original joins exactly once", async () => {
    // Both runs were admitted before the first terminal save failed.
    const first = await dispatch(), second = await dispatch();
    fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(() => first.dispatch.outcome({ status: "failed" })).toThrow();
    expect(() => second.dispatch.outcome({ status: "stopped" })).toThrow();
    restartedSwitch();
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "admission-blocked" });
    expect(fs.readFileSync(state(), "utf8")).toBe("");
    fs.chmodSync(state(), 0o600); expect(isRouteAdmissionBlocked("status-groom")).toBe(false);
    first.dispatch.outcome({ status: "failed" }); second.dispatch.outcome({ status: "stopped" });
    const saved = fs.readFileSync(state(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(saved).toHaveLength(2);
    expect(saved.map(row => [row.decisionId, row.status])).toEqual([[first.decision.decisionId, "failed"], [second.decision.decisionId, "stopped"]]);
    expect(rows().filter(row => row.type === "outcome")).toHaveLength(2);
    expect(rows().filter(row => row.type === "revert")).toHaveLength(0);
    restartedSwitch(false);
  });
  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("pins even one pending successful terminal save and resumes only after repair", async () => {
    const run = await dispatch(); fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(() => run.dispatch.outcome({ status: "completed" })).toThrow(); restartedSwitch();
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, reasonCode: "admission-blocked" });
    fs.chmodSync(state(), 0o600); expect(isRouteAdmissionBlocked("status-groom")).toBe(false);
    expect(await prepareModelRoute(input)).toMatchObject({ mode: "live", reasonCode: "live-choice" });
    run.dispatch.outcome({ status: "completed" }); expect(rows().filter(row => row.type === "outcome")).toHaveLength(1);
  });
  it.each(["EFBIG", "ENOSPC", "EIO"])("pins fresh Main and actor dispatch when both 8192-byte journals reject %s appends", async code => {
    const run = await dispatch();
    const journals = ["model-routing-pending.jsonl", "model-routing-refused.jsonl"].map(name => path.join(path.dirname(state()), name));
    const prefix = JSON.stringify({ type: "committed", receiptId: "full-fixture", at: 1, padding: "" });
    const full = prefix.slice(0, -2) + "x".repeat(8192 - Buffer.byteLength(prefix) - 1) + '"}\n';
    for (const journal of journals) fs.writeFileSync(journal, full, { mode: 0o600 });
    expect(Buffer.byteLength(full)).toBe(8192);
    expect(fs.statSync(ledger()).size).toBeLessThan(8192);
    // Native append-open/fstat/fsync all pass; only a real append reveals the fault.
    for (const journal of journals) { const fd = fs.openSync(journal, "a"); fs.fsyncSync(fd); fs.closeSync(fd); }
    const write = fs.writeFileSync;
    const fault = vi.spyOn(fs, "writeFileSync").mockImplementation((file, ...args) => {
      if (typeof file === "number" && fs.fstatSync(file).size === 8192) throw Object.assign(new Error(code), { code });
      return write(file, ...args);
    });
    expect(() => run.dispatch.outcome({ status: "failed" })).toThrow(code);
    fault.mockRestore();
    for (const journal of journals) expect(fs.readFileSync(journal, "utf8")).toBe(full); // No durable failure fence at all.
    const fresh = spawnSync("bun", ["-e", `import fs from "node:fs";
      import {prepareModelRoute} from ${JSON.stringify(path.resolve("src/agents/model-route-prepare.ts"))};
      import {AgentManager} from ${JSON.stringify(path.resolve("src/agents/manager.ts"))};
      import {DEFAULT_FABRIC_CONFIG} from ${JSON.stringify(path.resolve("src/config.ts"))};
      const write = fs.writeFileSync;
      let refusedAppends = 0;
      fs.writeFileSync = (file, ...args) => {
        if (typeof file === "number" && fs.fstatSync(file).size === 8192) {
          refusedAppends++; throw Object.assign(new Error(${JSON.stringify(code)}), {code:${JSON.stringify(code)}});
        }
        return write(file, ...args);
      };
      const results = [];
      const m = new AgentManager(${JSON.stringify(root)}, {...DEFAULT_FABRIC_CONFIG.agents,retainRuns:true}, {
        workerPath:${JSON.stringify(path.resolve("tests/fixtures/fake-worker.mjs"))},runRoot:${JSON.stringify(path.join(root, "fresh-runs"))},preparePiModel:async model=>model});
      try {
        for (const owner of ["fresh-main", "fresh-actor"]) {
          const decision = await prepareModelRoute({routeClass:"status-groom",protected:false,pinModel:"test/sol",pinThinking:"high",parentSessionId:owner,
            ...(owner === "fresh-actor" ? {actorId:"actor:fresh",activationId:"activation:fresh"} : {}),
            registry:{getAvailable:()=>[{provider:"test",id:"sol"},{provider:"test",id:"luna"}]},aliases:{},config:${JSON.stringify(config)},assertModelAllowed(){},evaluate:async()=>(${JSON.stringify(answer())})});
          if (decision.mode !== "live") throw new Error("Fixture already fenced before the real append");
          const h = await m.spawn({task:"ECHO_MODEL",routeDecision:decision});
          results.push({decision,result:await m.wait(h.id)});
        }
      } finally { await m.close(); }
      console.log(JSON.stringify({results,refusedAppends}));`], { encoding: "utf8", env: process.env, timeout: 30000 });
    expect(fresh.status, fresh.stderr).toBe(0);
    const evidence = JSON.parse(fresh.stdout.trim());
    expect(evidence.refusedAppends).toBe(2);
    for (const result of evidence.results) expect(result).toMatchObject({ decision: { ...pin, mode: "shadow", reasonCode: "record-failed" }, result: { status: "completed", model: pin.model, thinking: pin.effort } });
    for (const journal of journals) expect(fs.statSync(journal).size).toBe(8192);
  }, 30000);

  it("requires the shared admission append even for a pre-recorded decision and pins on fsync failure", async () => {
    const decision = await prepareModelRoute(input);
    const journal = path.join(path.dirname(state()), "model-routing-pending.jsonl");
    fs.mkdirSync(path.dirname(journal), { recursive: true, mode: 0o700 });
    fs.writeFileSync(journal, "", { mode: 0o600 });
    const sync = fs.fsyncSync;
    const fault = vi.spyOn(fs, "fsyncSync").mockImplementation(fd => {
      if (fs.fstatSync(fd).isFile() && fs.fstatSync(fd).size > 0) throw new Error("EIO admission fsync");
      return sync(fd);
    });
    prepareRouteDispatch(decision, undefined, path.join(root, decision.decisionId), decision.decisionId, { decisionRecorded: true });
    expect(decision).toMatchObject({ ...pin, mode: "shadow", reasonCode: "record-failed" });
    fault.mockRestore();
    expect(JSON.parse(fs.readFileSync(journal, "utf8").trim())).toMatchObject({ type: "admission", decisionId: decision.decisionId });
  });

  it.skipIf(process.platform === "win32" || process.getuid?.() === 0)("orders repaired A-success before B-failure and C-stop from three pre-admitted runs", async () => {
    const a = await dispatch(), b = await dispatch(), c = await dispatch();
    for (const run of [a, b, c]) expect(run.decision.mode).toBe("live");
    fs.writeFileSync(state(), "", { mode: 0o400 }); fs.chmodSync(state(), 0o400);
    expect(() => a.dispatch.outcome({ status: "completed" })).toThrow();
    fs.chmodSync(state(), 0o600); b.dispatch.outcome({ status: "failed" });
    fs.chmodSync(state(), 0o400); expect(() => c.dispatch.outcome({ status: "stopped" })).toThrow();
    restartedSwitch();
    fs.chmodSync(state(), 0o600); restartedSwitch(false); // Fresh owner repairs, in physical B/A/C order.
    const saved = fs.readFileSync(state(), "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(saved.map(row => row.decisionId)).toEqual([b.decision.decisionId, a.decision.decisionId, c.decision.decisionId]);
    expect([...saved].sort((x, y) => x.at - y.at).map(row => row.status)).toEqual(["completed", "failed", "stopped"]);
    expect(await prepareModelRoute(input)).toMatchObject({ ...candidate, mode: "live", reasonCode: "live-choice" });
    a.dispatch.outcome({ status: "completed" }); b.dispatch.outcome({ status: "failed" }); c.dispatch.outcome({ status: "stopped" });
    restartedSwitch(false);
    expect(fs.readFileSync(state(), "utf8").trim().split("\n")).toHaveLength(3);
    expect(rows().filter(row => row.type === "outcome")).toHaveLength(3);
    expect(rows().filter(row => row.type === "revert")).toHaveLength(0);
  });

  it("fails closed on corrupt, linked or unsafe durable state", async () => {
    fs.mkdirSync(path.dirname(state()), { recursive: true, mode: 0o700 });
    fs.writeFileSync(state(), "not-json\n", { mode: 0o600 });
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "admission-state-error" });
    fs.writeFileSync(state(), "{}\n");
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "admission-state-error" });
    fs.unlinkSync(state()); const target = path.join(root, "state-target"); fs.writeFileSync(target, "", { mode: 0o600 }); fs.symlinkSync(target, state());
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "admission-state-error" });
  });
});
