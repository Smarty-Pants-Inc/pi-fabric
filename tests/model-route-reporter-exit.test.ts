import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { prepareModelRoute } from "../src/agents/model-route-prepare.js";
import { prepareRouteDispatch, readRouteQualityReceipt, reportRouteQualityReceipt } from "../src/agents/model-route.js";
import { ActorLogStore } from "../src/actors/log-store.js";
import { pruneActorRunArchives, runTreeExitVeto } from "../src/storage/retention.js";

const pin = { model: "test/sol", effort: "high" as const };
const candidate = { model: "test/luna", effort: "medium" as const };
const answer = { model: "jev", answers: { route: { type: "choice", choice: "candidate-1", confidence: .95,
  probabilities: { "candidate-0": .05, "candidate-1": .95 } } }, usage: { input_tokens: 1, output_tokens: 1 } };
const config = { live: false as const, liveClasses: ["status-groom", "task:exact-checks"], shadowCandidates: [candidate] };
const input = { routeClass: "status-groom", protected: false, pinModel: pin.model, pinThinking: pin.effort,
  registry: { getAvailable: () => [{ provider: "test", id: "sol" }, { provider: "test", id: "luna" }] },
  aliases: {}, config, assertModelAllowed() {}, parentSessionId: "main", evaluate: async () => answer as any };
const moduleFile = path.resolve("src/agents/model-route.ts");
const prepareFile = path.resolve("src/agents/model-route-prepare.ts");
let root: string;
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "route-reporter-exit-")); vi.stubEnv("PI_CODING_AGENT_DIR", path.join(root, "agent")); });
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); fs.rmSync(root, { recursive: true, force: true }); });
const fresh = (code: string) => {
  const probe = spawnSync("bun", ["-e", code], { encoding: "utf8", env: process.env, timeout: 30000 });
  expect(probe.status, probe.stderr).toBe(0);
  return JSON.parse(probe.stdout.trim());
};
const ownedRun = async () => {
  const decision = await prepareModelRoute(input);
  const runDirectory = path.join(root, decision.decisionId);
  prepareRouteDispatch(decision, undefined, runDirectory, decision.decisionId);
  return { decision, runDirectory, file: path.join(runDirectory, "route-quality-receipt.json") };
};
// This reporter really exits: the next process has no unsavedSafety/module state.
const exitReporter = (file: string, receiptFails: boolean) => fresh(`
  import fs from "node:fs";
  import {readRouteQualityReceipt,reportRouteQualityReceipt,isRouteClassReverted} from ${JSON.stringify(moduleFile)};
  const open=fs.openSync,close=fs.closeSync,write=fs.writeFileSync,fds=new Map();
  let faults=[];
  fs.openSync=(file,...args)=>{const fd=open(file,...args);if(typeof file==="string")fds.set(fd,file);return fd;};
  fs.closeSync=fd=>{fds.delete(fd);return close(fd);};
  fs.writeFileSync=(fd,...args)=>{
    const file=typeof fd==="number"?fds.get(fd):fd;
    if(typeof file==="string" && (/model-routing-(pending|refused)\\.jsonl$/.test(file) || (${receiptFails} && file.includes("route-quality-receipt.json")))) {
      faults.push(file);throw Object.assign(new Error("EFBIG"),{code:"EFBIG"});
    }
    return write(fd,...args);
  };
  let result;
  try{reportRouteQualityReceipt(readRouteQualityReceipt(${JSON.stringify(file)}),"fail");result={acknowledged:true};}
  catch(error){result={acknowledged:false,code:error.code,retryable:error.retryable,error:String(error)};}
  const pinned=isRouteClassReverted("status-groom");
  console.log(JSON.stringify({...result,pinned,faults}));
`);
const freshOwners = () => fresh(`
  import {prepareModelRoute} from ${JSON.stringify(prepareFile)};
  const results=[];
  for(const parentSessionId of ["fresh-main","fresh-resident"]){
    results.push(await prepareModelRoute({routeClass:"status-groom",protected:false,pinModel:"test/sol",pinThinking:"high",parentSessionId,
      ...(parentSessionId==="fresh-resident"?{actorId:"actor:fresh",activationId:"activation:fresh"}:{}),
      registry:{getAvailable:()=>[{provider:"test",id:"sol"},{provider:"test",id:"luna"}]},aliases:{},config:${JSON.stringify(config)},assertModelAllowed(){},evaluate:async()=>(${JSON.stringify(answer)})}));
  }
  console.log(JSON.stringify(results));
`);

describe("quality FAIL reporter-exit durability boundary", () => {
  it.each([false, true])("fresh owners recover a run receipt after all shared sinks EFBIG (archived=%s)", async archived => {
    const run = await ownedRun();
    let file = run.file;
    if (archived) {
      const store = new ActorLogStore({ maxEventBytes: 65536 }, { eventContextChars: 4000 }, { actorRunArchiveMs: 86400000 });
      const actor = { sessionFile: path.join(root, "actor/session.jsonl") };
      await store.retainRun(actor, run.decision.decisionId, run.runDirectory);
      file = path.join(root, "actor/runs", run.decision.decisionId, "route-quality-receipt.json");
      fs.rmSync(run.runDirectory, { recursive: true, force: true });
    }
    const report = exitReporter(file, false);
    expect(report).toMatchObject({ acknowledged: false, code: "quality-report-not-durable", retryable: true, pinned: true });
    expect(report.faults.filter((f: string) => f.endsWith("model-routing-refused.jsonl")).length).toBeGreaterThanOrEqual(2);
    expect(readRouteQualityReceipt(file)?.qualityFail).toBeDefined();
    expect(runTreeExitVeto(path.dirname(file))).toBe("route quality FAIL recovery is pending");
    // Capacity has recovered. These two new processes/owners must not read LIVE.
    for (const decision of freshOwners()) expect(decision).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
    const state = path.join(root, "agent/fabric/model-routing-state.jsonl");
    const rows = fs.readFileSync(state, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ decisionId: run.decision.decisionId, routeQuality: "fail" });
    expect(readRouteQualityReceipt(file)).toMatchObject({ routeQuality: "fail" });
    expect(readRouteQualityReceipt(file)?.qualityFail).toBeUndefined();
    for (const decision of freshOwners()) expect(decision.mode).toBe("shadow");
    expect(fs.readFileSync(state, "utf8").trim().split("\n")).toHaveLength(1);
    expect(await prepareModelRoute({ ...input, routeClass: "task:exact-checks" })).toMatchObject({ mode: "live" });
    expect(await prepareModelRoute({ ...input, config: { ...config, revertReset: { "status-groom": "owner-reset" } } })).toMatchObject({ mode: "live" });
  });

  it("total sink refusal surfaces retryable failure; after exit only the caller retry can recover the FAIL", async () => {
    const run = await ownedRun();
    expect(exitReporter(run.file, true)).toMatchObject({ acknowledged: false, code: "quality-report-not-durable", retryable: true, pinned: true });
    expect(readRouteQualityReceipt(run.file)?.qualityFail).toBeUndefined();
    // The explicit residual: zero accepted writes, reporter exit, no retry means
    // no durable FAIL exists. Admission append succeeds after capacity recovers.
    for (const decision of freshOwners()) expect(decision).toMatchObject({ mode: "live", reasonCode: "live-choice" });
    fresh(`import {readRouteQualityReceipt,reportRouteQualityReceipt} from ${JSON.stringify(moduleFile)};
      reportRouteQualityReceipt(readRouteQualityReceipt(${JSON.stringify(run.file)}),"fail");console.log(JSON.stringify({retried:true}));`);
    for (const decision of freshOwners()) expect(decision).toMatchObject({ ...pin, mode: "shadow", reasonCode: "class-reverted" });
  });

  it.each(["symlink", "oversized", "mismatched"])('rejects an unsafe indexed receipt (%s) before fresh LIVE admission', async fault => {
    const run = await ownedRun();
    if (fault === "symlink") {
      const target = path.join(root, "other.json"); fs.copyFileSync(run.file, target);
      fs.unlinkSync(run.file); fs.symlinkSync(target, run.file);
    } else if (fault === "oversized") {
      fs.writeFileSync(run.file, "x".repeat(64 * 1024 + 1), { mode: 0o600 });
    } else {
      const receipt = JSON.parse(fs.readFileSync(run.file, "utf8")); receipt.runId = "foreign-run";
      fs.writeFileSync(run.file, JSON.stringify(receipt), { mode: 0o600 });
    }
    expect(await prepareModelRoute(input)).toMatchObject({ ...pin, mode: "shadow", reasonCode: "revert-state-error" });
    expect(await prepareModelRoute({ ...input, routeClass: "task:exact-checks" })).toMatchObject({ mode: "live" });
  });

  it("PASS cannot erase an unresolved receipt FAIL and re-archival preserves it", async () => {
    const run = await ownedRun();
    exitReporter(run.file, false);
    const fail = readRouteQualityReceipt(run.file)?.qualityFail;
    const write = fs.writeFileSync;
    vi.spyOn(fs, "writeFileSync").mockImplementation((file, ...args) => {
      if (typeof file === "number") throw Object.assign(new Error("EFBIG"), { code: "EFBIG" });
      return write(file, ...args);
    });
    expect(() => reportRouteQualityReceipt(readRouteQualityReceipt(run.file)!, "pass")).toThrow("quality-report-not-durable");
    expect(readRouteQualityReceipt(run.file)?.qualityFail).toEqual(fail);
    vi.restoreAllMocks();
    const store = new ActorLogStore({ maxEventBytes: 65536 }, { eventContextChars: 4000 }, { actorRunArchiveMs: 86400000 });
    const actor = { sessionFile: path.join(root, "actor/session.jsonl") };
    await store.retainRun(actor, run.decision.decisionId, run.runDirectory);
    const archived = path.join(root, "actor/runs", run.decision.decisionId, "route-quality-receipt.json");
    const source = JSON.parse(fs.readFileSync(run.file, "utf8")); delete source.qualityFail;
    fs.writeFileSync(run.file, JSON.stringify(source), { mode: 0o600 });
    await store.retainRun(actor, run.decision.decisionId, run.runDirectory);
    expect(readRouteQualityReceipt(archived)?.qualityFail).toEqual(fail);
    fs.writeFileSync(path.join(path.dirname(archived), "status.json"), JSON.stringify({ status: "completed", finishedAt: 1, transport: "process", sessionId: "2147483647" }), { mode: 0o600 });
    expect(pruneActorRunArchives({ runsDirectory: path.dirname(path.dirname(archived)), retentionMs: 1, now: Date.now() })).toEqual([]);
    expect(fs.existsSync(archived)).toBe(true);
  });
});
