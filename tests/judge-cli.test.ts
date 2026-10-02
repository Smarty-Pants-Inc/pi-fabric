import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { VERDICTS } from "../src/judge/contract.js";

let root: string;
let server: http.Server | undefined;
beforeEach(() => { root = fs.mkdtempSync(path.join(os.tmpdir(), "judge-cli-test-")); });
afterEach(async () => { if (server) await new Promise<void>(resolve => server!.close(() => resolve())); server = undefined; fs.rmSync(root, { recursive: true, force: true }); });
const request = () => ({ questionClass: "item-stalled", itemRef: "org/repo#1", evidenceRefs: [{ url: "https://example.test/evidence", revision: "v1", observedAt: "2026-10-01T00:00:00Z" }], evidence: { facts: { fixture: true }, excerpts: [] }, allowedVerdicts: [...VERDICTS], timeboxMs: 5000, budget: { maxEvaluations: 1, maxAgents: 1, maxTokens: 1000 }, requestKey: "cli-proof" });
const call = (input: string, config?: string) => new Promise<{ code: number | null; stdout: string; stderr: string }>((resolve, reject) => {
  const env = { ...process.env }; delete env.FABRIC_JUDGE_CONFIG;
  if (config) env.FABRIC_JUDGE_CONFIG = config;
  const child = spawn(process.execPath, [path.resolve("bin/fabric-judge")], { cwd: root, env, stdio: ["pipe", "pipe", "pipe"] });
  let stdout = "", stderr = "";
  child.stdout.on("data", chunk => { stdout += chunk; }); child.stderr.on("data", chunk => { stderr += chunk; });
  child.on("error", reject); child.on("close", code => resolve({ code, stdout, stderr }));
  child.stdin.on("error", error => { if ((error as NodeJS.ErrnoException).code !== "EPIPE") reject(error); });
  child.stdin.end(input);
});
describe.skipIf(!fs.existsSync("dist/judge-cli.js"))("built fabric-judge stdin/stdout contract", () => {
  it.each(["", "not-json", "{} {}", JSON.stringify({ ...request(), model: "cheap" }), "x".repeat(32769)])("rejects a non-document or untrusted extra fields with one terminal envelope %#", async input => {
    const result = await call(input);
    expect(result.code).toBe(2); expect(result.stderr).toBe("");
    expect(result.stdout.trim().split("\n")).toHaveLength(1);
    expect(JSON.parse(result.stdout)).toMatchObject({ verdict: "unknown", status: "unknown", reasonCode: "invalid_input", confidenceProvenance: "unavailable" });
  });
  it("rejects absent trusted configuration rather than selecting binaries from input", async () => { const result = await call(JSON.stringify(request())); expect(result.code).toBe(2); expect(JSON.parse(result.stdout).reasonCode).toBe("invalid_config"); });
  it("records before a local typed Jev request, emits only JSON and does not start Pi", async () => {
    const ledger = path.join(root, "ledger/model-routing.jsonl");
    let calls = 0;
    server = http.createServer((req, res) => {
      let body = ""; req.on("data", chunk => { body += chunk; }); req.on("end", () => {
        calls++;
        const rows = fs.readFileSync(ledger, "utf8").trim().split("\n").map(line => JSON.parse(line));
        expect(rows).toHaveLength(1); expect(rows[0].type).toBe("decision");
        expect(Object.keys(JSON.parse(body).questions.verdict.criteria)).toEqual(VERDICTS);
        res.setHeader("Content-Type", "application/json"); res.end(JSON.stringify({ model: "fixture", answers: { verdict: { type: "choice", choice: "moving", confidence: .99, probabilities: { moving: .99, stalled: 0, dependency: 0, agent_decision: 0, human_decision: 0, unknown: .01 } } }, usage: { input_tokens: 3, output_tokens: 2 } }));
      });
    });
    await new Promise<void>(resolve => server!.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as { port: number }).port;
    const config = path.join(root, "config.json");
    fs.writeFileSync(config, JSON.stringify({ policy: { version: "test-v1", role: "Sol", pin: { model: "test/sol", effort: "max" } }, ledger, piBinary: "/never/start/this/binary", fixtureEndpoint: `http://127.0.0.1:${port}/jev` }));
    const result = await call(JSON.stringify(request()), config);
    expect(result.code).toBe(0); expect(result.stderr).toBe(""); expect(calls).toBe(1);
    expect(result.stdout.trim().split("\n")).toHaveLength(1);
    const envelope = JSON.parse(result.stdout);
    expect(envelope).toMatchObject({ verdict: "moving", status: "completed", reasonCode: "jev_accepted", confidenceProvenance: "jev-distribution", cost: { agents: 0, evaluations: 1 } });
    expect(envelope.decisionId).toMatch(/^[a-f0-9]{32}$/);
    const rows = fs.readFileSync(ledger, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(rows.filter(row => row.type === "decision")).toHaveLength(1); expect(rows.at(-1).decisionId).toBe(envelope.decisionId);
  });
});
