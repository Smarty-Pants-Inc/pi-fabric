import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { AgentRunResult } from "../src/agents/types.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];
const SCALE = 0.05;
const until = async (ready: () => boolean): Promise<void> => {
  const deadline = Date.now() + 30_000;
  while (!ready()) {
    if (Date.now() >= deadline) throw new Error("Native Pi event did not arrive");
    await new Promise(resolve => setTimeout(resolve, 20));
  }
};
const entries = (file: string): Array<Record<string, any>> => fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
const events = (result: { logFile?: string }) => entries(result.logFile!);
const explain = (result: AgentRunResult) => JSON.stringify(result) + "\n" + fs.readFileSync(result.logFile!, "utf8").slice(-16_000);
const evidence = (name: string, result: AgentRunResult) => {
  const directory = process.env.FABRIC_OVERLOAD_TEST_EVIDENCE_DIR;
  if (!directory) return;
  fs.mkdirSync(directory, { recursive: true });
  fs.writeFileSync(path.join(directory, name + "-result.json"), JSON.stringify(result, null, 2));
  const run = path.dirname(result.logFile!);
  for (const file of ["events.jsonl", "lifecycle.jsonl", "session.jsonl", "status.json"]) {
    if (fs.existsSync(path.join(run, file))) fs.copyFileSync(path.join(run, file), path.join(directory, name + "-" + file));
  }
};
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const setup = async (mode: "burst" | "forever" | "resume" | "400" | "429" | "disconnect", retry?: Record<string, unknown>) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-overload-")); roots.push(dir);
  let firstRequestAt = 0;
  const requests: Array<Record<string, any>> = [];
  const server = http.createServer((request, response) => {
    let body = "";
    request.on("data", chunk => { body += chunk; });
    request.on("end", () => {
      requests.push(JSON.parse(body)); firstRequestAt ||= Date.now();
      if (mode === "disconnect" && requests.length === 1) { response.destroy(); return; }
      const overloaded = mode === "forever" ||
        (mode === "burst" && Date.now() - firstRequestAt < 90_000 * SCALE) ||
        (mode === "resume" && requests.length === 1);
      if (overloaded || mode === "400" || (mode === "429" && requests.length === 1)) {
        response.writeHead(mode === "400" ? 400 : mode === "429" ? 429 : 503, { "Content-Type": "application/json" });
        response.end(JSON.stringify({ error: { message: mode === "400" ? "invalid_request_error: invalid input" : "server_is_overloaded", type: mode === "400" ? "invalid_request_error" : "server_is_overloaded" } }));
        return;
      }
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
        id: "offline", object: "chat.completion.chunk", created: 1, model: "offline",
        choices: [{ index: 0, delta, finish_reason }],
      })}\n\n`);
      chunk({ role: "assistant", content: "survived overload" }); chunk({}, "stop");
      response.end("data: [DONE]\n\n");
    });
  });
  servers.push(server);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as { port: number }).port;
  const agentDir = path.join(dir, "agent"); fs.mkdirSync(agentDir);
  fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: {
    "overload-test": { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [{
      id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    }] },
  } }));
  const settingsFile = path.join(agentDir, "settings.json");
  const settings = JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: false }, ...(retry ? { retry } : {}) });
  fs.writeFileSync(settingsFile, settings);
  vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
  vi.stubEnv("PI_OFFLINE", "1");
  vi.stubEnv("PI_FABRIC_TEST_RECOVERY_TIME_SCALE", String(SCALE));
  const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 120_000 }, {
    workerPath: path.resolve(process.env.FABRIC_OVERLOAD_TEST_WORKER ?? "src/worker.ts"),
    piBinary: path.resolve(process.env.FABRIC_OVERLOAD_TEST_PI_BINARY ?? "node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
    runRoot: path.join(dir, "runs"),
  });
  managers.push(manager);
  const spawn = () => manager.spawn({ task: "Finish ORIGINAL_TASK, preserving completed work.", model: "overload-test/offline", tools: [], extensions: false, transport: "process" });
  return { dir, manager, requests, settingsFile, settings, spawn };
};

describe("real Pi process provider recovery (offline localhost)", () => {
  it("survives a 90-second scaled overload burst in one native session, with no kill", async () => {
    const s = await setup("burst"); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("burst", result);
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    const log = events(result);
    expect(log.filter(event => event.type === "auto_retry_start").map(event => event.delayMs)).toEqual([250, 500, 1000, 2000, 4000]);
    expect(log.some(event => event.type === "fabric_recovery_error" || event.type === "fabric_provider_resume")).toBe(false);
    expect(result.runnerSessionIds).toHaveLength(1);
    const journal = entries(path.join(path.dirname(result.logFile!), "session.jsonl"));
    expect(journal.filter(entry => entry.type === "session")).toHaveLength(1);
    expect(journal[0]!.id).toBe(result.runnerSessionId);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 120_000);

  it("fails at the same ten-minute recovery bound across relaunches, retaining the session", async () => {
    const s = await setup("forever"); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("bound", result);
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(result.error).toContain("10-minute bound");
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(fs.existsSync(path.join(path.dirname(result.logFile!), "session.jsonl"))).toBe(true);
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting").length).toBeLessThanOrEqual(3);
    expect(events(result).filter(event => event.type === "auto_retry_start").some(event => event.delayMs === 8000)).toBe(true);
  }, 120_000);

  it("resumes an exhausted retryable Pi child on the exact same file and records the lifecycle", async () => {
    const s = await setup("resume", { maxRetries: 0 }); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("resume", result);
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    expect(s.requests).toHaveLength(2);
    expect(JSON.stringify(s.requests[1])).toContain("ORIGINAL_TASK");
    expect(JSON.stringify(s.requests[1])).toContain("Continue the task");
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(events(result).filter(event => event.type === "fabric_provider_resume")).toMatchObject([
      { attempt: 1, delayMs: 1500, phase: "scheduled" }, { attempt: 1, delayMs: 1500, phase: "starting" },
    ]);
    expect(entries(path.join(path.dirname(result.logFile!), "lifecycle.jsonl")).some(event => event.event === "run.resumed")).toBe(true);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 120_000);

  it.each(["429", "disconnect"] as const)("resumes a retryable %s with Pi's classification family", async mode => {
    const s = await setup(mode, { maxRetries: 0 }); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence(mode, result);
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "survived overload" });
    expect(s.requests).toHaveLength(2);
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting")).toHaveLength(1);
  }, 120_000);

  it("never exceeds three same-session resumes even when the user's Pi retries are disabled", async () => {
    const s = await setup("forever", { maxRetries: 0 }); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("resume-cap", result);
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(result.error).toContain("exhausted 3 same-session resumes");
    expect(result.runnerSessionIds).toHaveLength(1);
    expect(s.requests).toHaveLength(4);
    expect(events(result).filter(event => event.type === "fabric_provider_resume" && event.phase === "starting").map(event => event.delayMs)).toEqual([1500, 3000, 6000]);
  }, 120_000);

  it.each(["native retry", "transport resume"])("stops promptly during %s backoff, with no later resume", async backoff => {
    const s = await setup("forever", backoff === "transport resume" ? { maxRetries: 0 } : undefined);
    const handle = await s.spawn();
    const logFile = path.join(s.dir, "runs", handle.id, "events.jsonl");
    await until(() => entries(logFile).some(event => backoff === "native retry"
      ? event.type === "auto_retry_start" && event.delayMs >= 4000
      : event.type === "fabric_provider_resume" && event.phase === "scheduled"));
    const before = Date.now(); await s.manager.stop(handle.id);
    const result = await s.manager.wait(handle.id); evidence(backoff.replace(" ", "-"), result);
    expect(result, explain(result)).toMatchObject({ status: "stopped" });
    expect(Date.now() - before).toBeLessThan(2000);
    expect(events(result).some(event => event.type === "fabric_provider_resume" && event.phase === "starting")).toBe(false);
    expect(result.runnerSessionIds).toHaveLength(1);
  }, 120_000);

  it("never resumes a nonretryable 400 response", async () => {
    const s = await setup("400"); const handle = await s.spawn();
    const result = await s.manager.wait(handle.id); evidence("400", result);
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(s.requests).toHaveLength(1);
    expect(result.error).toContain("400");
    expect(events(result).some(event => event.type === "fabric_provider_resume" || event.type === "auto_retry_start")).toBe(false);
  }, 120_000);
});
