import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RpcClient } from "@earendil-works/pi-coding-agent";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { parseFakeOpenAIRequest, probeFakeOpenAIServer } from "./fake-openai-server.js";

// Real dist/worker.js -> installed Pi CLI -> compiled Fabric extension ->
// actual QuickJS fabric_exec -> actual agents provider. Inference is the only
// substituted component: a deterministic loopback OpenAI-compatible stream.
// Temp evidence is intentionally retained; these tests never rm/unlink paths.
const workerPath = path.resolve("dist/worker.js");
const piBinary = path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => {
    server.closeAllConnections(); server.close(() => resolve());
  })));
  vi.unstubAllEnvs();
});

const installedWorker = fs.existsSync(workerPath) && fs.existsSync(piBinary);
describe.skipIf(!installedWorker)("task-agent spawn authority in a real process worker", () => {
  it("allows a real native-Pi Main host to spawn a real process child", { timeout: 90_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-spawn-main-")); roots.push(root);
    const cwd = path.join(root, "main"); const agentDir = path.join(root, "agent");
    fs.mkdirSync(cwd); fs.mkdirSync(agentDir);
    const messages: Array<{ model: string; transcript: string }> = [];
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = parseFakeOpenAIRequest(body);
        if (!payload) { response.writeHead(400); response.end("Bad Request"); return; }
        const transcript = JSON.stringify(payload.messages); messages.push({ model: payload.model, transcript });
        const hasToolResult = payload.messages.some(message => (message as { role?: string }).role === "tool");
        const isToolCall = payload.model === "main" && !hasToolResult;
        // The Main's explicit unrestricted child keeps the shared private test
        // TMPDIR: Pi's settings lock is beside its profile, not a child temp root.
        // This isolates spawn authority from the independent write-policy test.
        const responseDelta = payload.model === "child"
          ? { role: "assistant", content: "real child completed" }
          : !isToolCall
            ? { role: "assistant", content: "real Main spawn completed" }
            : { role: "assistant", tool_calls: [{ index: 0, id: "main-task-spawn", type: "function",
              function: { name: "fabric_exec", arguments: JSON.stringify({ code:
                'const child = await agents.spawn({task:"bounded real child", model:"proof/child", transport:"process", extensions:false, tools:[], writePolicy:"unrestricted"}); const result = await agents.wait({id:child.id}); return result.status;' }) } }] };
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        for (const [part, finish] of [[responseDelta, isToolCall ? "tool_calls" : "stop"], [{}, isToolCall ? "tool_calls" : "stop"]] as const) {
          response.write("data: " + JSON.stringify({ id: "offline-main-spawn", object: "chat.completion.chunk", created: 1,
            model: payload.model, choices: [{ index: 0, delta: part, finish_reason: finish }] }) + "\n\n");
        }
        response.end("data: [DONE]\n\n");
      });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve)); servers.push(server);
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("loopback server did not bind TCP");
    await probeFakeOpenAIServer(address.port);
    expect(messages).toHaveLength(0);
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { proof: {
      baseUrl: `http://127.0.0.1:${address.port}/v1`, apiKey: "offline-test", api: "openai-completions",
      models: ["main", "child"].map(id => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 32000,
        maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { retainRuns: true, sessionExport: false, timeoutMs: 60_000 }, mesh: { enabled: false }, mcp: { enabled: false },
      memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false } }));
    for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
    vi.stubEnv("SMARTY_ROLE", "main");
    const client = new RpcClient({ cliPath: piBinary, cwd, provider: "proof", model: "main", env: {
      PI_CODING_AGENT_DIR: agentDir, PI_FABRIC_AGENT_DIR: path.join(root, "exports"),
      PI_FABRIC_RUN_ROOT: path.join(root, "runs"), PI_FABRIC_PI_BINARY: piBinary,
      PI_FABRIC_PROJECT_ROOT: cwd, PI_OFFLINE: "1", SMARTY_ROLE: "main",
    }, args: ["--no-skills", "--no-prompt-templates", "--no-themes", "--no-context-files", "--approve",
      "--thinking", "off", "--session-dir", path.join(root, "sessions"), "-e", path.resolve("dist/index.js")] });
    try {
      await client.start();
      await client.prompt("Use fabric_exec agents.spawn once; report completion.");
      await client.waitForIdle(60_000);
      const toolResults = messages.flatMap(row => (JSON.parse(row.transcript) as Array<{ role?: string }>).filter(message => message.role === "tool"));
      const records = fs.existsSync(path.join(root, "runs")) ? fs.readdirSync(path.join(root, "runs"), { withFileTypes: true })
        .filter(entry => entry.isDirectory()).map(entry => JSON.parse(fs.readFileSync(path.join(root, "runs", entry.name, "status.json"), "utf8"))) : [];
      expect(messages.map(row => row.model), JSON.stringify({ toolResults, records })).toEqual(["main", "child", "main"]);
      expect(await client.getLastAssistantText()).toBe("real Main spawn completed");
      const runs = fs.readdirSync(path.join(root, "runs"), { withFileTypes: true }).filter(entry => entry.isDirectory());
      expect(runs).toHaveLength(1);
      expect(JSON.parse(fs.readFileSync(path.join(root, "runs", runs[0]!.name, "status.json"), "utf8")))
        .toMatchObject({ status: "completed", task: "bounded real child" });
    } finally { await client.abort().catch(() => undefined); await client.stop(); }
  });

  it("executes agents.spawn through the real runtime, returns typed refusal, and creates no child run", { timeout: 90_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-spawn-worker-")); roots.push(root);
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { proof: {
      baseUrl: "http://127.0.0.1:1/v1", apiKey: "offline-test", api: "openai-completions",
      models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000,
        maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ defaultProjectTrust: "never",
      enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { retainRuns: true, sessionExport: false }, mesh: { enabled: false }, mcp: { enabled: false },
      memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false } }));

    const observed: unknown[] = [];
    let requestCount = 0;
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = parseFakeOpenAIRequest(body);
        if (!payload) { response.writeHead(400); response.end("Bad Request"); return; }
        observed.push(payload.messages);
        requestCount++;
        const delta = requestCount === 1
          ? { role: "assistant", tool_calls: [{ index: 0, id: "task-spawn-probe", type: "function",
            function: { name: "fabric_exec", arguments: JSON.stringify({ code:
              'try { const result = await agents.spawn({task:"must-not-launch", transport:"process"}); return JSON.stringify({admitted:true,id:result.id}); } catch (error) { return JSON.stringify({name:error.name,code:error.code,launchOutcome:error.launchOutcome,message:error.message}); }' }) } }] }
          : { role: "assistant", content: "task spawn authority probe finished" };
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        for (const [part, finish] of [[delta, null], [{}, requestCount === 1 ? "tool_calls" : "stop"]] as const) {
          response.write("data: " + JSON.stringify({ id: "offline-task-spawn", object: "chat.completion.chunk", created: 1,
            model: payload.model, choices: [{ index: 0, delta: part, finish_reason: finish }] }) + "\n\n");
        }
        response.end("data: [DONE]\n\n");
      });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve)); servers.push(server);
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("loopback server did not bind TCP");
    await probeFakeOpenAIServer(address.port);
    expect(requestCount).toBe(0);
    expect(observed).toHaveLength(0);
    const models = JSON.parse(fs.readFileSync(path.join(agentDir, "models.json"), "utf8"));
    models.providers.proof.baseUrl = `http://127.0.0.1:${address.port}/v1`;
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify(models));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir); vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("PI_FABRIC_RUN_ROOT", undefined); vi.stubEnv("SMARTY_ROLE", undefined);

    const runRoot = path.join(root, "runs");
    const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi", timeoutMs: 60_000,
      retainRuns: true, budgetUsd: 0, deniedModels: [], sessionExport: false }, {
      workerPath, fabricExtensionPath: path.resolve("dist/index.js"), piBinary, runRoot, fullCodeMode: true,
    }); managers.push(manager);
    const result = await manager.run({ task: "Call fabric_exec and attempt agents.spawn. Return the caught error as JSON.",
      model: "proof/offline", transport: "process", extensions: true, tools: ["fabric_exec"] });

    expect(result.status, result.error + "\n" + result.stderr).toBe("completed");
    expect(requestCount).toBe(2);
    const transcript = JSON.stringify(observed);
    expect(transcript).toContain("AgentSpawnForbiddenError");
    expect(transcript).toContain("FABRIC_AGENT_SPAWN_FORBIDDEN");
    expect(transcript).toContain("unlaunched");
    expect(transcript).not.toContain('"admitted":true');
    // The manager's parent run is the only top-level run record: refusal preceded
    // every child manager launch, rather than merely hiding its result.
    expect(fs.readdirSync(runRoot, { withFileTypes: true }).filter(entry => entry.isDirectory()).map(entry => entry.name))
      .toEqual([result.id]);
  });
});
