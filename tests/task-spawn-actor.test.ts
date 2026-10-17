import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { ResidentHost } from "../src/residency/host.js";
import { RESIDENT_HOST_FORMAT, residentRoot, type ResidentHostConfig } from "../src/residency/protocol.js";
import { parseFakeOpenAIRequest, probeFakeOpenAIServer } from "./fake-openai-server.js";

// Keep all run evidence in a retained temp tree. No rm/unlink cleanup: the
// scoped no-delete guard must never need to intercept this test's cleanup.
const roots: string[] = [];
const hosts: ResidentHost[] = [];
const servers: http.Server[] = [];
afterEach(async () => {
  await Promise.all(hosts.splice(0).map(host => host.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => {
    server.closeAllConnections(); server.close(() => resolve());
  })));
  vi.unstubAllEnvs();
});

const workerPath = path.resolve("dist/worker.js");
const fabricExtensionPath = path.resolve("dist/index.js");
const piBinary = path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const installedRuntime = fs.existsSync(workerPath) && fs.existsSync(fabricExtensionPath) && fs.existsSync(piBinary);

describe.skipIf(!installedRuntime)("real resident actor process task-spawn authority", () => {
  it("lets a durable actor activation run and await a real task agent", { timeout: 150_000 }, async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-task-spawn-actor-")); roots.push(root);
    const profile = path.join(root, "profile"); fs.mkdirSync(profile, { recursive: true });
    vi.stubEnv("PI_CODING_AGENT_DIR", profile); vi.stubEnv("PI_OFFLINE", "1"); vi.stubEnv("SMARTY_ROLE", "main");
    const requests: Array<{ model: string; messages: unknown[] }> = [];
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = parseFakeOpenAIRequest(body);
        if (!payload) { response.writeHead(400); response.end("Bad Request"); return; }
        requests.push(payload);
        const hasToolResult = payload.messages.some(message => (message as { role?: string }).role === "tool");
        const isSpawnTurn = payload.model === "main" && !hasToolResult;
        const delta = payload.model === "child"
          ? { role: "assistant", content: "real actor child completed" }
          : isSpawnTurn
            ? { role: "assistant", tool_calls: [{ index: 0, id: "actor-task-spawn", type: "function",
              function: { name: "fabric_exec", arguments: JSON.stringify({ code:
                'const child = await agents.spawn({task:"durable actor child", model:"proof/child", transport:"process", extensions:false, tools:[]}); const result = await agents.wait({id: child.id}); return JSON.stringify({status:result.status,output:result.output ?? result.text ?? result.result});' }) } }] }
            : { role: "assistant", content: "durable actor activation finished" };
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        for (const [part, finish] of [[delta, isSpawnTurn ? "tool_calls" : "stop"], [{}, isSpawnTurn ? "tool_calls" : "stop"]] as const) {
          response.write("data: " + JSON.stringify({ id: "offline-actor-spawn", object: "chat.completion.chunk", created: 1,
            model: payload.model, choices: [{ index: 0, delta: part, finish_reason: finish }] }) + "\n\n");
        }
        response.end("data: [DONE]\n\n");
      });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve)); servers.push(server);
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("loopback inference server did not bind");
    await probeFakeOpenAIServer(address.port);
    expect(requests).toHaveLength(0);
    fs.writeFileSync(path.join(profile, "models.json"), JSON.stringify({ providers: { proof: {
      baseUrl: `http://127.0.0.1:${address.port}/v1`, apiKey: "offline-test", api: "openai-completions",
      models: ["main", "child"].map(id => ({ id, name: id, reasoning: false, input: ["text"], contextWindow: 32000,
        maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
    } } }));
    fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ retry: { enabled: false }, compaction: { enabled: false } }));
    fs.writeFileSync(path.join(profile, "fabric.json"), JSON.stringify({ fullCodeMode: true,
      agents: { retainRuns: true, sessionExport: false, timeoutMs: 90_000 }, mesh: { enabled: false },
      mcp: { enabled: false }, memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false } }));

    const identity = { id: "session:task-spawn-actor", sessionId: "task-spawn-actor", kind: "main" as const, name: "Main" };
    const mesh = { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 };
    const config: ResidentHostConfig = {
      format: RESIDENT_HOST_FORMAT, rootId: identity.id, sessionId: identity.sessionId,
      cwd: root, projectRoot: root, meshRoot: path.join(root, "mesh"), actorRoot: path.join(root, "actors"),
      sessionActorRoot: path.join(root, "session-actors"), residencyRoot: residentRoot(path.join(root, "mesh"), identity.id),
      fullCodeMode: true, agents: { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 90_000, retainRuns: true, budgetUsd: 0 },
      mesh, retention: DEFAULT_FABRIC_CONFIG.retention, workerPath, fabricExtensionPath, piBinary,
      claudeBinary: "claude", vedaBinary: "veda",
      piModels: { available: [{ provider: "proof", id: "main" }, { provider: "proof", id: "child" }], aliases: {}, defaultModel: "proof/main" },
    };
    fs.mkdirSync(config.residencyRoot, { recursive: true });
    fs.writeFileSync(path.join(config.residencyRoot, "config.json"), JSON.stringify(config));
    const host = new ResidentHost(config); hosts.push(host); await host.start();
    const actor = await host.actors.create({ name: "task-spawn-proof", instructions: "Use fabric_exec to spawn and await exactly one task agent, then report its result.",
      residency: "durable", runner: "pi", transport: "process", extensions: true, tools: ["fabric_exec"],
      model: "proof/main", responseMode: "text" });
    expect(actor.residency).toBe("durable");
    expect(actor.responseMode).toBe("text");

    const activation = await host.actors.ask(actor.id, "Perform the task-agent proof now.");
    expect(activation.text).toContain("durable actor activation finished");
    expect(requests.map(row => row.model)).toEqual(["main", "child", "main"]);
    const transcript = JSON.stringify(requests);
    expect(transcript).toContain("real actor child completed");
    expect(transcript).toContain("durable actor child");
    const toolMessage = requests[2]!.messages.find(message => (message as { role?: string }).role === "tool") as { content?: string } | undefined;
    expect(JSON.parse(toolMessage?.content ?? "{}")).toMatchObject({ status: "completed", output: "real actor child completed" });
    const runRoot = path.join(config.residencyRoot, "runs");
    const runDirs = fs.existsSync(runRoot) ? fs.readdirSync(runRoot, { withFileTypes: true }).filter(entry => entry.isDirectory()) : [];
    expect(runDirs).toHaveLength(1);
    const childRun = JSON.parse(fs.readFileSync(path.join(runRoot, runDirs[0]!.name, "status.json"), "utf8"));
    expect(childRun).toMatchObject({ status: "completed" });
    await host.actors.close();
    fs.writeFileSync(path.join(root, "actor-result.json"), JSON.stringify({ verdict: "PASS", actorId: actor.id,
      activationRunId: activation.runId, childRunId: runDirs[0]!.name, modelRequests: requests.map(row => row.model) }, null, 2));
  });
});
