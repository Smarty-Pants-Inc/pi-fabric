import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ActorManager } from "../src/actors/manager.js";
import { GlobalActorRegistry } from "../src/actors/global-registry.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG, type FabricPythonRuntime } from "../src/config.js";
import { ActionRegistry } from "../src/core/action-registry.js";
import { FabricExecutionService } from "../src/execution-service.js";
import { LifecycleBroker } from "../src/lifecycle/broker.js";
import type { FabricMainAgentTarget } from "../src/main-agent.js";
import { MeshStore, type MeshIdentity } from "../src/mesh/store.js";
import { AgentsProvider } from "../src/providers/agents-provider.js";
import type { FabricParticipantSource } from "../src/topology/types.js";
import { parseFakeOpenAIRequest, probeFakeOpenAIServer } from "./fake-openai-server.js";

// Keep reason: public caller/configured-sandbox trust boundary and the recorded
// post-merge catch in PR #1873. Metadata-only persistence did not prove execution.
// Real public create -> ActorManager.ask -> AgentManager -> compiled worker ->
// installed Pi CLI -> compiled fabric_exec -> real Python backend. Only inference
// is substituted, using task-spawn-worker.test.ts's loopback OpenAI SSE pattern.
// Retain temporary evidence, including native actor/session traces.
const workerPath = path.resolve("dist/worker.js");
const extensionPath = path.resolve("dist/index.js");
const piBinary = path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
const cleanups: Array<() => Promise<unknown>> = [];
const unused = (): never => { throw new Error("Unexpected metadata collaborator call"); };

beforeEach(() => {
  // This fixture is an owning Main, not its test launcher's factory task child.
  for (const key of Object.keys(process.env)) if (key.startsWith("PI_FABRIC_")) vi.stubEnv(key, undefined);
  vi.stubEnv("SMARTY_ROLE", "main");
  vi.stubEnv("PI_OFFLINE", "1");
});
afterEach(async () => {
  try { for (const close of cleanups.splice(0).reverse()) await close(); }
  finally { vi.unstubAllEnvs(); }
});

type ToolEnd = {
  type: string;
  toolCallId?: string;
  toolName?: string;
  isError?: boolean;
  result?: { content?: Array<{ type: string; text?: string }>; details?: { success?: boolean; error?: string } };
};
const toolText = (event: ToolEnd): string => (event.result?.content ?? []).map(part => part.text ?? "").join("\n");

// Arithmetic proves both backends really executed Python. Importing subprocess
// without launching anything distinguishes native CPython's stdlib from Monty:
// a wrong backend cannot pass by echoing the actor's selector or final model text.
const programs = [
  'return {"answer": sum([19, 23]), "squares": [n * n for n in range(4)]}',
  'import subprocess\nreturn {"nativeModule": subprocess.__name__, "answer": 6 * 7}',
];
const calls = ["python-arithmetic", "python-native-stdlib"];

describe("created actor selected Python backend execution", () => {
  it.each(["cpython", "monty"] as const)("actually executes %s after creation, preserving the frozen backend across a policy change", { timeout: 90_000 }, async pythonRuntime => {
    expect(fs.existsSync(workerPath), "fresh build required: dist/worker.js").toBe(true);
    expect(fs.existsSync(extensionPath), "fresh build required: dist/index.js").toBe(true);
    expect(fs.existsSync(piBinary), "installed Pi CLI required").toBe(true);
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-actor-python-execution-")));
    const agentDir = path.join(root, "agent"); fs.mkdirSync(agentDir);
    const observedToolResults: unknown[][] = [];
    let requests = 0;
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = parseFakeOpenAIRequest(body);
        if (!payload) { response.writeHead(400); response.end("Bad Request"); return; }
        // Diagnostics need actual tool replies, never the full system prompt.
        observedToolResults.push(payload.messages.filter(message =>
          typeof message === "object" && message !== null && "role" in message && message.role === "tool"));
        const index = requests++;
        const code = programs[index];
        const delta = code === undefined
          ? { role: "assistant", content: "selected Python backend probe finished" }
          : { role: "assistant", tool_calls: [{ index: 0, id: calls[index], type: "function",
            function: { name: "fabric_exec", arguments: JSON.stringify({ code, resultFormat: "json" }) } }] };
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        for (const [part, finish] of [[delta, null], [{}, code === undefined ? "stop" : "tool_calls"]] as const) {
          response.write("data: " + JSON.stringify({ id: "offline-actor-python", object: "chat.completion.chunk", created: 1,
            model: payload.model, choices: [{ index: 0, delta: part, finish_reason: finish }] }) + "\n\n");
        }
        response.end("data: [DONE]\n\n");
      });
    });
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise<void>(resolve => { server.closeAllConnections(); server.close(() => resolve()); }));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("loopback server did not bind TCP");
    await probeFakeOpenAIServer(address.port);
    expect(requests).toBe(0);
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: { proof: {
      baseUrl: `http://127.0.0.1:${address.port}/v1`, apiKey: "offline-test", api: "openai-completions",
      models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000,
        maxTokens: 1024, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    } } }));
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({ defaultProjectTrust: "never",
      enableInstallTelemetry: false, compaction: { enabled: false }, retry: { enabled: false } }));
    fs.writeFileSync(path.join(agentDir, "fabric.json"), JSON.stringify({ fullCodeMode: false,
      // Deliberately not Python: the actor's worker launch must override this.
      executor: { kernel: "typescript", pythonRuntime: "cpython" },
      agents: { retainRuns: true, sessionExport: false }, mesh: { enabled: false }, mcp: { enabled: false },
      memory: { enabled: false }, jev: { enabled: false }, residency: { enabled: false },
      prewalk: { enabled: false, alwaysRearm: false } }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);

    let configuredRuntime: FabricPythonRuntime = "cpython";
    const agents = new AgentManager(root, { ...structuredClone(DEFAULT_FABRIC_CONFIG.agents), runner: "pi",
      timeoutMs: 60_000, retainRuns: true, sessionExport: false, budgetUsd: 0, deniedModels: [] }, {
      workerPath, fabricExtensionPath: extensionPath, piBinary, runRoot: path.join(root, "runs"),
      fullCodeMode: false, kernel: () => "typescript", pythonRuntime: () => configuredRuntime,
    });
    const mesh = new MeshStore(path.join(root, "mesh"), 64 * 1024, 100);
    const identity: MeshIdentity = { id: "session:actor-python-execution", name: "main", kind: "main", sessionId: "actor-python-execution" };
    const main: FabricMainAgentTarget = { id: identity.id, local: true, matches: () => false, info: unused, deliverAgent: unused };
    const participants: FabricParticipantSource = {
      list: () => [], get: () => undefined, self: unused, peers: () => [], async refresh() {}, scheduleRefresh() {},
    };
    const actors = new ActorManager("actor-python-execution", identity, mesh, {
      ...DEFAULT_FABRIC_CONFIG.mesh, enabled: true, actorPollMs: 20,
    }, agents, () => {}, {
      actorRoot: path.join(root, "actors"), persistent: true, mainAgent: main,
      canConsumeMesh: () => false, reapDeadSessionPresence: false,
    });
    const lifecycle = new LifecycleBroker(mesh, identity, participants, { enabled: false, pollMs: 20, maxReadEvents: 100 }, async () => {});
    const registry = new ActionRegistry();
    registry.register(new AgentsProvider(agents, actors, new GlobalActorRegistry(root, 64 * 1024), main, participants, undefined, lifecycle));
    cleanups.push(() => registry.close());
    const config = structuredClone(DEFAULT_FABRIC_CONFIG);
    config.fullCodeMode = false;
    config.executor.kernel = "typescript";
    config.executor.runtime = "quickjs";
    config.executor.pythonRuntime = "cpython";
    config.approvals.agent = "allow";
    const service = new FabricExecutionService(registry, config);
    const model = { provider: "proof", id: "offline" };
    const created = await service.execute({
      code: `return tools.call({ ref: "agents.create", args: ${JSON.stringify({
        name: `execute-${pythonRuntime}`, instructions: "Run the two supplied Python probes, then report completion.",
        runner: "pi", model: "proof/offline", thinking: "off", residency: "session", transport: "process",
        extensions: true, tools: ["fabric_exec"], kernel: "python", pythonRuntime,
      })} });`,
      signal: undefined, parentToolCallId: "actor-python-execution-create",
      context: { cwd: root, hasUI: false, modelRegistry: { getAvailable: () => [model], find: () => model } } as unknown as ExtensionContext,
      onPartial() {},
    });
    expect(created.success, created.error).toBe(true);
    const actor = actors.list().find(row => row.name === `execute-${pythonRuntime}`)!;
    expect(actor).toBeDefined();
    expect(agents.list()).toEqual([]); // Create is not activation.
    configuredRuntime = "monty";
    const reply = await actors.ask(actor.id, "Execute the arithmetic and native-stdlib probes now.");
    expect(reply.text).toBe("selected Python backend probe finished");
    const run = agents.status(reply.runId!);
    if (!("toolCalls" in run)) throw new Error(`Expected a native actor run record: ${JSON.stringify(run)}`);
    const diagnostics = JSON.stringify({ root, pythonRuntime, requests, observedToolResults,
      run: { id: run.id, status: run.status, toolCalls: run.toolCalls, error: run.error } });
    expect(run.status, diagnostics).toBe("completed");
    expect(requests, diagnostics).toBe(3);
    expect(run.toolCalls, diagnostics).toBe(2);
    expect(agents.list().map(row => row.id)).toEqual([reply.runId]);
    const events = fs.readFileSync(path.join(root, "runs", reply.runId!, "events.jsonl"), "utf8")
      .trim().split("\n").map(line => JSON.parse(line) as ToolEnd);
    // worker/tool-result-log.ts records raw completion metadata and separately
    // projects Pi's final guarded toolResult message to the same event type.
    // Only that canonical projection has result content; do not dedupe call IDs,
    // which would hide a missing or repeated actual result.
    const ends = events.filter(event => event.type === "tool_execution_end" && event.toolName === "fabric_exec"
      && event.result !== undefined);
    expect(ends.map(event => event.toolCallId), diagnostics).toEqual(calls);
    const arithmetic = ends[0]!;
    expect(arithmetic.isError, diagnostics).toBe(false);
    expect(JSON.parse(toolText(arithmetic))).toEqual({ answer: 42, squares: [0, 1, 4, 9] });
    const native = ends[1]!;
    if (pythonRuntime === "cpython") {
      expect(native.isError, diagnostics).toBe(false);
      expect(JSON.parse(toolText(native))).toEqual({ nativeModule: "subprocess", answer: 42 });
    } else {
      // A completed actor with successful arithmetic and this specific import
      // refusal is expected Monty behavior, not missing-runtime infrastructure.
      expect(native.isError, diagnostics).toBe(true);
      expect(toolText(native)).toMatch(/(?:ImportError|ModuleNotFoundError).*subprocess/s);
      // Recovery guidance can mention unavailable proxy attributes; only the
      // primary exception determines whether the runtime itself was missing.
      expect(toolText(native).split("\n\nRecovery hint:")[0])
        .not.toMatch(/unavailable|incompatible|Cannot find.*monty|optional native package/i);
    }
  });
});
