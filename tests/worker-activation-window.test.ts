import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { getCurrentSystemMessage, getCurrentSystemPrompt } from "@earendil-works/pi-ai";
import { estimateContextTokens } from "@earendil-works/pi-ai/utils/estimate";
import { SessionManager, buildSessionContext, convertToLlm, sessionEntryToContextMessages, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { ActivationWindow } from "../src/worker/activation-window.js";
import { AgentManager } from "../src/agents/manager.js";
import { ActorManager } from "../src/actors/manager.js";
import { MeshStore } from "../src/mesh/store.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: Array<{ close(): Promise<void> }> = [];
const servers: http.Server[] = [];
const root = () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-window-"));
  roots.push(dir);
  return dir;
};
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>(resolve => server.close(() => resolve()))));
  vi.unstubAllEnvs();
  // Windows releases an exited child's cwd a moment after its close event;
  // Node retries EBUSY/EPERM with backoff when maxRetries is set (smarty-dev#883).
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
// smarty-dev#883: these runs boot a worker and a child Node process. That
// boot is CPU-bound, so its wall time grows with runner contention (1.1 s of
// CPU took 16 s on a loaded Dev1). Every outcome below arrives as an event
// (a child frame, exit or the fake model's reply), so the run timeout is only
// a hang guard and must not decide a pass. Only the "timeout" scenario tests
// the timeout itself.
const HANG_GUARD_MS = 120_000;
const TEST_GUARD_MS = HANG_GUARD_MS + 30_000;
const user = (text: string) => ({ role: "user" as const, content: text, timestamp: 1 });
const assistant = (content: string, input = 10) => ({
  role: "assistant" as const, content: [{ type: "text" as const, text: content }],
  api: "openai-completions", provider: "window-test", model: "offline", stopReason: "stop" as const,
  timestamp: 2,
  usage: { input, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: input + 1,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
});

describe("activation projection", () => {
  it("keeps the full journal and every complete current tool exchange across model calls", () => {
    const old = [user("old activation"), assistant("old reply")];
    const current = user("current envelope: quiet/hold/task references");
    const window = new ActivationWindow(old);
    const call = { ...assistant(""), content: [{ type: "toolCall" as const, id: "read-1", name: "read", arguments: { path: "task" } }] };
    const result = { role: "toolResult" as const, toolCallId: "read-1", toolName: "read", content: [{ type: "text" as const, text: "task result" }], isError: false, timestamp: 3 };
    const journal = [...old, current];
    expect(window.project(journal)).toEqual([current]);
    const extended = [...journal, call, result];
    expect(window.project(extended)).toEqual([current, call, result]);
    expect(extended.slice(0, old.length)).toEqual(old);
    expect(() => window.project([...old, current, result])).toThrow(/lost current/);
    expect(() => window.project([current])).toThrow(/boundary/);
  });

  // smarty-dev#390: Pi journals system-prompt changes as role "system" entries that the
  // model context omits; a snapshot that kept them failed every activation after the first.
  it("ignores the system-prompt entries Pi journals but leaves out of the model context", () => {
    const system = (text: string) => ({ role: "system", content: "", sections: { preamble: text }, timestamp: 1 });
    const session = SessionManager.inMemory("/repo");
    session.appendMessage(system("first prompt") as never);
    session.appendMessage(user("old activation"));
    session.appendMessage(assistant("old reply"));
    session.appendMessage(system("changed prompt") as never);
    const journal = buildSessionContext(session.getBranch()).messages;
    expect(journal.map(message => message.role)).toContain("system");
    const window = new ActivationWindow(journal);
    const current = user("current envelope");
    const context = [...journal.filter(message => message.role !== "system"), current];
    expect(window.project(context)).toEqual([current]);
    expect(() => window.project([user("rewritten history"), assistant("old reply"), current])).toThrow(/boundary/);
  });

  // Astra P1 on pi-fabric#33: the conversation check cannot see system records, because
  // Pi strips them before the context hook and restores their replayed head after it.
  it("fails closed when an earlier system record is rewritten, and allows only appended ones", () => {
    // Each record patches its own section, as Pi's section diffs do, so a rewrite shows in the head.
    const system = (text: string, section = "preamble") => ({ role: "system", content: "", sections: { [section]: text }, timestamp: 1 }) as never;
    const journal = [system("first prompt"), user("old activation"), assistant("old reply"), system("changed rules", "rules")];
    const head = (records: unknown[]) => getCurrentSystemMessage(records as never) as never;
    const window = new ActivationWindow(journal);
    const current = user("current envelope");
    const appended = system("this activation's tools", "tools");
    const now = [...journal, appended, current];
    const running = [head([journal[0], journal[3], appended]), current];
    expect(() => window.verifySystem(now, running)).not.toThrow();
    // A rewritten old record in what reaches the model.
    const rewritten = [head([system("REWRITTEN"), journal[3], appended]), current];
    expect(() => window.verifySystem(now, rewritten)).toThrow(/system prompt/);
    // A rewritten old record in the journal.
    const forged = [system("REWRITTEN"), ...now.slice(1)];
    expect(() => new ActivationWindow(journal).verifySystem(forged, [head([forged[0], journal[3], appended]), current])).toThrow(/system records/);
    // Records appended in this activation may not disappear on a later model call.
    expect(() => window.verifySystem(journal, [head([journal[0], journal[3]]), current])).toThrow(/system records/);
  });

  it("projects a large prior activation without a token ceiling on the current activation", () => {
    const old = [user("x".repeat(900_000)), assistant("old", 240_000)];
    const current = user("y".repeat(100_000));
    expect(new ActivationWindow(old).project([...old, current])).toEqual([current]);
  });

  it.each(["context", "boundary", "setup", "manual", "threshold", "overflow", "registration"])("exits the disposable child before a request despite native-style swallowed exceptions: %s", mode => {
    const dir = root();
    const requestCounter = path.join(dir, "requests");
    const hook = path.resolve("src/worker/activation-window.ts");
    const child = path.join(dir, "probe.mjs");
    fs.writeFileSync(child, `
      import fs from 'node:fs';
      import hook from ${JSON.stringify(pathToFileURL(hook).href)};
      process.env.PI_FABRIC_ACTIVATION_WORKER_PID = String(process.ppid);
      process.env.PI_FABRIC_ACTIVATION_NONCE = 'test-only-nonce';
      process.env.PI_FABRIC_PARENT_RUN = 'test-run';
      process.env.PI_FABRIC_ACTIVATION_HOOK = ${JSON.stringify(fs.realpathSync(hook))};
      // Match native RPC's output guard, rather than allowing a logging write
      // to masquerade as the protocol readiness ACK.
      process.stdout.write = process.stderr.write.bind(process.stderr);
      const handlers = new Map();
      try { await hook({on(name, fn) { if (${JSON.stringify(mode)} === 'registration') throw new Error('registration failed'); handlers.set(name, fn); }}); }
      catch {} // The real loader also catches registration errors.
      try {
        const mode = ${JSON.stringify(mode)};
        if (mode === 'boundary' || mode === 'setup') {
          await handlers.get('session_start')({}, {mode: 'rpc', sessionManager: {getBranch() {
            if (mode === 'setup') throw new Error('snapshot failed');
            return [{type: 'message', id: 'old', parentId: null, timestamp: new Date().toISOString(), message: {role: 'user', content: 'old', timestamp: 1}}];
          }}});
        }
        await handlers.get(['context', 'boundary', 'setup'].includes(mode) ? 'context' : 'session_before_compact')?.({ messages: [], reason: mode });
      } catch {} // Pi catches hook exceptions; a throw alone would reach the request.
      fs.writeFileSync(${JSON.stringify(requestCounter)}, 'REQUEST');
    `);
    const result = spawnSync(process.execPath, [child], { encoding: "utf8", timeout: HANG_GUARD_MS });
    expect(result.status, result.stderr).toBe(78);
    expect(result.stderr).toContain("Fabric activation window failed");
    if (mode === "boundary") {
      expect(JSON.parse(result.stdout)).toMatchObject({
        type: "fabric_activation_window_ready", runId: "test-run",
        nonce: "test-only-nonce", policy: "activation", protocol: 1,
        hook: fs.realpathSync(hook),
      });
      expect(result.stderr).not.toContain("fabric_activation_window_ready");
    }
    expect(fs.existsSync(requestCounter)).toBe(false);
  }, TEST_GUARD_MS);

  it.each(["stream", "streamSimple"].flatMap(method =>
    ["expand", "shrink", "unsupported", "binary", "serialized-primitive", "snapshot", "no-callback"].map(mode => [method, mode]),
  ))("guards the final JSON boundary via %s (%s)", (method, mode) => {
    const dir = root();
    const requestFile = path.join(dir, "requests");
    const callbackFile = path.join(dir, "callback");
    const hook = fs.realpathSync(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER
      ? path.join(path.dirname(path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER)), "worker/activation-window.js")
      : path.resolve("src/worker/activation-window.ts"));
    const child = path.join(dir, "dispatch.mjs");
    fs.writeFileSync(child, `
      import fs from 'node:fs';
      import hook from ${JSON.stringify(pathToFileURL(hook).href)};
      process.env.PI_FABRIC_ACTIVATION_WORKER_PID = String(process.ppid);
      process.env.PI_FABRIC_ACTIVATION_NONCE = 'dispatch-nonce';
      process.env.PI_FABRIC_PARENT_RUN = 'dispatch-test';
      process.env.PI_FABRIC_ACTIVATION_HOOK = ${JSON.stringify(hook)};
      const handlers = new Map();
      await hook({on(name, fn) {handlers.set(name, fn)}});
      const model = {provider:'test', contextWindow:8000};
      const provider = {stream:dispatch, streamSimple:dispatch};
      async function dispatch(model, context, options) {
        if (this !== provider || options?.headers?.probe !== 'preserved') throw new Error('lost provider/options');
        let payload = {messages:context.messages};
        const replacement = await options?.onPayload?.(payload, model);
        if (replacement !== undefined) payload = replacement;
        fs.writeFileSync(${JSON.stringify(requestFile)}, JSON.stringify(payload));
      }
      const ctx = {mode:'rpc', model, sessionManager:{getBranch(){return []}},
        modelRegistry:{getAll(){return [model]}, getProvider(){return provider}}};
      await handlers.get('session_start')({}, ctx);
      await handlers.get('before_provider_headers')({}, ctx);
      const mode = ${JSON.stringify(mode)};
      const options = {headers:{probe:'preserved'}};
      if (mode !== 'no-callback') options.onPayload = async (payload, requestModel) => {
        if (requestModel !== model) throw new Error('lost callback model');
        fs.appendFileSync(${JSON.stringify(callbackFile)}, 'callback\\n');
        if (mode === 'unsupported') {payload.cycle = payload; return payload}
        if (mode === 'binary') return {...payload, context:new Uint8Array([1, 2, 3])};
        if (mode === 'serialized-primitive') return {toJSON(){return 'not a request object'}};
        if (mode === 'snapshot') {
          let reads = 0;
          return {get messages() {return ++reads === 1 ? [{role:'user', content:'SMALL'}]
            : [{role:'user', content:'x'.repeat(40_000)}]}};
        }
        return {messages:[{role:'user', content:mode === 'expand' ? 'x'.repeat(40_000) : 'SMALL'}]};
      };
      await provider[${JSON.stringify(method)}](model,
        {messages:[{role:'user', content:mode === 'shrink' ? 'x'.repeat(80_000) : 'SMALL'}]}, options);
    `);
    const result = spawnSync(process.execPath, [child], {encoding: "utf8", timeout: HANG_GUARD_MS});
    const refuses = ["expand", "unsupported", "binary", "serialized-primitive"].includes(mode);
    expect(result.status, result.stderr).toBe(refuses ? 78 : 0);
    if (refuses) {
      expect(fs.existsSync(requestFile)).toBe(false);
      expect(result.stderr).toContain(mode === "expand" ? "Context exceeds window" : "Fabric activation window failed");
    } else {
      expect(JSON.parse(fs.readFileSync(requestFile, "utf8"))).toEqual({messages:[{role:"user", content:"SMALL"}]});
    }
    if (mode === "no-callback") expect(fs.existsSync(callbackFile)).toBe(false);
    else expect(fs.readFileSync(callbackFile, "utf8")).toBe("callback\n");
  }, TEST_GUARD_MS);

  it("does not terminate an owner that accidentally loads the hook without worker binding", async () => {
    const { default: hook } = await import("../src/worker/activation-window.js");
    vi.stubEnv("PI_FABRIC_ACTIVATION_WORKER_PID", "");
    await expect(hook({ on: vi.fn() } as unknown as ExtensionAPI)).rejects.toThrow(/not in a disposable worker/);
  });
});

describe("actor context refusal (offline transport fixture)", () => {
  it("3238 stops full-history native retries on the first window failure and alarms once", async () => {
    const dir = root();
    const counter = path.join(dir, "attempts");
    const binary = path.join(dir, "retrying-pi.mjs");
    fs.writeFileSync(binary, `
      import fs from 'node:fs';
      const emit = event => process.stdout.write(JSON.stringify(event) + '\\n');
      let input = '';
      process.stdin.on('data', chunk => {
        input += chunk;
        while (input.includes('\\n')) {
          const end = input.indexOf('\\n');
          const frame = JSON.parse(input.slice(0, end)); input = input.slice(end + 1);
          if (frame.type !== 'prompt') continue;
          fs.appendFileSync(${JSON.stringify(counter)}, 'attempt\\n');
          const message = {role:'assistant', provider:'test', model:'test', content:[], stopReason:'error',
            errorMessage:'Context exceeds window: estimated 272511 input tokens, window 272000',
            usage:{input:0, output:0, cacheRead:0, cacheWrite:0, totalTokens:0}};
          emit({type:'agent_start'});
          emit({type:'message_end', message});
          emit({type:'agent_end', willRetry:true});
          setTimeout(() => {
            fs.appendFileSync(${JSON.stringify(counter)}, 'retry\\n');
            emit({type:'auto_retry_start', errorMessage:message.errorMessage});
            emit({type:'message_end', message:{...message, content:[{type:'text', text:'retried incorrectly'}], stopReason:'stop'}});
            emit({type:'auto_retry_end', success:true});
            emit({type:'agent_end'}); emit({type:'agent_settled', outcome:'completed'});
          }, 500);
        }
      });
      process.stdin.on('end', () => process.exit(0));
    `);
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: HANG_GUARD_MS }, {
      workerPath: path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER ?? "src/worker.ts"),
      piBinary: binary, runRoot: path.join(dir, "runs"),
    });
    const mesh = new MeshStore(path.join(dir, "mesh"), 64 * 1024, 100);
    const alarms: string[] = [];
    const actors = new ActorManager("retry-test", { id: "owner", name: "owner", kind: "main", sessionId: "retry-test" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, manager, ({ message }) => { alarms.push(message.text ?? ""); },
      { actorRoot: path.join(dir, "actors"), persistent: true });
    managers.push(actors, manager);
    const actor = await actors.create({ name: "retrying", instructions: "Act.", inferenceContext: "full-history", extensions: false, tools: [], transport: "process" });
    await expect(actors.ask(actor.id, "event")).rejects.toThrow(/Context exceeds window/);
    await actors.close();
    expect(fs.readFileSync(counter, "utf8")).toBe("attempt\n");
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toContain("Context exceeds window");
    expect(mesh.read({ topic: "ops.owner" }).filter(event => event.kind === "actor.alarm")).toHaveLength(1);
  }, TEST_GUARD_MS);
});

describe("activation worker admission (offline transport fixture)", () => {
  it.each(["missing-hook", "wrong-run", "wrong-nonce", "wrong-policy", "wrong-hook", "wrong-protocol", "ignored-flag", "still-enabled", "exit", "timeout"])("does not send a prompt or inference request on %s", async scenario => {
    const dir = root();
    const scenarioFile = path.join(dir, "scenario");
    fs.writeFileSync(scenarioFile, `activation:${scenario}`);
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenarioFile);
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: scenario === "timeout" ? 1500 : HANG_GUARD_MS }, {
      workerPath: path.resolve("src/worker.ts"), piBinary: path.resolve("tests/fixtures/fake-pi-model.mjs"), runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    const result = await manager.run({ task: "must not infer", actorId: "same-actor", sessionFile: path.join(dir, "actor.jsonl"), inferenceContext: "activation", extensions: false, tools: [], transport: "process" });
    expect(["failed", "timed_out"]).toContain(result.status);
    const logPath = path.join(dir, "runs", result.id, "events.jsonl");
    const log = fs.existsSync(logPath) ? fs.readFileSync(logPath, "utf8") : "";
    expect(log).not.toContain('"type":"prompt"');
    expect(log).not.toContain('"type":"message_start"');
    if (scenario !== "timeout") expect(result.error).toMatch(/activation window|before.*admission|model selection/);
    // A timeout can fire before a slow worker launches the child; then no
    // launch arguments are logged. Every other scenario must show them.
    if (scenario !== "timeout" || log) {
      expect(log).toContain("--no-extensions");
      expect(log).toContain("--no-tools");
      expect(log).toContain("--no-auto-compaction");
    }
    expect(log).not.toContain("set_auto_compaction");
  }, TEST_GUARD_MS);
});

// Required qualification target, not a live-model probe. CI must supply the
// exact native artifact with the ephemeral flag + explicit get_state marker.
const selectedNativeBinary = process.env.PI_FABRIC_ACTIVATION_TEST_PI_BINARY;
const nativeBinary = selectedNativeBinary ?? path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
describe("native activation window (offline; opted-in success needs exact native artifact)", () => {
  const readJournal = (journal: string) => {
    const bytes = fs.readFileSync(journal);
    const entries = bytes.toString("utf8").split("\n").filter(line => line.length > 0)
      .map(line => JSON.parse(line) as { type: string; customType?: string; data?: { scope: string; activationId: string; entry: { type: string } } });
    return { bytes, entries };
  };
  const expectJournalAppended = (journal: string, before: ReturnType<typeof readJournal>, allowCompaction = false) => {
    const after = readJournal(journal);
    expect(after.bytes.subarray(0, before.bytes.length)).toEqual(before.bytes);
    expect(after.entries.slice(0, before.entries.length)).toEqual(before.entries);
    if (!allowCompaction) expect(after.entries.some(entry => entry.type === "compaction")).toBe(false);
    return after;
  };
  // The run's event log shows how far the worker and child Pi got.
  const explain = (result: { logFile?: string }) => {
    const log = result.logFile && fs.existsSync(result.logFile) ? fs.readFileSync(result.logFile, "utf8") : "";
    return `${JSON.stringify(result)}\n${log.slice(-12_000)}`;
  };
  const setup = async (toolRounds = 0, oversizedRound = 0, toolTask?: string,
    api: "openai-completions" | "google-generative-ai" = "openai-completions", fabric = false) => {
    const dir = root();
    const requests: Array<Record<string, any>> = [];
    let requestCount = 0;
    const server = http.createServer((request, response) => {
      requestCount++; // Count HTTP arrival even if its body is interrupted.
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = JSON.parse(body);
        requests.push(payload);
        if (api === "google-generative-ai") {
          response.writeHead(200, { "Content-Type": "text/event-stream" });
          response.end(`data: ${JSON.stringify({ candidates: [{ index: 0, content: { role: "model", parts: [{ text: "useful current result" }] }, finishReason: "STOP" }], usageMetadata: { promptTokenCount: 10, candidatesTokenCount: 3, totalTokenCount: 13 } })}\n\n`);
          return;
        }
        const round = toolTask ? payload.messages.filter((m: {role: string}) => m.role === "tool").length + 1 : requests.length;
        const currentTask = JSON.stringify(payload.messages.findLast((m: {role: string}) => m.role === "user"));
        const useTool = (!toolTask || currentTask?.includes(toolTask)) && (toolRounds ? round <= toolRounds : payload.tools?.length && !payload.messages.some((m: {role: string}) => m.role === "tool"));
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
          id: "offline", object: "chat.completion.chunk", created: 1, model: "offline",
          choices: [{ index: 0, delta, finish_reason }],
        })}\n\n`);
        if (useTool) {
          chunk({ role: "assistant", tool_calls: [{ index: 0, id: toolRounds ? `read-${round}` : "read-current", type: "function", function: { name: "read", arguments: JSON.stringify({ path: path.join(dir, toolRounds ? `task-${round}.txt` : "task.txt") }) } }] });
          chunk({}, "tool_calls");
        } else {
          chunk({ role: "assistant", content: "useful current result" });
          chunk({}, "stop");
        }
        response.end("data: [DONE]\n\n");
      });
    });
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
    const port = (server.address() as {port: number}).port;
    const agentDir = path.join(dir, "agent");
    fs.mkdirSync(agentDir);
    // Fake local credentials only. No model or fleet credential is read.
    fs.writeFileSync(path.join(agentDir, "models.json"), JSON.stringify({ providers: {
      "window-test": { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: "offline-only", api, models: [{
        id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: fabric ? 128000 : 8000, maxTokens: 1024,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }] },
    } }));
    const settingsFile = path.join(agentDir, "settings.json");
    const settings = JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: true, reserveTokens: 1000 } });
    fs.writeFileSync(settingsFile, settings);
    fs.writeFileSync(path.join(dir, "task.txt"), "current tool result");
    for (let round = 1; round <= toolRounds; round++) {
      fs.writeFileSync(path.join(dir, `task-${round}.txt`), `ROUND_${round}_FULL_RESULT ` + "x".repeat(round === oversizedRound ? 45_000 : 15_000));
    }
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    const fabricExtensionPath = path.join(dir, "noop.ts");
    fs.writeFileSync(fabricExtensionPath, "export default function () {}\n");
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: HANG_GUARD_MS }, {
      fabricExtensionPath: fabric ? path.resolve(process.env.FABRIC_ACTIVATION_TEST_EXTENSION ?? "dist/index.js") : fabricExtensionPath,
      workerPath: path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER ?? "src/worker.ts"),
      piBinary: nativeBinary!, runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    return { dir, manager, requests, get requestCount() { return requestCount; }, settingsFile, settings };
  };

  it.skipIf(!selectedNativeBinary).each(["google-generative-ai", "google-vertex"].flatMap(api =>
    ["stream", "streamSimple"].flatMap(method =>
      ["success", "abort", "snapshot", "expand", "shrink", "non-json", "invalid-control"].map(mode => [api, method, mode])),
  ))("preserves native Google SDK payload and signal identity: %s %s %s", (api, method, mode) => {
    const dir = root();
    const resultFile = path.join(dir, "google-result.json");
    const sdkCallFile = path.join(dir, "google-sdk-called");
    const hook = fs.realpathSync(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER
      ? path.join(path.dirname(path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER)), "worker/activation-window.js")
      : path.resolve("src/worker/activation-window.ts"));
    const adapter = path.resolve(path.dirname(nativeBinary), "../../pi-ai/dist/api", `${api}.js`);
    const child = path.join(dir, "google-dispatch.mjs");
    fs.writeFileSync(child, `
      import assert from 'node:assert/strict';
      import fs from 'node:fs';
      import http from 'node:http';
      import path from 'node:path';
      import {findPackageJSON} from 'node:module';
      import {pathToFileURL} from 'node:url';
      import hook from ${JSON.stringify(pathToFileURL(hook).href)};
      import * as native from ${JSON.stringify(pathToFileURL(adapter).href)};
      const sdkPackage = findPackageJSON('@google/genai', ${JSON.stringify(pathToFileURL(adapter).href)});
      const {Models} = await import(pathToFileURL(path.join(path.dirname(sdkPackage), 'dist/node/index.mjs')).href);
      const controller = new AbortController();
      // If cancellation state is accidentally serialized, this non-JSON graph
      // fails admission. Its own serializer must never run, either.
      controller.signal.context = {cycle:controller.signal, big:1n};
      controller.signal.toJSON = () => {throw new Error('serialized SDK cancellation control')};
      let observed, sdkCalls = 0, requests = 0, wire, disconnected = false, guardFired = false;
      // The SDK installs this public method as an instance arrow function. Wrap
      // that assignment to inspect the EXACT adapter input before SDK transforms.
      Object.defineProperty(Models.prototype, 'generateContentStream', {configurable:true, set(fn) {
        Object.defineProperty(this, 'generateContentStream', {value:async params => {
          assert.equal(params, observed, 'replaced native SDK parameter object');
          assert.equal(params.config.abortSignal, controller.signal, 'lost native AbortSignal identity');
          sdkCalls++;
          fs.writeFileSync(${JSON.stringify(sdkCallFile)}, 'called');
          return fn(params);
        }, configurable:true, writable:true});
      }});
      let closed;
      const connectionClosed = new Promise(resolve => {closed = resolve});
      const server = http.createServer((request, response) => {
        requests++;
        let body = '';
        request.on('data', chunk => {body += chunk});
        request.on('end', () => {
          wire = JSON.parse(body);
          response.once('close', () => {disconnected = true; closed()});
          if (${JSON.stringify(mode)} === 'abort') {controller.abort(); return}
          response.writeHead(200, {'Content-Type':'text/event-stream'});
          response.end('data: ' + JSON.stringify({candidates:[{index:0, content:{role:'model', parts:[{text:'GOOGLE_OK'}]}, finishReason:'STOP'}],
            usageMetadata:{promptTokenCount:10, candidatesTokenCount:3, totalTokenCount:13}}) + '\\n\\n');
        });
      });
      await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
      const timeout = setTimeout(() => {guardFired = true; controller.abort(); server.closeAllConnections()}, 15_000);
      try {
        process.env.PI_FABRIC_ACTIVATION_WORKER_PID = String(process.ppid);
        process.env.PI_FABRIC_ACTIVATION_NONCE = 'google-nonce';
        process.env.PI_FABRIC_PARENT_RUN = 'google-test';
        process.env.PI_FABRIC_ACTIVATION_HOOK = ${JSON.stringify(hook)};
        const handlers = new Map();
        await hook({on(name, fn) {handlers.set(name, fn)}});
        const model = {id:'offline', provider:'google-test', api:${JSON.stringify(api)}, baseUrl:'http://127.0.0.1:' + server.address().port + '/v1',
          contextWindow:8000, maxTokens:1024, reasoning:false, input:['text'], cost:{input:0, output:0, cacheRead:0, cacheWrite:0}};
        const provider = {stream:native.stream, streamSimple:native.streamSimple};
        const ctx = {mode:'rpc', model, sessionManager:{getBranch(){return []}},
          modelRegistry:{getAll(){return [model]}, getProvider(){return provider}}};
        await handlers.get('session_start')({}, ctx);
        await handlers.get('before_provider_headers')({}, ctx);
        const result = await provider[${JSON.stringify(method)}](model, {messages:[{role:'user', content:${JSON.stringify(mode === "shrink" ? "x".repeat(80_000) : "SMALL_GOOGLE_INPUT")}, timestamp:1}]},
          {apiKey:'offline-only', signal:controller.signal, maxRetries:0, onPayload:async payload => {
            observed = payload;
            assert.equal(payload.config.abortSignal, controller.signal);
            if (${JSON.stringify(mode)} === 'expand') payload.contents.push({role:'user', parts:[{text:'x'.repeat(40_000)}]});
            if (${JSON.stringify(mode)} === 'shrink') payload.contents = [{role:'user', parts:[{text:'SMALL_GOOGLE_INPUT'}]}];
            // An identically named value anywhere else is context, not a control.
            if (${JSON.stringify(mode)} === 'non-json') payload.config.extra = {abortSignal:controller.signal};
            if (${JSON.stringify(mode)} === 'invalid-control') payload.config.abortSignal = {aborted:false};
            if (${JSON.stringify(mode)} === 'snapshot') {
              let reads = 0;
              Object.defineProperty(payload, 'contents', {enumerable:true, configurable:true, get() {
                return [{role:'user', parts:[{text:++reads === 1 ? 'SMALL_GOOGLE_INPUT' : 'x'.repeat(40_000)}]}];
              }});
            }
          }}).result();
        assert.equal(result.stopReason, ${JSON.stringify(mode === "abort" ? "aborted" : "stop")}, result.errorMessage);
        assert.equal(sdkCalls, 1);
        assert.equal(requests, 1);
        assert.equal(JSON.stringify(wire).includes('abortSignal'), false);
        assert.equal(JSON.stringify(wire).includes('SMALL_GOOGLE_INPUT'), true);
        if (${JSON.stringify(mode)} === 'abort') await connectionClosed;
        else assert.equal(result.content[0].text, 'GOOGLE_OK');
        assert.equal(guardFired, false, 'request/disconnect ended only because the hang guard fired');
        fs.writeFileSync(${JSON.stringify(resultFile)}, JSON.stringify({sdkCalls, requests, samePayload:true, sameSignal:true,
          stopReason:result.stopReason, disconnected, wireKeys:Object.keys(wire)}));
      } finally {
        clearTimeout(timeout);
        server.closeAllConnections();
        await new Promise(resolve => server.close(resolve));
      }
    `);
    const result = spawnSync(process.execPath, [child], {encoding: "utf8", timeout: HANG_GUARD_MS});
    expect(result.error).toBeUndefined();
    const refuses = ["expand", "non-json", "invalid-control"].includes(mode);
    expect(result.status, result.stderr).toBe(refuses ? 78 : 0);
    if (refuses) {
      expect(fs.existsSync(sdkCallFile)).toBe(false);
      expect(fs.existsSync(resultFile)).toBe(false);
      expect(result.stderr).toContain(mode === "expand" ? "Context exceeds window" : "Fabric activation window failed");
      return;
    }
    expect(JSON.parse(fs.readFileSync(resultFile, "utf8"))).toMatchObject({sdkCalls: 1, requests: 1, samePayload: true, sameSignal: true,
      stopReason: mode === "abort" ? "aborted" : "stop", ...(mode === "abort" ? {disconnected: true} : {})});
  }, TEST_GUARD_MS);

  // Astra round 4: the native Google callback receives SDK parameters with a
  // real config.abortSignal, while the wire body contains only model data.
  it.skipIf(!selectedNativeBinary)("3238 admits a native Google activation with its normal cancellation control", async () => {
    const s = await setup(0, 0, undefined, "google-generative-ai");
    const result = await s.manager.run({ task: "CURRENT_GOOGLE_ACTIVATION", model: "window-test/offline", actorId: "google-actor",
      sessionFile: path.join(s.dir, "actor.jsonl"), inferenceContext: "activation", tools: [], extensions: false, transport: "process" });
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "useful current result" });
    expect(s.requestCount).toBe(1);
    expect(s.requests).toHaveLength(1);
    expect(JSON.stringify(s.requests)).toContain("CURRENT_GOOGLE_ACTIVATION");
    expect(JSON.stringify(s.requests)).not.toContain("abortSignal");
  }, TEST_GUARD_MS);

  it.skipIf(Boolean(selectedNativeBinary))("rejects an old native CLI that ignores the flag even when global compaction is already false", async () => {
    const s = await setup();
    fs.writeFileSync(s.settingsFile, JSON.stringify({ compaction: { enabled: false }, enableInstallTelemetry: false }));
    const result = await s.manager.run({ task: "must not infer", model: "window-test/offline", actorId: "same-actor", sessionFile: path.join(s.dir, "actor.jsonl"), inferenceContext: "activation", tools: [], extensions: false, transport: "process" });
    expect(result, explain(result)).toMatchObject({ status: "failed" });
    expect(result.error).toContain("--no-auto-compaction");
    expect(s.requests).toHaveLength(0);
  }, TEST_GUARD_MS);

  it("keeps the omitted full-history default on the actual native request path", async () => {
    const s = await setup();
    const journal = path.join(s.dir, "actor.jsonl");
    const session = SessionManager.open(journal);
    session.appendMessage(user("OLD_DEFAULT_HISTORY"));
    session.appendMessage(assistant("old reply"));
    const result = await s.manager.run({ task: "CURRENT_DEFAULT", model: "window-test/offline", actorId: "same-actor", sessionFile: journal, tools: [], extensions: false, transport: "process" });
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "useful current result" });
    expect(s.requests).toHaveLength(1);
    expect(JSON.stringify(s.requests)).toContain("OLD_DEFAULT_HISTORY");
    expect(JSON.stringify(s.requests)).toContain("CURRENT_DEFAULT");
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary).each(["threshold", "overflow"])("retains LARGE old %s journal yet runs useful current inference, extensions:false/tools:[]", async mode => {
    const s = await setup();
    const journal = path.join(s.dir, "actor.jsonl");
    const session = SessionManager.open(journal);
    session.appendMessage(user("OLD_PRIVATE_ACTIVATION " + "x".repeat(160_000)));
    session.appendMessage(mode === "overflow"
      ? { ...assistant("", 90_000), stopReason: "error", errorMessage: "maximum context length exceeded" }
      : assistant("old reply", 90_000));
    const before = readJournal(journal);
    const result = await s.manager.run({ task: "CURRENT_ACTIVATION", model: "window-test/offline", actorId: "same-actor", sessionFile: journal, inferenceContext: "activation", tools: [], extensions: false, transport: "process" });
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "useful current result" });
    expect(s.requests).toHaveLength(1);
    expect(JSON.stringify(s.requests)).not.toContain("OLD_PRIVATE_ACTIVATION");
    expect(JSON.stringify(s.requests)).toContain("CURRENT_ACTIVATION");
    expect(s.requests[0]!.tools ?? []).toHaveLength(0);
    const after = expectJournalAppended(journal, before);
    expect(after.bytes.toString("utf8")).toContain("CURRENT_ACTIVATION");
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, TEST_GUARD_MS);

  // smarty-dev#3238: context projection is too late for a before_agent_start
  // window guard, which still sees the native carried session.
  it.skipIf(!selectedNativeBinary)("3238 oversized tool history cannot block activation preflight", async () => {
    const s = await setup();
    const extensionDir = path.join(s.dir, "agent", "extensions");
    fs.mkdirSync(extensionDir);
    fs.writeFileSync(path.join(extensionDir, "window-preflight.ts"), `
      import { buildSessionContext, convertToLlm, getPackageDir } from '@earendil-works/pi-coding-agent';
      import fs from 'node:fs';
      import path from 'node:path';
      import { pathToFileURL } from 'node:url';
      import * as nodeModule from 'node:module';
      export default async function(pi) {
        let estimatorUrl = '@earendil-works/pi-ai/utils/estimate';
        if (typeof nodeModule.findPackageJSON === 'function') {
          const aiPackage = nodeModule.findPackageJSON('@earendil-works/pi-ai', pathToFileURL(path.join(getPackageDir(), 'package.json')));
          estimatorUrl = pathToFileURL(path.join(path.dirname(aiPackage), 'dist', 'utils', 'estimate.js')).href;
        }
        const { estimateContextTokens } = await import(estimatorUrl);
        pi.on('before_agent_start', (event, ctx) => {
          const carried = convertToLlm(buildSessionContext(ctx.sessionManager.getBranch()).messages);
          const tokens = estimateContextTokens([...carried, {role:'user', content:event.prompt, timestamp:Date.now()}]).tokens;
          fs.appendFileSync(${JSON.stringify(path.join(s.dir, "preflight-tokens"))}, String(tokens) + '\\n');
          if (tokens > ctx.model.contextWindow) {
            fs.writeSync(2, 'Context exceeds window: estimated ' + tokens + ' input tokens, window ' + ctx.model.contextWindow + '\\n');
            process.exit(78);
          }
        });
      }
    `);
    const journal = path.join(s.dir, "actor.jsonl");
    const session = SessionManager.open(journal);
    session.appendMessage(user("OLD_PRIVATE_ACTIVATION"));
    session.appendMessage({ ...assistant(""), content: [{ type: "toolCall", id: "old-read", name: "read", arguments: { path: "huge" } }] });
    session.appendMessage({ role: "toolResult", toolCallId: "old-read", toolName: "read", content: [{ type: "text", text: "HUGE_PRIVATE_TOOL_RESULT " + "x".repeat(1_100_000) }], isError: false, timestamp: 3 });
    const before = readJournal(journal);
    const result = await s.manager.run({ task: "CURRENT_ACTIVATION", systemPrompt: "CURRENT_INSTRUCTIONS", model: "window-test/offline", actorId: "same-actor", sessionFile: journal, inferenceContext: "activation", tools: [], extensions: true, transport: "process" });
    expect(result, explain(result)).toMatchObject({ status: "completed", text: "useful current result" });
    expect(s.requests).toHaveLength(1);
    expect(JSON.stringify(s.requests)).not.toContain("HUGE_PRIVATE_TOOL_RESULT");
    expect(JSON.stringify(s.requests)).not.toContain("OLD_PRIVATE_ACTIVATION");
    expect(JSON.stringify(s.requests)).toContain("CURRENT_ACTIVATION");
    expect(JSON.stringify(s.requests)).toContain("CURRENT_INSTRUCTIONS");
    expect(Number(fs.readFileSync(path.join(s.dir, "preflight-tokens"), "utf8").trim())).toBeLessThan(8000);
    expectJournalAppended(journal, before);
    const reloaded = buildSessionContext(SessionManager.open(journal).getBranch()).messages;
    expect(JSON.stringify(reloaded)).toContain("HUGE_PRIVATE_TOOL_RESULT");
    expect(JSON.stringify(reloaded)).toContain("CURRENT_ACTIVATION");
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary)("3238 an unfittable single activation alarms once without retries", async () => {
    const s = await setup();
    const alarms: Array<{ delivery: string; triggerTurn: boolean; message: { text?: string } }> = [];
    const mesh = new MeshStore(path.join(s.dir, "mesh"), 2 * 1024 * 1024, 100);
    const actors = new ActorManager("window-test", { id: "owner", name: "owner", kind: "main", sessionId: "window-test" }, mesh,
      { ...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20 }, s.manager, request => { alarms.push(request); },
      { actorRoot: path.join(s.dir, "actors"), persistent: true });
    managers.push(actors);
    const actor = await actors.create({ name: "oversized", instructions: "Act on this event.", residency: "durable", inferenceContext: "activation", model: "window-test/offline", tools: [], extensions: false, transport: "process", delivery: "mailbox" });
    const run = vi.spyOn(s.manager, "run");
    await expect(actors.ask(actor.id, "x".repeat(80_000))).rejects.toThrow(/Context exceeds window/);
    await actors.close(); // settles the drain; no timing sleep can hide a retry
    expect(run).toHaveBeenCalledTimes(1);
    expect(s.requests).toHaveLength(0);
    expect(alarms).toHaveLength(1);
    expect(alarms[0]).toMatchObject({ delivery: "followUp", triggerTurn: true });
    expect(alarms[0]!.message.text).toContain("Context exceeds window");
    expect(mesh.read({ topic: "ops.owner" }).filter(event => event.kind === "actor.alarm")).toEqual([
      expect.objectContaining({ data: expect.objectContaining({ actorId: actor.id, reason: "context_window" }) }),
    ]);
    expect(actors.messages(actor.id, 20).filter(message => message.error?.includes("Context exceeds window"))).toHaveLength(1);
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary).each(["expand", "shrink"])("3238 admits only the final context after a later context_with_system %s", async mode => {
    const s = await setup();
    const extensionDir = path.join(s.dir, "agent", "extensions");
    fs.mkdirSync(extensionDir);
    fs.writeFileSync(path.join(extensionDir, "late-transform.ts"), `
      import fs from 'node:fs';
      export default function(pi) {
        pi.on('context_with_system', event => {
          fs.appendFileSync(${JSON.stringify(path.join(s.dir, "transforms"))}, 'transform\\n');
          return {messages: ${mode === "expand"
            ? "[...event.messages, {role:'user', content:'LATE_CONTEXT ' + 'x'.repeat(40_000), timestamp:Date.now()}]"
            : "event.messages.map(message => message.role === 'user' ? {...message, content:'SMALL_TRANSFORMED_CONTEXT'} : message)"}};
        });
      }
    `);
    const alarms: Array<{message: {text?: string}}> = [];
    const mesh = new MeshStore(path.join(s.dir, "mesh"), 2 * 1024 * 1024, 100);
    const actors = new ActorManager("late-window-test", {id: "owner", name: "owner", kind: "main", sessionId: "late-window-test"}, mesh,
      {...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20}, s.manager, request => { alarms.push(request); },
      {actorRoot: path.join(s.dir, "actors"), persistent: true});
    managers.push(actors);
    const actor = await actors.create({name: "late-transform", instructions: "Act.", inferenceContext: "activation",
      model: "window-test/offline", tools: [], extensions: true, transport: "process", delivery: "mailbox"});
    const run = vi.spyOn(s.manager, "run");
    const outcome = actors.ask(actor.id, mode === "expand" ? "SMALL_RAW_CONTEXT" : "LARGE_RAW_CONTEXT " + "x".repeat(80_000));
    if (mode === "expand") {
      await expect(outcome).rejects.toThrow(/Context exceeds window/);
      await actors.close();
      expect(s.requests).toHaveLength(0);
      expect(alarms).toHaveLength(1);
      expect(alarms[0]!.message.text).toContain("Context exceeds window");
      expect(mesh.read({topic: "ops.owner"}).filter(event => event.kind === "actor.alarm")).toHaveLength(1);
    } else {
      await expect(outcome).resolves.toMatchObject({text: "useful current result"});
      await actors.close();
      expect(s.requests).toHaveLength(1);
      expect(JSON.stringify(s.requests)).toContain("SMALL_TRANSFORMED_CONTEXT");
      expect(JSON.stringify(s.requests)).not.toContain("LARGE_RAW_CONTEXT");
      expect(alarms).toHaveLength(0);
    }
    expect(run).toHaveBeenCalledTimes(1);
    expect(fs.readFileSync(path.join(s.dir, "transforms"), "utf8")).toBe("transform\n");
  }, TEST_GUARD_MS);

  // Astra round 3: native before_provider_request runs AFTER dispatch has
  // converted the transcript. Exercise its real awaited handler chain and count
  // actual requests at the localhost server, not calls to streamSimple.
  it.skipIf(!selectedNativeBinary).each([
    "expand-replace", "expand-in-place", "expand-tools", "shrink", "expand-shrink", "shrink-expand",
  ])("3238 admits only the final payload after before_provider_request %s", async mode => {
    const s = await setup();
    const extensionDir = path.join(s.dir, "agent", "extensions");
    fs.mkdirSync(extensionDir);
    const transforms = path.join(s.dir, "payload-transforms");
    fs.writeFileSync(path.join(extensionDir, "late-payload.ts"), `
      import fs from 'node:fs';
      export default function(pi) {
        const shrink = payload => ({...payload, messages: payload.messages.map(message =>
          message.role === 'user' ? {...message, content:'SMALL_FINAL_PAYLOAD'} : message)});
        const expand = payload => ({...payload, messages: [...payload.messages,
          {role:'user', content:'LATE_PAYLOAD ' + 'x'.repeat(40_000)}]});
        pi.on('before_provider_request', async event => {
          await Promise.resolve();
          fs.appendFileSync(${JSON.stringify(transforms)}, 'first\\n');
          const mode = ${JSON.stringify(mode)};
          if (mode === 'expand-in-place') {
            event.payload.messages.push({role:'user', content:'LATE_PAYLOAD ' + 'x'.repeat(40_000)});
            return; // Undefined means retain the mutated input.
          }
          if (mode === 'expand-tools') return {...event.payload, tools:[{type:'function',
            function:{name:'oversized', description:'LATE_TOOL ' + 'x'.repeat(40_000),
              parameters:{type:'object', properties:{}}}}]};
          return mode.startsWith('expand') ? expand(event.payload) : shrink(event.payload);
        });
        pi.on('before_provider_request', async event => {
          await Promise.resolve();
          fs.appendFileSync(${JSON.stringify(transforms)}, 'last\\n');
          if (${JSON.stringify(mode)} === 'expand-shrink') return shrink(event.payload);
          if (${JSON.stringify(mode)} === 'shrink-expand') return expand(event.payload);
        });
      }
    `);
    const alarms: Array<{message: {text?: string}}> = [];
    const mesh = new MeshStore(path.join(s.dir, "mesh"), 2 * 1024 * 1024, 100);
    const actors = new ActorManager("payload-window-test", {id: "owner", name: "owner", kind: "main", sessionId: "payload-window-test"}, mesh,
      {...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20}, s.manager, request => { alarms.push(request); },
      {actorRoot: path.join(s.dir, "actors"), persistent: true});
    managers.push(actors);
    const actor = await actors.create({name: "late-payload", instructions: "Act.", inferenceContext: "activation",
      model: "window-test/offline", tools: [], extensions: true, transport: "process", delivery: "mailbox"});
    const run = vi.spyOn(s.manager, "run");
    const reduces = mode === "shrink" || mode === "expand-shrink";
    const outcome = actors.ask(actor.id, reduces ? "LARGE_RAW_PAYLOAD " + "x".repeat(80_000) : "SMALL_RAW_PAYLOAD");
    if (reduces) await expect(outcome).resolves.toMatchObject({text: "useful current result"});
    else await expect(outcome).rejects.toThrow(/Context exceeds window/);
    await actors.close(); // Join the drain: exactly one activation, no hidden retry.
    expect(run).toHaveBeenCalledTimes(1);
    expect(await run.mock.results[0]!.value).toMatchObject({status: reduces ? "completed" : "failed"});
    expect(s.requestCount).toBe(reduces ? 1 : 0);
    expect(s.requests).toHaveLength(reduces ? 1 : 0);
    expect(alarms).toHaveLength(reduces ? 0 : 1);
    expect(mesh.read({topic: "ops.owner"}).filter(event => event.kind === "actor.alarm")).toHaveLength(reduces ? 0 : 1);
    if (reduces) {
      expect(JSON.stringify(s.requests)).toContain("SMALL_FINAL_PAYLOAD");
      expect(JSON.stringify(s.requests)).not.toContain("LARGE_RAW_PAYLOAD");
      expect(JSON.stringify(s.requests)).not.toContain("LATE_PAYLOAD");
    } else {
      expect(alarms[0]!.message.text).toContain("Context exceeds window");
      expect(actors.messages(actor.id, 20).filter(message => message.error?.includes("Context exceeds window"))).toHaveLength(1);
    }
    expect(fs.readFileSync(transforms, "utf8")).toBe("first\nlast\n");
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary)("blocks native manual compaction before any summary request and retains the full journal", async () => {
    const s = await setup();
    const journal = path.join(s.dir, "actor.jsonl");
    const session = SessionManager.open(journal);
    session.appendMessage(user("OLD_MANUAL_HISTORY " + "x".repeat(160_000)));
    // Exceed the native 20k keep-recent budget in the reply too, so native
    // preparation has a real split-turn summary and reaches the blocking hook.
    session.appendMessage(assistant("old reply " + "y".repeat(100_000), 90_000));
    const before = readJournal(journal);
    const hook = fs.realpathSync(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER
      ? path.join(path.dirname(path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER)), "worker/activation-window.js")
      : path.resolve("src/worker/activation-window.ts"));
    const child = spawn(process.execPath, [nativeBinary, "--mode", "rpc", "--session", journal,
      "--no-extensions", "-e", hook, "--no-auto-compaction", "--no-tools", "--model", "window-test/offline"], {
      cwd: s.dir, env: { ...process.env, PI_FABRIC_PARENT_RUN: "manual-test", PI_FABRIC_ACTIVATION_NONCE: "manual-nonce",
        PI_FABRIC_ACTIVATION_HOOK: hook, PI_FABRIC_ACTIVATION_WORKER_PID: String(process.pid) },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let output = "";
    let stderr = "";
    let state: Record<string, unknown> | undefined;
    child.stderr.on("data", chunk => { stderr += chunk; });
    child.stdout.on("data", chunk => {
      output += chunk;
      while (output.includes("\n")) {
        const end = output.indexOf("\n");
        const line = output.slice(0, end);
        output = output.slice(end + 1);
        let event: Record<string, any>;
        try { event = JSON.parse(line); } catch { continue; }
        if (event.type === "response" && event.id === "state") {
          state = event.data;
          child.stdin.write(`${JSON.stringify({ type: "compact", id: "manual" })}\n`);
        }
      }
    });
    child.stdin.on("error", () => {});
    const timeout = setTimeout(() => child.kill("SIGKILL"), HANG_GUARD_MS);
    child.stdin.write(`${JSON.stringify({ type: "get_state", id: "state" })}\n`);
    const exit = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", resolve);
    }).finally(() => clearTimeout(timeout));
    expect(state, stderr).toMatchObject({ autoCompactionDisabledForProcess: true, autoCompactionEnabled: false });
    expect(exit, stderr).toBe(78);
    expect(stderr).toContain("Compaction is unsupported");
    expect(s.requests).toHaveLength(0);
    expectJournalAppended(journal, before);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary).each([false, true])("3238 compacts mid-run growth in a real activation (unfittable latest batch: %s)", async unfittable => {
    const s = await setup(5, unfittable ? 4 : 0);
    const alarms: Array<{message: {text?: string}}> = [];
    const mesh = new MeshStore(path.join(s.dir, "mesh"), 2 * 1024 * 1024, 100);
    const actors = new ActorManager("growing-window-test", {id: "owner", name: "owner", kind: "main", sessionId: "growing-window-test"}, mesh,
      {...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20}, s.manager, request => { alarms.push(request); },
      {actorRoot: path.join(s.dir, "actors"), persistent: true});
    managers.push(actors);
    const actor = await actors.create({name: "growing", instructions: "Read each requested file; preserve the current result.", inferenceContext: "activation",
      model: "window-test/offline", tools: ["read"], extensions: false, transport: "process", delivery: "mailbox"});
    const run = vi.spyOn(s.manager, "run");
    const outcome = actors.ask(actor.id, "CURRENT_GROWING_ACTIVATION");
    if (unfittable) await expect(outcome).rejects.toThrow(/Context exceeds window/);
    else await expect(outcome).resolves.toMatchObject({text: "useful current result"});
    await actors.close();
    expect(run).toHaveBeenCalledTimes(1);
    expect(alarms).toHaveLength(unfittable ? 1 : 0);
    expect(mesh.read({topic: "ops.owner"}).filter(event => event.kind === "actor.alarm")).toHaveLength(unfittable ? 1 : 0);
    expect(s.requests).toHaveLength(unfittable ? 4 : 6);
    const journal = readJournal(path.join(s.dir, "actors", actor.id, "session.jsonl"));
    const rawMessages = SessionManager.open(path.join(s.dir, "actors", actor.id, "session.jsonl")).getBranch()
      .flatMap(entry => entry.type === "message" ? sessionEntryToContextMessages(entry) : []);
    expect(estimateContextTokens(convertToLlm(rawMessages)).tokens).toBeGreaterThan(8000);
    const localContext = journal.entries.filter(entry => entry.type === "custom" && entry.customType === "fabric-activation-context");
    expect(localContext.every(entry => entry.data?.scope === "activation" && typeof entry.data.activationId === "string")).toBe(true);
    expect(localContext.filter(entry => entry.data?.entry.type === "compaction").length).toBeGreaterThanOrEqual(2);
    expect(localContext.filter(entry => entry.data?.entry.type === "context_edit").length).toBeGreaterThanOrEqual(2);
    expect(journal.entries.some(entry => entry.type === "compaction" || entry.type === "context_edit")).toBe(false);
    const raw = journal.bytes.toString("utf8");
    expect(raw).toContain("CURRENT_GROWING_ACTIVATION");
    for (let round = 1; round <= (unfittable ? 4 : 5); round++) {
      const full = fs.readFileSync(path.join(s.dir, `task-${round}.txt`), "utf8");
      expect(raw).toContain(full); // Full original output survives every inference compaction.
      const next = s.requests[round];
      if (!next) continue; // Latest oversized result was never dispatched.
      const tool = next.messages.find((message: any) => message.role === "tool" && message.tool_call_id === `read-${round}`);
      const text = typeof tool.content === "string" ? tool.content : tool.content.map((part: any) => part.text ?? "").join("");
      expect(text).toBe(full); // Current batch is not compacted, even at the threshold.
      const call = next.messages.find((message: any) => message.tool_calls?.some((call: any) => call.id === `read-${round}`));
      expect(call.tool_calls.some((call: any) => call.id === tool.tool_call_id)).toBe(true);
    }
    expect(JSON.stringify(s.requests.at(-1))).toContain("Compacted tool output");
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary)("3238 compacts before final admission through the complete awaited payload chain", async () => {
    const s = await setup(5);
    const extensionDir = path.join(s.dir, "agent", "extensions");
    fs.mkdirSync(extensionDir);
    const transforms = path.join(s.dir, "compaction-payload-transforms");
    fs.writeFileSync(path.join(extensionDir, "compaction-payload.ts"), `
      import fs from 'node:fs';
      export default function(pi) {
        pi.on('before_provider_request', async event => {
          await Promise.resolve();
          fs.appendFileSync(${JSON.stringify(transforms)}, 'first\\n');
          return {...event.payload, transientExpansion:'x'.repeat(40_000)};
        });
        pi.on('before_provider_request', async event => {
          await Promise.resolve();
          fs.appendFileSync(${JSON.stringify(transforms)}, 'last\\n');
          const {transientExpansion, ...final} = event.payload;
          return {...final, finalPayloadPadding:'FINAL_ADMITTED ' + 'x'.repeat(2000)};
        });
      }
    `);
    const journal = path.join(s.dir, "actor.jsonl");
    const result = await s.manager.run({task: "CURRENT_COMPOSED_ACTIVATION", model: "window-test/offline",
      actorId: "same-actor", sessionFile: journal, inferenceContext: "activation", tools: ["read"],
      extensions: true, transport: "process"});
    expect(result, explain(result)).toMatchObject({status: "completed", text: "useful current result"});
    expect(s.requestCount).toBe(6);
    expect(s.requests).toHaveLength(6);
    expect(fs.readFileSync(transforms, "utf8")).toBe("first\nlast\n".repeat(6));
    for (const request of s.requests) {
      expect(request.finalPayloadPadding).toMatch(/^FINAL_ADMITTED /);
      expect(request).not.toHaveProperty("transientExpansion");
      expect(Math.ceil(JSON.stringify(request).length / 4)).toBeLessThanOrEqual(8000);
    }
    const final = s.requests.at(-1)!;
    expect(JSON.stringify(final)).toContain("Compacted tool output");
    expect(JSON.stringify(final)).toContain("CURRENT_COMPOSED_ACTIVATION");
    expect(final.messages.find((message: any) => message.role === "tool" && message.tool_call_id === "read-5").content)
      .toBe(fs.readFileSync(path.join(s.dir, "task-5.txt"), "utf8"));
    const audit = readJournal(journal);
    expect(audit.entries.some(entry => entry.type === "custom" && entry.customType === "fabric-activation-context" &&
      entry.data?.scope === "activation" && entry.data.entry.type === "compaction")).toBe(true);
    expect(audit.entries.some(entry => entry.type === "compaction" || entry.type === "context_edit")).toBe(false);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary)("3238 restores an earlier same-actor sentinel in the real provider request after activation-local compaction", async () => {
    const s = await setup(5, 0, "CURRENT_COMPACTING_ACTIVATION");
    const mesh = new MeshStore(path.join(s.dir, "mesh"), 2 * 1024 * 1024, 100);
    const actors = new ActorManager("restore-window-test", {id: "owner", name: "owner", kind: "main", sessionId: "restore-window-test"}, mesh,
      {...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20}, s.manager, () => {},
      {actorRoot: path.join(s.dir, "actors"), persistent: true});
    managers.push(actors);
    const actor = await actors.create({name: "restored", instructions: "Act on each event.", inferenceContext: "full-history",
      model: "window-test/offline", tools: ["read"], extensions: false, transport: "process", delivery: "mailbox"});
    const journalFile = path.join(s.dir, "actors", actor.id, "session.jsonl");
    const sentinel = "ACTIVATION_ONE_SENTINEL: the launch password is violet-orchard-731.";
    await expect(actors.ask(actor.id, sentinel)).resolves.toMatchObject({text: "useful current result"});
    expect(s.requests).toHaveLength(1);
    const before = readJournal(journalFile);
    await actors.setInferenceContext(actor.id, "activation");
    await expect(actors.ask(actor.id, "CURRENT_COMPACTING_ACTIVATION")).resolves.toMatchObject({text: "useful current result"});
    expect(s.requests).toHaveLength(7);
    expect(JSON.stringify(s.requests.slice(1))).not.toContain(sentinel);
    expect(JSON.stringify(s.requests.at(-1))).toContain("Compacted tool output"); // Actual in-run compaction, not a synthetic journal.
    const compacted = expectJournalAppended(journalFile, before, true);
    expect(compacted.bytes.toString("utf8")).toContain(sentinel);

    // Full raw tool history now legitimately needs a larger model. Avoid an
    // unrelated native auto-compaction masking the restored-history request.
    const modelsFile = path.join(s.dir, "agent", "models.json");
    const models = JSON.parse(fs.readFileSync(modelsFile, "utf8"));
    models.providers["window-test"].models[0].contextWindow = 128_000;
    fs.writeFileSync(modelsFile, JSON.stringify(models));
    await actors.setInferenceContext(actor.id, "full-history");
    const nextRequest = s.requests.length;
    await expect(actors.ask(actor.id, "RESTORED_FULL_HISTORY_ACTIVATION_THREE")).resolves.toMatchObject({text: "useful current result"});
    await actors.close();
    expect(s.requests).toHaveLength(nextRequest + 1);
    // HTTP server captured the real serialized provider body, not a context hook.
    expect(JSON.stringify(s.requests[nextRequest])).toContain(sentinel);
    expect(JSON.stringify(s.requests[nextRequest])).toContain("CURRENT_COMPACTING_ACTIVATION");
    expect(JSON.stringify(s.requests[nextRequest])).toContain("RESTORED_FULL_HISTORY_ACTIVATION_THREE");
    expect(JSON.stringify(s.requests[nextRequest])).toContain(fs.readFileSync(path.join(s.dir, "task-1.txt"), "utf8")); // Local edits do not leak either.
    expect(compacted.entries.some(entry => entry.type === "compaction" || entry.type === "context_edit")).toBe(false);
    expectJournalAppended(journalFile, compacted, true);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, TEST_GUARD_MS);

  it.skipIf(!selectedNativeBinary)("3704 journals and replays full Fabric guidance during real actor activations", async () => {
    const s = await setup(0, 0, "NO_TOOL_ROUNDS", "openai-completions", true);
    const mesh = new MeshStore(path.join(s.dir, "mesh"), 2 * 1024 * 1024, 100);
    const actors = new ActorManager("fabric-window-test", {id: "owner", name: "owner", kind: "main", sessionId: "fabric-window-test"}, mesh,
      {...DEFAULT_FABRIC_CONFIG.mesh, actorPollMs: 20}, s.manager, () => {},
      {actorRoot: path.join(s.dir, "actors"), persistent: true});
    managers.push(actors);
    const actor = await actors.create({name: "fabric-actor", instructions: "Act on each event.", inferenceContext: "activation",
      model: "window-test/offline", kernel: "typescript", tools: ["read", "bash"], extensions: true, transport: "process", delivery: "mailbox"});
    const journal = path.join(s.dir, "actors", actor.id, "session.jsonl");
    const run = vi.spyOn(s.manager, "run");
    for (const task of ["FIRST_FABRIC_ACTIVATION", "SECOND_FABRIC_ACTIVATION"]) {
      // Retain the failed run too, so release qualification records the exact refusal.
      await actors.ask(actor.id, task).catch(() => undefined);
      const result = await run.mock.results.at(-1)!.value;
      const evidence = process.env.FABRIC_ACTIVATION_TEST_EVIDENCE_DIR;
      if (evidence) {
        fs.mkdirSync(evidence, { recursive: true });
        fs.writeFileSync(path.join(evidence, task + "-result.json"), JSON.stringify(result, null, 2));
        fs.copyFileSync(result.logFile!, path.join(evidence, task + "-events.jsonl"));
        if (fs.existsSync(journal)) fs.copyFileSync(journal, path.join(evidence, task + "-session.jsonl"));
        fs.writeFileSync(path.join(evidence, "requests.json"), JSON.stringify(s.requests, null, 2));
      }
      expect(result, explain(result)).toMatchObject({ status: "completed", text: "useful current result" });
      const records = buildSessionContext(SessionManager.open(journal).getBranch()).messages;
      const system = getCurrentSystemMessage(records as never)!;
      expect(system.content).toBe("");
      expect(system.sections).toBeDefined();
      expect(system.toolsAdded?.some(tool => tool.name === "fabric_exec")).toBe(true);
      const wire = JSON.stringify(s.requests.at(-1));
      expect(wire).toContain("Configured fabric_exec kernel");
      expect(JSON.stringify(system)).toContain("Configured fabric_exec kernel");
      // Witness every rendered section, not merely the presence of our guidance.
      const wireSystem = s.requests.at(-1)!.messages.find((message: {role: string}) =>
        message.role === "system" || message.role === "developer");
      expect(wireSystem.content).toBe(getCurrentSystemPrompt(records as never));
    }
    expect(s.requests).toHaveLength(2);
  }, 2 * HANG_GUARD_MS + 30_000);

  it.skipIf(!selectedNativeBinary)("two real activations retain full journals and current tool pairs, without Fabric tool enablement", async () => {
    const s = await setup();
    const journal = path.join(s.dir, "actor.jsonl");
    const request = { model: "window-test/offline", actorId: "same-actor", sessionFile: journal, inferenceContext: "activation" as const, tools: ["read"], extensions: false, transport: "process" as const };
    const session = SessionManager.open(journal);
    session.appendMessage(user("PRIOR_ACTIVATION"));
    session.appendMessage(assistant("prior reply"));
    let before = readJournal(journal);
    for (const [task, excluded] of [
      ["FIRST_ACTIVATION", "PRIOR_ACTIVATION"],
      ["SECOND_ACTIVATION", "FIRST_ACTIVATION"],
    ] as const) {
      const requestStart = s.requests.length;
      const result = await s.manager.run({ ...request, task });
      expect(result, explain(result)).toMatchObject({ status: "completed", text: "useful current result" });
      const inputs = s.requests.slice(requestStart);
      expect(inputs).toHaveLength(2);
      expect(JSON.stringify(inputs)).not.toContain(excluded);
      expect(JSON.stringify(inputs)).not.toContain("PRIOR_ACTIVATION");
      for (const input of inputs) {
        expect(input.tools.map((tool: {function: {name: string}}) => tool.function.name)).toEqual(["read"]);
      }
      const conversation = (input: Record<string, any>) => input.messages.filter(
        (message: {role: string}) => message.role !== "system" && message.role !== "developer",
      );
      const initial = conversation(inputs[0]!);
      const continuation = conversation(inputs[1]!);
      expect(initial.map((message: {role: string}) => message.role)).toEqual(["user"]);
      expect(continuation.map((message: {role: string}) => message.role)).toEqual(["user", "assistant", "tool"]);
      expect(continuation[0]).toEqual(initial[0]);
      const text = (content: string | Array<{type: string; text?: string}>) => typeof content === "string"
        ? content : content.map(part => part.type === "text" ? part.text : "").join("");
      expect(text(initial[0].content)).toBe(task);
      expect(continuation[1].tool_calls).toHaveLength(1);
      const call = continuation[1].tool_calls[0];
      expect(call).toMatchObject({ id: "read-current", type: "function", function: { name: "read" } });
      expect(JSON.parse(call.function.arguments)).toEqual({ path: path.join(s.dir, "task.txt") });
      expect(continuation[2].tool_call_id).toBe(call.id);
      expect(text(continuation[2].content)).toBe("current tool result");
      // OpenAI tool results identify their tool by the matching call ID, not a name field.
      before = expectJournalAppended(journal, before);
      expect(before.bytes.toString("utf8")).toContain(task);
      expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
    }
  }, 2 * HANG_GUARD_MS + 30_000);
});
