import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import { spawn, spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SessionManager, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { ActivationWindow } from "../src/worker/activation-window.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const roots: string[] = [];
const managers: AgentManager[] = [];
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
  for (const dir of roots.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});
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
      const handlers = new Map();
      try { hook({on(name, fn) { if (${JSON.stringify(mode)} === 'registration') throw new Error('registration failed'); handlers.set(name, fn); }}); }
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
    const result = spawnSync(process.execPath, [child], { encoding: "utf8" });
    expect(result.status, result.stderr).toBe(78);
    expect(result.stderr).toContain("Fabric activation window failed");
    expect(fs.existsSync(requestCounter)).toBe(false);
  });

  it("does not terminate an owner that accidentally loads the hook without worker binding", async () => {
    const { default: hook } = await import("../src/worker/activation-window.js");
    vi.stubEnv("PI_FABRIC_ACTIVATION_WORKER_PID", "");
    expect(() => hook({ on: vi.fn() } as unknown as ExtensionAPI)).toThrow(/not in a disposable worker/);
  });
});

describe("activation worker admission (offline transport fixture)", () => {
  it.each(["missing-hook", "wrong-run", "wrong-nonce", "wrong-policy", "wrong-hook", "wrong-protocol", "ignored-flag", "still-enabled", "exit", "timeout"])("does not send a prompt or inference request on %s", async scenario => {
    const dir = root();
    const scenarioFile = path.join(dir, "scenario");
    fs.writeFileSync(scenarioFile, `activation:${scenario}`);
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenarioFile);
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 1500 }, {
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
    expect(log).toContain("--no-extensions");
    expect(log).toContain("--no-tools");
    expect(log).toContain("--no-auto-compaction");
    expect(log).not.toContain("set_auto_compaction");
  }, 10_000);
});

// Required qualification target, not a live-model probe. CI must supply the
// exact native artifact with the ephemeral flag + explicit get_state marker.
const selectedNativeBinary = process.env.PI_FABRIC_ACTIVATION_TEST_PI_BINARY;
const nativeBinary = selectedNativeBinary ?? path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js");
describe("native activation window (offline; opted-in success needs exact native artifact)", () => {
  const readJournal = (journal: string) => {
    const bytes = fs.readFileSync(journal);
    const entries = bytes.toString("utf8").split("\n").filter(line => line.length > 0)
      .map(line => JSON.parse(line) as { type: string });
    return { bytes, entries };
  };
  const expectJournalAppended = (journal: string, before: ReturnType<typeof readJournal>) => {
    const after = readJournal(journal);
    expect(after.bytes.subarray(0, before.bytes.length)).toEqual(before.bytes);
    expect(after.entries.slice(0, before.entries.length)).toEqual(before.entries);
    expect(after.entries.some(entry => entry.type === "compaction")).toBe(false);
    return after;
  };
  const setup = async () => {
    const dir = root();
    const requests: Array<Record<string, any>> = [];
    const server = http.createServer((request, response) => {
      let body = "";
      request.on("data", chunk => { body += chunk; });
      request.on("end", () => {
        const payload = JSON.parse(body);
        requests.push(payload);
        const useTool = payload.tools?.length && !payload.messages.some((m: {role: string}) => m.role === "tool");
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        const chunk = (delta: unknown, finish_reason: string | null = null) => response.write(`data: ${JSON.stringify({
          id: "offline", object: "chat.completion.chunk", created: 1, model: "offline",
          choices: [{ index: 0, delta, finish_reason }],
        })}\n\n`);
        if (useTool) {
          chunk({ role: "assistant", tool_calls: [{ index: 0, id: "read-current", type: "function", function: { name: "read", arguments: JSON.stringify({ path: path.join(dir, "task.txt") }) } }] });
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
      "window-test": { baseUrl: `http://127.0.0.1:${port}/v1`, apiKey: "offline-only", api: "openai-completions", models: [{
        id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 8000, maxTokens: 1024,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      }] },
    } }));
    const settingsFile = path.join(agentDir, "settings.json");
    const settings = JSON.stringify({ enableInstallTelemetry: false, compaction: { enabled: true, reserveTokens: 1000 } });
    fs.writeFileSync(settingsFile, settings);
    fs.writeFileSync(path.join(dir, "task.txt"), "current tool result");
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    const manager = new AgentManager(dir, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 15_000 }, {
      workerPath: path.resolve(process.env.PI_FABRIC_ACTIVATION_TEST_WORKER ?? "src/worker.ts"),
      piBinary: nativeBinary!, runRoot: path.join(dir, "runs"),
    });
    managers.push(manager);
    return { dir, manager, requests, settingsFile, settings };
  };

  it.skipIf(Boolean(selectedNativeBinary))("rejects an old native CLI that ignores the flag even when global compaction is already false", async () => {
    const s = await setup();
    fs.writeFileSync(s.settingsFile, JSON.stringify({ compaction: { enabled: false }, enableInstallTelemetry: false }));
    const result = await s.manager.run({ task: "must not infer", model: "window-test/offline", actorId: "same-actor", sessionFile: path.join(s.dir, "actor.jsonl"), inferenceContext: "activation", tools: [], extensions: false, transport: "process" });
    expect(result, JSON.stringify(result)).toMatchObject({ status: "failed" });
    expect(result.error).toContain("--no-auto-compaction");
    expect(s.requests).toHaveLength(0);
  }, 25_000);

  it("keeps the omitted full-history default on the actual native request path", async () => {
    const s = await setup();
    const journal = path.join(s.dir, "actor.jsonl");
    const session = SessionManager.open(journal);
    session.appendMessage(user("OLD_DEFAULT_HISTORY"));
    session.appendMessage(assistant("old reply"));
    const result = await s.manager.run({ task: "CURRENT_DEFAULT", model: "window-test/offline", actorId: "same-actor", sessionFile: journal, tools: [], extensions: false, transport: "process" });
    expect(result, JSON.stringify(result)).toMatchObject({ status: "completed", text: "useful current result" });
    expect(s.requests).toHaveLength(1);
    expect(JSON.stringify(s.requests)).toContain("OLD_DEFAULT_HISTORY");
    expect(JSON.stringify(s.requests)).toContain("CURRENT_DEFAULT");
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 25_000);

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
    expect(result, JSON.stringify(result)).toMatchObject({ status: "completed", text: "useful current result" });
    expect(s.requests).toHaveLength(1);
    expect(JSON.stringify(s.requests)).not.toContain("OLD_PRIVATE_ACTIVATION");
    expect(JSON.stringify(s.requests)).toContain("CURRENT_ACTIVATION");
    expect(s.requests[0]!.tools ?? []).toHaveLength(0);
    const after = expectJournalAppended(journal, before);
    expect(after.bytes.toString("utf8")).toContain("CURRENT_ACTIVATION");
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 25_000);

  it.skipIf(!selectedNativeBinary)("blocks native manual compaction before any summary request and retains the full journal", async () => {
    const s = await setup();
    const journal = path.join(s.dir, "actor.jsonl");
    const session = SessionManager.open(journal);
    session.appendMessage(user("OLD_MANUAL_HISTORY " + "x".repeat(160_000)));
    session.appendMessage(assistant("old reply", 90_000));
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
    const timeout = setTimeout(() => child.kill("SIGKILL"), 15_000);
    child.stdin.write(`${JSON.stringify({ type: "get_state", id: "state" })}\n`);
    const exit = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject);
      child.once("close", resolve);
    }).finally(() => clearTimeout(timeout));
    expect(state).toMatchObject({ autoCompactionDisabledForProcess: true, autoCompactionEnabled: false });
    expect(exit, stderr).toBe(78);
    expect(stderr).toContain("Compaction is unsupported");
    expect(s.requests).toHaveLength(0);
    expectJournalAppended(journal, before);
    expect(fs.readFileSync(s.settingsFile, "utf8")).toBe(s.settings);
  }, 20_000);

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
      expect(result, JSON.stringify(result)).toMatchObject({ status: "completed", text: "useful current result" });
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
  }, 40_000);
});
