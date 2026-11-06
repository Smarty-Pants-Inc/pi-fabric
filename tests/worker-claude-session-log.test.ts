import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { ProcessTransport } from "../src/agents/transports/process-transport.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Keep reason: B1 / R-keep-all-traces. The Claude native actor session is a
// different persistence boundary from the minimized events.jsonl run log.
// Exercise real manager admission -> compiled worker -> offline native protocol;
// never call the production projection or assert against production source text.
const workerPath = process.env.FABRIC_CLAUDE_SESSION_TEST_WORKER_ENTRY ?? path.resolve("dist/worker.js");
const expectedWorkerPath = process.env.FABRIC_CLAUDE_SESSION_TEST_EXPECTED_WORKER_ENTRY;
const fakeClaude = pathToFileURL(path.resolve("tests/fixtures/fake-claude.mjs")).href;
const sessionId = "11111111-1111-4111-8111-111111111111";
const actorId = "1234567890abcdef1234567890abcdef";
const actorName = "claude-native-trace-fixture";
const model = "claude-haiku-test";
const toolId = "toolu_fake_1";
const usage = { input: 10, output: 7, cacheRead: 2, cacheWrite: 3, cost: 0.001 };
const canaries = {
  argument: "FAKE-B1-argument-only.txt",
  nestedArgument: "FAKE-B1-nested-argument-only",
  toolExtra: "FAKE-B1-tool-use-extra-only",
  content: "FAKE-B1-result-content-only",
  nestedContent: "FAKE-B1-nested-result-content-only",
  details: "FAKE-B1-result-details-only",
  mixedBlock: "FAKE-B1-mixed-user-text-only",
  messageExtra: "FAKE-B1-user-message-extra-only",
  topLevelResult: "FAKE-B1-top-level-tool-use-result-only",
  eventExtra: "FAKE-B1-user-event-extra-only",
};
const roots: string[] = [];
const managers: AgentManager[] = [];
const nativeCloses: Promise<unknown>[] = [];

afterEach(async () => {
  // Terminal status alone is not native process closure. Join the real transport
  // handles before deleting only the mkdtemp roots this test created.
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(nativeCloses.splice(0));
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) {
    await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
  }
});

type NativeEvent = {
  type: string;
  message?: { role?: string; content?: unknown; [key: string]: unknown };
  [key: string]: unknown;
};
const eventsIn = (text: string): NativeEvent[] => text.trimEnd().split("\n").map(line => JSON.parse(line) as NativeEvent);
const assistantToolEvent = (events: NativeEvent[]) => events.find(event => {
  const content = event.message?.content;
  return event.type === "assistant" && Array.isArray(content) && content.some(part => part.type === "tool_use");
});
const userToolEvent = (events: NativeEvent[]) => events.find(event => {
  const content = event.message?.content;
  return event.type === "user" && Array.isArray(content) && content.some(part => part.type === "tool_result");
});

const runNativeActor = async (isError: boolean) => {
  expect(path.isAbsolute(workerPath), "select an absolute compiled worker entry").toBe(true);
  expect(path.extname(workerPath), "this regression must run the compiled worker, not the source entry").toBe(".js");
  expect(fs.existsSync(workerPath), "parent must build the selected worker; never skip missing security coverage").toBe(true);
  if (expectedWorkerPath !== undefined) {
    expect(path.isAbsolute(expectedWorkerPath)).toBe(true);
    expect(fs.realpathSync(workerPath)).toBe(fs.realpathSync(expectedWorkerPath));
  }
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-claude-native-trace-")));
  roots.push(root);
  const profile = path.join(root, "profile");
  const home = path.join(root, "home");
  const cwd = path.join(root, "cwd");
  const claudeConfig = path.join(root, "claude-config");
  for (const directory of [profile, home, cwd, claudeConfig]) fs.mkdirSync(directory, { mode: 0o700 });
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({ extensions: [], packages: [] }));
  vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  vi.stubEnv("PI_OFFLINE", "1");
  vi.stubEnv("HOME", home);
  vi.stubEnv("USERPROFILE", home);
  vi.stubEnv("CLAUDE_CONFIG_DIR", claudeConfig);
  vi.stubEnv("FAKE_CLAUDE_SESSION_ID", sessionId);
  const launchesFile = path.join(root, "claude-launches.jsonl");
  vi.stubEnv("FAKE_CLAUDE_LOG", launchesFile);
  const emittedFile = path.join(root, "child-native-frames.jsonl");
  const sessionFile = path.join(root, "actor-session.jsonl");
  const prior = JSON.stringify({ type: "user", message: { role: "user", content: "Earlier clean actor turn." },
    parent_tool_use_id: null, session_id: sessionId }) + "\n";
  fs.writeFileSync(sessionFile, prior, { mode: 0o600 });

  const originalAssistant = {
    type: "assistant",
    message: {
      role: "assistant",
      content: [{ type: "tool_use", id: toolId, name: "Read",
        input: { file_path: canaries.argument, nested: { query: canaries.nestedArgument } },
        fixture_extra: { nested: [canaries.toolExtra] } }],
      usage: { input_tokens: 10, output_tokens: 1, cache_read_input_tokens: 2, cache_creation_input_tokens: 3 },
    },
    parent_tool_use_id: null,
    session_id: sessionId,
  };
  const originalUser = {
    type: "user",
    message: {
      role: "user",
      content: [
        { type: "tool_result", tool_use_id: toolId, is_error: isError,
          content: [{ type: "text", text: canaries.content }, { type: "text", text: canaries.nestedContent }],
          details: { nested: { stdout: canaries.details }, exitCode: isError ? 1 : 0 } },
        { type: "text", text: canaries.mixedBlock },
      ],
      fixture_extra: canaries.messageExtra,
    },
    parent_tool_use_id: null,
    session_id: sessionId,
    tool_use_result: { stdout: canaries.topLevelResult, nested: { content: [canaries.topLevelResult] } },
    fixture_extra: { nested: canaries.eventExtra },
  };

  // Reuse the existing fake's stream-json input parser, native model support,
  // clean final text, and usage. Its catalog handshake remains supported but is
  // not required by this native actor route with an explicit canonical model.
  // This wrapper changes ONLY the two FAKE tool records and records the actual
  // child wire bytes independently of either production persistence sink.
  // .mjs uses the real launcher's Node-script path on POSIX and Windows; no chmod,
  // shell wrapper, credential source, live Claude executable, or network server.
  const executable = path.join(root, "offline-claude.mjs");
  fs.writeFileSync(executable, `import fs from "node:fs";
const originalAssistant = ${JSON.stringify(originalAssistant)};
const originalUser = ${JSON.stringify(originalUser)};
const write = process.stdout.write.bind(process.stdout);
process.stdout.write = (chunk, ...args) => {
  let event = JSON.parse(String(chunk));
  if (event.type === "assistant" && event.message?.content?.some(part => part.type === "tool_use")) event = originalAssistant;
  if (event.type === "user") event = originalUser;
  const line = JSON.stringify(event) + "\\n";
  if (event.type !== "control_response") fs.appendFileSync(${JSON.stringify(emittedFile)}, line);
  return write(line, ...args);
};
await import(${JSON.stringify(fakeClaude)});
`);

  const realLaunch = ProcessTransport.prototype.launch;
  vi.spyOn(ProcessTransport.prototype, "launch").mockImplementation(async function (this: ProcessTransport, request) {
    expect(fs.realpathSync(request.workerPath)).toBe(fs.realpathSync(workerPath));
    const handle = await realLaunch.call(this, request);
    expect(handle.closed, "the real native process transport must expose a close receipt").toBeDefined();
    if (handle.closed) nativeCloses.push(handle.closed);
    return handle;
  });
  const manager = new AgentManager(cwd, {
    ...DEFAULT_FABRIC_CONFIG.agents,
    runner: "claude", timeoutMs: 30_000, retainRuns: true, budgetUsd: 0, deniedModels: [], sessionExport: false,
    claude: { ...DEFAULT_FABRIC_CONFIG.agents.claude, model },
    // In-memory offline fixture policy only: do not read a live model exception
    // source or forge trusted metadata. With no deny/reason policy, native model
    // admission passes the requested selector through without catalog discovery.
    modelPolicy: { ...DEFAULT_FABRIC_CONFIG.agents.modelPolicy, requireReason: [] },
  }, { workerPath, claudeBinary: executable, runRoot: path.join(root, "runs"), fullCodeMode: false });
  managers.push(manager);
  const task = isError ? "CLAUDE_FAIL" : "Retain this native actor trace.";
  const result = await manager.run({ task, model, transport: "process", extensions: false, tools: ["read"],
    actorId, actorName, sessionFile, runnerSessionId: sessionId, inferenceContext: "full-history" });
  const finalText = isError ? "fake Claude failure" : `fake claude complete: ${task.slice(0, 40)}`;
  const expectedTerminal = {
    status: isError ? "failed" : "completed", runner: "claude", model,
    actorId, actorName, runnerSessionId: sessionId, turns: 2, toolCalls: 1, usage, text: finalText, exitCode: 0,
    ...(isError ? { error: "fake Claude failure" } : {}),
  };
  expect(result, result.error).toMatchObject(expectedTerminal);
  if (!isError) expect(result.error).toBeUndefined();
  const runDirectory = manager.runDirectory(result.id)!;
  // The explicit canonical selector is retained in worker status and manager
  // receipts alike; the native init frame does not rewrite a requested model.
  // Immutable manager settlement receipt, worker terminal status, and public
  // rejoin all retain the same execution facts. Claude has no Pi writeReceipt;
  // do not manufacture one or confuse tool-payload minimization with accounting.
  for (const record of [manager.status(result.id), await manager.wait(result.id),
    JSON.parse(fs.readFileSync(path.join(runDirectory, "status.json"), "utf8")),
    JSON.parse(fs.readFileSync(path.join(runDirectory, "manager-result.json"), "utf8"))]) {
    expect(record).toMatchObject(expectedTerminal);
  }
  const launches = eventsIn(fs.readFileSync(launchesFile, "utf8"));
  // Count the actual resumed actor child, not optional catalog probes. The
  // fixture's no-policy admission path does not require a discovery invocation.
  const actorLaunches = launches.filter(launch => (launch.argv as string[]).includes("--resume"));
  expect(actorLaunches).toHaveLength(1);
  expect(actorLaunches[0]!.options).toMatchObject({
    "--input-format": "stream-json", "--output-format": "stream-json", "--model": model,
    "--tools": "Read", "--allowedTools": "Read", "--resume": sessionId,
  });
  expect(actorLaunches[0]!.argv).not.toContain("--no-session-persistence");
  const emitted = fs.readFileSync(emittedFile, "utf8");
  const nativeFrames = eventsIn(emitted);
  expect(nativeFrames.map(event => event.type)).toEqual(["system", "assistant", "user", "assistant", "result"]);
  expect(assistantToolEvent(nativeFrames)).toEqual(originalAssistant);
  expect(userToolEvent(nativeFrames)).toEqual(originalUser);
  expect(result.logFile).toBe(path.join(runDirectory, "events.jsonl"));
  const log = fs.readFileSync(result.logFile!, "utf8");
  return { result, log, logEvents: eventsIn(log), sessionFile, prior, emitted, originalAssistant, originalUser, finalText };
};

describe.each([{ label: "success", isError: false }, { label: "error", isError: true }])(
  "Claude native actor session / minimized run log ($label)", ({ isError }) => {
    it("retains ORIGINAL child records in the native sessionFile, including tool arguments and full results", async () => {
      const fixture = await runNativeActor(isError);
      const nativeBytes = fs.readFileSync(fixture.sessionFile, "utf8");
      const nativeEvents = eventsIn(nativeBytes);
      // These are the B1 RED assertions: on the reviewed head both native tool
      // records are projected, despite options.sessionFile being the actor trace.
      expect.soft(assistantToolEvent(nativeEvents), "native tool_use.input and extras must not be projected").toEqual(fixture.originalAssistant);
      expect.soft(userToolEvent(nativeEvents), "native tool_result content/details/top-level envelope must survive").toEqual(fixture.originalUser);
      expect.soft(nativeBytes, "append original child wire bytes after the existing native session").toBe(fixture.prior + fixture.emitted);
    }, 45_000);

    it("still minimizes events.jsonl while preserving completion IDs/error, final text, usage and terminal receipts", async () => {
      const fixture = await runNativeActor(isError);
      for (const canary of Object.values(canaries)) expect(fixture.log).not.toContain(canary);
      expect(assistantToolEvent(fixture.logEvents)?.message?.content).toEqual([
        { type: "tool_use", id: toolId, name: "Read" },
      ]);
      expect(userToolEvent(fixture.logEvents)).toEqual({ type: "user", message: { role: "user", content: [
        { type: "tool_result", tool_use_id: toolId, is_error: isError, content: [] },
      ] } });
      expect(fixture.logEvents.find(event => event.type === "result")).toMatchObject({
        subtype: isError ? "error_during_execution" : "success", is_error: isError, result: fixture.finalText,
        session_id: sessionId, total_cost_usd: 0.001,
        usage: { input_tokens: 10, output_tokens: 7, cache_read_input_tokens: 2, cache_creation_input_tokens: 3 },
        ...(isError ? { errors: ["fake Claude failure"] } : {}),
      });
      // Terminal compaction must not retain a raw backup of events.jsonl either.
      const directory = path.dirname(fixture.result.logFile!);
      for (const name of fs.readdirSync(directory).filter(name => name.startsWith("events.jsonl"))) {
        const bytes = fs.readFileSync(path.join(directory, name), "utf8");
        for (const canary of Object.values(canaries)) expect(bytes, name).not.toContain(canary);
      }
    }, 45_000);
  },
);
