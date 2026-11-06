import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import type { FabricActorActivationIdentity } from "../src/actors/activation-identity.js";
import type { RunLogSecretCase } from "./fixtures/run-log-secret-guard.js";

// Keep reason: real child-tool -> tool_result hook -> worker persistence is a
// secret/data-safety trust boundary for both Pi CLI and the native durable host.
// Do not replace this with a fake Pi stream
// or a unit test that merely starts from an already-redacted event.
// Baseline override accepts a separately built origin/main worker without Git
// mutation in this checkout. The candidate defaults to its freshly built worker.
// External controls must survive scripts/test-temp.ts's PI_FABRIC_* scrub.
const workerPath = process.env.FABRIC_RUN_LOG_TEST_WORKER_ENTRY ?? path.resolve("dist/worker.js");
const expectedWorkerPath = process.env.FABRIC_RUN_LOG_TEST_EXPECTED_WORKER_ENTRY;
const guardFixture = path.resolve("tests/fixtures/run-log-secret-guard.ts");
const FAKE_SECRET = "FAKE-RUN-LOG-configured/credential+2026=only?";
const GUARD_MARKER = "[REDACTED:run-log-fixture]";
const roots: string[] = [];
const managers: AgentManager[] = [];
const servers: http.Server[] = [];

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve, reject) => {
    server.close(error => error ? reject(error) : resolve());
    server.closeAllConnections();
  })));
  vi.unstubAllEnvs();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

type ProviderToolMessage = { role: string; content?: unknown; tool_call_id?: string };
type LogEvent = {
  type: string;
  toolCallId?: string;
  toolName?: string;
  isError?: boolean;
  durationMs?: number;
  result?: Record<string, unknown>;
  message?: { role?: string; toolCallId?: string; toolName?: string; content?: unknown; [key: string]: unknown };
};

type RunMode = "ordinary task" | "AgentManager actor activation seam";
type PiRunner = "pi" | "pi-durable";

const runGuardedChild = async (variant: RunLogSecretCase, mode: RunMode, runner: PiRunner) => {
  expect(path.isAbsolute(workerPath), "FABRIC_RUN_LOG_TEST_WORKER_ENTRY must be an absolute built worker path").toBe(true);
  expect(fs.existsSync(workerPath), "Build the selected worker before running this regression; missing builds must not skip security coverage").toBe(true);
  // Set independently by the caller for baseline/candidate controls. A scrubbed
  // or miswired override must fail here, not quietly test the checkout's worker.
  if (expectedWorkerPath !== undefined) {
    expect(path.isAbsolute(expectedWorkerPath), "FABRIC_RUN_LOG_TEST_EXPECTED_WORKER_ENTRY must be absolute").toBe(true);
    expect(fs.realpathSync(workerPath), "selected worker must match the independent expected path").toBe(fs.realpathSync(expectedWorkerPath));
  }
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "fabric-guarded-run-log-")));
  roots.push(root);
  const profile = path.join(root, "profile");
  const cwd = path.join(root, "cwd");
  const home = path.join(root, "home");
  const auth = path.join(root, "empty-auth");
  for (const dir of [profile, cwd, home, auth]) fs.mkdirSync(dir, { mode: 0o700 });
  vi.stubEnv("PI_CODING_AGENT_DIR", profile);
  vi.stubEnv("PI_OFFLINE", "1");
  vi.stubEnv("HOME", home);
  vi.stubEnv("USERPROFILE", home);
  vi.stubEnv("PI_FABRIC_TEST_RUN_LOG_FAKE_SECRET", FAKE_SECRET);
  vi.stubEnv("PI_FABRIC_TEST_RUN_LOG_EMPTY_AUTH", auth);
  vi.stubEnv("PI_FABRIC_TEST_RUN_LOG_CASE", variant);
  // This is the real AgentManager launch seam used by ActorManager, not a test
  // of ActorManager's registry, queue, ownership or activation state machinery.
  const actorSelf: FabricActorActivationIdentity | undefined = mode === "AgentManager actor activation seam" ? {
    actorId: "1234567890abcdef1234567890abcdef", name: "run-log-fixture-actor",
    rootId: "session:run-log-fixture", ownershipToken: "offline-fixture-only",
    instructionsDigest: "0".repeat(64),
  } : undefined;
  vi.stubEnv("PI_FABRIC_TEST_RUN_LOG_ACTOR_SELF", JSON.stringify(actorSelf ?? null));

  const providerToolMessages: ProviderToolMessage[] = [];
  const server = http.createServer((request, response) => {
    const chunks: Buffer[] = [];
    request.on("data", chunk => chunks.push(Buffer.from(chunk)));
    request.on("end", () => {
      const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as { messages: ProviderToolMessage[] };
      const toolResult = body.messages.find(message => message.role === "tool");
      if (toolResult) providerToolMessages.push(toolResult);
      const delta = toolResult
        ? { role: "assistant", content: "guarded run-log fixture completed" }
        : { role: "assistant", tool_calls: [{ index: 0, id: "guarded-secret-call", type: "function",
          function: { name: "run_log_secret_fixture", arguments: "{}" } }] };
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      for (const [part, finish] of [[delta, null], [{}, toolResult ? "stop" : "tool_calls"]]) {
        response.write(`data: ${JSON.stringify({ id: "offline-run-log", object: "chat.completion.chunk",
          choices: [{ index: 0, delta: part, finish_reason: finish }] })}\n\n`);
      }
      response.end("data: [DONE]\n\n");
    });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Offline provider did not bind a loopback port");
  fs.writeFileSync(path.join(profile, "settings.json"), JSON.stringify({
    extensions: [guardFixture], packages: [], defaultProjectTrust: "never", enableInstallTelemetry: false,
    compaction: { enabled: false }, retry: { enabled: false },
  }));
  fs.writeFileSync(path.join(profile, "models.json"), JSON.stringify({ providers: { "run-log-proof": {
    baseUrl: `http://127.0.0.1:${address.port}/v1`, apiKey: "offline-only-fake-key", api: "openai-completions",
    models: [{ id: "offline", name: "offline", reasoning: false, input: ["text"], contextWindow: 32000, maxTokens: 1024,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
  } } }));
  const cli = fs.realpathSync(path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"));
  const manager = new AgentManager(cwd, {
    ...DEFAULT_FABRIC_CONFIG.agents, runner, timeoutMs: 30_000, retainRuns: true,
    budgetUsd: 0, deniedModels: [], sessionExport: false,
  }, { workerPath, piBinary: cli, fullCodeMode: false, runRoot: path.join(root, "runs") });
  managers.push(manager);
  const result = await manager.run({ task: "Invoke run_log_secret_fixture once, then finish.", transport: "process",
    extensions: true, tools: ["run_log_secret_fixture"], model: "run-log-proof/offline",
    ...(actorSelf ? { actorId: actorSelf.actorId, actorName: actorSelf.name, actorSelf,
      sessionFile: path.join(root, "actor-session.jsonl"), inferenceContext: "full-history" as const } : {}),
  });
  expect(result.status, result.error).toBe("completed");
  expect(result.runner, "the requested native runner must not silently fall back").toBe(runner);
  expect(result.actorId).toBe(actorSelf?.actorId);
  expect(result.actorName).toBe(actorSelf?.name);
  expect(result.text).toBe("guarded run-log fixture completed");
  expect(result.toolCalls).toBe(1);
  expect(providerToolMessages).toHaveLength(1);
  const observedText = providerToolMessages[0]!.content;
  expect(observedText).toEqual(expect.any(String));
  expect((observedText as string).length).toBeGreaterThan(0);
  // Portable fixtures stay strict: clean supplemental text and exact configured
  // FAKE replacement. The optional real guard may legitimately fail closed for
  // supplemental payloads; its observed post-hook text is authoritative, not a
  // requirement to manufacture clean text. Text remains the replacement control.
  const expectedText = process.env.FABRIC_RUN_LOG_TEST_SECRET_GUARD && variant !== "text"
    ? observedText as string
    : variant === "text" ? `fixture text: ${GUARD_MARKER}` : `clean fixture text: ${variant}`;
  expect(providerToolMessages[0]).toMatchObject({ role: "tool", content: expectedText });
  expect(JSON.stringify(providerToolMessages)).not.toContain(FAKE_SECRET);
  expect(JSON.stringify(providerToolMessages)).not.toContain(encodeURIComponent(FAKE_SECRET));
  expect(fs.readdirSync(auth), "the real guard must use only the owned empty auth directory").toEqual([]);
  expect(result.logFile).toBeDefined();
  expect(path.basename(result.logFile!)).toBe("events.jsonl");
  const log = fs.readFileSync(result.logFile!, "utf8");
  const events = log.trim().split("\n").map(line => JSON.parse(line) as LogEvent);
  return { result, log, events, expectedText, toolCallId: providerToolMessages[0]!.tool_call_id };
};

describe.each<{ runner: PiRunner; mode: RunMode }>([
  { runner: "pi", mode: "ordinary task" },
  { runner: "pi", mode: "AgentManager actor activation seam" },
  { runner: "pi-durable", mode: "ordinary task" },
  { runner: "pi-durable", mode: "AgentManager actor activation seam" },
])("real worker post-hook persistence ($runner): $mode", ({ runner, mode }) => {
  it.each<RunLogSecretCase>(["text", "structuredContent", "details", "percent-encoded"])(
    "does not retain a configured FAKE secret from child %s payloads", async variant => {
      const { result, log, events, expectedText, toolCallId } = await runGuardedChild(variant, mode, runner);
      const content = [{ type: "text", text: expectedText }];
      // Assert physical persisted bytes, not a reader-side redacted projection.
      // The base worker leaks raw execution-update/end text and supplemental data
      // despite the canonical text having passed the child's tool_result hook.
      expect(log, "events.jsonl must never persist pre-hook/supplemental FAKE secret bytes").not.toContain(FAKE_SECRET);
      expect(log, "percent-encoded supplemental credentials must be omitted, not regex-redacted").not.toContain(encodeURIComponent(FAKE_SECRET));
      expect(log).not.toContain("structured-content-must-not-be-persisted");
      expect(log).not.toContain("nested-details-must-not-be-persisted");
      // Raw execution completion closes the lifecycle using metadata only; the
      // canonical post-hook message supplies the sole content-bearing completion.
      const canonical = events.filter(event => event.type === "message_end" && event.message?.role === "toolResult");
      expect(canonical).toHaveLength(0);
      expect(events.filter(event => event.type === "tool_execution_update")).toHaveLength(0);
      const ends = events.filter(event => event.type === "tool_execution_end");
      const guardedEnds = ends.filter(event => Object.hasOwn(event, "result"));
      const metadataEnds = ends.filter(event => !Object.hasOwn(event, "result"));
      expect(guardedEnds).toHaveLength(1);
      expect(metadataEnds).toHaveLength(1);
      expect(toolCallId).toEqual(expect.any(String));
      for (const event of ends) {
        const duration = Object.hasOwn(event, "durationMs")
          ? { durationMs: expect.any(Number) } : {};
        if ("durationMs" in event) expect(Number.isFinite(event.durationMs)).toBe(true);
        expect(event).toEqual({ type: "tool_execution_end", toolCallId,
          toolName: "run_log_secret_fixture", isError: false, ...duration,
          ...(Object.hasOwn(event, "result") ? { result: { content } } : {}) });
      }
      expect(guardedEnds[0]!.result).toEqual({ content });
      // Terminal compaction may retain original bytes; it must never resurrect
      // a raw secret in an original/temporary sibling of this event log either.
      const directory = path.dirname(result.logFile!);
      for (const name of fs.readdirSync(directory).filter(name => name.startsWith("events.jsonl"))) {
        const bytes = fs.readFileSync(path.join(directory, name), "utf8");
        expect(bytes, name).not.toContain(FAKE_SECRET);
        expect(bytes, name).not.toContain(encodeURIComponent(FAKE_SECRET));
      }
    }, 45_000,
  );
});
