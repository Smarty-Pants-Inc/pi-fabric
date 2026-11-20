import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";
import { rmTempSync } from "./fixtures/temp-cleanup.js";

const workerPath = path.resolve("dist/worker.js");
const requested = "openai-codex/gpt-5.6-sol";

describe.skipIf(!fs.existsSync(workerPath))("real worker model admission", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) rmTempSync(root);
  });
  const root = () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-model-admission-"));
    roots.push(directory);
    return directory;
  };
  const run = async (scenario: string, model = requested) => {
    const directory = root();
    const scenarioFile = path.join(directory, "scenario");
    fs.writeFileSync(scenarioFile, scenario);
    vi.stubEnv("FAKE_MODEL_SCENARIO", scenarioFile);
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi" as const, timeoutMs: 5_000 }, {
      workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-model.mjs"), runRoot: path.join(directory, "runs"),
    });
    managers.push(manager);
    const result = await manager.run({ task: "must run on requested model", model, thinking: "high", transport: "process" });
    // The manager may win the overall deadline and return a synthetic result
    // without logFile. Inspect the durable log at its known run location either way.
    const logFile = path.join(directory, "runs", result.id, "events.jsonl");
    const events = fs.readFileSync(logFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
    const frames = events.filter(event => event.type === "fake_received").map(event => event.frame);
    return { manager, result, frames, events };
  };

  it("overrides startup MRU and remembered reasoning before sending any task", async () => {
    const { result, frames, manager, events } = await run("success");
    expect(result).toMatchObject({ status: "completed", model: requested, requestedModel: requested, thinking: "high" });
    expect(frames.map(frame => frame.type)).toEqual(["get_state", "set_model", "set_thinking_level", "get_state", "prompt"]);
    expect(frames[1]).toMatchObject({ provider: "openai-codex", modelId: "gpt-5.6-sol" });
    expect(manager.listForUi()[0]).toMatchObject({ model: requested, thinking: "high" });

    const agentStarts = events.filter(event => event.type === "agent_start");
    const assistantAnswers = events.filter(event => event.type === "message_end" && event.message?.role === "assistant");
    expect(agentStarts).toHaveLength(1);
    expect(events.filter(event => event.type === "agent_end")).toHaveLength(1);
    expect(events.filter(event => event.type === "agent_settled")).toHaveLength(1);
    expect(assistantAnswers).toEqual([
      expect.objectContaining({
        type: "message_end",
        message: expect.objectContaining({
          role: "assistant",
          provider: "openai-codex",
          model: "gpt-5.6-sol",
          content: [{ type: "text", text: "correct model ran" }],
          stopReason: "stop",
          usage: { input: 2, output: 3, cacheRead: 0, cacheWrite: 0 },
        }),
      }),
    ]);
    expect(events.indexOf(agentStarts[0]!)).toBeLessThan(events.indexOf(assistantAnswers[0]!));
  });

  it.each(["reject", "reswitch", "malformed", "exit", "timeout", "startup-timeout"])("never sends work when admission fails: %s", async scenario => {
    const { result, frames } = await run(scenario);
    expect(["failed", "timed_out"]).toContain(result.status);
    expect(result.error).toMatch(/model|timed out/);
    expect(frames.some(frame => frame.type === "prompt")).toBe(false);
    expect(result.toolCalls).toBe(0);
    if (scenario === "startup-timeout" || scenario === "timeout") {
      expect(result.status).toBe("timed_out");
      expect(result.error).toContain("Agent timed out after 5000ms");
      expect(frames.map(frame => frame.type)).toEqual(scenario === "startup-timeout" ? ["get_state"] : ["get_state", "set_model"]);
    }
  }, 40_000);

  it("reports actual model drift through the result and UI, without erasing the failure", async () => {
    const { result, manager, events } = await run("drift");
    expect(result).toMatchObject({ status: "failed", model: "runinfra/glm-5-3-flash", requestedModel: requested });
    expect(result.error).toContain("terminating child");
    expect(manager.listForUi()[0]).toMatchObject({ status: "failed", model: "runinfra/glm-5-3-flash" });

    const agentStartIndices = events.flatMap((event, index) => event.type === "agent_start" ? [index] : []);
    const assistantMessageEndIndex = events.findIndex(event =>
      event.type === "message_end" && event.message?.role === "assistant",
    );
    expect(agentStartIndices).toHaveLength(2);
    expect(assistantMessageEndIndex).toBeGreaterThanOrEqual(0);
    expect(agentStartIndices[0]!).toBeLessThan(assistantMessageEndIndex);
    expect(agentStartIndices[1]!).toBeGreaterThan(assistantMessageEndIndex);
  });

  it("supports an exact bare model ID without using the startup default", async () => {
    const { result, frames } = await run("success", "gpt-5.6-sol");
    expect(result).toMatchObject({ status: "completed", model: requested });
    expect(frames.slice(0, 2).map(frame => frame.type)).toEqual(["get_state", "get_available_models"]);
  });

  it.each([
    { phase: "immediate startup", startupDelay: 0, selectionDelay: 0, concurrency: 1 },
    { phase: "slow startup", startupDelay: 16_000, selectionDelay: 0, concurrency: 1 },
    { phase: "concurrent slow model selection", startupDelay: 0, selectionDelay: 16_000, concurrency: 3 },
  ])("reasserts selection after a real Pi session_start hijack: $phase (offline)", async ({ startupDelay, selectionDelay, concurrency }) => {
    const directory = root();
    const agentDir = path.join(directory, "agent");
    fs.mkdirSync(agentDir);
    fs.writeFileSync(path.join(agentDir, "settings.json"), JSON.stringify({
      extensions: [path.resolve("tests/fixtures/model-hijack-extension.ts")],
      enableInstallTelemetry: false,
    }));
    vi.stubEnv("PI_CODING_AGENT_DIR", agentDir);
    vi.stubEnv("PI_OFFLINE", "1");
    vi.stubEnv("MODEL_PROBE_STARTUP_DELAY_MS", String(startupDelay));
    vi.stubEnv("MODEL_PROBE_SELECTION_DELAY_MS", String(selectionDelay));
    const manager = new AgentManager(directory, { ...DEFAULT_FABRIC_CONFIG.agents, runner: "pi" as const, maxConcurrent: concurrency, timeoutMs: 45_000 }, {
      workerPath,
      piBinary: path.resolve("node_modules/@earendil-works/pi-coding-agent/dist/cli.js"),
      fullCodeMode: false,
      fabricExtensionPath: path.resolve("dist/index.js"),
      runRoot: path.join(directory, "runs"),
    });
    managers.push(manager);
    const results = await Promise.all(Array.from({ length: concurrency }, () => manager.run({
      task: "probe model", model: "model-probe/requested", thinking: "high", extensions: true, transport: "process",
    })));
    for (const result of results) {
      const log = fs.readFileSync(path.join(directory, "runs", result.id, "events.jsonl"), "utf8");
      expect(result, `${result.error}\n${result.stderr}\n${log}`).toMatchObject({ status: "completed", model: "model-probe/requested", thinking: "high" });
      expect(result.text).toBe("model-probe/requested:high");
      expect(log).toContain("startup-hijacked:model-probe/mru");
      if (selectionDelay > 0) {
        expect(log).toContain("model-selection-waiting");
        expect(log).toContain("model-selection-completed");
        expect(log.indexOf("model-selection-completed")).toBeLessThan(log.indexOf('"type":"agent_start"'));
      }
    }
  }, 60_000);
});
