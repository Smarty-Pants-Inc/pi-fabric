import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const workerPath = path.resolve("dist/worker.js");

describe("real worker tool-call stream guard", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
    vi.unstubAllEnvs();
    for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
  });
  const run = async (task: string, timeoutMs = 5_000) => {
    // Deliberately not skipped without dist: the compiled worker is part of acceptance.
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-toolcall-stream-"));
    roots.push(root);
    const manager = new AgentManager(process.cwd(), { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs }, {
      workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-toolcall-stream.mjs"), runRoot: root,
    });
    managers.push(manager);
    const result = await manager.run({ task, thinking: "high", transport: "process" });
    const directory = path.join(root, result.id);
    const status = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
    const events = fs.readFileSync(path.join(directory, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    const lifecycle = fs.readFileSync(path.join(directory, "lifecycle.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    const evidenceDir = process.env.FABRIC_WHITESPACE_TEST_EVIDENCE_DIR;
    if (evidenceDir) {
      fs.mkdirSync(evidenceDir, { recursive: true });
      fs.writeFileSync(path.join(evidenceDir, task + "-result.json"), JSON.stringify(result, null, 2));
      for (const file of ["status.json", "events.jsonl", "lifecycle.jsonl"]) fs.copyFileSync(path.join(directory, file), path.join(evidenceDir, task + "-" + file));
    }
    return { manager, result, status, events, lifecycle };
  };
  const stalls = (events: Array<Record<string, any>>) => events.filter(event => event.type === "stall.whitespace-toolcall");
  const assertOneRetry = (events: Array<Record<string, any>>, runId: string) => {
    expect(events.filter(event => event.type === "fabric_whitespace_toolcall_retry")).toEqual([
      expect.objectContaining({ runId, taskId: runId, attempt: 1, maxAttempts: 1,
        model: "openai-codex/gpt-5.6-sol", effort: "high" }),
    ]);
    const prompts = events.filter(event => event.type === "fabric_fixture_command" && event.command === "prompt");
    expect(prompts).toHaveLength(2);
    const [first, second] = prompts;
    if (!first || !second) throw new Error("Expected exactly two native attempts");
    expect(second.message).toBe("Continue the task from the existing session. Do not repeat completed work.");
    expect(second.message).not.toBe(first.message);
    expect(second.pid).not.toBe(first.pid);
    expect(second.sessionFile).toBe(first.sessionFile);
    expect(prompts.map(event => [event.model, event.effort])).toEqual([
      ["openai-codex/gpt-5.6-sol", "high"], ["openai-codex/gpt-5.6-sol", "high"],
    ]);
    expect(events.some(event => event.type === "fabric_fixture_command" && event.command === "set_model" &&
      event.provider === "openai-codex" && event.modelId === "gpt-5.6-sol")).toBe(true);
    // A returned terminal result must follow native close/drain of both attempts.
    for (const event of prompts) expect(() => process.kill(event.pid, 0)).toThrow();
  };

  it("retries the 64 KiB prefix runaway once and propagates the repeated typed error", async () => {
    const { manager, result, status, events, lifecycle } = await run("whitespace");
    const expected = { status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" };
    expect(result).toMatchObject(expected);
    expect(status).toMatchObject(expected);
    expect(manager.listForUi()[0]).toMatchObject(expected);
    expect(result.error).toMatch(/^runaway: whitespace-only tool-call stream for [\d.]+s \/ 65536 bytes \(openai-codex\/gpt-5.6-sol, high\); whitespace tool-call stall repeated after one same-model retry; terminating child$/);
    expect(stalls(events)).toEqual([
      expect.objectContaining({ taskId: result.id, attempt: 1, action: "retry", bytes: 65536 }),
      expect.objectContaining({ taskId: result.id, attempt: 2, action: "failed", bytes: 65536 }),
    ]);
    expect(events.filter(event => event.type === "fabric_runaway_error")).toEqual([
      expect.objectContaining({ errorCode: expected.errorCode, error: result.error, bytes: 65536,
        model: "openai-codex/gpt-5.6-sol", effort: "high", contentIndex: 0 }),
    ]);
    expect(lifecycle.filter(event => event.event === "stall.whitespace-toolcall")).toHaveLength(2);
    assertOneRetry(events, result.id);
  });

  it("aborts each indefinite blank stream at the default 90s, retries once, then fails", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", undefined);
    const { result, status, events } = await run("whitespace-time", 210_000);
    expect(result).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    expect(status).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    expect(stalls(events)).toHaveLength(2);
    for (const stall of stalls(events)) {
      expect(stall.elapsedMs).toBeGreaterThanOrEqual(90_000);
      expect(stall.elapsedMs).toBeLessThan(95_000);
      expect(stall.bytes).toBeGreaterThan(0);
      expect(stall.bytes).toBeLessThan(65536);
    }
    expect(result.error).toContain("repeated after one same-model retry");
    assertOneRetry(events, result.id);
  }, 240_000);

  it.each(["whitespace-time-native", "prefix-time"])("honours the configured bound and fails the second stall (%s)", async task => {
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const { result, events } = await run(task);
    expect(result).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    expect(stalls(events)).toHaveLength(2);
    expect(stalls(events).every(event => event.elapsedMs >= 150 && event.elapsedMs < 1500)).toBe(true);
    assertOneRetry(events, result.id);
  });

  it("fails closed after draining a child that refuses abort without persisting the stalled turn", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const { result, events } = await run("whitespace-time-stubborn", 30_000);
    expect(result).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    expect(result.error).toContain("cannot safely retry");
    expect(stalls(events)).toHaveLength(1);
    expect(events.filter(event => event.type === "fabric_whitespace_toolcall_retry")).toHaveLength(0);
    const prompt = events.find(event => event.type === "fabric_fixture_command" && event.command === "prompt");
    expect(() => process.kill(prompt!.pid, 0)).toThrow();
  }, 40_000);

  it("completes when the one same-model retry makes progress", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const { result, status, events } = await run("whitespace-time-success");
    expect(result.status, result.error).toBe("completed");
    expect(result.error).toBeUndefined();
    expect(status.errorCode).toBeUndefined();
    expect(stalls(events)).toEqual([expect.objectContaining({ taskId: result.id, action: "retry", attempt: 1 })]);
    expect(events.some(event => event.type === "fabric_runaway_error")).toBe(false);
    assertOneRetry(events, result.id);
  });

  it("retries once on the exact session with Unicode separators in preserved native history", async () => {
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const task = "whitespace-time-success\u2028left\u2029right";
    const { result, events } = await run(task);
    expect(result.status, result.error).toBe("completed");
    expect(stalls(events)).toHaveLength(1);
    assertOneRetry(events, result.id);
    const prompt = events.find(event => event.type === "fabric_fixture_command" && event.command === "prompt");
    const entries = fs.readFileSync(prompt!.sessionFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
    expect(entries.filter(entry => entry.message?.role === "user" && entry.message.content === task)).toHaveLength(1);
    expect(entries.find(entry => entry.message?.role === "toolResult").message.content[0].text).toBe("completed\u2028tool\u2029output");
    expect(entries.find(entry => entry.message?.stopReason === "aborted").message.content[0].text).toBe("assistant\u2028text\u2029kept");
  });

  it.each(["mixed", "text-whitespace"])("does not abort healthy/unaffected deltas across multiple intervals (%s)", async task => {
    vi.stubEnv("PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS", "150");
    const { result, events } = await run(task);
    expect(result.status, result.error).toBe("completed");
    expect(stalls(events)).toHaveLength(0);
    expect(events.filter(event => event.type === "fabric_fixture_command" && event.command === "prompt")).toHaveLength(1);
  });

  it.each(["missing-session", "invalid-session", "header-only-session"])("fails clearly rather than replaying a task when its durable session is unsafe (%s)", async task => {
    const { result, events } = await run(task);
    expect(result).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    expect(result.error).toContain("cannot safely retry");
    expect(events.some(event => event.type === "fabric_whitespace_toolcall_retry")).toBe(false);
  });

  it("does not infer a runaway from whitespace after a dropped oversized meaningful argument event", async () => {
    const { result, events } = await run("oversized-normal");
    expect(result.status).toBe("completed");
    expect(result.error).toBeUndefined();
    expect(result.warnings).toEqual([expect.stringContaining("Dropped an oversized agent event line (message_update")]);
    expect(stalls(events)).toHaveLength(0);
  });

  it("does not abort a normal long tool call with more than 64 KiB of whitespace after JSON content", async () => {
    const { result, events } = await run("normal");
    expect(result.status).toBe("completed");
    expect(result.error).toBeUndefined();
    expect(stalls(events)).toHaveLength(0);
  });
});
