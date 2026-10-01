import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

const workerPath = path.resolve("dist/worker.js");

describe("real worker tool-call stream guard", () => {
  const roots: string[] = [];
  const managers: AgentManager[] = [];
  afterEach(async () => {
    await Promise.all(managers.splice(0).map(manager => manager.close()));
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
    const status = JSON.parse(fs.readFileSync(path.join(root, result.id, "status.json"), "utf8"));
    const events = fs.readFileSync(path.join(root, result.id, "events.jsonl"), "utf8").trim().split("\n").map(line => JSON.parse(line));
    return { manager, result, status, events };
  };

  it("aborts at 64 KiB and propagates the typed error to result, durable status, UI and run log", async () => {
    const { manager, result, status, events } = await run("whitespace");
    const expected = { status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" };
    expect(result).toMatchObject(expected);
    expect(status).toMatchObject(expected);
    expect(manager.listForUi()[0]).toMatchObject(expected);
    expect(result.error).toMatch(/^runaway: whitespace-only tool-call stream for [\d.]+s \/ 65536 bytes \(openai-codex\/gpt-5.6-sol, high\)$/);
    expect(events.filter(event => event.type === "fabric_runaway_error")).toEqual([
      expect.objectContaining({ errorCode: expected.errorCode, error: result.error, bytes: 65536,
        model: "openai-codex/gpt-5.6-sol", effort: "high", contentIndex: 0 }),
    ]);
  });

  it("aborts at 60 seconds of blank deltas and propagates the timed typed error", async () => {
    const { result, status, events } = await run("whitespace-time", 75_000);
    expect(result).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    expect(status).toMatchObject({ status: "failed", errorCode: "RUNAWAY_TOOL_CALL_STREAM" });
    const errors = events.filter(event => event.type === "fabric_runaway_error");
    expect(errors).toHaveLength(1);
    expect(errors[0]).toMatchObject({ error: result.error, model: "openai-codex/gpt-5.6-sol", effort: "high" });
    expect(errors[0].elapsedMs).toBeGreaterThanOrEqual(60_000);
    expect(errors[0].elapsedMs).toBeLessThan(65_000);
    expect(errors[0].bytes).toBeGreaterThan(0);
    expect(errors[0].bytes).toBeLessThan(65536);
  }, 90_000);

  it("does not infer a runaway from whitespace after a dropped oversized meaningful argument event", async () => {
    const { result, events } = await run("oversized-normal");
    expect(result.status).toBe("completed");
    expect(result.error).toBeUndefined();
    expect(result.warnings).toEqual([expect.stringContaining("Dropped an oversized agent event line (message_update")]);
    expect(events.some(event => event.type === "fabric_runaway_error")).toBe(false);
  });

  it("does not abort a normal long tool call with more than 64 KiB of whitespace after JSON content", async () => {
    const { result, events } = await run("normal");
    expect(result.status).toBe("completed");
    expect(result.error).toBeUndefined();
    expect(events.some(event => event.type === "fabric_runaway_error")).toBe(false);
  });
});
