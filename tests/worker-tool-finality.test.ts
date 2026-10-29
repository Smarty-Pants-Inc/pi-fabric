import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Source by default; the same production-worker/process-transport regression
// can target a later bundle with FABRIC_TOOL_FINALITY_WORKER.
const workerPath = path.resolve(process.env.FABRIC_TOOL_FINALITY_WORKER ?? "src/worker.ts");
const toolCallId = "tool-finality-1";
const finalText = "\t  Final café 🌙  \n";
const roots: string[] = [];
const managers: AgentManager[] = [];

afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) {
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  }
});

const run = async (behavior: string) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-tool-finality-"));
  roots.push(root);
  const manager = new AgentManager(root, {
    ...DEFAULT_FABRIC_CONFIG.agents,
    timeoutMs: 60_000,
  }, {
    workerPath,
    piBinary: path.resolve("tests/fixtures/fake-tool-finality.mjs"),
    runRoot: path.join(root, "runs"),
  });
  managers.push(manager);
  const result = await manager.run({ task: behavior, cwd: root, transport: "process" });
  return { manager, result };
};

type Event = Record<string, any>;
const readEvents = (logFile: string): Event[] => fs.readFileSync(logFile, "utf8").trim().split("\n")
  .filter(Boolean).map(line => JSON.parse(line));
const statusFor = (logFile: string): Event => JSON.parse(fs.readFileSync(path.join(path.dirname(logFile), "status.json"), "utf8"));
const isRunning = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    // A killed orphan may await init's reap on Linux; a zombie cannot run.
    if (process.platform === "linux") return !/^\d+ \(.*\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8"));
    return true;
  } catch { return false; }
};

const expectOneAttemptAndClosedChildren = (events: Event[], result: Awaited<ReturnType<typeof run>>["result"], stops: string[]): void => {
  expect(events.filter(event => event.type === "message_end" && event.message?.role === "assistant")
    .map(event => event.message.stopReason)).toEqual(stops);
  expect(events.filter(event => event.type === "agent_end")).toEqual([{ type: "agent_end", willRetry: false }]);
  expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome: "completed" }]);
  expect(events.filter(event => event.type === "fake_follow_up_prompt")).toHaveLength(0);
  expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);

  // One process-transport worker and one synthetic Pi child; both must be
  // closed before the public run result is accepted.
  const piChildren = events.filter(event => event.type === "fake_child_pid");
  expect(piChildren).toHaveLength(1);
  expect(isRunning(piChildren[0]!.pid)).toBe(false);
  expect(result.sessionId).toMatch(/^\d+$/);
  expect(isRunning(Number(result.sessionId))).toBe(false);
};

const expectToolCallLifecycle = (events: Event[], completed: boolean): void => {
  const assistantCallIndex = events.findIndex(event => event.type === "message_end" && event.message?.role === "assistant");
  const starts = events.filter(event => event.type === "tool_execution_start");
  const ends = events.filter(event => event.type === "tool_execution_end");
  const results = events.filter(event => event.type === "message_end" && event.message?.role === "toolResult");

  expect(events[assistantCallIndex]?.message?.stopReason).toBe("toolUse");
  expect(events[assistantCallIndex]?.message?.content).toEqual([
    { type: "toolCall", id: toolCallId, name: "fixture_tool", arguments: {} },
  ]);
  expect(starts).toEqual([expect.objectContaining({ type: "tool_execution_start", toolCallId, toolName: "fixture_tool" })]);
  if (completed) {
    expect(ends).toEqual([expect.objectContaining({ type: "tool_execution_end", toolCallId, toolName: "fixture_tool", isError: false })]);
    expect(results).toEqual([expect.objectContaining({
      type: "message_end",
      message: expect.objectContaining({ role: "toolResult", toolCallId, toolName: "fixture_tool", isError: false }),
    })]);
    const startIndex = events.findIndex(event => event.type === "tool_execution_start" && event.toolCallId === toolCallId);
    const endIndex = events.findIndex(event => event.type === "tool_execution_end" && event.toolCallId === toolCallId);
    const resultIndex = events.findIndex(event => event.type === "message_end" && event.message?.role === "toolResult" && event.message.toolCallId === toolCallId);
    expect(startIndex).toBeGreaterThan(assistantCallIndex);
    expect(endIndex).toBeGreaterThan(startIndex);
    expect(resultIndex).toBeGreaterThan(endIndex);
  } else {
    expect(ends).toHaveLength(0);
    expect(results).toHaveLength(0);
    expect(events.findIndex(event => event.type === "tool_execution_start" && event.toolCallId === toolCallId))
      .toBeGreaterThan(assistantCallIndex);
  }
};

const expectNoSuccessfulSettlementReceipt = (logFile: string): void => {
  expect(fs.existsSync(path.join(path.dirname(logFile), "settlement.json"))).toBe(false);
};

describe("settled Pi tool-call finality", () => {
  // Keep reason: only a production worker through ProcessTransport can prove
  // the post-settlement no-answer classification while owned child processes drain.
  it.each([
    ["resolved-no-answer", "NON_FINAL_STOP_REASON"],
    ["outstanding-no-answer", "UNRESOLVED_TOOL_CALLS"],
  ] as const)("keeps %s failed with the cause supported by actual tool evidence", async (behavior, errorCode) => {
    const { result } = await run(behavior);
    expect(result).toMatchObject({ status: "failed", errorCode, exitCode: 0 });
    expect(result.error).toBeTruthy();
    if (errorCode === "UNRESOLVED_TOOL_CALLS") expect(result.error).toContain("tool calls remain unresolved");
    else expect(result.error).toContain("latest assistant stop reason is not final");

    const events = readEvents(result.logFile!);
    expectOneAttemptAndClosedChildren(events, result, ["toolUse"]);
    expectToolCallLifecycle(events, behavior === "resolved-no-answer");
    expectNoSuccessfulSettlementReceipt(result.logFile!);
    expect(statusFor(result.logFile!)).toMatchObject({ status: "failed", errorCode, exitCode: 0 });
  }, 45_000);

  it("accepts a final assistant after the completed tool and preserves its exact UTF-8 bytes", async () => {
    const { manager, result } = await run("resolved-final-answer");
    expect(result).toMatchObject({ status: "completed", exitCode: 0, text: finalText });
    expect(result.error).toBeUndefined();
    expect(Buffer.from(result.text, "utf8")).toEqual(Buffer.from(finalText, "utf8"));

    const events = readEvents(result.logFile!);
    expectOneAttemptAndClosedChildren(events, result, ["toolUse", "stop"]);
    expectToolCallLifecycle(events, true);
    const assistantEnds = events.filter(event => event.type === "message_end" && event.message?.role === "assistant");
    expect(assistantEnds[1]!.message.content).toEqual([{ type: "text", text: finalText }]);

    const status = statusFor(result.logFile!);
    expect(status).toMatchObject({ status: "completed", exitCode: 0, text: finalText });
    expect(Buffer.from(status.text, "utf8")).toEqual(Buffer.from(finalText, "utf8"));
    const settlement = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "settlement.json"), "utf8"));
    expect(settlement).toMatchObject({ runId: result.id, outcome: "completed", text: finalText });
    expect(Buffer.from(settlement.text, "utf8")).toEqual(Buffer.from(finalText, "utf8"));
    expect(manager.listForUi()[0]).toMatchObject({ status: "completed" });
  }, 45_000);
});
