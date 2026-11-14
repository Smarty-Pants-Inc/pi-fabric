import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { directiveSchema } from "../src/actors/manager.js";
import { AgentManager } from "../src/agents/manager.js";
import { DEFAULT_FABRIC_CONFIG } from "../src/config.js";

// Source by default for a regression against HEAD before building; the same
// assertions also run against dist via FABRIC_EXIT_GRACE_WORKER.
const workerPath = path.resolve(process.env.FABRIC_EXIT_GRACE_WORKER ?? "src/worker.ts");
const roots: string[] = [];
const managers: AgentManager[] = [];
afterEach(async () => {
  await Promise.all(managers.splice(0).map(manager => manager.close()));
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});
const run = async (behavior: string, schema?: Record<string, unknown>, replyTool = false) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "fabric-exit-grace-"));
  roots.push(root);
  const manager = new AgentManager(root, { ...DEFAULT_FABRIC_CONFIG.agents, timeoutMs: 60_000 }, {
    workerPath, piBinary: path.resolve("tests/fixtures/fake-pi-exit-grace.mjs"), runRoot: root,
  });
  managers.push(manager);
  // The fake child's signal handler reads the durable record in its cwd.
  const result = await manager.run({ task: behavior, cwd: root, transport: "process", ...(schema ? { schema } : {}), ...(replyTool ? { replyTool } : {}) });
  return { manager, result, root };
};
const readEvents = (logFile: string) => fs.readFileSync(logFile, "utf8").trim().split("\n").map(line => JSON.parse(line));
const readLifecycle = (logFile: string) => {
  const file = path.join(path.dirname(logFile), "lifecycle.jsonl");
  return fs.existsSync(file) ? fs.readFileSync(file, "utf8").trim().split("\n").filter(Boolean).map(line => JSON.parse(line)) : [];
};
const assistantTelemetryFor = (behavior: string) => {
  if (behavior === "length-without-prose") return { lastAssistantStopReason: "length", lastAssistantBlockCounts: { thinking: 1 }, lastAssistantTextBytes: 0 };
  if (behavior === "length-with-prose") return { lastAssistantStopReason: "length", lastAssistantBlockCounts: { text: 1 }, lastAssistantTextBytes: Buffer.byteLength("answer truncated at length", "utf8") };
  if (behavior === "unresolved-tool-use") {
    const text = "Prose 🌙 before an unresolved tool call.";
    return { lastAssistantStopReason: "toolUse", lastAssistantBlockCounts: { text: 1, toolCall: 1 }, lastAssistantTextBytes: Buffer.byteLength(text, "utf8") };
  }
  return { lastAssistantStopReason: "stop", lastAssistantBlockCounts: { thinking: 1 }, lastAssistantTextBytes: 0 };
};
const expectAssistantTelemetry = (logFile: string, expected: ReturnType<typeof assistantTelemetryFor>, settleOutcome?: string): void => {
  const telemetry = { ...expected, ...(settleOutcome ? { settleOutcome } : {}) };
  const status = JSON.parse(fs.readFileSync(path.join(path.dirname(logFile), "status.json"), "utf8"));
  expect(status.lastAssistantStopReason).toBe(expected.lastAssistantStopReason);
  expect(status.lastAssistantBlockCounts).toEqual(expected.lastAssistantBlockCounts);
  expect(status.lastAssistantTextBytes).toBe(expected.lastAssistantTextBytes);
  if (settleOutcome) expect(status.settleOutcome).toBe(settleOutcome);
  else expect(status).not.toHaveProperty("settleOutcome");

  const settled = readLifecycle(logFile).reverse().find(event => event.event === "pi.agent_settled");
  expect(settled?.data).toMatchObject(telemetry);
  expect(settled?.data.lastAssistantBlockCounts).toEqual(expected.lastAssistantBlockCounts);
  expect(settled?.data.lastAssistantTextBytes).toBe(expected.lastAssistantTextBytes);
  if (settleOutcome) expect(settled?.data.settleOutcome).toBe(settleOutcome);
  else expect(settled?.data).not.toHaveProperty("settleOutcome");
  expect(settled?.data).not.toHaveProperty("text");
  expect(settled?.data).not.toHaveProperty("content");
  const serialized = JSON.stringify(settled?.data);
  for (const content of ["reasoning only; no final text", "answer truncated at length", "Prose 🌙 before an unresolved tool call."]) {
    expect(serialized).not.toContain(content);
  }
};
const expectNoCompletedSettlementReceipt = (logFile: string): void => {
  const file = path.join(path.dirname(logFile), "settlement.json");
  if (fs.existsSync(file)) expect(JSON.parse(fs.readFileSync(file, "utf8"))).not.toMatchObject({ outcome: "completed" });
};
const isRunning = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    // A killed orphan may await init's reap on Linux; a zombie cannot run.
    if (process.platform === "linux") return !/^\d+ \(.*\) Z /.test(fs.readFileSync(`/proc/${pid}/stat`, "utf8"));
    return true;
  } catch { return false; }
};

describe("settled Pi exit grace", () => {
  it.each(["slow-exit", "never-exit"])("preserves the final result and warns once for %s", async behavior => {
    const { manager, result } = await run(behavior);
    expect(result.status).toBe("completed");
    expect(result.text).toBe("durable final result");
    expect(result.error).toBeUndefined();
    expect(result.warnings).toEqual([expect.stringContaining("did not exit after stdin closed for 5000ms")]);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([
      { type: "agent_settled", ...(behavior === "never-exit" ? {} : { outcome: "completed" }) },
    ]);
    expect(events.filter(event => event.type === "worker_warning")).toHaveLength(1);
    expect(events.some(event => event.type === "fabric_recovery_error")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    const snapshot = events.find(event => event.type === "fake_term_snapshot");
    // Do not publish terminal status until pipe draining and validation finish:
    // the manager can immediately retain/remove terminal run artifacts.
    // Windows SIGTERM is unconditional termination; there is no handler probe.
    if (process.platform !== "win32") {
      expect(snapshot).toMatchObject({ status: "running", text: result.text, warnings: result.warnings });
    }
    const durable = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(durable).toMatchObject({ status: "completed", text: result.text, warnings: result.warnings });
    expect(manager.listForUi()[0]).toMatchObject({ status: "completed" });
    for (const event of events.filter(event => ["fake_child_pid", "fake_descendant_pid"].includes(event.type))) {
      expect(isRunning(event.pid)).toBe(false);
    }
    if (behavior === "never-exit" && process.platform !== "win32") {
      expect(events.filter(event => event.type === "fake_descendant_pid")).toHaveLength(1);
    }
  }, 45_000);

  it.each([
    ["settle-error", "error", false],
    ["settle-aborted", "aborted", false],
    ["reply-settle-error", "error", true],
    ["reply-settle-aborted", "aborted", true],
  ] as const)("keeps unsuccessful settlement failed after slow exit for %s", async (behavior, outcome, replyTool) => {
    const { manager, result } = await run(behavior, replyTool ? directiveSchema : undefined, replyTool);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("did not exit after stdin closed for 5000ms");
    expect(result.text).toBe(replyTool ? "" : "durable final result");
    expect(result.value).toBeUndefined();
    expect(result.replyVia).toBeUndefined();
    expect(result.warnings ?? []).toEqual([]);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome }]);
    expect(events.find(event => event.type === "compaction_end")).toMatchObject({
      reason: "threshold", aborted: outcome === "aborted", willRetry: false,
      ...(outcome === "error" ? { errorMessage: "Auto-compaction failed: fixture failure" } : {}),
    });
    expect(events.filter(event => event.type === "message_end" && event.message.role === "assistant")
      .every(event => !["error", "aborted"].includes(event.message.stopReason))).toBe(true);
    if (replyTool) {
      expect(JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "reply.json"), "utf8"))).toEqual({ action: "silent" });
    }
    expect(events.filter(event => event.type === "worker_warning")).toHaveLength(0);
    expect(events.filter(event => event.type === "fabric_recovery_error")).toHaveLength(1);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    const durable = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(durable.status).toBe("failed");
    expect(manager.listForUi()[0]).toMatchObject({ status: "failed" });
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
  }, 45_000);

  it.each(["crash", "crash-before-settle"])("keeps %s failed even if text was recorded", async behavior => {
    const { result } = await run(behavior);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Pi exited with code 1");
    expect(result.warnings ?? []).toEqual([]);
  });

  it("keeps a process task that never settles failed despite recorded text and an 8s lifetime", async () => {
    const { manager, result } = await run("never-settles");
    expect(result.status).toBe("failed");
    expect(result.text).toBe("durable final result");
    expect(result.error).toContain("Pi exited with code 1");
    expect(result.warnings ?? []).toEqual([]);
    const events = readEvents(result.logFile!);
    expect(events.some(event => event.type === "agent_settled")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(false);
    expect(fs.existsSync(path.join(path.dirname(result.logFile!), "settlement.json"))).toBe(false);
    expect(manager.listForUi()[0]).toMatchObject({status: "failed"});
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
  }, 45_000);

  it("still validates a structured result after slow-exit cleanup", async () => {
    const { result } = await run("slow-exit", { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"] });
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(/^Structured agent output was invalid:/);
    expect(result.text).toBe("durable final result");
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it.each(["reply-slow-exit", "reply-after-settle"])("preserves a tool-only reply written after assistant consumption for %s", async behavior => {
    const { result } = await run(behavior, directiveSchema, true);
    expect(result).toMatchObject({ status: "completed", value: { action: "silent" }, replyVia: "tool", text: "" });
    expect(result.error).toBeUndefined();
    expect(result.warnings).toEqual([expect.stringContaining("did not exit after stdin closed for 5000ms")]);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "fake_assistant_consumed")).toEqual([
      { type: "fake_assistant_consumed", replyExists: false },
    ]);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome: "completed" }]);
    expect(events.filter(event => event.type === "worker_warning")).toHaveLength(1);
    expect(events.some(event => event.type === "fabric_recovery_error")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    const durable = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(durable).toMatchObject({ status: "completed", value: result.value, warnings: result.warnings });
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
  }, 45_000);

  it.each([
    ["thinking-only-explicit-exit", "NO_FINAL_TEXT", ["stop"], "completed", "clean"],
    ["thinking-only-omitted-exit", "NO_FINAL_TEXT", ["stop"], undefined, "clean"],
    ["thinking-only-stuck-eof", "NO_FINAL_TEXT", ["stop"], "completed", "stuck"],
    ["thinking-only-omitted-stuck-eof", "NO_FINAL_TEXT", ["stop"], undefined, "stuck"],
    ["length-without-prose", "NON_FINAL_STOP_REASON", ["length"], "completed", "clean"],
    ["length-with-prose", "NON_FINAL_STOP_REASON", ["length"], "completed", "clean"],
    ["unresolved-tool-use", "UNRESOLVED_TOOL_CALLS", ["toolUse"], "completed", "clean"],
    ["prior-prose-thinking-only", "NO_FINAL_TEXT", ["stop", "stop"], "completed", "clean"],
  ] as const)("fails settled output without an eligible final answer for %s", async (behavior, errorCode, stopReasons, outcome, exitMode) => {
    const { result } = await run(behavior);
    expect(result).toMatchObject({ status: "failed", errorCode });
    expect(result.error).toBeTruthy();
    if (exitMode === "clean") expect(result.exitCode).toBe(0);

    const events = readEvents(result.logFile!);
    const assistantEnds = events.filter(event => event.type === "message_end" && event.message.role === "assistant");
    expect(assistantEnds.map(event => event.message.stopReason)).toEqual(stopReasons);
    expect(events.filter(event => event.type === "agent_end")).toEqual([{ type: "agent_end", willRetry: false }]);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([
      { type: "agent_settled", ...(outcome ? { outcome } : {}) },
    ]);
    const latestAssistantEnd = events.map(event => event.type === "message_end" && event.message.role === "assistant")
      .lastIndexOf(true);
    expect(events.findIndex(event => event.type === "agent_end")).toBeGreaterThan(latestAssistantEnd);
    expect(events.findIndex(event => event.type === "agent_settled")).toBeGreaterThan(events.findIndex(event => event.type === "agent_end"));
    expect(events.filter(event => event.type === "fake_child_pid")).toHaveLength(1);
    expect(events.some(event => event.type === "fake_follow_up_prompt")).toBe(false);
    if (behavior === "prior-prose-thinking-only") {
      expect(assistantEnds[0].message.content).toEqual([{ type: "text", text: "earlier progress prose" }]);
      expect(assistantEnds[1].message.content).toEqual([{ type: "thinking", thinking: "reasoning only; no final text" }]);
    }
    if (behavior === "unresolved-tool-use") {
      expect(events.some(event => event.type === "tool_execution_end" && event.toolCallId === "unresolved-1")).toBe(false);
    }
    if (exitMode === "stuck") {
      expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
      for (const event of events.filter(event => ["fake_child_pid", "fake_descendant_pid"].includes(event.type))) {
        expect(isRunning(event.pid)).toBe(false);
      }
    }
    expectNoCompletedSettlementReceipt(result.logFile!);
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8")))
      .toMatchObject({ status: "failed", errorCode });
    expectAssistantTelemetry(result.logFile!, assistantTelemetryFor(behavior), outcome);
  }, 45_000);

  it("keeps a real outstanding tool execution unresolved across a new assistant start", async () => {
    const { manager, result } = await run("unresolved-tool-use-before-final");
    expect(result).toMatchObject({ status: "failed", errorCode: "UNRESOLVED_TOOL_CALLS", exitCode: 0 });
    expect(result.error).toContain("tool calls remain unresolved");
    const events = readEvents(result.logFile!);
    const assistantEnds = events.filter(event => event.type === "message_end" && event.message.role === "assistant");
    expect(assistantEnds.map(event => event.message.stopReason)).toEqual(["toolUse", "stop"]);
    const firstAssistantEnd = events.findIndex(event => event.type === "message_end" && event.message.role === "assistant");
    const toolStart = events.findIndex(event => event.type === "tool_execution_start" && event.toolCallId === "pending-before-final");
    const nextAssistantStart = events.findIndex(event => event.type === "message_start" && event.message.role === "assistant");
    const finalAssistantEnd = events.reduce((lastIndex, event, index) =>
      event.type === "message_end" && event.message.role === "assistant" ? index : lastIndex, -1);
    expect(toolStart).toBeGreaterThan(firstAssistantEnd);
    expect(nextAssistantStart).toBeGreaterThan(toolStart);
    expect(finalAssistantEnd).toBeGreaterThan(nextAssistantStart);
    expect(events.some(event => event.type === "tool_execution_end" && event.toolCallId === "pending-before-final")).toBe(false);
    expect(events.filter(event => event.type === "agent_end")).toEqual([{ type: "agent_end", willRetry: false }]);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome: "completed" }]);
    expect(events.some(event => event.type === "fake_follow_up_prompt" || event.type === "fabric_provider_resume")).toBe(false);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    expectNoCompletedSettlementReceipt(result.logFile!);
    expect(JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8")))
      .toMatchObject({ status: "failed", errorCode: "UNRESOLVED_TOOL_CALLS" });
    const child = events.find(event => event.type === "fake_child_pid");
    expect(child).toBeTruthy();
    expect(isRunning(child.pid)).toBe(false);
    expect(manager.listForUi()[0]).toMatchObject({ status: "failed", errorCode: "UNRESOLVED_TOOL_CALLS" });
  }, 45_000);

  it("fails a clean exit with no assistant message or settlement as incomplete", async () => {
    const { result } = await run("no-assistant-clean-exit");
    expect(result).toMatchObject({ status: "failed", errorCode: "NO_FINAL_TEXT", exitCode: 0 });
    const events = readEvents(result.logFile!);
    expect(events.some(event => event.type === "message_end" && event.message.role === "assistant")).toBe(false);
    expect(events.some(event => event.type === "agent_end" || event.type === "agent_settled")).toBe(false);
    expect(events.filter(event => event.type === "fake_child_pid")).toHaveLength(1);
    expect(events.some(event => event.type === "fake_follow_up_prompt")).toBe(false);
    expectNoCompletedSettlementReceipt(result.logFile!);
    const status = JSON.parse(fs.readFileSync(path.join(path.dirname(result.logFile!), "status.json"), "utf8"));
    expect(status).not.toHaveProperty("lastAssistantStopReason");
    expect(status).not.toHaveProperty("lastAssistantBlockCounts");
    expect(status).not.toHaveProperty("lastAssistantTextBytes");
    expect(status).not.toHaveProperty("settleOutcome");
    expect(readLifecycle(result.logFile!).some(event => event.event === "pi.agent_settled")).toBe(false);
  }, 45_000);

  it("does not reuse an earlier final after a new assistant turn only streams partial text", async () => {
    const { result } = await run("prior-prose-partial-clean-exit");
    expect(result).toMatchObject({ status: "failed", errorCode: "NO_FINAL_TEXT", exitCode: 0 });
    const events = readEvents(result.logFile!);
    const priorEnd = events.findIndex(event => event.type === "message_end" && event.message.role === "assistant");
    const nextStart = events.findIndex(event => event.type === "message_start" && event.message.role === "assistant");
    const partial = events.findIndex(event => event.type === "message_update" && event.assistantMessageEvent?.delta === "partial latest answer");
    expect(priorEnd).toBeGreaterThanOrEqual(0);
    expect(nextStart).toBeGreaterThan(priorEnd);
    expect(partial).toBeGreaterThan(nextStart);
    expect(events[priorEnd].message.content).toEqual([{ type: "text", text: "earlier eligible answer" }]);
    expect(events.filter(event => event.type === "fake_child_pid")).toHaveLength(1);
    expect(events.some(event => event.type === "agent_settled")).toBe(false);
    expect(events.some(event => event.type === "fake_follow_up_prompt")).toBe(false);
    expectNoCompletedSettlementReceipt(result.logFile!);
  }, 45_000);

  it.each([
    ["error-lies-completed", "error"],
    ["aborted-lies-completed", "aborted"],
  ] as const)("does not let a completed settlement override assistant %s with exit 0", async (behavior, stopReason) => {
    const { result } = await run(behavior);
    expect(result).toMatchObject({ status: "failed", exitCode: 0 });
    expect(result.error).toContain(`Fixture ${stopReason} provider error`);
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "message_end" && event.message.role === "assistant")
      .map(event => event.message.stopReason)).toEqual([stopReason]);
    expect(events.filter(event => event.type === "agent_end")).toEqual([{ type: "agent_end", willRetry: false }]);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome: "completed" }]);
    expectNoCompletedSettlementReceipt(result.logFile!);
  }, 45_000);

  it.each([
    ["settle-error-clean-exit", "error"],
    ["settle-aborted-clean-exit", "aborted"],
  ] as const)("does not let unsuccessful %s settle complete on exit 0 after valid text", async (behavior, outcome) => {
    const { result } = await run(behavior);
    expect(result).toMatchObject({ status: "failed", text: "durable final result", exitCode: 0 });
    expect(result.error).toBeTruthy();
    const events = readEvents(result.logFile!);
    expect(events.filter(event => event.type === "message_end" && event.message.role === "assistant")
      .map(event => event.message.stopReason)).toEqual(["stop"]);
    expect(events.filter(event => event.type === "agent_end")).toEqual([{ type: "agent_end", willRetry: false }]);
    expect(events.filter(event => event.type === "agent_settled")).toEqual([{ type: "agent_settled", outcome }]);
    expect(events.find(event => event.type === "compaction_end")).toMatchObject({
      reason: "threshold", aborted: outcome === "aborted", willRetry: false,
      ...(outcome === "error" ? { errorMessage: "Auto-compaction failed: fixture failure" } : {}),
    });
    expectNoCompletedSettlementReceipt(result.logFile!);
  }, 45_000);

  it.each([
    ["legacy-thinking-empty-receipt", "NO_FINAL_TEXT", "stop", { thinking: 1 }, 0, ""],
    ["legacy-whitespace-receipt", "NO_FINAL_TEXT", "stop", { text: 1 }, Buffer.byteLength(" \t\n ", "utf8"), " \t\n "],
    ["legacy-unresolved-tool-use-receipt", "UNRESOLVED_TOOL_CALLS", "toolUse", { text: 1, toolCall: 1 }, Buffer.byteLength("Old answer 🌙 before an unresolved tool call.", "utf8"), "stale nonblank answer"],
    ["legacy-valid-unicode-receipt", undefined, "stop", { text: 1 }, Buffer.byteLength("Final café 🌙", "utf8"), "Final café 🌙"],
  ] as const)("worker independently gates legacy final-answer receipt %s", async (behavior, errorCode, stopReason, blockCounts, textBytes, receiptText) => {
    const { result } = await run(behavior);
    const valid = errorCode === undefined;
    expect(result.status).toBe(valid ? "completed" : "failed");
    if (errorCode) expect(result.errorCode).toBe(errorCode);
    else expect(result.text).toBe(receiptText);

    const directory = path.dirname(result.logFile!);
    const receipt = JSON.parse(fs.readFileSync(path.join(directory, "final-answer.json"), "utf8"));
    expect(receipt).toMatchObject({ version: 1, id: expect.any(String), runId: result.id, recordedAt: expect.any(Number), text: receiptText });
    expect(receipt.assistant).toBeUndefined(); // These model prior worker receipts without native evidence.
    const events = readEvents(result.logFile!);
    expect(events).toContainEqual(expect.objectContaining({ type: "fabric_final_answer", runId: result.id, receiptId: receipt.id }));
    const status = JSON.parse(fs.readFileSync(path.join(directory, "status.json"), "utf8"));
    expect(status).toMatchObject({
      status: valid ? "completed" : "failed",
      ...(errorCode ? { errorCode } : {}),
      lastAssistantStopReason: stopReason,
      lastAssistantBlockCounts: blockCounts,
      lastAssistantTextBytes: textBytes,
    });
    expect(events.filter(event => event.type === "message_end" && event.message.role === "assistant")
      .map(event => event.message.stopReason)).toEqual([stopReason]);
    expect(events.filter(event => event.type === "fake_child_pid")).toHaveLength(1);
    expect(events.some(event => event.type === "fake_stdin_eof")).toBe(true);
    expect(events.some(event => event.type === "fake_follow_up_prompt")).toBe(false);
    for (const event of events.filter(event => event.type === "fake_child_pid")) expect(isRunning(event.pid)).toBe(false);
    if (errorCode) expectNoCompletedSettlementReceipt(result.logFile!);
    if (behavior === "legacy-unresolved-tool-use-receipt") {
      expect(events.some(event => event.type === "tool_execution_end" && event.toolCallId === "legacy-unresolved-1")).toBe(false);
    }
  }, 45_000);

  it("fails closed when the explicit-success receipt cannot be persisted", async () => {
    const { result } = await run("receipt-unwritable");
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Cannot persist Pi settlement receipt");
    expect(result.warnings ?? []).toEqual([]);
  }, 45_000);

  it("keeps a missing tool-only reply failed after slow exit", async () => {
    const { result } = await run("reply-missing", directiveSchema, true);
    expect(result.status).toBe("failed");
    expect(result.error).toContain("Directive reply missing");
    expect(result.value).toBeUndefined();
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it.each([
    ["reply-invalid", /^Structured agent output was invalid:/],
    ["reply-malformed", /^Directive reply missing:/],
  ] as const)("keeps %s failed during post-drain reply validation", async (behavior, error) => {
    const { result } = await run(behavior, directiveSchema, true);
    expect(result.status).toBe("failed");
    expect(result.error).toMatch(error);
    expect(result.warnings).toHaveLength(1);
  }, 45_000);

  it("reports a settled child without any assistant result as incomplete", async () => {
    const { result } = await run("settled-no-result");
    expect(result).toMatchObject({ status: "failed", errorCode: "NO_FINAL_TEXT" });
    expectNoCompletedSettlementReceipt(result.logFile!);
  }, 45_000);
});
