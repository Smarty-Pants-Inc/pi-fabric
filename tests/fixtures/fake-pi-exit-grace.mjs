#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const emitAssistantEnd = (content, stopReason, details = {}) => emit({ type: "message_end", message: {
  role: "assistant", content, stopReason, usage: { output: 1 }, ...details,
} });
let input = "";
process.stdin.on("data", async (chunk) => {
  input += chunk.toString();
  const newline = input.indexOf("\n");
  if (newline < 0) return;
  const frame = JSON.parse(input.slice(0, newline));
  if (frame.type !== "prompt") return;
  let trailingInput = input.slice(newline + 1);
  const observeFollowUpFrames = (chunk) => {
    trailingInput += chunk.toString();
    let nextNewline;
    while ((nextNewline = trailingInput.indexOf("\n")) >= 0) {
      const line = trailingInput.slice(0, nextNewline);
      trailingInput = trailingInput.slice(nextNewline + 1);
      try {
        if (JSON.parse(line).type === "prompt") emit({ type: "fake_follow_up_prompt" });
      } catch { /* Ignore unrelated or incomplete control frames. */ }
    }
  };
  process.stdin.removeAllListeners("data");
  process.stdin.on("data", observeFollowUpFrames);
  observeFollowUpFrames("");
  const behavior = frame.message;
  if (behavior === "crash") process.exit(1);
  const toolReply = behavior.startsWith("reply-");
  const noAnswerBehaviors = new Set([
    "thinking-only-explicit-exit", "thinking-only-omitted-exit", "thinking-only-stuck-eof",
    "thinking-only-omitted-stuck-eof", "length-without-prose", "length-with-prose",
    "unresolved-tool-use", "prior-prose-thinking-only",
  ]);
  const noSettlementBehaviors = new Set(["no-assistant-clean-exit", "prior-prose-partial-clean-exit"]);
  const legacyReceiptCases = new Map([
    ["legacy-thinking-empty-receipt", { content: [{ type: "thinking", thinking: "reasoning only" }], stopReason: "stop", receiptText: "" }],
    ["legacy-whitespace-receipt", { content: [{ type: "text", text: " \t\n " }], stopReason: "stop", receiptText: " \t\n " }],
    ["legacy-unresolved-tool-use-receipt", { content: [
      { type: "text", text: "Old answer 🌙 before an unresolved tool call." },
      { type: "toolCall", id: "legacy-unresolved-1", name: "fixture_tool", arguments: {} },
    ], stopReason: "toolUse", receiptText: "stale nonblank answer" }],
    ["legacy-valid-unicode-receipt", { content: [{ type: "text", text: "Final café 🌙" }], stopReason: "stop", receiptText: "Final café 🌙" }],
  ]);
  const lyingSuccessStops = new Map([["error-lies-completed", "error"], ["aborted-lies-completed", "aborted"]]);
  const cleanExitBehaviors = new Set([
    "thinking-only-explicit-exit", "thinking-only-omitted-exit", "length-without-prose",
    "length-with-prose", "unresolved-tool-use", "prior-prose-thinking-only",
    "no-assistant-clean-exit", "prior-prose-partial-clean-exit",
    "settle-error-clean-exit", "settle-aborted-clean-exit", ...lyingSuccessStops.keys(),
  ]);
  // Keep omitted legacy outcomes distinct from explicit native success. The
  // clean-exit unsuccessful-settlement cases catch exit-code fallback too.
  const outcome = behavior.endsWith("-error") || behavior === "settle-error-clean-exit" ? "error"
    : behavior.endsWith("-aborted") || behavior === "settle-aborted-clean-exit" ? "aborted"
    : ["never-exit", "settled-no-result", "thinking-only-omitted-exit", "thinking-only-omitted-stuck-eof"].includes(behavior) ? undefined : "completed";
  const unsuccessfulSettlement = outcome === "error" || outcome === "aborted";
  emit({ type: "fake_child_pid", pid: process.pid });
  process.on("SIGTERM", () => {
    // This snapshot proves persistence happened BEFORE cleanup signalled Pi.
    const status = JSON.parse(fs.readFileSync(path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "status.json"), "utf8"));
    const receipt = path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "settlement.json");
    emit({ type: "fake_term_snapshot", text: status.text, status: status.status, warnings: status.warnings,
      ...(fs.existsSync(receipt) && fs.statSync(receipt).isFile() ? { receipt: JSON.parse(fs.readFileSync(receipt, "utf8")) } : {}) });
  });
  process.stdin.on("end", () => {
    const receipt = path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "settlement.json");
    emit({ type: "fake_stdin_eof", ...(fs.existsSync(receipt) && fs.statSync(receipt).isFile()
      ? { receipt: JSON.parse(fs.readFileSync(receipt, "utf8")) } : {}) });
    if (legacyReceiptCases.has(behavior) || behavior === "unresolved-tool-use-before-final") {
      process.stdout.write("", () => process.exit(0));
    }
    if (!cleanExitBehaviors.has(behavior) && (behavior === "slow-exit" || toolReply || unsuccessfulSettlement)) {
      setTimeout(() => process.exit(0), 10_000);
    }
  });
  setInterval(() => {}, 1000);
  if (toolReply) {
    const replyFile = process.env.PI_FABRIC_REPLY_FILE;
    const statusFile = path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "status.json");
    emit({ type: "message_end", message: {
      role: "assistant", content: [{ type: "toolCall", id: "reply-1", name: "fabric_reply", arguments: { action: "silent" } }],
      stopReason: "toolUse", usage: { output: 1 },
    } });
    // A durable usage sentinel proves the worker consumed message_end before
    // the tool writes reply.json. A fixed delay would leave the race unproven.
    const deadline = Date.now() + 10_000;
    while (JSON.parse(fs.readFileSync(statusFile, "utf8")).usage.output !== 1) {
      if (Date.now() >= deadline) throw new Error("Worker did not consume the assistant event");
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    emit({ type: "fake_assistant_consumed", replyExists: fs.existsSync(replyFile) });
    if (behavior === "reply-after-settle") {
      emit({ type: "agent_settled", outcome });
      // Exercise the grace-time check too, after EOF has requested disposal.
      await new Promise(resolve => process.stdin.once("end", resolve));
    }
    if (behavior !== "reply-missing") {
      fs.writeFileSync(replyFile, behavior === "reply-malformed" ? "{" : JSON.stringify({ action: behavior === "reply-invalid" ? "shout" : "silent" }));
      emit({ type: "message_end", message: { role: "toolResult", toolCallId: "reply-1", toolName: "fabric_reply", content: [{ type: "text", text: "Reply delivered." }] } });
    }
  } else if (legacyReceiptCases.has(behavior)) {
    const legacy = legacyReceiptCases.get(behavior);
    const runId = process.env.PI_FABRIC_PARENT_RUN;
    if (!runId) throw new Error("Legacy receipt fixture has no parent run ID");
    const receiptId = `legacy-${process.pid}`;
    emitAssistantEnd(legacy.content, legacy.stopReason);
    fs.writeFileSync(path.join(process.cwd(), runId, "final-answer.json"), JSON.stringify({
      version: 1, id: receiptId, runId, recordedAt: Date.now(), text: legacy.receiptText,
    }));
    emit({ type: "fabric_final_answer", runId, receiptId });
    emit({ type: "agent_end", willRetry: false });
  } else if (behavior === "prior-prose-thinking-only") {
    emitAssistantEnd([{ type: "text", text: "earlier progress prose" }], "stop");
    emit({ type: "message_start", message: { role: "assistant" } });
    emitAssistantEnd([{ type: "thinking", thinking: "reasoning only; no final text" }], "stop");
  } else if (behavior === "prior-prose-partial-clean-exit") {
    emitAssistantEnd([{ type: "text", text: "earlier eligible answer" }], "stop");
    emit({ type: "message_start", message: { role: "assistant" } });
    emit({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "partial latest answer" } });
  } else if (behavior === "unresolved-tool-use-before-final") {
    emitAssistantEnd([{ type: "toolCall", id: "pending-before-final", name: "fixture_tool", arguments: {} }], "toolUse");
    emit({ type: "tool_execution_start", toolCallId: "pending-before-final", toolName: "fixture_tool", args: {} });
    emit({ type: "message_start", message: { role: "assistant" } });
    emitAssistantEnd([{ type: "text", text: "new final text after pending tool" }], "stop");
  } else if (noSettlementBehaviors.has(behavior)) {
    // Deliberately end without any assistant message or native settlement frame.
  } else if (noAnswerBehaviors.has(behavior)) {
    const message = behavior === "length-with-prose"
      ? { content: [{ type: "text", text: "answer truncated at length" }], stopReason: "length" }
      : behavior === "unresolved-tool-use"
        ? { content: [
          { type: "text", text: "Prose 🌙 before an unresolved tool call." },
          { type: "toolCall", id: "unresolved-1", name: "fixture_tool", arguments: {} },
        ], stopReason: "toolUse" }
        : { content: [{ type: "thinking", thinking: "reasoning only; no final text" }],
          stopReason: behavior === "length-without-prose" ? "length" : "stop" };
    emitAssistantEnd(message.content, message.stopReason);
  } else if (lyingSuccessStops.has(behavior)) {
    const stopReason = lyingSuccessStops.get(behavior);
    emitAssistantEnd([{ type: "text", text: `Partial output before ${stopReason}.` }], stopReason, {
      errorMessage: `Fixture ${stopReason} provider error`,
    });
  } else if (behavior !== "settled-no-result") {
    emitAssistantEnd("durable final result", "stop");
  }
  if (behavior === "crash-before-settle") process.exit(1);
  if (behavior === "never-settles") {
    // Recording text and staying alive beyond the exit grace is NOT settlement.
    // No successful receipt or EOF-disposal request may be inferred from it.
    setTimeout(() => process.exit(1), 8_000);
    return;
  }
  // Both the leader and a descendant refuse TERM, so cleanup must target the
  // owned process group, not just the immediate Pi pid.
  if (["never-exit", "thinking-only-stuck-eof", "thinking-only-omitted-stuck-eof"].includes(behavior) && process.platform !== "win32") {
    const descendant = spawn(process.execPath, ["-e", 'process.on("SIGTERM", () => {}); console.log(JSON.stringify({type:"fake_descendant_pid",pid:process.pid})); setInterval(() => {}, 1000);'], { stdio: ["ignore", "inherit", "inherit"] });
    descendant.on("error", (error) => { throw error; });
  }

  if (unsuccessfulSettlement) {
    // Automatic compaction can fail/abort after a valid assistant response
    // without emitting another error-bearing assistant message_end.
    emit({ type: "compaction_start", reason: "threshold" });
    emit({ type: "compaction_end", reason: "threshold", aborted: outcome === "aborted", willRetry: false,
      ...(outcome === "error" ? { errorMessage: "Auto-compaction failed: fixture failure" } : {}) });
    emit({ type: "agent_end", willRetry: false });
  } else if (noAnswerBehaviors.has(behavior) || lyingSuccessStops.has(behavior) || behavior === "unresolved-tool-use-before-final") {
    emit({ type: "agent_end", willRetry: false });
  }
  if (behavior === "receipt-unwritable") fs.mkdirSync(path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "settlement.json"));
  if (behavior !== "reply-after-settle" && !noSettlementBehaviors.has(behavior)) emit({ type: "agent_settled", outcome });
  if (cleanExitBehaviors.has(behavior)) process.stdout.write("", () => process.exit(0));
});
