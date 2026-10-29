#!/usr/bin/env node

const TOOL_CALL_ID = "tool-finality-1";
const FINAL_TEXT = "\t  Final café 🌙  \n";
const emit = event => process.stdout.write(`${JSON.stringify(event)}\n`);
const emitAssistantEnd = (content, stopReason) => emit({ type: "message_end", message: {
  role: "assistant", content, stopReason, usage: { output: 1 },
} });

let input = "";
let started = false;
const runPrompt = behavior => {
  emit({ type: "fake_child_pid", pid: process.pid });
  emit({ type: "message_start", message: { role: "assistant" } });
  emitAssistantEnd([{ type: "toolCall", id: TOOL_CALL_ID, name: "fixture_tool", arguments: {} }], "toolUse");
  emit({ type: "tool_execution_start", toolCallId: TOOL_CALL_ID, toolName: "fixture_tool", args: {} });

  const completed = behavior !== "outstanding-no-answer";
  if (completed) {
    const content = [{ type: "text", text: "synthetic tool result" }];
    emit({ type: "tool_execution_end", toolCallId: TOOL_CALL_ID, toolName: "fixture_tool",
      result: { content }, isError: false });
    emit({ type: "message_end", message: { role: "toolResult", toolCallId: TOOL_CALL_ID,
      toolName: "fixture_tool", content, isError: false } });
  }

  if (behavior === "resolved-final-answer") {
    emit({ type: "message_start", message: { role: "assistant" } });
    emitAssistantEnd([{ type: "text", text: FINAL_TEXT }], "stop");
  }
  emit({ type: "agent_end", willRetry: false });
  emit({ type: "agent_settled", outcome: "completed" });
};

const acceptLine = line => {
  if (!line.trim()) return;
  let frame;
  try { frame = JSON.parse(line); }
  catch { return; }
  if (frame.type !== "prompt") return;
  if (started) {
    emit({ type: "fake_follow_up_prompt" });
    return;
  }
  started = true;
  runPrompt(frame.message);
};

process.stdin.on("data", chunk => {
  input += chunk.toString();
  let newline;
  while ((newline = input.indexOf("\n")) >= 0) {
    const line = input.slice(0, newline);
    input = input.slice(newline + 1);
    acceptLine(line);
  }
});
process.stdin.on("end", () => {
  emit({ type: "fake_stdin_eof" });
  process.stdout.end(() => process.exit(0));
});
