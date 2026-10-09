#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const mode = process.env.FAKE_PI_REPORT_MODE ?? "partial";
const error = process.env.FAKE_PI_REPORT_ERROR ?? "stream disconnected before completion";
const session = process.argv[process.argv.indexOf("--session") + 1];
const statusFile = path.join(path.dirname(session), "status.json");
const emit = event => process.stdout.write(JSON.stringify(event) + "\n");
const previous = "Tool work finished: patched worker and tests.";
const final = "FINAL: completed the patch. 🦄";
process.stdin.resume();
process.stdin.on("data", () => {});
fs.appendFileSync(process.env.FAKE_PI_REPORT_LAUNCHES, "launch\n");

// Read the actual worker record while streaming, not a record written by the fake.
const snapshot = async (field, value) => {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const record = JSON.parse(fs.readFileSync(statusFile, "utf8"));
    if (record[field] === value && record.status === "running") {
      fs.appendFileSync(process.env.FAKE_PI_REPORT_SNAPSHOTS, JSON.stringify(record) + "\n");
      return;
    }
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  throw new Error(`Worker did not persist ${field}=${value}`);
};

emit({ type: "agent_start" });
const tools = !["no-output", "tool-less"].includes(mode);
if (tools) {
  emit({ type: "message_start", message: { role: "assistant", content: [] } });
  if (mode !== "no-text") {
    emit({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: previous }], stopReason: "toolUse" } });
    await snapshot("lastCompleteText", previous);
  }
  emit({ type: "tool_execution_start", toolName: "fabric_exec", toolCallId: "completed-work", args: {} });
  if (mode !== "unfinished-tool") emit({ type: "tool_execution_end", toolName: "fabric_exec", toolCallId: "completed-work", result: { content: [{ type: "text", text: "checked" }] }, isError: false });
  if (!["unfinished-tool", "unfinished-turn"].includes(mode)) emit({ type: "turn_end", toolResults: [] });
}
// A real durable session makes retry-on-close eligible. Only positive cases need
// this fence probe; negative cases preserve the current terminal failure path.
if (["partial", "legacy", "fallback", "raw-cut", "normal", "reply"].includes(mode)) {
  fs.writeFileSync(session, JSON.stringify({ type: "session", version: 3, id: "fake-report-session", timestamp: new Date().toISOString(), cwd: process.cwd() }) + "\n");
}
if (mode === "reply" && process.env.PI_FABRIC_REPLY_FILE) fs.writeFileSync(process.env.PI_FABRIC_REPLY_FILE, JSON.stringify({ ok: true }));
emit({ type: "message_start", message: { role: "assistant", content: [] } });
if (!["fallback", "no-output", "no-text"].includes(mode)) {
  if (mode === "legacy") {
    emit({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: "FINAL: completed" } });
    await snapshot("partialText", "FINAL: completed");
    emit({ type: "message_update", assistantMessageEvent: { type: "thinking_delta", delta: "not output" } });
    emit({ type: "message_update", assistantMessageEvent: { type: "toolcall_delta", delta: "not output either" } });
    emit({ type: "message_update", assistantMessageEvent: { type: "text_delta", delta: " the patch. 🦄" } });
  } else {
    emit({ type: "message_update", message: { role: "assistant", content: [{ type: "text", text: "FINAL: completed" }] }, assistantMessageEvent: { type: "text_delta", delta: "FINAL: completed" } });
    await snapshot("partialText", "FINAL: completed");
    emit({ type: "message_update", message: { role: "assistant", content: [{ type: "text", text: final }] }, event: { type: "text_delta", delta: " the patch. 🦄" } });
  }
  await snapshot("partialText", final);
}
if (mode === "raw-cut") {
  // No message_end at all: stdout must be drained and stderr retained.
  process.stderr.write(error, () => process.exit(1));
} else {
  emit({ type: "message_end", message: { role: "assistant", content: mode === "normal" ? [{ type: "text", text: final }] : [], stopReason: mode === "normal" ? "stop" : "error", ...(mode === "normal" ? {} : { errorMessage: error }) } });
  emit({ type: "agent_end", messages: [], willRetry: false });
  process.stdout.write(JSON.stringify({ type: "agent_settled", outcome: mode === "normal" ? "completed" : "error" }) + "\n", () => process.exit(mode === "normal" ? 0 : 1));
}
