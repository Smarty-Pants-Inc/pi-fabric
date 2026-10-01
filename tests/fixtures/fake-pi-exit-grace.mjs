#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
let input = "";
process.stdin.on("data", async (chunk) => {
  input += chunk.toString();
  const newline = input.indexOf("\n");
  if (newline < 0) return;
  const frame = JSON.parse(input.slice(0, newline));
  if (frame.type !== "prompt") return;
  process.stdin.removeAllListeners("data");
  const behavior = frame.message;
  if (behavior === "crash") process.exit(1);
  const toolReply = behavior.startsWith("reply-");
  emit({ type: "fake_child_pid", pid: process.pid });
  process.on("SIGTERM", () => {
    // This snapshot proves persistence happened BEFORE cleanup signalled Pi.
    const status = JSON.parse(fs.readFileSync(path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "status.json"), "utf8"));
    emit({ type: "fake_term_snapshot", text: status.text, status: status.status, warnings: status.warnings });
  });
  process.stdin.on("end", () => {
    emit({ type: "fake_stdin_eof" });
    if (behavior === "slow-exit" || toolReply) setTimeout(() => process.exit(0), 10_000);
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
      emit({ type: "agent_settled" });
      // Exercise the grace-time check too, after EOF has requested disposal.
      await new Promise(resolve => process.stdin.once("end", resolve));
    }
    if (behavior !== "reply-missing") {
      fs.writeFileSync(replyFile, behavior === "reply-malformed" ? "{" : JSON.stringify({ action: behavior === "reply-invalid" ? "shout" : "silent" }));
      emit({ type: "message_end", message: { role: "toolResult", toolCallId: "reply-1", toolName: "fabric_reply", content: [{ type: "text", text: "Reply delivered." }] } });
    }
  } else if (behavior !== "settled-no-result") {
    emit({ type: "message_end", message: { role: "assistant", content: "durable final result", stopReason: "stop" } });
  }
  if (behavior === "crash-before-settle") process.exit(1);
  // Both the leader and a descendant refuse TERM, so cleanup must target the
  // owned process group, not just the immediate Pi pid.
  if (behavior === "never-exit" && process.platform !== "win32") {
    const descendant = spawn(process.execPath, ["-e", 'process.on("SIGTERM", () => {}); console.log(JSON.stringify({type:"fake_descendant_pid",pid:process.pid})); setInterval(() => {}, 1000);'], { stdio: ["ignore", "inherit", "inherit"] });
    descendant.on("error", (error) => { throw error; });
  }

  if (behavior !== "reply-after-settle") emit({ type: "agent_settled" });
});
