#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";

const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
let input = "";
process.stdin.on("data", (chunk) => {
  input += chunk.toString();
  const newline = input.indexOf("\n");
  if (newline < 0) return;
  const frame = JSON.parse(input.slice(0, newline));
  if (frame.type !== "prompt") return;
  process.stdin.removeAllListeners("data");
  const behavior = frame.message;
  if (behavior === "crash") process.exit(1);
  if (behavior !== "settled-no-result") {
    emit({ type: "message_end", message: { role: "assistant", content: "durable final result", stopReason: "stop" } });
  }
  if (behavior === "crash-before-settle") process.exit(1);
  // Both the leader and a descendant refuse TERM, so cleanup must target the
  // owned process group, not just the immediate Pi pid.
  if (behavior === "never-exit" && process.platform !== "win32") {
    const descendant = spawn(process.execPath, ["-e", 'process.on("SIGTERM", () => {}); console.log(JSON.stringify({type:"fake_descendant_pid",pid:process.pid})); setInterval(() => {}, 1000);'], { stdio: ["ignore", "inherit", "inherit"] });
    descendant.on("error", (error) => { throw error; });
  }
  emit({ type: "fake_child_pid", pid: process.pid });
  process.on("SIGTERM", () => {
    // This snapshot proves persistence happened BEFORE cleanup signalled Pi.
    const status = JSON.parse(fs.readFileSync(path.join(process.cwd(), process.env.PI_FABRIC_PARENT_RUN, "status.json"), "utf8"));
    emit({ type: "fake_term_snapshot", text: status.text, status: status.status, warnings: status.warnings });
  });
  process.stdin.on("end", () => {
    emit({ type: "fake_stdin_eof" });
    if (behavior === "slow-exit") setTimeout(() => process.exit(0), 10_000);
  });
  setInterval(() => {}, 1000);
  emit({ type: "agent_settled" });
});
