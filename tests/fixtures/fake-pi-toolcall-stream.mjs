#!/usr/bin/env node
import fs from "node:fs";
import { randomUUID } from "node:crypto";
const emit = event => process.stdout.write(JSON.stringify(event) + "\n");
const args = process.argv.slice(2);
const sessionFile = args[args.indexOf("--session") + 1];
const persistent = args.includes("--session");
const fixtureFile = persistent ? sessionFile + ".fixture.json" : undefined;
let prior = fixtureFile && fs.existsSync(fixtureFile) ? JSON.parse(fs.readFileSync(fixtureFile, "utf8")) : undefined;
// Simulate a changed launcher default on restart: the worker must explicitly
// re-admit the first attempt's model and effort rather than inherit this one.
let model = prior ? { provider: "fixture", id: "wrong-default" } : { provider: "openai-codex", id: "gpt-5.6-sol" };
let effort = prior ? "low" : "high";
let buffer = "";
let streamTimer;
let finishTimer;
let activeMessage;
const finish = (message, text = "fixture completed") => {
  clearInterval(streamTimer);
  emit({ type: "message_end", message: { ...message, content: [{ type: "text", text }], stopReason: "stop" } });
  emit({ type: "agent_end" });
  emit({ type: "agent_settled", outcome: "completed" });
};
process.stdin.on("data", chunk => {
  buffer += chunk.toString("utf8");
  while (buffer.includes("\n")) {
    const newline = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    emit({ type: "fabric_fixture_command", command: frame.type, message: frame.message,
      pid: process.pid, sessionFile, model: `${model.provider}/${model.id}`, effort,
      provider: frame.provider, modelId: frame.modelId, level: frame.level });
    if (frame.type === "get_state") {
      emit({ type: "response", id: frame.id, command: frame.type, success: true,
        data: { model, thinkingLevel: effort, isStreaming: false, isCompacting: false } });
    } else if (frame.type === "set_model") {
      model = { provider: frame.provider, id: frame.modelId };
      emit({ type: "response", id: frame.id, command: frame.type, success: true, data: model });
    } else if (frame.type === "set_thinking_level") {
      effort = frame.level;
      emit({ type: "response", id: frame.id, command: frame.type, success: true });
    } else if (frame.type === "prompt") {
      const task = prior?.task ?? frame.message;
      const attempt = (prior?.attempt ?? 0) + 1;
      if (persistent && task !== "missing-session") {
        if (!fs.existsSync(sessionFile)) fs.writeFileSync(sessionFile, JSON.stringify({ type: "session", version: 3, id: randomUUID(), timestamp: new Date().toISOString(), cwd: process.cwd() }) + "\n");
        fs.writeFileSync(fixtureFile, JSON.stringify({ task, attempt }));
      }
      prior = { task, attempt };
      const message = { role: "assistant", provider: model.provider, model: model.id, content: [] };
      activeMessage = message;
      const field = task.endsWith("-native") ? "event" : "assistantMessageEvent";
      const update = stream => emit({ type: "message_update", message, [field]: stream });
      const delta = text => update({ type: "toolcall_delta", contentIndex: 0, delta: text });
      emit({ type: "agent_start" });
      emit({ type: "message_start", message });
      if (task === "whitespace-time-success" && attempt === 2) {
        finish(message);
        continue;
      }
      update({ type: "toolcall_start", contentIndex: 0 });
      const timeout = Number(process.env.PI_FABRIC_TOOL_CALL_WHITESPACE_TIMEOUT_MS ?? 90_000);
      if (task.startsWith("whitespace-time") || task === "prefix-time") {
        if (task === "prefix-time") delta('{"code":');
        delta(" ");
        streamTimer = setInterval(() => delta("\t\n"), Math.max(5, Math.floor(timeout / 10)));
        continue;
      }
      if (task === "mixed") {
        delta('{"code":"');
        let ticks = 0;
        streamTimer = setInterval(() => {
          ticks++;
          delta(ticks % 2 ? " \t" : "real content");
        }, Math.max(5, Math.floor(timeout / 4)));
        finishTimer = setTimeout(() => {
          delta('"}');
          update({ type: "toolcall_end", contentIndex: 0 });
          finish(message);
        }, timeout * 4);
        continue;
      }
      if (task === "text-whitespace") {
        // No argument deltas: whitespace text must not arm the guard.
        streamTimer = setInterval(() => update({ type: "text_delta", contentIndex: 1, delta: " \t\n" }), Math.max(5, Math.floor(timeout / 10)));
        finishTimer = setTimeout(() => finish(message), timeout * 3);
        continue;
      }
      if (task === "oversized-normal") {
        delta(" ");
        // This meaningful frame is intentionally dropped by the worker's 4 MiB line cap.
        delta('{"code":"' + "x".repeat(4 * 1024 * 1024));
      }
      if (task === "normal") delta('{"code":"');
      for (let i = 0; i < 16; i++) delta(" ".repeat(4096));
      if (task === "normal") delta('"}');
      update({ type: "toolcall_end", contentIndex: 0 });
      finish(message);
    }
  }
});
process.on("SIGTERM", () => {
  if (prior?.task === "whitespace-time-stubborn") {
    emit({ type: "agent_settled", outcome: "aborted" });
    return; // Keep streaming and refuse SIGTERM: only SIGKILL releases this child.
  }
  clearInterval(streamTimer);
  clearTimeout(finishTimer);
  // Exercise late shutdown frames: these are not a second error or successful
  // completion. Usage still belongs to the aborted attempt.
  if (activeMessage) {
    emit({ type: "message_end", message: { ...activeMessage, stopReason: "aborted", errorMessage: "fixture abort",
      usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, cost: { total: 0 } } } });
    emit({ type: "agent_end" });
    emit({ type: "agent_settled", outcome: "aborted" });
  }
  process.exit(0);
});
process.stdin.on("end", () => {
  if (prior?.task === "whitespace-time-stubborn") return;
  clearInterval(streamTimer);
  clearTimeout(finishTimer);
  process.exit(0);
});
