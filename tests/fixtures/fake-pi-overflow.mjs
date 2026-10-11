#!/usr/bin/env node
// smarty-dev#3238: an offline Pi RPC child for actor context-overflow tests. It speaks the
// RPC frames the worker uses and appends Pi-shaped entries to its --session journal.
// The task text selects the run: OVERFLOW_ALWAYS refuses every run with the window error
// text the worker reports, OVERFLOW_ON_HISTORY refuses only a run whose session already
// holds a conversation, FILL_CONTEXT succeeds with 90% of the window used.
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { StringDecoder } from "node:string_decoder";

const WINDOW = 272_000;
const OVERFLOW = "Context exceeds window: estimated 272511 input tokens, window 272000";
const args = process.argv.slice(2);
const arg = (name) => (args.includes(name) ? args[args.indexOf(name) + 1] : undefined);
const sessionFile = arg("--session");
const [provider, ...rest] = (arg("--model") ?? "test/overflow").split("/");
const model = { provider, id: rest.join("/"), contextWindow: WINDOW };
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const entries = () => sessionFile && fs.existsSync(sessionFile)
  ? fs.readFileSync(sessionFile, "utf8").split("\n").filter(Boolean).map((line) => {
    try { return JSON.parse(line); } catch { return {}; }
  })
  : [];
const append = (message) => {
  if (sessionFile) fs.appendFileSync(sessionFile, JSON.stringify({
    type: "message", id: randomUUID().slice(0, 8), parentId: null, timestamp: new Date().toISOString(), message,
  }) + "\n");
};
const decoder = new StringDecoder("utf8");
let buffer = "";
process.stdin.on("data", (chunk) => {
  buffer += decoder.write(chunk);
  while (buffer.includes("\n")) {
    const end = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, end));
    buffer = buffer.slice(end + 1);
    const reply = (data) => emit({ type: "response", id: frame.id, command: frame.type, success: true, data });
    if (frame.type === "get_state") {
      reply({ model, thinkingLevel: "off", isStreaming: false, isCompacting: false });
    } else if (frame.type === "get_available_models") {
      reply({ models: [model] });
    } else if (frame.type === "set_model") {
      reply(model);
    } else if (frame.type === "set_thinking_level") {
      reply();
    } else if (frame.type === "prompt") {
      const task = String(frame.message ?? "");
      const journal = entries();
      const history = journal.some((entry) => entry.type === "message");
      const overflow = task.includes("OVERFLOW_ALWAYS") || (task.includes("OVERFLOW_ON_HISTORY") && history);
      if (process.env.FAKE_PI_LOG) {
        fs.appendFileSync(process.env.FAKE_PI_LOG, JSON.stringify({
          history, overflow, session: journal.find((entry) => entry.type === "session")?.id,
          marker: (task.match(/\b(?:OVERFLOW_ALWAYS|OVERFLOW_ON_HISTORY|FILL_CONTEXT|[A-Z]+_REVIEW)\b/g) ?? []).join(","),
        }) + "\n");
      }
      append({ role: "user", content: task, timestamp: Date.now() });
      emit({ type: "agent_start" });
      if (overflow) {
        const message = { role: "assistant", provider, model: model.id, content: [], stopReason: "error", errorMessage: OVERFLOW,
          usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0 }, timestamp: Date.now() };
        append(message);
        emit({ type: "message_end", message });
        emit({ type: "agent_end" });
        continue;
      }
      const input = task.includes("FILL_CONTEXT") ? Math.floor(WINDOW * 0.9) : 1000;
      const message = { role: "assistant", provider, model: model.id, content: [{ type: "text", text: "verdict: ok" }], stopReason: "stop",
        usage: { input, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: input + 10 }, timestamp: Date.now() };
      append(message);
      emit({ type: "message_start", message });
      emit({ type: "message_end", message });
      emit({ type: "agent_end" });
      emit({ type: "agent_settled", outcome: "completed" });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
