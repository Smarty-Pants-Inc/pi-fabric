// Offline RPC provider: advertise one admitted model, then fail its first inference turn.
import fs from "node:fs";
import readline from "node:readline";

const model = { provider: "fake", id: "b", api: "openai-completions", contextWindow: 100_000 };
const emit = event => process.stdout.write(JSON.stringify(event) + "\n");
const attempt = phase => fs.appendFileSync(process.env.MODEL_NOT_FOUND_ATTEMPTS, phase + "\n");
const errorMessage = process.env.MODEL_NOT_FOUND_ERROR;
const message = { role: "assistant", provider: model.provider, model: model.id, content: [],
  stopReason: "error", errorMessage, timestamp: 1, usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0 } };
const input = readline.createInterface({ input: process.stdin });
input.on("line", line => {
  const frame = JSON.parse(line);
  if (frame.type === "get_state") {
    emit({ type: "response", command: frame.type, id: frame.id, success: true, data: { model, thinkingLevel: "off" } });
  } else if (frame.type === "set_model" || frame.type === "set_thinking_level") {
    emit({ type: "response", command: frame.type, id: frame.id, success: true, data: model });
  } else if (frame.type === "prompt") {
    emit({ type: "response", command: frame.type, id: frame.id, success: true });
    attempt("first-turn");
    emit({ type: "agent_start" });
    emit({ type: "message_end", message });
    emit({ type: "turn_end", turnIndex: 0, message });
    emit({ type: "agent_end", willRetry: true });
    setTimeout(() => {
      attempt("retry");
      emit({ type: "auto_retry_start", errorMessage });
      const recovered = { ...message, stopReason: "stop", errorMessage: undefined, content: [{ type: "text", text: "recovered" }] };
      emit({ type: "message_end", message: recovered });
      emit({ type: "turn_end", turnIndex: 1, message: recovered });
      emit({ type: "auto_retry_end", success: true });
      emit({ type: "agent_end", willRetry: false });
      emit({ type: "agent_settled", outcome: "completed" });
    }, 500);
  }
});
input.on("close", () => process.exit(0));
