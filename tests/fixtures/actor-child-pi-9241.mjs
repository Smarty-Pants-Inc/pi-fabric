#!/usr/bin/env node
// smarty-dev#9241: a deterministic actor child for the real-Pi relaunch test. It speaks the
// Pi RPC model-admission handshake (get_state/set_model) and answers each prompt once.
import readline from "node:readline";
const send = (event) => process.stdout.write(JSON.stringify(event) + "\n");
let model = { provider: "relaunch-9241", id: "faux-1" };
let thinking = "off";
readline.createInterface({ input: process.stdin }).on("line", (line) => {
  if (!line.trim()) return;
  const frame = JSON.parse(line);
  const ok = (data) => send({ type: "response", id: frame.id, command: frame.type, success: true, ...(data === undefined ? {} : { data }) });
  if (frame.type === "get_state") return ok({ model, thinkingLevel: thinking, isStreaming: false, isCompacting: false,
    autoCompactionEnabled: false, autoCompactionDisabledForProcess: true });
  if (frame.type === "set_model") { model = { provider: frame.provider, id: frame.modelId }; return ok(model); }
  if (frame.type === "set_thinking_level") { thinking = frame.level; return ok(); }
  if (frame.type === "get_available_models") return ok({ models: [model] });
  if (frame.type !== "prompt") return ok();
  ok();
  const prompt = typeof frame.message === "string" ? frame.message : JSON.stringify(frame);
  const marker = /TELL-9241-[A-Z]+/.exec(prompt)?.[0] ?? "no-marker";
  const message = { role: "assistant", content: [{ type: "text", text: `handled ${marker}` }], provider: model.provider, model: model.id,
    usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0 }, stopReason: "stop" };
  send({ type: "agent_start" });
  send({ type: "message_end", message });
  send({ type: "turn_end", message, toolResults: [] });
  send({ type: "agent_end", messages: [message], willRetry: false });
  send({ type: "agent_settled" });
});
process.stdin.on("end", () => setTimeout(() => process.exit(0), 5));
