#!/usr/bin/env node
const emit = event => process.stdout.write(JSON.stringify(event) + "\n");
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += chunk.toString("utf8");
  while (buffer.includes("\n")) {
    const newline = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, newline));
    buffer = buffer.slice(newline + 1);
    if (frame.type === "get_state") {
      emit({ type: "response", id: frame.id, command: frame.type, success: true,
        data: { model: { provider: "openai-codex", id: "gpt-5.6-sol" }, thinkingLevel: "high" } });
    } else if (frame.type === "prompt") {
      const message = { role: "assistant", provider: "openai-codex", model: "gpt-5.6-sol", content: [] };
      const update = assistantMessageEvent => emit({ type: "message_update", message, assistantMessageEvent });
      emit({ type: "agent_start" });
      emit({ type: "message_start", message });
      update({ type: "toolcall_start", contentIndex: 0 });
      if (frame.message === "whitespace-time") {
        update({ type: "toolcall_delta", contentIndex: 0, delta: " " });
        setInterval(() => update({ type: "toolcall_delta", contentIndex: 0, delta: "\t" }), 5_000);
        continue;
      }
      if (frame.message === "oversized-normal") {
        update({ type: "toolcall_delta", contentIndex: 0, delta: " " });
        // This meaningful frame is intentionally dropped by the worker's 4 MiB line cap.
        update({ type: "toolcall_delta", contentIndex: 0, delta: '{"code":"' + "x".repeat(4 * 1024 * 1024) });
      }
      if (frame.message === "normal") update({ type: "toolcall_delta", contentIndex: 0, delta: '{"code":"' });
      for (let i = 0; i < 16; i++) update({ type: "toolcall_delta", contentIndex: 0, delta: " ".repeat(4096) });
      if (frame.message === "normal") update({ type: "toolcall_delta", contentIndex: 0, delta: '"}' });
      update({ type: "toolcall_end", contentIndex: 0 });
      emit({ type: "message_end", message: { ...message, stopReason: "stop" } });
      emit({ type: "agent_end" });
      emit({ type: "agent_settled" });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
