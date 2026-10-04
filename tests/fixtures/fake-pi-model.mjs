#!/usr/bin/env node
import fs from "node:fs";
import { StringDecoder } from "node:string_decoder";

const emit = event => process.stdout.write(JSON.stringify(event) + "\n");
const requested = { provider: "openai-codex", id: "gpt-5.6-sol" };
const wrong = { provider: "runinfra", id: "glm-5-3-flash" };
// Simulate an MRU extension replacing the --model selection during startup.
let model = wrong;
let thinkingLevel = "low";
let behavior = "success";
const taskFile = process.env.FAKE_MODEL_SCENARIO;
if (taskFile) behavior = fs.readFileSync(taskFile, "utf8");
const activation = behavior.startsWith("activation:") ? behavior.slice("activation:".length) : undefined;
if (activation) {
  emit({ type: "fake_argv", argv: process.argv.slice(2) });
  if (activation === "exit") process.exit(0);
  if (activation !== "missing-hook" && activation !== "timeout") emit({
    type: "fabric_activation_window_ready", runId: activation === "wrong-run" ? "wrong" : process.env.PI_FABRIC_PARENT_RUN,
    nonce: activation === "wrong-nonce" ? "wrong" : process.env.PI_FABRIC_ACTIVATION_NONCE,
    policy: activation === "wrong-policy" ? "full-history" : "activation",
    hook: activation === "wrong-hook" ? "/wrong/hook" : process.env.PI_FABRIC_ACTIVATION_HOOK,
    protocol: activation === "wrong-protocol" ? 2 : 1,
  });
}
const decoder = new StringDecoder("utf8");
let buffer = "";
process.stdin.on("data", chunk => {
  buffer += decoder.write(chunk);
  while (buffer.includes("\n")) {
    const index = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, index));
    buffer = buffer.slice(index + 1);
    emit({ type: "fake_received", frame });
    if (behavior === "startup-timeout" || activation === "timeout") continue;
    const reply = (data, success = true) => emit({ type: "response", id: frame.id, command: frame.type, success, data, ...(success ? {} : { error: "model unavailable" }) });
    if (frame.type === "get_available_models") {
      reply({ models: [requested, wrong] });
    } else if (frame.type === "set_model") {
      if (behavior === "exit") process.exit(0);
      if (behavior === "timeout") continue;
      if (behavior === "reject") { reply(undefined, false); continue; }
      model = { provider: frame.provider, id: frame.modelId };
      thinkingLevel = "max"; // model_select restores a remembered level.
      reply(model);
      if (behavior === "reswitch") model = wrong;
    } else if (frame.type === "set_thinking_level") {
      thinkingLevel = ["effort-lower", "effort-downgrade"].includes(behavior) ? "low" : behavior === "effort-off" ? "off" : behavior === "effort-missing" ? undefined : behavior === "effort-malformed" ? "invalid" : frame.level;
      reply();
    } else if (frame.type === "get_state") {
      reply(behavior === "malformed" ? {} : {
        model, ...(behavior === "effort-missing" ? {} : { thinkingLevel: behavior === "effort-malformed" ? "turbo" : thinkingLevel }), isStreaming: false, isCompacting: false,
        ...(activation ? {
          autoCompactionEnabled: activation === "still-enabled",
          ...(activation === "ignored-flag" ? {} : { autoCompactionDisabledForProcess: true }),
        } : {}),
      });
    } else if (frame.type === "prompt") {
      emit({ type: "agent_start" });
      const actual = behavior === "drift" ? wrong : model;
      const message = { role: "assistant", provider: actual.provider, model: actual.id, content: [{ type: "text", text: behavior === "refusal" ? "I refuse this judgment." : "correct model ran" }], stopReason: "stop", usage: { input: 2, output: 3, cacheRead: 0, cacheWrite: 0 } };
      emit({ type: "message_start", message });
      emit({ type: "message_end", message });
      if (behavior === "judge-reply") {
        // Simulate the host reply hook's durable tool receipt, not final-text JSON.
        const packet = JSON.parse(frame.message.split("\n")[1]);
        const value = { verdict: "dependency", confidence: .8, evidenceLinks: packet.evidenceRefs.map(ref => ref.url),
          nextAction: { kind: "wait_dependency", owner: "dev-lead", targetRef: packet.itemRef } };
        fs.writeFileSync(process.env.PI_FABRIC_REPLY_FILE, JSON.stringify(value));
        emit({ type: "tool_execution_end", toolName: "fabric_reply", result: { content: [{ type: "text", text: "Reply delivered." }] } });
        if (process.env.FAKE_JUDGE_LAUNCH_FILE) fs.appendFileSync(process.env.FAKE_JUDGE_LAUNCH_FILE,
          JSON.stringify({ cwd: process.cwd(), replyFile: process.env.PI_FABRIC_REPLY_FILE }) + "\n");
      }
      // Late events must never erase the model mismatch failure.
      emit({ type: "agent_start" });
      emit({ type: "agent_end" });
      emit({ type: "agent_settled" });
    }
  }
});
process.stdin.on("end", () => process.exit(0));
