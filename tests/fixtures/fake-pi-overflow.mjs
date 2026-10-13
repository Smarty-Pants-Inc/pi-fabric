#!/usr/bin/env node
// smarty-dev#3238: an offline Pi RPC child for actor context-overflow tests. It speaks the
// RPC frames the worker uses and appends Pi-shaped entries to its --session journal.
// The task text selects the run: OVERFLOW_ALWAYS refuses every run with the window error
// text the worker reports, OVERFLOW_ON_HISTORY refuses only a run whose session already
// holds a conversation, FILL_CONTEXT succeeds with 90% of the window used.
// Round 2: PROVIDER_OVERFLOW_ON_HISTORY refuses a carried run in a provider's own wording (typed by
// pi-ai isContextOverflow), QUOTED_PHRASE fails with a non-overflow error that quotes the phrase.
// Round 3: SILENT_OVERFLOW answers "stop" with usage above the window (pi-ai isContextOverflow case 2;
// Pi keeps such an answer without retrying). TOOL_THEN_OVERFLOW_ON_HISTORY runs one side-effecting
// tool (logged to FAKE_PI_LOG.tools) and then overflows on the next model turn of a carried session.
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { StringDecoder } from "node:string_decoder";
import { pathToFileURL } from "node:url";

const WINDOW = 272_000;
const OVERFLOW = "Context exceeds window: estimated 272511 input tokens, window 272000";
const PROVIDER_OVERFLOW = "prompt is too long: 280000 tokens > 272000 maximum";
const QUOTED = 'Tool "read" failed: the log line says "Context exceeds window: estimated 272511 input tokens, window 272000"';
const args = process.argv.slice(2);
const arg = (name) => (args.includes(name) ? args[args.indexOf(name) + 1] : undefined);
const sessionFile = arg("--session");
const [provider, ...rest] = (arg("--model") ?? "test/overflow").split("/");
const model = { provider, id: rest.join("/"), contextWindow: WINDOW };
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const entries = (file = sessionFile) => file && fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map((line) => {
    try { return JSON.parse(line); } catch { return {}; }
  })
  : [];
const append = (message) => {
  if (sessionFile) fs.appendFileSync(sessionFile, JSON.stringify({
    type: "message", id: randomUUID().slice(0, 8), parentId: null, timestamp: new Date().toISOString(), message,
  }) + "\n");
};
// #10049: only the RPC transport/provider boundary is simulated. Load the actual
// activation-window extension: its estimator, bound refusal and exit are not mocked.
let activation;
if (process.env.PI_FABRIC_ACTIVATION_HOOK) {
  const { default: hook } = await import(pathToFileURL(process.env.PI_FABRIC_ACTIVATION_HOOK).href);
  const { buildSessionContext } = await import("@earendil-works/pi-coding-agent");
  const handlers = new Map();
  await hook({ on(name, handler) { handlers.set(name, handler); } });
  const dispatch = async (_model, context, options) => {
    await options.onPayload({ messages: context.messages }, model);
    if (process.env.FAKE_PI_LOG) fs.appendFileSync(`${process.env.FAKE_PI_LOG}.requests`, "request\n");
  };
  const provider = { stream: dispatch, streamSimple: dispatch };
  const ctx = { mode: "rpc", model, sessionManager: { getBranch: entries },
    modelRegistry: { getAll: () => [model], getProvider: () => provider } };
  await handlers.get("session_start")({}, ctx);
  activation = { handlers, ctx, provider, messages: () => buildSessionContext(entries()).messages };
}
const decoder = new StringDecoder("utf8");
let buffer = "";
process.stdin.on("data", async (chunk) => {
  buffer += decoder.write(chunk);
  while (buffer.includes("\n")) {
    const end = buffer.indexOf("\n");
    const frame = JSON.parse(buffer.slice(0, end));
    buffer = buffer.slice(end + 1);
    const reply = (data) => emit({ type: "response", id: frame.id, command: frame.type, success: true, data });
    if (frame.type === "get_state") {
      reply({ model, thinkingLevel: "off", isStreaming: false, isCompacting: false,
        ...(activation ? { autoCompactionEnabled: false, autoCompactionDisabledForProcess: true } : {}) });
    } else if (frame.type === "get_available_models") {
      reply({ models: [model] });
    } else if (frame.type === "set_model") {
      reply(model);
    } else if (frame.type === "set_thinking_level") {
      reply();
    } else if (frame.type === "prompt") {
      const task = String(frame.message ?? "");
      // ActivationSession gives native Pi a header-only view. A late extension
      // can still read the registered durable journal; exercise that final payload.
      const journal = entries(activation ? process.env.PI_FABRIC_ACTOR_SESSION_FILE : sessionFile);
      const history = journal.some((entry) => entry.type === "message");
      const providerOverflow = task.includes("PROVIDER_OVERFLOW_ON_HISTORY") && history;
      const quoted = task.includes("QUOTED_PHRASE");
      const silent = task.includes("SILENT_OVERFLOW");
      const toolThenOverflow = task.includes("TOOL_THEN_OVERFLOW_ON_HISTORY") && history;
      const preflight = task.includes("PREFLIGHT_ALWAYS") || (task.includes("PREFLIGHT_ON_HISTORY") && history);
      const overflow = preflight || task.includes("OVERFLOW_ALWAYS") || silent ||
        (task.includes("OVERFLOW_ON_HISTORY") && !task.includes("TOOL_THEN_OVERFLOW_ON_HISTORY") && history) || toolThenOverflow;
      if (process.env.FAKE_PI_LOG) {
        fs.appendFileSync(process.env.FAKE_PI_LOG, JSON.stringify({
          history, overflow, session: journal.find((entry) => entry.type === "session")?.id,
          marker: (task.match(/\b(?:PREFLIGHT_ALWAYS|PREFLIGHT_ON_HISTORY|OVERFLOW_ALWAYS|PROVIDER_OVERFLOW_ON_HISTORY|TOOL_THEN_OVERFLOW_ON_HISTORY|OVERFLOW_ON_HISTORY|SILENT_OVERFLOW|QUOTED_PHRASE|FILL_CONTEXT|[A-Z]+_REVIEW)\b/g) ?? []).join(","),
        }) + "\n");
      }
      append({ role: "user", content: task, timestamp: Date.now() });
      emit({ type: "agent_start" });
      if (activation) {
        const projected = await activation.handlers.get("context")({ messages: activation.messages() });
        await activation.handlers.get("before_provider_headers")({}, activation.ctx);
        await activation.provider.streamSimple(model, { messages: projected.messages }, {
          // Model a late payload transform carrying stored context. A fresh session
          // removes that context, while PREFLIGHT_ALWAYS is irreducible input.
          onPayload: async payload => preflight ? { ...payload, carriedContext: task.includes("PREFLIGHT_ALWAYS")
            ? "x".repeat(WINDOW * 8) : JSON.stringify(journal) } : payload,
        });
      }
      if (silent) {
        // The provider accepted more input than the window and still answered "stop".
        const message = { role: "assistant", provider, model: model.id, content: [{ type: "text", text: "verdict: ok" }], stopReason: "stop",
          usage: { input: WINDOW + 8000, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: WINDOW + 8010 }, timestamp: Date.now() };
        append(message);
        emit({ type: "message_start", message });
        emit({ type: "message_end", message });
        emit({ type: "turn_end", message, toolResults: [] });
        emit({ type: "agent_end" });
        emit({ type: "agent_settled", outcome: "completed" });
        continue;
      }
      if (toolThenOverflow) {
        const call = { type: "toolCall", id: "call-side-effect", name: "bash", arguments: { command: "post-review-comment" } };
        const toolTurn = { role: "assistant", provider, model: model.id, content: [call], stopReason: "toolUse",
          usage: { input: 1000, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 1010 }, timestamp: Date.now() };
        append(toolTurn);
        emit({ type: "message_start", message: toolTurn });
        emit({ type: "message_end", message: toolTurn });
        emit({ type: "tool_execution_start", toolCallId: call.id, toolName: call.name, args: call.arguments });
        if (process.env.FAKE_PI_LOG) fs.appendFileSync(`${process.env.FAKE_PI_LOG}.tools`, `${JSON.stringify({ tool: call.name, marker: "TOOL_THEN_OVERFLOW_ON_HISTORY" })}\n`);
        const toolResult = { role: "toolResult", toolCallId: call.id, toolName: call.name, content: [{ type: "text", text: "posted" }], isError: false, timestamp: Date.now() };
        emit({ type: "tool_execution_end", toolCallId: call.id, toolName: call.name, result: { content: toolResult.content }, isError: false });
        append(toolResult);
        emit({ type: "message_start", message: toolResult });
        emit({ type: "message_end", message: toolResult });
        emit({ type: "turn_end", message: toolTurn, toolResults: [toolResult] });
        emit({ type: "turn_start" });
        const message = { role: "assistant", provider, model: model.id, content: [], stopReason: "error", errorMessage: PROVIDER_OVERFLOW,
          usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0 }, timestamp: Date.now() };
        append(message);
        emit({ type: "message_end", message });
        emit({ type: "agent_end" });
        emit({ type: "agent_settled", outcome: "error" });
        continue;
      }
      if ((overflow && !preflight) || quoted) {
        const message = { role: "assistant", provider, model: model.id, content: [], stopReason: "error",
          errorMessage: quoted ? QUOTED : providerOverflow ? PROVIDER_OVERFLOW : OVERFLOW,
          usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0 }, timestamp: Date.now() };
        append(message);
        emit({ type: "message_end", message });
        emit({ type: "agent_end" });
        // Pi's own window refusal is killed at message_end; other errors settle as Pi does.
        if (quoted || providerOverflow) emit({ type: "agent_settled", outcome: "error" });
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
