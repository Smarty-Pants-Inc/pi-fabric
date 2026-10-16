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
// STREAM_DISCONNECT_ONCE emits one provider-style incomplete-stream error, then succeeds on the
// next marker attempt. Its first/next contract is keyed by FAKE_PI_LOG, not native history, so
// a manager retry can prove that the worker did not persist a stale CONTEXT_OVERFLOW code.
import fs from "node:fs";
import { randomUUID } from "node:crypto";
import { StringDecoder } from "node:string_decoder";
import { pathToFileURL } from "node:url";

const WINDOW = 272_000;
const OVERFLOW = "Context exceeds window: estimated 272511 input tokens, window 272000";
const PROVIDER_OVERFLOW = "prompt is too long: 280000 tokens > 272000 maximum";
const STREAM_DISCONNECT = "cliproxyapi/gpt-6-luna: stream error: stream disconnected before completion: stream closed before response.completed";
const PROVIDER_OVERLOAD = "cliproxyapi/gpt-6-luna: Our servers are currently overloaded. Please try again later.";
const QUOTED = 'Tool "read" failed: the log line says "Context exceeds window: estimated 272511 input tokens, window 272000"';
const args = process.argv.slice(2);
const arg = (name) => (args.includes(name) ? args[args.indexOf(name) + 1] : undefined);
const sessionFile = arg("--session");
const [provider, ...rest] = (arg("--model") ?? "test/overflow").split("/");
const model = { provider, id: rest.join("/"), contextWindow: WINDOW,
  ...(process.env.FAKE_PI_RESPONSES === "1" ? { api: "openai-responses" } : {}) };
// The relevant native Responses shape: reasoning items are direct input[] entries,
// never JSON hidden under messages[]. Other fixture scenarios keep their old payload.
const payloadFor = context => model.api === "openai-responses" ? { input: context.messages.flatMap(message => {
  if (message.role === "assistant") return message.content.flatMap(block => block.type === "thinking" && block.thinkingSignature
    ? [JSON.parse(block.thinkingSignature)] : block.type === "toolCall"
      ? [{ type: "function_call", call_id: block.id, name: block.name, arguments: JSON.stringify(block.arguments) }]
      : block.type === "text" ? [{ role: "assistant", content: block.text }] : []);
  if (message.role === "toolResult") return [{ type: "function_call_output", call_id: message.toolCallId,
    output: message.content.map(block => block.text ?? "").join("") }];
  return [{ role: message.role, content: message.content }];
}) } : { messages: context.messages };
const emit = (event) => process.stdout.write(JSON.stringify(event) + "\n");
const entries = (file = sessionFile) => file && fs.existsSync(file)
  ? fs.readFileSync(file, "utf8").split("\n").filter(Boolean).map((line) => {
    try { return JSON.parse(line); } catch { return {}; }
  })
  : [];
const append = (message) => {
  if (sessionFile) fs.appendFileSync(sessionFile, JSON.stringify({
    type: "message", id: randomUUID().slice(0, 8),
    parentId: process.env.FAKE_PI_RESPONSES === "1" ? entries().findLast(entry => entry.type === "message")?.id ?? null : null,
    timestamp: new Date().toISOString(), message,
  }) + "\n");
};
// #10049: only the RPC transport/provider boundary is simulated. Load the actual
// activation-window extension: its estimator, bound refusal and exit are not mocked.
let activation;
if (process.env.PI_FABRIC_ACTIVATION_HOOK) {
  const { default: hook } = await import(pathToFileURL(process.env.PI_FABRIC_ACTIVATION_HOOK).href);
  const { buildSessionContext, convertToLlm } = await import("@earendil-works/pi-coding-agent");
  const handlers = new Map();
  await hook({ on(name, handler) { handlers.set(name, handler); } });
  const dispatch = async (_model, context, options) => {
    await options.onPayload(payloadFor(context), model);
    if (process.env.FAKE_PI_LOG) fs.appendFileSync(`${process.env.FAKE_PI_LOG}.requests`, "request\n");
  };
  const provider = { stream: dispatch, streamSimple: dispatch };
  const ctx = { mode: "rpc", model, sessionManager: { getBranch: entries },
    modelRegistry: { getAll: () => [model], getProvider: () => provider } };
  await handlers.get("session_start")({}, ctx);
  activation = { handlers, ctx, provider, convertToLlm, messages: () => buildSessionContext(entries()).messages };
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
      const priorMarkers = process.env.FAKE_PI_LOG && fs.existsSync(process.env.FAKE_PI_LOG)
        ? fs.readFileSync(process.env.FAKE_PI_LOG, "utf8").split("\n").filter(Boolean)
          .filter((line) => line.includes('"marker":"STREAM_DISCONNECT_ONCE"')).length : 0;
      const streamDisconnect = task.includes("STREAM_DISCONNECT_ONCE") && priorMarkers === 0;
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
          marker: (task.match(/\b(?:PREFLIGHT_ALWAYS|PREFLIGHT_ON_HISTORY|OVERFLOW_ALWAYS|PROVIDER_OVERFLOW_ON_HISTORY|TOOL_THEN_OVERFLOW_ON_HISTORY|OVERFLOW_ON_HISTORY|SILENT_OVERFLOW|STREAM_DISCONNECT_ONCE|OVERLOAD_NATIVE_RETRY|QUOTED_PHRASE|FILL_CONTEXT_85|FILL_CONTEXT|[A-Z]+_REVIEW)\b/g) ?? []).join(","),
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
      if (task.includes("OVERLOAD_NATIVE_RETRY")) {
        if (!activation) throw new Error("OVERLOAD_NATIVE_RETRY requires the actual activation hook");
        // #14075: model the retained native frames inside ONE activation/worker, not an
        // ActorManager replay. The provider transport is offline; the retry admission below
        // runs the actual hook, its estimator and its nonce-bound refusal protocol.
        const trace = event => {
          if (process.env.FAKE_PI_LOG) fs.appendFileSync(`${process.env.FAKE_PI_LOG}.native`, `${JSON.stringify(event)}\n`);
        };
        const call = { type: "toolCall", id: "call-overload-read", name: "read", arguments: { path: "synthetic-history.txt" } };
        const toolTurn = { role: "assistant", provider, model: model.id,
          api: model.api,
          content: [{ type: "thinking", thinking: "brief reasoning", thinkingSignature: JSON.stringify({ type: "reasoning",
            id: "rs_14075", encrypted_content: "h".repeat(1_128_000) }) }, call], stopReason: "toolUse",
          usage: { input: 190_000, output: 11_471, cacheRead: 0, cacheWrite: 0, totalTokens: 201_471 }, timestamp: Date.now() };
        append(toolTurn);
        // Keep the synthetic megabyte ciphertext in the real journal/payload/hook,
        // not the offline RPC diagnostic copy (the worker needs usage + tool calls).
        const rpcToolTurn = { ...toolTurn, content: [{ type: "thinking", thinking: "brief reasoning" }, call] };
        emit({ type: "message_end", message: rpcToolTurn });
        // Bind the successful report and its emitted opaque item to the initial dispatch.
        // Empty toolResults isolates admission from activation compaction.
        await activation.handlers.get("turn_end")({ message: toolTurn, toolResults: [], context: { contextMessages: activation.messages() } });
        emit({ type: "tool_execution_start", toolCallId: call.id, toolName: call.name, args: call.arguments });
        const toolResult = { role: "toolResult", toolCallId: call.id, toolName: call.name,
          content: [{ type: "text", text: "t".repeat(6144) }], isError: false, timestamp: Date.now() };
        append(toolResult);
        emit({ type: "tool_execution_end", toolCallId: call.id, toolName: call.name,
          result: { content: toolResult.content }, isError: false });
        emit({ type: "message_end", message: toolResult });
        emit({ type: "turn_end", message: rpcToolTurn, toolResults: [toolResult] });
        trace({ type: "tool_turn", stopReason: toolTurn.stopReason, totalTokens: toolTurn.usage.totalTokens, toolResultBytes: 6144 });
        // Keep stream frames ordered before the hook's synchronous protocol writes/exit.
        await new Promise(resolve => process.stdout.write("", resolve));
        const beforeError = await activation.handlers.get("context")({ messages: activation.messages() });
        await activation.handlers.get("before_provider_headers")({}, activation.ctx);
        await activation.provider.streamSimple(model, { messages: activation.convertToLlm(beforeError.messages) }, { onPayload: payload => payload });
        trace({ type: "pre_overload_admitted" });
        const usageMode = process.env.FAKE_PI_OVERLOAD_USAGE ?? "zero";
        const error = { role: "assistant", provider, model: model.id, api: model.api, content: [], stopReason: "error", errorMessage: PROVIDER_OVERLOAD,
          ...(usageMode === "absent" ? {} : { usage: { input: usageMode === "present" ? 201_471 : 0,
            output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: usageMode === "present" ? 201_471 : 0 } }), timestamp: Date.now() };
        append(error);
        emit({ type: "turn_start" });
        emit({ type: "message_end", message: error });
        emit({ type: "turn_end", message: error, toolResults: [] });
        await activation.handlers.get("turn_end")({ message: error, toolResults: [], context: { contextMessages: activation.messages() } });
        trace({ type: "overload", stopReason: error.stopReason, errorMessage: error.errorMessage, usage: error.usage });
        emit({ type: "agent_end", willRetry: true });
        const retry = { type: "auto_retry_start", attempt: 1, maxAttempts: 3, delayMs: 0, errorMessage: PROVIDER_OVERLOAD };
        emit(retry); trace(retry);
        emit({ type: "agent_start" });
        emit({ type: "turn_start" });
        // Native Pi omits the failed attempt from the next model projection, while
        // keeping it in raw history. Preserve that distinction without editing the journal.
        const context = activation.messages().filter(message => message !== error &&
          !(message.role === "assistant" && message.stopReason === "error"));
        const projected = await activation.handlers.get("context")({ messages: context });
        const messages = activation.convertToLlm(projected.messages);
        const { estimateContextTokens, estimateTextTokens } = await import("@earendil-works/pi-ai/utils/estimate");
        trace({ type: "retry_admission", nativeEstimate: estimateContextTokens(messages),
          rawPayloadEstimate: estimateTextTokens(JSON.stringify(payloadFor({ messages }))), contextWindow: WINDOW });
        await new Promise(resolve => process.stdout.write("", resolve));
        await activation.handlers.get("before_provider_headers")({}, activation.ctx);
        await activation.provider.streamSimple(model, { messages }, { onPayload: payload => payload });
        trace({ type: "retry_admitted" });
        const answer = { role: "assistant", provider, model: model.id, api: model.api, content: [{ type: "text", text: "verdict: ok" }], stopReason: "stop",
          usage: { input: 203_300, output: 10, cacheRead: 0, cacheWrite: 0, totalTokens: 203_310 }, timestamp: Date.now() };
        append(answer);
        emit({ type: "message_end", message: answer });
        emit({ type: "turn_end", message: answer, toolResults: [] });
        emit({ type: "auto_retry_end", success: true, attempt: 1 });
        emit({ type: "agent_end" });
        emit({ type: "agent_settled", outcome: "completed" });
        continue;
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
      if (streamDisconnect || (overflow && !preflight) || quoted) {
        const message = { role: "assistant", provider, model: model.id, content: [], stopReason: "error",
          errorMessage: streamDisconnect ? STREAM_DISCONNECT : quoted ? QUOTED : providerOverflow ? PROVIDER_OVERFLOW : OVERFLOW,
          usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0 }, timestamp: Date.now() };
        append(message);
        emit({ type: "message_end", message });
        emit({ type: "agent_end" });
        // Pi's own window refusal is killed at message_end; other errors settle as Pi does.
        if (streamDisconnect || quoted || providerOverflow) emit({ type: "agent_settled", outcome: "error" });
        continue;
      }
      const input = task.includes("FILL_CONTEXT_85") ? Math.floor(WINDOW * 0.85) :
        task.includes("FILL_CONTEXT") ? Math.floor(WINDOW * 0.9) : 1000;
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
