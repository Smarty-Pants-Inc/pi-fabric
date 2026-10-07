// Offline inference fixture: Fabric admission, worker, native Pi RPC, session
// persistence, abort and real bash effects remain production implementations.
import fs from "node:fs";
import path from "node:path";
import { createAssistantMessageEventStream, type AssistantMessage, type Api, type Model, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const log = (value: object) => fs.appendFileSync(process.env.FABRIC_NATIVE_WHITESPACE_LOG!, JSON.stringify({ pid: process.pid, ...value }) + "\n");
  let native: ExtensionContext;
  const model: Model<Api> = { provider: "whitespace-native", id: "offline", name: "Keyless native whitespace fixture", api: "whitespace-native-api", baseUrl: "http://invalid.local",
    reasoning: true, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096 };
  const stream = (selected: Model<Api>, context: TranscriptContext, options?: { signal?: AbortSignal }) => {
    const events = createAssistantMessageEventStream();
    const users = context.messages.filter(message => message.role === "user");
    const task = JSON.stringify(users[0]?.content);
    const resumed = JSON.stringify(users.at(-1)?.content).includes("Continue the task from the existing session.");
    const effects = task.includes("NATIVE_WHITESPACE_effects");
    const repeated = task.includes("NATIVE_WHITESPACE_repeat");
    const results = context.messages.filter(message => message.role === "toolResult");
    const stall = repeated || (!resumed && (!effects || results.length > 0));
    const tool = !stall && (!resumed || !results.some(result => JSON.stringify(result.content).includes("AFTER_RETRY")));
    const effectFile = path.join(process.env.FABRIC_NATIVE_WHITESPACE_ROOT!, "effects.txt");
    const command = resumed ? `printf 'after\\n' >> ${JSON.stringify(effectFile)}; printf AFTER_RETRY` : `printf 'before\\n' >> ${JSON.stringify(effectFile)}; printf BEFORE_STALL`;
    const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: stall || tool ? [{ type: "toolCall", id: `native-${process.pid}-${Date.now()}`, name: "bash", arguments: stall ? {} : { command } }] : [{ type: "text", text: "NATIVE_RETRY_COMPLETED" }],
      stopReason: stall ? "pending" : tool ? "toolUse" : "stop", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    log({ type: "provider-call", sessionId: native.sessionManager.getSessionId(), sessionFile: native.sessionManager.getSessionFile(), exists: fs.existsSync(native.sessionManager.getSessionFile()!),
      model: `${selected.provider}/${selected.id}`, effort: pi.getThinkingLevel(), resumed, stall, tool, messages: context.messages });
    events.push({ type: "start", partial: message });
    if (stall) {
      events.push({ type: "toolcall_start", contentIndex: 0, partial: message });
      const emit = () => events.push({ type: "toolcall_delta", contentIndex: 0, delta: " \t\n", partial: message });
      emit(); const timer = setInterval(emit, 20);
      const abort = () => {
        clearInterval(timer);
        message.stopReason = "aborted"; message.errorMessage = "offline whitespace provider aborted";
        log({ type: "provider-abort", sessionId: native.sessionManager.getSessionId(), exists: fs.existsSync(native.sessionManager.getSessionFile()!) });
        events.push({ type: "error", reason: "aborted", error: message }); events.end();
      };
      if (options?.signal?.aborted) abort();
      else options?.signal?.addEventListener("abort", abort, { once: true });
    } else {
      if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
      events.push({ type: "done", reason: tool ? "toolUse" : "stop", message }); events.end();
    }
    return events;
  };
  pi.registerProvider({ id: "whitespace-native", name: "Offline native whitespace proof", auth: { apiKey: { name: "Keyless fixture", check: async () => ({ type: "api_key", source: "offline fixture" }), resolve: async () => ({ auth: {} }) } }, getModels: () => [model], stream, streamSimple: stream });
  pi.on("session_start", (_event, ctx) => { native = ctx; log({ type: "native-start", mode: ctx.mode, sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), exists: fs.existsSync(ctx.sessionManager.getSessionFile()!) }); });
  pi.on("session_shutdown", (_event, ctx) => log({ type: "native-shutdown", sessionId: ctx.sessionManager.getSessionId(), exists: fs.existsSync(ctx.sessionManager.getSessionFile()!), entries: ctx.sessionManager.getEntries() }));
  pi.on("tool_call", event => { log({ type: "native-tool-call", event }); });
}
