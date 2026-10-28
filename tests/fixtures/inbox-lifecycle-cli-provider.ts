// Keyless MODEL TRANSPORT ONLY for probe-inbox-lifecycle.mjs. The CLI, fabric_exec,
// agents.followUp, inbox receipts, compaction, reload and terminal are production code.
// Lifecycle handlers below are passive observers: no custom commands, compaction
// replacement, trace writes, host-capability overrides, or delivery interception.
import fs from "node:fs";
import {
  createAssistantMessageEventStream,
  getCurrentSystemPrompt,
  type AssistantMessage,
  type Model,
  type Api,
  type TranscriptContext,
  type StreamOptions,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const textOf = (content: unknown): string => typeof content === "string" ? content : Array.isArray(content)
  ? content.map(part => typeof part === "object" && part !== null && "text" in part ? String(part.text) : "").join("\n") : "";
const markersOf = (text: string): string[] => [...new Set(text.match(/LIFECYCLE_(?:FU|INBOX)_[A-Z_]+_[a-f0-9-]+/g) ?? [])];
const append = (file: string | undefined, record: object) => {
  if (!file) throw new Error("Run this fixture through probe-inbox-lifecycle.mjs");
  fs.appendFileSync(file, JSON.stringify({ at: Date.now(), ...record }) + "\n");
};

export default function (pi: ExtensionAPI) {
  const epoch = `${process.pid}-${Date.now()}`;
  let nativeRunOrdinal = 0;
  let nativeTurnIndex: number | undefined;
  const model: Model<Api> = {
    provider: "inbox-lifecycle-proof", id: "offline", name: "Keyless native inbox lifecycle proof",
    api: "inbox-lifecycle-proof-api", baseUrl: "http://invalid.local", reasoning: false, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext, options?: StreamOptions) => {
    const events = createAssistantMessageEventStream();
    void (async () => {
      const messages = context.messages;
      let userIndex = -1;
      for (let index = messages.length - 1; index >= 0; index--) {
        if (messages[index]?.role === "user") { userIndex = index; break; }
      }
      const input = userIndex < 0 ? "" : textOf(messages[userIndex]!.content);
      const hasResult = messages.slice(userIndex + 1).some(message => message.role === "toolResult");
      const summarize = /summarization assistant/i.test(getCurrentSystemPrompt(messages));
      const activate = !summarize && input === "LIFECYCLE_ACTIVATE";
      const packet = !summarize && input.startsWith("LIFECYCLE_SEND ") ? input.slice("LIFECYCLE_SEND ".length).split(" ") : undefined;
      const tool = !hasResult && (activate || packet !== undefined);
      const markers = !summarize && !activate && !packet ? markersOf(input) : [];
      const stage = summarize ? "summary" : tool ? activate ? "activate-tool" : "followup-tool"
        : markers.length ? "delivery-ack" : activate ? "ready" : packet ? "send-accepted" : "unexpected-input";
      // A tiny transcript legitimately declines deterministic Fabric compaction:
      // its bounded summary reservation would exceed the source. Create enough
      // completed, synthetic tool output through the REAL tool call, not by
      // appending or rewriting session entries, so literal /compact has work.
      const code = activate ? 'const main = await agents.main(); return { main, completedSyntheticSetup: "SYNTHETIC_COMPLETED_CONTEXT ".repeat(1600) };' : packet ? `
const main = await agents.main();
const receipt = await agents.followUp({ id: main.id, message: ${JSON.stringify(packet[0])},
  data: { proof: "inbox-lifecycle", marker: ${JSON.stringify(packet[0])} } });
return { mainId: main.id, marker: ${JSON.stringify(packet[0])}, receipt };` : undefined;
      const assistantText = summarize ? "## Goal\nProve native inbox lifecycle delivery.\n## Progress\nPreviously acknowledged work is complete; do not repeat it.\n## Next Steps\nWait for new addressed work."
        : markers.length ? markers.map(marker => `LIFECYCLE_ACK ${marker}`).join("\n")
        : activate ? "LIFECYCLE_READY" : packet ? "LIFECYCLE_SEND_ACCEPTED" : "LIFECYCLE_UNEXPECTED_INPUT";
      const toolCall = { type: "toolCall" as const, id: `lifecycle-${epoch}-${Date.now()}`, name: "fabric_exec",
        arguments: { code: code!, resultFormat: "json", maxOutputTokens: 12000 } };
      const message: AssistantMessage = {
        role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
        content: tool ? [toolCall] : [{ type: "text", text: assistantText }], stopReason: tool ? "toolUse" : "stop",
        // Deterministic estimates, not a claim of remote provider billing.
        usage: { input: Math.ceil(JSON.stringify(messages).length / 4), output: Math.ceil((code ?? assistantText).length / 4),
          cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      };
      message.usage.totalTokens = message.usage.input + message.usage.output;
      try {
        await options?.onPayload?.({ fixture: "inbox-lifecycle", stage }, selected);
        options?.signal?.throwIfAborted();
        await options?.onResponse?.({ status: 200, headers: {} }, selected);
        append(process.env.INBOX_LIFECYCLE_INFERENCES, { epoch, nativeRunOrdinal, nativeTurnIndex, stage, markers, input,
          toolCode: tool ? code : undefined, assistantText: tool ? undefined : assistantText });
        const push = async (event: Parameters<typeof events.push>[0]) => {
          options?.signal?.throwIfAborted();
          await options?.onProviderStreamEvent?.({ type: "provider_stream_event" }, selected);
          events.push(event);
        };
        await push({ type: "start", partial: message });
        if (tool) {
          await push({ type: "toolcall_start", contentIndex: 0, partial: message });
          await push({ type: "toolcall_end", contentIndex: 0, toolCall, partial: message });
        } else {
          await push({ type: "text_start", contentIndex: 0, partial: message });
          await push({ type: "text_delta", contentIndex: 0, delta: assistantText, partial: message });
          await push({ type: "text_end", contentIndex: 0, content: assistantText, partial: message });
        }
        await push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
      } catch (error) {
        message.stopReason = options?.signal?.aborted ? "aborted" : "error";
        message.errorMessage = error instanceof Error ? error.message : String(error);
        events.push({ type: "error", reason: message.stopReason, error: message });
      } finally { events.end(); }
    })();
    return events;
  };
  pi.registerProvider({ id: model.provider, name: model.name,
    auth: { apiKey: { name: "No credentials", check: async () => ({ type: "api_key", source: "keyless proof" }),
      resolve: async () => ({ auth: {} }) } }, getModels: () => [model], stream, streamSimple: stream });

  pi.on("agent_start", () => { nativeRunOrdinal++; nativeTurnIndex = undefined; });
  pi.on("turn_start", event => {
    nativeTurnIndex = event.turnIndex;
    append(process.env.INBOX_LIFECYCLE_HOST, { event: "turn_start", epoch, nativeRunOrdinal, nativeTurnIndex });
  });
  pi.on("turn_end", event => append(process.env.INBOX_LIFECYCLE_HOST, { event: "turn_end", epoch, nativeRunOrdinal,
    nativeTurnIndex: event.turnIndex, messageEntryId: (event as unknown as { messageEntryId?: unknown }).messageEntryId,
    messageRole: event.message.role, assistantText: event.message.role === "assistant" ? textOf(event.message.content) : undefined }));
  pi.on("session_start", (_event, context) => append(process.env.INBOX_LIFECYCLE_HOST, {
    event: "session_start", epoch, pid: process.pid, sessionId: context.sessionManager.getSessionId(),
    sessionFile: context.sessionManager.getSessionFile(), mode: context.mode,
    capabilities: (pi as unknown as { hostCapabilities?: unknown }).hostCapabilities,
    isPromptPending: typeof (context as unknown as { isPromptPending?: unknown }).isPromptPending,
    testCapabilityOverride: Boolean((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.test.hostCapabilities")]),
  }));
  pi.on("session_shutdown", event => append(process.env.INBOX_LIFECYCLE_HOST, { event: "session_shutdown", epoch, reason: event.reason }));
  pi.on("session_before_compact", event => append(process.env.INBOX_LIFECYCLE_HOST, { event: "session_before_compact", epoch, reason: event.reason }));
  pi.on("session_compact", event => append(process.env.INBOX_LIFECYCLE_HOST, { event: "session_compact", epoch,
    compactionId: event.compactionEntry.id }));
  pi.on("session_compact_failed", event => append(process.env.INBOX_LIFECYCLE_HOST, { event: "session_compact_failed", epoch, ...event }));
  pi.on("agent_settled", event => append(process.env.INBOX_LIFECYCLE_HOST, { event: "agent_settled", epoch, outcome: (event as unknown as { outcome?: unknown }).outcome }));
  pi.on("tool_execution_end", event => {
    if (event.toolName === "fabric_exec") append(process.env.INBOX_LIFECYCLE_HOST, {
      event: "tool_execution_end", epoch, toolCallId: event.toolCallId, isError: event.isError, result: event.result,
    });
  });
  pi.on("message_end", event => {
    const message = event.message;
    if (message.role === "custom") append(process.env.INBOX_LIFECYCLE_HOST, { event: "native_custom_message", epoch, message });
  });
}
