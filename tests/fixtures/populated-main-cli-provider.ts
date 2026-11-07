// Deterministic LOCAL model transport, not remote inference. All inbox ownership,
// admission, issued-carrier validation, session consumption and ACKs remain native.
import fs from "node:fs";
import path from "node:path";
import {
  createAssistantMessageEventStream,
  type Api, type AssistantMessage, type Model, type StreamOptions, type TranscriptContext,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const textOf = (value: unknown): string => typeof value === "string" ? value : Array.isArray(value)
  ? value.map(part => part && typeof part === "object" && "text" in part ? String(part.text) : "").join("\n") : "";

export default function populatedTransport(pi: ExtensionAPI) {
  const root = process.env.POPULATED_MAIN_PRIVATE_ROOT;
  const trace = process.env.POPULATED_MAIN_TRANSPORT_TRACE;
  const setup = process.env.POPULATED_MAIN_BOOTSTRAP_FILE;
  if (!root || !trace || !setup || ![trace, setup].every(file => {
    const relative = path.relative(root, file);
    return relative && !relative.startsWith("..") && !path.isAbsolute(relative) && !fs.lstatSync(file).isSymbolicLink();
  })) throw new Error("Private cohort transport files required");
  const log = (record: object) => fs.appendFileSync(trace, JSON.stringify({ at: Date.now(), pid: process.pid,
    phase: process.env.POPULATED_MAIN_PHASE ?? "startup", ...record }) + "\n");
  let setupUsed = false, calls = 0, nativeRuns = 0;
  const model: Model<Api> = { provider: "cohort-local", id: "offline", name: "Deterministic local cohort ACK",
    api: "cohort-local-api", baseUrl: "http://invalid.local", reasoning: false, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 1000000, maxTokens: 4096 };
  const stream = (selected: Model<Api>, context: TranscriptContext, options?: StreamOptions) => {
    const events = createAssistantMessageEventStream();
    void (async () => {
      const phase = process.env.POPULATED_MAIN_PHASE ?? "startup";
      let userIndex = -1;
      for (let index = context.messages.length - 1; index >= 0; index--) if (context.messages[index]?.role === "user") { userIndex = index; break; }
      const input = userIndex >= 0 ? textOf(context.messages[userIndex]!.content) : "";
      const resultPresent = context.messages.slice(userIndex + 1).some(message => message.role === "toolResult");
      const doSetup = input === "COHORT_SETUP" && !resultPresent && !setupUsed && phase === "startup";
      const code = doSetup ? fs.readFileSync(setup, "utf8") : undefined;
      if (doSetup) setupUsed = true;
      const markers = [...new Set(JSON.stringify(context.messages.slice(userIndex)).match(/COHORT_(?:TRAFFIC|BOOTSTRAP)_[A-Z0-9_]+/g) ?? [])];
      const assistantText = input === "COHORT_SETUP" ? "COHORT_SETUP_ACK" : `COHORT_ACK ${markers.join(" ")}`;
      const toolCall = { type: "toolCall" as const, id: `cohort-setup-${process.pid}`, name: "fabric_exec",
        arguments: { code: code!, resultFormat: "json", maxOutputTokens: 12000 } };
      const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api,
        timestamp: Date.now(), content: doSetup ? [toolCall] : [{ type: "text", text: assistantText }], stopReason: doSetup ? "toolUse" : "stop",
        usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      try {
        if (phase === "idle" || (phase !== "startup" && phase !== "traffic" && phase !== "drain")) throw new Error(`Model transport forbidden in ${phase}`);
        if (++calls > 512) throw new Error("Bounded local transport call ceiling exceeded");
        if (input === "COHORT_SETUP" && !doSetup && !resultPresent) throw new Error("Setup transport may run only once");
        options?.signal?.throwIfAborted();
        await options?.onPayload?.({ fixture: "cohort-local", doSetup }, selected);
        await options?.onResponse?.({ status: 200, headers: {} }, selected);
        log({ event: "transport", ordinal: calls, nativeRuns, stage: doSetup ? "synthetic-setup-tool" : "fixed-ack", markers,
          realLlmInference: 0, networkCalls: 0, paidInference: 0 });
        events.push({ type: "start", partial: message });
        if (doSetup) {
          events.push({ type: "toolcall_start", contentIndex: 0, partial: message });
          events.push({ type: "toolcall_end", contentIndex: 0, toolCall, partial: message });
        } else {
          events.push({ type: "text_start", contentIndex: 0, partial: message });
          events.push({ type: "text_delta", contentIndex: 0, delta: assistantText, partial: message });
          events.push({ type: "text_end", contentIndex: 0, content: assistantText, partial: message });
        }
        events.push({ type: "done", reason: doSetup ? "toolUse" : "stop", message });
      } catch (error) {
        message.stopReason = options?.signal?.aborted ? "aborted" : "error";
        message.errorMessage = error instanceof Error ? error.message : String(error);
        log({ event: "transport-error", error: message.errorMessage });
        events.push({ type: "error", reason: message.stopReason, error: message });
      } finally { events.end(); }
    })();
    return events;
  };
  pi.registerProvider({ id: model.provider, name: model.name,
    auth: { apiKey: { name: "Keyless local fixture", check: async () => ({ type: "api_key", source: "local synthetic transport" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream });
  pi.on("agent_start", () => { nativeRuns++; log({ event: "agent_start", nativeRuns }); });
  pi.on("agent_settled", () => log({ event: "agent_settled", nativeRuns }));
  pi.on("tool_execution_end", event => log({ event: "tool_execution_end", tool: event.toolName, isError: event.isError, result: event.result }));
  pi.on("message_end", event => {
    if (event.message.role === "custom") log({ event: "native_custom_message", message: event.message });
  });
  pi.on("session_start", (_event, context) => log({ event: "session_start", sessionId: context.sessionManager.getSessionId(),
    sessionFile: context.sessionManager.getSessionFile(), mode: context.mode,
    testCapabilityOverride: Boolean((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.test.hostCapabilities")]) }));
}
