// Keyless deterministic model transport for the real native Pi RPC inbox proof.
// It only records the actual host capability surface and returns predictable assistant responses.
import fs from "node:fs";
import {
  createAssistantMessageEventStream,
  type AssistantMessage,
  type Model,
  type Api,
  type TranscriptContext,
} from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const EVENT_TEXTS = [
  "ANSWER_EVENT_A_",
  "ANSWER_EVENT_B_",
  "ANSWER_CONTROL_",
  "EXPLICIT_DELIVERYID_REPLAY_",
  "UNADDRESSED_EVENT_",
];

export default function (pi: ExtensionAPI) {
  let activationToolSent = false;
  let activationReadySent = false;
  let inferenceNumber = 0;
  const model: Model<Api> = {
    provider: "inbox-distinct-proof",
    id: "offline",
    name: "Keyless native inbox distinct-event proof",
    api: "inbox-distinct-proof-api",
    baseUrl: "http://invalid.local",
    reasoning: false,
    input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    contextWindow: 1000000,
    maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const serializedContext = JSON.stringify(context.messages);
    const activated = serializedContext.includes("NATIVE_INBOX_ACTIVATE");
    const tool = activated && !activationToolSent;
    if (tool) activationToolSent = true;
    const ready = !tool && activationToolSent && !activationReadySent;
    if (ready) activationReadySent = true;
    const stage = tool ? "activation-fabric_exec" : ready ? "activation-ready" : "inbox-ack";
    const toolCode = tool ? "return 1;" : undefined;
    const assistantText = ready ? "NATIVE_INBOX_READY" : tool ? undefined : "NATIVE_INBOX_ACK";
    const markers = EVENT_TEXTS.flatMap(prefix => {
      const matches = serializedContext.match(new RegExp(`${prefix}[A-Fa-f0-9-]+`, "g")) ?? [];
      return [...new Set(matches)];
    });
    inferenceNumber++;
    fs.appendFileSync(process.env.INBOX_PROOF_INFERENCES!, JSON.stringify({
      inferenceNumber,
      at: Date.now(),
      stage,
      toolCode,
      assistantText,
      markers,
      inboxFramePresent: serializedContext.includes("<fabric-inbox"),
    }) + "\n");

    const toolCall = {
      type: "toolCall" as const,
      id: "inbox-distinct-activation",
      name: "fabric_exec",
      arguments: { code: "return 1;", resultFormat: "json" },
    };
    const message: AssistantMessage = {
      role: "assistant",
      provider: selected.provider,
      model: selected.id,
      api: selected.api,
      timestamp: Date.now(),
      content: tool ? [toolCall] : [{ type: "text", text: assistantText! }],
      stopReason: tool ? "toolUse" : "stop",
      usage: {
        input: 0,
        output: 0,
        cacheRead: 0,
        cacheWrite: 0,
        totalTokens: 0,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
      },
    };
    events.push({ type: "start", partial: message });
    if (tool) events.push({
      type: "toolcall_end",
      contentIndex: 0,
      toolCall,
      partial: message,
    });
    events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
    events.end();
    return events;
  };

  pi.registerProvider({
    id: "inbox-distinct-proof",
    name: "Keyless native inbox distinct-event proof",
    auth: {
      apiKey: {
        name: "No credentials",
        check: async () => ({ type: "api_key", source: "keyless proof" }),
        resolve: async () => ({ auth: {} }),
      },
    },
    getModels: () => [model],
    stream,
    streamSimple: stream,
  });

  pi.on("session_start", (_event, context) => {
    const capabilities = (pi as unknown as { hostCapabilities?: {
      triggeredMessageQueuesBehindPreflight?: unknown;
      promptPendingVisible?: unknown;
    } }).hostCapabilities;
    fs.appendFileSync(process.env.INBOX_PROOF_HOST!, JSON.stringify({
      sessionId: context.sessionManager.getSessionId(),
      mode: context.mode,
      capabilities,
      testCapabilityOverride: Boolean((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.test.hostCapabilities")]),
      isPromptPending: typeof (context as unknown as { isPromptPending?: unknown }).isPromptPending,
    }) + "\n");
  });
}
