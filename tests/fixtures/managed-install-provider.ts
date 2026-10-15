import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type Api, type AssistantMessage, type Model } from "@earendil-works/pi-ai";

// Deterministic offline model: Pi and Fabric dispatch the real nested tools.
export default function (pi: ExtensionAPI) {
  let calls = 0;
  const model: Model<Api> = {
    provider: "managed-install", id: "probe", name: "Managed install probe",
    api: "managed-install-api", baseUrl: "http://127.0.0.1:1",
    reasoning: false, input: ["text"], contextWindow: 128000, maxTokens: 4096,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };
  const stream = (selected: Model<Api>) => {
    const file = JSON.stringify(process.env.FABRIC_PROBE_FILE);
    const code = calls++ === 0
      ? `await pi.write({ path: ${file}, text: "hello from probe\\n" }); return await pi.read({ path: ${file} });`
      : undefined;
    const message: AssistantMessage = {
      role: "assistant", api: selected.api, provider: selected.provider, model: selected.id,
      content: code
        ? [{ type: "toolCall", id: "probe-call", name: "fabric_exec", arguments: { code } }]
        : [{ type: "text", text: "Probe finished." }],
      stopReason: code ? "toolUse" : "stop", timestamp: Date.now(),
      usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    const events = createAssistantMessageEventStream();
    events.push({ type: "start", partial: message });
    if (code) events.push({ type: "toolcall_end", contentIndex: 0,
      toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: code ? "toolUse" : "stop", message });
    events.end(message);
    return events;
  };
  pi.registerProvider({
    id: "managed-install", name: "Managed install offline proof",
    auth: { apiKey: { name: "Keyless fixture", check: async () => ({ type: "api_key", source: "offline fixture" }),
      resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
}
