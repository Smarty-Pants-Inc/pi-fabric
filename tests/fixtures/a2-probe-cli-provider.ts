import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";

const model = {
  provider: "a2probe", id: "offline", name: "A2 probe", api: "a2probe-api", baseUrl: "http://invalid.local",
  reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 200_000, maxTokens: 4096,
};
const textOf = (message: any): string => message?.content?.filter((part: any) => part.type === "text").map((part: any) => part.text).join("") ?? "";

export default function register(pi: any): void {
  const stream = (selected: any, context: any) => {
    const events = createAssistantMessageEventStream();
    const prompt = textOf(context.messages.at(-1));
    const spec = JSON.parse(prompt.slice("A2PROBE:".length));
    const call = {
      type: "toolCall" as const, id: "a2probe-" + Date.now(), name: "fabric_exec",
      arguments: { code: spec.code, resultFormat: "json", display: { name: "A2 router probe", description: "Public Fabric A2 post-install acceptance" } },
    };
    const message: any = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: [{ type: "text", text: "" }, call], stopReason: "toolUse",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    events.push({ type: "start", partial: message });
    events.push({ type: "toolcall_end", contentIndex: 1, toolCall: call, partial: message });
    events.push({ type: "done", reason: "toolUse", message });
    events.end();
    return events;
  };
  pi.registerProvider({
    id: "a2probe", name: "A2 probe",
    auth: { apiKey: { name: "Keyless", check: async () => ({ type: "api_key", source: "keyless" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
}
