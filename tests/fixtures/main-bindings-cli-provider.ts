// Only inference is synthetic. The Pi CLI, public fabric_exec API, mesh owner and setters are real.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const model: Model<Api> = {
    provider: "main-binding-probe", id: "a", name: "Keyless Main binding probe", api: "main-binding-probe",
    baseUrl: "http://invalid.local", reasoning: true, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const request = [...context.messages].reverse().find(message => message.role === "user");
    const text = request?.content;
    const command = typeof text === "string" ? text : Array.isArray(text) ? text.filter(part => part.type === "text").map(part => part.text).join("") : "";
    const packet = command.startsWith("BIND ") ? JSON.parse(command.slice(5)) : {};
    const tool = command.startsWith("BIND ") && context.messages.at(-1)?.role !== "toolResult";
    fs.appendFileSync(process.env.MAIN_BINDING_RECEIPTS!, JSON.stringify({
      endpoint: process.env.MAIN_BINDING_ENDPOINT, tag: packet.tag, tool,
      model: `${selected.provider}/${selected.id}`, thinking: pi.getThinkingLevel(),
    }) + "\n");
    const code = `
const packet = JSON.parse(π.packet);
if (packet.compat) return { members: await agents.members({ scope: "project" }), sessions: await agents.sessions(), peers: await agents.peers() };
if (packet.message) return packet.delivery === "steer"
  ? await agents.steer({ id: packet.id, message: packet.message, data: { mixedGeneration: true } })
  : await agents.followUp({ id: packet.id, message: packet.message, data: { mixedGeneration: true } });
if (packet.observe) return await agents.main();
if (packet.own) {
  const before = await agents.main();
  const effort = await agents.setThinking({ id: before.id, thinking: "high" });
  const model = await agents.setModel({ id: before.id, model: "main-binding-probe/b" });
  return { before, effort, model, after: await agents.main() };
}
try {
  const result = packet.operation === "setThinking"
    ? await agents.setThinking({ id: packet.id, thinking: packet.thinking })
    : await agents.setModel({ id: packet.id, model: packet.model });
  return { result };
} catch (error) { return { refused: true, error: error.message }; }
`;
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: `binding-${Date.now()}`, name: "fabric_exec", arguments: { code, payloads: { packet: JSON.stringify(packet) }, resultFormat: "json" } }]
        : [{ type: "text", text: "Main binding probe completed" }],
      stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    const events = createAssistantMessageEventStream();
    const finish = () => {
      events.push({ type: "start", partial: message });
      if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
      events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message }); events.end();
    };
    finish();
    return events;
  };
  pi.registerProvider({ id: model.provider, name: model.name,
    auth: { apiKey: { name: "Keyless local probe", check: async () => ({ type: "api_key", source: "keyless test" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model, { ...model, id: "b" }], stream, streamSimple: stream,
  });
}
