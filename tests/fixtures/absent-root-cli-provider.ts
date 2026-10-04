// Only inference is synthetic: Mains, resident hosts, workers and public Fabric API are native.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const model: Model<Api> = { provider: "absent-root-proof", id: "offline", name: "Keyless absent-root proof", api: "absent-root-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096 };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const user = [...context.messages].reverse().find(message => message.role === "user");
    const command = typeof user?.content === "string" ? user.content : Array.isArray(user?.content) ? user.content.filter(part => part.type === "text").map(part => part.text).join("") : "";
    const packet = command.startsWith("ABSENT ") ? JSON.parse(command.slice(7)) : undefined;
    const tool = !process.env.PI_FABRIC_ACTOR_ID && packet && context.messages.at(-1)?.role !== "toolResult";
    const code = `
const packet = JSON.parse(π.packet);
if (packet.create) {
  const main = await agents.main();
  const actor = await agents.create({ name: packet.name, instructions: "Handle the delivered ADOPTED_EVENT exactly once.",
    residency: "durable", scope: "project", model: "absent-root-proof/offline", thinking: "off", tools: [], extensions: true,
    topics: packet.topics ? ["absent.native.proof"] : [], events: [], delivery: "mailbox", triggerTurn: false, responseMode: "text", nice: 19 });
  return { main, actor };
}
if (packet.status) return { main: await agents.main(), actor: await agents.actorStatus({ id: packet.id }) };
if (packet.event) return await mesh.publish({ topic: "absent.native.proof", kind: "proof", text: "ADOPTED_EVENT" });
if (packet.messages) return await agents.messages({ id: packet.id, limit: 50 });
throw new Error("Unknown absent-root proof command");
`;
    const text = process.env.PI_FABRIC_ACTOR_ID ? "HANDLED_ADOPTED_EVENT" : "Native proof command completed";
    const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: `absent-${Date.now()}`, name: "fabric_exec", arguments: { code, payloads: { packet: JSON.stringify(packet) }, resultFormat: "json" } }]
        : [{ type: "text", text }], stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    const events = createAssistantMessageEventStream(); events.push({ type: "start", partial: message });
    if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    else {
      events.push({ type: "text_start", contentIndex: 0, partial: message });
      events.push({ type: "text_delta", contentIndex: 0, delta: text, partial: message });
      events.push({ type: "text_end", contentIndex: 0, content: text, partial: message });
    }
    events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message }); events.end(); return events;
  };
  pi.registerProvider({ id: model.provider, name: model.name, auth: { apiKey: { name: "Offline keyless proof", check: async () => ({ type: "api_key", source: "keyless fixture" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream });
  pi.on("before_agent_start", (event, ctx) => fs.appendFileSync(process.env.ABSENT_ROOT_RECEIPTS!, JSON.stringify({ type: "activation", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null,
    rootId: process.env.PI_FABRIC_MAIN_AGENT_ID, pid: process.pid, sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), prompt: event.prompt }) + "\n"));
}
