import fs from "node:fs";
import { createAssistantMessageEventStream, getCurrentTools } from "@earendil-works/pi-ai";
import { Type } from "typebox";

const model = {
  provider: "spawn-extension-probe",
  id: "offline",
  name: "Offline spawn extension proof",
  api: "spawn-extension-probe-api",
  baseUrl: "http://invalid.local",
  reasoning: false,
  input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128000,
  maxTokens: 2048,
};

export default function (pi) {
  pi.registerTool({
    name: "probe_echo",
    label: "Probe echo",
    description: "Returns the supplied value for the isolated extension-path test.",
    parameters: Type.Object({ value: Type.String() }),
    async execute(_id, args) {
      fs.appendFileSync(process.env.FABRIC_EXTENSION_PROBE_EXECUTED, `${args.value}\n`);
      return { content: [{ type: "text", text: `probe-echo:${args.value}` }], details: {} };
    },
  });

  let calls = 0;
  const stream = (selected, context) => {
    const tools = getCurrentTools(context.messages).map(tool => tool.name);
    fs.appendFileSync(process.env.FABRIC_EXTENSION_PROBE_TOOLS, `${JSON.stringify(tools)}\n`);
    const result = context.messages.find(message => message.role === "toolResult");
    const content = typeof result?.content === "string"
      ? result.content
      : Array.isArray(result?.content) ? result.content.map(part => part.text ?? "").join("") : "";
    const callTool = calls++ === 0;
    const message = {
      role: "assistant",
      content: callTool
        ? [{ type: "toolCall", id: `probe-echo-${process.pid}`, name: "probe_echo", arguments: { value: "from-child" } }]
        : [{ type: "text", text: `extension-result:${content}` }],
      api: selected.api,
      provider: selected.provider,
      model: selected.id,
      usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
        cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      timestamp: Date.now(),
      stopReason: callTool ? "toolUse" : "stop",
    };
    const events = createAssistantMessageEventStream();
    events.push({ type: "start", partial: message });
    if (callTool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0], partial: message });
    events.push({ type: "done", reason: message.stopReason, message });
    events.end();
    return events;
  };

  pi.registerProvider({
    id: model.provider,
    name: model.name,
    auth: { apiKey: { name: "Offline fixture", check: async () => ({ type: "api_key", source: "test fixture" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model],
    stream,
    streamSimple: stream,
  });
}
