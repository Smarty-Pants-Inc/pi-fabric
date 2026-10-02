import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";

// Real Pi executes its native bash tool; this deterministic provider never makes a network request.
export default function (pi: ExtensionAPI) {
  pi.registerProvider("route-cwd-probe", {
    baseUrl: "http://127.0.0.1:1", apiKey: "offline-probe", api: "route-cwd-probe-api",
    models: [{ id: "pinned", name: "Pinned", reasoning: true, input: ["text"],
      contextWindow: 200000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } }],
    streamSimple(model, context) {
      const result = context.messages.find(message => message.role === "toolResult");
      const message: AssistantMessage = {
        role: "assistant", api: model.api, provider: model.provider, model: model.id,
        content: result ? [{ type: "text", text: JSON.stringify(result.content) }]
          : [{ type: "toolCall", id: "cwd-probe", name: "bash", arguments: { command: `${JSON.stringify(process.execPath)} -e "console.log(process.cwd()); console.log(require('node:fs').readFileSync('route-cwd.txt','utf8'))"` } }],
        stopReason: result ? "stop" : "toolUse", timestamp: Date.now(),
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
      };
      const stream = createAssistantMessageEventStream();
      stream.push({ type: "start", partial: message });
      stream.push({ type: "done", reason: result ? "stop" : "toolUse", message });
      stream.end();
      return stream;
    },
  });
}
