import assert from "node:assert/strict";
import { createAssistantMessageEventStream, getCurrentTools } from "@earendil-works/pi-ai";
import { Type } from "typebox";
export default function (pi) {
  pi.registerTool({ name: "entry_probe", label: "Entry", description: "Report the loaded Fabric registration source", parameters: Type.Object({}),
    async execute() {
      const tools = pi.getAllTools().filter(tool => tool.name === "fabric_exec");
      return { content: [{ type: "text", text: JSON.stringify(tools) }], details: {} };
    },
  });
  let calls = 0;
  pi.registerProvider("entry-offline", {
    api: "entry-offline", apiKey: "offline-fixture", baseUrl: "http://invalid.local",
    models: [{ id: "fixture", name: "Fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 2048 }],
    streamSimple(model, context) {
      assert.deepEqual(getCurrentTools(context.messages).map(tool => tool.name), ["fabric_exec"]);
      const first = calls++ === 0;
      const message = { role: "assistant", content: first ? [{ type: "toolCall", id: "entry-outer", name: "fabric_exec", arguments: { code: 'return await extensions.entry_probe({});' } }] : [{ type: "text", text: "entry-cli-ok" }],
        api: model.api, provider: model.provider, model: model.id,
        usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, timestamp: Date.now(), stopReason: first ? "toolUse" : "stop" };
      const stream = createAssistantMessageEventStream(); stream.push({ type: "start", partial: message });
      stream.push({ type: "done", reason: message.stopReason, message }); stream.end(); return stream;
    },
  });
}
