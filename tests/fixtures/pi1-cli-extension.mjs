import assert from "node:assert/strict";
import { createAssistantMessageEventStream, getCurrentTools } from "@earendil-works/pi-ai";
import { Type } from "typebox";
export default function (pi) {
  pi.on("session_start", () => pi.registerTool({ name: "fixture_cli_start", label: "Late", description: "Late CLI tool", parameters: Type.Object({}), async execute() { return { content: [{ type: "text", text: "late-cli-ok" }], details: {} }; } }));
  pi.registerTool({
    name: "fixture_cli", label: "Fixture", description: "CLI captured tool", parameters: Type.Object({}),
    async execute(_id, _args, _signal, _update, ctx) {
      assert.equal(typeof ctx.executeTool, "function");
      pi.setActiveTools(["codemode", "tool_search", "fixture_cli_start"]);
      assert.ok(ctx.tools.some(t => t.name === "fixture_cli_start"), JSON.stringify({ registered: pi.getAllTools().map(t => t.name), active: pi.getActiveTools() }));
      return { content: [{ type: "text", text: "cli-capture-ok" }], details: {} };
    },
  });
  let calls = 0;
  pi.registerProvider("offline-cli", {
    api: "offline-cli", apiKey: "offline", baseUrl: "http://invalid.local",
    models: [{ id: "fixture", name: "Fixture", reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 2048 }],
    streamSimple(model, context) {
      assert.deepEqual(getCurrentTools(context.messages).map((t) => t.name), ["fabric_exec"]);
      const first = calls++ === 0;
      const message = { role: "assistant", content: first ? [{ type: "toolCall", id: "cli-outer", name: "fabric_exec", arguments: { code: 'return await extensions.fixture_cli({});' } }] : [{ type: "text", text: "cli-smoke-ok" }], api: model.api, provider: model.provider, model: model.id, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, timestamp: Date.now(), stopReason: "pending" };
      const stream = createAssistantMessageEventStream(); stream.push({ type: "start", partial: message });
      message.stopReason = first ? "toolUse" : "stop";
      stream.push({ type: "done", reason: message.stopReason, message }); stream.end(); return stream;
    },
  });
}
