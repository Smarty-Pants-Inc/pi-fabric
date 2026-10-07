import assert from "node:assert/strict";
import { createAssistantMessageEventStream, getCurrentTools } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { fileURLToPath } from "node:url";
export default function (pi) {
  pi.registerMcpServer("fixture", { command: process.execPath, args: [fileURLToPath(new URL("./pi1-mcp.mjs", import.meta.url))], exposure: "direct" });
  pi.on("session_start", () => pi.registerTool({ name: "fixture_cli_start", label: "Late", description: "Late CLI tool", parameters: Type.Object({}), async execute() { return { content: [{ type: "text", text: "late-cli-ok" }], details: {} }; } }));
  pi.registerTool({
    name: "fixture_cli", label: "Fixture", description: "CLI captured tool", parameters: Type.Object({}),
    async execute(_id, _args, _signal, _update, ctx) {
      assert.equal(typeof ctx.executeTool, "function");
      pi.setActiveTools(["codemode", "tool_search", "fixture_cli_start", "mcp__fixture__echo"]);
      assert.ok(ctx.tools.some(t => t.name === "mcp__fixture__echo"), JSON.stringify({ registered: pi.getAllTools().map(t => t.name), active: pi.getActiveTools(), servers: pi.getMcpServers() }));
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
      const message = { role: "assistant", content: first ? [{ type: "toolCall", id: "cli-outer", name: "fabric_exec", arguments: { code: 'const a = await extensions.fixture_cli({}); const b = await extensions.mcp__fixture__echo({value:"cli-native"}); return {a,b};' } }] : [{ type: "text", text: "cli-smoke-ok" }], api: model.api, provider: model.provider, model: model.id, usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } }, timestamp: Date.now(), stopReason: "pending" };
      const stream = createAssistantMessageEventStream(); stream.push({ type: "start", partial: message });
      message.stopReason = first ? "toolUse" : "stop";
      stream.push({ type: "done", reason: message.stopReason, message }); stream.end(); return stream;
    },
  });
}
