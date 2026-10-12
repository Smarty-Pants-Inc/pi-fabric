// Native CLI proof (pi-fabric#864): only inference is synthetic. Pi, Fabric, the worker and the child are real.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Api, type Model, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const record = (value: object) => fs.appendFileSync(process.env.CHILD_EXTENSION_PROOF_LOG!, JSON.stringify({ at: Date.now(), pid: process.pid, ...value }) + "\n");
const MAIN_MARKER = "CHILD_EXTENSION_MAIN_CODE:\n";
export default function(pi: ExtensionAPI) {
  const model: Model<Api> = { provider: "child-extension-proof", id: "offline", name: "Keyless child extension proof", api: "child-extension-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16384 };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const users = context.messages.filter(m => m.role === "user");
    const content = users.at(-1)?.content;
    const prompt = typeof content === "string" ? content : Array.isArray(content) ? content.filter(p => p.type === "text").map(p => p.text).join("\n") : "";
    const child = Boolean(process.env.PI_FABRIC_PARENT_RUN);
    const answered = context.messages.at(-1)?.role === "toolResult";
    let code: string | undefined;
    if (!answered) {
      if (!child && prompt.startsWith(MAIN_MARKER)) code = prompt.slice(MAIN_MARKER.length);
      else if (child) code = "return 'CHILD_FABRIC_EXEC_OK';";
    }
    record({ type: "provider-call", child, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", answered, code });
    const text = child ? "CHILD_COMPLETED" : "MAIN_COMPLETED";
    const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: code ? [{ type: "toolCall", id: `child-extension-${process.pid}-${Date.now()}`, name: "fabric_exec", arguments: { code, resultFormat: "json" } }] : [{ type: "text", text }],
      stopReason: code ? "toolUse" : "stop", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    events.push({ type: "start", partial: message });
    if (code) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: code ? "toolUse" : "stop", message }); events.end(message); return events;
  };
  pi.registerProvider({ id: "child-extension-proof", name: "Native offline child extension proof",
    auth: { apiKey: { name: "Keyless offline fixture", check: async () => ({ type: "api_key", source: "keyless native proof" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream });
  pi.on("session_start", (_event, ctx) => record({ type: "native-session-start", mode: ctx.mode, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", argv: process.argv.slice(2), tools: pi.getActiveTools() }));
  pi.on("tool_call", event => record({ type: "native-tool-call", runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", tool: event.toolName }));
}
