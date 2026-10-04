// Native CLI proof: only inference is synthetic. Pi, Fabric and all workers are real.
import fs from "node:fs";
import path from "node:path";
import { createAssistantMessageEventStream, type AssistantMessage, type Api, type Model, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const record = (value: unknown) => fs.appendFileSync(process.env.HANDOFF_PROOF_LOG!, JSON.stringify({ at: Date.now(), pid: process.pid, ...value as object }) + "\n");
export default function(pi: ExtensionAPI) {
  const model: Model<Api> = { provider: "handoff-proof", id: "offline", name: "Keyless child handoff proof", api: "handoff-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16384 };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const users = context.messages.filter(m => m.role === "user");
    const user = JSON.stringify(users.at(-1)?.content ?? "");
    const plainUser = users.at(-1)?.content;
    const prompt = typeof plainUser === "string" ? plainUser : Array.isArray(plainUser) ? plainUser.filter(p => p.type === "text").map(p => p.text).join("\n") : "";
    const actorId = process.env.PI_FABRIC_ACTOR_ID;
    const leaf = prompt.includes("HANDOFF_LEAF_");
    const last = context.messages.at(-1);
    let code: string | undefined;
    if (last?.role !== "toolResult") {
      if (!actorId && prompt.startsWith("HANDOFF_MAIN_CODE:\n")) code = prompt.slice("HANDOFF_MAIN_CODE:\n".length);
      else if (actorId && prompt.includes("BEGIN_HANDOFF:")) {
        const mode = prompt.includes("BEGIN_HANDOFF:bytes") ? "bytes" : "count";
        const count = mode === "bytes" ? 5 : 19;
        const marker = path.join(process.env.HANDOFF_PROOF_SCRATCH!, mode + "-fenced.json");
        code = `const children = []; for (let i = 0; i < ${count}; i++) children.push(await agents.spawn({ task: "HANDOFF_LEAF_${mode}:" + i, name: "proof-${mode}-" + i, transport: "process", model: "handoff-proof/offline" }));\nawait pi.bash({ cmd: "sleep 20", timeout: 30 });\nconst stopped = await agents.stop({ id: children[0].id });\nawait pi.write({ path: ${JSON.stringify(marker)}, text: JSON.stringify({ actor: (await agents.self()).id, children, stopped: { id: stopped.id, status: stopped.status }, at: Date.now() }) });\nawait pi.bash({ cmd: "sleep 60", timeout: 70 });\nreturn { unexpectedDelivery: true };`;
      }
    }
    const separator = "context only, not current activation facts):\n\n";
    let handoff: unknown[] = [];
    let handoffBytes = 0;
    if (prompt.includes(separator)) {
      const json = prompt.split(separator)[1]!;
      handoff = JSON.parse(json); handoffBytes = Buffer.byteLength(json, "utf8");
    }
    record({ type: "provider-call", actorId: actorId ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", leaf, user, handoff, handoffBytes, code });
    const text = leaf ? (prompt.includes("HANDOFF_LEAF_bytes") ? "界".repeat(4000) : "CHILD_COMPLETED") : "NATIVE_ACTIVATION_COMPLETED";
    const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: code ? [{ type: "toolCall", id: `handoff-${process.pid}-${Date.now()}`, name: "fabric_exec", arguments: { code, resultFormat: "json" } }] : [{ type: "text", text }],
      stopReason: code ? "toolUse" : "stop", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    events.push({ type: "start", partial: message });
    if (code) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: code ? "toolUse" : "stop", message }); events.end(message); return events;
  };
  pi.registerProvider({ id: "handoff-proof", name: "Native offline handoff proof",
    auth: { apiKey: { name: "Keyless offline fixture", check: async () => ({ type: "api_key", source: "keyless native proof" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream });
  pi.on("session_start", (_event, ctx) => record({ type: "native-session-start", mode: ctx.mode, actorId: process.env.PI_FABRIC_ACTOR_ID ?? null,
    runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), tools: pi.getActiveTools() }));
  pi.on("tool_call", event => { record({ type: "native-tool-call", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", event }); });
  pi.on("session_shutdown", () => record({ type: "native-session-shutdown", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main" }));
}
