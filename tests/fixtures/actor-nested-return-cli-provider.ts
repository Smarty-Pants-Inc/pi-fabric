// Native actor/task return proof: only inference is synthetic. Pi and Fabric workers are real.
import fs from "node:fs";
import path from "node:path";
import { createAssistantMessageEventStream, type AssistantMessage, type Api, type Model, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const record = (value: unknown) => fs.appendFileSync(process.env.NESTED_PROOF_LOG!, JSON.stringify({ at: Date.now(), pid: process.pid, ...value as object }) + "\n");
export default function(pi: ExtensionAPI) {
  const model: Model<Api> = { provider: "nested-proof", id: "offline", name: "Keyless nested return proof", api: "nested-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16384 };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const users = context.messages.filter(m => m.role === "user");
    const user = JSON.stringify(users.at(-1)?.content ?? "");
    const plainUser = users.at(-1)?.content;
    const prompt = typeof plainUser === "string" ? plainUser : Array.isArray(plainUser) ? plainUser.filter(p => p.type === "text").map(p => p.text).join("\n") : "";
    const actorId = process.env.PI_FABRIC_ACTOR_ID;
    const leaf = prompt.includes("NESTED_LEAF_TASK");
    const last = context.messages.at(-1);
    let code: string | undefined;
    if (last?.role !== "toolResult") {
      if (!actorId && prompt.startsWith("NESTED_MAIN_CODE:\n")) code = prompt.slice("NESTED_MAIN_CODE:\n".length);
      else if (actorId && prompt.includes("BEGIN_NESTED_RETURN")) {
        const marker = path.join(process.env.NESTED_PROOF_SCRATCH!, "actor-fenced.json");
        code = `const owner = await agents.main(); const self = await agents.self(); const child = await agents.spawn({ task: "NESTED_LEAF_TASK", name: "nested-proof-child", transport: "process", model: "nested-proof/offline" }); await pi.write({ path: ${JSON.stringify(marker)}, text: JSON.stringify({owner, self, child}) }); await pi.bash({ cmd: "sleep 60", timeout: 70 }); return "UNEXPECTED_FENCE_RELEASE";`;
      } else if (actorId && prompt.includes("CHECK_OWNING_MAIN")) {
        const marker = path.join(process.env.NESTED_PROOF_SCRATCH!, "owner-guard.json");
        code = `const owner = await agents.main(); const self = await agents.self(); const byId = await agents.steer({ id: owner.id, message: "ACTOR_OWNER_STEER_BY_ID" }); const byAlias = await agents.steer({ id: "main", message: "ACTOR_OWNER_STEER_BY_ALIAS" }); const guard = {owner,self,byId,byAlias}; await pi.write({ path: ${JSON.stringify(marker)}, text: JSON.stringify(guard) }); return guard;`;
      } else if (leaf) {
        const marker = path.join(process.env.NESTED_PROOF_SCRATCH!, "leaf-result.json");
        code = `const owner = await agents.main(); const spawner = await agents.spawner(); const slice = await agents.followUp({ id: "main", message: "NESTED_SLICE_TO_SPAWNER" }); await pi.write({path:${JSON.stringify(marker)},text:JSON.stringify({owner,spawner,slice})}); return {owner,spawner,slice};`;
      }
    }
    record({ type: "provider-call", actorId: actorId ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", leaf, user, code });
    const text = leaf ? "NESTED_CHILD_COMPLETED" : "NATIVE_ACTIVATION_COMPLETED";
    const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: code ? [{ type: "toolCall", id: `handoff-${process.pid}-${Date.now()}`, name: "fabric_exec", arguments: { code, resultFormat: "json" } }] : [{ type: "text", text }],
      stopReason: code ? "toolUse" : "stop", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
    events.push({ type: "start", partial: message });
    if (code) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: code ? "toolUse" : "stop", message }); events.end(message); return events;
  };
  pi.registerProvider({ id: "nested-proof", name: "Native offline nested return proof",
    auth: { apiKey: { name: "Keyless offline fixture", check: async () => ({ type: "api_key", source: "keyless native proof" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream });
  pi.on("session_start", (_event, ctx) => record({ type: "native-session-start", mode: ctx.mode, actorId: process.env.PI_FABRIC_ACTOR_ID ?? null,
    runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), tools: pi.getActiveTools() }));
  pi.on("tool_call", event => { record({ type: "native-tool-call", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", event }); });
  pi.on("session_shutdown", () => record({ type: "native-session-shutdown", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? "main" }));
}
