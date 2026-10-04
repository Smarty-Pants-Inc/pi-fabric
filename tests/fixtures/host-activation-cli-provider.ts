// Only inference is deterministic: installed Pi, Fabric tools and workers are real.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Api, type Model, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const record = (value: object) => fs.appendFileSync(process.env.HOST_CAP_PROOF_LOG!, JSON.stringify({ at: Date.now(), pid: process.pid,
  workerPid: process.ppid, runId: process.env.PI_FABRIC_PARENT_RUN ?? null, actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, ...value }) + "\n");
export default function(pi: ExtensionAPI) {
  const model: Model<Api> = { provider: "host-cap-proof", id: "offline", name: "Keyless host activation proof", api: "host-cap-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 16384 };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const content = context.messages.filter(m => m.role === "user").at(-1)?.content;
    const prompt = typeof content === "string" ? content : Array.isArray(content) ? content.filter(p => p.type === "text").map(p => p.text).join("\n") : "";
    const afterTool = context.messages.at(-1)?.role === "toolResult";
    let code: string | undefined;
    if (!afterTool) {
      if (prompt.startsWith("HOST_CAP_MAIN_CODE:\n")) code = prompt.slice("HOST_CAP_MAIN_CODE:\n".length);
      else if (prompt.includes("HOST_CAP_PARENT_ASK:")) {
        const target = prompt.match(/HOST_CAP_PARENT_ASK:([a-f0-9]{32})/)![1]!;
        code = `const result = await agents.ask({id:${JSON.stringify(target)},message:"HOST_CAP_LEAF cross-root"}); if(result.error) throw new Error(result.error); return {parent:"completed",child:result};`;
      } else if (prompt.includes("HOST_CAP_PARENT_JOIN")) {
        code = 'const child = await agents.spawn({task:"HOST_CAP_LEAF joined",model:"host-cap-proof/offline",transport:"process"}); const result = await agents.wait({id:child.id}); if(result.status !== "completed") throw new Error(JSON.stringify(result)); return {parent:"completed",child:result};';
      } else if (prompt.includes("HOST_CAP_PARENT_RUN")) {
        code = 'const result = await agents.run({task:"HOST_CAP_LEAF run",model:"host-cap-proof/offline",transport:"process"}); if(result.status !== "completed") throw new Error(JSON.stringify(result)); return {parent:"completed",child:result};';
      }
    }
    let token: string | undefined; let held = false;
    try { token = fs.readlinkSync("/proc/self/fd/3"); held = fs.readFileSync("/proc/self/fdinfo/3", "utf8").includes("lock:"); } catch { /* Main has no slot. */ }
    record({ type: "provider-call", prompt, code, token, held });
    void (async () => {
      // Leaves stay live long enough for cross-root concurrency observations.
      if (!code && !afterTool && prompt.includes("HOST_CAP_LEAF")) await new Promise(resolve => setTimeout(resolve, 600));
      const message: AssistantMessage = { role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
        content: code ? [{ type: "toolCall", id: `host-cap-${process.pid}-${Date.now()}`, name: "fabric_exec", arguments: { code, resultFormat: "json" } }] : [{ type: "text", text: "HOST_CAP_COMPLETED" }],
        stopReason: code ? "toolUse" : "stop", usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      events.push({ type: "start", partial: message });
      if (code) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
      events.push({ type: "done", reason: code ? "toolUse" : "stop", message }); events.end(message);
    })();
    return events;
  };
  pi.registerProvider({ id: model.provider, name: model.name,
    auth: { apiKey: { name: "Keyless offline fixture", check: async () => ({ type: "api_key", source: "offline proof" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream });
  pi.on("session_start", (_event, ctx) => record({ type: "session-start", mode: ctx.mode, sessionId: ctx.sessionManager.getSessionId(), sessionFile: ctx.sessionManager.getSessionFile(), tools: pi.getActiveTools() }));
  pi.on("tool_call", event => record({ type: "tool-call", event }));
  pi.on("tool_result", event => record({ type: "tool-result", event }));
  pi.on("session_shutdown", () => record({ type: "session-shutdown" }));
}
