// Model/provider boundary only. All Main, resident, worker and Fabric paths stay real.
import fs from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
export default function (pi: ExtensionAPI) {
  pi.registerProvider("router-proof", {
    baseUrl: process.env.ROUTER_PROOF_ENDPOINT!, api: "openai-completions", apiKey: "offline-proof-not-a-credential",
    models: ["gpt-5-pin", "gpt-5-cheap"].map(id => ({ id, name: id, reasoning: true, input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096 })),
  });
  pi.on("before_agent_start", (event, context) => {
    const processStartTime = process.platform === "linux" ? (() => {
      const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
      return stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
    })() : undefined;
    fs.appendFileSync(process.env.ROUTER_PROOF_TRANSCRIPT!, JSON.stringify({ type: "native_activation", pid: process.pid, processStartTime,
      actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, runId: process.env.PI_FABRIC_PARENT_RUN ?? null,
      runDirectory: process.env.PI_FABRIC_AGENT_RUN_DIR ?? null, sessionId: context.sessionManager.getSessionId(),
      sessionFile: context.sessionManager.getSessionFile(), mode: context.mode,
      model: context.model ? `${context.model.provider}/${context.model.id}` : null, thinking: pi.getThinkingLevel(), prompt: event.prompt }) + "\n");
  });
}
