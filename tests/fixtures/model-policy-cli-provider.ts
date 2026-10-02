// Deterministic, keyless model transport for the real Pi CLI policy probe.
// Only inference is synthetic: Pi, Fabric, RPC admission and workers are real.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const mainCode = `
const before = await agents.list({ scope: "local" });
const beforeActors = await agents.actors();
const refusals = [];
for (const action of ["spawn", "create"]) {
  try {
    if (action === "spawn") await agents.spawn({ task: "DENIED_TASK", model: "policy-probe/denied" });
    else await agents.create({ name: "DENIED_ACTOR", instructions: "Never run", model: "policy-probe/denied" });
    refusals.push({ action, admitted: true });
  } catch (error) { refusals.push({ action, name: error.name, code: error.code, message: error.message }); }
}
const afterDenied = await agents.list({ scope: "local" });
const afterDeniedActors = await agents.actors();
const parent = await agents.spawn({ task: "POLICY_TASK_PARENT", recursive: true, transport: "process" });
const taskResult = await agents.wait({ id: parent.id });
const actor = await agents.create({ name: "policy-parent", instructions: "POLICY_ACTOR_PARENT", responseMode: "text", triggerTurn: false, transport: "process" });
const actorResult = await agents.ask({ id: actor.id, message: "POLICY_ACTOR_PARENT" });
await agents.remove({ id: actor.id });
return { before, afterDenied, beforeActors, afterDeniedActors, refusals, parent, taskResult, actor, actorResult };
`;
const parentCode = (kind: string) => `
const child = await agents.spawn({ task: "POLICY_LEAF_${kind}", transport: "process" });
const result = await agents.wait({ id: child.id });
return { kind: "${kind}", child, result };
`;

export default function (pi: ExtensionAPI) {
  const model: Model<Api> = {
    provider: "policy-probe", id: "allowed", name: "Offline policy probe", api: "policy-probe-api", baseUrl: "http://invalid.local",
    reasoning: true, thinkingLevelMap: { max: "max", xhigh: "xhigh" }, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const last = context.messages.at(-1);
    const prompt = JSON.stringify(context.messages.filter((message) => message.role === "user"));
    const leaf = prompt.includes("POLICY_LEAF_");
    const main = prompt.includes("POLICY_MAIN");
    const actor = prompt.includes("POLICY_ACTOR_PARENT");
    const tool = last?.role !== "toolResult" && !leaf;
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: `probe-${Date.now()}`, name: "fabric_exec", arguments: { code: main ? mainCode : parentCode(actor ? "ACTOR" : "TASK"), resultFormat: "json" } }]
        : [{ type: "text", text: leaf ? "POLICY_LEAF_COMPLETED" : "POLICY_PARENT_COMPLETED" }],
      stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    events.push({ type: "start", partial: message });
    if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
    events.end();
    return events;
  };
  pi.registerProvider({ id: "policy-probe", name: "Offline keyless policy probe",
    auth: { apiKey: { name: "Keyless local test", check: async () => ({ type: "api_key", source: "keyless test" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model, { ...model, id: "denied" }], stream, streamSimple: stream,
  });
  pi.on("before_agent_start", (event, context) => {
    const receipt = {
      runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null,
      depth: Number(process.env.PI_FABRIC_DEPTH ?? "0"), sessionId: context.sessionManager.getSessionId(),
      model: context.model ? `${context.model.provider}/${context.model.id}` : null, thinking: pi.getThinkingLevel(),
      prompt: event.prompt, nativeChanges: context.sessionManager.getEntries().filter((entry) => entry.type === "model_change" || entry.type === "thinking_level_change"),
    };
    fs.appendFileSync(process.env.POLICY_PROBE_RECEIPTS!, `${JSON.stringify(receipt)}\n`);
  });
}
