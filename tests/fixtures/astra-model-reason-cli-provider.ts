// Keyless inference for the real Pi CLI proof; Fabric and workers are not mocked.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const mainCode = `
const before = await agents.list({ scope: "local" });
const beforeActors = await agents.actors();
const refusals = [];
for (const action of ["run", "spawn", "create"]) {
  try {
    if (action === "create") await agents.create({ name: "refused-astra", instructions: "Never run", model: "cliproxyapi/gpt-6-astra" });
    else if (action === "run") await agents.run({ task: "REFUSED_ASTRA", model: "cliproxyapi/gpt-6-astra" });
    else await agents.spawn({ task: "REFUSED_ASTRA", model: "cliproxyapi/gpt-6-astra" });
    refusals.push({ action, admitted: true });
  } catch (error) { refusals.push({ action, message: error.message }); }
}
const after = await agents.list({ scope: "local" });
const afterActors = await agents.actors();
const modelReason = "  Explicit exception for the offline Astra CLI audit proof  ";
// Occupy the sole permit so the accepted spawn has an observable queued identity.
const blocker = await agents.spawn({ task: "DEFAULT_BLOCKER", transport: "process" });
const task = await agents.spawn({ task: "ASTRA_REASON_TASK", model: "cliproxyapi/gpt-6-astra", modelReason, transport: "process" });
await agents.subscribe({ from: task.id, events: ["run.spawned"], delivery: "followUp", triggerTurn: false });
const defaultResult = await agents.wait({ id: blocker.id, timeoutMs: 45000 });
const taskResult = await agents.wait({ id: task.id, timeoutMs: 45000 });
const actor = await agents.create({ name: "astra-reason-actor", instructions: "ASTRA_REASON_ACTOR", model: "cliproxyapi/gpt-6-astra", modelReason,
  thinking: "max", routeClass: "status-groom", protected: true, responseMode: "text", triggerTurn: false, transport: "process" });
await agents.subscribe({ from: actor.id, events: ["run.spawned"], delivery: "followUp", triggerTurn: false });
const activation = await agents.ask({ id: actor.id, message: "ASTRA_REASON_ACTOR" });
const beforeSetter = await agents.status({ id: actor.id });
try { await agents.setModel({ id: actor.id, model: "cliproxyapi/gpt-6-astra" }); refusals.push({ action: "setModel", admitted: true }); }
catch (error) { refusals.push({ action: "setModel", message: error.message }); }
const afterSetter = await agents.status({ id: actor.id });
const sessionReason = "  Explicit session-binding exception for the offline Astra CLI audit proof  ";
const rebound = await agents.setModel({ id: actor.id, model: "cliproxyapi/gpt-6-astra", modelReason: sessionReason });
const reboundActivation = await agents.ask({ id: actor.id, message: "ASTRA_REASON_ACTOR_REBOUND" });
const lifecycle = await mesh.read({ topic: "fabric.participant.lifecycle", limit: 100 });
return { refusals, before, after, beforeActors, afterActors, modelReason, task, taskResult, defaultResult,
  actor, activation, beforeSetter, afterSetter, sessionReason, rebound, reboundActivation, lifecycle };
`;

export default function (pi: ExtensionAPI) {
  const model: Model<Api> = {
    provider: "cliproxyapi", id: "gpt-6.1-sol", name: "Offline Astra audit proof", api: "astra-proof-api", baseUrl: "http://invalid.local",
    reasoning: true, thinkingLevelMap: { max: "max", xhigh: "xhigh" }, input: ["text"],
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096,
  };
  let mainIssued = false;
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const main = !process.env.PI_FABRIC_PARENT_RUN && JSON.stringify(context.messages).includes("ASTRA_CLI_MAIN");
    const tool = main && !mainIssued;
    if (tool) mainIssued = true;
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: `astra-proof-${Date.now()}`, name: "fabric_exec", arguments: { code: mainCode, resultFormat: "json" } }]
        : [{ type: "text", text: main ? "ASTRA_CLI_PROOF_COMPLETED" : "ASTRA_REASON_WORKER_COMPLETED" }],
      stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    if (!main && JSON.stringify(context.messages).includes("DEFAULT_BLOCKER")) {
      setTimeout(() => { events.push({ type: "done", reason: "stop", message }); events.end(); }, 10000);
      return events;
    }
    events.push({ type: "start", partial: message });
    if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
    events.end();
    return events;
  };
  pi.registerProvider({ id: "cliproxyapi", name: "Offline keyless Astra audit proof",
    auth: { apiKey: { name: "Keyless local test", check: async () => ({ type: "api_key", source: "keyless test" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model, { ...model, id: "gpt-6-astra" }], stream, streamSimple: stream,
  });
  pi.on("before_agent_start", (event, context) => {
    fs.appendFileSync(process.env.ASTRA_PROBE_RECEIPTS!, `${JSON.stringify({
      runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null,
      model: context.model ? `${context.model.provider}/${context.model.id}` : null,
      sessionId: context.sessionManager.getSessionId(), prompt: event.prompt,
    })}\n`);
  });
}
