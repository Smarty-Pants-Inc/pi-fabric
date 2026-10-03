// Inference-only fixture: native Main, resident host, workers, queues and Fabric API stay real.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const mainCode = `
const actor = await agents.create({ name: "reset-proof-resident", instructions: "KEEP_PERSONA: handle each event and retain the objective.",
  residency: "durable", transport: "process", model: "resident-reset-proof/offline", thinking: "off", tools: [], extensions: true,
  events: [], topics: ["reset.proof.events"], delivery: "mailbox", triggerTurn: false, responseMode: "text", inferenceContext: "full-history" });
await agents.tell({ id: actor.id, message: "HELD_EVENT" });
let active;
for (let attempt = 0; attempt < 200; attempt++) {
  active = await agents.actorStatus({ id: actor.id });
  if (active.status === "running") break;
  await pi.bash({ cmd: "sleep 0.05" });
}
if (active.status !== "running") throw new Error("Resident never entered its held activation");
await mesh.publish({ topic: "reset.proof.events", kind: "proof", data: { marker: "QUEUED_EVENT" } });
let before;
for (let attempt = 0; attempt < 200; attempt++) {
  before = await agents.actorStatus({ id: actor.id });
  if (before.queued > 0) break;
  await pi.bash({ cmd: "sleep 0.05" });
}
if (!before.queued) throw new Error("Topic event did not enter the durable mailbox");
const reset = await agents.resetSession({ id: actor.id });
const waitHandled = async marker => {
  for (let attempt = 0; attempt < 200; attempt++) {
    const messages = await agents.messages({ id: actor.id, limit: 50 });
    if (messages.some(message => message.direction === "out" && message.text === "HANDLED_" + marker)) return messages;
    await pi.bash({ cmd: "sleep 0.05" });
  }
  throw new Error("Resident did not handle " + marker);
};
const retainedMessages = await waitHandled("QUEUED_EVENT");
await mesh.publish({ topic: "reset.proof.events", kind: "proof", data: { marker: "NEW_EVENT" } });
const messages = await waitHandled("NEW_EVENT");
const after = await agents.actorStatus({ id: actor.id });
return { actor, before, reset, after, retainedMessages, messages };
`;

export default function (pi: ExtensionAPI) {
  const model: Model<Api> = {
    provider: "resident-reset-proof", id: "offline", name: "Keyless resident reset proof", api: "resident-reset-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096,
  };
  const receipt = (value: unknown) => fs.appendFileSync(process.env.RESIDENT_RESET_TRANSCRIPT!, JSON.stringify(value) + "\n");
  const stream = (selected: Model<Api>, context: TranscriptContext, options?: { signal?: AbortSignal }) => {
    const output = createAssistantMessageEventStream();
    const last = context.messages.at(-1);
    const prompt = JSON.stringify([...context.messages].reverse().find(message => message.role === "user"));
    const main = !process.env.PI_FABRIC_ACTOR_ID && prompt.includes("RESET_MAIN");
    const tool = main && last?.role !== "toolResult";
    const marker = ["NEW_EVENT", "QUEUED_EVENT", "HELD_EVENT"].find(value => prompt.includes(value)) ?? "MAIN_COMPLETE";
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: `reset-proof-${Date.now()}`, name: "fabric_exec", arguments: { code: mainCode, resultFormat: "json" } }]
        : [{ type: "text", text: "HANDLED_" + marker }], stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    void (async () => {
      try {
        if (process.env.PI_FABRIC_ACTOR_ID && marker === "HELD_EVENT") {
          const deadline = Date.now() + 25000;
          receipt({ type: "held_provider", actorId: process.env.PI_FABRIC_ACTOR_ID, runId: process.env.PI_FABRIC_PARENT_RUN, pid: process.pid });
          while (!fs.existsSync(process.env.RESIDENT_RESET_RELEASE!)) {
            if (options?.signal?.aborted) throw new Error("Proof activation aborted");
            if (Date.now() >= deadline) throw new Error("Proof release deadline exceeded");
            await new Promise(resolve => setTimeout(resolve, 25));
          }
        }
        output.push({ type: "start", partial: message });
        if (tool) output.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
        else {
          output.push({ type: "text_start", contentIndex: 0, partial: message });
          output.push({ type: "text_delta", contentIndex: 0, delta: "HANDLED_" + marker, partial: message });
          output.push({ type: "text_end", contentIndex: 0, content: "HANDLED_" + marker, partial: message });
        }
        output.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message }); output.end();
      } catch (error) {
        message.stopReason = options?.signal?.aborted ? "aborted" : "error";
        message.errorMessage = String(error);
        output.push({ type: "error", reason: message.stopReason, error: message }); output.end();
      }
    })();
    return output;
  };
  pi.registerProvider({ id: "resident-reset-proof", name: model.name,
    auth: { apiKey: { name: "Keyless offline proof", check: async () => ({ type: "api_key", source: "keyless fixture" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
  pi.on("before_agent_start", (event, context) => receipt({ type: "native_activation", actorId: process.env.PI_FABRIC_ACTOR_ID ?? null,
    runId: process.env.PI_FABRIC_PARENT_RUN ?? "main", pid: process.pid, sessionId: context.sessionManager.getSessionId(),
    sessionFile: context.sessionManager.getSessionFile(), model: context.model ? `${context.model.provider}/${context.model.id}` : null,
    thinking: pi.getThinkingLevel(), prompt: event.prompt }));
}
