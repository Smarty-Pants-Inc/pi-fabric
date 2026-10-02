// Keyless deterministic model transport for the real Pi Main CLI inbox proof.
// No session/host-capability mocks: follow-ups use the built Fabric's public API.
import fs from "node:fs";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const seedCode = `
const main = await agents.main();
const receipts = [];
for (let index = 0; index < 40; index++) {
  receipts.push(await agents.followUp({ id: main.id, message: "CLI delivered follow-up " + index,
    data: { ref: "ticket:shared", deliveryId: "CLI-D" + index } }));
}
return { main, receipts };
`;
export default function (pi: ExtensionAPI) {
  let seeded = false;
  const model: Model<Api> = {
    provider: "inbox-proof", id: "offline", name: "Keyless inbox CLI proof", api: "inbox-proof-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 1000000, maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const lastUser = context.messages.filter(message => message.role === "user").at(-1);
    const tool = !seeded && JSON.stringify(lastUser).includes("INBOX_SEED");
    seeded ||= tool;
    fs.appendFileSync(process.env.INBOX_PROOF_INFERENCES!, JSON.stringify({ at: Date.now(), tool,
      inbox: context.messages.map(message => JSON.stringify(message.content)).filter(content => content.includes("<fabric-inbox")) }) + "\n");
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: "inbox-cli-seed", name: "fabric_exec", arguments: { code: seedCode, resultFormat: "json" } }]
        : [{ type: "text", text: "CLI_PROOF_ACK" }],
      stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    events.push({ type: "start", partial: message });
    if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
    events.end();
    return events;
  };
  pi.registerProvider({ id: "inbox-proof", name: "Keyless local proof",
    auth: { apiKey: { name: "No credentials", check: async () => ({ type: "api_key", source: "keyless proof" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
  pi.on("session_start", (_event, ctx) => {
    fs.appendFileSync(process.env.INBOX_PROOF_HOST!, JSON.stringify({ sessionId: ctx.sessionManager.getSessionId(), mode: ctx.mode,
      capabilities: (pi as unknown as { hostCapabilities?: unknown }).hostCapabilities,
      testCapabilityOverride: Boolean((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.test.hostCapabilities")]),
      isPromptPending: typeof (ctx as unknown as { isPromptPending?: unknown }).isPromptPending,
    }) + "\n");
  });
  pi.registerCommand("inbox-proof-pad", { description: "Append native non-context history beyond the compatibility window", handler: async () => {
    for (let index = 0; index < 600; index++) pi.appendEntry("inbox-proof-padding", { index });
  } });
}
