// Deterministic, keyless model transport for the real Pi CLI P0 reservation probe
// (smarty-dev#4440, pi-fabric#548). Only inference is synthetic: the Pi CLI, the built
// Fabric extension, its fabric_exec tool, the agents provider and the actor manager are real.
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

const mainCode = `
const actor = await agents.create({ name: "p0-cli", instructions: "Never activated by this probe", topics: ["github.demo"], triggerTurn: false, responseMode: "text" });
const createdAt = Date.now();
const subject = { repository: "smarty/demo", pr: 7, head: "a".repeat(40) };
const request = { ...subject, createdAt, expiresAt: createdAt + 600000, requiredSecurity: ["security"] };
const reserved = await agents.setActivationFilter({ id: actor.id, activationFilter: [{ id: "p0", topic: ["github.demo"] }], reservation: request });
const token = reserved.activationFilterReservationToken!;
const identity = { ...subject, generation: reserved.activationFilterReservation!.generation };
const refusals: Array<{ label: string; admitted?: true; message?: string }> = [];
const attempt = async (label: string, args: any) => {
  try { await agents.setActivationFilter(args); refusals.push({ label, admitted: true }); }
  catch (error) { refusals.push({ label, message: String((error as Error).message) }); }
};
await attempt("forged-observation", { id: actor.id, reservationToken: "forged-capability", observation: { ...identity, reviewTerminal: true, securityTerminal: ["security"] } });
await attempt("forged-closed", { id: actor.id, reservationToken: "forged-capability", observation: { ...identity, prState: "closed" } });
await attempt("unscoped-replace", { id: actor.id, activationFilter: [{ id: "open", topic: ["other.demo"] }] });
await attempt("unscoped-clear", { id: actor.id, activationFilter: [] });
const afterRefusals = await agents.actorStatus({ id: actor.id });
const listed = await agents.actors();
const review = await agents.setActivationFilter({ id: actor.id, reservationToken: token, observation: { ...identity, reviewTerminal: true } });
const receipt = await agents.setActivationFilter({ id: actor.id, reservationToken: token, observation: { ...identity, securityTerminal: ["security"] } });
const readback = await agents.actorStatus({ id: actor.id });
await agents.remove({ id: actor.id });
const { activationFilterReservationToken: _issued, ...reservedInfo } = reserved;
return { request, reserved: reservedInfo, tokenIssued: typeof token === "string" && token.length >= 32,
  tokenInStatusOrList: JSON.stringify([afterRefusals, listed]).includes(token) || JSON.stringify([afterRefusals, listed]).includes("TokenSha256"),
  refusals, afterRefusals, review, receipt, readback };
`;

export default function (pi: ExtensionAPI) {
  const model: Model<Api> = {
    provider: "p0-probe", id: "offline", name: "Offline P0 reservation probe", api: "p0-probe-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 200000, maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const last = context.messages.at(-1);
    const tool = last?.role !== "toolResult";
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: tool ? [{ type: "toolCall", id: `p0-probe-${Date.now()}`, name: "fabric_exec", arguments: { code: mainCode, resultFormat: "json" } }]
        : [{ type: "text", text: "P0_PROBE_DONE" }],
      stopReason: tool ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    events.push({ type: "start", partial: message });
    if (tool) events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    events.push({ type: "done", reason: message.stopReason as "stop" | "toolUse", message });
    events.end();
    return events;
  };
  pi.registerProvider({ id: "p0-probe", name: "Offline keyless P0 probe",
    auth: { apiKey: { name: "Keyless local test", check: async () => ({ type: "api_key", source: "keyless test" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
}
