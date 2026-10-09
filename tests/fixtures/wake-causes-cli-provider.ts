// Deterministic/keyless MODEL TRANSPORT for a real, persisted Pi RPC CLI.
// The candidate index owns capture registration. This fixture never registers it,
// constructs a host/session mock, edits a session file, or launches a participant.
// /wake-replay injects calls to the candidate's COMPILED sendFabricMessage producer;
// actor/inbox/host-event are producer-boundary injections, NOT actor/DB executions.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { sendFabricMessage as Send, fabricWakeCause as Cause } from "../../src/fabric-provenance.js";

const log = (file: string | undefined, data: unknown): void => {
  if (!file) throw new Error("Wake replay log path is required");
  fs.appendFileSync(file, JSON.stringify({ at: Date.now(), ...data as object }) + "\n");
};

// esbuild shares host metadata between index and its lazy runtime in an existing
// chunk. Resolve its actual exported names, not src/ (which would create a second
// WeakMap and lose the candidate's session-owned capture identity).
async function producers(): Promise<{ send: typeof Send; cause: typeof Cause; module: string }> {
  const candidate = process.env.WAKE_REPLAY_CANDIDATE;
  if (!candidate) throw new Error("WAKE_REPLAY_CANDIDATE is required");
  const root = path.dirname(candidate);
  const chunks = path.join(root, "chunks");
  const files = [candidate, ...fs.readdirSync(chunks).filter(name => name.endsWith(".js")).sort().map(name => path.join(chunks, name))];
  for (const file of files) {
    const text = fs.readFileSync(file, "utf8");
    const start = text.lastIndexOf("\nexport {");
    if (start < 0) continue;
    const exports = text.slice(start, text.indexOf("};", start) + 2);
    if (!/\bsendFabricMessage\b/.test(exports) || !/\bfabricWakeCause\b/.test(exports)) continue;
    const module = await import(pathToFileURL(file).href);
    if (typeof module.sendFabricMessage === "function" && typeof module.fabricWakeCause === "function" && typeof module.registerFabricWakeCapture === "function") {
      return { send: module.sendFabricMessage, cause: module.fabricWakeCause, module: file };
    }
  }
  throw new Error("Built candidate lacks exported wake producers/capture. Build the parent implementation first.");
}

export default function (pi: ExtensionAPI): void {
  let producerPromise: ReturnType<typeof producers> | undefined;
  let bootstrapped = false;
  const model: Model<Api> = {
    provider: "wake-replay", id: "offline", name: "Keyless wake replay", api: "wake-replay-api", baseUrl: "http://invalid.local",
    reasoning: false, input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 1000000, maxTokens: 4096,
  };
  const stream = (selected: Model<Api>, context: TranscriptContext) => {
    const events = createAssistantMessageEventStream();
    const lastUser = context.messages.filter(message => message.role === "user").at(-1);
    const bootstrap = !bootstrapped && JSON.stringify(lastUser).includes("WAKE_REPLAY_BOOTSTRAP");
    bootstrapped ||= bootstrap;
    log(process.env.WAKE_REPLAY_INFERENCES, { bootstrap, messages: context.messages });
    const message: AssistantMessage = {
      role: "assistant", provider: selected.provider, model: selected.id, api: selected.api, timestamp: Date.now(),
      content: bootstrap ? [{ type: "toolCall", id: "wake-bootstrap", name: "fabric_exec", arguments: { code: "return await agents.main();", resultFormat: "json" } }]
        : [{ type: "text", text: "WAKE_REPLAY_ACK" }], stopReason: bootstrap ? "toolUse" : "stop",
      usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
    };
    events.push({ type: "start", partial: message });
    if (bootstrap) {
      events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
    } else {
      events.push({ type: "text_start", contentIndex: 0, partial: message });
      events.push({ type: "text_delta", contentIndex: 0, delta: "WAKE_REPLAY_ACK", partial: message });
      events.push({ type: "text_end", contentIndex: 0, content: "WAKE_REPLAY_ACK", partial: message });
    }
    events.push({ type: "done", reason: bootstrap ? "toolUse" : "stop", message });
    events.end();
    return events;
  };
  pi.registerProvider({ id: "wake-replay", name: "Keyless local replay",
    auth: { apiKey: { name: "No credentials", check: async () => ({ type: "api_key", source: "keyless replay" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
  pi.on("session_start", (_event, ctx) => log(process.env.WAKE_REPLAY_HOST, {
    event: "session_start", sessionId: ctx.sessionManager.getSessionId(), mode: ctx.mode,
    capabilities: (pi as unknown as { hostCapabilities?: unknown }).hostCapabilities,
    testCapabilityOverride: Boolean((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.test.hostCapabilities")]),
    isPromptPending: typeof (ctx as unknown as { isPromptPending?: unknown }).isPromptPending,
  }));
  pi.on("agent_settled", event => log(process.env.WAKE_REPLAY_HOST, { event: "agent_settled", outcome: (event as unknown as { outcome?: string }).outcome }));
  pi.registerCommand("wake-replay", { description: "Replay compiled producer boundary (not an actor or database execution)", handler: async (args, ctx) => {
    const { send, cause, module } = await (producerPromise ??= producers());
    const host = { id: `session:${ctx.sessionManager.getSessionId()}`, name: "main", kind: "main" as const };
    const actor = { id: "wake-replay:actor", name: "Replay actor", kind: "actor" as const };
    log(process.env.WAKE_REPLAY_HOST, { event: "injection", case: args, producerModule: module, idle: ctx.isIdle() });
    if (args === "passive") {
      send(pi, { customType: "pi-fabric-replay-passive", content: "PASSIVE_TRIGGER_FALSE", display: true }, { deliverAs: "followUp", triggerTurn: false });
      send(pi, { customType: "pi-fabric-replay-next-turn", content: "PASSIVE_NEXT_TURN", display: true }, { deliverAs: "nextTurn", triggerTurn: false }, actor, "actor", "mesh");
      pi.appendEntry("wake-replay-passive-mailbox", { text: "PASSIVE_APPEND_ENTRY" });
      return;
    }
    if (args === "explicit") {
      // Exercises the optional FINAL eighth argument, not message payload authority.
      send(pi, { customType: "pi-fabric-replay-explicit", content: "WAKE_EXPLICIT", display: true },
        { deliverAs: "followUp", triggerTurn: true }, undefined, "actor", undefined, undefined,
        cause(host, "host-event", "replay.explicit", "EXPLICIT-KEY"));
      return;
    }
    if (args === "actor") {
      send(pi, { customType: "pi-fabric-actor", content: "WAKE_ACTOR", display: true },
        { deliverAs: "followUp", triggerTurn: true }, actor, "actor", "mesh");
      return;
    }
    if (args === "inbox" || args === "host-event") {
      // A fixture's ExtensionAPI differs from production index's session-owned API.
      // Attribute this injection explicitly; unit tests cover production host fallback.
      send(pi, { customType: args === "inbox" ? "pi-fabric-agent-complete" : "pi-fabric-replay-host-event", content: `WAKE_${args}`, display: true },
        { deliverAs: "followUp", triggerTurn: true }, undefined, "actor", undefined, undefined, cause(host, args));
      return;
    }
    if (args !== "ready") throw new Error(`Unknown wake replay case: ${args}`);
  } });
}
