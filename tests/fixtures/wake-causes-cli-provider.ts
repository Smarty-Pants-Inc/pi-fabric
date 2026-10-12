// Deterministic/keyless MODEL TRANSPORT for a real, persisted Pi RPC CLI.
// Normal replay: candidate index owns capture. Raw-probe mode loads ONLY this
// fixture and registers exact compiled capture on its real native ExtensionAPI.
// Neither mode mocks a host/session, edits session JSONL, or launches a participant.
// /wake-replay injects calls to the candidate's COMPILED sendFabricMessage producer;
// actor/inbox/host-event are producer-boundary injections, NOT actor/DB executions.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { createAssistantMessageEventStream, type AssistantMessage, type Model, type Api, type TranscriptContext } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { sendFabricMessage as Send, sendFabricUserMessage as RawUser, registerFabricWakeCapture as Capture, fabricWakeCause as Cause } from "../../src/fabric-provenance.js";

const log = (file: string | undefined, data: unknown): void => {
  if (!file) throw new Error("Wake replay log path is required");
  fs.appendFileSync(file, JSON.stringify({ at: Date.now(), ...data as object }) + "\n");
};

// esbuild shares host metadata between index and its lazy runtime in an existing
// chunk. Resolve its actual exported names, not src/ (which would create a second
// WeakMap and lose the candidate's session-owned capture identity).
async function producers(): Promise<{ send: typeof Send; rawUser: typeof RawUser; capture: typeof Capture; cause: typeof Cause; module: string }> {
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
      return { send: module.sendFabricMessage, rawUser: module.sendFabricUserMessage, capture: module.registerFabricWakeCapture,
        cause: module.fabricWakeCause, module: file };
    }
  }
  throw new Error("Built candidate lacks exported wake producers/capture. Build the parent implementation first.");
}

export default function (pi: ExtensionAPI): void {
  let producerPromise: ReturnType<typeof producers> | undefined;
  let bootstrapped = false;
  const rawProbe = process.env.WAKE_REPLAY_RAW_PROBE === "1";
  let handleNextRaw = false;
  if (rawProbe) {
    // In this separate native reproduction load ONLY the fixture extension. This
    // earlier real input handler can short-circuit before compiled capture sees it.
    pi.on("input", event => {
      if (handleNextRaw && event.source === "extension" && event.text === "RAW_EQ") {
        handleNextRaw = false;
        log(process.env.WAKE_REPLAY_HOST, { event: "raw_handled", text: event.text });
        return { action: "handled" };
      }
    });
  }
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
    const finish = (): void => {
      if (bootstrap) {
        events.push({ type: "toolcall_end", contentIndex: 0, toolCall: message.content[0] as Extract<AssistantMessage["content"][number], { type: "toolCall" }>, partial: message });
      } else {
        events.push({ type: "text_start", contentIndex: 0, partial: message });
        events.push({ type: "text_delta", contentIndex: 0, delta: "WAKE_REPLAY_ACK", partial: message });
        events.push({ type: "text_end", contentIndex: 0, content: "WAKE_REPLAY_ACK", partial: message });
      }
      events.push({ type: "done", reason: bootstrap ? "toolUse" : "stop", message });
      events.end();
    };
    if (rawProbe && JSON.stringify(lastUser).includes("RAW_BUSY")) setTimeout(finish, 1000);
    else finish();
    return events;
  };
  pi.registerProvider({ id: "wake-replay", name: "Keyless local replay",
    auth: { apiKey: { name: "No credentials", check: async () => ({ type: "api_key", source: "keyless replay" }), resolve: async () => ({ auth: {} }) } },
    getModels: () => [model], stream, streamSimple: stream,
  });
  pi.on("session_start", async (_event, ctx) => {
    log(process.env.WAKE_REPLAY_HOST, {
      event: "session_start", sessionId: ctx.sessionManager.getSessionId(), mode: ctx.mode,
      capabilities: (pi as unknown as { hostCapabilities?: unknown }).hostCapabilities,
      testCapabilityOverride: Boolean((globalThis as Record<symbol, unknown>)[Symbol.for("pi-fabric.test.hostCapabilities")]),
      isPromptPending: typeof (ctx as unknown as { isPromptPending?: unknown }).isPromptPending,
    });
    if (rawProbe) {
      // Real native session and ExtensionAPI, exact compiled capture. No index is
      // loaded in this fixture-only reproduction, so capture registers only once.
      const producer = await (producerPromise ??= producers());
      if (typeof producer.rawUser !== "function") throw new Error("Compiled raw-user helper is missing");
      producer.capture(pi);
    }
  });
  pi.on("agent_settled", event => log(process.env.WAKE_REPLAY_HOST, { event: "agent_settled", outcome: (event as unknown as { outcome?: string }).outcome }));
  pi.registerCommand("wake-replay", { description: "Replay compiled producer boundary (not an actor or database execution)", handler: async (args, ctx) => {
    const { send, rawUser, cause, module } = await (producerPromise ??= producers());
    const host = { id: `session:${ctx.sessionManager.getSessionId()}`, name: "main", kind: "main" as const };
    const actor = { id: "wake-replay:actor", name: "Replay actor", kind: "actor" as const };
    log(process.env.WAKE_REPLAY_HOST, { event: "injection", case: args, producerModule: module, idle: ctx.isIdle() });
    if (rawProbe && args === "raw-pair") {
      rawUser(pi, "RAW_A", actor, "followUp", { deliverAs: "followUp" }, "mesh");
      rawUser(pi, "RAW_B", { ...actor, id: "wake-replay:second", name: "Second raw sender" }, "followUp", { deliverAs: "followUp" }, "mesh");
      return;
    }
    if (rawProbe && args === "raw-handled") {
      handleNextRaw = true;
      rawUser(pi, "RAW_EQ", actor, "followUp", { deliverAs: "followUp" }, "mesh");
      return;
    }
    if (rawProbe && args === "raw-human-equal") {
      // Same native API/source as the unlabelled human dashboard composer.
      pi.sendUserMessage("RAW_EQ", { deliverAs: "followUp" });
      return;
    }
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
