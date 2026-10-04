// Deterministic model/tool gate only; compiled Fabric and installed Pi own all routing,
// native session replacement, journalling, claim fencing and receipt delivery.
import fs from "node:fs";
import path from "node:path";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
export default function (pi: ExtensionAPI) {
  const dir = process.env.FABRIC_4313_GATE_DIR!;
  const mark = (name: string, data: unknown) => fs.writeFileSync(path.join(dir, name), JSON.stringify(data));
  const faux = fauxProvider({ provider: "stall-4313", api: "stall-4313" });
  pi.registerProvider(faux.provider);
  const responses = (messages: ReturnType<typeof fauxAssistantMessage>[]) => faux.setResponses(messages.map(message => context => {
    fs.appendFileSync(path.join(dir, "provider-context.jsonl"), `${JSON.stringify(context.messages)}\n`);
    return message;
  }));
  responses([fauxAssistantMessage("received inbox")]);
  pi.on("session_start", (event, ctx) => {
    mark("ready", { id: `session:${ctx.sessionManager.getSessionId()}`, sessionFile: ctx.sessionManager.getSessionFile(), reason: event.reason });
    fs.appendFileSync(path.join(dir, "sessions.jsonl"), JSON.stringify({ id: ctx.sessionManager.getSessionId(), file: ctx.sessionManager.getSessionFile(), reason: event.reason }) + "\n");
  });
  pi.on("input", event => {
    if (event.text === "HOLD") responses([
      fauxAssistantMessage(fauxToolCall("hold_4313", {}), { stopReason: "toolUse" }), fauxAssistantMessage("held finished"),
    ]);
    else if (event.text.startsWith("SEND ")) {
      const packet = JSON.parse(event.text.slice(5));
      responses([fauxAssistantMessage(fauxToolCall("fabric_exec", {
        code: "const packet = JSON.parse(π.packet); return await agents.followUp({ id: packet.id, message: packet.message });",
        payloads: { packet: JSON.stringify(packet) },
      }), { stopReason: "toolUse" }), fauxAssistantMessage("followUp accepted")]);
    }
    return { action: "continue" as const };
  });
  pi.registerTool({ name: "hold_4313", label: "hold", description: "Abortable receiving tool boundary", parameters: Type.Object({}),
    execute: async (_id, _args, signal) => {
      mark("holding", true);
      await new Promise<void>(resolve => { if (signal?.aborted) resolve(); else signal?.addEventListener("abort", () => resolve(), { once: true }); });
      return { content: [{ type: "text", text: "native rotation aborted the gate" }], details: {} };
    },
  });
}
