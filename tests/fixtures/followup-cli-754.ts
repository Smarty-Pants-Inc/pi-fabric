// Deterministic model and input/tool gates only. Fabric itself is loaded from dist/index.js;
// no routing, controller, receipt, or session implementation is substituted here.
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall, type AssistantMessage } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

export default function (pi: ExtensionAPI) {
  const root = process.env.FABRIC_754_GATE_DIR!;
  const mark = (name: string, value: unknown = true) => fs.writeFileSync(path.join(root, name), JSON.stringify(value));
  const gate = async (name: string) => {
    mark(`${name}-entered`);
    const deadline = Date.now() + 30_000;
    while (!fs.existsSync(path.join(root, `${name}-release`))) {
      if (Date.now() > deadline) throw new Error(`754 fixture gate expired: ${name}`);
      await new Promise(resolve => setTimeout(resolve, 10));
    }
  };
  const faux = fauxProvider({ provider: "followup-754", api: "followup-754" });
  pi.registerProvider(faux.provider);
  const responses = (messages: AssistantMessage[]) => faux.setResponses(messages.map(message => context => {
    // Exact provider-bound conversation, after Pi's custom-message conversion.
    const received = context.messages.filter(item => JSON.stringify(item).includes("fabric-agent-message"));
    fs.appendFileSync(path.join(root, "provider-context.jsonl"), `${JSON.stringify(received)}\n`);
    return message;
  }));
  pi.on("session_start", (_event, ctx) => {
    mark("ready", {
      id: `session:${ctx.sessionManager.getSessionId()}`,
      preflight: "isPromptPending" in ctx && typeof ctx.isPromptPending === "function",
      sessionFile: ctx.sessionManager.getSessionFile(),
    });
  });
  pi.on("input", async (event, ctx) => {
    if (event.text.startsWith("SEND ")) {
      const packet = JSON.parse(event.text.slice(5)) as { id: string; message: string };
      responses([
        fauxAssistantMessage(fauxToolCall("fabric_exec", {
          code: "const packet = JSON.parse(π.packet); return await agents.followUp({ id: packet.id, message: packet.message });",
          payloads: { packet: JSON.stringify(packet) },
        }), { stopReason: "toolUse" }),
        fauxAssistantMessage("followUp acknowledged"),
      ]);
    }
    if (event.text.startsWith("preflight-")) {
      const mode = event.text.slice("preflight-".length);
      responses([
        ...(mode === "success" ? [fauxAssistantMessage(fauxToolCall("work_754", {}), { stopReason: "toolUse" })] : []),
        fauxAssistantMessage("received"), fauxAssistantMessage("finished"),
      ]);
      // Optional host capability; Pi 0.87.0 does not declare it.
      const host = ctx as ExtensionContext & { isPromptPending?: () => boolean };
      mark("preflight-state", { idle: ctx.isIdle(), pending: host.isPromptPending?.() });
      await gate("preflight");
      if (mode === "handled") return { action: "handled" as const };
    }
    return { action: "continue" as const };
  });
  pi.on("before_agent_start", () => { mark("before-start"); });
  pi.registerTool({
    name: "work_754", label: "work", description: "held tool boundary", parameters: Type.Object({}),
    execute: async () => {
      await gate("tool");
      return { content: [{ type: "text", text: "done" }], details: {} };
    },
  });
  pi.on("agent_start", () => {
    const file = path.join(root, "starts");
    const starts = fs.existsSync(file) ? Number(fs.readFileSync(file, "utf8")) : 0;
    fs.writeFileSync(file, String(starts + 1));
  });
}
