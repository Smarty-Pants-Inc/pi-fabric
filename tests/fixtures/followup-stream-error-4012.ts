// Only the model is synthetic; Fabric delivery and Pi's RPC agent loop are native.
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const root = process.env.FABRIC_4012_PROBE_DIR!;
  const faux = fauxProvider({ provider: "stream-error-4012", api: "stream-error-4012" });
  pi.registerProvider(faux.provider);
  const record = (context: { messages: unknown[] }) => {
    fs.appendFileSync(path.join(root, "provider-context.jsonl"), JSON.stringify(context.messages) + "\n");
  };
  pi.on("session_start", (_event, ctx) => {
    fs.writeFileSync(path.join(root, "ready.json"), JSON.stringify({
      id: `session:${ctx.sessionManager.getSessionId()}`, sessionFile: ctx.sessionManager.getSessionFile(),
    }));
  });
  pi.on("input", (event) => {
    if (event.text === "FAIL_STREAM") {
      faux.setResponses([
        fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return await agents.self();" }), { stopReason: "toolUse" }),
        context => {
          record(context);
          return fauxAssistantMessage("partial response before disconnect", { stopReason: "error",
            errorMessage: "stream disconnected before completion: stream closed before response.completed" });
        },
        context => { record(context); return fauxAssistantMessage("followUp processed after stream error"); },
        context => { record(context); return fauxAssistantMessage("later followUp processed"); },
      ]);
    } else if (event.text.startsWith("SEND ") || event.text.startsWith("MAILBOX ")) {
      const mailbox = event.text.startsWith("MAILBOX ");
      const packet = event.text.slice(mailbox ? 8 : 5);
      faux.setResponses([
        fauxAssistantMessage(fauxToolCall("fabric_exec", {
          code: mailbox
            ? "const p = JSON.parse(π.packet); return await mesh.publish({ topic: 'fleet.work.pi-fabric.4012', kind: 'handoff', to: p.id, text: p.message });"
            : "const p = JSON.parse(π.packet); return await agents.followUp({ id: p.id, message: p.message });",
          payloads: { packet },
        }), { stopReason: "toolUse" }),
        fauxAssistantMessage("followUp acknowledged"),
      ]);
    }
    return { action: "continue" as const };
  });
}
