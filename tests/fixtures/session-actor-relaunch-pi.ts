// smarty-dev#9241: deterministic model gate only. Compiled Fabric and installed Pi own the
// session lifecycle, the actor registry, its restart and every tell.
import fs from "node:fs";
import path from "node:path";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const dir = process.env.FABRIC_9241_GATE_DIR!;
  const faux = fauxProvider({ provider: "relaunch-9241", api: "relaunch-9241" });
  pi.registerProvider(faux.provider);
  faux.setResponses([fauxAssistantMessage("idle")]);
  pi.on("session_start", (event, ctx) => {
    const ready = { id: `session:${ctx.sessionManager.getSessionId()}`, sessionFile: ctx.sessionManager.getSessionFile(), reason: event.reason, pid: process.pid };
    fs.writeFileSync(path.join(dir, "ready"), JSON.stringify(ready));
    fs.appendFileSync(path.join(dir, "sessions.jsonl"), `${JSON.stringify(ready)}\n`);
  });
  pi.on("input", event => {
    // EXEC <code>: one fabric_exec call through the real tool path, then a final reply.
    if (event.text.startsWith("EXEC ")) faux.setResponses([
      fauxAssistantMessage(fauxToolCall("fabric_exec", { code: event.text.slice(5), resultFormat: "json" }), { stopReason: "toolUse" }),
      fauxAssistantMessage("exec done"),
    ]);
    return { action: "continue" as const };
  });
}
