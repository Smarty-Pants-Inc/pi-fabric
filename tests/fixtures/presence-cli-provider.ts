// Substitute inference only; the CLI, Fabric public API, launcher, host and mesh remain real.
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const faux = fauxProvider({ provider: "presence-proof", api: "presence-proof" });
  pi.registerProvider(faux.provider);
  pi.on("session_start", (_event, ctx) => {
    fs.writeFileSync(path.join(process.env.PRESENCE_PROOF_ROOT!, "main-ready.json"), JSON.stringify({
      rootId: `session:${ctx.sessionManager.getSessionId()}`, sessionFile: ctx.sessionManager.getSessionFile(), pid: process.pid,
    }));
  });
  pi.on("input", event => {
    const code = event.text === "CREATE_PRESENCE" ? `
      const created = [];
      for (let i = 0; i < 12; i++) created.push(await agents.create({
        name: "presence-proof-" + i, instructions: "Remain idle until explicitly addressed.",
        scope: i % 2 ? "session" : "project", residency: "durable", events: [], topics: [],
        tools: [], delivery: "mailbox", triggerTurn: false
      }));
      return { created };
    ` : `return { restored: (await agents.actors()).filter(actor => actor.name.startsWith("presence-proof-")) };`;
    faux.setResponses([
      fauxAssistantMessage(fauxToolCall("fabric_exec", { code, resultFormat: "json" }), { stopReason: "toolUse" }),
      fauxAssistantMessage("Presence proof public operation complete"),
    ]);
    return { action: "continue" as const };
  });
}
