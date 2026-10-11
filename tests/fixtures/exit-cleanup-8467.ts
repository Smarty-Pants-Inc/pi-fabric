// TEST-ONLY Pi extension for tests/mesh-exit-cleanup-real-pi.e2e.test.ts (smarty-dev#8467). It is
// never bundled or shipped; the test loads it next to the compiled Fabric with `-e`, and it refuses
// to load without the test's report directory.
//
// It is the deterministic model only: an offline faux provider whose next turn is one fabric_exec
// call carrying the code of an `EXEC <code>` prompt, then a final text. The compiled Fabric runs
// that code, so every mesh write goes through the shipped mesh store. It also records the pid
// and session of each session_start, which the test uses to find this process's temp files.
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function exitCleanup8467(pi: ExtensionAPI): void {
  const dir = process.env.FABRIC_8467_REPORT_DIR;
  if (!dir) throw new Error("exit-cleanup-8467 is test-only: FABRIC_8467_REPORT_DIR is not set");
  const faux = fauxProvider({ provider: "exit-8467", api: "exit-8467" });
  pi.registerProvider(faux.provider);
  faux.setResponses([fauxAssistantMessage("idle")]);
  pi.on("session_start", (event, ctx) => {
    fs.writeFileSync(path.join(dir, "ready.json"), JSON.stringify({
      pid: process.pid, sessionId: ctx.sessionManager.getSessionId(), reason: event.reason,
    }));
  });
  pi.on("input", (event) => {
    if (event.text.startsWith("EXEC ")) {
      faux.setResponses([
        fauxAssistantMessage(fauxToolCall("fabric_exec", { code: event.text.slice("EXEC ".length) }), { stopReason: "toolUse" }),
        fauxAssistantMessage("done"),
      ]);
    }
    return { action: "continue" as const };
  });
}
