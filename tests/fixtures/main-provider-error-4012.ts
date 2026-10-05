// Real Pi/Fabric loops and routing; only provider responses are synthetic.
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const root = process.env.FABRIC_4012_PROBE_DIR!;
  const faux = fauxProvider({ provider: "provider-error-4012", api: "provider-error-4012", tokensPerSecond: 1_000 });
  pi.registerProvider(faux.provider);
  let delayedCompaction = false;
  const recordCompaction = (record: object) => fs.appendFileSync(path.join(root, "compactions.jsonl"), JSON.stringify({ at: Date.now(), ...record }) + "\n");
  pi.on("session_before_compact", async (event, context) => {
    if (!delayedCompaction) return;
    recordCompaction({ phase: "start" });
    // Hold real Pi compaction beyond Fabric's recovery backoff, in its own
    // configured-threshold settlement hook (not an artificial agent_settled).
    await new Promise(resolve => setTimeout(resolve, 1_100));
    recordCompaction({ phase: "backoff", idle: context.isIdle() });
    await new Promise(resolve => setTimeout(resolve, 500));
    return { compaction: { summary: "Warmup complete; continue the interrupted task.",
      firstKeptEntryId: event.preparation.firstKeptEntryId, tokensBefore: event.preparation.tokensBefore } };
  });
  pi.on("session_compact", () => {
    if (delayedCompaction) recordCompaction({ phase: "complete" });
  });
  pi.on("session_start", (_event, context) => {
    fs.writeFileSync(path.join(root, "ready.json"), JSON.stringify({
      id: `session:${context.sessionManager.getSessionId()}`, sessionFile: context.sessionManager.getSessionFile(),
    }));
  });
  pi.on("input", event => {
    const scenario = event.text.split("\n", 1)[0]!.replace(/^COMPACT_/, "");
    delayedCompaction = event.text.startsWith("COMPACT_");
    const error = scenario === "OVERFLOW" ? "prompt is too long: 200000 tokens > 100000 maximum"
      : scenario === "NATIVE_RETRY" ? "stream error: socket hang up"
      : "stream error: stream disconnected before completion\nsecond diagnostic line";
    const response = (failure: boolean) => (context: { messages: unknown[] }) => {
      fs.appendFileSync(path.join(root, "attempts.jsonl"), JSON.stringify({ failure, at: Date.now(), context: context.messages }) + "\n");
      return fauxAssistantMessage(failure ? "partial before disconnect" : "recovered turn", failure
        ? { stopReason: "error", errorMessage: error } : {});
    };
    if (scenario === "ABORT") {
      faux.setResponses([() => {
        fs.appendFileSync(path.join(root, "attempts.jsonl"), JSON.stringify({ abort: true }) + "\n");
        return fauxAssistantMessage("slow response ".repeat(4_000));
      }, response(false)]);
    } else if (scenario === "FAIL_TWICE") {
      faux.setResponses([response(true), response(true), response(false)]);
    } else if (["FAIL_ONCE", "NATIVE_RETRY", "OVERFLOW"].includes(scenario)) {
      faux.setResponses([response(true), response(false), response(false)]);
    } else {
      faux.setResponses([fauxAssistantMessage("parent ready"), fauxAssistantMessage("parent received BLOCKED")]);
    }
    return { action: "continue" as const };
  });
}
