import fs from "node:fs";
import { estimateTokens, type ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type AssistantMessage, type SimpleStreamOptions, type Context } from "@earendil-works/pi-ai";

// Offline scripted provider: plan + status, a complete two-write boundary batch,
// then three competing steers (last one read-only). Synthetic control-flow
// evidence only: usage is estimated, not measured provider cost or performance.
export default function (pi: ExtensionAPI) {
  const log = process.env.PREWALK_PROBE_REQUESTS;
  const delayMs = Number(process.env.PREWALK_PROBE_EXECUTOR_DELAY_MS ?? "0");
  const fillerKb = Number(process.env.PREWALK_PROBE_EXECUTOR_FILLER_KB ?? "0");
  if (![delayMs, fillerKb].every(value => Number.isSafeInteger(value) && value >= 0)) {
    throw new Error("Probe delay and filler must be nonnegative integers");
  }
  let requestIndex = 0;
  let steersSent = false;
  const planArgs = {
    outcome: "Probe the request contract for a completed batch",
    steps: ["PROBE-PLAN-STEP write both batch files, then verify"],
    verification: ["probe request contract evidence"],
    risks: "none recorded",
  };
  pi.registerProvider("prewalk-probe", {
    baseUrl: "http://127.0.0.1:1",
    apiKey: "offline-probe",
    api: "prewalk-probe-api",
    models: ["main", "executor"].map((id) => ({
      id, name: id, reasoning: true, input: ["text"], contextWindow: 200000, maxTokens: 65536,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    })),
    streamSimple(model: { api: string; provider: string; id: string }, context: Context, options?: SimpleStreamOptions) {
      requestIndex += 1;
      if (log) fs.appendFileSync(log, JSON.stringify({ record: "request", n: requestIndex, model: model.provider + "/" + model.id, messages: context.messages.length }) + "\n");
      const stream = createAssistantMessageEventStream();
      const input = context.messages.reduce((sum, message) => sum + estimateTokens(message), 0);
      const build = (text: string, stopReason: "stop" | "toolUse"): AssistantMessage => {
        const message: AssistantMessage = {
          role: "assistant",
          content: stopReason === "toolUse" ? [{ type: "toolCall", id: "probe-call-" + requestIndex, name: "fabric_exec", arguments: { code: text } }] : [{ type: "text", text }],
          api: model.api, provider: model.provider, model: model.id,
          usage: { input, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: input,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
          stopReason, timestamp: Date.now(),
        };
        message.usage.output = estimateTokens(message);
        message.usage.totalTokens += message.usage.output;
        return message;
      };
      const finish = (message: AssistantMessage, delay = 0): void => {
        let terminal = false;
        let timer: ReturnType<typeof setTimeout> | undefined;
        const settle = (aborted: boolean): void => {
          if (terminal) return;
          terminal = true;
          if (timer !== undefined) clearTimeout(timer);
          options?.signal?.removeEventListener("abort", abort);
          if (aborted) {
            message.stopReason = "aborted";
            message.content = [];
            message.errorMessage = "Offline probe cancelled";
            message.usage.output = 0;
            message.usage.totalTokens = input;
            stream.push({ type: "error", reason: "aborted", error: message });
          } else {
            stream.push({ type: "done", reason: message.stopReason === "toolUse" ? "toolUse" : "stop", message });
          }
          stream.end();
        };
        const abort = (): void => settle(true);
        stream.push({ type: "start", partial: message });
        options?.signal?.addEventListener("abort", abort, { once: true });
        if (options?.signal?.aborted) abort();
        else if (delay > 0) timer = setTimeout(() => settle(false), delay);
        else settle(false);
      };
      if (model.id === "executor") {
        const filler = fillerKb > 0 ? "\n" + "x".repeat(fillerKb * 1024) : "";
        finish(build("PROBE-EXECUTOR-TURN" + filler, "stop"), delayMs);
      } else if (requestIndex === 1) {
        const code = "const status = await tools.call({ ref: \"prewalk.status\", args: {} });\n"
          + "await tools.call({ ref: \"prewalk.plan\", args: " + JSON.stringify(planArgs) + " });\n"
          + "return status;";
        finish(build(code, "toolUse"));
      } else if (requestIndex === 2) {
        const code = "await pi.write({ path: 'probe-batch-a.txt', text: 'PROBE-BATCH-A' });\n"
          + "await pi.write({ path: 'probe-batch-b.txt', text: 'PROBE-BATCH-B' });\n"
          + "return 'PROBE-BATCH-DONE';";
        finish(build(code, "toolUse"));
      } else {
        finish(build("PROBE-UNEXPECTED-EXTRA-REQUEST", "stop"));
      }
      return stream;
    },
  });
  // The CLI selects Main. Never reset it in session_start: that would mask
  // whether Fabric actually restores Main through Escape or either reload.
  pi.on("tool_execution_start", (event) => {
    if (steersSent || event.toolName !== "fabric_exec") return;
    const args = event.args as { code?: unknown } | undefined;
    if (typeof args?.code !== "string" || !args.code.includes("PROBE-BATCH-A")) return;
    steersSent = true;
    pi.sendUserMessage?.("PROBE-STEER-1: keep both batch files intact", { deliverAs: "steer" });
    pi.sendUserMessage?.("PROBE-STEER-2: also verify probe-batch-b.txt", { deliverAs: "steer" });
    pi.sendUserMessage?.("PROBE-SCOPE: inspection only — make no further changes to any file", { deliverAs: "steer" });
  });
}
