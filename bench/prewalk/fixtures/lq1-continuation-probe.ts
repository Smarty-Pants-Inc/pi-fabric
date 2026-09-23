import fs from "node:fs";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";

// Offline scripted provider for the LQ1 in-place continuation probe.
// Request 1 records the plan, request 2 mutates a file (the boundary trigger),
// request 3 is the executor turn. A competing user steer is injected while the
// boundary turn runs. Every provider request is appended to
// $LQ1_PROBE_REQUESTS so the probe can assert first-request payloads, steer
// order, and the absence of a late completion-only request.
const LOG = process.env.LQ1_PROBE_REQUESTS;
const append = (value: unknown): void => {
  if (LOG) fs.appendFileSync(LOG, JSON.stringify(value) + "\n");
};

const usage = () => ({
  input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
});
const textOf = (content: unknown): string => {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((part) => (typeof part === "object" && part !== null && "text" in part
      ? String((part as { text?: unknown }).text ?? "")
      : ""))
    .join("\n");
};
const summarize = (value: unknown) => {
  const message = (value ?? {}) as Record<string, unknown>;
  return {
    role: message.role ?? null,
    customType: message.customType ?? null,
    stopReason: message.stopReason ?? null,
    text: textOf(message.content).slice(0, 8000),
  };
};

export default function (pi: ExtensionAPI) {
  let requestIndex = 0;
  let steerSent = false;
  let planTool: string | undefined;
  const planArgs = {
    outcome: "Probe the in-place continuation ordering",
    steps: ["LQ1-PROBE-PLAN"],
    verification: ["host probe request order"],
    risks: "none recorded",
  };

  pi.registerProvider("lq1-probe", {
    baseUrl: "http://127.0.0.1:1",
    apiKey: "offline-probe",
    api: "lq1-probe-api",
    models: ["main", "executor"].map((id) => ({
      id, name: id, reasoning: true, input: ["text"],
      contextWindow: 200000, maxTokens: 4096,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
    })),
    streamSimple(model, context) {
      requestIndex += 1;
      append({
        record: "request", n: requestIndex, model: `${model.provider}/${model.id}`,
        messages: (context.messages ?? []).map(summarize),
      });
      const stream = createAssistantMessageEventStream();
      const respond = (content: AssistantMessage["content"], stopReason: AssistantMessage["stopReason"]) => {
        const message: AssistantMessage = {
          role: "assistant", content, api: model.api, provider: model.provider,
          model: model.id, usage: usage(), stopReason, timestamp: Date.now(),
        };
        stream.push({ type: "start", partial: message });
        stream.push({ type: "done", reason: stopReason === "toolUse" ? "toolUse" : "stop", message });
        stream.end();
        return stream;
      };
      if (model.id === "executor") {
        return respond([{ type: "text", text: "LQ1-EXECUTOR-TURN" }], "stop");
      }
      if (requestIndex === 1) {
        // Provider actions are reachable from guest code through the tools
        // surface; the model-visible plan tool only registers after Fabric
        // initializes, so the scripted program records the plan directly.
        return respond([{
          type: "toolCall", id: "lq1-plan", name: "fabric_exec",
          arguments: {
            code: `await tools.call({ ref: "prewalk.plan", args: ${JSON.stringify(planArgs)} });\nreturn 'planned';`,
          },
        }], "toolUse");
      }
      if (requestIndex === 2) {
        return respond([{
          type: "toolCall", id: "lq1-mutate", name: "fabric_exec",
          arguments: { code: "await pi.write({ path: 'probe-output.txt', text: " + JSON.stringify("lq1 mutation") + " });\nreturn 'mutated';" },
        }], "toolUse");
      }
      return respond([{ type: "text", text: "LQ1-UNEXPECTED-EXTRA-REQUEST" }], "stop");
    },
  });

  pi.on("session_start", async (_event, ctx) => {
    const main = ctx.modelRegistry.find("lq1-probe", "main");
    if (main) await pi.setModel(main);
    const names = pi.getAllTools().map((tool) => tool.name);
    planTool = names.find((name) => /prewalk/i.test(name) && /plan/i.test(name));
    append({ record: "tools", planTool: planTool ?? null, names });
  });

  pi.on("tool_execution_start", (event) => {
    if (event.toolName !== "fabric_exec" || steerSent) return;
    const args = event.args as { code?: unknown } | undefined;
    if (typeof args?.code === "string" && args.code.includes("lq1 mutation")) {
      steerSent = true;
      pi.sendUserMessage?.(
        "USER-STEER: keep the probe artifact intact",
        { deliverAs: "steer" },
      );
    }
  });
}
