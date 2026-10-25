// Test-only observation and deny-input guard. No provider/model/session/runtime substitutes.
import fs from "node:fs";
import { isNativeSdkUnconfigured } from "../helpers/native-sdk-no-model.ts";
const pinsFile = process.env.FABRIC_NATIVE_OWNER_PINS;
if (!pinsFile) throw new Error("Native owner observer pins required");
const pins = JSON.parse(fs.readFileSync(pinsFile, "utf8"));
const role = process.env.PI_FABRIC_RESIDENT_CONFIG ? "resident" : "main";
const record = value => fs.appendFileSync(pins.observations, JSON.stringify({ role, pid: process.pid, ...value }) + "\n", { mode: 0o600 });
export default function(pi) {
  pi.on("input", () => { record({ event: "input-handled" }); return { action: "handled" }; });
  pi.on("before_agent_start", () => { record({ event: "FORBIDDEN-agent-preflight" }); throw new Error("Native fixture may not start a model turn"); });
  pi.on("agent_start", () => { record({ event: "FORBIDDEN-agent-start" }); });
  pi.on("before_provider_request", () => { record({ event: "FORBIDDEN-provider-request" }); throw new Error("Native fixture has no model authority"); });
  pi.on("provider_stream_event", () => { record({ event: "FORBIDDEN-provider-stream" }); });
  pi.on("after_provider_response", () => { record({ event: "FORBIDDEN-provider-response" }); });
  pi.on("tool_call", () => { record({ event: "FORBIDDEN-tool-call" }); return { block: true, reason: "Native owner fixture has no tool turns" }; });
  pi.on("session_start", (_event, ctx) => {
    const unconfigured = isNativeSdkUnconfigured(ctx.model, pins.sdkDirectory);
    const selectedModel = !unconfigured;
    record({ event: "session-start", selectedModel, unconfigured, projectTrusted: ctx.isProjectTrusted(),
      rootId: `session:${ctx.sessionManager.getSessionId()}`,
      sessionFile: ctx.sessionManager.getSessionFile(), inputAdmission: pi.hostCapabilities.inputAdmission,
      model: ctx.model ? { id: ctx.model.id, provider: ctx.model.provider, api: ctx.model.api,
        hasEndpoint: !!ctx.model.baseUrl, contextWindow: ctx.model.contextWindow, maxTokens: ctx.model.maxTokens } : null });
    if (selectedModel || pi.hostCapabilities.inputAdmission !== 1) ctx.shutdown();
  });
}
