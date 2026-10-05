import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";

/** Deterministic native provider: actual Pi/worker/tool/child execution, never network. */
export default function nativeRoleProbe(pi: ExtensionAPI) {
  const actor = Boolean(process.env.PI_FABRIC_ACTOR_ID);
  const scenario = process.env.NATIVE_ROLE_PROBE_SCENARIO ?? "inherit";
  if (actor && scenario === "restrictive-ceiling") process.env.PI_FABRIC_TOOL_ALLOWLIST = '["read","grep","find","ls","write"]';
  let turn = 0;
  pi.registerProvider("role-probe", {
    baseUrl: "http://127.0.0.1:1", apiKey: "offline-probe", api: "role-probe-api",
    models: ["requested", "global", "requested-prepared"].map(id => ({ id, name: id, reasoning: true, input: ["text"],
      thinkingLevelMap: { max: !actor && scenario === "effort-clamp" ? null : "max" },
      contextWindow: 200000, maxTokens: 4096, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } })),
    streamSimple(model, context) {
      const stream = createAssistantMessageEventStream();
      let content: AssistantMessage["content"];
      let stopReason: AssistantMessage["stopReason"] = "stop";
      if (actor && turn++ === 0) {
        const nativeTools = ["read", "grep", "find", "ls", "bash", "write"];
        const selections: Record<string, unknown> = scenario === "explicit"
          ? { model: "role-probe/requested", thinking: "max", tools: [...nativeTools].reverse() }
          : scenario === "wrong-model" ? { model: "role-probe/global" }
          : scenario === "wrong-effort" ? { thinking: "medium" }
          : scenario === "wrong-tools" ? { tools: ["read"] } : {};
        const request = { task: "role native child probe", ...selections };
        const handoff = scenario.startsWith("handoff-") ? { model: scenario === "handoff-model" ? "role-probe/global" : "role-probe/requested",
          ...(scenario === "handoff-effort" ? { thinking: "medium" } : {}),
          ...(scenario === "handoff-tools" ? { tools: ["read"] } : {}),
          ...(scenario === "handoff-extensions" ? { extensions: false } : {}) } : undefined;
        content = [{ type: "toolCall", id: "spawn-role-child", name: "fabric_exec", arguments: { code:
          `try { ${handoff ? `return await agents.handoff(${JSON.stringify(handoff)});` : ""} const child = await agents.spawn(${JSON.stringify(request)}); const result = await agents.wait({id: child.id}); return JSON.stringify({child: result}); } catch (error) { return JSON.stringify({refusal: String(error)}); }` } }];
        stopReason = "toolUse";
      } else if (!actor && turn++ === 0) {
        content = [{ type: "toolCall", id: "native-bash-proof", name: "bash", arguments: { command: "printf 'native-role-bash-delivered'" } }];
        stopReason = "toolUse";
      } else if (!actor && turn === 2) {
        content = [{ type: "toolCall", id: "child-self-proof", name: "fabric_exec", arguments: { code: "return JSON.stringify({self: await agents.self(), spawner: await agents.spawner()});" } }];
        stopReason = "toolUse";
      } else {
        const results = context.messages.filter(message => message.role === "toolResult").map(message => {
          const text = message.content.filter(part => part.type === "text").map(part => part.text).join("\n");
          try { return JSON.parse(text); } catch { return text; }
        });
        const events = !actor ? fs.readFileSync(path.join(process.env.PI_FABRIC_AGENT_RUN_DIR!, "events.jsonl"), "utf8")
          .trim().split("\n").map(line => JSON.parse(line) as Record<string, unknown>) : [];
        const attestation = events.find(event => event.type === "fabric_native_role_admitted");
        const beforeFirstInference = !actor && events.findIndex(event => event.type === "fabric_native_role_admitted") < events.findIndex(event => event.type === "agent_start");
        content = [{ type: "text", text: JSON.stringify({ attestation, beforeFirstInference, model: `${model.provider}/${model.id}`, thinking: pi.getThinkingLevel(),
          nativeTools: pi.getActiveTools().filter(tool => ["read", "grep", "find", "ls", "bash", "write", "edit", "powershell"].includes(tool)),
          actorId: process.env.PI_FABRIC_ACTOR_ID ?? null, results }) }];
      }
      const message: AssistantMessage = { role: "assistant", api: model.api, provider: model.provider, model: model.id,
        content, stopReason, timestamp: Date.now(), usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } } };
      stream.push({ type: "start", partial: message }); stream.push({ type: "done", reason: stopReason, message }); stream.end(); return stream;
    },
  });
  pi.on("tool_call", (event, ctx) => {
    if (actor && scenario === "prepared-model" && event.toolName === "fabric_exec") {
      // Exact provider admission sees the requested model; preparation sees a changed catalogue.
      // Its ordinary closest fallback must not rebind this role child.
      const available = ctx.modelRegistry.getAvailable.bind(ctx.modelRegistry);
      let calls = 0;
      ctx.modelRegistry.getAvailable = () => ++calls === 1 ? available() : available().filter(model => model.id !== "requested");
    }
  });
  pi.on("session_start", () => {
    if (!actor && scenario === "missing-bash") pi.setActiveTools(pi.getActiveTools().filter(tool => tool !== "bash"));
    if (!actor && scenario === "wrong-metadata") {
      // Fault injection at the native participant attestation adapter, not public handle labels.
      pi.events.emit("fabric.native-role.activation-query", { accept(authority: { setAttester(attest: () => Promise<unknown>): void }) {
        authority.setAttester(async () => ({ id: process.env.PI_FABRIC_PARENT_RUN, kind: "agent",
          model: "role-probe/requested", thinking: "medium", stale: false }));
      } });
    }
  });
}
