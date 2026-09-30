import type { FabricConfig } from "../config.js";
import type { FabricOwnedModelGuidance } from "./model-guidance.js";

// Built-in provider guidance must not wait for runtime activation. A guide
// that appears mid-session rewrites the system prompt suffix and drops the
// provider prefix cache for the whole conversation. These entries depend only
// on configuration, so they resolve identically on the first agent start.
const JEV_GUIDANCE_CONTENT =
  "Jev supplies typed Choice, Noul, and Score judgments, not generated text. Prefer shell-first orchestration: granted pi.bash runs existing CLIs; tasks.wait/watch await bounded receipts/monitor batches without polling or inference. Use UI-only monitors to avoid Main wakeups. Browser/macOS tools need no Fabric bridge. Code owns commands; never execute a model answer as shell source. Omit jev.evaluate and set maxEvaluations:0 for deterministic programs (host auto approvals remain independent). Use jev.evaluate only for explicit authorized batched questions; jev.run/spawn for isolated TypeScript programs that may loop using input, program.sleep, program.emit, and exact requires capabilities. run/wait return terminal envelopes (join aliases wait for both agents and Jev); inspect state and result/error. Programs and detached tasks are session-owned, not restart-durable. jev.status/stop control programs; tasks.stop separately stops their detached tasks. Observation timeout/cancellation never cancels the task; keep task IDs and finite process deadlines. For Main-turn advisors, spawn with observe, await program.nextEvent without polling, and opt into bounded context fields. program.advise requires jev.advise and explicit delivery; default is record-only. Return the observer ID without waiting in Main; Escape/Main abort cancels observers. Use /login jev, TYPESAFE_API_KEY, /login openrouter, OPENROUTER_API_KEY, /login vercel-ai-gateway, AI_GATEWAY_API_KEY, or a trusted credentialCommand. Credentials stay host-side; status never retrieves a key. See docs/jev.md for schemas, budgets, and shell/CLI composition.";

export const builtinModelGuidance = (
  config: Pick<FabricConfig, "jev" | "schema">,
): FabricOwnedModelGuidance[] =>
  config.jev.enabled && config.schema.mode !== "enforce"
    ? [{
        componentId: "fabric.builtin.jev",
        component: "fabric",
        revision: 1,
        label: "jev-programs",
        models: ["*/*"],
        targets: ["main", "participant"],
        placement: "append",
        content: JEV_GUIDANCE_CONTENT,
      }]
    : [];
