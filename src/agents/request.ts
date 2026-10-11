import type { AgentRunRequest } from "./types.js";
import { normalizeAgentRequires } from "./input-validation.js";
import { normalizeAgentCapabilityTokens } from "../host-compatibility.js";
import { isFabricThinking } from "../thinking.js";
import { parseAgentNice } from "./priority.js";
import { MAX_ACTOR_BASH_TIMEOUT_S } from "../guards/actor-bash-timeout.js";
import { aliasThinking, type FabricModelAliases } from "../core/model-resolution.js";

const stringArray = (value: unknown): string[] | undefined => Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : undefined;
const checkedKernel = (value: unknown): AgentRunRequest["kernel"] => {
  if (value === undefined || value === "inherit" || value === "typescript" || value === "python") return value;
  throw new Error(`Invalid Fabric agent kernel: ${String(value)}`);
};

/**
 * True when fleet policy would deny the configured selector: the selector itself or any target of the alias it
 * names, compared as assertFabricModelAllowed compares (trimmed, case-insensitive). ponytail: any denied alias
 * target counts, because alias resolution may pick it; a denied configured default then falls back to the
 * caller's admitted binding instead of failing later in the policy check.
 */
const configuredModelDenied = (
  model: string,
  denied: readonly string[] | undefined,
  aliases: FabricModelAliases | undefined,
): boolean => {
  if (!denied?.length) return false;
  const blocked = new Set(denied.map((entry) => entry.trim().toLowerCase()));
  const key = model.trim().toLowerCase();
  if (blocked.has(key)) return true;
  const name = aliases ? Object.keys(aliases).find((alias) => alias.toLowerCase() === key) : undefined;
  const targets = name !== undefined && aliases ? aliases[name]?.targets ?? [] : [];
  return targets.some((target) => blocked.has(target.trim().toLowerCase()));
};

export const normalizeAgentRunRequest = (
  args: Record<string, unknown>,
  defaults: {runner: NonNullable<AgentRunRequest["runner"]>; model?: string; configuredThinking?: AgentRunRequest["thinking"]; deniedModels?: readonly string[]; timeoutMs: number; inheritedModel?: {provider: string; id: string}; inheritedThinking?: string | undefined; models?: {aliases?: FabricModelAliases}},
  options: {allowCwd?: boolean} = {},
): AgentRunRequest => {
  if (args.model === "auto") throw new Error('model: "auto" is supported only by agents.spawn with required routing pins');
  const transport =
    args.transport === "auto" ||
    args.transport === "process" ||
    args.transport === "tmux" ||
    args.transport === "screen" ||
    args.transport === "localterm" ||
    args.transport === "herdr"
      ? args.transport
      : undefined;
  const runner =
    args.runner === "pi" || args.runner === "claude" || args.runner === "veda"
      ? args.runner
      : defaults.runner;
  const explicitModel = typeof args.model === "string" ? args.model.trim() || undefined : undefined;
  // A configured default that fleet policy denies does not win: fall back to the caller's admitted
  // binding (#2490), so the deny-replacement path stays where it was before #6062.
  const configuredModel = runner === "pi" && defaults.model
    && !configuredModelDenied(defaults.model, defaults.deniedModels, defaults.models?.aliases)
    ? defaults.model : undefined;
  // Configured defaults win. Otherwise inherit the caller's admitted Pi binding,
  // also in actor/task processes whose Main target is remote. Never infer from that target.
  const inheritedModel = runner === "pi" && !explicitModel && !configuredModel && defaults.inheritedModel
    ? `${defaults.inheritedModel.provider}/${defaults.inheritedModel.id}` : undefined;
  const requestedModel = explicitModel ?? configuredModel ?? inheritedModel;
  const thinking = isFabricThinking(args.thinking) ? args.thinking
    : inheritedModel && isFabricThinking(defaults.inheritedThinking) ? defaults.inheritedThinking
    : isFabricThinking(defaults.configuredThinking) ? defaults.configuredThinking
    : aliasThinking(defaults.models?.aliases, requestedModel ?? "");
  const tools = stringArray(args.tools);
  if (args.complexity !== undefined && args.complexity !== "simple" && args.complexity !== "normal" && args.complexity !== "complex" && args.complexity !== "delicate") {
    throw new Error("Invalid agent complexity: expected simple, normal, complex or delicate");
  }
  const requires = normalizeAgentRequires(args.requires);
  const needs = normalizeAgentCapabilityTokens(args.needs);
  const timeoutMs = typeof args.timeoutMs === "number" && Number.isFinite(args.timeoutMs) && args.timeoutMs > defaults.timeoutMs ? args.timeoutMs : undefined;
  const kernel = checkedKernel(args.kernel);
  const nice = parseAgentNice(args.nice);
  const bashIdleSeconds = args.bashIdleSeconds;
  if (bashIdleSeconds !== undefined && (typeof bashIdleSeconds !== "number" || !Number.isInteger(bashIdleSeconds) ||
    bashIdleSeconds < 0 || bashIdleSeconds > MAX_ACTOR_BASH_TIMEOUT_S)) {
    throw new Error(`bashIdleSeconds must be a non-negative integer at most ${MAX_ACTOR_BASH_TIMEOUT_S} (0 = no idle limit)`);
  }
  if (args.recursive === true && args.extensions === false) {
    throw new Error("Recursive Fabric requires extensions enabled; omit recursive or extensions: false");
  }
  return {
    task: String(args.task),
    ...(args.complexity !== undefined ? { complexity: args.complexity as NonNullable<AgentRunRequest["complexity"]> } : {}),
    runner,
    ...(typeof args.routeClass === "string" ? { routeClass: args.routeClass } : {}),
    ...(typeof args.protected === "boolean" ? { protected: args.protected } : {}),
    ...(kernel !== undefined ? { kernel } : {}),
    ...(typeof args.name === "string" ? { name: args.name } : {}),
    ...(transport ? { transport } : {}),
    ...(explicitModel
      ? { model: explicitModel }
      : inheritedModel
        ? { model: inheritedModel }
        : {}),
    ...(typeof args.modelReason === "string" ? { modelReason: args.modelReason } : {}),
    ...(typeof args.persona === "string" && args.persona.trim()
      ? { persona: args.persona.trim() }
      : {}),
    ...(thinking ? { thinking } : {}),
    ...(nice !== undefined ? { nice } : {}),
    ...(bashIdleSeconds !== undefined ? { bashIdleSeconds } : {}),
    ...(tools ? { tools } : {}),
    ...(needs !== undefined ? { needs } : {}),
    ...(requires !== undefined ? { requires } : {}),
    ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    ...(typeof args.extensions === "boolean"
      ? { extensions: args.extensions }
      : args.recursive === true ? { extensions: true } : {}),
    ...(typeof args.recursive === "boolean" ? { recursive: args.recursive } : {}),
    ...(options.allowCwd !== false && typeof args.cwd === "string" ? { cwd: args.cwd } : {}),
    ...(typeof args.worktree === "boolean" ? { worktree: args.worktree } : {}),
    ...(typeof args.idempotencyKey === "string" ? { idempotencyKey: args.idempotencyKey } : {}),
    ...(args.residency === "session" || args.residency === "durable"
      ? { residency: args.residency }
      : {}),
    ...(typeof args.schema === "object" && args.schema !== null && !Array.isArray(args.schema)
      ? { schema: args.schema as Record<string, unknown> }
      : {}),
    ...(typeof args.systemPrompt === "string" && args.systemPrompt.trim()
      ? { systemPrompt: args.systemPrompt }
      : {}),
  };
};
