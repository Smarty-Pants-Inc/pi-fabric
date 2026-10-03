import type { AgentRunRequest } from "./types.js";
<<<<<<< HEAD
import { isFabricThinking } from "../thinking.js";
import { parseAgentNice } from "./priority.js";
=======
import { isFabricThinking, normalizeThinkingBounds } from "../thinking.js";
>>>>>>> upstream-v0.105.0
import { aliasThinking, type FabricModelAliases } from "../core/model-resolution.js";
import { isFabricRunnerId } from "./runner-registry.js";

const stringArray = (value: unknown): string[] | undefined => Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : undefined;
const checkedKernel = (value: unknown): AgentRunRequest["kernel"] => {
  if (value === undefined || value === "inherit" || value === "typescript" || value === "python") return value;
  throw new Error(`Invalid Fabric agent kernel: ${String(value)}`);
};

export const normalizeAgentRunRequest = (
  args: Record<string, unknown>,
  defaults: {runner: NonNullable<AgentRunRequest["runner"]>; model?: string; timeoutMs: number; inheritedModel?: {provider: string; id: string}; inheritedThinking?: string | undefined; models?: {aliases?: FabricModelAliases}},
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
<<<<<<< HEAD
  const runner =
    args.runner === "pi" || args.runner === "claude" || args.runner === "veda"
      ? args.runner
      : defaults.runner;
  const explicitModel = typeof args.model === "string" ? args.model.trim() || undefined : undefined;
  // The caller's admitted Pi binding wins over package/workspace defaults, also in
  // actor/task processes whose Main target is remote. Never infer from that target.
  const inheritedModel = runner === "pi" && !explicitModel && defaults.inheritedModel
    ? `${defaults.inheritedModel.provider}/${defaults.inheritedModel.id}` : undefined;
  const requestedModel = explicitModel ?? inheritedModel ?? (runner === "pi" ? defaults.model : undefined);
  const thinking = isFabricThinking(args.thinking) ? args.thinking
    : inheritedModel && isFabricThinking(defaults.inheritedThinking) ? defaults.inheritedThinking
    : aliasThinking(defaults.models?.aliases, requestedModel ?? "");
  const tools = stringArray(args.tools);
  const timeoutMs = typeof args.timeoutMs === "number" && Number.isFinite(args.timeoutMs) && args.timeoutMs > defaults.timeoutMs ? args.timeoutMs : undefined;
  const kernel = checkedKernel(args.kernel);
  const nice = parseAgentNice(args.nice);
=======
  // An explicit call or actor level always wins; otherwise an alias can carry
  // the intended effort for the model it selects (e.g. a "cheap" alias that is
  // both cheaper and shallower), and the global agents.thinking default applies
  // last, inside the manager.
  const requestedModel =
    typeof args.model === "string"
      ? args.model
      : typeof defaults.model === "string"
        ? defaults.model
        : undefined;
  const thinking = isFabricThinking(args.thinking)
    ? args.thinking
    : aliasThinking(defaults.models?.aliases, requestedModel ?? "");
  const tools = stringArray(args.tools);
  const timeoutMs = typeof args.timeoutMs === "number" && Number.isFinite(args.timeoutMs) && args.timeoutMs > defaults.timeoutMs ? args.timeoutMs : undefined;
  if (args.runner !== undefined && !isFabricRunnerId(args.runner)) {
    throw new Error(`Invalid Fabric agent runner: ${JSON.stringify(args.runner)}`);
  }
  const runner = args.runner ?? defaults.runner;
  const inheritedModel =
    runner === "pi" && !defaults.model && defaults.inheritedModel
      ? `${defaults.inheritedModel.provider}/${defaults.inheritedModel.id}`
      : undefined;
  const kernel = checkedKernel(args.kernel);
  const thinkingBounds = args.thinkingBounds === undefined
    ? undefined
    : normalizeThinkingBounds(args.thinkingBounds, "thinkingBounds");
>>>>>>> upstream-v0.105.0
  if (args.recursive === true && args.extensions === false) {
    throw new Error("Recursive Fabric requires extensions enabled; omit recursive or extensions: false");
  }
  return {
    task: String(args.task),
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
<<<<<<< HEAD
    ...(nice !== undefined ? { nice } : {}),
=======
    ...(thinkingBounds ? { thinkingBounds } : {}),
>>>>>>> upstream-v0.105.0
    ...(tools ? { tools } : {}),
    ...(timeoutMs !== undefined ? { timeoutMs } : {}),
    ...(typeof args.extensions === "boolean"
      ? { extensions: args.extensions }
      : args.recursive === true ? { extensions: true } : {}),
    ...(typeof args.recursive === "boolean" ? { recursive: args.recursive } : {}),
    ...(options.allowCwd !== false && typeof args.cwd === "string" ? { cwd: args.cwd } : {}),
    ...(typeof args.worktree === "boolean" ? { worktree: args.worktree } : {}),
<<<<<<< HEAD
    ...(typeof args.idempotencyKey === "string" ? { idempotencyKey: args.idempotencyKey } : {}),
=======
    ...(args.worktreeSetup !== undefined ? { worktreeSetup: args.worktreeSetup as string } : {}),
    // Shapes are checked fail-closed by the manager before launch.
    ...(args.readOnly !== undefined ? { readOnly: args.readOnly as boolean } : {}),
    ...(args.writableRoots !== undefined ? { writableRoots: args.writableRoots as string[] } : {}),
    ...(args.shell !== undefined ? { shell: args.shell as "deny" | "unconfined" } : {}),
    ...(args.scope !== undefined ? { scope: args.scope as NonNullable<AgentRunRequest["scope"]> } : {}),
>>>>>>> upstream-v0.105.0
    ...(args.residency === "session" || args.residency === "durable"
      ? { residency: args.residency }
      : {}),
    ...(typeof args.persistSession === "boolean"
      ? { persistSession: args.persistSession }
      : {}),
    ...(typeof args.schema === "object" && args.schema !== null && !Array.isArray(args.schema)
      ? { schema: args.schema as Record<string, unknown> }
      : {}),
    ...(typeof args.systemPrompt === "string" && args.systemPrompt.trim()
      ? { systemPrompt: args.systemPrompt }
      : {}),
  };
};
