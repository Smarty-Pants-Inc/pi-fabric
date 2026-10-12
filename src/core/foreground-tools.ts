import { PI_CORE_TOOL_NAME_SET } from "./pi-tools.js";

/**
 * Foreground policy: registered tools full code mode keeps declared to the
 * model beside fabric_exec. It changes visibility only; a direct call takes
 * the same tool_call hooks and approval risk as `extensions.<name>`.
 */
export type FabricForegroundReason = "turn-steering" | "human-input" | "context-control" | "skill-loading";
export interface FabricForegroundTool { name: string; owner: string; reason: FabricForegroundReason }
export interface FabricForegroundConfig { tools: FabricForegroundTool[]; maxTools: number }
export type FabricForegroundRefusalReason =
  | "enforce" | "managed-host" | "core" | "builtin" | "unknown" | "inactive" | "not-allowed" | "duplicate" | "cap";
export interface FabricForegroundResolution {
  tools: readonly string[];
  refused: readonly { name: string; reason: FabricForegroundRefusalReason }[];
}

export const MAX_FOREGROUND_TOOLS = 8;
const MAX_FOREGROUND_ENTRIES = 64;
const REASONS = new Set(["turn-steering", "human-input", "context-control", "skill-loading"]);
const NAME = /^[^\s]{1,128}$/;
const isText = (value: unknown): value is string => typeof value === "string" && NAME.test(value);

/** Parse `foreground`. Malformed entries are hard config errors, never dropped. */
export const foregroundConfigValue = (value: unknown): FabricForegroundConfig => {
  const input = typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, unknown> : {};
  const tools = input.tools ?? [];
  if (!Array.isArray(tools) || tools.length > MAX_FOREGROUND_ENTRIES) {
    throw new Error(`foreground.tools must be an array of at most ${MAX_FOREGROUND_ENTRIES} entries`);
  }
  const maxTools = input.maxTools ?? 4;
  if (!Number.isInteger(maxTools) || (maxTools as number) < 0 || (maxTools as number) > MAX_FOREGROUND_TOOLS) {
    throw new Error(`foreground.maxTools must be an integer from 0 to ${MAX_FOREGROUND_TOOLS}`);
  }
  return {
    maxTools: maxTools as number,
    tools: tools.map((entry: unknown, index) => {
      const { name, owner, reason } = (entry ?? {}) as Record<string, unknown>;
      if (!isText(name) || name === "fabric_exec" || !isText(owner) || !REASONS.has(reason as string)) {
        throw new Error(
          `Invalid foreground.tools[${index}]: expected { name, owner, reason: ${[...REASONS].join(" | ")} }`,
        );
      }
      return { name, owner, reason: reason as FabricForegroundReason };
    }),
  };
};

export const NO_FOREGROUND: FabricForegroundResolution = { tools: [], refused: [] };

/** Resolve the declared set in policy order; every skipped entry is reported. */
export const resolveForegroundTools = (input: {
  policy: FabricForegroundConfig;
  mode: "full-code" | "enforce" | "orchestration";
  managedHost?: boolean;
  registered: readonly { name: string; exposure?: string; sourceInfo?: { source: string } }[];
  active: ReadonlySet<string>;
  allowlist?: ReadonlySet<string> | undefined;
}): FabricForegroundResolution => {
  const { policy } = input;
  // Orchestration already declares Pi's active set; the policy has nothing to add.
  if (policy.tools.length === 0 || input.mode === "orchestration") return NO_FOREGROUND;
  const blanket = input.mode === "enforce" ? "enforce" : input.managedHost ? "managed-host" : undefined;
  if (blanket) return { tools: [], refused: policy.tools.map(({ name }) => ({ name, reason: blanket })) };
  const registered = new Map(input.registered.map((tool) => [tool.name, tool]));
  const tools: string[] = [];
  const refused: { name: string; reason: FabricForegroundRefusalReason }[] = [];
  const seen = new Set<string>();
  for (const { name } of policy.tools) {
    const tool = registered.get(name);
    const reason: FabricForegroundRefusalReason | undefined =
      PI_CORE_TOOL_NAME_SET.has(name) ? "core"
        : seen.has(name) ? "duplicate"
          : !tool || tool.exposure === "hidden" ? "unknown"
            : tool.sourceInfo?.source === "builtin" ? "builtin"
              : input.allowlist && !input.allowlist.has(name) ? "not-allowed"
                : !input.active.has(name) ? "inactive"
                  : tools.length >= policy.maxTools ? "cap"
                    : undefined;
    seen.add(name);
    if (reason) refused.push({ name, reason });
    else tools.push(name);
  }
  return { tools, refused };
};

/** One line for the session notice and `/fabric status`. */
export const formatForeground = ({ tools, refused }: FabricForegroundResolution): string =>
  (refused.length > 0 ? `refused ${refused.map(({ name, reason }) => `${name} (${reason})`).join(", ")}; ` : "") +
  `declared ${tools.length > 0 ? tools.join(", ") : "fabric_exec only"}`;
