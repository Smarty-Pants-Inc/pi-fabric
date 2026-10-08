export interface FabricAgentRouterConfig {
  /** Trusted host command argv; never interpreted by a shell. */
  command: string[];
  timeoutMs?: number;
  mode?: "off" | "shadow" | "enforce";
  includeTask?: boolean;
}

export const normalizeAgentRouterConfig = (value: unknown): FabricAgentRouterConfig | undefined => {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const input = value as Record<string, unknown>;
  return {
    command: Array.isArray(input.command) && input.command.length > 0 &&
      input.command.every(arg => typeof arg === "string" && !arg.includes("\0")) &&
      typeof input.command[0] === "string" && !!input.command[0].trim()
      ? [...input.command as string[]] : [],
    timeoutMs: typeof input.timeoutMs === "number" && Number.isFinite(input.timeoutMs)
      ? Math.max(200, Math.min(5000, Math.round(input.timeoutMs))) : 1500,
    mode: input.mode === "shadow" || input.mode === "enforce" ? input.mode : "off",
    includeTask: input.includeTask === true,
  };
};
