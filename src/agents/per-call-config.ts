// Per-call routing config (smarty-dev#2890, #6062). Kept dependency-free: config.ts parses it at
// startup, and the router itself (per-call-route.ts) loads only when mode is "shadow".
export interface PerCallRoutingConfig {
  /** "off" (default) loads nothing; "shadow" classifies each LLM call and appends the per-call ledger. */
  mode: "shadow" | "off";
  /** Above this context estimate a call is never SIMPLE: a model switch would re-read it uncached. */
  maxContextTokens: number;
  /** An edit/write tool in the last N assistant turns marks the author phase: never SIMPLE. */
  authorTurns: number;
  /** Ambiguous steps: ask Jev through the Node gateway (jev.gatewaySocket), the direct client, or not at all. */
  jev: "gateway" | "direct" | "off";
  /** Jev questions per session (cache hits are free); past it, ambiguous steps stay MAIN. */
  jevMaxCallsPerSession: number;
}
export const DEFAULT_PER_CALL_ROUTING: PerCallRoutingConfig = {
  mode: "off", maxContextTokens: 100_000, authorTurns: 3, jev: "gateway", jevMaxCallsPerSession: 40,
};
/** Strict parse: unknown modes and "live" fail loudly, as #434's live flag does. */
export function parsePerCallRouting(value: unknown): PerCallRoutingConfig {
  if (value === undefined) return { ...DEFAULT_PER_CALL_ROUTING };
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("Invalid agents.modelRouting.perCall");
  const input = value as Record<string, unknown>;
  if (input.mode === "live") throw new Error("Live per-call model routing is unavailable: shadow only until the shadow ledger is priced (smarty-dev#2890)");
  if (input.mode !== undefined && input.mode !== "shadow" && input.mode !== "off") throw new Error("Invalid agents.modelRouting.perCall.mode");
  if (input.jev !== undefined && input.jev !== "gateway" && input.jev !== "direct" && input.jev !== "off") throw new Error("Invalid agents.modelRouting.perCall.jev");
  const integer = (key: "maxContextTokens" | "authorTurns" | "jevMaxCallsPerSession", min: number, max: number): number => {
    const v = input[key];
    if (v === undefined) return DEFAULT_PER_CALL_ROUTING[key];
    if (typeof v !== "number" || !Number.isSafeInteger(v) || v < min || v > max) throw new Error(`Invalid agents.modelRouting.perCall.${key}`);
    return v;
  };
  return {
    mode: (input.mode as PerCallRoutingConfig["mode"] | undefined) ?? "off",
    maxContextTokens: integer("maxContextTokens", 1_000, 10_000_000),
    authorTurns: integer("authorTurns", 1, 50),
    jev: (input.jev as PerCallRoutingConfig["jev"] | undefined) ?? DEFAULT_PER_CALL_ROUTING.jev,
    jevMaxCallsPerSession: integer("jevMaxCallsPerSession", 0, 10_000),
  };
}
