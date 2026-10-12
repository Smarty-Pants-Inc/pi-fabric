/**
 * Shared thinking (reasoning effort) level type and helpers.
 *
 * Fabric resolves a thinking level per run (explicit call/actor value, else the
 * Fabric default, "medium"). Pi receives it via "--thinking" and clamps it to
 * the model's supported levels using next-highest fallback (see pi-ai
 * clampThinkingLevel). Claude receives it via "--effort"; off/minimal map to
 * low. Fabric itself only selects the requested/default level.
 */
export type FabricThinking = "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";

/** Fabric-wide default thinking level, used when a call/actor omits one. */
export const DEFAULT_FABRIC_THINKING: FabricThinking = "medium";

/** Ordered lowest -> highest; matches pi-ai's EXTENDED_THINKING_LEVELS. */
export const THINKING_LEVELS: readonly FabricThinking[] = [
  "off",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
];

/** Type guard for a Fabric thinking level value (config, CLI args, JSON). */
export const isFabricThinking = (value: unknown): value is FabricThinking =>
  typeof value === "string" && (THINKING_LEVELS as readonly string[]).includes(value);

const LABELS: Record<FabricThinking, string> = {
  off: "Off",
  minimal: "Minimal",
  low: "Low",
  medium: "Medium",
  high: "High",
  xhigh: "XHigh",
  max: "Max",
};

/** Human-readable label for a thinking level (shown in pickers and settings). */
export const thinkingLabel = (level: FabricThinking): string => LABELS[level];

/**
 * Inclusive thinking-level bounds. An absent end is unbounded on that side
 * (the active model's levels decide). Bounds only ever narrow across process
 * boundaries: a child intersects its own configuration with the parent's
 * bounds received through PI_FABRIC_THINKING_BOUNDS.
 */
export interface FabricThinkingBounds {
  min?: FabricThinking;
  max?: FabricThinking;
}

/** Environment variable carrying a parent's effective bounds into a Pi child. */
export const FABRIC_THINKING_BOUNDS_ENV = "PI_FABRIC_THINKING_BOUNDS";
/** Session custom entry recording the active Fabric thinking override. */
export const FABRIC_THINKING_ENTRY_TYPE = "pi-fabric-thinking";

export const thinkingRank = (level: FabricThinking): number => THINKING_LEVELS.indexOf(level);

/**
 * Validate a bounds object from configuration, arguments, or the environment.
 * Unknown keys, unknown level names and inverted ranges are rejected: bounds
 * are a ceiling policy, so a malformed value fails closed instead of widening.
 */
export const normalizeThinkingBounds = (value: unknown, where: string): FabricThinkingBounds => {
  if (value === undefined || value === null) return {};
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${where} must be an object like {"min":"low","max":"high"}`);
  }
  const record = value as Record<string, unknown>;
  const extra = Object.keys(record).filter((key) => key !== "min" && key !== "max");
  if (extra.length > 0) throw new Error(`${where} accepts only min and max (got ${extra.join(", ")})`);
  const bounds: FabricThinkingBounds = {};
  for (const key of ["min", "max"] as const) {
    const level = record[key];
    if (level === undefined) continue;
    if (!isFabricThinking(level)) {
      throw new Error(`${where}.${key} must be one of ${THINKING_LEVELS.join(", ")}`);
    }
    bounds[key] = level;
  }
  if (bounds.min && bounds.max && thinkingRank(bounds.min) > thinkingRank(bounds.max)) {
    throw new Error(`${where}.min (${bounds.min}) must not exceed ${where}.max (${bounds.max})`);
  }
  return bounds;
};

/**
 * Narrow `outer` by `inner` without ever widening it. When the two ranges do
 * not overlap the outer (parent) range wins unchanged: it is the cost policy.
 */
export const intersectThinkingBounds = (
  outer: FabricThinkingBounds,
  inner: FabricThinkingBounds,
): FabricThinkingBounds => {
  const pick = (a: FabricThinking | undefined, b: FabricThinking | undefined, higher: boolean) =>
    a === undefined ? b : b === undefined ? a
      : (thinkingRank(a) >= thinkingRank(b)) === higher ? a : b;
  const max = pick(outer.max, inner.max, false);
  const min = pick(outer.min, inner.min, true);
  if (min !== undefined && max !== undefined && thinkingRank(min) > thinkingRank(max)) {
    return { ...outer };
  }
  return { ...(min ? { min } : {}), ...(max ? { max } : {}) };
};

/**
 * True when every explicit end of `inner` lies inside `outer`. An omitted end
 * inherits the outer end, so `{min:"low"}` fits inside `{max:"high"}`.
 */
export const thinkingBoundsWithin = (
  inner: FabricThinkingBounds,
  outer: FabricThinkingBounds,
): boolean => {
  const inside = (level: FabricThinking | undefined) => level === undefined ||
    (thinkingRank(level) >= thinkingRank(outer.min ?? "off") &&
      thinkingRank(level) <= thinkingRank(outer.max ?? "max"));
  return inside(inner.min) && inside(inner.max);
};

/**
 * Effective bounds for a child run: requested bounds must lie inside the
 * parent's (fail closed), then narrow them. No request inherits the parent.
 */
export const childThinkingBounds = (
  parent: FabricThinkingBounds,
  requested: FabricThinkingBounds | undefined,
): FabricThinkingBounds => {
  if (!requested) return { ...parent };
  if (!thinkingBoundsWithin(requested, parent)) {
    throw new Error(
      `thinkingBounds ${JSON.stringify(requested)} must lie inside this session's thinking bounds ` +
        `(${parent.min ?? "off"}..${parent.max ?? "max"})`,
    );
  }
  return intersectThinkingBounds(parent, requested);
};

/** Clamp a level into inclusive bounds (no model knowledge). */
export const clampThinkingToBounds = (
  level: FabricThinking,
  bounds: FabricThinkingBounds,
): FabricThinking => {
  if (bounds.min && thinkingRank(level) < thinkingRank(bounds.min)) return bounds.min;
  if (bounds.max && thinkingRank(level) > thinkingRank(bounds.max)) return bounds.max;
  return level;
};

/** Levels allowed by bounds, optionally restricted to a model's supported levels. */
export const thinkingLevelsWithin = (
  bounds: FabricThinkingBounds,
  supported: readonly FabricThinking[] = THINKING_LEVELS,
): FabricThinking[] =>
  THINKING_LEVELS.filter((level) =>
    supported.includes(level) && clampThinkingToBounds(level, bounds) === level);

/**
 * Pick the allowed level for a request: clamp into bounds, then use pi-ai's
 * next-highest-then-lower fallback restricted to `allowed`. Undefined when
 * nothing is allowed.
 */
export const selectThinkingLevel = (
  requested: FabricThinking,
  allowed: readonly FabricThinking[],
): FabricThinking | undefined => {
  if (allowed.includes(requested)) return requested;
  const index = thinkingRank(requested);
  for (let rank = index + 1; rank < THINKING_LEVELS.length; rank += 1) {
    if (allowed.includes(THINKING_LEVELS[rank]!)) return THINKING_LEVELS[rank];
  }
  for (let rank = index - 1; rank >= 0; rank -= 1) {
    if (allowed.includes(THINKING_LEVELS[rank]!)) return THINKING_LEVELS[rank];
  }
  return undefined;
};

/** Structural subset of a pi-ai Model; mirrors getSupportedThinkingLevels. */
export interface FabricThinkingModel {
  reasoning?: boolean;
  thinkingLevelMap?: Partial<Record<string, string | null>>;
}

/**
 * Supported levels for a model, matching pi-ai getSupportedThinkingLevels
 * without importing it (Fabric keeps pi-ai out of its eager graph). No model
 * means Pi offers every level.
 */
export const modelThinkingLevels = (model: FabricThinkingModel | undefined): FabricThinking[] => {
  if (!model) return [...THINKING_LEVELS];
  if (!model.reasoning) return ["off"];
  return THINKING_LEVELS.filter((level) => {
    const mapped = model.thinkingLevelMap?.[level];
    if (mapped === null) return false;
    if (level === "xhigh" || level === "max") return mapped !== undefined;
    return true;
  });
};

/** Bounds inherited from a parent Fabric; malformed values throw (fail closed). */
export const inheritedThinkingBounds = (
  env: NodeJS.ProcessEnv = process.env,
): FabricThinkingBounds | undefined => {
  const source = env[FABRIC_THINKING_BOUNDS_ENV];
  if (source === undefined || source.trim() === "") return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(source);
  } catch {
    throw new Error(`Invalid ${FABRIC_THINKING_BOUNDS_ENV}: expected JSON {"min"?,"max"?}`);
  }
  return normalizeThinkingBounds(parsed, FABRIC_THINKING_BOUNDS_ENV);
};

/** Compact JSON for PI_FABRIC_THINKING_BOUNDS; undefined when unbounded. */
export const serializeThinkingBounds = (bounds: FabricThinkingBounds): string | undefined =>
  bounds.min || bounds.max
    ? JSON.stringify({ ...(bounds.min ? { min: bounds.min } : {}), ...(bounds.max ? { max: bounds.max } : {}) })
    : undefined;
