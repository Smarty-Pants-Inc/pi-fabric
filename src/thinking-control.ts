import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
  clampThinkingToBounds,
  FABRIC_THINKING_ENTRY_TYPE,
  isFabricThinking,
  modelThinkingLevels,
  selectThinkingLevel,
  thinkingLevelsWithin,
  type FabricThinking,
  type FabricThinkingBounds,
  type FabricThinkingModel,
} from "./thinking.js";

// Host-session thinking control. The model can raise or lower the main
// session's reasoning effort for a bounded scope; Fabric applies it through
// Pi's own setThinkingLevel and reverts at agent_end. Overrides persist as a
// session custom entry so a reload restores the pending revert. FabricState
// loads this module on first need (see FabricThinkingHost), keeping it off the
// startup graph.

export { FABRIC_THINKING_ENTRY_TYPE };

export const MAX_THINKING_OVERRIDE_TURNS = 20;
export const MAX_THINKING_REASON_CHARS = 256;

export type FabricThinkingScope = "turn" | "turns" | "session";

export interface FabricThinkingOverride {
  level: FabricThinking;
  scope: FabricThinkingScope;
  /** `turns` scope only: agent_end events left before reverting. */
  remainingTurns?: number;
  reason?: string;
  setAt: number;
}

export interface FabricThinkingStatus {
  level: FabricThinking;
  /** Model-supported levels inside the effective bounds. */
  available: FabricThinking[];
  /** Effective inclusive bounds; unbounded ends resolve to the model's levels. */
  bounds: { min: FabricThinking; max: FabricThinking };
  /** Level restored when the override ends (the current level without one). */
  baseline: FabricThinking;
  override?: FabricThinkingOverride;
}

export interface FabricThinkingSetResult extends FabricThinkingStatus {
  clamped?: true;
  requested?: FabricThinking;
}

export interface FabricThinkingSetInput {
  level: FabricThinking;
  scope?: FabricThinkingScope;
  turns?: number;
  reason?: string;
}

interface PersistedOverride extends FabricThinkingOverride {
  baseline: FabricThinking;
}

interface FabricThinkingEntry {
  version: 1;
  override: PersistedOverride | null;
}

type ThinkingHost = Pick<ExtensionAPI, "getThinkingLevel" | "setThinkingLevel" | "appendEntry">;
type ThinkingContext = Pick<ExtensionContext, "sessionManager"> & { model?: FabricThinkingModel | undefined };

const SCOPES: readonly FabricThinkingScope[] = ["turn", "turns", "session"];

const persistedOverride = (value: unknown): PersistedOverride | null | undefined => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const entry = value as Record<string, unknown>;
  if (entry.version !== 1) return undefined;
  if (entry.override === null) return null;
  if (typeof entry.override !== "object" || Array.isArray(entry.override)) return undefined;
  const data = entry.override as Record<string, unknown>;
  if (!isFabricThinking(data.level) || !isFabricThinking(data.baseline)) return undefined;
  if (!SCOPES.includes(data.scope as FabricThinkingScope)) return undefined;
  if (typeof data.setAt !== "number" || !Number.isFinite(data.setAt)) return undefined;
  const remaining = data.remainingTurns;
  if (data.scope === "turns" && !(Number.isInteger(remaining) &&
    (remaining as number) >= 1 && (remaining as number) <= MAX_THINKING_OVERRIDE_TURNS)) return undefined;
  return {
    level: data.level,
    baseline: data.baseline,
    scope: data.scope as FabricThinkingScope,
    setAt: data.setAt,
    ...(data.scope === "turns" ? { remainingTurns: remaining as number } : {}),
    ...(typeof data.reason === "string" ? { reason: data.reason.slice(0, MAX_THINKING_REASON_CHARS) } : {}),
  };
};

export class FabricThinkingController {
  #override: PersistedOverride | undefined;
  /** Session id whose branch has been replayed; undefined forces a replay. */
  #restoredFor: string | undefined;

  constructor(
    private readonly host: ThinkingHost,
    private readonly bounds: () => FabricThinkingBounds,
  ) {}

  /** Session switch or tree navigation: replay the branch on next use. */
  invalidate(): void {
    this.#override = undefined;
    this.#restoredFor = undefined;
  }

  status(context: ThinkingContext): FabricThinkingStatus {
    this.#restore(context);
    const level = this.#level();
    const bounds = this.bounds();
    const supported = modelThinkingLevels(context.model);
    const available = thinkingLevelsWithin(bounds, supported);
    const override = this.#override;
    return {
      level,
      available,
      bounds: {
        min: bounds.min ?? supported[0] ?? "off",
        max: bounds.max ?? supported.at(-1) ?? "off",
      },
      baseline: override?.baseline ?? level,
      ...(override ? { override: publicOverride(override) } : {}),
    };
  }

  set(input: FabricThinkingSetInput, context: ThinkingContext): FabricThinkingSetResult {
    if (!isFabricThinking(input.level)) throw new Error(`Unknown thinking level: ${String(input.level)}`);
    const scope = input.scope ?? "turn";
    if (!SCOPES.includes(scope)) throw new Error(`Unknown thinking scope: ${String(scope)}`);
    if (scope === "turns") {
      if (!Number.isInteger(input.turns) || input.turns! < 1 || input.turns! > MAX_THINKING_OVERRIDE_TURNS) {
        throw new Error(`thinking.set scope "turns" requires turns between 1 and ${MAX_THINKING_OVERRIDE_TURNS}`);
      }
    } else if (input.turns !== undefined) {
      throw new Error('thinking.set turns is only valid with scope "turns"');
    }
    if (input.reason !== undefined && input.reason.length > MAX_THINKING_REASON_CHARS) {
      throw new Error(`thinking.set reason must be at most ${MAX_THINKING_REASON_CHARS} characters`);
    }
    this.#restore(context);
    const bounds = this.bounds();
    const allowed = thinkingLevelsWithin(bounds, modelThinkingLevels(context.model));
    const target = selectThinkingLevel(clampThinkingToBounds(input.level, bounds), allowed);
    if (!target) {
      throw new Error(
        `No thinking level supported by the active model lies within the configured bounds (${
          bounds.min ?? "off"}..${bounds.max ?? "max"})`,
      );
    }
    const baseline = this.#override?.baseline ?? this.#level();
    this.host.setThinkingLevel(target);
    // Pi clamps again to model capabilities; report what actually applies.
    const applied = this.#level();
    this.#override = {
      level: applied,
      baseline,
      scope,
      setAt: Date.now(),
      ...(scope === "turns" ? { remainingTurns: input.turns! } : {}),
      ...(input.reason !== undefined ? { reason: input.reason } : {}),
    };
    this.#persist();
    return {
      ...this.status(context),
      ...(applied !== input.level ? { clamped: true as const, requested: input.level } : {}),
    };
  }

  reset(context: ThinkingContext): FabricThinkingStatus {
    this.#restore(context);
    const override = this.#override;
    if (override) {
      this.#override = undefined;
      this.host.setThinkingLevel(override.baseline);
      this.#persist();
    }
    return this.status(context);
  }

  /**
   * agent_end boundary: count down `turns`, end `turn`. If someone else changed
   * the level since Fabric applied it (user, another extension), the override
   * ends without restoring the baseline: Fabric never fights for the setting.
   */
  agentEnded(context: ThinkingContext): void {
    this.#restore(context);
    const override = this.#override;
    if (!override || override.scope === "session") return;
    if (override.scope === "turns" && (override.remainingTurns ?? 1) > 1) {
      this.#override = { ...override, remainingTurns: (override.remainingTurns ?? 1) - 1 };
      this.#persist();
      return;
    }
    this.#override = undefined;
    if (this.#level() === override.level) this.host.setThinkingLevel(override.baseline);
    this.#persist();
  }

  /**
   * Child sessions: move a level that lies outside the inherited bounds back
   * inside them. Pi's own model clamp may otherwise overshoot a parent's max.
   */
  enforceBounds(context: ThinkingContext): void {
    const bounds = this.bounds();
    if (!bounds.min && !bounds.max) return;
    const level = this.#level();
    const allowed = thinkingLevelsWithin(bounds, modelThinkingLevels(context.model));
    if (allowed.includes(level)) return;
    const target = selectThinkingLevel(clampThinkingToBounds(level, bounds), allowed);
    if (target && target !== level) this.host.setThinkingLevel(target);
  }

  #level(): FabricThinking {
    const level = this.host.getThinkingLevel();
    return isFabricThinking(level) ? level : "off";
  }

  #persist(): void {
    const entry: FabricThinkingEntry = { version: 1, override: this.#override ?? null };
    this.host.appendEntry(FABRIC_THINKING_ENTRY_TYPE, entry);
  }

  #restore(context: ThinkingContext): void {
    const sessionId = context.sessionManager.getSessionId();
    if (this.#restoredFor === sessionId) return;
    this.#restoredFor = sessionId;
    this.#override = undefined;
    const branch = context.sessionManager.getBranch();
    for (let index = branch.length - 1; index >= 0; index -= 1) {
      const entry = branch[index];
      if (entry?.type !== "custom" || entry.customType !== FABRIC_THINKING_ENTRY_TYPE) continue;
      const restored = persistedOverride(entry.data);
      // A malformed latest entry ends the override rather than resurrecting an older one.
      this.#override = restored ?? undefined;
      return;
    }
  }
}

const publicOverride = (override: PersistedOverride): FabricThinkingOverride => {
  const { baseline: _baseline, ...visible } = override;
  return visible;
};
