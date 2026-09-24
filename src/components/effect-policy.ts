import { unknownConflict, knownConflict } from "../verified/policy.js";
import { boundedEffectResources, encodeResourceEffects, resourceGroups } from "../verified/resources.js";
import type { ResolvedFabricAction } from "../core/action-registry.js";
import type {
  FabricComponentEffectConflict,
  FabricComponentEffectInfo,
  FabricComponentEffectOptions,
  FabricComponentEffectRegistration,
} from "./types.js";

export class FabricComponentIndependenceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FabricComponentIndependenceError";
  }
}

const normalizeResources = boundedEffectResources;

export const trackedRegistration = (
  registration: FabricComponentEffectRegistration | undefined,
  fallbackLabel: string,
): FabricComponentEffectOptions => {
  if (typeof registration === "string") return { label: registration };
  return { label: fallbackLabel, ...registration };
};

export const registrationEffect = (
  registration: FabricComponentEffectOptions,
): FabricComponentEffectInfo => ({
  label: registration.label?.trim().slice(0, 256) || "anonymous",
  kind: registration.kind ?? "transactional",
  resources: normalizeResources(registration.resources),
  ordering: registration.ordering ?? "unknown",
});

export const actionEffect = (
  action: ResolvedFabricAction,
): FabricComponentEffectInfo | undefined => {
  if (!action.effect || action.effect.kind === "none") return undefined;
  return {
    label: action.ref,
    kind: action.effect.kind,
    resources: normalizeResources(action.effect.resources),
    ordering: action.effect.ordering ?? "unknown",
  };
};

interface FabricComponentEffectSummary {
  declarations: ReturnType<typeof encodeResourceEffects>;
  hasEffects: boolean;
  hasNoncommutative: boolean;
  hasUnknown: boolean;
  hasUnknownNoncommutative: boolean;
  resourceNoncommutative: Map<string, boolean>;
}

type FabricComponentConflictBasis = Omit<FabricComponentEffectConflict, "withComponent">;

export const summarizeEffects = (
  effects: readonly FabricComponentEffectInfo[],
): FabricComponentEffectSummary => {
  const resourceNoncommutative = new Map<string, boolean>();
  let hasNoncommutative = false;
  let hasUnknown = false;
  let hasUnknownNoncommutative = false;
  let effectful = 0;
  for (const effect of effects) {
    if (effect.kind === "none") continue;
    effectful++;
    const noncommutative = effect.ordering !== "commutative";
    hasNoncommutative ||= noncommutative;
    for (const resource of effect.resources) {
      if (resource === "*") {
        hasUnknown = true;
        hasUnknownNoncommutative ||= noncommutative;
      } else {
        resourceNoncommutative.set(
          resource,
          (resourceNoncommutative.get(resource) ?? false) || noncommutative,
        );
      }
    }
  }
  return {
    declarations: encodeResourceEffects(effects),
    hasEffects: effectful > 0,
    hasNoncommutative,
    hasUnknown,
    hasUnknownNoncommutative,
    resourceNoncommutative,
  };
};

export const effectConflictsBetween = (
  left: FabricComponentEffectSummary,
  right: FabricComponentEffectSummary,
): FabricComponentConflictBasis[] => {
  // Only the compiled full-declaration decision can grant independence.
  // The summaries below explain a conflict; they cannot erase one.
  if (!resourceGroups(left.declarations, right.declarations)) return [];
  const conflicts: FabricComponentConflictBasis[] = [];
  if (
    unknownConflict(
      left.hasUnknown, left.hasUnknownNoncommutative, left.hasNoncommutative,
      right.hasUnknown, right.hasUnknownNoncommutative, right.hasNoncommutative,
    )
  ) {
    conflicts.push({ resources: ["*"], reason: "unknown_resource" });
  }
  const overlap = [...left.resourceNoncommutative.keys()]
    .filter((resource) =>
      knownConflict(
        right.resourceNoncommutative.has(resource),
        left.resourceNoncommutative.get(resource) ?? false,
        right.resourceNoncommutative.get(resource) ?? false,
      ),
    )
    .sort();
  if (overlap.length > 0) {
    conflicts.push({ resources: overlap, reason: "shared_resource" });
  }
  return conflicts.length > 0 ? conflicts : [{ resources: ["*"], reason: "unknown_resource" }];
};

export const compareEffectInfo = (
  left: FabricComponentEffectInfo,
  right: FabricComponentEffectInfo,
): number =>
  left.label.localeCompare(right.label) ||
  left.kind.localeCompare(right.kind) ||
  left.ordering.localeCompare(right.ordering) ||
  left.resources.join("\0").localeCompare(right.resources.join("\0"));

