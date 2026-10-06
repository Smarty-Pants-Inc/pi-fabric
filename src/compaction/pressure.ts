import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import type { FabricConfig } from "../config.js";
import { ownerFromContext, type CompactionOwner } from "./owner.js";
import { modelCompactionKey, outputReserveBreached } from "./threshold.js";

// A read-only projection of how full the host context is. It never compacts:
// programs decide what to do with the band (request a compaction, narrow the
// carry list, hand off, or ignore it).

export type CompactionPressureBand = "ok" | "warn" | "urgent" | "unknown";

export interface CompactionPressure {
  tokens: number | null;
  contextWindow: number | null;
  fraction: number | null;
  headroomTokens: number | null;
  band: CompactionPressureBand;
  outputReserveTokens: number;
  thresholdFraction?: number;
  thresholdTokens?: number;
  owner: CompactionOwner;
}

type PressureConfig = Pick<
  FabricConfig["compaction"],
  "pressureBands" | "outputReserveTokens" | "thresholds" | "tokenThresholds"
>;

const positiveWindow = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) && value > 0 ? value : undefined;

export const compactionPressure = (
  context: Pick<ExtensionContext, "getContextUsage" | "model" | "sessionManager"> | undefined,
  config: PressureConfig,
): CompactionPressure => {
  let usage: ReturnType<ExtensionContext["getContextUsage"]>;
  try {
    usage = context?.getContextUsage?.();
  } catch {
    usage = undefined;
  }
  const modelKey = modelCompactionKey(context?.model);
  const thresholdTokens = modelKey === undefined ? undefined : config.tokenThresholds[modelKey];
  // A token threshold wins over a ratio for the same model (see threshold.ts).
  const thresholdFraction = modelKey === undefined || thresholdTokens !== undefined
    ? undefined
    : config.thresholds[modelKey];
  const contextWindow = positiveWindow(usage?.contextWindow) ?? positiveWindow(context?.model?.contextWindow);
  const tokens = typeof usage?.tokens === "number" && Number.isFinite(usage.tokens) ? usage.tokens : null;
  const known = tokens !== null && contextWindow !== undefined;
  const fraction = known ? tokens / contextWindow : null;
  const headroomTokens = known ? contextWindow - tokens : null;
  const band: CompactionPressureBand = fraction === null
    ? "unknown"
    : fraction >= config.pressureBands.urgent
      || outputReserveBreached({ tokens, contextWindow: contextWindow! }, config.outputReserveTokens)
      ? "urgent"
      : fraction >= config.pressureBands.warn
        ? "warn"
        : "ok";
  return {
    tokens,
    contextWindow: contextWindow ?? null,
    fraction,
    headroomTokens,
    band,
    outputReserveTokens: config.outputReserveTokens,
    ...(thresholdFraction !== undefined ? { thresholdFraction } : {}),
    ...(thresholdTokens !== undefined ? { thresholdTokens } : {}),
    owner: ownerFromContext(context),
  };
};
