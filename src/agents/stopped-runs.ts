import type { AgentRunResult } from "./types.js";

/**
 * Session entry for task agents a reload or shutdown stopped (smarty-dev#1602).
 * The session file outlives the runtime and its temporary run root, so it is the
 * run record the next runtime reads back: `stopped` at close, `delivered` once
 * the spawner received or consumed the result.
 */
export const STOPPED_AGENTS_ENTRY = "pi-fabric-stopped-agents";

export interface StoppedAgentsEntryData {
  stopped?: AgentRunResult[];
  delivered?: string[];
}

export const readStoppedRuns = (
  entries: readonly unknown[],
): { runs: AgentRunResult[]; undelivered: AgentRunResult[] } => {
  const runs = new Map<string, AgentRunResult>();
  const delivered = new Set<string>();
  for (const entry of entries) {
    const custom = entry as { type?: unknown; customType?: unknown; data?: StoppedAgentsEntryData };
    if (custom?.type !== "custom" || custom.customType !== STOPPED_AGENTS_ENTRY) continue;
    for (const run of custom.data?.stopped ?? []) {
      if (typeof run?.id === "string") runs.set(run.id, run);
    }
    for (const id of custom.data?.delivered ?? []) delivered.add(id);
  }
  const all = [...runs.values()];
  return { runs: all, undelivered: all.filter((run) => !delivered.has(run.id)) };
};
