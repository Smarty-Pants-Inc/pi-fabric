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

/**
 * Restore a previous runtime's stopped runs for wait/status/steer, and replay each undelivered
 * one through the completion inbox only when completion notices are on. Returns the callback that
 * marks a run delivered (notice flushed or result consumed), once per run.
 */
export const restoreStoppedRuns = (options: {
  entries: readonly unknown[];
  notifyOnComplete: boolean;
  restore: (runs: AgentRunResult[]) => void;
  enqueue: (run: AgentRunResult, delivered: () => void) => void;
  appendEntry: (data: StoppedAgentsEntryData) => void;
}): ((id: string) => void) => {
  const { runs, undelivered } = readStoppedRuns(options.entries);
  const pending = new Set(undelivered.map((run) => run.id));
  const markDelivered = (id: string): void => {
    if (!pending.delete(id)) return;
    try {
      options.appendEntry({ delivered: [id] });
    } catch { /* a stale runtime: the next start delivers it */ }
  };
  options.restore(runs);
  if (options.notifyOnComplete) {
    for (const run of undelivered) options.enqueue(run, () => markDelivered(run.id));
  }
  return markDelivered;
};

// ponytail: a process-global handoff. A reload replaces this module and clears the chat, but the
// process and the session id stay; the next session_start shows what the close stopped (smarty-dev#1882).
const STOPPED_AT_CLOSE = Symbol.for("pi-fabric.stopped-at-close");
const stoppedAtClose = (): Map<string, string[]> =>
  ((globalThis as Record<symbol, unknown>)[STOPPED_AT_CLOSE] ??= new Map<string, string[]>()) as Map<string, string[]>;

export const rememberStoppedAtClose = (sessionId: string, runs: readonly AgentRunResult[]): void => {
  stoppedAtClose().set(sessionId, runs.map((run) => run.name || run.id));
};

/** The notice for the reloaded session, once; any other start only drops the record. */
export const takeReloadStoppedNotice = (sessionId: string, reason: string): string | undefined => {
  const names = stoppedAtClose().get(sessionId);
  stoppedAtClose().delete(sessionId);
  if (reason !== "reload" || !names?.length) return undefined;
  return `The last /reload stopped ${names.length} task agent${names.length === 1 ? "" : "s"}: ${names.join(", ")}; ` +
    'spawn with residency: "durable" to keep agents across reloads.';
};
