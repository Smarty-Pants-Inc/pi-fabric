import { isActiveStatus, type FabricDashboardSnapshot, type FabricUiAgent } from "./types.js";

export const WIDGET_FINISHED_RETENTION_MS = 30_000;

export const isResidentWidgetAgent = (agent: FabricUiAgent): boolean =>
  agent.status === "idle" || agent.status === "state";

export const isRecentWidgetCompletion = (finishedAt: number, now: number): boolean =>
  Math.max(0, now - finishedAt) < WIDGET_FINISHED_RETENTION_MS;

/** Display-only leases; never change manager records or dashboard snapshots. */
export class FabricWidgetRetention {
  readonly #fallbacks = new Map<string, { startedAt: number | undefined; finishedAt: number }>();

  clear(): void {
    this.#fallbacks.clear();
  }

  sync(snapshot: FabricDashboardSnapshot): void {
    const ids = new Set(snapshot.agents.map(agent => agent.id));
    for (const id of this.#fallbacks.keys()) {
      if (!ids.has(id)) this.#fallbacks.delete(id);
    }
    for (const agent of snapshot.agents) this.finishedAt(agent, snapshot.now);
  }

  finishedAt(agent: FabricUiAgent, now: number): number {
    if (isActiveStatus(agent.status) || isResidentWidgetAgent(agent)) {
      this.#fallbacks.delete(agent.id);
      return agent.finishedAt ?? agent.updatedAt ?? agent.startedAt ?? now;
    }
    if (agent.finishedAt !== undefined && Number.isFinite(agent.finishedAt)) {
      this.#fallbacks.set(agent.id, { startedAt: agent.startedAt, finishedAt: agent.finishedAt });
      return agent.finishedAt;
    }
    const previous = this.#fallbacks.get(agent.id);
    if (previous && previous.startedAt === agent.startedAt) return previous.finishedAt;
    const finishedAt = [agent.updatedAt, agent.startedAt, now].find(
      (value): value is number => value !== undefined && Number.isFinite(value),
    ) ?? 0;
    // A heartbeat/control refresh must not renew a missing-finishedAt lease.
    this.#fallbacks.set(agent.id, { startedAt: agent.startedAt, finishedAt });
    return finishedAt;
  }

  visible(agent: FabricUiAgent, snapshot: FabricDashboardSnapshot): boolean {
    if (isActiveStatus(agent.status) || isResidentWidgetAgent(agent)) return true;
    const finishedAt = this.finishedAt(agent, snapshot.now);
    return (snapshot.widgetDismissedAt === undefined || finishedAt > snapshot.widgetDismissedAt) &&
      isRecentWidgetCompletion(finishedAt, snapshot.now);
  }

  nextExpiryDelay(snapshot: FabricDashboardSnapshot, now: number): number | undefined {
    const deadlines: number[] = [];
    for (const agent of snapshot.agents) {
      if (isActiveStatus(agent.status) || isResidentWidgetAgent(agent)) continue;
      const finishedAt = this.finishedAt(agent, snapshot.now);
      if (snapshot.widgetDismissedAt !== undefined && finishedAt <= snapshot.widgetDismissedAt) continue;
      deadlines.push(finishedAt + WIDGET_FINISHED_RETENTION_MS);
    }
    for (const shell of snapshot.shells ?? []) {
      if (shell.finishedAt !== undefined) deadlines.push(shell.finishedAt + WIDGET_FINISHED_RETENTION_MS);
    }
    const remaining = deadlines.filter(deadline => deadline > now).map(deadline => deadline - now);
    return remaining.length ? Math.min(...remaining) : undefined;
  }
}
