/** Value-free diagnostics only: never derive a label from a record key or payload. */
export const PRESENCE_KEY_CLASSES = ["topology/hosts", "topology/participants", "presence"] as const;
export const PRESENCE_DROP_REASONS = [
  "malformed", "writer-mismatch", "conflicting-copy", "owner-mismatch", "conflicting-root", "frame-budget", "unattributed",
] as const;
export type PresenceKeyClass = typeof PRESENCE_KEY_CLASSES[number];
export type PresenceDropReason = typeof PRESENCE_DROP_REASONS[number];
export interface PresenceDropCount { keyClass: PresenceKeyClass; reason: PresenceDropReason; count: number }

const countOf = (value: unknown): number =>
  typeof value === "number" && Number.isSafeInteger(value) && value > 0 ? value : 0;

/** Fixed vocabulary bounds both memory and wire size, even for unchecked peer diagnostics. */
export class PresenceDropReasons {
  readonly #counts = new Map<string, PresenceDropCount>();

  constructor(raw: unknown = [], total = 0) {
    let remaining = countOf(total);
    if (Array.isArray(raw)) for (const value of raw) {
      if (!value || typeof value !== "object" ||
        !PRESENCE_KEY_CLASSES.includes(value.keyClass) || !PRESENCE_DROP_REASONS.includes(value.reason)) continue;
      const count = Math.min(remaining, countOf(value.count));
      this.add(value.keyClass, value.reason, count);
      remaining -= count;
      if (remaining === 0) break;
    }
    // Old peers and adapters may only supply a count. Do not invent a more specific cause.
    this.add("presence", "unattributed", remaining);
  }

  add(keyClass: PresenceKeyClass, reason: PresenceDropReason, count = 1): void {
    if (count <= 0) return;
    const key = `${reason}:${keyClass}`;
    const previous = this.#counts.get(key)?.count ?? 0;
    this.#counts.set(key, { keyClass, reason, count: Math.min(Number.MAX_SAFE_INTEGER, previous + count) });
  }

  entries(): PresenceDropCount[] {
    return [...this.#counts.values()].sort((a, b) =>
      `${a.reason}:${a.keyClass}`.localeCompare(`${b.reason}:${b.keyClass}`));
  }

  summary(): string {
    const entries = this.entries();
    return entries.slice(0, 8).map(({ keyClass, reason, count }) => `${reason}:${keyClass}=${count}`).join(" ") +
      (entries.length > 8 ? ` +${entries.length - 8} more` : "");
  }
}
