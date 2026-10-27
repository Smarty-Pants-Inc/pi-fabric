/** Fixed diagnostic vocabulary; copy conflicts may additionally name bounded participant ids. */
export const PRESENCE_KEY_CLASSES = ["topology/hosts", "topology/participants", "presence"] as const;
export const PRESENCE_DROP_REASONS = [
  "malformed", "writer-mismatch", "conflicting-copy", "owner-mismatch", "conflicting-root", "frame-budget", "unattributed",
] as const;
export type PresenceKeyClass = typeof PRESENCE_KEY_CLASSES[number];
export type PresenceDropReason = typeof PRESENCE_DROP_REASONS[number];
export const PRESENCE_COPY_FIELDS = [
  "id", "kind", "rootId", "ownerHostId", "ownerIdentityId", "ownerIncarnation", "name", "sessionId", "status",
  "runner", "transport", "capabilities", "controlProtocol", "startedAt", "remoteHost", "bridgeVia",
  "pendingMessages", "actorQueued", "actorMessages", "actorRun", "actorPreparing", "actorRemoval", "actorOwnershipToken", "other",
] as const;
export interface PresenceCopyConflict { id: string; fields: string[] }
export interface PresenceDropCount {
  keyClass: PresenceKeyClass; reason: PresenceDropReason; count: number; copies?: PresenceCopyConflict[];
}
const MAX_COPY_SAMPLES = 3;
const copySamples = (raw: unknown, count: number): PresenceCopyConflict[] => {
  const result: PresenceCopyConflict[] = [];
  if (!Array.isArray(raw)) return result;
  for (const value of raw.slice(0, MAX_COPY_SAMPLES)) {
    if (!value || typeof value !== "object" || typeof value.id !== "string" ||
      !/^(?:session:[A-Za-z0-9-]{8,128}|[0-9a-f]{32}|participant:[0-9a-f]{64})$/.test(value.id) || !Array.isArray(value.fields)) continue;
    const fields = [...new Set<string>(value.fields.slice(0, 32).filter((field: unknown) =>
      (PRESENCE_COPY_FIELDS as readonly unknown[]).includes(field)))].slice(0, 8);
    if (!fields.length || result.some(copy => copy.id === value.id)) continue;
    result.push({ id: value.id, fields });
    if (result.length >= count) break;
  }
  return result;
};

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
      this.add(value.keyClass, value.reason, count, value.copies);
      remaining -= count;
      if (remaining === 0) break;
    }
    // Old peers and adapters may only supply a count. Do not invent a more specific cause.
    this.add("presence", "unattributed", remaining);
  }

  add(keyClass: PresenceKeyClass, reason: PresenceDropReason, count = 1, rawCopies: unknown = []): void {
    if (count <= 0) return;
    const key = `${reason}:${keyClass}`;
    const previous = this.#counts.get(key);
    const copies = reason === "conflicting-copy" && keyClass === "topology/participants"
      ? copySamples([...(previous?.copies ?? []), ...copySamples(rawCopies, count)], (previous?.count ?? 0) + count) : [];
    this.#counts.set(key, { keyClass, reason, count: Math.min(Number.MAX_SAFE_INTEGER, (previous?.count ?? 0) + count),
      ...(copies.length ? { copies } : {}) });
  }

  entries(): PresenceDropCount[] {
    return [...this.#counts.values()].sort((a, b) =>
      `${a.reason}:${a.keyClass}`.localeCompare(`${b.reason}:${b.keyClass}`));
  }

  summary(): string {
    const entries = this.entries();
    return entries.slice(0, 8).map(({ keyClass, reason, count, copies }) => `${reason}:${keyClass}=${count}` +
      (copies?.map(({ id, fields }) => ` copy(id=${JSON.stringify(id)}; fields=${fields.join(",")})`).join("") ?? "")).join(" ") +
      (entries.length > 8 ? ` +${entries.length - 8} more` : "");
  }
}
