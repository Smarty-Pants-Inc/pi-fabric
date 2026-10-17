/**
 * Incremental active-branch traversal using Pi's point-lookup SessionManager API.
 *
 * Pi's SessionManager docs (`docs/sdk.md#sessionmanager-api`) expose getLeafEntry/getEntry;
 * branch-sensitive state is restored from the active branch at session_start
 * (`docs/extensions.md#persist-state`). The session format links each entry through
 * parentId (`docs/session-format.md#tree-structure`). Keep only detached ids here,
 * never entry bodies; consumers project the rows they need.
 */
export interface SessionEntryCursorManager {
  getLeafEntry?: () => unknown;
  getEntry?: (id: string) => unknown;
  getSessionId?: () => string;
}

export interface SessionEntryRead {
  /** Chronological entries read for this update; full path only when reset is true. */
  entries: readonly unknown[];
  /** True when the cursor had to rederive the branch (startup, session or branch change). */
  reset: boolean;
  leafId: string | null;
}

type LinkedEntry = { id?: unknown; parentId?: unknown };
const fields = (value: unknown): LinkedEntry | undefined =>
  value !== null && typeof value === "object" ? value as LinkedEntry : undefined;
const entryId = (value: unknown): string | undefined => {
  const id = fields(value)?.id;
  return typeof id === "string" && id ? id : undefined;
};
const parentId = (value: unknown): string | null | undefined => {
  const parent = fields(value)?.parentId;
  return parent === null ? null : typeof parent === "string" && parent ? parent : undefined;
};

/**
 * Reads the active branch once, then only point-walks newly appended entries. A branch
 * navigation or session replacement re-derives the branch; no history-size cap is used.
 * `maxEntries` is for consumers whose semantics already have a documented lookback window.
 */
export class SessionEntryCursor {
  #initialized = false;
  #sessionId: string | undefined;
  #leafId: string | null = null;
  #pathIds: string[] = [];
  #known = new Set<string>();
  #maxEntries: number | undefined;

  reset(manager: SessionEntryCursorManager, maxEntries?: number): SessionEntryRead {
    this.#maxEntries = maxEntries;
    this.#sessionId = manager.getSessionId?.();
    const entries = this.#walk(manager, maxEntries);
    this.#setPath(entries);
    this.#initialized = true;
    return { entries, reset: true, leafId: this.#leafId };
  }

  read(manager: SessionEntryCursorManager): SessionEntryRead {
    if (!this.#initialized || this.#sessionId !== manager.getSessionId?.()) return this.reset(manager, this.#maxEntries);
    if (typeof manager.getLeafEntry !== "function" || typeof manager.getEntry !== "function") {
      return this.reset(manager, this.#maxEntries);
    }
    const leaf = manager.getLeafEntry();
    const leafId = entryId(leaf) ?? null;
    if (leafId === this.#leafId) return { entries: [], reset: false, leafId: this.#leafId };
    const reverse: unknown[] = [];
    const visited = new Set<string>();
    let current = leaf;
    while (current) {
      const id = entryId(current);
      if (!id || !visited.add(id)) return this.reset(manager, this.#maxEntries);
      if (id === this.#leafId && this.#leafId !== null) {
        const entries = reverse.reverse();
        this.#appendPath(entries);
        this.#leafId = leafId;
        return { entries, reset: false, leafId: this.#leafId };
      }
      // A branch now ends at an earlier known ancestor, not at the cursor. Rebuild so
      // consumers can discard receipts from the abandoned suffix without a lossy cap.
      if (this.#known.has(id)) return this.reset(manager, this.#maxEntries);
      reverse.push(current);
      const parent = parentId(current);
      if (parent === null || parent === undefined) return this.reset(manager, this.#maxEntries);
      if (parent === this.#leafId && this.#leafId !== null) {
        const entries = reverse.reverse();
        this.#appendPath(entries);
        this.#leafId = leafId;
        return { entries, reset: false, leafId: this.#leafId };
      }
      current = manager.getEntry(parent);
    }
    return this.reset(manager, this.#maxEntries);
  }

  #walk(manager: SessionEntryCursorManager, maxEntries?: number): unknown[] {
    if (typeof manager.getLeafEntry !== "function" || typeof manager.getEntry !== "function") return [];
    const reverse: unknown[] = [];
    const visited = new Set<string>();
    let current = manager.getLeafEntry();
    while (current && (maxEntries === undefined || reverse.length < maxEntries)) {
      const id = entryId(current);
      if (!id || !visited.add(id)) break;
      reverse.push(current);
      const parent = parentId(current);
      if (parent === null || parent === undefined) break;
      current = manager.getEntry(parent);
    }
    return reverse.reverse();
  }

  #setPath(entries: readonly unknown[]): void {
    this.#pathIds = entries.flatMap((entry) => {
      const id = entryId(entry);
      return id ? [id] : [];
    });
    this.#trimPath();
    this.#known = new Set(this.#pathIds);
    this.#leafId = this.#pathIds.at(-1) ?? null;
  }

  #appendPath(entries: readonly unknown[]): void {
    for (const entry of entries) {
      const id = entryId(entry);
      if (id) { this.#pathIds.push(id); this.#known.add(id); }
    }
    this.#leafId = this.#pathIds.at(-1) ?? null;
    this.#trimPath();
  }

  #trimPath(): void {
    if (this.#maxEntries === undefined || this.#pathIds.length <= this.#maxEntries) return;
    this.#pathIds = this.#pathIds.slice(-this.#maxEntries);
    this.#known = new Set(this.#pathIds);
  }
}

/** A bounded point walk for hot paths whose existing semantics inspect only a tail window. */
export const recentSessionEntries = (
  manager: SessionEntryCursorManager,
  limit: number,
): readonly unknown[] => {
  const cursor = new SessionEntryCursor();
  return cursor.reset(manager, Math.max(0, limit)).entries;
};
