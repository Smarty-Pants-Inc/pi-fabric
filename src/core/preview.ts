import { diagnostics } from "./diagnostics.js";

// ponytail: V8 slices retain their parent strings; copy the bounded preview.
export const detachedPreview = (text: string, max: number): string =>
  Buffer.from(text.slice(0, Math.max(0, max)), "utf16le").toString("utf16le");

/** SessionManager point-lookup interface shared by the active-branch readers. */
export interface SessionEntryCursorManager {
  getLeafId?: () => string | null;
  getLeafEntry?: () => unknown;
  getEntry?: (id: string) => unknown;
  getEntries?: () => readonly unknown[];
  getSessionId?: () => string;
}

const sessionEntryFields = (value: unknown): { id?: unknown; parentId?: unknown } | undefined =>
  value !== null && typeof value === "object" ? value as { id?: unknown; parentId?: unknown } : undefined;
const sessionEntryId = (value: unknown): string | undefined => {
  const id = sessionEntryFields(value)?.id;
  return typeof id === "string" && id ? id : undefined;
};
const sessionParentId = (value: unknown): string | null | undefined => {
  const id = sessionEntryFields(value)?.parentId;
  return id === null ? null : typeof id === "string" && id ? id : undefined;
};

let sessionBranchFallbackCount = 0;
const diagnosedFallbackManagers = new WeakSet<object>();

/** Diagnostic count of anomalous active-branch reads recovered through a full session snapshot. */
export const getSessionBranchFallbackCount = (): number => sessionBranchFallbackCount;

const fullActiveBranchRead = (
  manager: SessionEntryCursorManager,
  leaf: unknown,
  maxEntries: number | undefined,
  reason: string,
): unknown[] => {
  sessionBranchFallbackCount += 1;
  if (!diagnosedFallbackManagers.has(manager)) {
    diagnosedFallbackManagers.add(manager);
    try { diagnostics.warn("src/core/preview.ts", `[pi-fabric] active session ancestry required a full-read fallback #${sessionBranchFallbackCount}: ${reason}`); } catch {}
  }
  if (typeof manager.getEntries !== "function") {
    throw new Error("Active session ancestry is incomplete and no full session read is available");
  }
  const entries = manager.getEntries();
  if (maxEntries === 0) return [];
  const byId = new Map<string, unknown>();
  for (const entry of entries) {
    const id = sessionEntryId(entry);
    if (!id || byId.has(id)) throw new Error("Full session read has missing or duplicate entry ids");
    byId.set(id, entry);
  }
  const leafId = sessionEntryId(leaf) ?? manager.getLeafId?.() ?? null;
  if (leafId === null) {
    if (entries.length === 0) return [];
    throw new Error("Full session read cannot identify the active leaf");
  }
  let current = byId.get(leafId);
  if (!current) throw new Error("Full session read is missing the active leaf");
  const reverse: unknown[] = [];
  const visited = new Set<string>();
  while (current) {
    const id = sessionEntryId(current);
    if (!id || visited.has(id)) throw new Error("Full session read contains cyclic or invalid active ancestry");
    visited.add(id);
    reverse.push(current);
    if (maxEntries !== undefined && reverse.length >= maxEntries) break;
    const parent = sessionParentId(current);
    if (parent === null) break;
    if (parent === undefined) throw new Error("Full session read contains incomplete active ancestry");
    current = byId.get(parent);
    if (!current) throw new Error("Full session read is missing an active ancestor");
  }
  return reverse.reverse();
};

/**
 * The point-lookup walk established by ProxyContractLedger.restoreFromSessionManager (PR #1385),
 * exposed for other branch-local readers. Returns active-branch entries in chronological order.
 */
export const walkActiveSessionBranch = (manager: SessionEntryCursorManager, maxEntries?: number): unknown[] => {
  if (typeof manager.getLeafEntry !== "function" || typeof manager.getEntry !== "function") {
    const entries = manager.getEntries?.() ?? [];
    return maxEntries === undefined ? [...entries] : maxEntries === 0 ? [] : entries.slice(-maxEntries);
  }
  if (maxEntries === 0) return [];
  const leaf = manager.getLeafEntry();
  if (!leaf) {
    return manager.getLeafId?.()
      ? fullActiveBranchRead(manager, leaf, maxEntries, "active leaf lookup returned no entry")
      : [];
  }
  const reverse: unknown[] = [];
  const visited = new Set<string>();
  let current: unknown = leaf;
  while (current) {
    const id = sessionEntryId(current);
    if (!id) return fullActiveBranchRead(manager, leaf, maxEntries, "active entry has no id");
    if (visited.has(id)) return fullActiveBranchRead(manager, leaf, maxEntries, "cyclic active ancestry");
    visited.add(id);
    reverse.push(current);
    if (maxEntries !== undefined && reverse.length >= maxEntries) return reverse.reverse();
    const parent = sessionParentId(current);
    if (parent === null) return reverse.reverse();
    if (parent === undefined) return fullActiveBranchRead(manager, leaf, maxEntries, "active ancestry has no parent id");
    try {
      current = manager.getEntry(parent);
    } catch {
      return fullActiveBranchRead(manager, leaf, maxEntries, "active ancestor lookup failed");
    }
    if (!current) return fullActiveBranchRead(manager, leaf, maxEntries, "active ancestor lookup was incomplete");
  }
  return reverse.reverse();
};

export interface SessionEntryRead {
  /** Chronological entries read for this update; full path only when reset is true. */
  entries: readonly unknown[];
  /** True when the cursor had to rederive the branch (startup, session or branch change). */
  reset: boolean;
  leafId: string | null;
}

/** Incremental active-branch cursor; no history-size cap unless requested by existing semantics. */
export class SessionEntryCursor {
  #initialized = false;
  #sessionId: string | undefined;
  #leafId: string | null = null;
  #pathIds: string[] = [];
  #known = new Set<string>();
  #maxEntries: number | undefined;

  reset(manager: SessionEntryCursorManager, maxEntries?: number): SessionEntryRead {
    this.#initialized = false;
    this.#sessionId = manager.getSessionId?.();
    this.#leafId = null;
    this.#pathIds = [];
    this.#known.clear();
    this.#maxEntries = maxEntries;
    const entries = walkActiveSessionBranch(manager, maxEntries);
    this.#setPath(entries);
    this.#initialized = true;
    return { entries, reset: true, leafId: this.#leafId };
  }

  read(manager: SessionEntryCursorManager): SessionEntryRead {
    if (!this.#initialized || this.#sessionId !== manager.getSessionId?.()) return this.reset(manager, this.#maxEntries);
    if (typeof manager.getLeafEntry !== "function" || typeof manager.getEntry !== "function") return this.reset(manager, this.#maxEntries);
    const leaf = manager.getLeafEntry();
    const leafId = sessionEntryId(leaf) ?? null;
    const declaredLeafId = manager.getLeafId?.();
    if (declaredLeafId !== undefined && declaredLeafId !== leafId) return this.reset(manager, this.#maxEntries);
    if (leafId === this.#leafId) return { entries: [], reset: false, leafId: this.#leafId };
    const reverse: unknown[] = [];
    const visited = new Set<string>();
    let current = leaf;
    while (current) {
      const id = sessionEntryId(current);
      if (!id || visited.has(id)) return this.reset(manager, this.#maxEntries);
      visited.add(id);
      if (id === this.#leafId && this.#leafId !== null) {
        const entries = reverse.reverse(); this.#appendPath(entries); this.#leafId = leafId;
        return { entries, reset: false, leafId: this.#leafId };
      }
      if (this.#known.has(id)) return this.reset(manager, this.#maxEntries);
      reverse.push(current);
      const parent = sessionParentId(current);
      if (parent === null || parent === undefined) return this.reset(manager, this.#maxEntries);
      if (parent === this.#leafId && this.#leafId !== null) {
        const entries = reverse.reverse(); this.#appendPath(entries); this.#leafId = leafId;
        return { entries, reset: false, leafId: this.#leafId };
      }
      current = manager.getEntry(parent);
    }
    return this.reset(manager, this.#maxEntries);
  }

  #setPath(entries: readonly unknown[]): void {
    const ids = entries.flatMap((entry) => { const id = sessionEntryId(entry); return id ? [id] : []; });
    this.#leafId = ids.at(-1) ?? null;
    if (this.#maxEntries === undefined) { this.#pathIds = []; this.#known = new Set(ids); return; }
    this.#pathIds = this.#maxEntries === 0 ? [] : ids.slice(-this.#maxEntries);
    this.#known = new Set(this.#pathIds);
  }

  #appendPath(entries: readonly unknown[]): void {
    for (const entry of entries) {
      const id = sessionEntryId(entry);
      if (!id) continue;
      if (this.#maxEntries !== undefined && this.#maxEntries > 0) this.#pathIds.push(id);
      if (this.#maxEntries !== 0) this.#known.add(id);
      this.#leafId = id;
    }
    this.#trimPath();
  }

  #trimPath(): void {
    if (this.#maxEntries === undefined || this.#pathIds.length <= this.#maxEntries) return;
    this.#pathIds = this.#maxEntries === 0 ? [] : this.#pathIds.slice(-this.#maxEntries);
    this.#known = new Set(this.#pathIds);
  }
}

/** Bounded point walk for readers whose existing semantics inspect only a tail window. */
export const recentSessionEntries = (manager: SessionEntryCursorManager, limit: number): readonly unknown[] =>
  new SessionEntryCursor().reset(manager, Math.max(0, limit)).entries;
