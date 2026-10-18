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

class SessionEntryBranchRefusalError extends Error {}

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
    throw new SessionEntryBranchRefusalError("Active session ancestry is incomplete and no full session read is available");
  }
  const entries = manager.getEntries();
  if (maxEntries === 0) return [];
  const byId = new Map<string, unknown>();
  for (const entry of entries) {
    const id = sessionEntryId(entry);
    if (!id || byId.has(id)) throw new SessionEntryBranchRefusalError("Full session read has missing or duplicate entry ids");
    byId.set(id, entry);
  }
  const leafId = sessionEntryId(leaf) ?? manager.getLeafId?.() ?? null;
  if (leafId === null) {
    if (entries.length === 0) return [];
    throw new SessionEntryBranchRefusalError("Full session read cannot identify the active leaf");
  }
  let current = byId.get(leafId);
  if (!current) throw new SessionEntryBranchRefusalError("Full session read is missing the active leaf");
  const reverse: unknown[] = [];
  const visited = new Set<string>();
  while (current) {
    const id = sessionEntryId(current);
    if (!id || visited.has(id)) throw new SessionEntryBranchRefusalError("Full session read contains cyclic or invalid active ancestry");
    visited.add(id);
    reverse.push(current);
    if (maxEntries !== undefined && reverse.length >= maxEntries) break;
    const parent = sessionParentId(current);
    if (parent === null) break;
    if (parent === undefined) throw new SessionEntryBranchRefusalError("Full session read contains incomplete active ancestry");
    current = byId.get(parent);
    if (!current) throw new SessionEntryBranchRefusalError("Full session read is missing an active ancestor");
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

interface SessionEntryCursorFailure {
  sessionId: string | undefined;
  leafId: string | null;
  maxEntries: number | undefined;
  error: unknown;
}

const CURSOR_KNOWN_ID_LIMIT = 500;
const cursorKnownIdLimit = (maxEntries: number | undefined): number =>
  Math.max(0, Math.min(maxEntries ?? CURSOR_KNOWN_ID_LIMIT, CURSOR_KNOWN_ID_LIMIT));
const currentLeafId = (manager: SessionEntryCursorManager): string | null => {
  try {
    const declared = manager.getLeafId?.();
    return declared !== undefined ? declared : sessionEntryId(manager.getLeafEntry?.()) ?? null;
  } catch {
    return null;
  }
};

/** Incremental active-branch cursor; returned history is uncapped, cursor metadata keeps 500 ids. */
export class SessionEntryCursor {
  #initialized = false;
  #sessionId: string | undefined;
  #leafId: string | null = null;
  #pathIds: string[] = [];
  #known = new Set<string>();
  #maxEntries: number | undefined;
  #failure: SessionEntryCursorFailure | undefined;

  reset(manager: SessionEntryCursorManager, maxEntries?: number): SessionEntryRead {
    const sessionId = manager.getSessionId?.();
    const leafId = currentLeafId(manager);
    if (this.#failure) {
      const failure = this.#failure;
      if (failure.sessionId === sessionId && failure.leafId === leafId && failure.maxEntries === maxEntries) {
        throw failure.error;
      }
      this.#failure = undefined;
    }
    this.#initialized = false;
    this.#sessionId = sessionId;
    this.#leafId = null;
    this.#pathIds = [];
    this.#known.clear();
    this.#maxEntries = maxEntries;
    try {
      const entries = walkActiveSessionBranch(manager, maxEntries);
      this.#setPath(entries);
      this.#initialized = true;
      return { entries, reset: true, leafId: this.#leafId };
    } catch (error) {
      // Cache an explicit ancestry refusal before propagating it. Transient reader exceptions must
      // remain retryable on the next read at the same session/tip.
      if (error instanceof SessionEntryBranchRefusalError) {
        this.#failure = { sessionId, leafId, maxEntries, error };
      }
      throw error;
    }
  }

  read(manager: SessionEntryCursorManager): SessionEntryRead {
    const sessionId = manager.getSessionId?.();
    if (this.#failure) {
      const failure = this.#failure;
      if (failure.sessionId === sessionId && failure.leafId === currentLeafId(manager) &&
          failure.maxEntries === this.#maxEntries) throw failure.error;
      this.#failure = undefined;
    }
    if (!this.#initialized || this.#sessionId !== sessionId) return this.reset(manager, this.#maxEntries);
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
      try {
        current = manager.getEntry(parent);
      } catch {
        return this.reset(manager, this.#maxEntries);
      }
    }
    return this.reset(manager, this.#maxEntries);
  }

  #setPath(entries: readonly unknown[]): void {
    const ids = entries.flatMap((entry) => { const id = sessionEntryId(entry); return id ? [id] : []; });
    this.#leafId = ids.at(-1) ?? null;
    const limit = cursorKnownIdLimit(this.#maxEntries);
    this.#pathIds = limit === 0 ? [] : ids.slice(-limit);
    this.#known = new Set(this.#pathIds);
  }

  #appendPath(entries: readonly unknown[]): void {
    const limit = cursorKnownIdLimit(this.#maxEntries);
    for (const entry of entries) {
      const id = sessionEntryId(entry);
      if (!id) continue;
      if (limit > 0) this.#pathIds.push(id);
      if (limit > 0) this.#known.add(id);
      this.#leafId = id;
    }
    this.#trimPath();
  }

  #trimPath(): void {
    const limit = cursorKnownIdLimit(this.#maxEntries);
    if (limit === 0) { this.#pathIds = []; this.#known.clear(); return; }
    if (this.#pathIds.length <= limit) return;
    this.#pathIds = this.#pathIds.slice(-limit);
    this.#known = new Set(this.#pathIds);
  }
}

/** Bounded point walk for readers whose existing semantics inspect only a tail window. */
export const recentSessionEntries = (manager: SessionEntryCursorManager, limit: number): readonly unknown[] =>
  new SessionEntryCursor().reset(manager, Math.max(0, limit)).entries;
