import type { SessionEntry } from "@earendil-works/pi-coding-agent";
import { fabricCompactionVersion } from "./hook.js";

// Who produced the compaction that is in effect on the active branch.
// Derived from the committed entry itself, never from in-memory state, so it
// survives reload and restart: Fabric's summarizer tags `details` with
// `compactor: "fabric"`, Pi marks every extension-produced entry `fromHook`,
// and anything else came from Pi's own summarizer.
export type CompactionOwner = "fabric" | "pi" | "external" | "none";

export const COMPACTION_OWNERSHIP_DOC = "docs/compaction.md#pi-vcc-precedence";

interface CompactionEntryView {
  details?: unknown;
  fromHook?: boolean;
}

export const compactionOwnerOf = (entry: CompactionEntryView | undefined): CompactionOwner => {
  if (!entry) return "none";
  if (fabricCompactionVersion(entry.details) !== undefined) return "fabric";
  return entry.fromHook === true ? "external" : "pi";
};

export const observedCompactionOwner = (entries: readonly SessionEntry[]): CompactionOwner => {
  for (let index = entries.length - 1; index >= 0; index--) {
    const entry = entries[index]!;
    if (entry.type === "compaction") return compactionOwnerOf(entry);
  }
  return "none";
};

export const ownerFromContext = (
  context: { sessionManager?: { getBranch?: () => SessionEntry[] } } | undefined,
): CompactionOwner => {
  try {
    const branch = context?.sessionManager?.getBranch?.();
    return Array.isArray(branch) ? observedCompactionOwner(branch) : "none";
  } catch {
    return "none";
  }
};

export const ownershipWarning = (owner: Exclude<CompactionOwner, "fabric" | "none">): string =>
  `Fabric compaction is enabled, but the committed compaction came from ${
    owner === "external" ? "another extension" : "Pi's own summarizer"
  }. Pi keeps the last non-cancelling session_before_compact result in extension load order, so an extension loaded after Fabric wins. Fabric will not fight for ownership; load Fabric after other compaction extensions or set compaction.engine to "pi". See ${COMPACTION_OWNERSHIP_DOC}.`;

// One warning per session: a lost ownership race repeats on every compaction,
// and the explanation does not change between them.
export class CompactionOwnerObserver {
  #warnedSession: string | undefined;
  #deliberateYield = false;

  // The hook yielded on purpose (explicit `__pi_vcc__` sentinel or a pi-vcc
  // override with nothing for Fabric to compact): the next foreign result is
  // expected, not a load-order surprise.
  noteDeliberateYield(): void {
    this.#deliberateYield = true;
  }

  /** Returns the warning text the first time a foreign owner wins in a session. */
  observe(
    sessionId: string,
    entry: CompactionEntryView,
    fabricEngineEnabled: boolean,
  ): { owner: CompactionOwner; warning?: string } {
    const owner = compactionOwnerOf(entry);
    const deliberate = this.#deliberateYield;
    this.#deliberateYield = false;
    if (owner === "fabric" || owner === "none" || !fabricEngineEnabled || deliberate) return { owner };
    if (this.#warnedSession === sessionId) return { owner };
    this.#warnedSession = sessionId;
    return { owner, warning: ownershipWarning(owner) };
  }
}
