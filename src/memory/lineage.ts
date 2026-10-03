import crypto from "node:crypto";
import { readSessionDerived } from "./session-file-cache.js";

export type MemoryBranches = "active" | "all";

export interface LiveSessionBranch {
  entries: readonly unknown[];
  leafId: string | null;
  /** Trusted host identity for an immutable ID path; absent uses array identity. */
  revision?: string;
}

export interface SessionLineage {
  branches: MemoryBranches;
  leafId: string | null;
  entryOrdinals: ReadonlySet<number> | null;
  fingerprint: string;
  coverageReasons: string[];
}

interface PersistedNode {
  id: string;
  parentId: string | null;
  ordinal: number;
}

interface PersistedRecord {
  type?: unknown;
  id?: unknown;
  parentId?: unknown;
}

const asPersistedRecord = (value: unknown): PersistedRecord | null =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as PersistedRecord
    : null;

const isHeaderRecord = (record: PersistedRecord): boolean => record.type === "session";

const hasParentLink = (
  record: PersistedRecord,
): record is PersistedRecord & { id: string; parentId: string | null } =>
  typeof record.id === "string" && (record.parentId === null || typeof record.parentId === "string");

const persistedNodesFromRecords = (records: readonly unknown[]): PersistedNode[] => {
  const nodes: PersistedNode[] = [];
  let ordinal = 0;
  for (const value of records) {
    const record = asPersistedRecord(value);
    if (!record || isHeaderRecord(record)) continue;
    if (hasParentLink(record)) nodes.push({ id: record.id, parentId: record.parentId, ordinal });
    ordinal += 1;
  }
  return nodes;
};

const fingerprint = (branches: MemoryBranches, leafId: string | null, ids: string[]): string =>
  crypto.createHash("sha256").update(JSON.stringify({ branches, leafId, ids })).digest("hex");

interface PersistedNodes {
  nodes: PersistedNode[];
  ordinal: number;
  lineage?: { length: number; liveLength: number | undefined; liveEntries: WeakRef<readonly unknown[]> | undefined; revision: string | undefined; leafId: string | null | undefined; value: SessionLineage };
}
const readPersistedNodes = (sessionFile: string): PersistedNodes | undefined =>
  readSessionDerived(sessionFile, "lineage", () => ({ nodes: [], ordinal: 0 } as PersistedNodes), (state, value) => {
    const record = asPersistedRecord(value);
    if (!record || isHeaderRecord(record)) return;
    if (hasParentLink(record)) state.nodes.push({
      id: Buffer.from(record.id, "utf16le").toString("utf16le"),
      parentId: record.parentId === null ? null : Buffer.from(record.parentId, "utf16le").toString("utf16le"),
      ordinal: state.ordinal,
    });
    state.ordinal += 1;
  });

let cachedAllLineage: SessionLineage | undefined;
const allLineage = (): SessionLineage => cachedAllLineage ??= {
  branches: "all",
  leafId: null,
  entryOrdinals: null,
  fingerprint: fingerprint("all", null, []),
  coverageReasons: [],
};

/**
 * Reconstruct Pi 0.80.6's persisted leaf semantics without treating append
 * order as a transcript: the final persisted entry is the leaf, duplicate IDs
 * resolve to their last record in the ID map, and parent links are walked to a
 * root. Cycles are stopped defensively and reported as incomplete coverage.
 */
export const reconstructSessionLineage = (
  sessionFile: string,
  branches: MemoryBranches,
  liveBranch?: LiveSessionBranch,
): SessionLineage =>
  branches === "all" ? allLineage() : cachedActiveLineage(sessionFile, liveBranch);

const cachedActiveLineage = (file: string, liveBranch?: LiveSessionBranch): SessionLineage => {
  const state = readPersistedNodes(file);
  if (!state) return buildActiveLineage([], liveBranch);
  const cached = state.lineage;
  // Never retain the live transcript. Native snapshots carry a trusted revision;
  // custom branches without one preserve their exact array/ID-list semantics.
  const sameLive = liveBranch?.revision !== undefined
    ? cached?.revision === liveBranch.revision
    : cached?.liveEntries?.deref() === liveBranch?.entries;
  if (cached && sameLive && cached.length === state.nodes.length && cached.liveLength === liveBranch?.entries.length && cached.leafId === liveBranch?.leafId) return cached.value;
  const value = buildActiveLineage(state.nodes, liveBranch);
  state.lineage = { length: state.nodes.length, liveLength: liveBranch?.entries.length, liveEntries: liveBranch ? new WeakRef(liveBranch.entries) : undefined, revision: liveBranch?.revision, leafId: liveBranch?.leafId, value };
  return value;
};

const buildActiveLineage = (nodes: PersistedNode[], liveBranch?: LiveSessionBranch, selectedLeafId?: string | null): SessionLineage => {
  const byId = new Map(nodes.map((node) => [node.id, node]));
  const liveIds = liveBranch?.entries.flatMap((entry) => {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) return [];
    const id = (entry as Record<string, unknown>).id;
    return typeof id === "string" ? [id] : [];
  });
  const leafId = liveBranch ? liveBranch.leafId : selectedLeafId !== undefined ? selectedLeafId : (nodes[nodes.length - 1]?.id ?? null);
  const path: PersistedNode[] = [];
  const reasons = new Set<string>();

  if (liveIds) {
    for (const id of liveIds) {
      const node = byId.get(id);
      if (node) path.push(node);
    }
  } else {
    const seen = new Set<string>();
    let current = leafId ? byId.get(leafId) : undefined;
    while (current) {
      if (seen.has(current.id)) {
        reasons.add("invalid_parent_graph");
        break;
      }
      seen.add(current.id);
      path.push(current);
      current = current.parentId ? byId.get(current.parentId) : undefined;
    }
    path.reverse();
  }

  const ids = liveIds ?? path.map((node) => node.id);
  return {
    branches: "active",
    leafId,
    entryOrdinals: new Set(path.map((node) => node.ordinal)),
    fingerprint: fingerprint("active", leafId, ids),
    coverageReasons: [...reasons].sort(),
  };
};

/**
 * Records variant of {@link reconstructSessionLineage}: identical Pi 0.80.6
 * leaf semantics applied to already-parsed session records instead of a file.
 */
export const reconstructRecordsLineage = (
  records: readonly unknown[],
  branches: MemoryBranches,
  liveBranch?: LiveSessionBranch,
  selectedLeafId?: string | null,
): SessionLineage =>
  branches === "all" ? allLineage() : buildActiveLineage(persistedNodesFromRecords(records), liveBranch, selectedLeafId);
