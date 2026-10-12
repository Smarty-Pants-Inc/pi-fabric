/**
 * VENDORED COPY of an installed release's state.json read rule, for the NATS moved-marker fence (pi-fabric#917
 * security P1). Source: pi-fabric 8d0af4b07 (main, 2026-10-11): src/mesh/state-file.ts lines 135-229
 * (isMeshStateFile, recoverConcatenatedState, emptyState, readState) and src/mesh/backend-fence.ts lines 108-113
 * (isMeshStateMovedMarker), copied verbatim. Only the imports, the reduced types and `meshStateMovedError` (same
 * message, a plain Error) are local. Do NOT update this file to follow src/: it stands for an OLD binary.
 */
import fs from "node:fs";
import { readFileRetrying } from "../../src/core/atomic-write.js";

interface MeshStateEntry { key: string; value: unknown; version: number; updatedAt: number; updatedBy: unknown }
interface MeshStateFile {
  format: 1 | 2;
  revisionFormat?: 2;
  entries: Record<string, MeshStateEntry>;
  versions?: Record<string, number>;
  tombstoneOrder?: string[];
  highWater?: number;
  readGeneration?: string;
}
interface MeshStateMovedMarker { format: "sqlite"; movedTo: "state.db"; backend: "sqlite"; epoch: number; at: string }
const STATE_DB = "state.db";
const errorCode = (error: unknown): string | undefined => (error as NodeJS.ErrnoException | undefined)?.code;
const meshStateMovedError = (file: string, marker: MeshStateMovedMarker): Error =>
  new Error(`Fabric mesh state.json was moved to state.db (backend=sqlite at epoch ${marker.epoch}, ${marker.at}): `
    + `${file} is not state; this release must run in sqlite mode`);

// ---- verbatim from 8d0af4b0 src/mesh/backend-fence.ts ----
const isMeshStateMovedMarker = (value: unknown): value is MeshStateMovedMarker => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const marker = value as Record<string, unknown>;
  return marker.format === "sqlite" && marker.movedTo === STATE_DB && marker.backend === "sqlite"
    && typeof marker.epoch === "number" && Number.isSafeInteger(marker.epoch) && marker.epoch >= 1;
};

// ---- verbatim from 8d0af4b0 src/mesh/state-file.ts ----
const isMeshStateFile = (value: unknown): value is MeshStateFile => {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    ![1, 2].includes((value as { format?: unknown }).format as number)
  ) {
    return false;
  }
  const entries = (value as { entries?: unknown }).entries;
  return typeof entries === "object" && entries !== null && !Array.isArray(entries);
};

const recoverConcatenatedState = (serialized: string): MeshStateFile | undefined => {
  const snapshots: MeshStateFile[] = [];
  let documents = 0;
  let start = -1;
  let depth = 0;
  let inString = false;
  let escaped = false;

  for (let index = 0; index < serialized.length; index += 1) {
    const character = serialized[index]!;
    if (start < 0) {
      if (/\s/.test(character)) continue;
      if (character !== "{") return undefined;
      start = index;
      depth = 1;
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (character === "\\") escaped = true;
      else if (character === '"') inString = false;
      continue;
    }
    if (character === '"') inString = true;
    else if (character === "{") depth += 1;
    else if (character === "}") {
      depth -= 1;
      if (depth !== 0) continue;
      try {
        const parsed: unknown = JSON.parse(serialized.slice(start, index + 1));
        documents += 1;
        if (isMeshStateFile(parsed)) snapshots.push(parsed);
      } catch {
        return undefined;
      }
      start = -1;
    }
  }

  return start < 0 && documents > 1 ? snapshots.at(-1) : undefined;
};

const emptyState = (): MeshStateFile => ({ format: 1, revisionFormat: 2, entries: {}, highWater: 0 });

const readState = (
  filePath: string, maxBytes: number, recoverDamage = true, observed?: (serialized: string) => void,
): MeshStateFile => {
  let serialized: string;
  try {
    const stat = fs.statSync(filePath);
    if (stat.size > maxBytes) throw new Error(`state exceeds ${maxBytes} bytes`);
    if (stat.size === 0 && recoverDamage) return emptyState();
    serialized = readFileRetrying(filePath);            // a lock-free read can meet a replace on Windows
  } catch (error) {
    if (errorCode(error) === "ENOENT") return emptyState();
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to read Fabric mesh state: ${message}`);
  }
  if (!serialized.trim() && recoverDamage) return emptyState();
  let moved: MeshStateMovedMarker | undefined;
  try {
    const parsed: unknown = JSON.parse(serialized);
    if (isMeshStateFile(parsed)) {
      observed?.(serialized);
      return parsed;
    }
    if (!isMeshStateMovedMarker(parsed)) throw new Error("invalid state format");
    moved = parsed;
  } catch (error) {
    // Failed parsing must not silently erase the allocation clock. Read-only
    // startup can tolerate damage, but mutations require a repaired snapshot.
    const recovered = recoverConcatenatedState(serialized);
    if (recovered) return recovered;
    if (!recoverDamage) throw new Error("Failed to read Fabric mesh state: invalid state format");
    // Preserve the original bytes at this path as a barrier to clock reset.
    return emptyState();
  }
  // pi-fabric#627 review round 3: cutover's moved marker (backend-fence.ts). Never state, damage or an
  // empty mesh, for strict and tolerant reads alike: this file-mode store fails closed (a write is
  // refused before it stages anything; a reader does not report false absence).
  throw meshStateMovedError(filePath, moved);
};

/** The old binary's read: `recoverDamage` true is its tolerant path (readers), false its strict path (writers). */
export { readState as legacyReadState8d0af4b0 };
