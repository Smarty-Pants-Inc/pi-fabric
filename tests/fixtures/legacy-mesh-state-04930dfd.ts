/**
 * VENDORED COPY of the deployed release's state.json decode rule, for the cutover moved-marker tests
 * (pi-fabric#627 review round 3). Source: pi-fabric 04930dfd8b13043e96774384bef4796d2a526eae,
 * src/mesh/store.ts lines 192-195 (errorCode) and 248-336 (isMeshStateFile, recoverConcatenatedState,
 * emptyState, readState), copied verbatim; only the MeshStateFile type is reduced to the fields used.
 * Do NOT update this file to follow src/: it stands for an OLD binary that ignores backend fields.
 *
 * The deployed write rule (store.ts 1749-1756 #readStateForWrite, used by put 1951, delete 2020,
 * writeBatch 2065 and the tombstone compaction inside them) reads with recoverDamage=false and
 * commits only a decoded envelope; `legacyPut` below reproduces that under the mesh .lock.
 */
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { readFileRetrying } from "../../src/core/atomic-write.js";
import { MeshLock } from "../../src/mesh/mesh-lock.js";

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

// ---- verbatim from 04930dfd src/mesh/store.ts ----
const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string"
    ? error.code
    : undefined;

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
  try {
    const parsed: unknown = JSON.parse(serialized);
    if (isMeshStateFile(parsed)) {
      observed?.(serialized);
      return parsed;
    }
    throw new Error("invalid state format");
  } catch (error) {
    // Failed parsing must not silently erase the allocation clock. Read-only
    // startup can tolerate damage, but mutations require a repaired snapshot.
    const recovered = recoverConcatenatedState(serialized);
    if (recovered) return recovered;
    if (!recoverDamage) throw new Error("Failed to read Fabric mesh state: invalid state format");
    // Preserve the original bytes at this path as a barrier to clock reset.
    return emptyState();
  }
};
// ---- end of the verbatim copy ----

export { readState as legacyReadState };

/**
 * One legacy put: the deployed rule (strict read under .lock, then an atomic replace). Throws
 * exactly where the deployed write path throws, before anything is written.
 */
export const legacyPut = async (root: string, key: string, value: unknown, onAcquired?: () => void): Promise<void> => {
  const statePath = path.join(root, "state.json");
  const lock = new MeshLock(root, { lockTimeoutMs: 30_000 }, () => undefined);
  await lock.withLock(() => {
    onAcquired?.();
    const state = readState(statePath, 32 * 1024 * 1024, false);
    const version = (state.highWater ?? 0) + 1;
    state.entries[key] = { key, value, version, updatedAt: Date.now(), updatedBy: { id: "legacy", name: "legacy", kind: "agent" } };
    state.versions = { ...state.versions, [key]: version };
    state.highWater = version;
    state.format = 1;
    state.revisionFormat = 2;
    const temporary = `${statePath}.${process.pid}.${randomUUID()}.legacy.tmp`;
    fs.writeFileSync(temporary, JSON.stringify({ ...state, readGeneration: randomUUID() }));
    fs.renameSync(temporary, statePath);
  }, 30_000, "other");
};
