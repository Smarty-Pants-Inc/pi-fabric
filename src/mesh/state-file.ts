import { createCommitStats, type MeshLockClass } from "./commit-stats.js";
import { appendStateJournal, prepareStateJournal, journalBase, journalCursorOf, replayStateJournal, stateReadIdentity, verifyStateJournalEndpoint,
  readStateWitness, sameWitnessTuple, stateWitnessOf, type JournalBase, type JournalCursor, type JournalEndpoint, type StateWitness } from "./read-journal.js";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import childProcess from "node:child_process";
import { readFileRetrying, writeFileAtomic, renameAtomic, MeshLockTimeoutError } from "../core/atomic-write.js";
import { captureStoragePut, captureStorageDelete, storageRevision } from "../verified/storage.js";
import { delay, describeLockHolder, errorCode, lockStats, type MeshLock, type MeshStoreContext } from "./mesh-lock.js";
import { assertFileStateWritable, isMeshStateMovedMarker, meshStateMovedError, readMeshStateMovedMarker, type MeshStateMovedMarker } from "./backend-fence.js";
import type { MeshIdentity } from "./event-log.js";
import type { MeshCommitEffects, MeshStateFileRead, StateBackend, StateBackendBatchInput, StateBackendDiagnostics } from "./state-backend.js";

// Keyed mesh state on state.json (smarty-dev#6477 L0): reads, encoding, prepared and committed
// writes, revisions, tombstones, namespaces, the read signal and write snapshots.

export interface MeshStateEntry {
  key: string;
  value: unknown;
  version: number;
  updatedAt: number;
  updatedBy: MeshIdentity;
}

interface MeshStateFile {
  format: 1 | 2;
  revisionFormat?: 2;
  entries: Record<string, MeshStateEntry>;
  versions?: Record<string, number>;
  tombstoneOrder?: string[];
  /** Persisted allocation clock; never evicted with per-key tombstones. */
  highWater?: number;
  /**
   * smarty-dev#2014: a UUID unique per commit, serialized as the FIRST field so readers observe the
   * committed payload's identity from a bounded header. Older readers ignore unknown fields.
   */
  readGeneration?: string;
  /** Hash-chain head for optional incremental readers, committed with the canonical payload. */
  readJournalHash?: string;
}

export interface MeshReadOptions {
  /** Revalidate canonical commit/physical identity now, bypassing the idle age window. */
  fresh?: boolean;
  /** Opt in only for background display observations; never authority, routing or admission. */
  background?: boolean;
  /**
   * Display-only observation (smarty-dev#4250): may serve a journal-followed state whose terminal
   * endpoint is not yet bound to the canonical payload hash, so idle polls stay incremental.
   * Never for a caller that decides, routes, admits or writes; every other read binds it first.
   */
  displayOnly?: boolean;
  /** Reuse one already-captured canonical state for a multi-namespace scan. */
  snapshot?: object;
}

// Capture the opt-in once at process startup/module load: no timer, key classification,
// counters, extra serialization, filesystem work or per-commit environment lookup when off.
const commitStats = createCommitStats();

// Opt-in commit diagnostics: no values or stacks are collected on the normal path.
// Capture before entering the async lock so the actual writer survives the await boundary.
// Frame 2 is the StateFile writer below its MeshStore facade frame: skip it, keep the same 8 frames.
const commitTraceCaller = (): string[] | undefined => process.env.PI_FABRIC_COMMIT_TRACE
  ? new Error().stack?.split("\n").slice(3, 11).map(line => line.trim()) : undefined;

const KEY_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,255}$/;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
// ponytail: every tombstone is rewritten with the whole shared state on every write, and read
// by every process (smarty-dev#251, dev1 load P0: 4,787 tombstones were 40% of a 2.3 MB file).
// The persistent revision clock makes eviction safe: an evicted key is recreated above every
// earlier revision, and a stale compare-and-swap still conflicts. A key re-claimed with
// ifVersion 0 after eviction needs its id replayed; live claimers use fresh ids, and control
// commands are rejected once past their deadline.
const DEFAULT_MAX_STATE_TOMBSTONES = 1_000;
/**
 * Background-only read-cache age in a Fabric runtime and its resident host. Ordinary reads
 * are exact on change; only explicitly opted-in display observations reuse a recent parse.
 * Idle observers coalesce for 5 s by default (mesh.idleReadCoalesceMs), active observers for
 * 1 s. Fresh protocol decisions always bypass both age windows (smarty-dev#2355/#4383).
 * This is a reader policy only: no on-disk format or writer cadence change (mixed fleets).
 */
export const RUNTIME_MESH_READ_CACHE_MS = 5_000;
/** Floor for runtime background observations, including active turns (smarty-dev#4383). */
export const MIN_BACKGROUND_MESH_READ_CACHE_MS = 1_000;

export const jsonClone = <T>(value: T): T => {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) throw new Error("Mesh values must be JSON-serializable");
  return JSON.parse(serialized) as T;
};

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

// smarty-dev#2014 read signal: state.read-signal.json, rewritten best effort after each commit
// under the lock, binds SHA-256 digests of each namespace (first two complete key segments) to the
// exact state.json stat it describes. A runtime reader whose window expired may keep its older
// parse for a namespace whose digest is unchanged. A missing, damaged, oversized or mismatched
// signal (an older writer, a crash between the two files) only forces the normal re-read.
// ~40 KB on the fleet today; a larger signal is not written (or read), which only forces re-reads.
// Commit identity lives in the CANONICAL file: state.json's first field `readGeneration` is a UUID
// unique per commit, written atomically with the payload. Readers peek its first 64 bytes to
// observe the committed generation (the stat stamp alone can repeat, ABA). The signal is only a
// hint: its `generation` (first field) must equal the canonical header, so a failed, crashed or
// capped signal publication can never hide a commit; it only forces the canonical parse.
// Fresh observations bypass the idle window, but not the physical-generation gate. High-resolution
// inode/mtime/ctime metadata detects cooperating atomic and in-place replacements, including old
// writers copying a UUID. If an adapter cannot supply that identity, fresh reads retain the canonical
// payload fallback (#2355). Journal replay additionally binds both physical endpoints and every
// delta through the chain head committed IN state.json; a self-checksummed sidecar is not authority.
const MAX_SIGNAL_BYTES = 128 * 1024;
const HEADER_BYTES = 64;
const UUID = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";
const SIGNAL_HEADER = new RegExp(`^\\{"generation":"(${UUID})"`);
const STATE_HEADER = new RegExp(`^\\{"readGeneration":"(${UUID})"`);
const GENERATION = new RegExp(`^${UUID}$`);
// The generation named by an open file's bounded header, or undefined (no marker, old format, damaged).
const readHeader = (descriptor: number, header: RegExp): string | undefined => {
  const buffer = Buffer.alloc(HEADER_BYTES);
  const read = fs.readSync(descriptor, buffer, 0, HEADER_BYTES, 0);
  return header.exec(buffer.toString("latin1", 0, read))?.[1];
};
// A parsed payload's own commit generation: the label its cache entry is revalidated against.
const generationOf = (state: MeshStateFile): string | undefined =>
  typeof state.readGeneration === "string" && GENERATION.test(state.readGeneration) ? state.readGeneration : undefined;
const closeQuietly = (descriptor: number | undefined): void => {
  try {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  } catch {
    // Best effort: a close error must not fail the read.
  }
};
const SELECTION_MEMO_PREFIXES = 32;
const keyNamespace = (key: string): string | undefined => {
  const second = key.indexOf("/", key.indexOf("/") + 1);
  return key.indexOf("/") < 0 || second < 0 ? undefined : key.slice(0, second + 1);
};
// Entries in code-unit key order, so writer and reader hash the same sequence.
const digestEntries = (entries: Iterable<MeshStateEntry>): string => {
  const hash = createHash("sha256");
  for (const entry of entries) hash.update(`${JSON.stringify(entry)}\n`);
  return hash.digest("base64");
};
// Encode changed entries once for both the canonical payload and the optional namespace index.
// Preserve JSON.stringify's field/key order and omission rules, including legacy envelope fields.
// Reuse is authorized only by exact fresh canonical text, then the entry's own version after
// the locked transition. UUID/stat/version alone cannot defeat a copied-marker ABA (#2355, #2395).
// Namespace hashes are always recomputed from this commit's bytes, never cached.
interface WriteStateSnapshot {
  state: MeshStateFile;
  reuse: Map<string, EncodedStateEntry> | undefined;
  base: JournalBase;
}
interface PreparedStateCommit {
  stamped: MeshStateFile;
  generation: string;
  encoded: ReturnType<typeof encodeState>;
  journal: ReturnType<typeof prepareStateJournal>;
  namespaces: Record<string, string> | undefined;
  serializedText: string;
  temporary?: string;
}
const WRITE_SNAPSHOT_CHANGED = Symbol("mesh write snapshot changed");

interface EncodedStateEntry {
  version: number | undefined;
  member: Buffer;
  entry: Buffer;
}
const encodeEntry = (key: string, value: MeshStateEntry | undefined): EncodedStateEntry | undefined => {
  const serialized = JSON.stringify(value);
  if (serialized === undefined) return undefined;
  const encodedKey = JSON.stringify(key);
  const member = Buffer.from(`${encodedKey}:${serialized}`, "utf8");
  // Canonical member and hash input share the same UTF-8 bytes.
  return { version: value?.version, member, entry: member.subarray(Buffer.byteLength(encodedKey, "utf8") + 1) };
};
const encodeState = (state: MeshStateFile, reuse?: Map<string, EncodedStateEntry>): {
  serialized: Buffer; entries: Map<string, EncodedStateEntry>;
} => {
  const entries = new Map<string, EncodedStateEntry>();
  const fields: Buffer[] = [Buffer.from("{")];
  const comma = Buffer.from(",");
  for (const [field, value] of Object.entries(state)) {
    if (field === "entries") {
      if (fields.length > 1) fields.push(comma);
      fields.push(Buffer.from('"entries":{'));
      let first = true;
      for (const key of Object.keys(state.entries)) {
        const cached = reuse?.get(key);
        if (cached && cached.version === state.entries[key]?.version) {
          entries.set(key, cached);
          if (!first) fields.push(comma);
          first = false;
          fields.push(cached.member);
          continue;
        }
        const encoded = encodeEntry(key, state.entries[key]);
        if (!encoded) continue;
        entries.set(key, encoded);
        if (!first) fields.push(comma);
        first = false;
        fields.push(encoded.member);
      }
      fields.push(Buffer.from("}"));
    } else {
      const serialized = JSON.stringify(value);
      if (serialized === undefined) continue;
      if (fields.length > 1) fields.push(comma);
      fields.push(Buffer.from(`${JSON.stringify(field)}:${serialized}`, "utf8"));
    }
  }
  fields.push(Buffer.from("}"));
  return { serialized: Buffer.concat(fields), entries };
};
const EMPTY_DIGEST = digestEntries([]);
const sortedKeys = (keys: string[]): string[] => keys.sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));
const stampOf = (stat: fs.Stats): string =>
  `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`;
const statStamp = (filePath: string): string | undefined => {
  try {
    return stampOf(fs.statSync(filePath));
  } catch {
    return undefined;
  }
};

/**
 * Strict read-only check of a mesh root's state.json (pi-fabric#157). An absent file is a valid empty
 * mesh; a present file that is empty, damaged, or not a state envelope (`{}`, `null`) throws. The
 * runtime MeshStore stays tolerant; readers that must not report false absence call this first.
 */
export const assertMeshStateReadable = (root: string, maxBytes = DEFAULT_MAX_STATE_BYTES): void => {
  // After the SQLite switch state.json is the moved marker and state.db is the state: a SQLite store reads it
  // and a file store's reads fail closed on the marker (smarty-dev#6477), so the marker is not damage here.
  // A marker without its database still fails closed below (an alarm, never an empty mesh).
  const moved = readMeshStateMovedMarker(root);
  if (moved !== undefined && fs.existsSync(path.resolve(root, moved.movedTo))) return;
  const file = path.resolve(root, "state.json");
  const identity = stateReadIdentity(file, maxBytes);
  const shared = processReadSnapshots.get(file)?.deref();
  if (identity !== undefined && shared?.identity === identity && shared.canonicalReadable && shared.pending === undefined && shared.size <= maxBytes) return;
  let readable = false;
  const state = readState(file, maxBytes, false, () => { readable = true; });
  if (identity !== undefined && stateReadIdentity(file, maxBytes) === identity) {
    const stat = fs.statSync(file);
    rememberReadSnapshot(file, { device: stat.dev, inode: stat.ino, size: stat.size, modifiedAt: stat.mtimeMs,
      stamp: stampOf(stat), parsedAt: Date.now(), state, generation: generationOf(state), identity,
      canonicalReadable: readable });
  }
};

// Host invariant for the proved reducer: the persisted clock covers every
// issued token. Legacy files can seed only from retained entries/tombstones;
// tokens already evicted before this migration cannot be reconstructed.
const stateSlot = (state: MeshStateFile, key: string): {
  present: boolean; version: number; highWater: number;
} => {
  if (state.versions !== undefined &&
      (typeof state.versions !== "object" || state.versions === null || Array.isArray(state.versions))) {
    throw new Error("Invalid Fabric mesh revision table");
  }
  let retainedMaximum = 0;
  for (const revision of Object.values(state.versions ?? {})) {
    retainedMaximum = Math.max(retainedMaximum, storageRevision(revision));
  }
  for (const [entryKey, entry] of Object.entries(state.entries)) {
    if (typeof entry !== "object" || entry === null || entry.key !== entryKey) {
      throw new Error("Invalid Fabric mesh state entry");
    }
    const version = storageRevision(entry.version);
    const retained = state.versions !== undefined && Object.hasOwn(state.versions, entryKey)
      ? state.versions[entryKey] : undefined;
    if (version === 0 || (retained !== undefined && retained !== version)) {
      throw new Error("Inconsistent Fabric mesh revision");
    }
    retainedMaximum = Math.max(retainedMaximum, version);
  }
  // New snapshots require their clock: losing it must not look like legacy
  // migration and silently reissue revisions from an evicted history.
  if (Object.hasOwn(state, "revisionFormat") && state.revisionFormat !== 2) {
    throw new Error("Unsupported Fabric mesh revision protocol");
  }
  if ((state.format === 2 || state.revisionFormat === 2) && !Object.hasOwn(state, "highWater")) {
    throw new Error("Missing Fabric mesh high-water revision");
  }
  // Fork patch (pi-fabric#27 F1): Fabric builds before the persistent clock still write
  // version+1 without advancing highWater, so on a mixed fleet retained history can pass
  // the clock. Raise it to the retained maximum (the legacy seeding rule) instead of
  // refusing every later write on the root; retained tokens stay unique.
  const highWater = Object.hasOwn(state, "highWater")
    ? Math.max(storageRevision(state.highWater), retainedMaximum) : retainedMaximum;
  const present = Object.hasOwn(state.entries, key);
  const version = present ? state.entries[key]!.version
    : state.versions !== undefined && Object.hasOwn(state.versions, key) ? state.versions[key]! : 0;
  return { present, version, highWater };
};

const compactStateTombstones = (state: MeshStateFile, maxTombstones: number): void => {
  state.versions ??= {};
  const orderedKeys: string[] = [];
  const seen = new Set<string>();
  for (const key of state.tombstoneOrder ?? []) {
    if (Object.hasOwn(state.entries, key) || !Object.hasOwn(state.versions, key) || seen.has(key)) continue;
    seen.add(key);
    orderedKeys.push(key);
  }
  for (const key of Object.keys(state.versions)) {
    if (Object.hasOwn(state.entries, key) || seen.has(key)) continue;
    seen.add(key);
    orderedKeys.push(key);
  }
  const retainedKeys = orderedKeys.slice(-maxTombstones);
  const retained = new Set(retainedKeys);
  for (const key of Object.keys(state.versions)) {
    if (!Object.hasOwn(state.entries, key) && !retained.has(key)) delete state.versions[key];
  }
  state.tombstoneOrder = retainedKeys;
};

export type MeshBatchOperation =
  | {
      kind: "put";
      key: string;
      value: unknown | ((now: number) => unknown);
      /** Overrides the batch identity for a multi-owner transaction (e.g. bridge presence). */
      identity?: MeshIdentity;
      ifVersion?: number;
      onConflict?: "skip" | "abort" | ((current: MeshStateEntry | undefined) => "skip" | "abort");
    }
  | {
      kind: "delete";
      key: string;
      ifVersion?: number;
      onConflict?: "skip" | "abort" | ((current: MeshStateEntry | undefined) => "skip" | "abort");
      /**
       * Evaluated under the lock, against the state as this batch has changed it so far; false
       * skips the delete. For a delete that depends on another key (a participant whose owner
       * host must still be gone at commit, smarty-dev#367).
       */
      condition?: (current: (key: string) => MeshStateEntry | undefined) => boolean;
    };

export interface MeshBatchView {
  get(key: string): MeshStateEntry | undefined;
  listAll(prefix: string): MeshStateEntry[];
  /** Includes an absent key's retained CAS tombstone. */
  version(key: string): number;
}

export interface MeshBatchResult {
  key: string;
  applied: boolean;
  version: number;
}

/**
 * R11 (smarty-dev#6477 L2a): the files a writeBatch read before its transaction kept changing, so its
 * stamp never held inside the transaction within the allowed retries. Nothing was written.
 */
export class MeshStateFileReadChangedError extends Error {
  readonly code = "FABRIC_MESH_STATE_FILE_READ_CHANGED";
  constructor(readonly attempts: number) {
    super(`Fabric mesh writeBatch: the files read before the transaction changed ${attempts} times`);
    this.name = "MeshStateFileReadChangedError";
  }
}

/** Internal: a fileRead stamp did not hold inside the transaction; the backend re-reads and retries. */
export const FILE_READ_CHANGED: Error = new Error("Fabric mesh fileRead stamp changed");

/**
 * Internal: runs `fileRead.read()` bracketed by two stamps. Differing stamps mean a file changed
 * during the read, so the value may be older than any stamp taken afterwards: returns undefined and
 * the caller retries. Otherwise returns the value with the PRE-read stamp, the one the transaction
 * re-checks (a stamp taken only after the read could pair an old read with a newer file).
 */
export const bracketFileRead = (fileRead: MeshStateFileRead): { value: unknown; stamp: string | undefined } | undefined => {
  const stamp = fileRead.stamp();
  const value = fileRead.read();
  return fileRead.stamp() === stamp ? { value, stamp } : undefined;
};

// A copy of a committed state, readable after the transaction (commitOutbox effects).
const detachedView = (state: MeshStateFile): MeshBatchView => ({
  get: (key) => Object.hasOwn(state.entries, key) ? jsonClone(state.entries[key]) : undefined,
  listAll: (prefix) => Object.keys(state.entries).filter((key) => key.startsWith(prefix))
    .sort((left, right) => left.localeCompare(right)).map((key) => jsonClone(state.entries[key]!)),
  version: (key) => stateSlot(state, key).version,
});

export class MeshBatchConflictError extends Error {
  constructor(readonly key: string, readonly expected: number, readonly found: number) {
    super(`Mesh compare-and-swap failed for ${key}: expected version ${expected}, found ${found}`);
  }
}

interface ParsedStateSnapshot {
  device: number; inode: number; size: number; modifiedAt: number; stamp: string; parsedAt: number; state: MeshStateFile;
  generation: string | undefined;
  identity: string | undefined;
  canonicalReadable: boolean;
  journalCursor?: JournalCursor;
  /** A followed journal endpoint not yet bound to the canonical payload (false: that check failed). */
  pending?: JournalEndpoint | false;
  /** With pending: the last verified snapshot, from which an authoritative read replays instead. */
  anchor?: ParsedStateSnapshot;
}
// Reader snapshots only: mutable write transactions never share their state. Weak references
// avoid retaining abandoned roots; a small key cap bounds stale root names too.
const processReadSnapshots = new Map<string, WeakRef<ParsedStateSnapshot>>();
const rememberReadSnapshot = (file: string, snapshot: ParsedStateSnapshot): void => {
  if (processReadSnapshots.size >= 64 && !processReadSnapshots.has(file)) {
    processReadSnapshots.delete(processReadSnapshots.keys().next().value!);
  }
  processReadSnapshots.set(file, new WeakRef(snapshot));
};

export interface StateFileOptions {
  maxStateBytes?: number;
  maxStateTombstones?: number;
  readCacheMs?: number;
  backgroundReadCacheMs?: number;
  readActive?: () => boolean;
  writeReadJournal?: boolean;
}

const PREPARED_SWEEP_MS = 10 * 60_000;
const PREPARED_REUSE_AGE_MS = 60 * 60_000;
const PREPARED_STATE_NAME = /^([1-9]\d*)\.([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.prepared\.tmp$/i;

// ponytail: native process tools already report wall-clock birth time; avoid a second
// /proc tick/boot-time conversion. Unknown or second-resolution evidence stays conservative.
const processStartedAfter = (pid: number, modifiedAt: number): boolean => {
  try {
    let executable: string;
    let args: string[];
    if (process.platform === "linux" || process.platform === "darwin") {
      executable = "/bin/ps";
      args = ["-p", String(pid), "-o", "lstart="];
    } else if (process.platform === "win32" && process.env.SystemRoot) {
      executable = path.win32.join(process.env.SystemRoot, "System32", "WindowsPowerShell", "v1.0", "powershell.exe");
      args = ["-NoLogo", "-NoProfile", "-NonInteractive", "-Command",
        `(Get-Process -Id ${pid} -ErrorAction Stop).StartTime.ToUniversalTime().ToString('o')`];
    } else return false;
    const start = childProcess.execFileSync(executable, args, { encoding: "utf8", timeout: 2_000,
      maxBuffer: 4_096, windowsHide: true, stdio: ["ignore", "pipe", "ignore"],
      env: { ...process.env, LC_ALL: "C", TZ: "UTC" } }).trim();
    return Date.parse(process.platform === "win32" ? start : `${start} UTC`) > modifiedAt;
  } catch { return false; }
};

export class StateFile implements StateBackend {
  readonly kind = "file" as const;
  /** Set by the backend factory when a configured sqlite/shadow backend runs as file. */
  fallback: string | undefined;
  readonly root: string;
  readonly maxEventBytes: number;
  readonly maxReadEvents: number;
  readonly #lock: MeshLock;
  readonly #statePath: string;
  readonly #signalPath: string;
  /** Per parsed state: prefix selections (bounded) and namespace digests. Keyed by identity. */
  #memo = new WeakMap<MeshStateFile, { selections: Map<string, MeshStateEntry[]>; digests: Map<string, string> }>();
  /** Writer-only bytes from one commit; reusable only after a fresh, exact canonical-text match. */
  #writeEncodings: { serialized: string; entries: Map<string, EncodedStateEntry> } | undefined;
  /** The last full signal index parsed, keyed by its unique generation: one object, bounded. */
  #signalIndex: { generation: string; stamp: string; namespaces: Record<string, unknown> } | undefined;
  #signalIdentity: string | undefined;
  readonly #maxStateBytes: number;
  readonly #maxStateTombstones: number;
  readonly #readCacheMs: number;
  readonly #backgroundReadCacheMs: number | undefined;
  readonly #readActive: (() => boolean) | undefined;
  readonly #writeReadJournal: boolean;
  #requireCanonicalRead = false;
  #stateCache: ParsedStateSnapshot | undefined;
  #canonicalHeader: { identity: string; generation: string | undefined; journalHash: string | undefined } | undefined;
  #journalBase: JournalBase | undefined;
  #lastPreparedSweep = -Infinity;

  constructor(context: MeshStoreContext, options: StateFileOptions) {
    const { root, maxEventBytes } = context;
    this.root = root;
    this.maxEventBytes = maxEventBytes;
    this.maxReadEvents = context.maxReadEvents;
    this.#lock = context.lock;
    this.#statePath = path.join(root, "state.json");
    this.#signalPath = path.join(root, "state.read-signal.json");
    this.#maxStateBytes = Math.max(
      maxEventBytes * 2,
      Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES),
    );
    this.#maxStateTombstones = Math.max(
      1,
      Math.floor(options.maxStateTombstones ?? DEFAULT_MAX_STATE_TOMBSTONES),
    );
    this.#readCacheMs = Math.max(0, Math.floor(options.readCacheMs ?? 0));
    this.#backgroundReadCacheMs = options.backgroundReadCacheMs === undefined ? undefined
      : Math.max(MIN_BACKGROUND_MESH_READ_CACHE_MS, Math.floor(options.backgroundReadCacheMs));
    this.#readActive = options.readActive;
    this.#writeReadJournal = options.writeReadJournal !== false;
    this.#sweepPreparedState();
  }

  #sweepPreparedState(): void {
    const now = Date.now();
    if (now - this.#lastPreparedSweep < PREPARED_SWEEP_MS) return;
    this.#lastPreparedSweep = now;
    let removed = 0;
    try {
      const prefix = `${path.basename(this.#statePath)}.`;
      for (const entry of fs.readdirSync(this.root, { withFileTypes: true })) {
        if (!entry.isFile() || !entry.name.startsWith(prefix)) continue;
        const match = PREPARED_STATE_NAME.exec(entry.name.slice(prefix.length));
        if (!match) continue;
        const pid = Number(match[1]);
        if (!Number.isSafeInteger(pid) || pid === process.pid) continue;
        const file = path.join(this.root, entry.name);
        try {
          let dead = false;
          try { process.kill(pid, 0); }
          catch (error) {
            if (errorCode(error) !== "ESRCH") continue;
            dead = true;
          }
          if (!dead) {
            const modifiedAt = fs.statSync(file).mtimeMs;
            if (now - modifiedAt <= PREPARED_REUSE_AGE_MS || !processStartedAfter(pid, modifiedAt)) continue;
          }
          fs.unlinkSync(file);
          removed++;
        } catch { /* A sibling sweep/rename or unavailable evidence never blocks the store. */ }
      }
    } catch { /* Best-effort cleanup; ordinary state operations retain their own errors. */ }
    if (removed) console.info(`[mesh] Removed ${removed} abandoned prepared state file(s)`);
  }

  /** A failed locked operation means this store's view is behind (see MeshLock.withLock). */
  dropCache(): void {
    this.#stateCache = undefined;
  }

  /** The reuse window of reads (MeshStoreOptions.readCacheMs), for readers of files beside the state. */
  get readCacheMs(): number {
    return this.#readActive?.() ? 0 : this.#readCacheMs;
  }

  /** Opt-in background observations only; absence preserves legacy explicit TTL behavior. */
  get backgroundReadCacheMs(): number {
    if (this.#backgroundReadCacheMs === undefined) return this.readCacheMs;
    return this.#readActive?.() ? MIN_BACKGROUND_MESH_READ_CACHE_MS : this.#backgroundReadCacheMs;
  }

  /** Time until an idle observer may revalidate; hits do not slide this deadline. */
  get readCacheRemainingMs(): number {
    return this.#stateCache ? Math.max(0, this.backgroundReadCacheMs - (Date.now() - this.#stateCache.parsedAt)) : 0;
  }

  // Fresh protocol observations bypass idle coalescing, not the physical commit gate.
  // Actual writes still read authoritative bytes under the lock and apply verified CAS.
  get(key: string, options: MeshReadOptions = {}): MeshStateEntry | undefined {
    this.#validateKey(key);
    const state = options.fresh === true || options.snapshot === undefined
      ? this.#readCachedState(options.fresh === true, options.fresh === true, options.background === true, options.displayOnly === true)
      : options.snapshot as MeshStateFile;
    const entries = state.entries;
    return Object.hasOwn(entries, key) ? jsonClone(entries[key]) : undefined;
  }

  list(prefix = "", limit = 100, options: MeshReadOptions = {}): MeshStateEntry[] {
    const boundedLimit = Math.max(1, Math.min(Math.floor(limit), this.maxReadEvents));
    // Clone only the page: the dashboard lists the first 200 of the whole fleet state, and
    // cloning every entry to keep 200 was a large share of an idle Pi's CPU (smarty-dev#557).
    return this.#select(prefix, options).slice(0, boundedLimit).map((entry) => jsonClone(entry));
  }

  /** Internal project-state scan for host-managed indexes that must reconcile every key. */
  listAll(prefix = "", options: MeshReadOptions = {}): MeshStateEntry[] {
    return this.#select(prefix, options).map((entry) => jsonClone(entry));
  }

  /**
   * The parsed state that reads now return, as an opaque token: the same object until this
   * store parses the file again. A reader that derives an index from listAll can reuse it
   * while the token is unchanged (smarty-dev#557).
   */
  stateToken(options: MeshReadOptions = {}): object {
    return this.#readCachedState(options.fresh === true, options.fresh === true, options.background === true, options.displayOnly === true);
  }

  /**
   * listAll without the copies: the parsed entries themselves, which the caller must not change.
   * For an index that copies only the entries whose version moved (smarty-dev#557).
   */
  listAllShared(prefix = "", options: MeshReadOptions = {}): readonly Readonly<MeshStateEntry>[] {
    return this.#select(prefix, options).slice();
  }

  // The returned array is memoized per parsed state: callers copy it before handing it out.
  #select(prefix: string, options: MeshReadOptions): MeshStateEntry[] {
    if (prefix) this.#validateKey(prefix);
    const fresh = options.fresh === true;
    const state = options.snapshot !== undefined && !fresh
      ? options.snapshot as MeshStateFile
      : (!fresh && this.#signalledState(prefix, options.background === true, options.displayOnly === true)) ||
        this.#readCachedState(fresh, fresh, options.background === true, options.displayOnly === true);
    const memo = this.#memoOf(state);
    let selection = memo.selections.get(prefix);
    if (!selection) {
      selection = Object.values(state.entries)
        .filter((entry) => !prefix || entry.key.startsWith(prefix))
        .sort((left, right) => left.key.localeCompare(right.key));
      if (memo.selections.size >= SELECTION_MEMO_PREFIXES) memo.selections.delete(memo.selections.keys().next().value!);
      memo.selections.set(prefix, selection);
    }
    return selection;
  }

  #memoOf(state: MeshStateFile): { selections: Map<string, MeshStateEntry[]>; digests: Map<string, string> } {
    let memo = this.#memo.get(state);
    if (!memo) this.#memo.set(state, memo = { selections: new Map(), digests: new Map() });
    return memo;
  }

  // The cached parse, when the reuse window expired, the canonical generation changed, and the read signal bound
  // to the file's exact current stat shows this prefix's namespace unchanged. Otherwise undefined:
  // the caller re-reads as before. Never used by fresh reads or with readCacheMs 0.
  #signalledState(prefix: string, background: boolean, displayOnly: boolean): MeshStateFile | undefined {
    const cached = this.#stateCache;
    const namespace = keyNamespace(prefix);
    const readCacheMs = background ? this.backgroundReadCacheMs : this.readCacheMs;
    // An unbound journal endpoint is never a base for an authoritative read.
    if (!cached || (cached.pending !== undefined && !displayOnly) || !namespace || readCacheMs <= 0 || Date.now() - cached.parsedAt < readCacheMs) return undefined;
    const before = statStamp(this.#statePath);
    if (!before) return undefined;
    // The canonical header decides, not the stat (which can repeat): an unchanged generation is
    // revalidated cheaply by #readCachedState; a new one may still reuse an unchanged namespace.
    // The hint is trusted only for the exact commit the canonical header names, pinned by an
    // unchanged stat and header around the index read; a hint never published for it mismatches,
    // and a legacy or copied-marker file with a changed stat mismatches the index stamp.
    const current = this.#canonicalGeneration();
    if (typeof current !== "string" || current === cached.generation) return undefined;
    const index = this.#readSignalIndex();
    if (index?.generation !== current || index.stamp !== before) return undefined;
    if (statStamp(this.#statePath) !== before || this.#canonicalGeneration() !== current) return undefined;
    const { namespaces } = index;
    const expected = Object.hasOwn(namespaces, namespace) ? namespaces[namespace] : EMPTY_DIGEST;
    if (typeof expected !== "string" || expected !== this.#namespaceDigest(cached.state, namespace)) return undefined;
    return cached.state;
  }

  // The current signal's index: its bounded header every call, the full body only when the
  // generation differs from the memoized one (once per commit, across all namespaces).
  #readSignalIndex(): { generation: string; stamp: string; namespaces: Record<string, unknown> } | undefined {
    const identity = stateReadIdentity(this.#signalPath, MAX_SIGNAL_BYTES);
    if (identity !== undefined && this.#signalIdentity === identity && this.#signalIndex) return this.#signalIndex;
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#signalPath, "r");
      const generation = readHeader(descriptor, SIGNAL_HEADER);
      if (generation === undefined) return undefined;
      if (this.#signalIndex?.generation === generation) return this.#signalIndex;
      const size = fs.fstatSync(descriptor).size;
      if (size > MAX_SIGNAL_BYTES) return undefined;
      const buffer = Buffer.allocUnsafe(size);
      if (fs.readSync(descriptor, buffer, 0, size, 0) !== size) return undefined;
      const signal = JSON.parse(buffer.toString("utf8")) as { generation?: unknown; stamp?: unknown; namespaces?: unknown };
      const namespaces = signal?.namespaces;
      if (
        signal?.generation !== generation || typeof signal.stamp !== "string" ||
        typeof namespaces !== "object" || namespaces === null || Array.isArray(namespaces)
      ) return undefined;
      if (identity !== undefined && stateReadIdentity(this.#signalPath, MAX_SIGNAL_BYTES) === identity) this.#signalIdentity = identity;
      return this.#signalIndex = { generation, stamp: signal.stamp, namespaces: namespaces as Record<string, unknown> };
    } catch {
      return undefined;
    } finally {
      closeQuietly(descriptor);
    }
  }

  // Canonical commit UUID and optional journal hash: one bounded header per physical generation.
  // undefined: a legacy file without a marker; false: unreadable, which never matches a label.
  #canonicalGeneration(): string | undefined | false {
    const identity = stateReadIdentity(this.#statePath, this.#maxStateBytes);
    if (identity !== undefined && this.#canonicalHeader?.identity === identity) return this.#canonicalHeader.generation;
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#statePath, "r");
      // Kernel witness (read-journal.ts): an fstat tuple the writer recorded with this generation
      // and chain head proves the header bytes too, so they need not be read.
      const witness = readStateWitness(this.root, fs.fstatSync(descriptor, { bigint: true }));
      if (witness) {
        if (identity !== undefined && stateReadIdentity(this.#statePath, this.#maxStateBytes) === identity) {
          this.#canonicalHeader = { identity, generation: witness.generation, journalHash: witness.chainHash };
        }
        return witness.generation;
      }
      const buffer = Buffer.alloc(192);
      const read = fs.readSync(descriptor, buffer, 0, buffer.length, 0);
      const header = buffer.toString("latin1", 0, read);
      const generation = STATE_HEADER.exec(header)?.[1];
      const journalHash = /^\{"readGeneration":"[0-9a-f-]{36}","readJournalHash":"([0-9a-f]{64})"/.exec(header)?.[1];
      if (identity !== undefined && stateReadIdentity(this.#statePath, this.#maxStateBytes) === identity) this.#canonicalHeader = { identity, generation, journalHash };
      return generation;
    } catch {
      return false;
    } finally {
      closeQuietly(descriptor);
    }
  }

  #namespaceDigest(state: MeshStateFile, namespace: string): string {
    const memo = this.#memoOf(state);
    let digest = memo.digests.get(namespace);
    if (digest === undefined) {
      const keys = sortedKeys(Object.keys(state.entries).filter((key) => key.startsWith(namespace)));
      digest = digestEntries(keys.map((key) => state.entries[key]!));
      if (memo.digests.size >= SELECTION_MEMO_PREFIXES) memo.digests.delete(memo.digests.keys().next().value!);
      memo.digests.set(namespace, digest);
    }
    return digest;
  }

  // Capture canonical bytes OUTSIDE custody. The physical identity is rechecked around
  // preparation and again under the lock; copied UUID/version labels are never authority.
  #readStateForWrite(): WriteStateSnapshot {
    let reuse: Map<string, EncodedStateEntry> | undefined;
    const state = readState(this.#statePath, this.#maxStateBytes, false, (serialized) => {
      // Full content equality, not UUID/stat/version equality: a legacy copied-marker writer
      // can change an entry without advancing any of those labels. Always read and parse fresh.
      if (serialized === this.#writeEncodings?.serialized) reuse = this.#writeEncodings.entries;
    });
    // The base carries the snapshot's endpoint stamp so this commit's format-2 record binds
    // its predecessor (#560). The stamp is taken outside custody with the snapshot (#547);
    // the identity recheck around preparation and under the lock rejects any replacement.
    return { state, reuse, base: journalBase(state, this.#statePath, statStamp(this.#statePath)) };
  }

  #stateWriteIdentity(): string | undefined {
    const identity = stateReadIdentity(this.#statePath, this.#maxStateBytes);
    if (identity !== undefined) return identity;
    try { fs.statSync(this.#statePath); }
    catch (error) { if (errorCode(error) === "ENOENT") return "absent"; }
    // Adapters without usable physical metadata retain the conservative locked read.
    return undefined;
  }

  async #withWriteSnapshot<T, P>(prepare: (snapshot: WriteStateSnapshot) => P,
    commit: (prepared: P) => T, cleanup?: (prepared: P) => void, lockClass: MeshLockClass = "other"): Promise<T> {
    const scope = this.#lock.tryLockScope.getStore();
    if (!scope?.active) this.#sweepPreparedState();
    const deadline = Date.now() + this.#lock.lockTimeoutMs;
    // The reduced remaining budget below must not turn an ordinary cold protocol-2
    // write into a bounded custody try. Await constructor preparation outside custody;
    // an actual registry-fenced try still fails closed promptly in #withLock.
    if (this.#lock.lockProtocol === 2 && !this.#lock.ownIncarnationReady && !scope?.active) {
      await this.#lock.ownIncarnation;
    }
    for (;;) {
      this.#lock.writeAbortSignal?.throwIfAborted();
      const identity = this.#stateWriteIdentity();
      if (identity === undefined) {
        return this.#lock.withLock(() => {
          const prepared = prepare(this.#readStateForWrite());
          try { return commit(prepared); } finally { cleanup?.(prepared); }
        }, undefined, lockClass);
      }
      let admitted = false;
      let prepared: P | undefined;
      try {
        prepared = prepare(this.#readStateForWrite());
        if (this.#stateWriteIdentity() !== identity) throw WRITE_SNAPSHOT_CHANGED;
        return await this.#lock.withLock(() => {
          if (this.#stateWriteIdentity() !== identity) throw WRITE_SNAPSHOT_CHANGED;
          admitted = true;
          return commit(prepared!);
        }, Math.max(0, deadline - Date.now()), lockClass);
      } catch (error) {
        // A stale CAS/parse/size error belongs to the snapshot, not the current file.
        if (error !== WRITE_SNAPSHOT_CHANGED && (admitted || this.#stateWriteIdentity() === identity)) {
          this.#stateCache = undefined;
          throw error;
        }
        // Never retry inside an actor registry fence: its caller releases custody and
        // retries the whole admission step, just as for a busy mesh.
        if (scope?.active || Date.now() >= deadline) {
          if (!(error instanceof MeshLockTimeoutError)) lockStats?.failed(this.root, lockClass, 0, Boolean(scope?.active));
          throw new MeshLockTimeoutError(describeLockHolder(path.join(this.#lock.lockPath, "owner")), 0, 0);
        }
        await delay(0, this.#lock.writeAbortSignal);
      } finally {
        if (prepared !== undefined) cleanup?.(prepared);
      }
    }
  }

  // Only pure single-key transitions may be prepared optimistically. Callback batches
  // still select and invoke their callbacks exactly once under validated custody.
  async #writeState<T>(change: (state: MeshStateFile) => { result: T; keys?: string[] }, caller?: string[]): Promise<T> {
    return this.#withWriteSnapshot(({ state, reuse, base }) => {
      const outcome = change(state);
      const prepared = outcome.keys ? this.#prepareStateCommit(state, reuse, outcome.keys, base) : undefined;
      if (prepared) {
        // State remains a soft-state atomic replacement (no durability policy change).
        // Stage the large write off-lock too; custody only compares identity and renames.
        const temporary = `${this.#statePath}.${process.pid}.${randomUUID()}.prepared.tmp`;
        writeFileAtomic(temporary, prepared.encoded.serialized);
        prepared.temporary = temporary;
      }
      return { state, ...outcome, prepared };
    }, ({ state, result, keys, prepared }) => {
      if (prepared) this.#commitPreparedState(prepared, keys!, caller);
      else this.#cacheState(state, undefined);
      return result;
    }, ({ prepared }) => {
      if (prepared?.temporary) {
        try { fs.rmSync(prepared.temporary, { force: true }); } catch { /* Best-effort private staging cleanup. */ }
      }
    }, "put/delete");
  }

  // A commit's write, signal and cache, under the lock. The stamp is taken right after the rename,
  // before hashing the encoded entries; the signal is published and the cache kept only while the file still has it,
  // so a lock-bypassing writer replacing the file meanwhile never gets this payload's hashes or
  // cache label. ponytail: a replace between the rename and that first stat cannot be detected
  // without the written descriptor (atomic-write.ts); the lock protocol excludes it.
  // The commit's new readGeneration is serialized FIRST and atomically with the payload (the
  // previous one is dropped from the copy), so the canonical header alone identifies the commit
  // whether or not the optional signal is published afterwards. The stamped copy is cached.
  #commitState(state: MeshStateFile, reuse: Map<string, EncodedStateEntry> | undefined, keys: string[], caller: string[] | undefined,
    namespaces?: Record<string, string>): void {
    this.#commitPreparedState(this.#prepareStateCommit(state, reuse, keys, this.#journalBase, namespaces), keys, caller);
  }

  #prepareStateCommit(state: MeshStateFile, reuse: Map<string, EncodedStateEntry> | undefined,
    keys: string[], base: JournalBase | undefined, namespaces?: Record<string, string>): PreparedStateCommit {
    const payload: MeshStateFile = { ...state };
    delete payload.readGeneration;
    delete payload.readJournalHash;
    const generation = randomUUID();
    const unstamped: MeshStateFile = { readGeneration: generation, ...payload };
    const canonical = encodeState(unstamped, reuse);
    let journal = this.#writeReadJournal ? prepareStateJournal(unstamped, base, keys, canonical.entries, canonical.serialized) : undefined;
    let stamped: MeshStateFile = journal
      ? { readGeneration: generation, readJournalHash: journal.hash, ...payload } : unstamped;
    let encoded = journal ? encodeState(stamped, canonical.entries) : canonical;
    // An optional accelerator must not reject a write whose canonical payload fits.
    // Omit the chain head/record at the cap; readers safely fall back to canonical bytes.
    if (journal && encoded.serialized.byteLength > this.#maxStateBytes) {
      journal = undefined; stamped = unstamped; encoded = canonical;
    }
    if (encoded.serialized.byteLength > this.#maxStateBytes) {
      throw new Error(`Fabric mesh state exceeds ${this.#maxStateBytes} bytes`);
    }
    // Batch preparation hashes unchanged namespaces outside custody from this exact
    // snapshot. Never reuse a digest for a namespace changed by a locked callback.
    const unchanged = { ...namespaces };
    for (const key of keys) {
      const namespace = keyNamespace(key);
      if (namespace) delete unchanged[namespace];
    }
    return { stamped, generation, encoded, journal, namespaces: this.#signalNamespaces(encoded.entries, unchanged),
      serializedText: encoded.serialized.toString("utf8") };
  }

  // The fence's fallback when state.json's bounded header does not carry backendEpoch: this store's decoder.
  readonly #decodeFileEpoch = (file: string): number => {
    const epoch = (readState(file, this.#maxStateBytes, false) as MeshStateFile & { backendEpoch?: unknown }).backendEpoch;
    return epoch === undefined ? 0 : storageRevision(epoch);
  };

  #commitPreparedState(prepared: PreparedStateCommit, keys: string[], caller?: string[]): void {
    const { stamped, generation, encoded, journal, namespaces, serializedText } = prepared;
    // Kernel witness (read-journal.ts): the tuple of OUR inode, taken through a descriptor opened
    // on the staged file before the rename, so a replacement right after the rename cannot borrow
    // this payload's hash. ctime is read after the rename (rename updates it).
    // smarty-dev#6477 L4b (R1): under .lock and before the rename, for every write (put, delete,
    // batch, tombstone compaction). After a cutover set backend=sqlite (or during a rollback export)
    // state.json is not the authority: refuse with MeshBackendFenceError and leave it untouched.
    // One stat of state.db when absent; nothing is cached across lock holds.
    assertFileStateWritable(this.root, { maxStateBytes: this.#maxStateBytes, decodeFileEpoch: this.#decodeFileEpoch });
    let staged: number | undefined;
    if (journal && prepared.temporary) try { staged = fs.openSync(prepared.temporary, "r"); } catch { /* no witness */ }
    let witnessStat: fs.BigIntStats | undefined;
    try {
      if (prepared.temporary) renameAtomic(prepared.temporary, this.#statePath);
      else writeFileAtomic(this.#statePath, encoded.serialized);
      if (journal) {
        try { witnessStat = staged !== undefined ? fs.fstatSync(staged, { bigint: true }) : fs.statSync(this.#statePath, { bigint: true }); }
        catch { /* no witness: readers hash */ }
      }
    } finally { closeQuietly(staged); }
    commitStats?.record(encoded.serialized.byteLength, keys);
    const stamp = statStamp(this.#statePath);
    let journalCursor: JournalCursor | undefined;
    if (stamp !== undefined) {
      if (this.#writeReadJournal) journalCursor = appendStateJournal(this.root, journal, stamp);
      const witness = journal && witnessStat ? stateWitnessOf(witnessStat, journal.hash, journal.payloadHash) : undefined;
      this.#writeSignal(encoded.entries, stamp, generation, namespaces, witness);
    }
    if (stamp === undefined || !this.#cacheState(stamped, stamp)) this.#stateCache = undefined;
    // Under the lock our record ends the journal: the next read follows from there by offset.
    else if (journalCursor) this.#stateCache!.journalCursor = journalCursor;
    this.#writeEncodings = { serialized: serializedText, entries: encoded.entries };
    const trace = process.env.PI_FABRIC_COMMIT_TRACE;
    if (trace) {
      try {
        fs.appendFileSync(trace, JSON.stringify({ at: Date.now(), pid: process.pid, statePath: this.#statePath,
          generation, bytes: encoded.serialized.byteLength, keys: [...new Set(keys)], caller }) + "\n");
      } catch { /* Diagnostics must never fail a durable commit. */ }
    }
  }

  // Best effort, after a commit: a failure leaves an older signal whose generation no longer
  // matches the canonical header, which only forces re-reads. It never fails the committed write.
  #signalNamespaces(entries: Map<string, EncodedStateEntry>, reuse: Record<string, string> = {},
    omit: Set<string | undefined> = new Set()): Record<string, string> | undefined {
    try {
      const hashes = new Map<string, ReturnType<typeof createHash>>();
      const delimiter = Buffer.from("\n");
      // Same ordered entry bytes and newline framing as digestEntries, without re-encoding.
      for (const key of sortedKeys([...entries.keys()])) {
        const namespace = keyNamespace(key);
        if (!namespace || Object.hasOwn(reuse, namespace) || omit.has(namespace)) continue;
        let hash = hashes.get(namespace);
        if (!hash) hashes.set(namespace, hash = createHash("sha256"));
        hash.update(entries.get(key)!.entry).update(delimiter);
      }
      const namespaces: Record<string, string> = { ...reuse };
      for (const [namespace, hash] of hashes) namespaces[namespace] = hash.digest("base64");
      return namespaces;
    } catch { return undefined; } // Optional acceleration must never reject a canonical commit.
  }

  #writeSignal(entries: Map<string, EncodedStateEntry>, stamp: string, generation: string,
    namespaces = this.#signalNamespaces(entries), witness?: StateWitness): boolean {
    try {
      if (!namespaces || statStamp(this.#statePath) !== stamp) return false;   // replaced while preparing: publish nothing
      // The witness only while the path still names exactly the witnessed inode, unchanged.
      if (witness && !sameWitnessTuple(fs.statSync(this.#statePath, { bigint: true }), witness)) witness = undefined;
      // `generation` first: readers take it from the file's first HEADER_BYTES; it must equal the canonical readGeneration.
      // The kernel witness second, so a reader binds it from one bounded header read.
      const serialized = JSON.stringify({ generation, ...(witness ? { witness } : {}), stamp, namespaces });
      if (Buffer.byteLength(serialized, "utf8") > MAX_SIGNAL_BYTES) return false;
      writeFileAtomic(this.#signalPath, serialized);
      return true;
    } catch {
      // An older or missing signal only disables reuse.
      return false;
    }
  }

  async put(input: {
    key: string;
    value: unknown;
    identity: MeshIdentity;
    ifVersion?: number;
  }): Promise<MeshStateEntry> {
    const { key, value, identity, ifVersion } = input;
    const caller = commitTraceCaller();
    this.#validateKey(key);
    const request = captureStoragePut({ key, value, identity, ifVersion }, this.maxEventBytes);
    return this.#writeState((state) => {
      const slot = stateSlot(state, request.key);
      const plan = request.transition(slot.present, slot.version, slot.highWater);
      if (plan.kind !== "put") throw new Error("Invalid verified storage put plan");
      const entry: MeshStateEntry = {
        key: plan.key,
        value: plan.value,
        version: plan.version,
        updatedAt: Date.now(),
        updatedBy: plan.identity,
      };
      state.entries[plan.key] = entry;
      state.versions ??= {};
      state.versions[plan.key] = plan.version;
      // Keep the envelope readable by existing hosts; revisionFormat marks the
      // mandatory persistent clock without making old readers quarantine it.
      state.format = 1;
      state.revisionFormat = 2;
      state.highWater = plan.highWater;
      state.tombstoneOrder = (state.tombstoneOrder ?? []).filter((key) => key !== plan.key);
      compactStateTombstones(state, this.#maxStateTombstones);
      return { result: jsonClone(entry), keys: [plan.key] };
    }, caller);
  }

  /**
   * Takes and releases the mesh lock without writing the state: evidence that the shared state is
   * writable now, for a heartbeat that renewed only its file lease. Revalidates on the next
   * read without discarding an unchanged parsed snapshot. Explicit confirmation starts one
   * new fixed idle window; ordinary cache hits never slide that deadline.
   */
  /** R20 write fence: every state.json writer takes `.lock`, which the caller holds. */
  withWriteFence<T>(operation: () => T): T {
    return operation();
  }

  async confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    await this.#lock.withLock(() => {
      // Invalidate observation age, not the payload. Metadata + generation still guard reuse.
      this.#requireCanonicalRead = true;
      if (this.#stateCache) this.#stateCache = { ...this.#stateCache, parsedAt: 0 };
      onAcquired?.(Date.now());
    }, undefined, "heartbeat/confirm");
  }

  async delete(input: {
    key: string;
    ifVersion?: number;
  }): Promise<{ deleted: boolean; version?: number }> {
    const { key, ifVersion } = input;
    const caller = commitTraceCaller();
    this.#validateKey(key);
    const request = captureStorageDelete({ key, ifVersion });
    return this.#writeState<{ deleted: boolean; version?: number }>((state) => {
      const slot = stateSlot(state, request.key);
      const plan = request.transition(slot.present, slot.version, slot.highWater);
      if (plan.kind === "unchanged") {
        return { result: { deleted: false } };
      }
      if (plan.kind !== "delete") throw new Error("Invalid verified storage delete plan");
      delete state.entries[plan.key];
      state.versions ??= {};
      // Delete consumes a key successor and advances the persistent clock.
      // Eviction can forget a CAS tombstone, but not allocation history.
      state.versions[plan.key] = plan.version;
      // Keep the envelope readable by existing hosts; revisionFormat marks the
      // mandatory persistent clock without making old readers quarantine it.
      state.format = 1;
      state.revisionFormat = 2;
      state.highWater = plan.highWater;
      state.tombstoneOrder = [
        ...(state.tombstoneOrder ?? []).filter((key) => key !== plan.key),
        plan.key,
      ];
      compactStateTombstones(state, this.#maxStateTombstones);
      return { result: { deleted: true, version: plan.version }, keys: [plan.key] };
    }, caller);
  }

  // Applies several puts and deletes in ONE locked read-modify-write, so a caller that
  // updates many keys at once rewrites the shared state file once instead of once per
  // key. Each operation keeps put()/delete() semantics, including an optional
  // compare-and-swap. On a version mismatch, `onConflict` decides: "skip" leaves that
  // key alone, "abort" writes nothing at all and rejects. A put value may be a function,
  // evaluated under the lock at commit time (for timestamps such as lease stamps).
  // A synchronous prepare callback builds ops from the authoritative snapshot under the same lock. Its
  // view returns copies, never mutable state. afterCommit runs under that lock after a successful
  // commit (also for a no-op batch), for ownership-bound file leases; it must not call store writers.
  // Returns one result per operation, in order.
  // R11 (L2a): fileRead runs before the lock and its stamp is re-checked under it; commitOutbox
  // runs after the lock is released (see StateBackendBatchInput in state-backend.ts).
  // `committed` (internal, the shadow backend's mirror) receives the changed keys right after the
  // state file commit, before afterCommit/commitOutbox, so a throwing callback cannot hide a commit.
  async writeBatch(input: StateBackendBatchInput, committed?: (changed: readonly string[]) => void): Promise<MeshBatchResult[]> {
    const caller = commitTraceCaller();
    for (const op of input.ops) this.#validateKey(op.key);
    if (input.ops.length === 0 && !input.prepare && !input.afterCommit && !input.commitOutbox) return [];
    const fileRead = input.fileRead;
    const retries = Math.max(0, Math.floor(fileRead?.retries ?? 3));
    for (let attempt = 0; ; attempt += 1) {
      const bracket = fileRead ? bracketFileRead(fileRead) : undefined;
      let outcome: { results: MeshBatchResult[]; effects?: Omit<MeshCommitEffects, "stamp"> };
      try {
        if (fileRead && !bracket) throw FILE_READ_CHANGED;
        outcome = await this.#writeBatchOnce(input, caller, bracket?.value,
          fileRead && bracket ? () => fileRead.stamp() === bracket.stamp : undefined, committed);
      } catch (error) {
        if (error !== FILE_READ_CHANGED) throw error;
        if (attempt >= retries) throw new MeshStateFileReadChangedError(attempt + 1);
        continue;
      }
      if (input.commitOutbox && outcome.effects) input.commitOutbox({ ...outcome.effects, stamp: this.stateStamp() });
      return outcome.results;
    }
  }

  #writeBatchOnce(input: StateBackendBatchInput, caller: string[] | undefined, fileValue: unknown,
    stampHolds: (() => boolean) | undefined, committed?: (changed: readonly string[]) => void): Promise<{ results: MeshBatchResult[]; effects?: Omit<MeshCommitEffects, "stamp"> }> {
    return this.#withWriteSnapshot(snapshot => {
      const omitted = new Set(input.ops.map(op => op.key));
      const reuse = new Map(snapshot.reuse);
      // Cold multi-process writers pre-encode retained entries before custody too.
      // Explicitly changed/deleted keys are encoded only after their locked transition.
      for (const key of Object.keys(snapshot.state.entries)) {
        if (omitted.has(key) || reuse.has(key)) continue;
        const encoded = encodeEntry(key, snapshot.state.entries[key]);
        if (encoded) reuse.set(key, encoded);
      }
      const namespaces = this.#signalNamespaces(reuse, {}, new Set(input.ops.map(op => keyNamespace(op.key))));
      return { ...snapshot, reuse, namespaces };
    }, ({ state, reuse, base, namespaces }) => {
      if (stampHolds && !stampHolds()) throw FILE_READ_CHANGED;
      this.#journalBase = base;
      // Each operation takes the same verified transition as put()/delete(), so a batch
      // advances the persistent clock exactly as the single writes would, and damaged
      // state is the same write barrier.
      state.versions ??= {};
      const tombstones = new Set(state.tombstoneOrder ?? []);
      const results: MeshBatchResult[] = [];
      let changed = false;
      const now = Date.now();
      const current = (key: string): MeshStateEntry | undefined =>
        Object.hasOwn(state.entries, key) ? jsonClone(state.entries[key]) : undefined;
      const view: MeshBatchView = {
        get: current,
        listAll: (prefix) => Object.keys(state.entries).filter((key) => key.startsWith(prefix))
          .sort((left, right) => left.localeCompare(right)).map((key) => jsonClone(state.entries[key]!)),
        version: (key) => stateSlot(state, key).version,
      };
      const ops = [...input.ops, ...(input.prepare?.(view, fileValue) ?? [])];
      for (const op of ops) this.#validateKey(op.key);
      for (const op of ops) {
        const slot = stateSlot(state, op.key);
        const existing = state.entries[op.key];
        if (op.kind === "delete" && op.condition && !op.condition(current)) {
          results.push({ key: op.key, applied: false, version: slot.version });
          continue;
        }
        if (op.ifVersion !== undefined && op.ifVersion !== slot.version) {
          const policy = typeof op.onConflict === "function"
            ? op.onConflict(existing ? jsonClone(existing) : undefined)
            : op.onConflict ?? "abort";
          if (policy === "abort") {
            throw new MeshBatchConflictError(op.key, op.ifVersion, slot.version);
          }
          results.push({ key: op.key, applied: false, version: slot.version });
          continue;
        }
        const request = op.kind === "delete"
          ? captureStorageDelete({ key: op.key, ifVersion: op.ifVersion })
          : captureStoragePut({
            key: op.key,
            ifVersion: op.ifVersion,
            value: typeof op.value === "function" ? (op.value as (now: number) => unknown)(now) : op.value,
            identity: op.identity ?? input.identity,
          }, this.maxEventBytes);
        const plan = request.transition(slot.present, slot.version, slot.highWater);
        if (plan.kind === "unchanged") {
          results.push({ key: op.key, applied: false, version: slot.version });
          continue;
        }
        if (plan.kind === "delete") {
          delete state.entries[plan.key];
          tombstones.delete(plan.key);
          tombstones.add(plan.key);
        } else {
          state.entries[plan.key] = {
            key: plan.key, value: plan.value, version: plan.version, updatedAt: now, updatedBy: plan.identity,
          };
          tombstones.delete(plan.key);
        }
        state.versions[plan.key] = plan.version;
        state.format = 1;
        state.revisionFormat = 2;
        state.highWater = plan.highWater;
        results.push({ key: op.key, applied: true, version: plan.version });
        changed = true;
      }
      if (!changed) {
        this.#cacheState(state, undefined);
      } else {
        state.tombstoneOrder = [...tombstones];
        compactStateTombstones(state, this.#maxStateTombstones);
        const changedKeys = results.filter(result => result.applied).map(result => result.key);
        this.#commitState(state, reuse, changedKeys, caller, namespaces);
        committed?.(changedKeys);
      }
      input.afterCommit?.(view);
      if (!input.commitOutbox) return { results };
      return { results, effects: {
        backend: "file" as const,
        results: results.map((result) => ({ ...result })),
        changed: results.filter((result) => result.applied).map((result) => result.key),
        view: detachedView(jsonClone(state)),
      } };
    }, undefined, input.lockClass === "bridge" ? "bridge" : "writeBatch");
  }

  diagnostics(): StateBackendDiagnostics {
    return { kind: "file", ...(this.fallback ? { fallback: this.fallback } : {}) };
  }

  /** Nothing to release: state.json is opened per operation. */
  close(): void {}

  /**
   * Changes whenever the shared state file does, from its metadata alone: a poll can test it
   * without reading or parsing the file (review/astra F1 on #84).
   */
  stateStamp(): string | undefined {
    try {
      const stat = fs.statSync(this.#statePath);
      return `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}`;
    } catch {
      return undefined;
    }
  }

  /**
   * The stamp of the state payload that reads now return, from this store's cache. With fresh,
   * revalidate through the background read-cache window, not authoritative payload freshness.
   * This UI observer may lag remote changes by backgroundReadCacheMs; observing a cached payload does
   * not extend its age window. Expired reads use ordinary metadata/header validation and fallback.
   * An explicit remote rebuild may opt into revalidateGeneration: a new canonical UUID bypasses
   * even a warm window. Matching/copied, missing or unreadable markers keep ordinary TTL behavior;
   * this is not authority or proof against legacy copied-marker ABA.
   * A reader records what it consumed, not what is on disk (review/astra F2 on #84).
   */
  cachedStateStamp(fresh = false, revalidateGeneration = false): string | undefined {
    if (fresh) {
      try {
        const cached = this.#stateCache;
        let changed = false;
        if (revalidateGeneration && cached && this.backgroundReadCacheMs > 0 && Date.now() - cached.parsedAt < this.backgroundReadCacheMs) {
          const generation = this.#canonicalGeneration();
          changed = typeof generation === "string" && generation !== cached.generation;
        }
        this.#readCachedState(changed, false, true, true); // display-only observer; public payload reads stay bound
      } catch {
        return undefined;
      }
    }
    const cached = this.#stateCache;
    return cached ? `${cached.device}:${cached.inode}:${cached.size}:${cached.modifiedAt}` : undefined;
  }

  #readCachedState(fresh = false, canonical = fresh, background = false, displayOnly = false): MeshStateFile {
    const confirmed = this.#requireCanonicalRead;
    if (confirmed) { fresh = true; canonical = true; this.#requireCanonicalRead = false; }
    // Only an explicitly display-only, non-canonical read may serve a journal endpoint whose
    // payload hash is not bound yet. Every other read (decisions, routing, writes) binds it.
    const deferrable = displayOnly && !canonical;
    const recent = this.#stateCache;
    const readCacheMs = background ? this.backgroundReadCacheMs : this.readCacheMs;
    if (!fresh && recent && (deferrable || recent.pending === undefined) && readCacheMs > 0 && Date.now() - recent.parsedAt < readCacheMs) {
      return recent.state;
    }
    let before: string;
    let beforeIdentity: string | undefined;
    let replayFailed = false;
    try {
      const observed = fs.statSync(this.#statePath);
      if (observed.size > this.#maxStateBytes) throw new Error(`Failed to read Fabric mesh state: state exceeds ${this.#maxStateBytes} bytes`);
      before = stampOf(observed);
      const identity = beforeIdentity = stateReadIdentity(this.#statePath, this.#maxStateBytes);
      const generation = this.#canonicalGeneration();
      const cached = this.#stateCache;
      // Nanosecond ctime/inode also detect an older writer that copies the UUID or writes
      // in place. Without that identity retain the historical conservative fresh fallback.
      const matches = (snapshot: ParsedStateSnapshot | undefined): boolean =>
        !!snapshot && snapshot.size <= this.#maxStateBytes && snapshot.stamp === before &&
        identity !== undefined && snapshot.identity === identity &&
        (generation === undefined || snapshot.generation === generation); // Legacy markers need not occupy the header.
      // A display-only follow may defer the canonical payload hash of its journal endpoint. Any
      // other read binds that endpoint once (shared by every store in the process) before
      // serving it; a failed binding is never served again and falls back to the canonical read.
      const settled = (snapshot: ParsedStateSnapshot): boolean => {
        if (snapshot.pending === undefined || (snapshot.pending !== false && deferrable)) return true;
        if (snapshot.pending !== false && verifyStateJournalEndpoint(this.root, snapshot.pending)) {
          delete snapshot.pending;
          delete snapshot.anchor;
          return true;
        }
        snapshot.pending = false;
        replayFailed = true;
        return false;
      };
      if (cached && matches(cached) && settled(cached)) {
        if (confirmed) this.#stateCache = { ...cached, parsedAt: Date.now() };
        return cached.state;
      }
      // A copied marker/rounded stat cannot override a changed high-resolution identity.
      // Only adapters without that identity retain the historical nonfresh fallback.
      if (identity === undefined && !canonical && cached?.stamp === before && cached.pending === undefined && (cached.generation !== undefined || fresh) &&
        cached.generation === generation) return cached.state;
      const shared = processReadSnapshots.get(path.resolve(this.#statePath))?.deref();
      if (shared && matches(shared) && settled(shared)) {
        this.#stateCache = confirmed ? { ...shared, parsedAt: Date.now() } : shared;
        return shared.state;
      }
      // A newly constructed store can replay from this process's prior snapshot too;
      // it need not parse the whole file merely because another store observed it first.
      // An authoritative read never replays from an unbound endpoint: a forged terminal there
      // would poison every later state. It replays from that snapshot's last verified anchor.
      const replayBase = (snapshot: ParsedStateSnapshot | undefined): ParsedStateSnapshot | undefined =>
        !snapshot || snapshot.pending === false ? undefined
          : snapshot.pending === undefined || deferrable ? snapshot : snapshot.anchor;
      const base = replayBase(cached) ?? replayBase(shared);
      if (base && !replayFailed && identity !== undefined && typeof generation === "string" &&
        this.#canonicalHeader?.journalHash !== undefined) {
        const replay = replayStateJournal(this.root, base.state, generation, identity, before, base.identity,
          this.#canonicalHeader?.generation === generation ? this.#canonicalHeader.journalHash : undefined, base.journalCursor,
          !deferrable, base.stamp);
        if (replay && stateReadIdentity(this.#statePath, this.#maxStateBytes) === identity && this.#canonicalGeneration() === generation &&
          this.#cacheState(replay.state, before, true, identity)) {
          this.#stateCache!.journalCursor = replay.cursor;
          if (replay.pending) {
            this.#stateCache!.pending = replay.pending;
            const anchor = base.pending === undefined ? base : base.anchor;
            if (anchor) this.#stateCache!.anchor = anchor;
          }
          rememberReadSnapshot(path.resolve(this.#statePath), this.#stateCache!);
          return replay.state;
        }
        replayFailed = true;
      }
    } catch (error) {
      this.#stateCache = undefined;
      if (errorCode(error) === "ENOENT") return emptyState();
      throw error;
    }
    if (replayFailed) {
      // Failed verification may have consumed a replaced Windows file. Discard its
      // header/endpoints only on recovery, not on an ordinary canonical parse.
      this.#canonicalHeader = undefined;
      before = statStamp(this.#statePath) ?? before;
      beforeIdentity = stateReadIdentity(this.#statePath, this.#maxStateBytes);
    }
    // A payload is cached only under the stamp seen both before and after its read: a commit
    // landing during the parse must not label the older payload with the newer file's stamp.
    // The label is the parsed payload's own canonical readGeneration, never a separately observed
    // marker, so an older payload can never carry a newer commit's generation.
    for (let attempt = 0; ; attempt++) {
      let readable = false;
      // Taken before the canonical read: any record for a later generation lies past it,
      // so the next change is followed by offset instead of rescanning the whole journal.
      const journalCursor = journalCursorOf(this.root);
      const state = readState(this.#statePath, this.#maxStateBytes, true, () => { readable = true; });
      if (this.#cacheState(state, before, readable, beforeIdentity)) {
        if (journalCursor) this.#stateCache!.journalCursor = journalCursor;
        rememberReadSnapshot(path.resolve(this.#statePath), this.#stateCache!);
        return state;
      }
      const next = statStamp(this.#statePath);
      if (attempt >= 2 || next === undefined) {
        this.#stateCache = undefined;                   // served once, never cached or stamped
        return state;
      }
      before = next;
      beforeIdentity = stateReadIdentity(this.#statePath, this.#maxStateBytes);
    }
  }

  // Under the lock (writes) no expected stamp is needed; lock-free reads pass the pre-read stamp.
  // The entry is labelled with the payload's own canonical generation.
  #cacheState(state: MeshStateFile, expectedStamp: string | undefined, canonicalReadable = true, expectedIdentity?: string): boolean {
    try {
      const stat = fs.statSync(this.#statePath);
      if (expectedStamp !== undefined && stampOf(stat) !== expectedStamp) return false;
      const identity = stateReadIdentity(this.#statePath, this.#maxStateBytes);
      // Pin both physical endpoints: a same-marker replacement during a full parse must
      // never label older bytes with the replacement's identity, even when the stat repeats.
      if (expectedIdentity !== undefined && identity !== expectedIdentity) return false;
      this.#stateCache = {
        device: stat.dev,
        inode: stat.ino,
        size: stat.size,
        modifiedAt: stat.mtimeMs,
        stamp: stampOf(stat),
        parsedAt: Date.now(),
        state,
        generation: generationOf(state),
        identity,
        canonicalReadable,
      };
      return true;
    } catch {
      this.#stateCache = undefined;
      return false;
    }
  }

  #validateKey(key: string): void {
    const unsafeSegment = key
      .split(/[/:]/)
      .some(
        (segment) =>
          segment === "__proto__" || segment === "prototype" || segment === "constructor",
      );
    if (!KEY_PATTERN.test(key) || unsafeSegment) {
      throw new Error(`Invalid Fabric mesh key: ${key}`);
    }
  }
}

// smarty-dev#6477 L4b: the backend migration tool (backend-migration.ts) imports and exports
// state.json through this exact decoder and encoder.
export { readState as decodeMeshStateFile, encodeState as encodeMeshStateFile, type MeshStateFile };
