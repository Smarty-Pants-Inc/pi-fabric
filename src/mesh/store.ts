import { createCommitStats } from "./commit-stats.js";
import { retryDelayMs } from "../core/retry-backoff.js";
import { copyFabricPrincipal, type FabricPrincipal } from "../fabric-provenance.js";
import type { MeshLockProtocol } from "../config.js";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { ownProcessIncarnation, processIncarnation, validProcessIncarnation, readFileRetrying, writeFileAtomic, syncPathNamespace, MeshLockTimeoutError } from "../core/atomic-write.js";
export { MeshLockTimeoutError } from "../core/atomic-write.js";
import { readJsonlPage } from "../log-tail.js";
import { MeshArchive, MeshArchiveLookupUnavailableError, type MeshArchiveEntry } from "./archive.js";
import { captureStoragePut, captureStorageDelete, storageRevision } from "../verified/storage.js";

export interface MeshIdentity {
  id: string;
  name: string;
  kind: "main" | "actor" | "agent";
  sessionId?: string;
  /** Set only by the admitting mesh bridge, after its sender/ownership checks. */
  verified?: "bridge";
}

export interface MeshEvent {
  /** Runtime-captured originating principal; not an event.data field. */
  principal?: FabricPrincipal | undefined;
  /** Recorded at publication, never reconstructed from retained event payloads. */
  verification?: "mesh" | "bridge";
  id: string;
  sequence: number;
  /** Host-only once-publication identity; never accepted from the public mesh provider. */
  dedupeKey?: string;
  topic: string;
  kind: string;
  from: MeshIdentity;
  to?: string;
  text?: string;
  data?: unknown;
  createdAt: number;
}

export interface MeshTailResult {
  events: MeshEvent[];
  nextOffset: number;
  /** The cursor just past each event, from the same read (so a reader can stop at any event). */
  cursors?: number[];
}

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
}

export interface MeshReadOptions {
  /** Read and parse the canonical file on every call, without reusing a cached snapshot. */
  fresh?: boolean;
  /** Reuse one already-captured canonical state for a multi-namespace scan. */
  snapshot?: object;
}

export interface MeshStoreOptions {
  /** Captured at construction, never reloaded. Defaults to B68-compatible protocol 1. */
  lockProtocol?: MeshLockProtocol;
  maxEventLogBytes?: number;
  retainedEventLogBytes?: number;
  maxStateBytes?: number;
  maxStateTombstones?: number;
  lockTimeoutMs?: number;
  /** Grace for an empty ownerless directory; recorded live owners never expire. Default 30 s. */
  staleLockMs?: number;
  /**
   * Reads (get, list, listAll) reuse the last parsed state for up to this long, even when
   * another process has rewritten the file since. Every write still reads the file fresh
   * under the lock and checks versions, and a store sees its own writes at once. 0 (the
   * default) re-reads whenever the file changed.
   */
  readCacheMs?: number;
  /** A live turn/pending operation bypasses the idle reuse window on demand. */
  readActive?: () => boolean;
}

// Capture the opt-in once at process startup/module load: no timer, key classification,
// counters, extra serialization, filesystem work or per-commit environment lookup when off.
const commitStats = createCommitStats();

// Opt-in commit diagnostics: no values or stacks are collected on the normal path.
// Capture before entering the async lock so the actual writer survives the await boundary.
const commitTraceCaller = (): string[] | undefined => process.env.PI_FABRIC_COMMIT_TRACE
  ? new Error().stack?.split("\n").slice(2, 10).map(line => line.trim()) : undefined;

const TOPIC_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
const KEY_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,255}$/;
const LOCK_TIMEOUT_MS = 10_000;
const STALE_LOCK_MS = 30_000;
const DEFAULT_MAX_EVENT_LOG_BYTES = 64 * 1024 * 1024;
const DEFAULT_RETAINED_EVENT_LOG_BYTES = 16 * 1024 * 1024;
const DEFAULT_MAX_STATE_BYTES = 32 * 1024 * 1024;
// ponytail: every tombstone is rewritten with the whole shared state on every write, and read
// by every process (smarty-dev#251, dev1 load P0: 4,787 tombstones were 40% of a 2.3 MB file).
// The persistent revision clock makes eviction safe: an evicted key is recreated above every
// earlier revision, and a stale compare-and-swap still conflicts. A key re-claimed with
// ifVersion 0 after eviction needs its id replayed; live claimers use fresh ids, and control
// commands are rejected once past their deadline.
const DEFAULT_MAX_STATE_TOMBSTONES = 1_000;
/**
 * Read-cache age for non-fresh reads in a Fabric runtime and its resident host. Fresh protocol
 * decisions always read canonical state (smarty-dev#2355); ordinary polls reuse a recent parse
 * (smarty-dev#251: ~50 processes previously parsed every change, about 10 times a second).
 * Idle observers coalesce for 5 s by default (mesh.idleReadCoalesceMs). Active turns and pending
 * Main messages bypass the window; CAS, ownership and delivery still request canonical freshness.
 * This is a reader policy only: no on-disk format or writer cadence change (mixed fleets).
 */
export const RUNTIME_MESH_READ_CACHE_MS = 5_000;
const EVENT_READ_PAGE_BYTES = 4 * 1024 * 1024;
const EVENT_READ_CHUNK_BYTES = 64 * 1024;
// Line ends remembered from recent read({ after }) scans: enough for every reader near the log head.
const READ_HINT_LINES = 128;
const CURSOR_OFFSET_BASE = 2 ** 32;
/** A tail cursor's live-log generation: it changes when the log is rewritten. */
export const meshCursorGeneration = (cursor: number): number => Math.floor(cursor / CURSOR_OFFSET_BASE);
/** The cursor at the start of a generation's log. */
export const meshCursorAtStart = (generation: number): number => generation * CURSOR_OFFSET_BASE;

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const errorCode = (error: unknown): string | undefined =>
  error instanceof Error && "code" in error && typeof error.code === "string"
    ? error.code
    : undefined;

const PROCESS_STATES: Record<string, string> = {
  R: "running", S: "sleeping", D: "uninterruptible I/O wait", T: "stopped", t: "stopped by tracer",
  Z: "zombie", X: "dead", I: "idle",
};

// Names the holder in a lock timeout. A live holder is never taken over (a resumed
// holder could commit stale state), so a stuck one must be found and restarted from
// outside; tonight's stopped holder (smarty-dev#266) only showed up as expired sessions.
// ponytail: the process state comes from Linux /proc; other platforms report the PID only.
const describeLockHolder = (ownerPath: string): string => {
  let owner: string;
  try {
    owner = fs.readFileSync(ownerPath, "utf8");
  } catch {
    return " (lock directory has no owner record)";
  }
  const [, pidText, createdText] = owner.trim().split("\n");
  const pid = Number(pidText);
  if (!Number.isSafeInteger(pid) || pid <= 0) return " (lock owner record is unreadable)";
  const createdAt = Number(createdText);
  const held = Number.isFinite(createdAt) ? ` for ${Math.max(0, Math.round((Date.now() - createdAt) / 1000))} s` : "";
  if (!processAlive(pid)) return ` held by pid ${pid} (not running)${held}`;
  let state = "";
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const code = stat.slice(stat.lastIndexOf(")") + 2).charAt(0);
    if (code) state = `, state ${code}${PROCESS_STATES[code] ? ` ${PROCESS_STATES[code]}` : ""}`;
  } catch {
    // No /proc: report the PID only.
  }
  return ` held by pid ${pid} (alive${state})${held}`;
};

const processAlive = (pid: number): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // Only the native no-such-process result proves death. Permission denial and
    // unexpected/unknown probe failures must not authorize detaching a live holder.
    return errorCode(error) !== "ESRCH";
  }
};

const jsonClone = <T>(value: T): T => {
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
// Fresh authoritative payload reads always parse the canonical file: legacy writers can copy an existing marker
// unchanged, and repeated metadata plus that marker cannot prove the payload unchanged.
// Nonfresh expired reads re-parse markerless files; copied-marker, same-stat rewrites remain
// outside the nonfresh cache's change detection (smarty-dev#2355).
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
interface EncodedStateEntry {
  version: number | undefined;
  member: Buffer;
  entry: Buffer;
}
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
        const serialized = JSON.stringify(state.entries[key]);
        if (serialized === undefined) continue;
        const encodedKey = JSON.stringify(key);
        const bytes = Buffer.from(`${encodedKey}:${serialized}`, "utf8");
        // The canonical member and the hash's entry-only view share the same UTF-8 bytes.
        const entry = bytes.subarray(Buffer.byteLength(encodedKey, "utf8") + 1);
        entries.set(key, { version: state.entries[key]?.version, member: bytes, entry });
        if (!first) fields.push(comma);
        first = false;
        fields.push(bytes);
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
  readState(path.join(root, "state.json"), maxBytes, false);
};

const atomicWrite = (filePath: string, value: unknown, maxBytes = Number.POSITIVE_INFINITY): void => {
  // Compact: the file is rewritten under the mesh lock on every write, and indenting made it 22%
  // larger and slower to serialize (smarty-dev#2004).
  const serialized = JSON.stringify(value);
  if (Buffer.byteLength(serialized, "utf8") > maxBytes) {
    throw new Error(`Fabric mesh state exceeds ${maxBytes} bytes`);
  }
  writeFileAtomic(filePath, serialized);
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

export class MeshBatchConflictError extends Error {
  constructor(readonly key: string, readonly expected: number, readonly found: number) {
    super(`Mesh compare-and-swap failed for ${key}: expected version ${expected}, found ${found}`);
  }
}

interface MeshDedupeIntent {
  dedupeKey: string;
  reservedSequence: number;
  eventId: string;
  /** Byte offset captured before the live append; it makes recovery a direct read. */
  liveOffset: number;
  /** Captured before append: removing/changing archive configuration cannot authorize retry. */
  archiveDir?: string;
}

export class MeshDedupeRecoveryError extends Error {
  readonly retryable = true;
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "MeshDedupeRecoveryError";
  }
}

export class MeshStore {
  readonly #eventsPath: string;
  readonly #statePath: string;
  readonly #counterPath: string;
  readonly #generationPath: string;
  readonly #lockPath: string;
  readonly #lockProtocol: MeshLockProtocol;
  readonly #signalPath: string;
  /** Per parsed state: prefix selections (bounded) and namespace digests. Keyed by identity. */
  #memo = new WeakMap<MeshStateFile, { selections: Map<string, MeshStateEntry[]>; digests: Map<string, string> }>();
  /** Writer-only bytes from one commit; reusable only after a fresh, exact canonical-text match. */
  #writeEncodings: { serialized: string; entries: Map<string, EncodedStateEntry> } | undefined;
  /** The last full signal index parsed, keyed by its unique generation: one object, bounded. */
  #signalIndex: { generation: string; stamp: string; namespaces: Record<string, unknown> } | undefined;
  readonly #maxEventLogBytes: number;
  readonly #retainedEventLogBytes: number;
  readonly #maxStateBytes: number;
  readonly #maxStateTombstones: number;
  readonly #lockTimeoutMs: number;
  readonly #staleLockMs: number;
  readonly #readCacheMs: number;
  readonly #readActive: (() => boolean) | undefined;
  /**
   * Line ends (sequence, offset) that recent read({ after }) scans passed, by rising sequence. A
   * read starts at the last one at or below its cursor. One remembered point was not enough:
   * several readers at one cursor (a host's lifecycle subscriptions after a new event) moved it
   * past each other, and all but the first scanned the whole log again (smarty-dev#557).
   */
  #readHints: { generation: number; inode: number; lines: Array<{ sequence: number; offset: number }> } | undefined;
  #stateCache:
    | {
      device: number; inode: number; size: number; modifiedAt: number; stamp: string; parsedAt: number; state: MeshStateFile;
      /** The payload's own canonical readGeneration; undefined for a legacy (no-marker) payload. */
      generation: string | undefined;
    }
    | undefined;
  #oldestLive: { identity: string; sequence: number | undefined } | undefined;

  constructor(
    readonly root: string,
    readonly maxEventBytes: number,
    readonly maxReadEvents: number,
    options: MeshStoreOptions = {},
  ) {
    // ponytail: keep this tiny validation local; importing config's runtime adds eager graph edges.
    const lockProtocol = options.lockProtocol === undefined ? 1 : options.lockProtocol;
    if (lockProtocol !== 1 && lockProtocol !== 2) throw new Error("mesh.lockProtocol must be 1 or 2");
    this.#lockProtocol = lockProtocol;
    this.#eventsPath = path.join(root, "events.jsonl");
    this.#statePath = path.join(root, "state.json");
    this.#counterPath = path.join(root, "sequence");
    this.#generationPath = path.join(root, "generation");
    this.#lockPath = path.join(root, ".lock");
    this.#signalPath = path.join(root, "state.read-signal.json");
    this.#maxEventLogBytes = Math.min(
      CURSOR_OFFSET_BASE - 1,
      Math.max(maxEventBytes + 2, Math.floor(options.maxEventLogBytes ?? DEFAULT_MAX_EVENT_LOG_BYTES)),
    );
    this.#retainedEventLogBytes = Math.min(
      this.#maxEventLogBytes - 1,
      Math.max(
        maxEventBytes + 1,
        Math.floor(options.retainedEventLogBytes ?? DEFAULT_RETAINED_EVENT_LOG_BYTES),
      ),
    );
    this.#maxStateBytes = Math.max(
      maxEventBytes * 2,
      Math.floor(options.maxStateBytes ?? DEFAULT_MAX_STATE_BYTES),
    );
    this.#maxStateTombstones = Math.max(
      1,
      Math.floor(options.maxStateTombstones ?? DEFAULT_MAX_STATE_TOMBSTONES),
    );
    this.#lockTimeoutMs = Math.max(100, Math.floor(options.lockTimeoutMs ?? LOCK_TIMEOUT_MS));
    this.#staleLockMs = Math.max(100, Math.floor(options.staleLockMs ?? STALE_LOCK_MS));
    this.#readCacheMs = Math.max(0, Math.floor(options.readCacheMs ?? 0));
    this.#readActive = options.readActive;
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  }

  get lockProtocol(): MeshLockProtocol {
    return this.#lockProtocol;
  }

  /** The reuse window of reads (MeshStoreOptions.readCacheMs), for readers of files beside the state. */
  get readCacheMs(): number {
    return this.#readActive?.() ? 0 : this.#readCacheMs;
  }

  /** Time until an idle observer may revalidate; hits do not slide this deadline. */
  get readCacheRemainingMs(): number {
    return this.#stateCache ? Math.max(0, this.readCacheMs - (Date.now() - this.#stateCache.parsedAt)) : 0;
  }

  #dedupePath(dedupeKey: string, suffix: string): string {
    return path.join(this.root, "event-receipts", createHash("sha256").update(dedupeKey).digest("hex") + suffix);
  }

  #confirmEventFile(file: string): void {
    const fd = fs.openSync(file, process.platform === "win32" ? "r+" : "r");
    try { fs.fsyncSync(fd); syncPathNamespace(file, fs.fstatSync(fd)); } finally { fs.closeSync(fd); }
  }

  #readDedupeReceipt(dedupeKey: string): MeshEvent | undefined {
    const file = this.#dedupePath(dedupeKey, ".json");
    let text: string;
    try { text = fs.readFileSync(file, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
    const event = JSON.parse(text) as MeshEvent;
    if (event.dedupeKey !== dedupeKey || typeof event.id !== "string" || !Number.isSafeInteger(event.sequence)) {
      throw new Error("Invalid event publication receipt");
    }
    // A visible rename whose final barrier failed is not yet a durable receipt.
    this.#confirmEventFile(file);
    return event;
  }

  #removeDedupeIntent(file: string): void {
    fs.rmSync(file, { force: true });
    syncPathNamespace(path.dirname(file));
  }

  /** Read the exact live line named by an intent; archive fallback is a separate direct lookup. */
  #readEventAtIntent(intent: MeshDedupeIntent): MeshEvent | undefined {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const stat = fs.fstatSync(descriptor);
      if (intent.liveOffset >= stat.size) return undefined;
      const bytes = Buffer.allocUnsafe(Math.min(this.maxEventBytes + 1, stat.size - intent.liveOffset));
      const count = fs.readSync(descriptor, bytes, 0, bytes.length, intent.liveOffset);
      const newline = bytes.subarray(0, count).indexOf(0x0a);
      if (newline < 0) return undefined; // A partial append never committed.
      const event = JSON.parse(bytes.subarray(0, newline).toString("utf8")) as MeshEvent;
      return event.sequence === intent.reservedSequence && event.id === intent.eventId &&
        event.dedupeKey === intent.dedupeKey ? event : undefined;
    } catch (error) {
      if (errorCode(error) === "ENOENT" || error instanceof SyntaxError) return undefined;
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #settleDedupeIntent(file: string, dedupeKey?: string, archive?: MeshArchive): MeshEvent | undefined {
    let text: string;
    try { text = fs.readFileSync(file, "utf8"); }
    catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
    const intent = JSON.parse(text) as MeshDedupeIntent;
    if (typeof intent.dedupeKey !== "string" || !intent.dedupeKey ||
        (dedupeKey !== undefined && intent.dedupeKey !== dedupeKey) ||
        !Number.isSafeInteger(intent.reservedSequence) || intent.reservedSequence < 1 ||
        typeof intent.eventId !== "string" || !intent.eventId ||
        !Number.isSafeInteger(intent.liveOffset) || intent.liveOffset < 0 ||
        (intent.archiveDir !== undefined && (typeof intent.archiveDir !== "string" || !path.isAbsolute(intent.archiveDir))) ||
        file !== this.#dedupePath(intent.dedupeKey, ".pending.json")) {
      throw new Error("Invalid event publication intent");
    }
    // A crash may also leave both the receipt and its intent. Never replace a receipt.
    const prior = this.#readDedupeReceipt(intent.dedupeKey);
    const live = prior ? undefined : this.#readEventAtIntent(intent);
    let event = prior ?? live;
    if (!event && intent.archiveDir !== undefined && archive?.dir !== intent.archiveDir) {
      throw new MeshDedupeRecoveryError(`Cannot recover dedupe intent ${intent.dedupeKey}: event archive configuration is unavailable`);
    }
    if (!event && !prior && archive) {
      let entry: (MeshArchiveEntry & { committed: boolean }) | undefined;
      try { entry = archive.lookupEntry(intent.reservedSequence); }
      catch (error) {
        if (error instanceof MeshArchiveLookupUnavailableError) {
          throw new MeshDedupeRecoveryError(`Cannot recover dedupe intent ${intent.dedupeKey}: event archive lookup is unavailable`, { cause: error });
        }
        throw error;
      }
      const archived = entry?.event;
      if (archived?.id === intent.eventId && archived.dedupeKey !== intent.dedupeKey) {
        throw new MeshDedupeRecoveryError(`Cannot recover dedupe intent ${intent.dedupeKey}: reserved archive key does not match`);
      }
      // A different archived identity positively proves this reservation is absent. Never
      // abort that other event; leave its archive visibility and index intact.
      if (entry && archived?.id === intent.eventId) {
        this.#repairEventLog();
        const lastLive = this.#readLastEventSequence();
        if (lastLive < archived.sequence) {
          // An archive append is not a publication. Restore its exact bytes before issuing
          // a receipt, and update the anchor first so another death cannot append it twice.
          let liveOffset = 0;
          try { liveOffset = fs.statSync(this.#eventsPath).size; }
          catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
          writeFileAtomic(file, JSON.stringify({ ...intent, liveOffset }), { durable: true });
          fs.appendFileSync(this.#eventsPath, `${entry.line}\n`, { encoding: "utf8", mode: 0o600 });
          this.#confirmEventFile(this.#eventsPath);
        }
        // A false sidecar is not non-publication evidence: an old writer can recover a
        // completed live append without updating it, then compact away the live anchor.
        // An overtaken archive-only append is indistinguishable. Prefer its one archive
        // delivery over loss; never append behind the live sequence or publish a new id.
        archive.confirmLive(archived.sequence, archived.id);
        const pending = archive.pending();
        // Receipt recovery is direct metadata work. Leave closed-day sealing to the next
        // ordinary archive append, never scan history just to resolve this intent.
        if (pending?.id === archived.id && pending.sequence === archived.sequence) archive.commit(pending, false);
        event = archived;
      }
    }
    if (event && !prior) {
      if (live) {
        this.#confirmEventFile(this.#eventsPath);
        archive?.confirmLive(live.sequence, live.id);
        const pending = archive?.pending();
        if (pending?.id === live.id && pending.sequence === live.sequence) archive!.commit(pending, false);
      }
      writeFileAtomic(this.#dedupePath(intent.dedupeKey, ".json"), JSON.stringify(event), { durable: true });
    }
    this.#removeDedupeIntent(file);
    return event;
  }

  /** Under the mesh lock, settle all intents before a rewrite can invalidate byte offsets. */
  #settleDedupeIntents(archive?: MeshArchive): void {
    const directory = path.join(this.root, "event-receipts");
    let names: string[];
    try { names = fs.readdirSync(directory); }
    catch (error) { if (errorCode(error) === "ENOENT") return; throw error; }
    for (const name of names.filter(entry => /^[a-f0-9]{64}\.pending\.json$/.test(entry))) {
      this.#settleDedupeIntent(path.join(directory, name), undefined, archive);
    }
  }

  async publish(input: {
    topic: string;
    /** Host-only durable publication receipt (alarms and inbox disposition receipts). */
    dedupeKey?: string;
    kind?: string;
    from: MeshIdentity;
    to?: string;
    text?: string;
    /** Host-only cancellation fence, checked under the lock before admission. */
    signal?: AbortSignal | undefined;
    /** Host-only relay metadata. The public provider never forwards args.principal. */
    principal?: FabricPrincipal | undefined;
    /** A function receives the commit time, under the lock (smarty-dev#816). */
    data?: unknown;
  }): Promise<MeshEvent> {
    this.#validateTopic(input.topic);
    if (input.to !== undefined && !input.to.trim()) throw new Error("Mesh recipient is empty");
    const principal = copyFabricPrincipal(input.principal);
    const stamp = typeof input.data === "function" ? input.data as (createdAt: number) => unknown : undefined;
    const fixedData = stamp || input.data === undefined ? undefined : jsonClone(input.data);
    return this.#withLock(() => {
      input.signal?.throwIfAborted();
      const receiptPath = input.dedupeKey ? this.#dedupePath(input.dedupeKey, ".json") : undefined;
      const intentPath = input.dedupeKey ? this.#dedupePath(input.dedupeKey, ".pending.json") : undefined;
      if (input.dedupeKey) {
        const prior = this.#readDedupeReceipt(input.dedupeKey);
        if (prior) {
          // Receipt-before-unlink crash: the receipt is authoritative; finish cleanup.
          if (fs.existsSync(intentPath!)) this.#removeDedupeIntent(intentPath!);
          return prior;
        }
      }
      let archive: MeshArchive | undefined;
      try { archive = MeshArchive.fromRoot(this.root); }
      catch (error) {
        if (intentPath && fs.existsSync(intentPath)) throw new MeshDedupeRecoveryError("Event archive configuration is unavailable during dedupe recovery", { cause: error });
        throw error;
      }
      this.#repairEventLog();
      // Recover the whole reboot suffix before any one intent can advance the live horizon.
      // In the same boot, defer ordinary pending cutback until the exact retry has settled.
      // New keys still take only the normal recovery path, with no event-history lookup.
      if (input.dedupeKey) {
        if (archive && fs.existsSync(intentPath!)) {
          try { this.#recoverArchive(archive, true); }
          catch (error) {
            throw new MeshDedupeRecoveryError("Event archive reboot recovery is unavailable during dedupe recovery", { cause: error });
          }
        }
        const prior = this.#settleDedupeIntent(intentPath!, input.dedupeKey, archive);
        if (prior) return prior;
      }
      if (archive) this.#recoverArchive(archive);
      const createdAt = Date.now();
      const eventData = stamp ? jsonClone(stamp(createdAt)) : fixedData;
      const sequence = Math.max(this.#readSequence(), this.#readLastEventSequence()) + 1;
      const event: MeshEvent = {
        id: randomUUID(),
        ...(input.dedupeKey ? { dedupeKey: input.dedupeKey } : {}),
        sequence,
        topic: input.topic,
        kind: input.kind?.trim() || "message",
        from: jsonClone(input.from),
        ...(principal ? { principal } : {}),
        // Old bridges only wrote data.bridge. It can veto a native attestation, but
        // arbitrary payload data cannot establish bridge verification or any authority.
        ...(input.from.verified === "bridge" ? { verification: "bridge" as const }
          : eventData && typeof eventData === "object" && "bridge" in eventData ? {}
          : { verification: "mesh" as const }),
        ...(input.to ? { to: input.to } : {}),
        ...(input.text !== undefined ? { text: input.text } : {}),
        ...(eventData !== undefined ? { data: eventData } : {}),
        createdAt,
      };
      const line = JSON.stringify(event);
      if (Buffer.byteLength(line, "utf8") > this.maxEventBytes) {
        throw new Error(`Mesh event exceeds ${this.maxEventBytes} bytes`);
      }
      // The counter is a reservation: a crash after it leaves a gap, never a reused sequence.
      // The archive holds the event durably before it goes live (smarty-dev#754); the live
      // append commits it. If either step fails, the event is cut back out of the archive.
      // ponytail: the archive's fdatasync (~15 ms on Dev1's NVMe) runs under the lock, so a
      // burst of 160 publishes held other writers up to 1.3 s at 5x the fleet rate. If the
      // lock's held share matters (#816), sync after unlocking so concurrent syncs share a commit.
      atomicWrite(this.#counterPath, sequence);
      let liveOffset = 0;
      try { liveOffset = fs.statSync(this.#eventsPath).size; }
      catch (error) { if (errorCode(error) !== "ENOENT") throw error; }
      if (intentPath) {
        // A durable negative lookup exists before the intent. Only begin() can replace it
        // with the synced archive line address, before any live append.
        archive?.reserveLookup(sequence);
        // This is the crash fence: the intent is durable before the live append begins.
        writeFileAtomic(intentPath, JSON.stringify({
          dedupeKey: input.dedupeKey!, reservedSequence: sequence, eventId: event.id, liveOffset,
          ...(archive ? { archiveDir: archive.dir } : {}),
        } satisfies MeshDedupeIntent), { durable: true });
      }
      const pending = archive?.begin({ event, line });
      // Test-only process-death fence: unlike an append exception, no rollback can run.
      if (receiptPath && pending && process.env.PI_FABRIC_TEST_CRASH_AFTER_ARCHIVE_BEGIN === "1") process.kill(process.pid, "SIGKILL");
      try {
        fs.appendFileSync(this.#eventsPath, `${line}\n`, { encoding: "utf8", mode: 0o600 });
      } catch (error) {
        if (pending) archive!.rollback(pending);
        throw error;
      }
      // This distinct fence leaves the live event complete but the sidecar unconfirmed.
      if (receiptPath && pending && process.env.PI_FABRIC_TEST_CRASH_BEFORE_ARCHIVE_COMMIT === "1") process.kill(process.pid, "SIGKILL");
      if (pending) archive!.commit(pending);
      // Test-only crash fence for the installed-Pi recovery proof; production never sets this.
      if (receiptPath && process.env.PI_FABRIC_TEST_CRASH_AFTER_LIVE_APPEND === "1") process.kill(process.pid, "SIGKILL");
      if (receiptPath) {
        this.#confirmEventFile(this.#eventsPath);
        writeFileAtomic(receiptPath, JSON.stringify(event), { durable: true });
        if (intentPath) this.#removeDedupeIntent(intentPath);
      }
      this.#compactEventLog();
      return event;
    });
  }

  read(
    input: {
      after?: number;
      topic?: string;
      to?: string;
      limit?: number;
    } = {},
  ): MeshEvent[] {
    if (input.topic !== undefined) this.#validateTopic(input.topic);
    const limit = Math.max(1, Math.min(Math.floor(input.limit ?? 100), this.maxReadEvents));
    const after = input.after === undefined ? undefined : Math.max(0, Math.floor(input.after));
    const events =
      after === undefined
        ? this.#readRecentEvents(input, limit)
        : this.#readArchivedAfter(after, input, limit) ?? this.#readEventsAfter(after, input, limit);
    return events.map((event) => jsonClone(event));
  }

  /**
   * The first committed event after a sequence. From the archive's first sequence on, only the
   * archive answers: it holds each event before the event goes live, so it is one coherent
   * source across a live-log rewrite (smarty-dev#754). Below it, and in a store without the
   * archive, the live log answers: it is the only source there, so an event a rewrite cuts
   * from that range is gone either way.
   */
  nextEventAfter(after: number): MeshEvent | undefined {
    const archive = MeshArchive.fromRoot(this.root);
    const first = archive?.firstSequence();
    if (!archive || first === undefined) return this.#cloned(this.#readEventsAfter(after, {}, 1)[0]);
    if (after + 1 < first) {
      // ponytail: not expected in practice (a newly set archive backfills the whole live log),
      // but the answer stays right if the archive ever starts above a live event.
      const live = this.#readEventsAfter(after, {}, 1)[0];
      if (live && live.sequence < first) return this.#cloned(live);
    }
    return this.#cloned(archive.readAfter(Math.max(after, first - 1), this.#readLastEventSequence(), () => true, 1)[0]);
  }

  #cloned(event: MeshEvent | undefined): MeshEvent | undefined {
    return event ? jsonClone(event) : undefined;
  }

  // A cursor older than the live log reads the archive, which holds every event since it was
  // set (smarty-dev#754). The oldest live sequence changes only when the log is rewritten.
  #readArchivedAfter(after: number, input: { topic?: string; to?: string }, limit: number): MeshEvent[] | undefined {
    let identity: string;
    try {
      const stat = fs.statSync(this.#eventsPath);
      identity = `${this.#readGeneration()}:${stat.ino}`;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    }
    if (this.#oldestLive?.identity !== identity) this.#oldestLive = { identity, sequence: this.oldestSequence() };
    const oldest = this.#oldestLive.sequence;
    if (oldest === undefined || after + 1 >= oldest) return undefined;
    const archived = MeshArchive.fromRoot(this.root)
      ?.readAfter(after, this.#readLastEventSequence(), (event) => this.#eventMatches(event, input), limit, input.topic);
    return archived?.length ? archived : undefined;
  }

  // Before a publish, under the lock, bring the live log and the archive level. A publish that
  // stopped between its archive append and its commit is cut back out; if its event did go
  // live, the catch-up below archives it again from the live log. So do events that a store
  // without the archive appended (an older Fabric, or before the archive was set).
  #recoverArchive(archive: MeshArchive, rebootOnly = false): void {
    const recovery = archive.recover(this.#readLastEventSequence(), rebootOnly);
    if (rebootOnly && !recovery.rebooted) return;
    if (recovery.rebooted) {
      // A power loss took live appends whose archive lines were synced: they go live again,
      // synced this time, before anything else can take their sequences.
      const last = recovery.promote.at(-1);
      if (last) {
        fs.appendFileSync(this.#eventsPath, recovery.promote.map(({ line }) => `${line}\n`).join(""), { encoding: "utf8", mode: 0o600 });
        const descriptor = fs.openSync(this.#eventsPath, "r+");
        try {
          fs.fdatasyncSync(descriptor);
        } finally {
          fs.closeSync(descriptor);
        }
        atomicWrite(this.#counterPath, Math.max(this.#readSequence(), last.event.sequence));
      }
      archive.recovered(last, recovery.promote);
    }
    const archived = archive.head()?.sequence ?? 0;
    if (archived < this.#readLastEventSequence()) archive.catchUp(this.#liveEntriesAfter(archived));
  }

  // Live lines after a sequence, oldest first. It reads back from the end, so the usual one or
  // two unarchived events cost one chunk.
  #liveEntriesAfter(after: number): MeshArchiveEntry[] {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      let position = fs.fstatSync(descriptor).size;
      let carry = Buffer.alloc(0);
      const entries: MeshArchiveEntry[] = [];
      while (position > 0) {
        const start = Math.max(0, position - EVENT_READ_CHUNK_BYTES);
        const chunk = Buffer.allocUnsafe(position - start);
        fs.readSync(descriptor, chunk, 0, chunk.length, start);
        position = start;
        const text = Buffer.concat([chunk, carry]);
        // Before the first newline, a line may continue in the earlier chunk.
        const split = position === 0 ? 0 : text.indexOf(0x0a) + 1;
        if (split === 0 && position > 0) {
          carry = text;
          continue;
        }
        carry = text.subarray(0, split);
        const lines = text.subarray(split).toString("utf8").split("\n");
        for (let index = lines.length - 1; index >= 0; index--) {
          const line = lines[index];
          if (!line) continue;
          let event: MeshEvent;
          try {
            event = JSON.parse(line) as MeshEvent;
          } catch {
            continue;
          }
          if (typeof event.sequence !== "number") continue;
          if (event.sequence <= after) return entries.reverse();
          entries.push({ event, line });
        }
      }
      return entries.reverse();
    } catch (error) {
      if (errorCode(error) === "ENOENT") return [];
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  /**
   * The sequence of the oldest event still in the log, read from its first line. Undefined when
   * it cannot tell (no log, or no readable event near its start): callers must then keep
   * anything that depends on an event still being replayable.
   */
  oldestSequence(): number | undefined {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const readBytes = Math.min(fs.fstatSync(descriptor).size, this.maxEventBytes + 1);
      const head = Buffer.allocUnsafe(readBytes);
      const bytesRead = fs.readSync(descriptor, head, 0, readBytes, 0);
      const text = head.subarray(0, bytesRead).toString("utf8");
      for (const line of text.slice(0, text.lastIndexOf("\n") + 1).split("\n")) {
        try {
          const parsed = JSON.parse(line) as { sequence?: unknown };
          if (typeof parsed.sequence === "number" && Number.isSafeInteger(parsed.sequence)) return parsed.sequence;
        } catch { /* skip a malformed line */ }
      }
      return undefined;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return undefined;
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  latestSequence(): number {
    return Math.max(this.#readSequence(), this.#readLastEventSequence());
  }

  latestOffset(): number {
    return this.latestCursor().cursor;
  }

  /** Tail offset and sequence boundary from the same file handle, never the reservation/archive head. */
  latestCursor(): { cursor: number; last?: { sequence: number; id: string } } {
    const generation = this.#readGeneration();
    let descriptor: number | undefined;
    let completeOffset = 0;
    let last: { sequence: number; id: string } | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size > 0) {
        const lastByte = Buffer.allocUnsafe(1);
        fs.readSync(descriptor, lastByte, 0, 1, size - 1);
        if (lastByte[0] === 0x0a) {
          completeOffset = size;
        } else {
          const readBytes = Math.min(size, this.maxEventBytes + 1);
          const tail = Buffer.allocUnsafe(readBytes);
          fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
          const newline = tail.lastIndexOf(0x0a);
          completeOffset = newline >= 0 ? size - readBytes + newline + 1 : 0;
        }
      }
      if (completeOffset > 0) {
        // Bounded startup work: just the last complete line, using the captured offset even
        // if another publisher has since appended. A partial append is never an anchor.
        const readBytes = Math.min(completeOffset, this.maxEventBytes + 2);
        const tail = Buffer.allocUnsafe(readBytes);
        const bytesRead = fs.readSync(descriptor, tail, 0, readBytes, completeOffset - readBytes);
        if (bytesRead === readBytes) {
          const lineStart = tail.lastIndexOf(0x0a, tail.length - 2) + 1;
          if (lineStart > 0 || readBytes === completeOffset) {
            try {
              const event = JSON.parse(tail.subarray(lineStart, tail.length - 1).toString("utf8")) as MeshEvent;
              if (Number.isSafeInteger(event.sequence) && event.sequence > 0 && typeof event.id === "string") {
                last = { sequence: event.sequence, id: event.id };
              }
            } catch { /* unreadable boundary: a saved lastless cursor reconciles conservatively */ }
          }
        }
      }
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
    // Sequence zero is a start boundary, not a later reservation that could skip unread work.
    if (completeOffset === 0) last = { sequence: 0, id: "" };
    return { cursor: this.#encodeCursor(generation, completeOffset), ...(last ? { last } : {}) };
  }

  tail(cursor: number, limit = 100): MeshTailResult {
    const boundedLimit = Math.max(1, Math.min(Math.floor(limit), this.maxReadEvents));
    const generation = this.#readGeneration();
    const decoded = this.#decodeCursor(cursor);
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      let position = decoded.generation === generation ? Math.min(decoded.offset, size) : 0;
      if (position > 0) {
        const previousByte = Buffer.allocUnsafe(1);
        fs.readSync(descriptor, previousByte, 0, 1, position - 1);
        if (previousByte[0] !== 0x0a) position = 0;
      }
      if (position >= size) {
        return { events: [], nextOffset: this.#encodeCursor(generation, position) };
      }
      const chunkBytes = Math.min(
        size - position,
        Math.max(this.maxEventBytes + 1, EVENT_READ_PAGE_BYTES),
      );
      const buffer = Buffer.allocUnsafe(chunkBytes);
      const bytesRead = fs.readSync(descriptor, buffer, 0, chunkBytes, position);
      const events: MeshEvent[] = [];
      const cursors: number[] = [];
      let lineStart = 0;
      let consumed = 0;
      for (let index = 0; index < bytesRead; index++) {
        if (buffer[index] !== 0x0a) continue;
        const line = buffer.subarray(lineStart, index).toString("utf8").trim();
        lineStart = index + 1;
        consumed = lineStart;
        if (line) {
          try {
            const event = JSON.parse(line) as MeshEvent;
            if (typeof event.sequence === "number") {
              events.push(event);
              cursors.push(this.#encodeCursor(generation, position + consumed));
            }
          } catch { /* skip malformed mesh log line */ }
        }
        if (events.length >= boundedLimit) break;
      }
      return {
        events: events.map((event) => jsonClone(event)),
        nextOffset: this.#encodeCursor(generation, position + consumed),
        cursors,
      };
    } catch (error) {
      if (errorCode(error) === "ENOENT") {
        return { events: [], nextOffset: this.#encodeCursor(generation, 0) };
      }
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #readRecentEvents(
    input: { topic?: string; to?: string },
    limit: number,
  ): MeshEvent[] {
    let events: MeshEvent[] = [];
    let before: number | undefined;
    while (events.length < limit) {
      const page = readJsonlPage(
        this.#eventsPath,
        this.maxReadEvents,
        before,
        Math.max(this.maxEventBytes + 1, EVENT_READ_PAGE_BYTES),
      );
      const pageEvents: MeshEvent[] = [];
      for (const line of page.lines) {
        const parsed = line.parsed;
        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) continue;
        const event = parsed as MeshEvent;
        if (typeof event.sequence !== "number" || !this.#eventMatches(event, input)) continue;
        pageEvents.push(event);
      }
      events = [...pageEvents, ...events].slice(-limit);
      if (!page.hasMore || page.before === undefined || page.before === before) break;
      before = page.before;
    }
    return events;
  }

  #readEventsAfter(
    after: number,
    input: { topic?: string; to?: string },
    limit: number,
  ): MeshEvent[] {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const stat = fs.fstatSync(descriptor);
      const size = stat.size;
      const events: MeshEvent[] = [];
      // Sequences rise in log order (appends run under the lock), so every line before the
      // hint has a sequence at or below it and cannot match a read after it. Without this,
      // each read scanned the whole log (tens of MB on the fleet) from the start
      // (smarty-dev#557). A rotated log is a new file and generation, and the hint must end a line.
      const generation = this.#readGeneration();
      const hints = this.#readHints?.generation === generation && this.#readHints.inode === stat.ino
        ? this.#readHints.lines
        : [];
      let position = 0;
      for (let index = hints.length - 1; index >= 0; index--) {
        const hint = hints[index]!;
        if (hint.sequence > after) continue;
        if (hint.offset <= size && this.#endsLine(descriptor, hint.offset)) position = hint.offset;
        break;
      }
      const scanned: Array<{ sequence: number; offset: number }> = [];
      let lineChunks: Buffer[] = [];
      let lineBytes = 0;
      let skippingOversizedLine = false;
      let reachedLimit = false;

      const emitLine = (lineEnd?: number): void => {
        if (!skippingOversizedLine && lineBytes > 0) {
          const decoded = Buffer.concat(lineChunks, lineBytes).toString("utf8");
          const line = decoded.endsWith(String.fromCharCode(13)) ? decoded.slice(0, -1) : decoded;
          try {
            const event = JSON.parse(line) as MeshEvent;
            if (typeof event.sequence === "number" && lineEnd !== undefined) {
              scanned.push({ sequence: event.sequence, offset: lineEnd });
              if (scanned.length > 2 * READ_HINT_LINES) scanned.splice(0, scanned.length - READ_HINT_LINES);
            }
            if (
              typeof event.sequence === "number" &&
              event.sequence > after &&
              this.#eventMatches(event, input)
            ) {
              events.push(event);
              reachedLimit = events.length >= limit;
            }
          } catch { /* skip malformed mesh log line */ }
        }
        lineChunks = [];
        lineBytes = 0;
        skippingOversizedLine = false;
      };

      while (position < size && !reachedLimit) {
        const readLength = Math.min(EVENT_READ_CHUNK_BYTES, size - position);
        const chunk = Buffer.allocUnsafe(readLength);
        const bytesRead = fs.readSync(descriptor, chunk, 0, readLength, position);
        if (bytesRead <= 0) break;
        const chunkStart = position;
        position += bytesRead;
        const captured = chunk.subarray(0, bytesRead);
        let segmentStart = 0;
        while (segmentStart < captured.length && !reachedLimit) {
          const newline = captured.indexOf(0x0a, segmentStart);
          const segmentEnd = newline < 0 ? captured.length : newline;
          const segment = captured.subarray(segmentStart, segmentEnd);
          if (!skippingOversizedLine) {
            if (lineBytes + segment.length <= this.maxEventBytes) {
              if (segment.length > 0) lineChunks.push(segment);
              lineBytes += segment.length;
            } else {
              lineChunks = [];
              lineBytes = 0;
              skippingOversizedLine = true;
            }
          }
          if (newline < 0) break;
          emitLine(chunkStart + newline + 1);
          segmentStart = newline + 1;
        }
      }
      if (!reachedLimit && (lineBytes > 0 || skippingOversizedLine)) emitLine();
      if (scanned.length > 0) {
        const lines = new Map(hints.map((hint) => [hint.sequence, hint.offset]));
        for (const line of scanned) lines.set(line.sequence, line.offset);
        this.#readHints = {
          generation,
          inode: stat.ino,
          lines: [...lines].sort((left, right) => left[0] - right[0]).slice(-READ_HINT_LINES)
            .map(([sequence, offset]) => ({ sequence, offset })),
        };
      }
      return events;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return [];
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #endsLine(descriptor: number, offset: number): boolean {
    if (offset === 0) return true;
    const byte = Buffer.allocUnsafe(1);
    return fs.readSync(descriptor, byte, 0, 1, offset - 1) === 1 && byte[0] === 0x0a;
  }

  #eventMatches(event: MeshEvent, input: { topic?: string; to?: string }): boolean {
    if (input.topic !== undefined && event.topic !== input.topic) return false;
    if (input.to !== undefined && event.to !== input.to) return false;
    return true;
  }

  // fresh: read and parse the canonical file, bypassing all snapshot reuse, for a read that
  // decides a protocol step rather than a listing.
  get(key: string, options: MeshReadOptions = {}): MeshStateEntry | undefined {
    this.#validateKey(key);
    const state = options.fresh === true || options.snapshot === undefined
      ? this.#readCachedState(options.fresh === true)
      : options.snapshot as MeshStateFile;
    const entries = state.entries;
    return Object.hasOwn(entries, key) ? jsonClone(entries[key]) : undefined;
  }

  list(prefix = "", limit = 100): MeshStateEntry[] {
    const boundedLimit = Math.max(1, Math.min(Math.floor(limit), this.maxReadEvents));
    // Clone only the page: the dashboard lists the first 200 of the whole fleet state, and
    // cloning every entry to keep 200 was a large share of an idle Pi's CPU (smarty-dev#557).
    return this.#select(prefix, {}).slice(0, boundedLimit).map((entry) => jsonClone(entry));
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
    return this.#readCachedState(options.fresh === true);
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
    const state = options.snapshot !== undefined
      ? options.snapshot as MeshStateFile
      : (!fresh && this.#signalledState(prefix)) || this.#readCachedState(fresh);
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
  #signalledState(prefix: string): MeshStateFile | undefined {
    const cached = this.#stateCache;
    const namespace = keyNamespace(prefix);
    const readCacheMs = this.readCacheMs;
    if (!cached || !namespace || readCacheMs <= 0 || Date.now() - cached.parsedAt < readCacheMs) return undefined;
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
      return this.#signalIndex = { generation, stamp: signal.stamp, namespaces: namespaces as Record<string, unknown> };
    } catch {
      return undefined;
    } finally {
      closeQuietly(descriptor);
    }
  }

  // The canonical state.json's commit generation from its 64-byte header: open, read, close.
  // undefined: a legacy file without a marker; false: unreadable, which never matches a label.
  #canonicalGeneration(): string | undefined | false {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#statePath, "r");
      return readHeader(descriptor, STATE_HEADER);
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

  // A writer still reads and parses the canonical file under the lock on every operation.
  #readStateForWrite(): { state: MeshStateFile; reuse: Map<string, EncodedStateEntry> | undefined } {
    let reuse: Map<string, EncodedStateEntry> | undefined;
    const state = readState(this.#statePath, this.#maxStateBytes, false, (serialized) => {
      // Full content equality, not UUID/stat/version equality: a legacy copied-marker writer
      // can change an entry without advancing any of those labels. Always read and parse fresh.
      if (serialized === this.#writeEncodings?.serialized) reuse = this.#writeEncodings.entries;
    });
    return { state, reuse };
  }

  // A commit's write, signal and cache, under the lock. The stamp is taken right after the rename,
  // before hashing the encoded entries; the signal is published and the cache kept only while the file still has it,
  // so a lock-bypassing writer replacing the file meanwhile never gets this payload's hashes or
  // cache label. ponytail: a replace between the rename and that first stat cannot be detected
  // without the written descriptor (atomic-write.ts); the lock protocol excludes it.
  // The commit's new readGeneration is serialized FIRST and atomically with the payload (the
  // previous one is dropped from the copy), so the canonical header alone identifies the commit
  // whether or not the optional signal is published afterwards. The stamped copy is cached.
  #commitState(state: MeshStateFile, reuse?: Map<string, EncodedStateEntry>, keys: string[] = [], caller?: string[]): void {
    const payload: MeshStateFile = { ...state };
    delete payload.readGeneration;
    const generation = randomUUID();
    const stamped: MeshStateFile = { readGeneration: generation, ...payload };
    const encoded = encodeState(stamped, reuse);
    if (encoded.serialized.byteLength > this.#maxStateBytes) {
      throw new Error(`Fabric mesh state exceeds ${this.#maxStateBytes} bytes`);
    }
    writeFileAtomic(this.#statePath, encoded.serialized);
    commitStats?.record(encoded.serialized.byteLength, keys);
    const stamp = statStamp(this.#statePath);
    if (stamp !== undefined) this.#writeSignal(encoded.entries, stamp, generation);
    if (stamp === undefined || !this.#cacheState(stamped, stamp)) this.#stateCache = undefined;
    this.#writeEncodings = { serialized: encoded.serialized.toString("utf8"), entries: encoded.entries };
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
  #writeSignal(entries: Map<string, EncodedStateEntry>, stamp: string, generation: string): boolean {
    try {
      const hashes = new Map<string, ReturnType<typeof createHash>>();
      const delimiter = Buffer.from("\n");
      // Same ordered entry bytes and newline framing as digestEntries, without re-encoding.
      for (const key of sortedKeys([...entries.keys()])) {
        const namespace = keyNamespace(key);
        if (!namespace) continue;
        let hash = hashes.get(namespace);
        if (!hash) hashes.set(namespace, hash = createHash("sha256"));
        hash.update(entries.get(key)!.entry).update(delimiter);
      }
      const namespaces: Record<string, string> = {};
      for (const [namespace, hash] of hashes) namespaces[namespace] = hash.digest("base64");
      if (statStamp(this.#statePath) !== stamp) return false;   // replaced while hashing: publish nothing
      // `generation` first: readers take it from the file's first HEADER_BYTES; it must equal the canonical readGeneration.
      const serialized = JSON.stringify({ generation, stamp, namespaces });
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
    return this.#withLock(() => {
      const { state, reuse } = this.#readStateForWrite();
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
      this.#commitState(state, reuse, [plan.key], caller);
      return jsonClone(entry);
    });
  }

  /**
   * Runs an operation under the mesh lock without touching the state: for a rare step that must
   * be serialized fleet-wide, such as recovering a per-key lock whose holder died.
   */
  async exclusive<T>(operation: () => T, lockTimeoutMs?: number): Promise<T> {
    return this.#withLock(operation, lockTimeoutMs);
  }

  /**
   * Takes and releases the mesh lock without writing the state: evidence that the shared state is
   * writable now, for a heartbeat that renewed only its file lease. Discards this store's cached
   * snapshot so the next state read must read the canonical file, even if metadata is unchanged.
   */
  async confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    await this.#withLock(() => {
      this.#stateCache = undefined;
      onAcquired?.(Date.now());
    });
  }

  async delete(input: {
    key: string;
    ifVersion?: number;
  }): Promise<{ deleted: boolean; version?: number }> {
    const { key, ifVersion } = input;
    const caller = commitTraceCaller();
    this.#validateKey(key);
    const request = captureStorageDelete({ key, ifVersion });
    return this.#withLock(() => {
      const { state, reuse } = this.#readStateForWrite();
      const slot = stateSlot(state, request.key);
      const plan = request.transition(slot.present, slot.version, slot.highWater);
      if (plan.kind === "unchanged") {
        this.#cacheState(state, undefined);
        return { deleted: false };
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
      this.#commitState(state, reuse, [plan.key], caller);
      return { deleted: true, version: plan.version };
    });
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
  async writeBatch(input: {
    identity: MeshIdentity;
    ops: MeshBatchOperation[];
    prepare?: (view: MeshBatchView) => MeshBatchOperation[];
    afterCommit?: (view: MeshBatchView) => void;
  }): Promise<MeshBatchResult[]> {
    const caller = commitTraceCaller();
    for (const op of input.ops) this.#validateKey(op.key);
    if (input.ops.length === 0 && !input.prepare && !input.afterCommit) return [];
    return this.#withLock(() => {
      // Each operation takes the same verified transition as put()/delete(), so a batch
      // advances the persistent clock exactly as the single writes would, and damaged
      // state is the same write barrier.
      const { state, reuse } = this.#readStateForWrite();
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
      const ops = [...input.ops, ...(input.prepare?.(view) ?? [])];
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
        this.#commitState(state, reuse, results.filter(result => result.applied).map(result => result.key), caller);
      }
      input.afterCommit?.(view);
      return results;
    });
  }

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
   * revalidate through the ordinary read-cache window, not authoritative payload freshness.
   * This UI observer may lag remote changes by readCacheMs; observing a cached payload does
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
        if (revalidateGeneration && cached && this.#readCacheMs > 0 && Date.now() - cached.parsedAt < this.#readCacheMs) {
          const generation = this.#canonicalGeneration();
          changed = typeof generation === "string" && generation !== cached.generation;
        }
        this.#readCachedState(changed, false); // observer only; public fresh payload reads stay canonical
      } catch {
        return undefined;
      }
    }
    const cached = this.#stateCache;
    return cached ? `${cached.device}:${cached.inode}:${cached.size}:${cached.modifiedAt}` : undefined;
  }

  #readCachedState(fresh = false, canonical = fresh): MeshStateFile {
    const recent = this.#stateCache;
    const readCacheMs = this.readCacheMs;
    if (!fresh && recent && readCacheMs > 0 && Date.now() - recent.parsedAt < readCacheMs) {
      return recent.state;
    }
    let before: string;
    try {
      before = stampOf(fs.statSync(this.#statePath));
      // Metadata alone can repeat (ABA): a same-stamp cache is reused only while the canonical
      // header still names the payload's own generation (a 64-byte peek, not a parse). A payload
      // without a marker (legacy) is UNKNOWN: expired nonfresh reads always re-parse it.
      // Only the UI observer (fresh, !canonical) may reuse matching metadata + missing header.
      const cached = this.#stateCache;
      if (
        !canonical && cached?.stamp === before && (cached.generation !== undefined || fresh) && cached.generation === this.#canonicalGeneration()
      ) return cached.state;
    } catch (error) {
      this.#stateCache = undefined;
      if (errorCode(error) === "ENOENT") return emptyState();
      throw error;
    }
    // A payload is cached only under the stamp seen both before and after its read: a commit
    // landing during the parse must not label the older payload with the newer file's stamp.
    // The label is the parsed payload's own canonical readGeneration, never a separately observed
    // marker, so an older payload can never carry a newer commit's generation.
    for (let attempt = 0; ; attempt++) {
      const state = readState(this.#statePath, this.#maxStateBytes);
      if (this.#cacheState(state, before)) return state;
      const next = statStamp(this.#statePath);
      if (attempt >= 2 || next === undefined) {
        this.#stateCache = undefined;                   // served once, never cached or stamped
        return state;
      }
      before = next;
    }
  }

  // Under the lock (writes) no expected stamp is needed; lock-free reads pass the pre-read stamp.
  // The entry is labelled with the payload's own canonical generation.
  #cacheState(state: MeshStateFile, expectedStamp: string | undefined): boolean {
    try {
      const stat = fs.statSync(this.#statePath);
      if (expectedStamp !== undefined && stampOf(stat) !== expectedStamp) return false;
      this.#stateCache = {
        device: stat.dev,
        inode: stat.ino,
        size: stat.size,
        modifiedAt: stat.mtimeMs,
        stamp: stampOf(stat),
        parsedAt: Date.now(),
        state,
        generation: generationOf(state),
      };
      return true;
    } catch {
      this.#stateCache = undefined;
      return false;
    }
  }

  async #withLock<T>(operation: () => T, lockTimeoutMs = this.#lockTimeoutMs): Promise<T> {
    fs.mkdirSync(this.root, { recursive: true, mode: 0o700 });
    const deadline = Date.now() + Math.min(this.#lockTimeoutMs, Math.max(0, lockTimeoutMs));
    const token = randomUUID();
    const ownerPath = path.join(this.#lockPath, "owner");
    const startTime = this.#lockProtocol === 2 ? await ownProcessIncarnation() : undefined;
    const ownerRecord = `${token}\n${process.pid}\n${Date.now()}\n${startTime ? `${startTime}\n` : ""}`;
    const releaseOwned = (): void => {
      try {
        if (fs.readFileSync(ownerPath, "utf8") === ownerRecord) {
          // Detach the complete owned directory before unlinking anything inside it.
          // Interrupted/resumed recursive cleanup must never follow the canonical name.
          const released = `${this.#lockPath}.released.${token}`;
          fs.renameSync(this.#lockPath, released);
          fs.rmSync(released, { recursive: true, force: true });
        }
      } catch {
        // Already replaced/removed, unreadable, or cleanup failed: never delete canonical.
      }
    };
    // Attempts and the largest gap between two of them: a large gap means this waiter stalled
    // (no CPU); many attempts with small gaps mean it kept losing the race (smarty-dev#816).
    let attempts = 0;
    let maxGapMs = 0;
    let lastAttemptAt = Date.now();
    let retryAttempt = 0;
    while (true) {
      const attemptAt = Date.now();
      if (attempts > 0) maxGapMs = Math.max(maxGapMs, attemptAt - lastAttemptAt);
      attempts += 1;
      lastAttemptAt = attemptAt;
      try {
        if (this.#lockProtocol === 1) {
          // Keep the B68 three-line wire, but never overwrite an owner published by a
          // successor while this initializer was stopped after canonical mkdir.
          fs.mkdirSync(this.#lockPath, { mode: 0o700 });
          const ownershipLost = () => Object.assign(new Error("Fabric mesh lock ownership lost during acquisition"), {
            code: "FABRIC_MESH_LOCK_OWNERSHIP_LOST",
          });
          try {
            const directory = fs.lstatSync(this.#lockPath);
            fs.writeFileSync(ownerPath, ownerRecord, {
              encoding: "utf8", flag: "wx", mode: 0o600,
            });
            // The exclusive create may itself have paused with an open descriptor to a
            // recovered directory. Prove publication still belongs to the canonical lock
            // before entering the critical section; never clean a successor on failure.
            const current = fs.lstatSync(this.#lockPath);
            if (!current.isDirectory() || current.dev !== directory.dev || current.ino !== directory.ino ||
              fs.readFileSync(ownerPath, "utf8") !== ownerRecord) throw ownershipLost();
          } catch (error) {
            // A resumed initializer may have published into an empty replacement before
            // rejecting its directory identity. Remove only that attempt's exact receipt.
            releaseOwned();
            if (errorCode(error) === "EEXIST" || errorCode(error) === "ENOENT") throw ownershipLost();
            throw error;
          }
        } else {
          // Never expose an ownerless canonical directory: a stalled initializer must not
          // resume its owner write through a name that legacy recovery gave to a successor.
          const staging = fs.mkdtempSync(`${this.#lockPath}.pending.${token}.`);
          try {
            fs.writeFileSync(path.join(staging, "owner"), ownerRecord, {
              encoding: "utf8", flag: "wx", mode: 0o600,
            });
            // POSIX rename can replace an EMPTY directory, but a fresh ownerless legacy
            // lock may be an in-flight creator. Route every observed canonical path through
            // the original owner/stale checks instead of publishing over it.
            try {
              fs.lstatSync(this.#lockPath);
              throw Object.assign(new Error("Fabric mesh lock already exists"), { code: "EEXIST" });
            } catch (error) {
              if (errorCode(error) !== "ENOENT") throw error;
            }
            // New-format competitors publish nonempty owners atomically. This does not fence
            // old-format writers that create an empty canonical after the absence check.
            fs.renameSync(staging, this.#lockPath);
          } finally {
            fs.rmSync(staging, { recursive: true, force: true });
          }
        }
        break;
      } catch (error) {
        const code = errorCode(error);
        if (code !== "EEXIST" && (this.#lockProtocol === 1 ||
          (code !== "ENOTEMPTY" && code !== "EPERM" && code !== "EACCES"))) throw error;
        if (await this.#clearStaleLock(ownerPath)) continue;
        if (Date.now() >= deadline) {
          throw new MeshLockTimeoutError(describeLockHolder(ownerPath), attempts, maxGapMs);
        }
        // Full jitter spreads a fleet after a stalled holder resumes. The original
        // absolute deadline still bounds every sleep (including a zero draw).
        await delay(retryDelayMs(retryAttempt++, 20, 250, deadline - Date.now()));
      }
    }
    try {
      return operation();
    } catch (error) {
      // A failed write (a version conflict above all) means this store's view is behind: the
      // next read parses the file again instead of reusing a recent parse.
      this.#stateCache = undefined;
      throw error;
    } finally {
      releaseOwned();
    }
  }

  // Complete dead/different-incarnation receipts recover immediately. Empty ownerless
  // directories recover only after the grace, using atomic rmdir (never recursive removal
  // or rename): an owner published after our last comparison makes rmdir fail closed.
  // Torn/corrupt receipts and nonempty unrecorded directories remain protected.
  async #clearStaleLock(ownerPath: string): Promise<boolean> {
    try {
      const stat = fs.lstatSync(this.#lockPath);
      if (!stat.isDirectory()) return false;
      const readOwner = (): string | undefined => {
        try { return fs.readFileSync(ownerPath, "utf8"); }
        catch (error) { if (errorCode(error) === "ENOENT") return undefined; throw error; }
      };
      const owner = readOwner();
      if (owner === undefined) {
        if (Date.now() - stat.mtimeMs <= this.#staleLockMs) return false;
        const current = fs.lstatSync(this.#lockPath);
        if (!current.isDirectory() || current.dev !== stat.dev || current.ino !== stat.ino ||
          current.mtimeMs !== stat.mtimeMs || readOwner() !== undefined) return false;
        // Native emptiness is the final fence, including against an initializer that
        // publishes and enters while this recoverer is paused at the removal syscall.
        fs.rmdirSync(this.#lockPath);
        // Retain a nonempty recovery receipt, without ever renaming a live canonical.
        // Inode reuse may revisit the same receipt; that must not undo successful recovery.
        const fence = `${this.#lockPath}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:`).digest("hex")}`;
        fs.mkdirSync(fence, { recursive: true, mode: 0o700 });
        try { fs.writeFileSync(path.join(fence, ".recovery-fence"), "1\n", { flag: "wx", mode: 0o600 }); }
        catch (error) { if (errorCode(error) !== "EEXIST") throw error; }
        return true;
      }
      const fields = owner.split("\n");
      const [token, pidText, createdText, startText] = fields;
      // An in-flight/torn fourth line is not evidence of PID reuse.
      const recordedStart = owner?.endsWith("\n") ? startText : undefined;
      const pid = Number(pidText);
      const validPid = Number.isSafeInteger(pid) && pid > 0;
      const validOwner = owner?.endsWith("\n") && (fields.length === 4 || fields.length === 5) &&
        !!token && validPid && createdText !== undefined &&
        createdText.trim() !== "" && Number.isFinite(Number(createdText));
      if (!validOwner) return false;
      if (processAlive(pid)) {
        if (!validProcessIncarnation(recordedStart)) return false;
        const actualStart = await processIncarnation(pid);
        if (!actualStart || actualStart === recordedStart) return false;
      }
      const unchanged = (): boolean => {
        const current = fs.lstatSync(this.#lockPath);
        return current.isDirectory() && current.dev === stat.dev && current.ino === stat.ino && readOwner() === owner;
      };
      if (!unchanged()) return false;
      const fence = `${this.#lockPath}.dead.${createHash("sha256").update(`${stat.dev}:${stat.ino}:${owner}`).digest("hex")}`;
      // ponytail: retain this tiny nonempty directory permanently. A paused old cleaner
      // cannot rename a successor over the same fence (native EEXIST/ENOTEMPTY). Deleting
      // it, or recursively deleting the canonical name after a re-read, reopens that race.
      fs.renameSync(this.#lockPath, fence);
      return true;
    } catch {
      return false;
    }
  }

  #readGeneration(): number {
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(this.#generationPath, "utf8"));
      return typeof parsed === "number" && Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
    } catch {
      return 0;
    }
  }

  #encodeCursor(generation: number, offset: number): number {
    const cursor = generation * CURSOR_OFFSET_BASE + offset;
    if (!Number.isSafeInteger(cursor) || cursor < 0) {
      throw new Error("Fabric mesh cursor exhausted its safe integer range");
    }
    return cursor;
  }

  #decodeCursor(cursor: number): { generation: number; offset: number } {
    if (!Number.isSafeInteger(cursor) || cursor < 0) return { generation: -1, offset: 0 };
    return {
      generation: Math.floor(cursor / CURSOR_OFFSET_BASE),
      offset: cursor % CURSOR_OFFSET_BASE,
    };
  }

  #compactEventLog(): void {
    // Never rewrite away an event named by a durable intent. Resolve every intent while
    // the publish lock is held, before taking the retained tail snapshot.
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size <= this.#maxEventLogBytes) return;
      this.#settleDedupeIntents(MeshArchive.fromRoot(this.root));
      const readBytes = Math.min(
        size,
        this.#retainedEventLogBytes + this.maxEventBytes + 1,
      );
      const buffer = Buffer.allocUnsafe(readBytes);
      const bytesRead = fs.readSync(descriptor, buffer, 0, readBytes, size - readBytes);
      const captured = buffer.subarray(0, bytesRead);
      const retentionBoundary = Math.max(0, captured.length - this.#retainedEventLogBytes);
      const newline = retentionBoundary === 0 ? -1 : captured.indexOf(0x0a, retentionBoundary);
      const retainedStart = retentionBoundary === 0 ? 0 : newline >= 0 ? newline + 1 : captured.length;
      const retained = captured.subarray(retainedStart);
      fs.closeSync(descriptor);
      descriptor = undefined;
      // Persist both the retained bytes and the rename. Later intents may name offsets in
      // this generation; a reboot must not resurrect its unsynced predecessor or lose bytes.
      writeFileAtomic(this.#eventsPath, retained, { durable: true });
      atomicWrite(this.#generationPath, this.#readGeneration() + 1);
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #repairEventLog(): void {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r+");
      const size = fs.fstatSync(descriptor).size;
      if (size === 0) return;
      const lastByte = Buffer.allocUnsafe(1);
      fs.readSync(descriptor, lastByte, 0, 1, size - 1);
      if (lastByte[0] === 0x0a) return;
      const readBytes = Math.min(size, this.maxEventBytes + 1);
      const tail = Buffer.allocUnsafe(readBytes);
      fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
      const newline = tail.lastIndexOf(0x0a);
      fs.ftruncateSync(descriptor, newline >= 0 ? size - readBytes + newline + 1 : 0);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #readLastEventSequence(): number {
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size === 0) return 0;
      const readBytes = Math.min(size, this.maxEventBytes + 1);
      const tail = Buffer.allocUnsafe(readBytes);
      fs.readSync(descriptor, tail, 0, readBytes, size - readBytes);
      const lines = tail.toString("utf8").trim().split("\n");
      for (let index = lines.length - 1; index >= 0; index--) {
        const line = lines[index];
        if (!line) continue;
        try {
          const parsed = JSON.parse(line) as { sequence?: unknown };
          if (typeof parsed.sequence === "number" && Number.isSafeInteger(parsed.sequence)) {
            return parsed.sequence;
          }
        } catch { /* skip malformed sequence line */ }
      }
      return 0;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return 0;
      throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
  }

  #readSequence(): number {
    try {
      const parsed: unknown = JSON.parse(fs.readFileSync(this.#counterPath, "utf8"));
      return typeof parsed === "number" && Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : 0;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return 0;
      return 0;
    }
  }

  #validateTopic(topic: string): void {
    if (!TOPIC_PATTERN.test(topic)) throw new Error(`Invalid Fabric mesh topic: ${topic}`);
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
