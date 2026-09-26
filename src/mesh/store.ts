import { randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { writeFileAtomic } from "../core/atomic-write.js";
import { readJsonlPage } from "../log-tail.js";
import { captureStoragePut, captureStorageDelete, storageRevision } from "../verified/storage.js";

export interface MeshIdentity {
  id: string;
  name: string;
  kind: "main" | "actor" | "agent";
  sessionId?: string;
}

export interface MeshEvent {
  id: string;
  sequence: number;
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
}

export interface MeshReadOptions {
  /** Read the current file (re-parsing only if it changed), not a recent parse. */
  fresh?: boolean;
}

export interface MeshStoreOptions {
  maxEventLogBytes?: number;
  retainedEventLogBytes?: number;
  maxStateBytes?: number;
  maxStateTombstones?: number;
  lockTimeoutMs?: number;
  staleLockMs?: number;
  /**
   * Reads (get, list, listAll) reuse the last parsed state for up to this long, even when
   * another process has rewritten the file since. Every write still reads the file fresh
   * under the lock and checks versions, and a store sees its own writes at once. 0 (the
   * default) re-reads whenever the file changed.
   */
  readCacheMs?: number;
}

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
 * Read-cache age for the stores a Fabric runtime and its resident host use: at most one parse
 * of the shared state per process per this interval (smarty-dev#251, dev1 load P0: ~50
 * processes each re-parsed the whole file on every change, about 10 times a second).
 * ponytail: listings may lag other hosts by up to 2 s; leases are 15 s and heartbeats 5 s.
 */
export const RUNTIME_MESH_READ_CACHE_MS = 2_000;
const EVENT_READ_PAGE_BYTES = 4 * 1024 * 1024;
const EVENT_READ_CHUNK_BYTES = 64 * 1024;
// Line ends remembered from recent read({ after }) scans: enough for every reader near the log head.
const READ_HINT_LINES = 128;
const CURSOR_OFFSET_BASE = 2 ** 32;

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
  } catch {
    return false;
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

const readState = (filePath: string, maxBytes: number, recoverDamage = true): MeshStateFile => {
  let serialized: string;
  try {
    const stat = fs.statSync(filePath);
    if (stat.size > maxBytes) throw new Error(`state exceeds ${maxBytes} bytes`);
    if (stat.size === 0 && recoverDamage) return emptyState();
    serialized = fs.readFileSync(filePath, "utf8");
  } catch (error) {
    if (errorCode(error) === "ENOENT") return emptyState();
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(`Failed to read Fabric mesh state: ${message}`);
  }
  if (!serialized.trim() && recoverDamage) return emptyState();
  try {
    const parsed: unknown = JSON.parse(serialized);
    if (isMeshStateFile(parsed)) return parsed;
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

const atomicWrite = (filePath: string, value: unknown, maxBytes = Number.POSITIVE_INFINITY): void => {
  const serialized = JSON.stringify(value, null, 2);
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

export class MeshStore {
  readonly #eventsPath: string;
  readonly #statePath: string;
  readonly #counterPath: string;
  readonly #generationPath: string;
  readonly #lockPath: string;
  readonly #maxEventLogBytes: number;
  readonly #retainedEventLogBytes: number;
  readonly #maxStateBytes: number;
  readonly #maxStateTombstones: number;
  readonly #lockTimeoutMs: number;
  readonly #staleLockMs: number;
  readonly #readCacheMs: number;
  /**
   * Line ends (sequence, offset) that recent read({ after }) scans passed, by rising sequence. A
   * read starts at the last one at or below its cursor. One remembered point was not enough:
   * several readers at one cursor (a host's lifecycle subscriptions after a new event) moved it
   * past each other, and all but the first scanned the whole log again (smarty-dev#557).
   */
  #readHints: { generation: number; inode: number; lines: Array<{ sequence: number; offset: number }> } | undefined;
  #stateCache:
    | { device: number; inode: number; size: number; modifiedAt: number; parsedAt: number; state: MeshStateFile }
    | undefined;

  constructor(
    readonly root: string,
    readonly maxEventBytes: number,
    readonly maxReadEvents: number,
    options: MeshStoreOptions = {},
  ) {
    this.#eventsPath = path.join(root, "events.jsonl");
    this.#statePath = path.join(root, "state.json");
    this.#counterPath = path.join(root, "sequence");
    this.#generationPath = path.join(root, "generation");
    this.#lockPath = path.join(root, ".lock");
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
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
  }

  async publish(input: {
    topic: string;
    kind?: string;
    from: MeshIdentity;
    to?: string;
    text?: string;
    data?: unknown;
  }): Promise<MeshEvent> {
    this.#validateTopic(input.topic);
    if (input.to !== undefined && !input.to.trim()) throw new Error("Mesh recipient is empty");
    const eventData = input.data === undefined ? undefined : jsonClone(input.data);
    return this.#withLock(() => {
      this.#repairEventLog();
      const sequence = Math.max(this.#readSequence(), this.#readLastEventSequence()) + 1;
      const event: MeshEvent = {
        id: randomUUID(),
        sequence,
        topic: input.topic,
        kind: input.kind?.trim() || "message",
        from: jsonClone(input.from),
        ...(input.to ? { to: input.to } : {}),
        ...(input.text !== undefined ? { text: input.text } : {}),
        ...(eventData !== undefined ? { data: eventData } : {}),
        createdAt: Date.now(),
      };
      const line = JSON.stringify(event);
      if (Buffer.byteLength(line, "utf8") > this.maxEventBytes) {
        throw new Error(`Mesh event exceeds ${this.maxEventBytes} bytes`);
      }
      fs.appendFileSync(this.#eventsPath, `${line}\n`, { encoding: "utf8", mode: 0o600 });
      atomicWrite(this.#counterPath, sequence);
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
    const events =
      input.after === undefined
        ? this.#readRecentEvents(input, limit)
        : this.#readEventsAfter(Math.max(0, Math.floor(input.after)), input, limit);
    return events.map((event) => jsonClone(event));
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
    const generation = this.#readGeneration();
    let descriptor: number | undefined;
    let completeOffset = 0;
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
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
    } finally {
      if (descriptor !== undefined) fs.closeSync(descriptor);
    }
    return this.#encodeCursor(generation, completeOffset);
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

  // fresh: skip the read cache's recent-parse reuse (readCacheMs), for a read that decides a
  // protocol step rather than a listing. The file is still read only when it changed.
  get(key: string, options: MeshReadOptions = {}): MeshStateEntry | undefined {
    this.#validateKey(key);
    const entries = this.#readCachedState(options.fresh === true).entries;
    return Object.hasOwn(entries, key) ? jsonClone(entries[key]) : undefined;
  }

  list(prefix = "", limit = 100): MeshStateEntry[] {
    const boundedLimit = Math.max(1, Math.min(Math.floor(limit), this.maxReadEvents));
    return this.listAll(prefix).slice(0, boundedLimit);
  }

  /** Internal project-state scan for host-managed indexes that must reconcile every key. */
  listAll(prefix = "", options: MeshReadOptions = {}): MeshStateEntry[] {
    if (prefix) this.#validateKey(prefix);
    return Object.values(this.#readCachedState(options.fresh === true).entries)
      .filter((entry) => !prefix || entry.key.startsWith(prefix))
      .sort((left, right) => left.key.localeCompare(right.key))
      .map((entry) => jsonClone(entry));
  }

  async put(input: {
    key: string;
    value: unknown;
    identity: MeshIdentity;
    ifVersion?: number;
  }): Promise<MeshStateEntry> {
    const { key, value, identity, ifVersion } = input;
    this.#validateKey(key);
    const request = captureStoragePut({ key, value, identity, ifVersion }, this.maxEventBytes);
    return this.#withLock(() => {
      const state = readState(this.#statePath, this.#maxStateBytes, false);
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
      atomicWrite(this.#statePath, state, this.#maxStateBytes);
      this.#cacheState(state);
      return jsonClone(entry);
    });
  }

  /**
   * Takes and releases the mesh lock without writing the state: evidence that the shared state is
   * writable now, for a heartbeat that renewed only its file lease. It also drops this store's
   * cached view, so a read after the confirmation cannot return an earlier snapshot.
   */
  async confirmWritable(): Promise<void> {
    await this.#withLock(() => {
      this.#stateCache = undefined;
    });
  }

  async delete(input: {
    key: string;
    ifVersion?: number;
  }): Promise<{ deleted: boolean; version?: number }> {
    const { key, ifVersion } = input;
    this.#validateKey(key);
    const request = captureStorageDelete({ key, ifVersion });
    return this.#withLock(() => {
      const state = readState(this.#statePath, this.#maxStateBytes, false);
      const slot = stateSlot(state, request.key);
      const plan = request.transition(slot.present, slot.version, slot.highWater);
      if (plan.kind === "unchanged") {
        this.#cacheState(state);
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
      atomicWrite(this.#statePath, state, this.#maxStateBytes);
      this.#cacheState(state);
      return { deleted: true, version: plan.version };
    });
  }

  // Applies several puts and deletes in ONE locked read-modify-write, so a caller that
  // updates many keys at once rewrites the shared state file once instead of once per
  // key. Each operation keeps put()/delete() semantics, including an optional
  // compare-and-swap. On a version mismatch, `onConflict` decides: "skip" leaves that
  // key alone, "abort" writes nothing at all and rejects. A put value may be a function,
  // evaluated under the lock at commit time (for timestamps such as lease stamps).
  // Returns one result per operation, in order.
  async writeBatch(input: {
    identity: MeshIdentity;
    ops: MeshBatchOperation[];
  }): Promise<MeshBatchResult[]> {
    for (const op of input.ops) this.#validateKey(op.key);
    if (input.ops.length === 0) return [];
    return this.#withLock(() => {
      // Each operation takes the same verified transition as put()/delete(), so a batch
      // advances the persistent clock exactly as the single writes would, and damaged
      // state is the same write barrier.
      const state = readState(this.#statePath, this.#maxStateBytes, false);
      state.versions ??= {};
      const tombstones = new Set(state.tombstoneOrder ?? []);
      const results: MeshBatchResult[] = [];
      let changed = false;
      const now = Date.now();
      const current = (key: string): MeshStateEntry | undefined =>
        Object.hasOwn(state.entries, key) ? jsonClone(state.entries[key]) : undefined;
      for (const op of input.ops) {
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
            identity: input.identity,
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
        this.#cacheState(state);
        return results;
      }
      state.tombstoneOrder = [...tombstones];
      compactStateTombstones(state, this.#maxStateTombstones);
      atomicWrite(this.#statePath, state, this.#maxStateBytes);
      this.#cacheState(state);
      return results;
    });
  }

  #readCachedState(fresh = false): MeshStateFile {
    const recent = this.#stateCache;
    if (!fresh && recent && this.#readCacheMs > 0 && Date.now() - recent.parsedAt < this.#readCacheMs) {
      return recent.state;
    }
    try {
      const stat = fs.statSync(this.#statePath);
      const cached = this.#stateCache;
      if (
        cached &&
        cached.device === stat.dev &&
        cached.inode === stat.ino &&
        cached.size === stat.size &&
        cached.modifiedAt === stat.mtimeMs
      ) {
        return cached.state;
      }
    } catch (error) {
      this.#stateCache = undefined;
      if (errorCode(error) === "ENOENT") return emptyState();
      throw error;
    }
    const state = readState(this.#statePath, this.#maxStateBytes);
    this.#cacheState(state);
    return state;
  }

  #cacheState(state: MeshStateFile): void {
    try {
      const stat = fs.statSync(this.#statePath);
      this.#stateCache = {
        device: stat.dev,
        inode: stat.ino,
        size: stat.size,
        modifiedAt: stat.mtimeMs,
        parsedAt: Date.now(),
        state,
      };
    } catch {
      this.#stateCache = undefined;
    }
  }

  async #withLock<T>(operation: () => T): Promise<T> {
    fs.mkdirSync(this.root, { recursive: true, mode: 0o700 });
    const deadline = Date.now() + this.#lockTimeoutMs;
    const token = randomUUID();
    const ownerPath = path.join(this.#lockPath, "owner");
    while (true) {
      try {
        fs.mkdirSync(this.#lockPath, { mode: 0o700 });
        fs.writeFileSync(ownerPath, `${token}\n${process.pid}\n${Date.now()}\n`, {
          encoding: "utf8",
          mode: 0o600,
        });
        break;
      } catch (error) {
        if (errorCode(error) !== "EEXIST") throw error;
        if (this.#clearStaleLock(ownerPath)) continue;
        if (Date.now() >= deadline) {
          throw Object.assign(new Error(`Timed out waiting for the Fabric mesh lock${describeLockHolder(ownerPath)}`), {
            code: "FABRIC_MESH_LOCK_TIMEOUT",
          });
        }
        await delay(10);
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
      try {
        const owner = fs.readFileSync(ownerPath, "utf8");
        if (owner.startsWith(`${token}\n`)) {
          fs.rmSync(this.#lockPath, { recursive: true, force: true });
        }
      } catch {
        // Another process already recovered or removed this lock.
      }
    }
  }

  // Returns true when a stale lock was removed and acquisition should retry.
  // A lock is stale when it outlived the stale window without a live owner:
  // either the owner file names a dead process, or the owner file is missing
  // or corrupt — the owner crashed between creating the lock directory and
  // writing the owner file — and the untouched lock directory itself is
  // stale. Removal re-reads the state it judged stale so a freshly rotated
  // owner is never deleted mid-check.
  #clearStaleLock(ownerPath: string): boolean {
    let lockModifiedAt: number | undefined;
    let owner: string | undefined;
    try {
      owner = fs.readFileSync(ownerPath, "utf8");
    } catch {
      try {
        lockModifiedAt = fs.statSync(this.#lockPath).mtimeMs;
      } catch {
        return false;
      }
    }
    if (owner !== undefined) {
      const [, pidText, createdText] = owner.trim().split("\n");
      const createdAt = Number(createdText);
      if (Number.isFinite(createdAt) && Date.now() - createdAt <= this.#staleLockMs) return false;
      if (processAlive(Number(pidText))) return false;
      try {
        if (fs.readFileSync(ownerPath, "utf8") !== owner) return false;
        fs.rmSync(this.#lockPath, { recursive: true, force: true });
        return true;
      } catch {
        return false;
      }
    }
    if (lockModifiedAt === undefined || Date.now() - lockModifiedAt <= this.#staleLockMs) {
      return false;
    }
    try {
      if (fs.statSync(this.#lockPath).mtimeMs !== lockModifiedAt) return false;
      fs.rmSync(this.#lockPath, { recursive: true, force: true });
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
    let descriptor: number | undefined;
    try {
      descriptor = fs.openSync(this.#eventsPath, "r");
      const size = fs.fstatSync(descriptor).size;
      if (size <= this.#maxEventLogBytes) return;
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
      const temporaryPath =
        this.#eventsPath + "." + process.pid + "." + randomUUID() + ".tmp";
      try {
        fs.writeFileSync(temporaryPath, retained, { mode: 0o600 });
        fs.renameSync(temporaryPath, this.#eventsPath);
      } finally {
        try { fs.rmSync(temporaryPath, { force: true }); } catch {}
      }
      atomicWrite(this.#generationPath, this.#readGeneration() + 1);
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
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
