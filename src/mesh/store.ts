import type { MeshLockProtocol } from "../config.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshLock, type MeshStoreContext } from "./mesh-lock.js";
import { withMeshCustody } from "./custody-lock.js";
import type { MeshReadOptions, MeshStateEntry, MeshBatchResult } from "./state-file.js";
import { createStateBackend, type MeshStateBackendKind, type StateBackend, type StateBackendBatchInput,
  type StateBackendDiagnostics } from "./state-backend.js";
import { EventLog, type MeshEvent, type MeshIdentity, type MeshPublishInput, type MeshTailResult } from "./event-log.js";
export { MeshLockTimeoutError } from "../core/atomic-write.js";
export type { MeshIdentity, MeshEvent, MeshPublishInput, MeshTailResult } from "./event-log.js";
export { meshCursorGeneration, meshCursorAtStart, MeshDedupeRecoveryError } from "./event-log.js";
export type { MeshStateEntry, MeshReadOptions, MeshBatchOperation, MeshBatchView, MeshBatchResult } from "./state-file.js";
export { RUNTIME_MESH_READ_CACHE_MS, MIN_BACKGROUND_MESH_READ_CACHE_MS, assertMeshStateReadable, MeshBatchConflictError } from "./state-file.js";
export type { MeshStateBackendKind, MeshCommitEffects, MeshStateFileRead, StateBackendBatchInput, StateBackendDiagnostics } from "./state-backend.js";

// The public mesh store (smarty-dev#6477 L0): a thin facade over three lock domains.
//   mesh-lock.ts   the `.lock` acquisition, its tickets, timeouts, metrics and stale-owner recovery;
//                  the lock order is written there.
//   state-file.ts  keyed state on state.json; state-backend.ts selects it, sqlite or shadow (L2a).
//   event-log.ts   events, receipts, the live log, compaction and the archive.
// Each domain keeps its own private fields; they share only the context (root, bounds, lock).

const meshProcessStartedAt = Math.floor(Date.now() - process.uptime() * 1000);

let bootTimeMs: number | undefined;
/** Linux only: the pid's start time in epoch ms from /proc (USER_HZ is 100); else undefined. */
const procStartedAt = (pid: number): number | undefined => {
  if (process.platform !== "linux") return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const ticks = Number(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]);
    bootTimeMs ??= Number(/^btime\s+(\d+)\s*$/m.exec(fs.readFileSync("/proc/stat", "utf8"))?.[1]) * 1000;
    return Number.isFinite(ticks) && Number.isFinite(bootTimeMs) ? bootTimeMs + ticks * 10 : undefined;
  } catch { return undefined; }
};

/**
 * Same-host census liveness (smarty-dev#6477 L4a): the pid is alive and, where /proc tells, is the
 * same incarnation (start within 2 s of the record). A record without startedAt cannot be refuted
 * and stays live (fail closed). Meaningless for another host's pid.
 */
export const censusRecordAlive = (pid: number, startedAt: unknown): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; }
  const started = procStartedAt(pid);
  return started === undefined || typeof startedAt !== "number" || Math.abs(started - startedAt) <= 2000;
};

/** Best effort: removes this host's census records whose process is gone; never throws. */
export const pruneDeadCensusRecords = (root: string): void => {
  const directory = path.join(root, ".writer-census");
  let names: string[];
  try { names = fs.readdirSync(directory); } catch { return; }
  const host = os.hostname();
  for (const name of names) {
    // Records are <pid>-<startedAt>.json: only a name that already looks dead is opened.
    const match = /^(\d+)-(\d+)\.json$/.exec(name);
    if (!match || censusRecordAlive(Number(match[1]), Number(match[2]))) continue;
    const file = path.join(directory, name);
    try {
      const value = JSON.parse(fs.readFileSync(file, "utf8")) as Record<string, unknown>;
      if (value.host === host && !censusRecordAlive(Number(value.pid), value.startedAt)) fs.rmSync(file, { force: true });
    } catch { /* unreadable or already gone: left to the next prune */ }
  }
};

const prunedCensusRoots = new Set<string>();
const ownCensusRecords = new Set<string>();
const removeOwnCensusRecords = (): void => {
  for (const file of ownCensusRecords) try { fs.rmSync(file, { force: true }); } catch { /* best effort */ }
};

const recordMeshWriter = (root: string, lockProtocol: number, stateBackend: string): void => {
  const directory = path.join(root, ".writer-census");
  if (!prunedCensusRoots.has(directory)) {
    prunedCensusRoots.add(directory);
    pruneDeadCensusRecords(root);
  }
  const file = path.join(directory, `${process.pid}-${meshProcessStartedAt}.json`);
  try {
    fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
    if (fs.existsSync(file)) return;
    const temporary = `${file}.tmp`;
    const writer = { format: 1, pid: process.pid, host: os.hostname(),
      releaseSha: process.env.PI_FABRIC_RELEASE_SHA ?? process.env.PI_FABRIC_BUILD_SHA ?? process.env.GITHUB_SHA ?? "unknown",
      lockProtocol, stateBackend, startedAt: meshProcessStartedAt };
    fs.writeFileSync(temporary, JSON.stringify(writer), { flag: "w", mode: 0o600 });
    try { fs.renameSync(temporary, file); }
    catch (error) { fs.rmSync(temporary, { force: true }); throw error; }
    if (ownCensusRecords.size === 0) process.once("exit", removeOwnCensusRecords);
    ownCensusRecords.add(file);
  } catch {
    // ponytail: a read-only or full disk must not fail MeshStore construction, so this process
    // writes no census record and no new mechanism records the failure. While it queues for or
    // holds the mesh lock its ticket/owner has no census metadata, so census() lists it unknown
    // (clean:false). Otherwise only its host lease shows it: a sqlite writer between custody
    // operations takes no .lock, and without a lease the census cannot see it.
  }
};

export interface MeshStoreOptions {
  /** Captured at construction, never reloaded. Defaults to B68-compatible protocol 1. */
  lockProtocol?: MeshLockProtocol;
  maxEventLogBytes?: number;
  retainedEventLogBytes?: number;
  maxStateBytes?: number;
  maxStateTombstones?: number;
  lockTimeoutMs?: number;
  /** Host-owned lifetime of advisory writes. Abort stops acquisition without
   * bypassing ownership checks or rolling back an admitted synchronous commit. */
  writeSignal?: AbortSignal;
  /** Grace for an empty ownerless directory; recorded live owners never expire. Default 30 s. */
  staleLockMs?: number;
  /**
   * Reads (get, list, listAll) reuse the last parsed state for up to this long, even when
   * another process has rewritten the file since. Every write still reads the file fresh
   * against an identity revalidated under the lock and checks versions, and a store sees its own writes at once. 0 (the
   * default) re-reads whenever the file changed.
   */
  readCacheMs?: number;
  /** Background-only idle window. Ordinary reads stay exact unless readCacheMs is explicitly set. */
  backgroundReadCacheMs?: number;
  /** A live turn/pending operation bypasses explicit ordinary TTL and bounds background TTL to 1 s. */
  readActive?: () => boolean;
  /** Disable optional delta publication, e.g. for legacy-writer compatibility probes. */
  writeReadJournal?: boolean;
  /** Keyed-state backend (smarty-dev#6477 L2a). Explicit wins; else PI_FABRIC_MESH_STATE_BACKEND; else "file". */
  stateBackend?: MeshStateBackendKind;
}

export class MeshStore {
  readonly #lock: MeshLock;
  readonly #state: StateBackend;
  readonly #events: EventLog;

  constructor(
    readonly root: string,
    readonly maxEventBytes: number,
    readonly maxReadEvents: number,
    options: MeshStoreOptions = {},
  ) {
    // A failed operation under the lock drops the parsed state (see MeshLock.withLock).
    this.#lock = new MeshLock(root, options, () => this.#state.dropCache());
    const context: MeshStoreContext = { root, maxEventBytes, maxReadEvents, lock: this.#lock };
    this.#state = createStateBackend(context, options);
    this.#events = new EventLog(context, options);
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    recordMeshWriter(root, this.#lock.lockProtocol, this.#state.kind);
  }

  get lockProtocol(): MeshLockProtocol {
    return this.#lock.lockProtocol;
  }

  /** The keyed-state backend in use (after any fallback to "file"). */
  get stateBackend(): MeshStateBackendKind {
    return this.#state.kind;
  }

  /** The backend itself, for maintenance tools (shadow verify/repair, the L3 projector, L4 census). */
  get stateBackendHandle(): StateBackend {
    return this.#state;
  }

  stateDiagnostics(): StateBackendDiagnostics {
    return this.#state.diagnostics();
  }

  /** Releases the state database handle (sqlite, shadow); the file backend holds none. */
  closeState(): void {
    this.#state.close();
  }

  get readCacheMs(): number {
    return this.#state.readCacheMs;
  }

  get backgroundReadCacheMs(): number {
    return this.#state.backgroundReadCacheMs;
  }

  get readCacheRemainingMs(): number {
    return this.#state.readCacheRemainingMs;
  }

  // Events (event-log.ts).

  publish(input: MeshPublishInput): Promise<MeshEvent> {
    return this.#events.publish(input);
  }

  publishBatch(inputs: MeshPublishInput[]): Promise<MeshEvent[]> {
    return this.#events.publishBatch(inputs);
  }

  read(input: { after?: number; topic?: string; to?: string; limit?: number } = {}): MeshEvent[] {
    return this.#events.read(input);
  }

  nextEventAfter(after: number): MeshEvent | undefined {
    return this.#events.nextEventAfter(after);
  }

  oldestSequence(): number | undefined {
    return this.#events.oldestSequence();
  }

  latestSequence(): number {
    return this.#events.latestSequence();
  }

  latestOffset(): number {
    return this.#events.latestOffset();
  }

  latestCursor(): { cursor: number; last?: { sequence: number; id: string } } {
    return this.#events.latestCursor();
  }

  tail(cursor: number, limit = 100): MeshTailResult {
    return this.#events.tail(cursor, limit);
  }

  // Keyed state (state-file.ts).

  get(key: string, options: MeshReadOptions = {}): MeshStateEntry | undefined {
    return this.#state.get(key, options);
  }

  list(prefix = "", limit = 100, options: MeshReadOptions = {}): MeshStateEntry[] {
    return this.#state.list(prefix, limit, options);
  }

  listAll(prefix = "", options: MeshReadOptions = {}): MeshStateEntry[] {
    return this.#state.listAll(prefix, options);
  }

  stateToken(options: MeshReadOptions = {}): object {
    return this.#state.stateToken(options);
  }

  listAllShared(prefix = "", options: MeshReadOptions = {}): readonly Readonly<MeshStateEntry>[] {
    return this.#state.listAllShared(prefix, options);
  }

  put(input: { key: string; value: unknown; identity: MeshIdentity; ifVersion?: number }): Promise<MeshStateEntry> {
    return this.#state.put(input);
  }

  delete(input: { key: string; ifVersion?: number }): Promise<{ deleted: boolean; version?: number }> {
    return this.#state.delete(input);
  }

  /** Transaction and callback semantics: StateBackendBatchInput (state-backend.ts). */
  writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]> {
    return this.#state.writeBatch(input);
  }

  confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    return this.#state.confirmWritable(onAcquired);
  }

  stateStamp(): string | undefined {
    return this.#state.stateStamp();
  }

  cachedStateStamp(fresh = false, revalidateGeneration = false): string | undefined {
    return this.#state.cachedStateStamp(fresh, revalidateGeneration);
  }

  // The lock (mesh-lock.ts).

  withTryLock<T>(operation: () => Promise<T>, timeoutMs = 0): Promise<T> {
    return this.#lock.withTryLock(operation, timeoutMs);
  }

  /** Runs a synchronous operation under mesh custody without writing shared state. */
  async exclusive<T>(operation: () => T, lockTimeoutMs?: number): Promise<T> {
    // An explicit zero budget is a bounded try (lock stats count it as a try, not a timeout).
    return this.#lock.withLock(operation, lockTimeoutMs, "custody", lockTimeoutMs === 0);
  }

  /** File custody (smarty-dev#6477 L5): the custody lock, plus the mesh lock in the default
   * transition-safe "dual" mode. For operations that guard only files beside the mesh. */
  async custody<T>(operation: () => T, lockTimeoutMs?: number): Promise<T> {
    return withMeshCustody(this, operation, lockTimeoutMs);
  }

  /** The active withTryLock budget in this async context, if any. */
  get tryLockBudgetMs(): number | undefined {
    const scope = this.#lock.tryLockScope.getStore();
    return scope?.active ? scope.timeoutMs : undefined;
  }
}
