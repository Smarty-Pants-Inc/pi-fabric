import type { MeshLockProtocol } from "../config.js";
import fs from "node:fs";
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

  /**
   * The R20 write fence (StateBackend.withWriteFence): `operation` runs synchronously while no state
   * commit can happen. Only with `.lock` held or no lock at all: never take `.lock` inside it.
   */
  withStateWriteFence<T>(operation: () => T): T {
    return this.#state.withWriteFence(operation);
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
