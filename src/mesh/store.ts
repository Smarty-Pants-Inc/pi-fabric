import type { MeshLockProtocol } from "../config.js";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { MeshLock, type MeshStoreContext } from "./mesh-lock.js";
import { withMeshCustody } from "./custody-lock.js";
import type { MeshReadOptions, MeshStateEntry, MeshBatchResult } from "./state-file.js";
import { createStateBackend, type MeshStateBackendKind, type StateBackend, type StateBackendBatchInput,
  type StateBackendDiagnostics } from "./state-backend.js";
import { EventLog, type MeshEvent, type MeshIdentity, type MeshPublishInput, type MeshTailResult } from "./event-log.js";
import { indexResidentDeliveries, type WakeSubscription } from "../residency/wake-index.js";
export { MeshLockTimeoutError } from "../core/atomic-write.js";
export type { MeshIdentity, MeshEvent, MeshPublishInput, MeshTailResult } from "./event-log.js";
export { meshCursorGeneration, meshCursorAtStart, MeshDedupeRecoveryError, MeshDedupeStoreFullError } from "./event-log.js";
export type { MeshStateEntry, MeshReadOptions, MeshBatchOperation, MeshBatchView, MeshBatchResult } from "./state-file.js";
export { RUNTIME_MESH_READ_CACHE_MS, MIN_BACKGROUND_MESH_READ_CACHE_MS, assertMeshStateReadable, MeshBatchConflictError } from "./state-file.js";
export type { MeshStateBackendKind, MeshCommitEffects, MeshStateFileRead, StateBackendBatchInput, StateBackendDiagnostics } from "./state-backend.js";

// The public mesh store (smarty-dev#6477 L0): a thin facade over three lock domains.
//   mesh-lock.ts   the `.lock` acquisition, its tickets, timeouts, metrics and stale-owner recovery;
//                  the lock order is written there.
//   state-file.ts  keyed state on state.json; state-backend.ts selects it, sqlite or shadow (L2a).
//   event-log.ts   events, receipts, the live log, compaction and the archive.
// Each domain keeps its own private fields; they share only the context (root, bounds, lock).

// Census record validation (smarty-dev#6477 L4a). host-leases.ts holds the same predicates for
// leases and the census; they are repeated here on purpose, because either module importing the
// other's values splits a new chunk into the startup graph (assert:build-artifacts budget).
const validWriterPid = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;
const validWriterHost = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const validWriterStartedAt = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) > 0;
const validWriterLockProtocol = (value: unknown): value is number => value === 1 || value === 2;
/** Backends by danger to a cutover: sqlite and shadow write state without .lock (pi-fabric#638). */
const writerBackendRank: Readonly<Record<string, number>> = { file: 0, shadow: 1, sqlite: 2 };
const validWriterStateBackend = (value: unknown): value is string => typeof value === "string" && Object.hasOwn(writerBackendRank, value);

/**
 * Filesystem-safe host identity for census file names: the sanitized hostname (bounded) plus a
 * hash of the exact hostname, so hosts that sanitize or case-fold alike still get distinct names.
 */
export const censusHostSlug = (host: string): string =>
  `${host.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 64) || "_"}-${createHash("sha256").update(host).digest("hex").slice(0, 12)}`;

/** `<hostSlug>-<pid>-<startedAt>.json`: unique per host incarnation, so hosts never share a record. */
export const censusRecordFileName = (host: string, pid: number, startedAt: number): string =>
  `${censusHostSlug(host)}-${pid}-${startedAt}.json`;

/** This process's census start time (epoch ms), fixed at module load. */
export const meshProcessStartedAt = Math.floor(Date.now() - process.uptime() * 1000);

/**
 * Linux only: a pid's clock-independent incarnation, the kernel boot id and the pid's start time in
 * clock ticks since boot (/proc/<pid>/stat field 22); else undefined. The wall clock never decides
 * liveness: it steps (NTP, VM resume), so a start time in epoch ms cannot prove a pid was reused
 * (smarty-dev#6982).
 */
const procIncarnation = (pid: number): { bootId: string; startTicks: number } | undefined => {
  if (process.platform !== "linux") return undefined;
  try {
    const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
    const startTicks = Number(stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19]);
    const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim();
    return Number.isSafeInteger(startTicks) && bootId.length > 0 ? { bootId, startTicks } : undefined;
  } catch { return undefined; }
};
let ownIncarnation: { bootId: string; startTicks: number } | null | undefined;

/**
 * Same-host census liveness (smarty-dev#6477 L4a, smarty-dev#6982): a record's process is dead only
 * on positive proof: its pid is gone (ESRCH), or the record names its incarnation (bootId,
 * startTicks) and the live pid is another one (another boot, or the same boot and another start
 * tick: the pid was reused). Without that proof (no incarnation, no /proc, EPERM) it stays live
 * (fail closed). Meaningless for another host's pid.
 */
export const censusRecordAlive = (pid: number, record?: unknown): boolean => {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; }
  const fields = typeof record === "object" && record !== null ? record as Record<string, unknown> : {};
  if (typeof fields.bootId !== "string" || fields.bootId.length === 0 || !Number.isSafeInteger(fields.startTicks)) return true;
  const current = procIncarnation(pid);
  return current === undefined || (current.bootId === fields.bootId && current.startTicks === fields.startTicks);
};

/**
 * The only census record that may be deleted: a format-1 record of exactly this host (host ===
 * os.hostname(), byte for byte) with a valid pid and positive start time whose process is gone
 * or whose pid now belongs to another incarnation. Another host, a missing or empty host, or an
 * invalid start time proves nothing here, so such a record is retained (fail closed).
 */
export const censusRecordPrunable = (value: unknown, host = os.hostname()): boolean => {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return record.format === 1 && validWriterPid(record.pid) && validWriterHost(record.host) && record.host === host &&
    validWriterStartedAt(record.startedAt) && !censusRecordAlive(record.pid, record);
};

/** Best effort: removes this host's census records whose process is gone; never throws. */
export const pruneDeadCensusRecords = (root: string): void => {
  const directory = path.join(root, ".writer-census");
  let names: string[];
  try { names = fs.readdirSync(directory); } catch { return; }
  const host = os.hostname();
  const prefix = `${censusHostSlug(host)}-`;
  for (const name of names) {
    // This host's records are <hostSlug>-<pid>-<startedAt>.json: another host's are never opened.
    // The name carries no incarnation, so only the content can prove a live pid was reused.
    if (!name.startsWith(prefix)) continue;
    if (!/^\d+-\d+\.json$/.test(name.slice(prefix.length))) continue;
    const file = path.join(directory, name);
    try {
      if (censusRecordPrunable(JSON.parse(fs.readFileSync(file, "utf8")), host)) fs.rmSync(file, { force: true });
    } catch { /* unreadable or already gone: left to the next prune */ }
  }
};

/**
 * Canonical filesystem identity of a census directory (created first): its real path, else its
 * device and inode. A mesh root and its symlink/junction alias reach the same census file, so
 * every per-root map below is keyed by this identity, never by the path a store was opened with
 * (pi-fabric#638): otherwise closing a store opened through one path would remove the record
 * another, still open, store opened through the other path relies on.
 */
const censusDirectoryIdentity = (directory: string, create = true): string => {
  if (create) fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  try { return fs.realpathSync.native(directory); }
  catch {
    const stat = fs.statSync(directory);
    return `${stat.dev}:${stat.ino}`;
  }
};

/** Census directories (by identity) whose dead records this process has pruned once. */
const prunedCensusRoots = new Set<string>();
/** This process's census records by directory identity: the file and its stores still open. */
const ownCensusRecords = new Map<string, { file: string; stores: number }>();
const removeOwnCensusRecords = (): void => {
  for (const { file } of ownCensusRecords.values()) try { fs.rmSync(file, { force: true }); } catch { /* best effort */ }
};

/** One store of this process closed: the record goes with the last one (else at exit). */
const releaseMeshWriterRecord = (identity: string): void => {
  const record = ownCensusRecords.get(identity);
  if (record === undefined) return;
  if (--record.stores > 0) return;
  ownCensusRecords.delete(identity);
  try { fs.rmSync(record.file, { force: true }); } catch { /* best effort; pruned once this process is gone */ }
};

const unique = <T>(values: T[]): T[] => [...new Set(values)].sort();

/**
 * Writes or widens this process's census record. One record per (root, host, pid, startedAt)
 * lists every backend and lock protocol any store of this process opened there; its top-level
 * stateBackend is the most dangerous of them (sqlite > shadow > file) and its lockProtocol the
 * oldest, so a census reader without the lists still sees the strongest writer. An existing
 * record that is not this process's valid record is never trusted or overwritten: the
 * registration fails (pi-fabric#638). Returns the failure, or undefined once recorded.
 */
const writeMeshWriterRecord = (directory: string, file: string, host: string, lockProtocol: number,
  stateBackend: string): { identity: string } | { error: unknown } => {
  try {
    const identity = censusDirectoryIdentity(directory);
    let text: string | undefined;
    try { text = fs.readFileSync(file, "utf8"); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
    let backends = [stateBackend], protocols = [lockProtocol];
    let releaseSha = process.env.PI_FABRIC_RELEASE_SHA ?? process.env.PI_FABRIC_BUILD_SHA ?? process.env.GITHUB_SHA ?? "unknown";
    if (text !== undefined) {
      let value: unknown;
      try { value = JSON.parse(text); } catch { value = undefined; }
      const prior = typeof value === "object" && value !== null ? value as Record<string, unknown> : {};
      const priorBackends = prior.stateBackends ?? [prior.stateBackend];
      const priorProtocols = prior.lockProtocols ?? [prior.lockProtocol];
      if (prior.format !== 1 || prior.pid !== process.pid || prior.host !== host || prior.startedAt !== meshProcessStartedAt ||
        typeof prior.releaseSha !== "string" || prior.releaseSha.length === 0 ||
        !validWriterStateBackend(prior.stateBackend) || !validWriterLockProtocol(prior.lockProtocol) ||
        !Array.isArray(priorBackends) || !priorBackends.includes(prior.stateBackend) || !priorBackends.every(validWriterStateBackend) ||
        !Array.isArray(priorProtocols) || !priorProtocols.includes(prior.lockProtocol) || !priorProtocols.every(validWriterLockProtocol)) {
        throw new Error(`existing census record ${file} is not this process's valid record`);
      }
      backends = unique([...priorBackends as string[], stateBackend]);
      protocols = unique([...priorProtocols as number[], lockProtocol]);
      releaseSha = prior.releaseSha;
    }
    const strongest = backends.reduce((a, b) => writerBackendRank[b]! > writerBackendRank[a]! ? b : a);
    if (ownIncarnation === undefined) ownIncarnation = procIncarnation(process.pid) ?? null;
    const writer = { format: 1, pid: process.pid, host, releaseSha, lockProtocol: Math.min(...protocols),
      stateBackend: strongest, startedAt: meshProcessStartedAt, ...(ownIncarnation ?? {}),
      ...(backends.length > 1 ? { stateBackends: backends } : {}), ...(protocols.length > 1 ? { lockProtocols: protocols } : {}) };
    const serialized = JSON.stringify(writer);
    if (serialized !== text) {
      const temporary = `${file}.${randomBytes(8).toString("hex")}.tmp`;
      fs.writeFileSync(temporary, serialized, { flag: "w", mode: 0o600 });
      try { fs.renameSync(temporary, file); }
      catch (error) { fs.rmSync(temporary, { force: true }); throw error; }
    }
    if (ownCensusRecords.size === 0) process.once("exit", removeOwnCensusRecords);
    const own = ownCensusRecords.get(identity);
    if (own === undefined) ownCensusRecords.set(identity, { file, stores: 1 });
    else own.stores += 1;
    return { identity };
  } catch (error) {
    return { error: error ?? new Error("census record write failed") };
  }
};

/**
 * Records this process in the census (one retry); returns the record's directory identity (the
 * key closeState() releases), or the failure. The writer census is advisory (smarty-dev#6982), so
 * a failed record never stops the store: the census then reports its evidence as unknown.
 */
const recordMeshWriter = (root: string, lockProtocol: number, stateBackend: string): { identity: string } | { error: unknown } => {
  const directory = path.join(root, ".writer-census");
  // Not created here: a missing directory has nothing to prune (and its path is then the key).
  let pruneKey: string;
  try { pruneKey = censusDirectoryIdentity(directory, false); } catch { pruneKey = path.resolve(directory); }
  if (!prunedCensusRoots.has(pruneKey)) {
    prunedCensusRoots.add(pruneKey);
    pruneDeadCensusRecords(root);
  }
  // Host-scoped: two hosts' writers with the same pid and start time never share a file, so neither
  // skips its record nor removes the other's on exit. Only an earlier store in this process matches.
  const host = os.hostname();
  const file = path.join(directory, censusRecordFileName(host, process.pid, meshProcessStartedAt));
  const first = writeMeshWriterRecord(directory, file, host, lockProtocol, stateBackend);
  return "identity" in first ? first : writeMeshWriterRecord(directory, file, host, lockProtocol, stateBackend);
};

export interface MeshStoreOptions {
  /** Captured at construction, never reloaded. Defaults to B68-compatible protocol 1. */
  lockProtocol?: MeshLockProtocol;
  maxEventLogBytes?: number;
  retainedEventLogBytes?: number;
  /** Receipt lifetime from publication; enforced at compaction/capacity pressure. Default 7 days. */
  dedupeReceiptTtlMs?: number;
  /** Hard cap on receipt/intent keys; protected pending intents can refuse new keys. Default 100,000. */
  maxDedupeReceipts?: number;
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

/** Distinct revisions for a SQLite stamp that could not be read: never equal, so never validating. */
let unreadableStateRevisions = 0;

export class MeshStore {
  readonly #lock: MeshLock;
  readonly #state: StateBackend;
  readonly #events: EventLog;
  /** This store's census registration (directory identity), released once by closeState(). */
  #censusRecord: string | undefined;

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
    this.#events = new EventLog(context, options, event => {
      if (!fs.existsSync(path.join(this.root, "residency"))) return;
      const subscriptions = event.topic === "fabric.participant.lifecycle"
        ? this.#state.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as WakeSubscription) : [];
      indexResidentDeliveries(this.root, event, subscriptions);
    });
    fs.mkdirSync(root, { recursive: true, mode: 0o700 });
    // Best effort: the writer census is advisory, never a gate (smarty-dev#6982), so a failed
    // record changes nothing here; the census reports the writer's evidence as unknown instead.
    const recorded = recordMeshWriter(root, this.#lock.lockProtocol, this.#state.kind);
    if ("identity" in recorded) this.#censusRecord = recorded.identity;
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
    // A closed sqlite backend cannot reopen and a closed shadow writes only through .lock: this
    // store no longer needs the record. Another open store of this process keeps it.
    const record = this.#censusRecord;
    this.#censusRecord = undefined;
    if (record !== undefined) releaseMeshWriterRecord(record);
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

  async publish(input: MeshPublishInput): Promise<MeshEvent> {
    const event = await this.#events.publish(input);
    await this.#wakeAfterPublish([event]);
    return event;
  }

  async publishBatch(inputs: MeshPublishInput[]): Promise<MeshEvent[]> {
    const events = await this.#events.publishBatch(inputs);
    await this.#wakeAfterPublish(events);
    return events;
  }

  async #wakeAfterPublish(events: readonly MeshEvent[]): Promise<void> {
    if (!events.length || !fs.existsSync(path.join(this.root, "residency"))) return;
    try {
      const { wakeResidentActors } = await import("../residency/wake.js");
      await wakeResidentActors(this, events);
    } catch (error) {
      // Already committed: throwing would invite a duplicate publish. The wake request and
      // archived event remain retryable; report failure rather than inventing delivery.
      console.warn(`[pi-fabric] resident wake deferred: ${String(error)}`);
    }
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

  /**
   * The committed-state revision of the ACTIVE backend, for validating an observation across a
   * state commit (publicationGeneration; pi-fabric#640 review round 1, P1). file and shadow:
   * undefined, because state.json is their authority and the caller stamps that file (bigint stat,
   * nanosecond times). sqlite: the backend stamp `<store>:<epoch>:<commit_no>`; SQLite state
   * commits never touch state.json, so its stat would pass a stale observation. A SQLite stamp that
   * cannot be read is unique, so a validation across it fails instead of passing.
   */
  stateRevision(): string | undefined {
    if (this.#state.kind !== "sqlite") return undefined;
    return this.#state.stateStamp() ?? `unreadable:${++unreadableStateRevisions}`;
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
