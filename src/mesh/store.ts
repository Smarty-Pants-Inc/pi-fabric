import type { MeshLockProtocol } from "../config.js";
import { diagnostics } from "../core/diagnostics.js";
import { MeshIncarnationPendingError, meshJournalAlarmLine, type MeshOutageAlarm } from "../core/atomic-write.js";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { delay, MeshLock, type MeshStoreContext } from "./mesh-lock.js";
import type { LockTimingContext } from "./commit-stats.js";
import { isNamespaceRace } from "../core/atomic-write.js";
import { acquireMeshCustodyLock, withMeshCustody } from "./custody-lock.js";
import type { MeshReadOptions, MeshStateEntry, MeshBatchResult, MeshBatchView } from "./state-file.js";
import { createStateBackend, isMeshStateWalCap, type MeshStateBackendKind, type StateBackend, type StateBackendBatchInput,
  type StateBackendDiagnostics, type StateChangeExceptions } from "./state-backend.js";
import { EventLog, type MeshEvent, type MeshIdentity, type MeshPublishInput, type MeshTailResult } from "./event-log.js";
import { closeResidentWakeRetryCursor, indexResidentDeliveries, type WakeSubscription } from "../residency/wake-index.js";
import { writeFileAtomic } from "../core/atomic-write.js";
import type { FabricMessageSender } from "../protocol.js";
import { copyFabricPrincipal, type FabricPrincipal } from "../fabric-provenance.js";
import { processSender } from "../scope.js";
import { verifiedRemove } from "../storage/verified-remove.js";
export { MeshIncarnationPendingError, MESH_INCARNATION_PENDING_CODE, isMeshIncarnationPending,
  MeshSnapshotRetriesExhaustedError, MESH_SNAPSHOT_RETRIES_EXHAUSTED_CODE, isMeshSnapshotRetriesExhausted,
  MeshLockTimeoutError } from "../core/atomic-write.js";
export type { MeshIdentity, MeshEvent, MeshPublishInput, MeshTailResult } from "./event-log.js";
export { meshCursorGeneration, meshCursorAtStart, MeshDedupeRecoveryError, MeshDedupeStoreFullError } from "./event-log.js";
export type { MeshStateEntry, MeshReadOptions, MeshBatchOperation, MeshBatchView, MeshBatchResult } from "./state-file.js";
export { RUNTIME_MESH_READ_CACHE_MS, MIN_BACKGROUND_MESH_READ_CACHE_MS, assertMeshStateReadable, MeshBatchConflictError } from "./state-file.js";
export type { MeshStateBackendKind, MeshCommitEffects, MeshStateFileRead, StateBackendBatchInput, StateBackendDiagnostics } from "./state-backend.js";

// Scheduled events and grant transactions (upstream pi-fabric 0.103+), ported onto the facade:
// schedules.json lives beside the mesh state, serialized by the mesh `.lock`, outside the
// verified storage kernel's per-key revision table.
const TOPIC_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;
export const validateMeshTopic = (topic: string): void => {
  if (!TOPIC_PATTERN.test(topic)) throw new Error(`Invalid Fabric mesh topic: ${topic}`);
};
export const MESH_MAX_PENDING_SCHEDULES = 1_000;
export const MESH_MAX_SCHEDULE_AHEAD_MS = 366 * 24 * 60 * 60 * 1_000;
const MAX_SCHEDULE_FILE_BYTES = 16 * 1024 * 1024;
const SCHEDULE_KEY_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,127}$/;

/** Host-internal append request used inside {@link MeshStore.transact}. */
export interface MeshAppendInput {
  id?: string;
  topic: string;
  kind?: string;
  from: MeshIdentity;
  to?: string;
  text?: string;
  data?: unknown;
  origin?: "external";
  untrusted?: true;
  grantId?: string;
  scheduled?: { dueAt: number; key?: string };
  sender?: FabricMessageSender;
  /** A tighter per-event byte ceiling than the store's own. */
  maxEventBytes?: number;
}

/** A pending scheduled event; released into its topic log once due. */
export interface MeshSchedule {
  id: string;
  key?: string;
  topic: string;
  kind: string;
  from: MeshIdentity;
  to?: string;
  text?: string;
  data?: unknown;
  dueAt: number;
  createdAt: number;
  /** Stamped when scheduled; carried onto the released event. */
  sender?: FabricMessageSender;
  /** The scheduling invocation's principal; revalidated and carried onto the released event. */
  principal?: FabricPrincipal | undefined;
}

interface MeshScheduleFile {
  format: 1;
  schedules: MeshSchedule[];
}

const compareSchedules = (left: MeshSchedule, right: MeshSchedule): number =>
  left.dueAt - right.dueAt || left.createdAt - right.createdAt || left.id.localeCompare(right.id);

const isMeshSchedule = (value: unknown): value is MeshSchedule => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const entry = value as Partial<MeshSchedule>;
  return typeof entry.id === "string" && typeof entry.topic === "string" &&
    typeof entry.kind === "string" && typeof entry.from === "object" && entry.from !== null &&
    Number.isSafeInteger(entry.dueAt) && Number.isSafeInteger(entry.createdAt) &&
    (entry.key === undefined || typeof entry.key === "string");
};

const scheduleErrorCode = (error: unknown): string | undefined =>
  typeof error === "object" && error !== null && "code" in error ? String((error as { code?: unknown }).code) : undefined;

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
const validWriterLockProtocol = (value: unknown): value is number => value === 1 || value === 2 || value === 3;
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
      // smarty-dev#8907: the stat and the judged content come from one fd, so only that judged inode is removed
      // (a record swapped in after the judgment stays); off Linux main's removal (smarty-dev#7858).
      const fd = fs.openSync(file, "r");
      let judged: fs.Stats, text: string;
      try { judged = fs.fstatSync(fd); text = fs.readFileSync(fd, "utf8"); } finally { fs.closeSync(fd); }
      if (censusRecordPrunable(JSON.parse(text), host)) {
        verifiedRemove(file, judged, { deleter: "censusRecordPrune", fallback: () => { fs.rmSync(file, { force: true }); return true; } });
      }
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
// ponytail: no judgment precedes these two removals: the name <hostSlug>-<pid>-<startedAt>.json is this live process's
// own record, written only by it (writeMeshWriterRecord refuses any other content), and no other process prunes it
// while this incarnation is alive (censusRecordPrunable), so no aged or judged inode can be swapped in.
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
      // ponytail: our own unpublished temporary (random name, created just above), removed only after its rename failed.
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
   * A `.lock` or SQLite writer-lock hold longer than this emits a `held` event to
   * `<root>/lock-stats/events.jsonl` (smarty-dev#9290). Default 1,000 ms; 0 disables them.
   */
  lockHeldEventMs?: number;
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
  /** A resident startup owns a scoped recovery pass before opening ordinary dispatch. */
  canWakeResidents?: () => boolean;
}

/** Distinct revisions for a SQLite stamp that could not be read: never equal, so never validating. */
let unreadableStateRevisions = 0;

/** The SQLite backend's revision views (SqliteStateBackend), reached without a value import. */
interface SqliteStateViews {
  unchangedSince(stamp: string, prefixes: readonly string[], exceptKeys?: StateChangeExceptions): boolean;
  stateView(): { stamp: string; view: MeshBatchView };
}

/** A background mesh path whose writes stay deferred past MESH_OUTAGE_ALARM_AFTER_MS, and its recovery (smarty-dev#8825). */
export const FABRIC_MESH_OUTAGE_ALARM_TOPIC = "fabric.alarm.mesh-outage";
const ALARM_SESSION = /^session:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
let refusedAlarmTo: string | undefined;
/** Whether a value is the one accepted alarm target, `session:<uuid>` (also the bridge's check on a carried alarm). */
export const isMeshAlarmTarget = (value: unknown): value is string => typeof value === "string" && ALARM_SESSION.test(value);
/**
 * Who a mesh-health alarm goes to (pi-fabric#893 rounds 3-5, R-org-delegate; owner decision by fabric-store): never
 * the whole shared mesh, and only ever one session. `PI_FABRIC_MESH_ALARM_TO` must be exactly `session:<uuid>`
 * (lowercase canonical uuid). Anything else (a bare name such as org or fabric-store, a topic, whitespace, any other
 * form) is refused, with one stderr line naming it, and the launcher's `SMARTY_LEAD_SESSION` is used only if it is a
 * `session:<uuid>`. Neither: undefined, and the alarm stays on stderr only (fail closed). Whether the session is a
 * principal's is not checked here: accepted security gap smarty-dev#9820. ponytail: the env only, not the
 * `.local/lead` marker walk, which would pull topology/project-identity into the store's eager startup graph.
 */
export const meshAlarmRecipient = (env: NodeJS.ProcessEnv = process.env): string | undefined => {
  const configured = env.PI_FABRIC_MESH_ALARM_TO;
  if (configured !== undefined && configured !== "") {
    if (ALARM_SESSION.test(configured)) return configured;
    if (refusedAlarmTo !== configured) {
      refusedAlarmTo = configured;
      diagnostics.warn("src/mesh/store.ts", `[pi-fabric] PI_FABRIC_MESH_ALARM_TO refused (${JSON.stringify(configured.slice(0, 80))}): `
        + "only session:<uuid>; mesh alarms fall back to SMARTY_LEAD_SESSION or stderr only");
    }
  }
  const lead = env.SMARTY_LEAD_SESSION;
  return lead !== undefined && ALARM_SESSION.test(lead) ? lead : undefined;
};

/** The MeshBackgroundRetry.alarmsTo sink of an owner: resolved at alarm time (owners assign their mesh after field init). */
export const meshOutageAlarmSink = (owner: () => { mesh: Pick<MeshStore, "publish"> | undefined; from: MeshIdentity | undefined }, env: NodeJS.ProcessEnv = process.env) =>
  (alarm: MeshOutageAlarm): Promise<unknown> | void => {
    const { mesh, from } = owner();
    const to = meshAlarmRecipient(env);
    if (!mesh || !from || !to) return; // no recipient: the stderr line is the only sink, never a broadcast
    return mesh.publish({ topic: FABRIC_MESH_OUTAGE_ALARM_TOPIC, to, kind: `mesh-${alarm.state}`, from,
      text: `${alarm.label}: mesh writes ${alarm.state === "outage" ? "deferred" : "recovered"} after ${Math.floor(alarm.durationMs / 1_000)}s`,
      data: { ...alarm } });
  };

/** A SQLite WAL-cap refusal (FABRIC_MESH_STATE_WAL_CAP): a reader pins the WAL above the hard cap (smarty-dev#7451). */
export const MESH_WAL_CAP_ALARM_TOPIC = "fabric.alarm.mesh-wal-cap";
const MESH_WAL_CAP_ALARM_MS = 10 * 60 * 1_000;

/** Transport-owned publisher. Closing stops work and joins the in-flight attempt; durable rows stay local. */
export interface MeshEventPublisherBinding {
  retry(): Promise<void>;
  close(): Promise<void>;
}

export class MeshStore {
  readonly #lock: MeshLock;
  readonly #state: StateBackend;
  readonly #events: EventLog;
  readonly #canWakeResidents: () => boolean;
  readonly #schedulesPath: string;
  #scheduleCache:
    | { inode: number; size: number; modifiedAt: number; file: MeshScheduleFile; error?: string }
    | undefined;
  /** Parsed wake routes are private to this store and revalidated under the publication lock. */
  readonly #wakeRouteCache = new Map<string, unknown>();
  /** This store's census registration (directory identity), released once by closeState(). */
  #censusRecord: string | undefined;
  #eventPublisher: MeshEventPublisherBinding | undefined;

  constructor(
    readonly root: string,
    readonly maxEventBytes: number,
    readonly maxReadEvents: number,
    options: MeshStoreOptions = {},
  ) {
    this.#canWakeResidents = options.canWakeResidents ?? (() => true);
    // A failed operation under the lock drops the parsed state (see MeshLock.withLock).
    this.#lock = new MeshLock(root, options, () => this.#state.dropCache());
    const context: MeshStoreContext = { root, maxEventBytes, maxReadEvents, lock: this.#lock };
    this.#state = createStateBackend(context, options);
    this.#events = new EventLog(context, options, event => {
      if (!fs.existsSync(path.join(this.root, "residency"))) return;
      const subscriptions = event.topic === "fabric.participant.lifecycle"
        ? this.#state.listAll("topology/subscriptions/", { fresh: true }).map(entry => entry.value as WakeSubscription) : [];
      indexResidentDeliveries(this.root, event, subscriptions, this.#wakeRouteCache);
    });
    this.#schedulesPath = path.join(root, "schedules.json");
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

  /** Projection-applied state change hints (see `StateBackend.subscribeStateChanges`); a no-op where the backend has none. */
  subscribeStateChanges(listener: () => void): () => void {
    return this.#state.subscribeStateChanges?.(listener) ?? (() => undefined);
  }

  stateDiagnostics(): StateBackendDiagnostics {
    return this.#state.diagnostics();
  }

  /** Releases the state database handle (sqlite, shadow); the file backend holds none. */
  closeState(): void {
    // The transport joins this same close before releasing its connection. A synchronous state
    // teardown can at least stop admission now; no detached worker may start another publish.
    void this.#eventPublisher?.close();
    closeResidentWakeRetryCursor(this.root);
    this.#wakeRouteCache.clear();
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

  /** Bind only from an opened transport. No NATS runtime dependency, dial or import lives in MeshStore.
   * Local commits still return their local sequence; forwarding never rewrites the local/control log.
   * A forwarding failure cannot reject a committed publish and invite a fresh event id from its caller. */
  bindEventPublisher(scope: string, publish: (event: MeshEvent) => Promise<unknown>, validate?: (event: MeshEvent) => void): MeshEventPublisherBinding {
    if (this.#eventPublisher) throw new Error("Mesh event publisher is already bound");
    let closed = false;
    let queued: NodeJS.Immediate | undefined;
    let pending: Promise<void> | undefined;
    const retry = (): Promise<void> => {
      if (closed) return Promise.resolve();
      if (pending) return pending;
      const run = async (): Promise<void> => {
        // Serialize send + checkpoint across stores/processes, but NEVER hold the shared mesh
        // lock across network I/O. A live/foreign delivery-custody holder is not age-reclaimed.
        const release = await acquireMeshCustodyLock(path.join(this.#events.eventOutboxDirectory(scope), ".delivery"), 0,
          { hostQualified: true, lockProtocol: this.#lock.lockProtocol });
        try {
          while (!closed) {
            const event = await this.#events.nextOutboxEvent(scope);
            if (!event || closed) return;
            // Includes a retry after an uncertain answer: the immutable row keeps the ORIGINAL id
            // and resolved body. Never call local publish(), stamp data again or allocate another id.
            await publish(event);
            this.#events.ackOutboxEvent(scope, event);
          }
        } finally { release(); }
      };
      const task = run().catch(error => {
        diagnostics.warn("src/mesh/store.ts", `[pi-fabric] mesh event forwarding deferred (original ids retained): ${String(error)}`);
      }).finally(() => { if (pending === task) pending = undefined; });
      pending = task;
      return task;
    };
    const schedule = (): void => {
      if (closed || queued) return;
      // This is driven by a real committed append, not registration/idle work. Even a partial
      // transaction schedules it; setImmediate leaves the synchronous mesh critical section first.
      queued = setImmediate(() => { queued = undefined; void retry(); });
    };
    this.#events.enableOutbox(scope, schedule, validate);
    const binding: MeshEventPublisherBinding = {
      retry,
      close: () => {
        closed = true;
        if (queued) clearImmediate(queued);
        queued = undefined;
        return pending ?? Promise.resolve();
      },
    };
    this.#eventPublisher = binding;
    // Recover rows from a previous activation only after an actual transport open.
    schedule();
    return binding;
  }

  /** Reuse owner maintenance/reconnect, not a second polling timer. No-op with events disabled. */
  retryEventPublications(): Promise<void> {
    return this.#eventPublisher?.retry() ?? Promise.resolve();
  }

  async publish(input: MeshPublishInput): Promise<MeshEvent> {
    // Due schedules go first so the log keeps due-time order relative to this event; damaged
    // schedule state must not block ordinary publishing (upstream). With nothing due this stays
    // synchronous up to the EventLog call: no extra await or lock on the ordinary path.
    // releaseDueSchedules wakes the recipients of released schedules; this wakes the event's.
    const stamped = { ...input, sender: input.sender ?? processSender() };
    if (this.#scheduleDue()) await this.releaseDueSchedules().catch(() => undefined);
    const event = await this.#events.publish(stamped);
    await this.#wakeAfterPublish([event]);
    return event;
  }

  async publishBatch(inputs: MeshPublishInput[], lockTiming?: LockTimingContext): Promise<MeshEvent[]> {
    const stamped = inputs.map((input) => ({ ...input, sender: input.sender ?? processSender() }));
    if (this.#scheduleDue()) await this.releaseDueSchedules().catch(() => undefined);
    const events = await this.#events.publishBatch(stamped, lockTiming);
    await this.#wakeAfterPublish(events);
    return events;
  }

  async #wakeAfterPublish(events: readonly MeshEvent[]): Promise<void> {
    if (!events.length || !this.#canWakeResidents() || !fs.existsSync(path.join(this.root, "residency"))) return;
    try {
      const { wakeResidentActors } = await import("../residency/wake.js");
      await wakeResidentActors(this, events, undefined, undefined, () => this.#canWakeResidents());
    } catch (error) {
      // Already committed: throwing would invite a duplicate publish. The durable pending
      // index and archived event survive even without a wake request; scoped startup,
      // maintenance or the next matching publish retries it. Report failure rather than inventing delivery.
      diagnostics.warn("src/mesh/store.ts", `[pi-fabric] resident wake deferred: ${String(error)}`);
    }
  }

  /** Existing owner maintenance cadence only; respect publication's resident admission gate. */
  async retryResidentWakes(): Promise<void> {
    if (!this.#canWakeResidents() || !fs.existsSync(path.join(this.root, "residency"))) return;
    const { retryResidentWakes } = await import("../residency/wake.js");
    // Import preparation may have yielded to staging, drain or handover.
    if (!this.#canWakeResidents()) return;
    await retryResidentWakes(this, undefined, undefined, () => this.#canWakeResidents());
  }

  #scheduleDue(now = Date.now()): boolean {
    try {
      const next = this.nextScheduleDueAt();
      return next !== undefined && next <= now;
    } catch {
      return false;
    }
  }

  /**
   * Host-internal: runs `operation` under the mesh lock with an append
   * capability, for side stores (grants) that must commit with an event.
   */
  async transact<T>(operation: (append: (input: MeshAppendInput) => MeshEvent) => T): Promise<T> {
    const committed: MeshEvent[] = [];
    const result = await this.#lock.withLock(() => {
      try { committed.push(...this.#releaseDueLocked(Date.now())); } catch { /* schedule mutations report it */ }
      return operation((input) => {
        validateMeshTopic(input.topic);
        const event = this.#events.appendLocked(input);
        committed.push(event);
        return event;
      });
    }, undefined, "publish");
    // Committed appends wake dormant resident recipients, as publish() does.
    await this.#wakeAfterPublish(committed);
    return result;
  }

  /**
   * Stores a pending event released into `topic` once `dueAt` passes. A `key`
   * replaces any pending schedule with that key in the same locked write, so
   * concurrent publishers never leave two schedules for one key.
   */
  async schedule(input: {
    topic: string;
    kind?: string;
    from: MeshIdentity;
    to?: string;
    text?: string;
    data?: unknown;
    dueAt: number;
    key?: string;
    /** Runtime-captured originating principal (host-only), as for publish. */
    principal?: FabricPrincipal | undefined;
    /** Checked under the lock before the schedule is written, as for publish. */
    signal?: AbortSignal | undefined;
  }, now = Date.now()): Promise<MeshSchedule> {
    input.signal?.throwIfAborted();
    validateMeshTopic(input.topic);
    if (input.to !== undefined && !input.to.trim()) throw new Error("Mesh recipient is empty");
    if (input.key !== undefined && !SCHEDULE_KEY_PATTERN.test(input.key)) {
      throw new Error(`Invalid Fabric mesh schedule key: ${input.key}`);
    }
    if (!Number.isSafeInteger(input.dueAt) || input.dueAt < 0) {
      throw new Error("Mesh schedule due time must be a nonnegative epoch millisecond integer");
    }
    if (input.dueAt > now + MESH_MAX_SCHEDULE_AHEAD_MS) {
      throw new Error("Mesh schedules may be at most 366 days ahead");
    }
    const schedule: MeshSchedule = {
      id: randomUUID(),
      ...(input.key !== undefined ? { key: input.key } : {}),
      topic: input.topic,
      kind: input.kind?.trim() || "message",
      from: JSON.parse(JSON.stringify(input.from)) as MeshIdentity,
      ...(input.to ? { to: input.to } : {}),
      ...(input.text !== undefined ? { text: input.text } : {}),
      ...(input.data !== undefined ? { data: JSON.parse(JSON.stringify(input.data)) as unknown } : {}),
      dueAt: input.dueAt,
      createdAt: now,
      sender: processSender(),
      ...(copyFabricPrincipal(input.principal) ? { principal: copyFabricPrincipal(input.principal) } : {}),
    };
    // Reject at schedule time what release could never append.
    const released: MeshEvent = {
      id: schedule.id, sequence: Number.MAX_SAFE_INTEGER, topic: schedule.topic, kind: schedule.kind,
      from: schedule.from, ...(schedule.to ? { to: schedule.to } : {}),
      ...(schedule.text !== undefined ? { text: schedule.text } : {}),
      ...(schedule.data !== undefined ? { data: schedule.data } : {}),
      scheduled: { dueAt: schedule.dueAt, ...(schedule.key !== undefined ? { key: schedule.key } : {}) },
      ...(schedule.sender ? { sender: schedule.sender } : {}),
      ...(schedule.principal ? { principal: schedule.principal } : {}),
      createdAt: Number.MAX_SAFE_INTEGER,
    };
    if (Buffer.byteLength(JSON.stringify(released), "utf8") > this.maxEventBytes) {
      throw new Error(`Mesh event exceeds ${this.maxEventBytes} bytes`);
    }
    let releasedEvents: MeshEvent[] = [];
    const scheduled = await this.#lock.withLock(() => {
      // A caller cancelled while this waited for the lock must not commit: the provider's
      // abortable race rejects the caller but cannot stop this promise.
      input.signal?.throwIfAborted();
      const file = this.#readSchedules(false);
      const pending = file.schedules.filter((entry) => input.key === undefined || entry.key !== input.key);
      if (pending.length >= MESH_MAX_PENDING_SCHEDULES) {
        throw new Error(`At most ${MESH_MAX_PENDING_SCHEDULES} mesh schedules may be pending`);
      }
      pending.push(schedule);
      this.#writeSchedules(pending);
      releasedEvents = this.#releaseDueLocked(now);
      return JSON.parse(JSON.stringify(schedule)) as MeshSchedule;
    }, undefined, "publish");
    await this.#wakeAfterPublish(releasedEvents);
    return scheduled;
  }

  /** Removes the pending schedule with `key`, if any. */
  async unschedule(key: string): Promise<{ removed: boolean }> {
    if (!SCHEDULE_KEY_PATTERN.test(key)) throw new Error(`Invalid Fabric mesh schedule key: ${key}`);
    return this.#lock.withLock(() => {
      const file = this.#readSchedules(false);
      const pending = file.schedules.filter((entry) => entry.key !== key);
      if (pending.length === file.schedules.length) return { removed: false };
      this.#writeSchedules(pending);
      return { removed: true };
    }, undefined, "publish");
  }

  /** Pending schedules by due time; a due one stays listed until a releaser appends it. */
  scheduled(input: { topic?: string; limit?: number } = {}): MeshSchedule[] {
    if (input.topic !== undefined) validateMeshTopic(input.topic);
    const limit = Math.max(1, Math.min(Math.floor(input.limit ?? 100), MESH_MAX_PENDING_SCHEDULES));
    return this.#readSchedules(true).schedules
      .filter((entry) => input.topic === undefined || entry.topic === input.topic)
      .sort(compareSchedules)
      .slice(0, limit)
      .map((entry) => JSON.parse(JSON.stringify(entry)) as MeshSchedule);
  }

  /** Earliest pending due time, read without the lock (one stat when unchanged). */
  nextScheduleDueAt(): number | undefined {
    let next: number | undefined;
    for (const entry of this.#readSchedules(true).schedules) {
      if (next === undefined || entry.dueAt < next) next = entry.dueAt;
    }
    return next;
  }

  /**
   * Appends every due schedule to its topic and removes it, under the mesh
   * lock, so concurrent releasers (monitors, resident hosts, CLI posts) append
   * each schedule once. The lock is taken only when something is due.
   */
  async releaseDueSchedules(now = Date.now()): Promise<MeshEvent[]> {
    const next = this.nextScheduleDueAt();
    if (next === undefined || next > now) return [];
    const released = await this.#lock.withLock(() => this.#releaseDueLocked(now), undefined, "publish");
    // Released schedules are deliveries: wake their dormant resident recipients.
    await this.#wakeAfterPublish(released);
    return released;
  }

  #releaseDueLocked(now: number): MeshEvent[] {
    if (!fs.existsSync(this.#schedulesPath)) return [];
    const file = this.#readSchedules(false);
    const due = file.schedules.filter((entry) => entry.dueAt <= now).sort(compareSchedules);
    if (due.length === 0) return [];
    const released: MeshEvent[] = [];
    const releasedIds = new Set<string>();
    try {
      // Events first, then the shrunken schedule file: a crash in between
      // re-releases with the same event id (at-least-once, dedupe by id).
      for (const entry of due) {
        released.push(this.#events.appendLocked({
          id: entry.id,
          topic: entry.topic,
          kind: entry.kind,
          from: entry.from,
          ...(entry.to ? { to: entry.to } : {}),
          ...(entry.text !== undefined ? { text: entry.text } : {}),
          ...(entry.data !== undefined ? { data: entry.data } : {}),
          scheduled: { dueAt: entry.dueAt, ...(entry.key !== undefined ? { key: entry.key } : {}) },
          // Older schedules stay unstamped: the releasing host never lends its own authority.
          ...(entry.sender ? { sender: entry.sender } : {}),
          // The scheduler's principal, revalidated: a damaged record carries none.
          ...(copyFabricPrincipal(entry.principal) ? { principal: copyFabricPrincipal(entry.principal) } : {}),
        }));
        releasedIds.add(entry.id);
      }
    } finally {
      if (releasedIds.size > 0) {
        this.#writeSchedules(file.schedules.filter((entry) => !releasedIds.has(entry.id)));
      }
    }
    return released;
  }

  #readSchedules(lenient: boolean): MeshScheduleFile {
    let stat: fs.Stats;
    try {
      stat = fs.statSync(this.#schedulesPath);
    } catch (error) {
      this.#scheduleCache = undefined;
      if (scheduleErrorCode(error) === "ENOENT") return { format: 1, schedules: [] };
      throw error;
    }
    // Locked mutations always re-read; the stat-keyed cache serves only lockless reads.
    const cached = lenient ? this.#scheduleCache : undefined;
    if (cached && cached.inode === stat.ino && cached.size === stat.size && cached.modifiedAt === stat.mtimeMs) {
      return cached.error === undefined ? cached.file : { format: 1, schedules: [] };
    }
    let file: MeshScheduleFile = { format: 1, schedules: [] };
    let damage: string | undefined;
    try {
      if (stat.size > MAX_SCHEDULE_FILE_BYTES) throw new Error(`exceeds ${MAX_SCHEDULE_FILE_BYTES} bytes`);
      const parsed = JSON.parse(fs.readFileSync(this.#schedulesPath, "utf8")) as Partial<MeshScheduleFile>;
      if (parsed?.format !== 1 || !Array.isArray(parsed.schedules) || !parsed.schedules.every(isMeshSchedule)) {
        throw new Error("invalid format");
      }
      file = { format: 1, schedules: parsed.schedules };
    } catch (error) {
      damage = error instanceof Error ? error.message : String(error);
    }
    this.#scheduleCache = {
      inode: stat.ino, size: stat.size, modifiedAt: stat.mtimeMs, file, ...(damage !== undefined ? { error: damage } : {}),
    };
    if (damage === undefined) return file;
    // Reads tolerate damage; mutations refuse to overwrite it silently.
    if (lenient) return { format: 1, schedules: [] };
    throw new Error(`Failed to read Fabric mesh schedules: ${damage}`);
  }

  #writeSchedules(schedules: MeshSchedule[]): void {
    if (schedules.length === 0) {
      // ponytail: not an expiry judgment: a locked state write (callers re-read under the mesh lock), the empty
      // schedule list, exactly as writeFileAtomic below replaces the name; no other writer changes it unlocked.
      fs.rmSync(this.#schedulesPath, { force: true });
      this.#scheduleCache = undefined;
      return;
    }
    const serialized = JSON.stringify({ format: 1, schedules } satisfies MeshScheduleFile);
    if (Buffer.byteLength(serialized, "utf8") > MAX_SCHEDULE_FILE_BYTES) {
      throw new Error(`Fabric mesh schedules exceed ${MAX_SCHEDULE_FILE_BYTES} bytes`);
    }
    writeFileAtomic(this.#schedulesPath, serialized);
    this.#scheduleCache = undefined;
  }

  read(input: { after?: number; topic?: string; to?: string; limit?: number } = {}): MeshEvent[] {
    return this.#events.read(input);
  }

  /**
   * Durability barrier (smarty-dev#9313): resolves once every event AND every keyed-state commit
   * made on this root before the call, by any process, is on disk; otherwise it throws, for SQLite
   * state within about `budgetMs`. A trusted producer awaits it before it fsyncs its own cursor or
   * delivered mark, so an OS crash can never keep a cursor past an event or state record it covers.
   * No writer lock is held while it syncs: events use the group-commit barrier of a durable publish
   * (fsync of events.jsonl and its namespace after `.lock`); SQLite state uses the PASSIVE
   * checkpoint barrier (smarty-dev#6100: no copy or fsync under SQLite's writer lock); file state
   * fsyncs state.json and its namespace. On Windows (no directory barrier) the event barrier fails
   * closed first with MeshEventsNotDurableError, and file state would with MeshStateFileNotDurableError;
   * every refusal is a MeshSyncNotDurableError. A caller must not advance its external record on a throw.
   *
   * It resolves with `through`, the highest event sequence committed to the log (a complete line)
   * before this barrier's fsync, so every event at or below it that is still live is now on disk.
   * Callers must advance an event cursor only to `through`, never to latestSequence(): that
   * includes the reservation counter, which a publisher advances under `.lock` before it appends,
   * so a cursor past `through` could skip an event appended after the barrier (pi-fabric#894).
   */
  async sync(budgetMs = 1_000): Promise<{ through: number }> {
    if (!Number.isFinite(budgetMs) || budgetMs < 0) throw new Error("Mesh sync budgetMs must be a finite number >= 0");
    const deadline = Date.now() + budgetMs;
    let through: number;
    for (;;) {
      try { through = await this.#events.sync(); break; }
      catch (error) {
        // A compaction renamed the live log during the namespace check; the next barrier covers it.
        if (!isNamespaceRace(error) || Date.now() >= deadline) throw error;
        await delay(1 + Math.random() * 3);
      }
    }
    await this.#state.sync(Math.max(0, deadline - Date.now()));
    return { through };
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
    return this.#alarmWalCap(this.#state.put(input), input.identity);
  }

  delete(input: { key: string; ifVersion?: number }): Promise<{ deleted: boolean; version?: number }> {
    return this.#alarmWalCap(this.#state.delete(input));
  }

  /** Transaction and callback semantics: StateBackendBatchInput (state-backend.ts). */
  writeBatch(input: StateBackendBatchInput): Promise<MeshBatchResult[]> {
    return this.#alarmWalCap(this.#state.writeBatch(input), input.identity);
  }

  // smarty-dev#7451 item 6: a write the SQLite WAL cap refused (a reader pins the WAL) raises one log line and one
  // MESH_WAL_CAP_ALARM_TOPIC event per store at most every MESH_WAL_CAP_ALARM_MS. The event goes to events.jsonl,
  // which the cap does not gate. The refusal itself is rethrown unchanged (callers retry it, item 8).
  // smarty-dev#9513: the event names the refused operation's own writer (a delete has none: the store's process), and
  // carries only validated sizes and a reader COUNT, never reader pids or comm. Errors can cross the bridge too.
  #walCapAlarmAt = Number.NEGATIVE_INFINITY;
  #alarmWalCap<T>(write: Promise<T>, identity?: MeshIdentity): Promise<T> {
    return write.catch((error: unknown) => {
      const now = Date.now();
      if (isMeshStateWalCap(error) && now - this.#walCapAlarmAt >= MESH_WAL_CAP_ALARM_MS) {
        this.#walCapAlarmAt = now;
        const fields = error as { walBytes?: unknown; capBytes?: unknown; readerCount?: unknown };
        const walBytes = typeof fields.walBytes === "number" && Number.isFinite(fields.walBytes) && fields.walBytes >= 0
          ? fields.walBytes : undefined;
        const capBytes = typeof fields.capBytes === "number" && Number.isFinite(fields.capBytes) && fields.capBytes > 0
          ? fields.capBytes : undefined;
        const readerCount = typeof fields.readerCount === "number" && Number.isSafeInteger(fields.readerCount) && fields.readerCount >= 0
          ? fields.readerCount : undefined;
        const hasSizes = walBytes !== undefined && capBytes !== undefined;
        const text = hasSizes
          ? `mesh state write refused by the SQLite WAL cap (${String(walBytes)} > ${String(capBytes)} bytes; `
            + `${readerCount ?? "unknown"} WAL reader(s))`
          : `mesh state write refused by the SQLite WAL cap (sizes not available; ${readerCount ?? "unknown"} WAL reader(s))`;
        // The journal alarm line #9729 pages on; no mesh root path, no reader pids (pi-fabric#893 rounds 4-5).
        diagnostics.warn("src/mesh/store.ts", meshJournalAlarmLine({ label: "mesh state writes", state: "outage", sinceMs: now,
          reason: `SQLite WAL cap: ${text}`, walBytes, capBytes, readerCount }));
        const from: MeshIdentity = identity ?? { id: `mesh-store:${process.pid}`, name: "Fabric mesh store", kind: "agent" };
        // publish() can throw synchronously (a size or validation refusal): it must never replace the WAL-cap error.
        const recipient = meshAlarmRecipient(); // no recipient: the log line only, never a broadcast
        try {
          if (recipient) void this.publish({ topic: MESH_WAL_CAP_ALARM_TOPIC, to: recipient, kind: "mesh-wal-cap", from, text,
            data: { root: this.root, walBytes, capBytes, readerCount, writer: identity ? identity.id : null } })
            .catch(() => undefined);
        } catch { /* the alarm is best effort */ }
      }
      throw error;
    });
  }

  confirmWritable(onAcquired?: (at: number) => void): Promise<void> {
    return this.#state.confirmWritable(onAcquired);
  }

  /**
   * The R20 write fence (StateBackend.withWriteFence): `operation` runs synchronously while no state
   * commit can happen. Only with `.lock` held or no lock at all: never take `.lock` inside it.
   */
  withStateWriteFence<T>(operation: () => T, timing?: LockTimingContext): T {
    return this.#state.withWriteFence(operation, timing);
  }

  /**
   * Runs synchronous validation against one live state snapshot. SQLite uses a WAL read
   * transaction: concurrent writers may commit, but every callback read sees the pinned state.
   * This does NOT exclude ownership takeovers or settle pending-writer/file-watch hints; event
   * appends requiring that exclusion must keep withStateWriteFence. Other backends retain their
   * existing write-fence contract, including its caller-held custody requirement.
   * The callback must not return a thenable or call state writers.
   */
  withStateReadSnapshot<T>(operation: () => T): T {
    return this.#state.withReadSnapshot
      ? this.#state.withReadSnapshot(operation)
      : this.#state.withWriteFence(operation);
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

  /**
   * smarty-dev#10391, sqlite only: whether no key under `prefixes` changed after `revision` (a `stateRevision()`),
   * from the change feed, cheap enough to run in `withStateWriteFence`. False on other backends or when unknown.
   */
  stateUnchangedSince(revision: string, prefixes: readonly string[], exceptKeys?: StateChangeExceptions): boolean {
    const state = this.#sqliteState();
    return state !== undefined && state.unchangedSince(revision, prefixes, exceptKeys);
  }

  /** smarty-dev#10391, sqlite only: a read-only view of the committed state now and its `stateRevision()`. */
  stateView(): { revision: string; view: MeshBatchView } | undefined {
    const state = this.#sqliteState();
    if (!state) return undefined;
    const { stamp, view } = state.stateView();
    return { revision: stamp, view };
  }

  #sqliteState(): (StateBackend & SqliteStateViews) | undefined {
    const state = this.#state as StateBackend & Partial<SqliteStateViews>;
    return state.kind === "sqlite" && typeof state.unchangedSince === "function" && typeof state.stateView === "function"
      ? state as StateBackend & SqliteStateViews : undefined;
  }

  cachedStateStamp(fresh = false, revalidateGeneration = false): string | undefined {
    return this.#state.cachedStateStamp(fresh, revalidateGeneration);
  }

  // The lock (mesh-lock.ts).

  /** Settle constructor-owned identity before opening a bridge or entering custody. UNKNOWN is allowed.
   * A cold caller already in a bounded scope must release custody, never wait for native preparation. */
  async prepareLock(): Promise<void> {
    if (this.#lock.ownIncarnation && !this.#lock.ownIncarnationReady && this.tryLockBudgetMs !== undefined) {
      throw new MeshIncarnationPendingError();
    }
    await this.#lock.ownIncarnation;
  }

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

  /** Per-host lease commits and recovery serialize without the shared state/custody lock.
   * A foreign commit gate is never PID/age-recovered: a paused synchronous CAS must finish
   * before recovery can change its receipt. Async lease-owned transactions retain this gate until
   * settled; recovery requires matching machine identity and death/current-boot evidence.
   * Without boot identity, only ESRCH plus the custody staleness bound permits recovery. Without a
   * machine id (Windows, macOS, a Linux without one), a record naming this host whose PID is gone
   * or now another process (another native incarnation) permits it; one with no or an invalid
   * incarnation (a failed or not yet settled native read) only when its PID is gone (pi-fabric#1562; custody-lock.ts
   * clearDeadCustodyLock);
   * a missing owner pid is an alarmed typed refusal, never an ordinary busy timeout. */
  async leaseCustody<T>(file: string, operation: () => T | Promise<T>, lockTimeoutMs = 0,
    options: { ownIncarnation?: string | undefined } = {}): Promise<T> {
    const hash = createHash("sha256").update(path.basename(file)).digest("hex");
    const legacy = path.join(this.root, "host-lease-commits", hash);
    // One-release compatibility: prefer an existing full-hash domain, including an idle one,
    // so its old readers/holders still share custody. New domains stay within Windows MAX_PATH.
    // ponytail: 64-bit prefix collisions among one mesh's hosts are negligible; a collision
    // only serializes two hosts on the same gate, never bypasses their lease-token checks.
    const domain = fs.existsSync(legacy) ? legacy
      : path.join(this.root, "host-lease-commits", hash.slice(0, 16));
    const release = await acquireMeshCustodyLock(domain, lockTimeoutMs, { ...options, hostQualified: true, lockProtocol: this.#lock.lockProtocol });
    try { return await operation(); } finally { release(); }
  }

  /** The active withTryLock budget in this async context, if any. */
  get tryLockBudgetMs(): number | undefined {
    const scope = this.#lock.tryLockScope.getStore();
    return scope?.active ? scope.timeoutMs : undefined;
  }
}
