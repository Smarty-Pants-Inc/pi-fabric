import { createHash, randomUUID } from "node:crypto";
import { diagnostics, logDiagnostic } from "../core/diagnostics.js";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { MeshBackgroundQueue, MeshBackgroundRetry, isMeshWriteDeferred } from "../core/atomic-write.js";
import { participantProject, ParticipantRoleGrant, repositoryOf } from "./project-identity.js";
import { isPlacementRunId, placementRunIdForParticipant } from "./placement-run-id.js";
import { mainPublicationFenced, MainPublicationFencedError } from "./main-publication-fence.js";
import type { FabricMainAgentInfo } from "../main-agent.js";
import { DirectoryNotReadyError, TransportUnavailableError, type FabricTransport } from "../transport/types.js";
import { assertMeshStateReadable, MeshStore, meshOutageAlarmSink, meshProcessStartedAt, type MeshBatchOperation, type MeshIdentity, type MeshStateEntry, type MeshReadOptions } from "../mesh/store.js";
import type {
  FabricControlAdmission,
  FabricHostRecord,
  FabricIsolatedSourceFailure,
  FabricParticipantInfo,
  FabricParticipantKind,
  FabricParticipantListOptions,
  FabricParticipantRecord,
  FabricParticipantSource,
  FabricPeerInfo,
} from "./types.js";

import { DIRECTORY_RETENTION_MS, reapDeadHostRecords } from "./host-reaper.js";
import { observeActorOwnership, ownershipPointReads, type ActorOwnershipObservation } from "./publication-generation.js";
import { compactExpiredHostRecords } from "./host-record-compaction.js";
import { effectiveLiveness } from "./liveness.js";
import { isLiveLegacyRootEntry, sessionLiveness, LEGACY_ROOT_LEASE_MS as PARTICIPANT_LEASE_MS } from "./legacy-root-liveness.js";
import {
  fileLeasesOnly,
  hostLeaseExpiry,
  hostLeasePath,
  hostLeasePredecessor,
  hostLiveness,
  hostMirrorLease,
  mirroredOwnerRoutingStatus,
  waitForMirroredLeaseRenewal,
  type MirrorLeaseWaitBudget,
  type FabricMirrorLease,
  LIVENESS_POLICY_KEY,
  readHostLease,
  readHostLeaseCurrent,
  readHostLeaseSnapshot,
  participantLeaseGraceMs,
  waitForHostLeaseRenewal,
  type RoutingLeaseWaitOptions,
  readHostLeases,
  removeHostLeaseIf,
  removeOwnedHostLease,
  FabricHostLeaseSupersededError,
  sameHostLeaseOwner,
  assertHostLeaseOwner,
  commitHostLeaseUnderCustody,
  withOwnedHostLease,
  type FabricHostLease,
  STATE_LEASE_RENEW_MS,
  TRANSPORT_HOST_LEASE_SAFETY_MS,
  renewHostLease,
  meshWriterLeaseRecord,
} from "./host-leases.js";
import { HostLeaseLockBusyError, withHostLeaseLock } from "./host-lease-lock.js";
import type { TransportHostLease } from "./host-leases.js";
import type { KvParticipantDirectory, ParticipantLeaseGate, ParticipantView } from "./participant-view.js";
import type { NatsLocalProjection, NatsLocalProjectionMetrics } from "./nats-local-projection.js";
import { peerLabelPrefix } from "./peer-settle.js";
import { rootParticipantName } from "./participant-name.js";
import { controlAdmissionAlarm, isolatedParticipantSourceAlarm } from "./stall-alarms.js";
import { knownOwnProcessIncarnation, ownProcessIncarnation } from "../core/atomic-write.js";
import {
  ParticipantFileLockBusyError,
  prepareParticipantFileLocks,
  type ParticipantFileLockOptions,
  participantFilePresent,
  participantFilesOnly,
  readParticipantFile,
  readParticipantFiles,
  removeParticipantFileIf,
  writeParticipantFileIf,
} from "./participant-files.js";

const PARTICIPANT_PREFIX = "topology/participants/";
/** A clean close retries its withdrawal batch this often when the mesh lock is busy
 * (smarty-dev#6622): a swallowed lock timeout used to leave the entry fresh. */
const CLOSE_WITHDRAW_ATTEMPTS = 3;
/** Of those attempts, at most this many may wait a full mesh lock timeout: a resident's
 * idle exit under a held lock must still end within its shutdown window, and a failed
 * withdrawal lapses with the unrenewed lease anyway (smarty-dev#6622, round 3). */
const CLOSE_WITHDRAW_LOCK_WAITS = 2;
/** Project-scoped monotonic counter backing Linear-style peer labels. Never shrinks. */
const PEER_SEQ_KEY = "topology/peer-seq";
const HOST_PREFIX = "topology/hosts/";
// Root-owned clean-close receipts survive record cleanup. Absence alone (including a
// lease-based reaper's cleanup) is not positive evidence that a lineage ended.
const LINEAGE_CLOSURE_PREFIX = "topology/lineage-closures/";
const LEGACY_SESSION_PREFIX = "sessions/";
const LEGACY_ACTOR_PREFIX = "actors/";
// smarty-dev#7978: the shared namespaces each derived peer answer reads (its participant-scoped change signal).
const LEGACY_RENEWAL_PREFIXES = [HOST_PREFIX, LEGACY_SESSION_PREFIX, PARTICIPANT_PREFIX] as const;
// list() (the root-collision report) also reads the legacy actor records.
const ROOT_COLLISION_PREFIXES = [HOST_PREFIX, PARTICIPANT_PREFIX, LEGACY_SESSION_PREFIX, LEGACY_ACTOR_PREFIX] as const;
const PARTICIPANT_HEARTBEAT_MS = 5_000;
const TRANSPORT_RENEW_BUDGET_MS = 5_000;
/**
 * Addressable across a live reload, but a failed reload stops accepting after this lease.
 * quiesce("reload") writes it once, at the start of teardown; nothing renews it until the new
 * release's first heartbeat replaces it, because the old heartbeat stops at teardown and a
 * synchronous release import blocks the event loop anyway. A reload on a loaded host takes a
 * minute or more (smarty-dev#6729: p50 56 s, max 102 s), so 30 s let Mains lapse mid-reload.
 * Bounded so that a Main that dies mid-reload still drops out of the directory within 3 min.
 */
export const MAIN_RELOAD_LEASE_MS = 180_000;
/**
 * Change-driven refreshes (agent UI updates, actor changes) run at most once per this
 * interval, and write only when a published record changed (smarty-dev#367: each write
 * rewrites the whole shared mesh state under its lock). The heartbeat timer still renews
 * the lease every heartbeat interval.
 */
const CHANGE_REFRESH_MIN_MS = 1_000;
const NATS_TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled", "stopped", "timed_out", "done", "aborted"]);

// smarty-dev#6477 L6: an idle heartbeat has nothing to write; it only needs evidence that the
// shared state is writable now (confirmedAt; peer-settle relies on it, #24). Taking the single
// mesh lock for that evidence made every live participant queue on it every heartbeat. A busy
// mesh already supplies that evidence without the lock: a commit by any writer. These files are
// only written by a mesh-lock holder (MeshStore owns the names): the state rename, the read
// signal and journal, and event appends. state.json's mtime is the pre-lock staging time, i.e.
// no later than its commit, so it can only understate the evidence. A wedged holder
// (signal-stopped, #266) commits nothing: confirmations then keep taking the lock.
// On an otherwise idle mesh, a confirmation that does take the lock leaves its own witness,
// touched UNDER that lock, so one real acquisition per interval serves every idle participant.
const CONFIRM_WITNESS_FILE = "participant-confirm.witness";
const COMMIT_WITNESS_FILES = ["state.read-signal.json", "state.read-journal.jsonl", "state.json", "events.jsonl", CONFIRM_WITNESS_FILE] as const;

/** The current uid, where the platform has one (not on Windows). */
const CURRENT_UID = typeof process.getuid === "function" ? process.getuid() : undefined;

/** A witness counts only as a regular file of this user under the mesh root (security round on
 * #592): every name is a fixed basename joined to the root, and its times are read with lstat,
 * never through a link. A symlink, directory, FIFO or other special file, a hard-linked file, or a
 * file of another uid is no proof: a process able to write the shared mesh root could otherwise
 * plant one pointing at a recently modified path and authorize a lock-free confirmation. */
function ownRegularWitness(stat: fs.Stats): boolean {
  return stat.isFile() && stat.nlink === 1 && (CURRENT_UID === undefined || stat.uid === CURRENT_UID);
}

/** The open flags the witness touch uses. Read at call time; a test seam replaces it to simulate a
 * platform without O_NOFOLLOW (review round 4 on #592). */
export const confirmWitnessPlatform: { constants: { O_WRONLY: number; O_CREAT: number; O_EXCL: number; O_NOFOLLOW?: number | undefined; O_NONBLOCK?: number | undefined } } =
  { constants: fs.constants };

/** Runs under the mesh lock only (confirmWritable's callback). Best effort: a failure only
 * withholds evidence from other participants, which then take the lock themselves. Never follows
 * a planted link (lstat first, then O_NOFOLLOW) and touches only this user's own regular witness:
 * anything else under the name is left alone (no truncation of a link target) and gives no proof.
 * Without an atomic no-follow open (O_NOFOLLOW undefined, e.g. Windows) the witness is never
 * opened or written: lstat-then-open would race a swap to a symlink, so that platform leaves no
 * confirmation witness and idle participants take the lock (review round 4 on #592). After the
 * open, the descriptor must be the very file lstat saw (same dev and ino); a file swapped in
 * between is refused. A missing witness is created exclusively (O_EXCL), never opened if it
 * appeared meanwhile. The truncation of the empty witness stamps its mtime with the kernel file
 * clock, as before. */
function touchConfirmWitness(meshRoot: string): void {
  const { O_WRONLY, O_CREAT, O_EXCL, O_NOFOLLOW, O_NONBLOCK = 0 } = confirmWitnessPlatform.constants;
  if (typeof O_NOFOLLOW !== "number" || O_NOFOLLOW === 0) return;
  const file = path.join(meshRoot, CONFIRM_WITNESS_FILE);
  let fd: number | undefined;
  try {
    let seen: fs.Stats | undefined;
    try {
      seen = fs.lstatSync(file);
      if (!ownRegularWitness(seen)) return;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") return;
    }
    fd = fs.openSync(file, O_WRONLY | O_NOFOLLOW | O_NONBLOCK | (seen ? 0 : O_CREAT | O_EXCL), 0o600);
    const opened = fs.fstatSync(fd);
    if (!ownRegularWitness(opened)) return;
    if (seen && (opened.dev !== seen.dev || opened.ino !== seen.ino)) return;
    fs.ftruncateSync(fd, 0);
  } catch { /* no evidence */ } finally {
    if (fd !== undefined) try { fs.closeSync(fd); } catch { /* best effort */ }
  }
}

/** The latest commit witness mtime under the mesh root, or 0 when none can be read. Lock-free.
 * Floored to whole milliseconds: mtimeMs is fractional, but every time it is compared with
 * (receipts, Date.now()) is an integer millisecond, and a fractional witness in the same
 * millisecond would read as later than a receipt or clock that already includes it. Flooring
 * only ages the evidence, so a same-millisecond tie fails safe: not newer, take the lock. */
function latestCommitWitness(meshRoot: string): number {
  let latest = 0;
  for (const name of COMMIT_WITNESS_FILES) {
    try {
      const stat = fs.lstatSync(path.join(meshRoot, name));
      if (ownRegularWitness(stat)) latest = Math.max(latest, stat.mtimeMs);
    } catch { /* absent: no evidence */ }
  }
  return Math.floor(latest);
}
/**
 * Activity counters change on almost every turn and tool call. A change in them alone does not
 * rewrite the shared state; a write carries them at most this often, or with any other write
 * (smarty-dev#816: more than half of the participant rewrites were counters only). Readers
 * (the dashboard, agents.list/status/members of remote agents) show them up to this old.
 */
const ACTIVITY_REFRESH_MS = 60_000;
/** Ignore noisy model activity, not actor queue/mailbox state needed for live reads (#2726). */
const QUIET_FIELDS = {
  updatedAt: undefined,
  currentTool: undefined,
  turns: undefined,
  toolCalls: undefined,
  usage: undefined,
} as const;
/** How often a host sweeps records of long-dead hosts (smarty-dev#367); the first sweep waits too. */
const DEAD_HOST_SWEEP_MS = 15 * 60 * 1_000;
const keyFor = (prefix: string, id: string): string =>
  prefix + createHash("sha256").update(id).digest("hex");

// A record may be in its own file and in the shared state (smarty-dev#2004): during a mixed
// rollout a newer runtime writes both and an older one only the state. The later write wins; on a
// tie, the file. An older runtime takes over a key only after its owner lapsed, so its write is the
// later one.
// A mirror (a bridged remote record, only ever in the state) never outranks a native file at the
// same key, whatever the times: a local record keeps precedence (#132).
const mirrored = (entry: MeshStateEntry): boolean =>
  isObject(entry.value) && entry.value.remoteHost !== undefined;
const newer = (file: MeshStateEntry | undefined, state: MeshStateEntry | undefined): MeshStateEntry | undefined =>
  !file ? state : !state || mirrored(state) ? file : state.updatedAt > file.updatedAt ? state : file;

// The merged entries, and the state mirrors a native file shadowed (still checked for a collision,
// so a refused mirror is reported whichever copy of the native won).
const mergeParticipantEntries = (
  files: readonly MeshStateEntry[],
  state: readonly MeshStateEntry[],
): { entries: MeshStateEntry[]; shadowed: MeshStateEntry[] } => {
  const byKey = new Map<string, MeshStateEntry>();
  const shadowed: MeshStateEntry[] = [];
  for (const entry of files) byKey.set(entry.key, entry);
  for (const entry of state) {
    const file = byKey.get(entry.key);
    if (file && mirrored(entry)) shadowed.push(entry);
    byKey.set(entry.key, newer(file, entry)!);
  }
  return { entries: [...byKey.values()], shadowed };
};

// smarty-dev#7978: whether two host-lease readings agree on every PEER lease, by the cached lease
// object's identity (readHostLeases reuses it while that file's identity and timestamps are unchanged).
// This host's own lease is left out: no derived peer answer reads it, and it renews every heartbeat.
const samePeerLeases = (
  left: ReadonlyMap<string, FabricHostLease>, right: ReadonlyMap<string, FabricHostLease>, ownHostId: string,
): boolean => {
  let peers = 0;
  for (const [id, lease] of right) {
    if (id === ownHostId) continue;
    peers += 1;
    if (left.get(id) !== lease) return false;
  }
  for (const id of left.keys()) if (id !== ownHostId) peers -= 1;
  return peers === 0;
};

/**
 * Whether two selections of the shared entries a derived peer answer reads are the same state (smarty-dev#7978,
 * Astra P2 on fd31b1c1). A commit to an unrelated key moves the state token but leaves every one of these
 * entries the same frozen object (the snapshot is copy on write), so the check is an identity walk; an entry
 * that is a different object is the same only with the same key, version, updatedAt and bytes (a full
 * re-export, or a writer that rewrote a row without a new version, still counts as a change).
 */
const sameEntrySelections = (
  left: readonly (readonly Readonly<MeshStateEntry>[])[], right: readonly (readonly Readonly<MeshStateEntry>[])[],
): boolean => {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    const before = left[index]!;
    const after = right[index]!;
    if (before.length !== after.length) return false;
    for (let at = 0; at < before.length; at += 1) {
      const was = before[at]!;
      const is = after[at]!;
      if (was !== is && (was.key !== is.key || was.version !== is.version || was.updatedAt !== is.updatedAt ||
        JSON.stringify(was) !== JSON.stringify(is))) return false;
    }
  }
  return true;
};

/** The change signals a derived peer answer was computed from (smarty-dev#7978). */
interface PeerSignals {
  /**
   * The canonical state snapshot (stateToken) last seen with these entries: a fast path only. A
   * different token is a change only when the selected entries changed (Astra P2 on fd31b1c1).
   */
  token: object;
  /** The shared entries it read, by prefix (the participant, legacy-session, host and actor namespaces). */
  entries: readonly (readonly Readonly<MeshStateEntry>[])[];
  /** The participant-file listing: the same array while no file changed. */
  files: readonly MeshStateEntry[];
  leases: ReadonlyMap<string, FabricHostLease>;
  /** The `now` it was computed for, and the wall clock when (the safety interval's reference). */
  at: number;
  checkedAt: number;
}

// Message-only legacy compatibility stays narrow; retry policy also uses shared isMeshWriteDeferred.
const isMeshLockTimeout = (error: unknown): error is Error =>
  error instanceof Error &&
  ((error as Error & { code?: unknown }).code === "FABRIC_MESH_LOCK_TIMEOUT" ||
    error.message.startsWith("Timed out waiting for the Fabric mesh lock"));

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const participantKind = (value: unknown): FabricParticipantKind | undefined =>
  value === "root" || value === "agent" || value === "actor" || value === "provider"
    ? value
    : undefined;

// Inlined RUNNER_ID_PATTERN (src/agents/runner-registry.ts).
const RUNNER_ID_PATTERN = /^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/;
const isRunnerId = (value: unknown): value is string =>
  typeof value === "string" && value.length <= 64 && RUNNER_ID_PATTERN.test(value);

const transports = new Set([
  "host",
  "hosted",
  "auto",
  "process",
  "sandbox",
  "tmux",
  "screen",
  "localterm",
  "herdr",
]);
const capabilities = new Set([
  "steer",
  "followUp",
  "stop",
  "ask",
  "actor-bindings",
  "attach",
  "fabric",
]);

interface ParsedDirectory {
  hosts: MeshStateEntry[];
  participants: FabricParticipantRecord[];
  /** Raw terminal candidates stay uncached: private generation/death proof can change alone. */
  terminalActors: MeshStateEntry[];
  /** State mirrors a native participant file shadowed: still checked for a collision. */
  shadowed: FabricParticipantRecord[];
  legacySessions: MeshStateEntry[];
  legacyActors: MeshStateEntry[];
  /** Entries that failed validation. Refusals for them are published only from a bound read. */
  malformed: MeshStateEntry[];
}

const deepFreeze = <T>(value: T): T => {
  if (typeof value === "object" && value !== null && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
};

// A host name the mesh bridge marks mirrored records with (smarty-dev#2004). Absent: a record
// this mesh's own hosts wrote. Present but invalid: the record is rejected.
const REMOTE_HOST = /^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$/;
const remoteHostValid = (value: unknown): boolean =>
  value === undefined || (typeof value === "string" && REMOTE_HOST.test(value));

const optionalStrings = (value: Record<string, unknown>, keys: readonly string[]): boolean =>
  keys.every((key) => value[key] === undefined || typeof value[key] === "string");
const placementFields = ["placementRunId", "placementKind", "placementHost", "expectedSeconds"] as const;
const placementPresenceValid = (value: Record<string, unknown>): boolean => {
  const count = placementFields.filter(key => value[key] !== undefined).length;
  if (count === 0) return true;
  // Island Main roots publish only the Pod-injected run correlation; process-placement
  // records additionally carry the launcher admission metadata below.
  if (count === 1 && isPlacementRunId(value.placementRunId)) return true;
  return count === placementFields.length && isPlacementRunId(value.placementRunId)
    && (value.placementKind === "process" || value.placementKind === "remote")
    && typeof value.placementHost === "string" && /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/.test(value.placementHost)
    && Number.isSafeInteger(value.expectedSeconds) && (value.expectedSeconds as number) >= 1 && (value.expectedSeconds as number) <= 604_800;
};

let ownProcessIdentity: { pid: number; host: string; startTime?: string } | undefined;
/** This process's identity, read once: stable for the process's life. */
const ownMainProcess = (): { pid: number; host: string; startTime?: string } => {
  if (!ownProcessIdentity) {
    // Linux /proc/<pid>/stat field 22 (start ticks), after comm; the same value residency's processStartTime reads.
    let startTime: string | undefined;
    if (process.platform === "linux") {
      try {
        const stat = fs.readFileSync(`/proc/${process.pid}/stat`, "utf8");
        startTime = stat.slice(stat.lastIndexOf(")") + 2).trim().split(/\s+/)[19];
      } catch { /* unknown: the record carries no start time */ }
    }
    ownProcessIdentity = { pid: process.pid, host: os.hostname(), ...(startTime ? { startTime } : {}) };
  }
  return ownProcessIdentity;
};

const sessionServicesValid = (value: unknown, kind: FabricParticipantKind): boolean => {
  if (value === undefined) return true;
  if (kind !== "root" || !Array.isArray(value)) return false;
  const names = new Set<string>();
  return value.every((service) => {
    if (!isObject(service) || typeof service.name !== "string" ||
      /^[a-z0-9][a-z0-9-]{0,47}$/.exec(service.name)?.[0] !== service.name ||
      !Number.isSafeInteger(service.version) || Number(service.version) < 1 || names.has(service.name)) return false;
    names.add(service.name);
    return true;
  });
};

/** The participant record this directory reads from an entry, or undefined when it refuses the entry.
 * Exported for the mesh bridge, which never writes a mirror row its own host's readers must refuse. */
export const participantFromEntry = (entry: MeshStateEntry): FabricParticipantRecord | undefined => {
  if (!isObject(entry.value) || entry.value.format !== 1 || entry.value.stopReason === "owner-root-gone") return undefined;
  const value = entry.value as Partial<FabricParticipantRecord> & Record<string, unknown>;
  const kind = participantKind(value.kind);
  if (
    !kind ||
    !sessionServicesValid(value.services, kind) ||
    (value.interactive !== undefined && typeof value.interactive !== "boolean") ||
    !remoteHostValid(value.remoteHost) ||
    !remoteHostValid(value.bridgeVia) ||
    (value.bridgeVia !== undefined && (value.remoteHost === undefined || value.bridgeVia === value.remoteHost)) ||
    // Optional fields that consumers read as strings (peer cards, labels, leader selection):
    // a malformed one drops this record alone, never the listing (smarty-dev#2045).
    !optionalStrings(value, ["sessionId", "herdrPane", "cwd", "label", "role", "project", "projectRoot", "repository", "model", "thinking", "parentId", "actorOwnershipToken", "finalAnswerReceiptId"]) ||
    !placementPresenceValid(value) ||
    // v1 of the bridge mirrors root presence only; remote agents and actors come in v2.
    (value.remoteHost !== undefined && kind !== "root") ||
    typeof value.id !== "string" ||
    entry.key !== keyFor(PARTICIPANT_PREFIX, value.id) ||
    typeof value.rootId !== "string" ||
    typeof value.ownerHostId !== "string" ||
    typeof value.ownerIdentityId !== "string" ||
    entry.updatedBy.id !== value.ownerIdentityId ||
    (value.ownerIncarnation !== undefined &&
      (typeof value.ownerIncarnation !== "string" || !value.ownerIncarnation.length || value.ownerIncarnation.length > 128)) ||
    typeof value.name !== "string" ||
    typeof value.status !== "string" ||
    (kind === "provider"
      ? value.runner !== undefined || typeof value.provider !== "string"
      : !isRunnerId(value.runner)) ||
    typeof value.transport !== "string" ||
    !transports.has(value.transport) ||
    !Array.isArray(value.capabilities) ||
    !value.capabilities.every(
      (capability) => typeof capability === "string" && capabilities.has(capability),
    ) ||
    typeof value.startedAt !== "number" ||
    typeof value.updatedAt !== "number" ||
    value.controlProtocol !== "v1" &&
    value.controlProtocol !== "legacy"
  ) {
    return undefined;
  }
  return value as FabricParticipantRecord;
};

const hostFromEntry = (entry: MeshStateEntry): FabricHostRecord | undefined => {
  if (!isObject(entry.value) || entry.value.format !== 1) return undefined;
  const value = entry.value as Partial<FabricHostRecord>;
  if (
    typeof value.id !== "string" ||
    entry.key !== keyFor(HOST_PREFIX, value.id) ||
    !remoteHostValid(value.remoteHost) ||
    !remoteHostValid(value.bridgeVia) ||
    (value.bridgeVia !== undefined && (value.remoteHost === undefined || value.bridgeVia === value.remoteHost)) ||
    typeof value.rootId !== "string" ||
    !isObject(value.identity) ||
    typeof value.identity.id !== "string" ||
    typeof value.identity.name !== "string" ||
    entry.updatedBy.id !== value.identity.id ||
    (value.identity.kind !== "main" &&
      value.identity.kind !== "agent" &&
      value.identity.kind !== "actor") ||
    (value.incarnationToken !== undefined && (typeof value.incarnationToken !== "string" || value.incarnationToken.length === 0)) ||
    (value.mirrorLease !== undefined && value.mirrorLease !== 1) ||
    typeof value.startedAt !== "number" ||
    typeof value.updatedAt !== "number" ||
    typeof value.expiresAt !== "number"
  ) {
    return undefined;
  }
  return value as FabricHostRecord;
};

// Explicit root names take precedence; unnamed roots keep their stable short peer labels.
const rootPeerName = (name: string, label: string | undefined, sessionId: string): string =>
  name !== "main" ? name : label ?? `Peer ${sessionId.slice(0, 8)}`;

export interface FabricPeerListOptions extends FabricParticipantListOptions {
  /** Project display listing from this refresh; never reused by authoritative/fresh reads. */
  displayParticipants?: readonly FabricParticipantInfo[];
}

const peerFromParticipant = (participant: FabricParticipantInfo): FabricPeerInfo | undefined => {
  if (
    participant.kind !== "root" ||
    !participant.cwd ||
    !participant.sessionId ||
    (participant.status !== "idle" && participant.status !== "running" && participant.status !== "stopping" && participant.status !== "reloading")
  ) {
    return undefined;
  }
  // A mirrored root's label comes from its own mesh's sequence, so it can repeat a local label:
  // the host suffix keeps every label a selector can match unique (smarty-dev#2004).
  const at = participant.remoteHost ? `@${participant.remoteHost}` : "";
  const label = participant.label ? participant.label + at : undefined;
  return {
    id: participant.id,
    name: rootPeerName(participant.name, participant.label, participant.sessionId) + at,
    ...(label ? { label } : {}),
    ...(typeof participant.role === "string" ? { role: participant.role } : {}),
    ...(typeof participant.project === "string" ? { project: participant.project } : {}),
    ...(participant.repository ? { repository: participant.repository } : {}),
    ...(participant.interactive !== undefined ? { interactive: participant.interactive } : {}),
    kind: "peer",
    // A shutting-down root stays listed as a peer (its steer fails with a clear error).
    status: participant.status === "running" ? "running" : "idle",
    runner: "pi",
    transport: "host",
    cwd: participant.cwd,
    sessionId: participant.sessionId,
    ...(participant.model ? { model: participant.model } : {}),
    ...(participant.thinking ? { thinking: participant.thinking } : {}),
    startedAt: participant.startedAt,
    updatedAt: participant.updatedAt,
    pendingMessages: participant.pendingMessages === true,
    local: false,
    ...(participant.remoteHost ? { host: participant.remoteHost } : {}),
  };
};

// A mirrored participant counts only under a host record the bridge mirrored from the same
// remote host, and a local participant only under a local host record: neither side can claim
// the other's owner. A mirrored host never carries this host's id (smarty-dev#2004).
const ownerMatches = (
  participant: FabricParticipantRecord,
  owner: FabricHostRecord | undefined,
  localHostId: string,
): boolean =>
  !owner ||
  (owner.remoteHost === participant.remoteHost && owner.bridgeVia === participant.bridgeVia &&
    !(owner.remoteHost !== undefined && owner.id === localHostId));

const isLocal = (participant: FabricParticipantRecord, localHostId: string): boolean =>
  participant.remoteHost === undefined && participant.ownerHostId === localHostId;

/** Mesh topic of a refused mirrored record (smarty-dev#2004); one event per collision and process. */
export const MIRROR_COLLISION_TOPIC = "fabric.topology.mirror-refused";

const legacyRootFromEntry = (
  entry: MeshStateEntry,
  localRootId: string,
  now: number,
  meshRoot?: string,
): FabricParticipantInfo | undefined => {
  if (!isLiveLegacyRootEntry(entry, now, meshRoot) || entry.value.id === localRootId) return undefined;
  const value = entry.value;
  return {
    format: 1,
    id: value.id,
    kind: "root",
    rootId: value.id,
    ownerHostId: value.id,
    ownerIdentityId: value.id,
    name: typeof value.name === "string" ? value.name : "main",
    status: value.status,
    runner: "pi",
    transport: "host",
    capabilities: ["steer", "followUp", "fabric"],
    cwd: value.cwd,
    sessionId: value.sessionId,
    ...(typeof value.model === "string" ? { model: value.model } : {}),
    ...(typeof value.thinking === "string" ? { thinking: value.thinking } : {}),
    startedAt: value.startedAt,
    updatedAt: sessionLiveness(entry, meshRoot).updatedAt,
    pendingMessages: value.pendingMessages === true,
    controlProtocol: "legacy",
    local: false,
    stale: false,
  };
};

// Retention is a display decision, never an ownership/proof decision. Only the
// authenticated record's immutable settlement time may start the display clock.
const retainedTerminalActor = (participant: FabricParticipantRecord, now: number): boolean => {
  const age = now - (participant.settledAt ?? NaN);
  return age >= 0 && age < DIRECTORY_RETENTION_MS;
};

const legacyActorFromEntry = (
  entry: MeshStateEntry,
  roots: Map<string, FabricParticipantInfo>,
  now: number,
  terminalActor?: (entry: MeshStateEntry) => FabricParticipantRecord | undefined,
): FabricParticipantInfo | undefined => {
  if (!isObject(entry.value)) return undefined;
  if (entry.value.stopReason === "owner-root-gone") {
    const terminal = terminalActor?.(entry);
    // Only the private validator's immutable settlement starts display retention;
    // observer rewrites and caller-controlled envelope timestamps cannot renew it.
    return terminal && retainedTerminalActor(terminal, now)
      ? { ...terminal, local: false, stale: true } : undefined;
  }
  const value = entry.value;
  if (
    typeof value.id !== "string" ||
    typeof value.name !== "string" ||
    !isRunnerId(value.runner) ||
    typeof value.status !== "string"
  ) {
    return undefined;
  }
  const root = roots.get(entry.updatedBy.id);
  if (
    !root?.sessionId ||
    entry.key !== `${LEGACY_ACTOR_PREFIX}${root.sessionId}/${value.id}`
  ) {
    return undefined;
  }
  const active = value.status !== "stopped";
  return {
    format: 1,
    id: value.id,
    kind: "actor",
    rootId: root.id,
    ownerHostId: root.id,
    ownerIdentityId: entry.updatedBy.id,
    parentId: root.id,
    name: value.name,
    status: value.status,
    runner: value.runner,
    transport: "host",
    capabilities: [
      ...(active ? (["steer", "followUp"] as const) : []),
      ...(value.runner === "pi" || value.runner === "pi-durable" ? (["fabric"] as const) : []),
    ],
    startedAt: typeof value.createdAt === "number" ? value.createdAt : entry.updatedAt,
    updatedAt: entry.updatedAt,
    controlProtocol: "legacy",
    local: false,
    stale: false,
  };
};

/** Advisory only: names are lookup labels, not an exclusive authority claim. */
export interface FabricRootCollision {
  reason: "duplicate-name" | "duplicate-session";
  name: string;
  ids: string[];
  sessionId?: string;
}

export const ROOT_COLLISION_TOPIC = "fabric.topology.root-collision";

class ParticipantPublicationChangedError extends Error {
  constructor() { super("Actor registry changed during participant preparation"); }
}

export interface ParticipantDirectoryOptions {
  enabled: boolean;
  /** Select KV presence/lease lifecycle instead of FILE publication; never import FILE routes. */
  transport?: FabricTransport;
  /** Existing machine-local runtime state, never the shared mesh root, for the NATS lease receipt. */
  transportStateDir?: string;
  /** Existing actor file renewal only; validate the last known lineage against
   * an atomic registry snapshot at the per-key write decision. Never admits keys. */
  actorRenewalAllowed?: (record: FabricParticipantRecord) => boolean;
  /** Complete runtime mesh activation before any presence write. A deferred write
   * follows the same startup warning and background refresh retry as publication. */
  beforePublication?: () => Promise<void>;
  /** Prepare outside custody; validate exact registry generations inside the
   * existing publication fence before any ownership/presence write. */
  preparePublicationFence?: () => () => boolean;
  /** Explicit legacy list observation TTL, without a floor. Authority must request fresh. */
  listReadCacheMs?: number;
  /** Registry -> mesh/key order for shared publication. An exact empty actor set
   * proves no actor effects; undefined retains the conservative all-registry fence.
   * Accepted actor file copies may follow outside custody with exact token validation
   * under their key lock. */
  withPublicationFence?: <T>(publish: () => Promise<T>, actorIds?: ReadonlySet<string>) => Promise<T>;
  /** Wait for mesh admission WITHOUT registry custody, then retry under a fresh fence.
   * Resident hosts supply a bounded FIFO wait; no snapshot/commit receipt crosses it. */
  waitForPublicationRetry?: () => Promise<void>;
  /** Extra presence writes share this host's one fenced heartbeat acquisition. */
  publicationBatch?: (full: boolean) => { ops: MeshBatchOperation[]; committed: () => void; actorIds?: readonly string[] };
  hostId: string;
  rootId: string;
  identity: MeshIdentity;
  onRootCollision?: (collision: FabricRootCollision) => void;
  selfOwnerHostId?: string;
  selfOwnerIdentityId?: string;
  heartbeatMs?: number;
  /** Deterministic scheduling seam; production uses Math.random. */
  random?: () => number;
  leaseMs?: number;
  /** Resolution-only grace; clock/lock/sleep injection keeps contention tests deterministic. */
  routingLease?: RoutingLeaseWaitOptions;
  /**
   * Sweep records of hosts gone for this long (default 6 h), at most every sweepMs (default
   * 15 min, the first sweep waiting as long). false disables it (secondary directories, tests).
   */
  reapDeadHosts?: false | { deadAfterMs?: number; sweepMs?: number };
  /** Stall checks share the committed heartbeat; secondary/resident hosts can participate. */
  presencePass?: () => Promise<void>;
  presencePassMs?: number;
  /** ADDITIONAL host/composition gate only. Undefined admits; a nonempty reason gates
   * diagnostics/alarms, not canConsumeMesh(). Must not call back into this directory. */
  controlAdmissionReason?: () => string | undefined;
  /** Lazy host validator: PRIVATE actor outbox plus independent raw-root death evidence.
   * Returns a closed, zero-capability terminal record, never caller envelope fields. */
  terminalActor?: (entry: MeshStateEntry) => FabricParticipantRecord | undefined;
  /** Told once when close() could not withdraw this host's entries (default: console.warn). */
  onWithdrawalFailure?: (message: string) => void;
  /**
   * Session lifecycle token (smarty-dev#5962). False once the owner's extension ctx retired
   * (reload, session replacement): background ticks skip quietly until the owner rebinds.
   * Explicit refresh()/quiesce() calls still publish from the owner's last live snapshot.
   */
  live?: () => boolean;
  /**
   * Once per outage: more than LIFECYCLE_LOST_TICKS consecutive heartbeats found no live
   * lifecycle to read (smarty-dev#5962/#4313). The lease is no longer renewed, so the owner
   * must say so visibly instead of vanishing from the directory in silence.
   */
  onLifecycleLost?: (ticks: number) => void;
}

/** Heartbeats a retired lifecycle may skip quietly (a rebind is normally immediate). */
export const LIFECYCLE_LOST_TICKS = 2;

/** Pi's error for a ctx or pi used after session replacement or reload (ExtensionRunner.invalidate). */
export const isStaleExtensionContextError = (error: unknown): boolean =>
  error instanceof Error && /extension ctx is stale after session replacement or reload/.test(error.message);

export type ParticipantSnapshotSource = (reportFailure: (error: unknown, participantId?: string) => void) => FabricParticipantRecord[];

type SourceFailure = FabricIsolatedSourceFailure & { dedupeKey: string; alarmed: boolean };

type RemoteMirrorLease = { state: "none" } | { state: "ended" } | {
  state: "live" | "expired" | "lapsed"; ownerHostId: string; ownerIdentityId: string; rootId: string;
  remoteHost: string; expiresAt: number; mirror?: FabricMirrorLease; bridgeStale?: true;
};

type ReconnectRefresh = { dueAt: number | undefined; timer?: NodeJS.Timeout; ready?: AbortController; running?: true };

export class ParticipantDirectory implements FabricParticipantSource {
  /** Distinct from PID incarnation and actor lineage; rotates on every activation/reload. */
  readonly ownerIncarnation = randomUUID();
  #transportLease: TransportHostLease | undefined;
  #participantView: KvParticipantDirectory | undefined;
  #localProjection: NatsLocalProjection | undefined;
  readonly #natsPrelaunch = new Map<string, FabricParticipantRecord>();
  readonly #natsPending = new Map<string, Promise<void>>();
  readonly #natsPublished = new Map<string, FabricParticipantRecord>();
  readonly #natsPublishedAt = new Map<string, number>();
  readonly #natsRetired = new Set<string>();
  /** Successful source absences still retire ids removed during an earlier outage. */
  readonly #natsAbsent = new Set<string>();
  #natsStarting: Promise<void> | undefined;
  #natsClosing: Promise<void> | undefined;
  #transportRenewing: Promise<void> | undefined;
  /** Request-start expiry of the last confirmed KV lease write; sleep may stop performance.now(). */
  #transportLeaseWallDeadline: number | undefined;
  #transportReconnectUnsubscribe: (() => void) | undefined;
  /** One reconnect deadline, retained across repeated notifications and the refresh it drives. */
  #reconnectRefresh: ReconnectRefresh | undefined;
  #verifiedReconnectGeneration = 0;
  #confirmedReconnectGeneration = 0;
  #natsConfirmedAt = 0;
  #transportRecovering = false;
  /** The owner lease was re-created after expiring in KV; its root must be re-checked (confirmSoleRoot). */
  #rootRecheckPending = false;
  #transportLost = false;
  readonly #backgroundRefresh = new MeshBackgroundRetry("participant heartbeat/change refresh")
    .diagnosticsTo(line => logDiagnostic("src/topology/participant-directory.ts", line))
    .alarmsTo(meshOutageAlarmSink(() => ({ mesh: this.mesh, from: this.options.identity })));
  readonly #notifications = new MeshBackgroundQueue("participant refusal/reap");
  readonly #sources = new Set<ParticipantSnapshotSource>();
  readonly #isolatedSources = new Set<ParticipantSnapshotSource>();
  readonly #sourceNames = new Map<ParticipantSnapshotSource, string>();
  /** Identity sets only: failed sources must never replay cached presence payloads. */
  readonly #sourceIds = new Map<ParticipantSnapshotSource, Set<string>>();
  readonly #sourceOmittedIds = new Map<ParticipantSnapshotSource, Set<string>>();
  readonly #sourceFailures = new Map<ParticipantSnapshotSource, SourceFailure>();
  #sourceSequence = 0;
  #controlAdmissionIncident: { since: number; dedupeKey: string; alarmed: boolean } | undefined;
  #controlAdmissionTimer: NodeJS.Timeout | undefined;
  #controlAdmissionDueAt: number | undefined;
  #startedAt = Date.now();
  #incarnationToken = randomUUID();
  #leasePredecessor: FabricHostLease | undefined;
  readonly #ownIncarnation: Promise<string | undefined>;
  #ownIncarnationValue: string | undefined;
  #prepareFileLocks = false;
  readonly #roleGrant = new ParticipantRoleGrant();
  readonly #heartbeatMs: number;
  /** Latest commit witness mtime already behind a confirmation receipt (smarty-dev#6477 L6). */
  #commitWitnessSeen = 0;
  readonly #leaseMs: number;
  readonly #localRecords = new Map<string, FabricParticipantRecord>();
  #parsedCache: { token: object; files: readonly MeshStateEntry[]; value: ParsedDirectory } | undefined;
  #parsedEntries = new Map<string, { source: Readonly<MeshStateEntry>; entry: MeshStateEntry }>();
  /** fv2-11107: the last poll ownership map, reusable only while its observation of the managed actors holds. */
  #pollOwnership: { hostId: string; ids: ReadonlySet<string>; map: ReadonlyMap<string, boolean>; observation: ActorOwnershipObservation } | undefined;
  readonly #reportedCollisions = new Set<string>();
  readonly #reportedRootCollisions = new Set<string>();
  #timer: NodeJS.Timeout | undefined;
  /** Keep the sampled cadence when a pending PubAck temporarily shortens the safety cap. */
  #heartbeatDueAt: number | undefined;
  /** The heartbeat was stopped by suspend() (a retired owner lifecycle), not by close(). */
  #suspended = false;
  #closed = false;
  #leaseAbort = new AbortController();
  #leaseClaimed = false;
  #prepareLeaseLock = true;
  #superseded: FabricHostLeaseSupersededError | undefined;
  /** Consecutive heartbeats skipped for want of a live lifecycle; degraded past the limit. */
  #retiredTicks = 0;
  #degraded = false;
  #refreshing: Promise<void> | undefined;
  /** Coalesce this directory's own lease writes, not an independent actor key wait. */
  #leaseRenewing: Promise<number> | undefined;
  #lastLeaseRenewedAt = 0;
  /** Serialize only our own asynchronous lease gate users; foreign custody stays a one-shot try. */
  #leaseCommitting: Promise<void> = Promise.resolve();
  /** A successful sequence claim survives discarded preparations for this root. */
  #claimedPeerLabel: string | undefined;
  #refreshScheduled = false;
  #refreshAgain = false;
  #refreshTimer: ReturnType<typeof setTimeout> | undefined;
  #publicationRetryTimer: ReturnType<typeof setTimeout> | undefined;
  #publicationRetrying: Promise<void> | undefined;
  /** smarty-dev#8880: a publication retry's admission, awaited by #refresh after selection, right before the fence. */
  #admitBeforeCommit: (() => Promise<void>) | undefined;
  /** The admission #refresh is waiting for: an explicit refresh() that joins stops the wait (interrupt). */
  #admitting: { interrupt: () => void; admitted: Promise<void> } | undefined;
  /** The last admission started for a retry; #retryPublication awaits it, so close/quiesce drain it. */
  #admitted: Promise<void> | undefined;
  #publicationRetryDelay = 750;
  /** When the last change-driven refresh started (the throttle's reference). */
  #changeRefreshAt = 0;
  /** Whether the refresh in flight renews the lease (a heartbeat) or only publishes changes. */
  #refreshingFull = false;
  /** Per-key waits must not stop the independent host heartbeat. */
  #fileWork = 0;
  #refreshedAt = Date.now();
  #refreshStartedAt: number | undefined;
  #refreshError: unknown;
  /** A bounded routing read is not a heartbeat/admission receipt. */
  #routingReadAt = 0;
  #routingError: unknown;
  #leaseConfirmed = false;
  /** Only a kept predecessor reload lease needs takeover after the first host commit. */
  #keptReloadLease = false;
  #deadHostSweepAt = performance.now();
  #presencePassAt = Date.now();
  #quiescing = false;
  #reloadUntil: number | undefined;
  #reloadPublished = false;
  /** When a committed write last carried this host's participant records. */
  #recordsWrittenAt = 0;
  /** Set once close() has committed this host's withdrawal (or its reload withdrawal). */
  #withdrawn = false;
  /** close() could not withdraw: the entries lapse with their unrenewed lease instead. */
  #withdrawalFailed = false;
  /** close() started but its withdrawal has not committed yet: an exit treats this as failed. */
  #withdrawalPending = false;
  #withdrawalWarned = false;
  #exitHook: (() => void) | undefined;
  /**
   * smarty-dev#7978: an idle heartbeat derived these two answers from an unchanged fleet several times a
   * pass, each a full read of every host, session and participant entry. Each is reused only while its
   * change signals (state snapshot, participant files, peer leases) are unchanged, and is recomputed at
   * least once per lease period (#leaseMs), the safety net of the existing heartbeat timer.
   */
  #legacyRenewals: (PeerSignals & { required: boolean; until: number }) | undefined;
  #rootCollisionPass: (PeerSignals & { local: string }) | undefined;

  constructor(
    readonly mesh: MeshStore,
    readonly options: ParticipantDirectoryOptions,
  ) {
    const nats = options.transport?.kind === "nats";
    this.#transportLease = undefined;
    this.#participantView = undefined;
    this.#leasePredecessor = options.enabled && !nats ? hostLeasePredecessor(mesh.root, options.hostId) : undefined;
    if (nats) {
      const local = options.transportStateDir;
      if (!local || !path.isAbsolute(local)) throw new TypeError("NATS participant directory requires an absolute transportStateDir");
      const relative = path.relative(path.resolve(mesh.root), path.resolve(local));
      if (relative === "" || relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative)) {
        throw new TypeError("transportStateDir must not be within the shared mesh root");
      }
      if (options.transport?.self.hostId !== options.hostId) throw new TypeError("Transport hostId must match participant directory hostId");
    }
    // Prepare the participant-file lock receipt before any publication fence. A cold
    // Darwin/Windows native identity read must never be awaited while registries are held.
    this.#ownIncarnation = options.withPublicationFence
      ? ownProcessIncarnation().then((value) => {
          this.#ownIncarnationValue = value;
          return value;
        }, () => undefined)
      : Promise.resolve(undefined);
    this.#heartbeatMs = nats ? PARTICIPANT_HEARTBEAT_MS : Math.max(100, options.heartbeatMs ?? PARTICIPANT_HEARTBEAT_MS);
    this.#leaseMs = Math.max(this.#heartbeatMs * 2, options.leaseMs ?? PARTICIPANT_LEASE_MS);
    // A permanent non-lock refresh failure (EACCES, a refused confirmation) must not retrigger at every
    // canConsumeMesh gate or change wake: back off up to half a heartbeat so a recovered lease stays fresh.
    const half = Math.floor(this.#heartbeatMs / 2);
    this.#backgroundRefresh.backoffFailures(Math.min(1_000, half), half);
  }

  /** A retired owner lifecycle skips background work; a throwing token is retired too. */
  #live(): boolean {
    try { return this.options.live?.() ?? true; } catch { return false; }
  }

  /** True while no live lifecycle could be resolved for more than LIFECYCLE_LOST_TICKS heartbeats. */
  get degraded(): boolean { return this.#degraded; }

  /** Whether this directory currently owns an armed heartbeat timer (smarty-dev#10004). */
  get heartbeatArmed(): boolean { return this.#timer !== undefined; }

  /** A1's view seam: FILE mode intentionally exposes no transport view. */
  get participantView(): ParticipantView | undefined { return this.#participantView; }

  /** Composition aborts transport consumers when this local host lease is lost. */
  get transportLeaseSignal(): AbortSignal | undefined { return this.#transportLease?.signal; }

  /** Write/read rates for the host-local NATS projection; undefined for FILE transport. */
  get natsProjectionMetrics(): NatsLocalProjectionMetrics | undefined { return this.#localProjection?.metrics(); }

  #isNats(): boolean { return this.options.transport?.kind === "nats"; }

  #natsView(): KvParticipantDirectory {
    const view = this.#participantView;
    if (!view?.ready) throw new DirectoryNotReadyError();
    return view;
  }

  #transportLostNow(error: unknown): void {
    if (this.#closed || this.#transportLost) return;
    this.#cancelReconnectRefresh();
    if (this.#transportLease?.recovering()) {
      if (this.#transportRecovering) return;
      this.#transportRecovering = true;
      this.#leaseConfirmed = false;
      this.#natsConfirmedAt = 0;
      this.#refreshError = error;
      this.#routingError = error;
      this.controlAdmission();
      return;
    }
    this.#transportLost = true;
    this.#transportRecovering = false;
    this.#leaseConfirmed = false;
    this.#refreshError = error;
    this.#routingError = error;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#heartbeatDueAt = undefined;
    if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
    this.#refreshTimer = undefined;
    this.#refreshScheduled = false;
    try { this.#leaseAbort.abort(error); } catch { /* already aborted */ }
    const view = this.#participantView;
    if (view) void view.close().catch(() => undefined);
    this.controlAdmission(); // Terminal loss disarms heartbeats, but not its one-shot alarm.
  }

  #renewTransportLease(): Promise<void> {
    const lease = this.#transportLease;
    if (!lease || this.#closed || this.#transportLost) return Promise.resolve();
    if (this.#transportRenewing) return this.#transportRenewing;
    const signal = () => AbortSignal.any([this.#leaseAbort.signal, AbortSignal.timeout(TRANSPORT_RENEW_BUDGET_MS)]);
    const work = Promise.resolve().then(() => lease.renew(signal())).then(async value => {
      this.#transportLeaseWallDeadline = value.expiresAt - TRANSPORT_HOST_LEASE_SAFETY_MS;
      // smarty-dev#15310: a lease that expired in KV and was created again may have let another
      // machine register this root meanwhile. Re-check before admission resumes; yield if so.
      for (let attempt = 0; attempt <= 3; attempt++) {
        if (this.#closed) return;
        if (lease.takeRecreated() || lease.rootRecheck) this.#rootRecheckPending = true;
        if (!this.#rootRecheckPending || !lease.live()) break;
        // At most three re-checks per heartbeat; leave admission closed for the next tick.
        if (attempt === 3) return;
        const root = this.#natsPublished.get(this.options.rootId);
        const view = this.#participantView;
        if (root?.kind === "root" && view) {
          try { await view.confirmSoleRoot(root, lease.revision, signal()); }
          catch (error) {
            if ((error as { code?: unknown } | null)?.code === "FABRIC_PRESENCE_OWNED_ELSEWHERE") {
              this.#transportRecovering = false;
              this.#transportLostNow(error);
            }
            throw error; // Otherwise admission stays suspended; the next renewal checks again.
          }
        }
        if (this.#closed) return;
        this.#rootRecheckPending = false;
        // The re-created lease was written with rootRecheck (routes nothing); write it routable again.
        // That write may re-create it again: loop before opening recovery/publication.
        lease.clearRootRecheck();
        const routable = await lease.renew(signal());
        this.#transportLeaseWallDeadline = routable.expiresAt - TRANSPORT_HOST_LEASE_SAFETY_MS;
      }
      if (this.#transportRecovering && lease.live()) {
        this.#transportRecovering = false;
        this.#refreshError = undefined;
        this.#routingError = undefined;
        this.#natsConfirmedAt = 0;
        void this.#localProjection?.resumeAfterLeaseRecovery().catch(error => {
          this.#refreshError = error;
          this.#routingError = error;
        });
        this.scheduleRefresh();
      }
    }).catch(error => {
      if (!lease.live()) this.#transportLostNow(error);
      this.#refreshError = error;
      throw error;
    });
    this.#transportRenewing = work;
    const settled = (failed: boolean): void => {
      if (this.#transportRenewing !== work) return;
      this.#transportRenewing = undefined;
      if (this.#closed || this.#suspended || this.#transportLost) return;
      // Re-evaluate the cap after every PubAck without losing a joined heartbeat
      // turn. Failure samples a fresh recovery wait, but never postpones an earlier
      // cadence/retry deadline (a slow failure may have almost spent its lease).
      if (failed) this.#heartbeatDueAt = Math.min(this.#heartbeatDueAt ?? Infinity,
        performance.now() + this.#recoveryDelay());
      if (this.#timer) clearTimeout(this.#timer);
      this.#timer = undefined;
      this.#armHeartbeat();
    };
    void work.then(() => settled(false), () => settled(true));
    return work;
  }

  #recoveryDelay(): number {
    // Independent 1–5s recovery turns spread a shared outage without a hot loop.
    // The live-lease operation cap and any earlier heartbeat still take priority.
    return 1_000 + 4_000 * (this.options.random ?? Math.random)();
  }

  #remainingTransportLeaseMs(): number {
    // Both clocks can only shorten the scheduling budget. A backwards wall correction
    // cannot extend monotonic authority; host sleep cannot extend the broker's TTL.
    return Math.min(this.#transportLease?.remainingMs() ?? 0,
      Math.max(0, (this.#transportLeaseWallDeadline ?? 0) - Date.now()));
  }

  #cancelReconnectRefresh(preserveDeadline = false): void {
    const pending = this.#reconnectRefresh;
    if (pending?.timer) clearTimeout(pending.timer);
    pending?.ready?.abort();
    // A new identity fences even an already-running async continuation. Suspension keeps
    // the sampled deadline only for unconfirmed evidence, including a newer reconnect
    // received after the running commit. A confirmed token must not block heartbeats.
    this.#reconnectRefresh = preserveDeadline && pending &&
      this.#confirmedReconnectGeneration < this.#verifiedReconnectGeneration ? { dueAt: pending.dueAt } : undefined;
  }

  #scheduleReconnectRefresh(): void {
    if (this.#closed || this.#quiescing || this.#transportLost ||
      this.#confirmedReconnectGeneration >= this.#verifiedReconnectGeneration) return;
    if (this.#reconnectRefresh?.running) return;
    const lease = this.#transportLease;
    if (!lease?.revision) {
      // Bootstrap has no lease budget to sample yet, but ordinary publication must
      // not mistake the absence of an armed timer for eligibility to force presence.
      this.#reconnectRefresh ??= { dueAt: undefined };
      return;
    }
    // Reserve the first full operation AND one full retry, strictly inside the
    // existing 13s admission cutoff. Heartbeats renew independently during this wait.
    const window = Math.min(this.#heartbeatMs, Math.max(0, this.#remainingTransportLeaseMs() - 2 * TRANSPORT_RENEW_BUDGET_MS - 1));
    if (this.#closed || this.#transportLost) return; // remainingMs can latch terminal loss.
    const now = performance.now();
    const pending: ReconnectRefresh = this.#reconnectRefresh ?? { dueAt: undefined };
    pending.dueAt ??= now + window * (this.options.random ?? Math.random)();
    // Resume and repeated notifications never resample or extend the deadline. Retain
    // tighter caps so a backwards wall correction or renewed lease cannot undo them.
    pending.dueAt = Math.min(pending.dueAt, now + window);
    this.#reconnectRefresh = pending;
    if (this.#suspended || !this.#live()) return;
    // A real disconnect resets the view. Retain the original monotonic deadline
    // and use the watch's actual READY notification to reconsider its aged cap;
    // do not wait for (or couple lease renewal to) the next heartbeat.
    const view = this.#participantView;
    if (view && !view.ready) {
      if (!pending.ready) {
        const ready = new AbortController();
        pending.ready = ready;
        void (async () => {
          while (!view.ready) await view.whenChanged(this.options.rootId, ready.signal);
          if (!ready.signal.aborted && this.#reconnectRefresh === pending) this.#scheduleReconnectRefresh();
        })().catch(error => {
          if (!ready.signal.aborted && this.#reconnectRefresh === pending) {
            this.#refreshError = error;
            this.#routingError = error;
          }
        }).finally(() => { if (pending.ready === ready) delete pending.ready; });
      }
      // An already-armed lease renewal remains independent of view readiness.
      if (!pending.timer) return;
    }
    if (!view) return;
    const delay = Math.max(0, pending.dueAt - now);
    if (pending.timer) clearTimeout(pending.timer);
    const active = (): boolean => this.#reconnectRefresh === pending && !this.#closed &&
      !this.#suspended && !this.#quiescing && !this.#transportLost && this.#live();
    const run = async (): Promise<void> => {
      delete pending.timer;
      try {
        if (!active()) return;
        pending.running = true;
        await this.#renewTransportLease();
        // Renewal alone can leave cached presence looking fresh after a JetStream restart.
        // Repeated notifications during publication share this work, with no further delay.
        while (active() && lease.live() && this.#confirmedReconnectGeneration < this.#verifiedReconnectGeneration) {
          await this.#refreshNats(true, pending);
        }
      } catch (error) {
        if (active()) {
          this.#refreshError = error;
          this.#routingError = error;
        }
      } finally {
        if (this.#reconnectRefresh === pending) {
          // Failed or unready publication is still unconfirmed evidence. Keep its
          // elapsed deadline, but let READY or the next heartbeat retry (never a hot loop).
          pending.ready?.abort();
          this.#reconnectRefresh = this.#confirmedReconnectGeneration < this.#verifiedReconnectGeneration
            ? { dueAt: pending.dueAt } : undefined;
          if (this.#reconnectRefresh && !view.ready) this.#scheduleReconnectRefresh();
        }
      }
    };
    if (delay <= 0) void run();
    else {
      pending.timer = setTimeout(() => { void run(); }, delay);
      pending.timer.unref?.();
    }
  }

  // The one heartbeat timer. Every tick reads the owner's lifecycle first and skips when it
  // retired; the background work itself runs through #runBackground (smarty-dev#5962/#10004).
  #armHeartbeat(): void {
    if (this.#timer || this.#closed || this.#suspended || this.#transportLost) return;
    const now = performance.now(), nats = this.#isNats();
    const budget = nats && this.#transportLease?.revision
      ? Math.max(0, Math.floor(this.#remainingTransportLeaseMs() - TRANSPORT_RENEW_BUDGET_MS - 1)) : undefined;
    if (this.#closed || this.#transportLost) return; // Budget read can latch terminal loss.
    let delay = this.#heartbeatDueAt === undefined
      ? budget === 0 ? this.#recoveryDelay()
        // NATS draws 4–5s: a 5s failure still leaves time for the joined retry.
        // FILE keeps its original independent 0.8–1.2 half-lease cadence.
        : this.#heartbeatMs * (0.8 + (nats ? 0.2 : 0.4) * (this.options.random ?? Math.random)())
      : Math.max(0, this.#heartbeatDueAt - now);
    this.#heartbeatDueAt ??= now + delay;
    // Reserve one full operation strictly inside admission. If none fits, keep
    // the sampled bounded recovery deadline, including an earlier failed turn;
    // never replace it with a constant tick or a zero-delay budget loop.
    if (budget !== undefined && budget > 0) delay = Math.min(delay, budget);
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      this.#heartbeatDueAt = undefined;
      if (this.#closed || this.#suspended || this.#transportLost) return;
      // Arm before work so suspend/close during the tick can cancel its successor.
      // Renewals keep their existing coalescing; a slow write does not shift the cadence.
      this.#armHeartbeat();
      if (this.#isNats()) {
        const live = this.#live();
        this.#lifecycleTick(live);
        this.controlAdmission();
        // A notification can arrive while the lifecycle is retired but not yet
        // suspended. Once live again, arm its retained deadline rather than losing it.
        const renewing = this.#transportRenewing;
        if (live) this.#scheduleReconnectRefresh();
        if (live) {
          // A 4s tick can join a 5s PubAck. Do not lose that renewal turn: the next
          // random interval could be 5s, too late for another 5s operation at the old
          // request-start cutoff. Joined ticks coalesce one fresh CAS after it settles.
          const retry = (): Promise<void> | undefined => {
            if (!this.#closed && !this.#suspended && !this.#transportLost && this.#live()) return this.#renewTransportLease();
            return undefined;
          };
          void (renewing ? renewing.then(retry, retry) : retry())?.catch(() => undefined);
          if (!this.#quiescing && !this.#transportRecovering && !this.#reconnectRefresh) void this.#refreshNats(true).catch(error => { this.#refreshError = error; });
        }
        return;
      }
      const live = this.#live();
      this.#lifecycleTick(live);
      if (!live) { this.controlAdmission(); return; }
      // The retry runner coalesces shared publication, not independent liveness.
      // An admitted resident stays live during registry/key waits without rewriting
      // its actor files. This does not certify shared admission or advance confirmation.
      if (this.options.enabled && !this.#quiescing &&
        ((this.#leaseConfirmed && this.options.actorRenewalAllowed) || (this.#refreshing && this.#fileWork > 0))) {
        void this.#renewFileLease().catch(error => {
          this.#refreshError = this.#superseded ?? error;
          this.#routingReadAt = 0;
          if (this.#retiredDuring(error)) return;
          this.#backgroundRefresh.failure(error);
          this.controlAdmission();
        });
        if (this.#refreshing && this.#fileWork > 0) { this.controlAdmission(); return; }
      }
      if (!this.#publicationRetryTimer && !this.#publicationRetrying) this.#runBackground(() => this.refresh());
      else this.controlAdmission();
    }, Math.max(1, Math.floor(delay)));
    this.#timer.unref();
  }

  /**
   * A background refresh whose owner lifecycle retired while it ran is a quiet skip, never a
   * "background operation failed" line every heartbeat (smarty-dev#10004). Pi's stale-ctx error is
   * proof that the ctx or pi a source reached is gone: stop this heartbeat until a live rebind.
   */
  #runBackground(operation: () => Promise<void>): void {
    void this.#backgroundRefresh.run(async () => {
      try { await operation(); }
      catch (error) {
        if (this.#retiredDuring(error)) return;
        throw error;
      }
    }, false).finally(() => {
      // Observe even a backed-off/skipped heartbeat. A completed recovery clears its
      // episode before this check; the independent threshold covers in-flight work.
      this.controlAdmission();
    });
  }

  #retiredDuring(error: unknown): boolean {
    // smarty-dev#8881: the owner lifecycle decides first. When it already retired (SDK dispose,
    // reload, replacement) any failure is that retirement's side effect: suspend quietly, with no
    // degraded notice. Only a stale SOURCE under a still-live OWNER is a degraded directory.
    if (!this.#closed && !this.#live()) {
      this.suspend();
      return true;
    }
    if (isStaleExtensionContextError(error)) {
      this.#lifecycleRetired();
      return true;
    }
    return false;
  }

  #lifecycleRetired(): void {
    this.suspend();
    if (this.#degraded) return;
    this.#degraded = true;
    const ticks = Math.max(this.#retiredTicks, LIFECYCLE_LOST_TICKS + 1);
    diagnostics.warn("src/topology/participant-directory.ts", `[pi-fabric] participant heartbeat: the session ctx it read was retired (reload or session replacement); ` +
      `${this.options.hostId} is degraded and its heartbeat is stopped until a live session rebinds it.`);
    try { this.options.onLifecycleLost?.(ticks); } catch { /* the warning above already surfaced it */ }
  }

  /**
   * Stop this directory's background heartbeat now, without withdrawing anything: its owner
   * lifecycle retired (reload, session replacement, teardown). Explicit refresh(), quiesce()
   * and close() still publish. resume() re-arms exactly one timer (smarty-dev#10004).
   */
  suspend(): void {
    if (this.#closed) return;
    this.#suspended = true;
    this.#cancelControlAdmissionTimer();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#heartbeatDueAt = undefined;
    this.#cancelReconnectRefresh(true);
    if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
    this.#refreshTimer = undefined;
    this.#refreshScheduled = false;
    this.#cancelPublicationRetry();
  }

  /** A live owner lifecycle rebound this directory: re-arm its single heartbeat and publish. */
  resume(): void {
    if (this.#isNats() && !this.#transportLease?.live()) return;
    if (!this.#suspended || this.#closed || this.#superseded || !this.options.enabled) return;
    this.#suspended = false;
    this.#retiredTicks = 0;
    this.#degraded = false;
    this.#armHeartbeat();
    if (this.#isNats()) this.#scheduleReconnectRefresh();
    this.scheduleRefresh();
  }

  #lifecycleTick(live: boolean): void {
    if (live) {
      this.#retiredTicks = 0;
      this.#degraded = false;
      return;
    }
    if (++this.#retiredTicks <= LIFECYCLE_LOST_TICKS || this.#degraded) return;
    this.#degraded = true;
    diagnostics.warn("src/topology/participant-directory.ts", `[pi-fabric] participant heartbeat: no live session ctx for ${this.#retiredTicks} heartbeats; ` +
      `${this.options.hostId} is degraded and its lease is not renewed until a live session rebinds it.`);
    try { this.options.onLifecycleLost?.(this.#retiredTicks); } catch { /* the warning above already surfaced it */ }
  }

  registerSource(source: ParticipantSnapshotSource, options?: { isolateErrors?: boolean; name?: string }): () => void {
    this.#sources.add(source);
    this.#sourceNames.set(source, options?.name?.trim().slice(0, 80) || this.#sourceNames.get(source) || `source-${++this.#sourceSequence}`);
    // Root/authority sources must remain fatal; only independent operational sources opt in.
    if (options?.isolateErrors) this.#isolatedSources.add(source);
    else this.#isolatedSources.delete(source);
    if (this.#timer) this.scheduleRefresh();
    return () => {
      if (this.#isNats()) for (const id of this.#sourceIds.get(source) ?? []) {
        if (id !== this.options.rootId) this.#natsAbsent.add(id);
      }
      this.#sources.delete(source);
      this.#isolatedSources.delete(source);
      this.#sourceNames.delete(source);
      this.#sourceIds.delete(source);
      this.#sourceOmittedIds.delete(source);
      this.#sourceFailures.delete(source);
      this.controlAdmission();
      if (this.#timer) this.scheduleRefresh();
    };
  }

  #snapshot(source: ParticipantSnapshotSource): FabricParticipantRecord[] {
    let failed = false, wholeSource = false, detail: unknown;
    const failedIds = new Set<string>();
    const reportFailure = (error: unknown, participantId?: string): void => {
      if (!failed) detail = error;
      failed = true;
      if (participantId) failedIds.add(participantId);
      else wholeSource = true;
    };
    let records: FabricParticipantRecord[];
    try { records = source(reportFailure); } // Only the direct callback is eligible for isolation.
    catch (error) {
      if (!this.#isolatedSources.has(source)) throw error;
      reportFailure(error);
      records = []; // Never reuse a prior source snapshot as current presence.
    }
    if (failed && !this.#isolatedSources.has(source)) throw detail;
    const previous = this.#sourceIds.get(source);
    const ids = new Set(records.map(record => record.id));
    const omitted = new Set<string>();
    if (failed) {
      for (const id of previous ?? []) if (wholeSource || failedIds.has(id)) {
        if (!ids.has(id)) omitted.add(id);
        ids.add(id);
      }
      let incident = this.#sourceFailures.get(source);
      if (!incident) {
        incident = { source: this.#sourceNames.get(source)!, since: Date.now(), reason: "participant snapshot source failed",
          dedupeKey: `participant-source:${this.options.hostId}:${this.ownerIncarnation}:${randomUUID()}`, alarmed: false };
        this.#sourceFailures.set(source, incident);
        // Dedupe by source + fixed reason, NOT changing error text or failed item.
        logDiagnostic("src/topology/participant-directory.ts", `[pi-fabric] isolated participant snapshot source ${incident.source} failed; omitting failed records: ${detail instanceof Error ? detail.message : String(detail)}`);
      }
      if (!wholeSource && failedIds.size === 1) incident.participantId = failedIds.values().next().value!;
      else delete incident.participantId;
    } else this.#sourceFailures.delete(source);
    if (this.#isNats()) for (const id of previous ?? []) {
      if (!ids.has(id) && id !== this.options.rootId) this.#natsAbsent.add(id);
    }
    this.#sourceIds.set(source, ids);
    this.#sourceOmittedIds.set(source, omitted);
    return records;
  }

  isolatedSourceFailures(): FabricIsolatedSourceFailure[] {
    return [...this.#sourceFailures.values()].slice(0, 100).map(({ source, since, reason, participantId }) =>
      ({ source, since, reason, ...(participantId ? { participantId } : {}) }));
  }

  publishBeforeLaunch(record: FabricParticipantRecord): Promise<void> {
    if (!this.#isNats()) return Promise.resolve();
    const view = this.#participantView;
    if (!view) return Promise.reject(new TransportUnavailableError("NATS participant view is unavailable"));
    if (!this.canConsumeMesh()) return Promise.reject(new TransportUnavailableError("NATS participant admission is stopped"));
    const owned = structuredClone(this.#natsRecord(record));
    if (NATS_TERMINAL_STATUSES.has(owned.status)) return Promise.reject(new TransportUnavailableError("Terminal participant cannot be admitted"));
    const previous = this.#natsPending.get(owned.id);
    // Reserve synchronously; source selection AND commit skip pending ids. Join any
    // earlier source pass before choosing create-only versus an already-owned CAS.
    const pending = Promise.resolve().then(async () => {
      await previous;
      await this.#refreshing;
      if (!this.canConsumeMesh()) throw new TransportUnavailableError("NATS participant admission is stopped");
      if (this.#natsPublished.has(owned.id)) await view.publish(owned);
      else await view.publishBeforeLaunch(owned);
      if (!this.canConsumeMesh()) {
        await view.remove(owned.id);
        throw new TransportUnavailableError("NATS participant admission stopped before launch");
      }
      this.#natsRetired.delete(owned.id);
      this.#natsAbsent.delete(owned.id);
      this.#natsPrelaunch.set(owned.id, owned);
      this.#natsPublished.set(owned.id, owned);
      this.#natsPublishedAt.set(owned.id, Date.now());
    });
    this.#natsPending.set(owned.id, pending);
    void pending.finally(() => {
      if (this.#natsPending.get(owned.id) === pending) this.#natsPending.delete(owned.id);
    }).catch(() => undefined);
    return pending;
  }

  withdrawBeforeLaunch(id: string): Promise<void> {
    if (!this.#isNats()) return Promise.resolve();
    const previous = this.#natsPending.get(id);
    this.#natsRetired.add(id);
    const pending = Promise.resolve().then(async () => {
      await previous?.catch(() => undefined);
      await this.#refreshing?.catch(() => undefined);
      this.#natsRetired.add(id);
      this.#natsPrelaunch.delete(id);
      this.#natsPublished.delete(id);
      this.#natsPublishedAt.delete(id);
      this.#localRecords.delete(id);
      await this.#participantView?.remove(id);
    });
    this.#natsPending.set(id, pending);
    void pending.finally(() => {
      if (this.#natsPending.get(id) === pending) this.#natsPending.delete(id);
    }).catch(() => undefined);
    return pending;
  }

  async start(): Promise<void> {
    this.#assertLeaseActive();
    if (this.#isNats()) {
      if (this.#closed || this.#transportLost) throw new TransportUnavailableError("NATS directory requires a new activation");
      if (!this.#natsStarting) this.#natsStarting = this.#startNats();
      await this.#natsStarting;
      return;
    }
    if (this.#timer) return;
    if (this.#closed) {
      this.#leaseAbort = new AbortController();
      this.#startedAt = Date.now();
      this.#incarnationToken = randomUUID();
      this.#leasePredecessor = this.options.enabled ? hostLeasePredecessor(this.mesh.root, this.options.hostId) : undefined;
      this.#leaseClaimed = false;
      this.#prepareLeaseLock = true;
    }
    // Restarting this directory is activation too, even if its last local view held a root.
    if (this.#closed) this.#localRecords.delete(this.options.rootId);
    this.#legacyRenewals = undefined;
    this.#rootCollisionPass = undefined;
    this.#closed = false;
    this.#refreshError = undefined;
    this.#publicationRetryDelay = 750;
    this.#routingReadAt = 0;
    this.#leaseConfirmed = false;
    this.#refreshedAt = Date.now();
    if (this.options.enabled) {
      // Start before the initial publish: its per-key work can contend too. The
      // timer also retries a failed initial publish so the host can join later.
      this.#retiredTicks = 0;
      this.#degraded = false;
      this.#suspended = false;
      this.#armHeartbeat();
      this.#installExitHook();
    }
    await this.refresh();
  }

  async #startNats(): Promise<void> {
    if (this.#timer) return;
    if (!this.#transportLease || !this.#participantView) {
      const [{ TransportHostLease }, { KvParticipantDirectory }, { NatsLocalProjection }] = await Promise.all([
        import("./host-leases.js"), import("./participant-view.js"), import("./nats-local-projection.js"),
      ]);
      const transport = this.options.transport;
      if (!transport || !this.options.transportStateDir) throw new Error("NATS participant carrier is incomplete");
      const lease = new TransportHostLease({
        kv: transport.kv("leases"), self: transport.self, rootId: this.options.rootId,
        identityId: this.options.identity.id, incarnation: this.ownerIncarnation,
        localStateDir: this.options.transportStateDir,
        onLost: error => this.#transportLostNow(error),
        onSuspended: error => this.#transportLostNow(error),
      });
      this.#transportLease = lease;
      this.#transportReconnectUnsubscribe = transport.onVerifiedReconnect?.(() => {
        if (this.#closed || this.#quiescing || this.#transportLost) return;
        this.#lifecycleTick(this.#live());
        // Verified reconnect evidence survives a retired lifecycle and bootstrap too.
        this.#verifiedReconnectGeneration += 1;
        this.#scheduleReconnectRefresh();
      });
      // Presence writes stay closed while the lease recovers, including after a re-created lease
      // until its root re-check (confirmSoleRoot) passes: a live lease alone is not enough there.
      const gate: ParticipantLeaseGate = {
        live: () => lease.live() && !this.#transportRecovering,
        recovering: () => lease.recovering() || this.#transportRecovering,
        get signal() { return lease.signal; },
      };
      this.#participantView = new KvParticipantDirectory({
        transport, lease: gate, identityId: this.options.identity.id,
        ownerIncarnation: this.ownerIncarnation, rootId: this.options.rootId,
        ...(this.options.random ? { random: this.options.random } : {}),
        onError: error => { this.#refreshError = error; this.#routingError = error; },
      });
      this.#localProjection = new NatsLocalProjection({
        mesh: this.mesh, transport, leaseLive: () => lease.live(),
        onError: error => { this.#refreshError = error; this.#routingError = error; },
      });
    }
    const lease = this.#transportLease, view = this.#participantView;
    if (!lease || !view) throw new Error("NATS participant carrier is incomplete");
    if (this.#closed) throw new TransportUnavailableError("Closed NATS directories require a new activation");
    try {
      const acquired = await lease.acquire(AbortSignal.timeout(5_000));
      this.#transportLeaseWallDeadline = acquired.expiresAt - TRANSPORT_HOST_LEASE_SAFETY_MS;
      if (this.#closed) throw new TransportUnavailableError("Participant directory closed during acquisition");
      view.start();
      this.#localProjection?.start();
      this.#armHeartbeat();
      await Promise.all([this.#waitNatsReady(), this.#localProjection?.whenReady()]);
      this.#scheduleReconnectRefresh();
      await this.refresh();
    } catch (error) {
      await this.#closeNats();
      throw error;
    }
  }

  async #waitNatsReady(): Promise<void> {
    const view = this.#participantView;
    if (!view) throw new DirectoryNotReadyError();
    if (view.ready) return;
    const signal = AbortSignal.any([this.#leaseAbort.signal, AbortSignal.timeout(10_000)]);
    while (!view.ready) await view.whenChanged(this.options.identity.id, signal);
  }

  // An exit before close() finished withdrawing (a /quit or SIGTERM that ends the process
  // while close waits for a busy mesh lock, or a shutdown step that threw first) must not
  // leave this host fresh: synchronously drop its lease file so its records expire with
  // the stored lease, as close() would (smarty-dev#6622). A published reload keeps both, but
  // only when its withdrawal committed: otherwise the reload lease would keep stale child or
  // legacy entries alive after the process is gone (smarty-dev#6622, review round 2). A close()
  // still awaiting its withdrawal has not committed either (round 4).
  // smarty-dev#13645: Node's synchronous `exit` event cannot await the per-host custody gate, but
  // removeHostLeaseIf is itself a compare-and-remove: it renames the file aside, rechecks the moved
  // bytes' owner and restores anything else, so a successor's or a mirror's lease is never removed.
  #installExitHook(): void {
    this.#withdrawn = false;
    this.#withdrawalFailed = false;
    this.#withdrawalPending = false;
    if (this.#exitHook) return;
    this.#exitHook = () => {
      if (this.#withdrawn || !this.options.enabled) return;
      if (this.#reloadPublished && !this.#withdrawalFailed && !this.#withdrawalPending) return;
      try {
        // Only this incarnation's lease, by its UUID receipt rather than a repeatable start
        // time: a reloaded or superseding successor keeps the lease it wrote.
        const owner = this.#leaseOwner();
        removeHostLeaseIf(this.mesh.root, this.options.hostId, (lease) => sameHostLeaseOwner(lease, owner));
      } catch { /* Best-effort exit cleanup. */ }
    };
    process.once("exit", this.#exitHook);
  }

  #removeExitHook(): void {
    if (this.#exitHook) process.removeListener("exit", this.#exitHook);
    this.#exitHook = undefined;
  }

  /**
   * One version-fenced batch per attempt, reread fresh and retried while the mesh lock is
   * busy or a delete was skipped on a version conflict: a skipped delete is not a withdrawal.
   * Committed only once a fresh reread selects nothing left to withdraw.
   */
  async #withdraw(select: () => MeshBatchOperation[],
    commit: (ops: MeshBatchOperation[]) => Promise<readonly { key: string; applied: boolean }[]>,
  ): Promise<{ ok: true } | { ok: false; reason: string }> {
    const text = (error: unknown): string => error instanceof Error ? error.message : String(error);
    let reason = "no attempt";
    let lockWaits = 0;
    for (let attempt = 1; attempt <= CLOSE_WITHDRAW_ATTEMPTS; attempt++) {
      try {
        const ops = select();
        if (ops.length === 0) return { ok: true };
        const skipped = (await commit(ops))
          .filter((result) => !result.applied).map((result) => result.key);
        if (skipped.length === 0) return { ok: true };
        reason = `delete skipped on a version conflict: ${skipped.join(", ")}`;
      } catch (error) {
        reason = text(error);
        if (!(isMeshWriteDeferred(error) || isMeshLockTimeout(error)) || ++lockWaits >= CLOSE_WITHDRAW_LOCK_WAITS) return { ok: false, reason };
      }
    }
    try {
      if (select().length === 0) return { ok: true };
    } catch (error) { reason = text(error); }
    return { ok: false, reason };
  }

  // Keep the entries: they lapse with this host's lease, which nothing renews after close().
  #withdrawalFailure(reason: string): void {
    this.#withdrawalFailed = true;
    if (this.#withdrawalWarned) return;
    this.#withdrawalWarned = true;
    const message = `participant withdrawal for ${this.options.hostId} did not commit (${reason}); ` +
      "its entries now lapse with their unrenewed lease";
    try {
      if (this.options.onWithdrawalFailure) this.options.onWithdrawalFailure(message);
      else diagnostics.warn("src/topology/participant-directory.ts", `[pi-fabric] ${message}`);
    } catch { /* A diagnostic sink never fails close(). */ }
  }

  // Publishes changed local records soon: at once after a quiet second, otherwise at the
  // end of that second (one write for a burst of changes).
  scheduleRefresh(): void {
    if (this.#isNats()) {
      this.#scheduleNatsRefresh();
      return;
    }
    if (this.#closed || this.#superseded || isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError)) return;
    if (this.#refreshing) {
      this.#refreshAgain = true;
      return;
    }
    if (this.#refreshScheduled) return;
    this.#refreshScheduled = true;
    const run = (): void => {
      if (!this.#refreshScheduled || this.#closed) return;
      this.#refreshScheduled = false;
      this.#refreshTimer = undefined;
      if (!this.#live()) return;
      this.#runBackground(() => this.#runRefresh(false));
    };
    const wait = this.#changeRefreshAt + CHANGE_REFRESH_MIN_MS - Date.now();
    if (wait <= 0) {
      queueMicrotask(run);
      return;
    }
    this.#refreshTimer = setTimeout(run, wait);
    this.#refreshTimer.unref?.();
  }

  #scheduleNatsRefresh(): void {
    if (this.#closed || this.#suspended || this.#quiescing || this.#transportLost || this.#refreshScheduled) return;
    if (this.#refreshing) { this.#refreshAgain = true; return; }
    this.#refreshScheduled = true;
    this.#refreshTimer = setTimeout(() => {
      this.#refreshTimer = undefined;
      this.#refreshScheduled = false;
      if (this.#closed || this.#quiescing || this.#suspended || !this.#live()) return;
      void this.#refreshNats(false).catch(error => { this.#refreshError = error; });
    }, Math.max(0, this.#changeRefreshAt + CHANGE_REFRESH_MIN_MS - Date.now()));
    this.#refreshTimer.unref?.();
  }

  /** Actor mutations publish changes, not a full heartbeat or a deferred-write retry. */
  async refreshPresence(): Promise<void> {
    this.#assertLeaseActive();
    if (this.#isNats()) {
      await this.#refreshNats(false);
      return;
    }
    if (isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError)) return;
    // A setter may finish saving after an in-flight heartbeat selected its source.
    // Wait for that round, then flush the pending revision under a fresh registry
    // fence. Coalescing with it could acknowledge presence it never committed.
    while (this.#refreshing) {
      try { await this.#refreshing; }
      catch (error) {
        if (isMeshWriteDeferred(error) || isMeshLockTimeout(error)) return;
        throw error;
      }
    }
    if (isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError)) return;
    // Do not call refresh(): mutation callers publish changed content, not a
    // heartbeat. The next full round verifies all rows and renews the host lease.
    await this.#runRefresh(false);
  }

  /** A heartbeat: publishes the records and renews this host's lease. */
  async refresh(): Promise<void> {
    if (this.#closed) return;
    this.#assertLeaseActive();
    if (this.#isNats()) {
      await this.#refreshNats(true);
      return;
    }
    // Independent file renewal must not delay shared admission or let an
    // in-flight change failure disappear before a heartbeat joins its receipt.
    // smarty-dev#8880: while a publication retry is in flight, its admission has just opened a short free
    // mesh window. Preparation and the mesh try go first; the host lease renewal starts once it settles.
    const deferRenewal = this.#publicationRetrying !== undefined;
    // Preparation owns the cancellable wait; an eager zero-wait renewal must not
    // reject startup (or recovery) before that wait can observe close().
    const renewing = !this.options.enabled || this.#prepareLeaseLock || deferRenewal || (this.#quiescing && this.#reloadUntil === undefined)
      ? undefined : this.#renewFileLease();
    const join = (publication: Promise<void>): Promise<void> => deferRenewal
      ? publication
      : Promise.all([renewing, publication]).then(() => undefined);
    if (this.#refreshing) {
      // smarty-dev#8880: an explicit refresh does not wait for a retry's admission window; the heartbeat
      // timer never calls refresh() while a retry runs, so only an explicit caller gets here. Its try goes now.
      this.#admitting?.interrupt();
      // A key waiter is still a live host. Do not turn this into a mesh-lock bypass:
      // confirmation remains gated on the shared write; shared-lock-only waits still lapse.
      if (this.#fileWork > 0 && this.options.enabled && !this.#quiescing) await this.#renewFileLease();
      if (this.#refreshingFull) return join(this.#refreshing);
      // A change-only refresh may skip its write; renew the lease right after it.
      return join(this.#refreshing.catch(error => {
        if (isMeshWriteDeferred(error) || isMeshLockTimeout(error)) throw error;
      }).then(() => this.refresh()));
    }
    return join(this.#runRefresh(true, renewing));
  }

  async #runRefresh(full: boolean, leaseRenewal?: Promise<number>): Promise<void> {
    if (this.#isNats()) {
      await this.#refreshNats(full);
      return;
    }
    if (this.#closed) return;
    this.#assertLeaseActive();
    if (this.#refreshing) {
      this.#refreshAgain = true;
      return;
    }
    const startedAt = Date.now();
    this.#refreshStartedAt = startedAt;
    if (!full) this.#changeRefreshAt = startedAt;
    // Include preparation in #refreshing so a cold heartbeat coalesces and close
    // drains it, but do not acquire any registry fence until identity is ready.
    const operation = this.#ownIncarnation.then(async () => {
      if (this.#closed) return false;
      // Join the one host lease write outside registry custody. A slow publication
      // never turns actor rows into independent heartbeat work.
      const leaseAt = await (leaseRenewal ?? this.#leaseRenewing);
      if (this.#closed) return false;
      if (this.options.enabled && this.#prepareLeaseLock) {
        await this.#withLeaseCommit(() => withHostLeaseLock(this.mesh, hostLeasePath(this.mesh.root, this.options.hostId), () => undefined,
          { timeoutMs: 1_000, signal: this.#leaseAbort.signal, ...this.#leaseLockIncarnation() }));
        this.#prepareLeaseLock = false;
        if (this.#closed) return false;
      }
      // Complete runtime mesh activation (custody + lineage resume) after the host
      // lease is joined/prepared and before any presence write.
      if (this.options.beforePublication) await this.options.beforePublication();
      if (this.#closed) return false;
      if (this.#prepareFileLocks) {
        await prepareParticipantFileLocks(this.mesh);
        this.#prepareFileLocks = false;
        if (this.#closed) return false;
      }
      if (!this.options.preparePublicationFence) return this.options.withPublicationFence
        ? this.options.withPublicationFence(() => this.#refresh(full, leaseAt)) : this.#refresh(full, leaseAt);
      // A changed preparation drops every selected operation. Re-select only after
      // the old registry/mesh fences have unwound; no snapshot crosses a retry wait.
      for (let attempt = 0; ; attempt++) {
        try { return await this.#refresh(full, leaseAt); }
        catch (error) {
          if (!(error instanceof ParticipantPublicationChangedError) || attempt >= 4 || this.#closed) throw error;
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }
    });
    this.#refreshingFull = full;
    // A joined refresh owns the whole receipt, including asynchronous lease
    // fencing and confirmation bookkeeping after the state operation settles.
    const refreshing = this.#finishRefresh(operation, full);
    this.#refreshing = refreshing;
    refreshing.catch(() => undefined);                        // awaiters still see a failure
    await refreshing;
  }

  async #finishRefresh(operation: Promise<number | false>, full: boolean): Promise<void> {
    let writeDeferred = false;
    try {
      const committed = await operation;
      if (!committed || this.#closed) return;
      this.#assertLeaseActive();
      // Fence delayed post-commit/confirmation continuations against a successor.
      if (this.options.enabled && (!this.#quiescing || this.#reloadUntil !== undefined)) {
        if (Date.now() - this.#lastLeaseRenewedAt >= this.#heartbeatMs) await this.#renewFileLease();
        else await this.#withLeaseFence(() => undefined);
      }
      // An unchanged change-refresh proves nothing about the lock. Only a committed
      // write/confirmWritable acquisition ends the background path's lock outage.
      if (this.options.enabled) this.#backgroundRefresh.success();
      // Preserve receipt age across post-commit copies and delayed continuations.
      this.#refreshedAt = committed;
      // Every commit witness up to here is accounted for by this receipt, our own commit's included.
      this.#commitWitnessSeen = latestCommitWitness(this.mesh.root);
      this.#leaseConfirmed = true;
      // Only a reload successor withheld its own lease before the host commit.
      if (this.#keptReloadLease && this.options.enabled && (!this.#quiescing || this.#reloadUntil !== undefined)) {
        await this.#renewFileLease();
      }
      this.#refreshError = undefined;
      this.#cancelPublicationRetry();
      this.#publicationRetryDelay = 750;
      this.#routingError = undefined;
      if (full) {
        this.#sweepDeadHosts();
        if (this.options.enabled && !this.#closed && !this.#quiescing && this.options.presencePass &&
          Date.now() - this.#presencePassAt >= (this.options.presencePassMs ?? 60_000)) {
          this.#presencePassAt = Date.now();
          void this.#notifications.enqueue(this.options.presencePass);
        }
      }
    } catch (error) {
      // A failed shared write is NOT a terminal liveness transition. An already-admitted
      // live process retains its ownership/incarnation and renews only that file lease.
      // Do not advance confirmedAt, clear the outage, admit an unregistered host, or let
      // canConsumeMesh treat this as a successful commit. Peers still use #484's grace.
      if (this.#leaseConfirmed && !this.#closed && !this.#quiescing && !(error instanceof HostLeaseLockBusyError)) {
        try { await this.#renewFileLease(); } catch (renewError) {
          if (renewError instanceof FabricHostLeaseSupersededError) error = renewError;
        }
      }
      if (error instanceof FabricHostLeaseSupersededError) this.#markSuperseded(error);
      error = this.#superseded ?? error;
      this.#refreshError = error;
      this.#routingReadAt = 0;
      if (error instanceof ParticipantFileLockBusyError) this.#prepareFileLocks = true;
      writeDeferred = isMeshWriteDeferred(error) || isMeshLockTimeout(error);
      if (error instanceof HostLeaseLockBusyError) this.#cancelPublicationRetry();
      if (writeDeferred) {
        // A change timer may predate this heartbeat and still be waiting to run.
        // Cancel it too: pending state rides one host-level recovery, not actor retries.
        if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
        this.#refreshTimer = undefined;
        this.#refreshScheduled = false;
      }
      throw error;
    } finally {
      this.#refreshing = undefined;
      this.#refreshStartedAt = undefined;
      if (this.#refreshAgain) {
        this.#refreshAgain = false;
        // Never retry separately for every mutation received during an outage.
        if (!writeDeferred) this.scheduleRefresh();
      }
      if (writeDeferred) this.#schedulePublicationRetry();
      this.controlAdmission();
    }
  }

  #cancelPublicationRetry(): void {
    if (this.#publicationRetryTimer) clearTimeout(this.#publicationRetryTimer);
    this.#publicationRetryTimer = undefined;
  }

  #schedulePublicationRetry(): void {
    if (!this.options.waitForPublicationRetry || !this.#timer || this.#closed || this.#quiescing ||
      !(isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError)) || this.#refreshError instanceof HostLeaseLockBusyError ||
      this.#publicationRetryTimer || this.#publicationRetrying) return;
    // Not tied to the heartbeat phase: one jittered, capped retry for the whole host.
    const wait = 50 + Math.floor(Math.random() * (this.#publicationRetryDelay - 50));
    this.#publicationRetryDelay = Math.min(2_000, this.#publicationRetryDelay * 2);
    this.#publicationRetryTimer = setTimeout(() => {
      this.#publicationRetryTimer = undefined;
      if (!this.#live()) return;
      const work = this.#retryPublication();
      this.#publicationRetrying = work;
      void work.finally(() => {
        this.#publicationRetrying = undefined;
        this.#schedulePublicationRetry();
      });
    }, wait);
    this.#publicationRetryTimer.unref?.();
  }

  async #retryPublication(): Promise<void> {
    try {
      // The failed fence has fully unwound. A FIFO mesh ticket can now wait without
      // occupying either actor registry; admission is not itself a heartbeat receipt.
      // Admission releases mesh BEFORE registries are taken, preserving #504/#531 and the registry -> mesh
      // lock order. Without a prepared fence, #refresh runs inside the publication fence: admit first.
      if (!this.options.preparePublicationFence) {
        await this.options.waitForPublicationRetry!();
        if (this.#closed || this.#quiescing || !(isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError))) return;
        await this.refresh();
        return;
      }
      // smarty-dev#8880: with a prepared fence, selection runs outside every fence, so the admission is
      // awaited inside #refresh, after selection and right before the fence: the short free window it opens
      // goes to the fence and the mesh try, not to selection. No fence is held while it waits.
      if (this.#closed || this.#quiescing || !(isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError))) return;
      this.#admitBeforeCommit = this.options.waitForPublicationRetry!;
      try { await this.refresh(); }
      finally {
        this.#admitBeforeCommit = undefined;
        const admitted = this.#admitted; this.#admitted = undefined;
        await admitted?.catch(() => undefined); // an interrupted admission still finishes inside this retry
      }
    } catch (error) {
      if (!(isMeshWriteDeferred(error) || isMeshLockTimeout(error))) this.#refreshError = error;
      this.#backgroundRefresh.failure(error);
    }
  }

  list(
    options: FabricParticipantListOptions = {},
    now = Date.now(),
  ): FabricParticipantInfo[] {
    if (this.#isNats()) {
      return this.#natsView().list(options);
    }
    if (!this.options.enabled) {
      const byId = new Map<string, FabricParticipantInfo>();
      for (const participant of this.#localRecords.values()) {
        if (options.scope === "lineage" && participant.rootId !== this.options.rootId) continue;
        if (options.kinds && !options.kinds.includes(participant.kind)) continue;
        byId.set(participant.id, { ...participant, local: true, stale: false });
      }
      const self = this.self(now, options);
      if (!options.kinds || options.kinds.includes(self.kind)) byId.set(self.id, self);
      return [...byId.values()];
    }
    // smarty-dev#4250: `background` is display-only by contract (FabricParticipantListOptions):
    // its only callers are the dashboard snapshot's participantInfos/peerInfos, which feed the
    // widget and never route, admit or write. So it may read a journal-followed state whose
    // terminal endpoint is not bound to the payload hash yet (displayOnly) and stay incremental.
    // Ownership, routing and delivery callers never pass background and keep the bound read.
    const display = options.background === true && options.fresh !== true;
    let read: MeshReadOptions = { fresh: options.fresh === true, ...(options.background ? { background: true } : {}),
      ...(display ? { displayOnly: true } : {}) };
    let parsed = this.#parsed(read, this.options.listReadCacheMs);
    let hosts = this.#liveHosts(parsed.hosts);
    // The one write below is the advisory collision refusal (#reportRefusal publishes). A display
    // view is only a change check for it: on any collision, re-read authoritatively (bound) and
    // report from that view alone, so an unbound endpoint can never publish a refusal.
    // A malformed mirror is the same: its refusal is published only from the bound re-read.
    if (display && (parsed.malformed.length > 0 ||
      [...parsed.shadowed, ...parsed.participants].some((participant) => this.#collides(participant, hosts, false)))) {
      read = { fresh: false, background: true };
      parsed = this.#parsed(read, this.options.listReadCacheMs);
      hosts = this.#liveHosts(parsed.hosts);
    }
    const byId = new Map<string, FabricParticipantInfo>();
    const refused: string[] = [];
    for (const mirror of parsed.shadowed) this.#collides(mirror, hosts);
    for (const participant of parsed.participants) {
      // Apply on every list call, after private validation, so cached file/state
      // copies age too without changing ownership/publication consumers.
      if (participant.stopReason === "owner-root-gone" && !retainedTerminalActor(participant, now)) continue;
      const owner = hosts.get(participant.ownerHostId);
      if (!ownerMatches(participant, owner, this.options.hostId)) continue;
      if (this.#collides(participant, hosts)) {
        refused.push(participant.id);
        continue;
      }
      const stale =
        !owner ||
        (owner.remoteHost ? this.#mirrorStatus(owner, now) === "lease-stale" : owner.expiresAt < now) ||
        (participant.status === "reloading" && (participant.reloadUntil ?? 0) < now) ||
        owner.identity.id !== participant.ownerIdentityId ||
        owner.rootId !== participant.rootId;
      if (stale && !options.includeStale) continue;
      const local = isLocal(participant, this.options.hostId);
      if (options.scope === "local" && !local) continue;
      if (options.scope === "lineage" && participant.rootId !== this.options.rootId) continue;
      if (options.kinds && !options.kinds.includes(participant.kind)) continue;
      byId.set(participant.id, { ...participant, local, stale,
        ...(owner?.remoteHost && this.#mirrorStatus(owner, now) === "bridge-stale" ? { bridgeStale: true as const } : {}) });
    }
    // A local record a mirror overwrote in the shared state still answers from memory until
    // this host's next heartbeat puts it back: local always wins (smarty-dev#2004).
    for (const id of refused) {
      const record = this.#localRecords.get(id);
      if (!record || byId.has(id)) continue;
      if (options.scope === "lineage" && record.rootId !== this.options.rootId) continue;
      if (options.kinds && !options.kinds.includes(record.kind)) continue;
      byId.set(id, { ...record, local: true, stale: false });
    }
    const legacyRoots = new Map(
      parsed.legacySessions
        .flatMap((entry) => {
          const root = legacyRootFromEntry(entry, this.options.rootId, now, this.mesh.root);
          return root ? [[root.id, root] as const] : [];
        }),
    );
    if (options.scope !== "local" && options.scope !== "lineage") {
      if (!options.kinds || options.kinds.includes("root")) {
        for (const root of legacyRoots.values()) {
          if (!byId.has(root.id)) byId.set(root.id, root);
        }
      }
      if (!options.kinds || options.kinds.includes("actor")) {
        for (const entry of parsed.legacyActors) {
          const actor = legacyActorFromEntry(entry, legacyRoots, now, this.options.terminalActor);
          if (actor && (!actor.stale || options.includeStale) && !byId.has(actor.id)) byId.set(actor.id, actor);
        }
      }
    }
    // Same read as the listing: a display listing's self fallback scan stays displayOnly too.
    const self = this.self(now, read);
    if (
      (!options.kinds || options.kinds.includes(self.kind)) &&
      options.scope !== "project" &&
      !byId.has(self.id)
    ) {
      byId.set(self.id, self);
    }
    return [...byId.values()].sort(
      (left, right) =>
        left.startedAt - right.startedAt || left.name.localeCompare(right.name) || left.id.localeCompare(right.id),
    );
  }

  // The directory's records, parsed once per parse of the shared state. list() ran for every
  // dashboard rebuild, peer listing and ownership check, and each call cloned every participant
  // and host record of the fleet state: most of an idle Pi's CPU (smarty-dev#557). A new parse
  // copies only the entries whose version moved; the fleet state is rewritten several times a
  // second, but by a few writers. The copies are frozen, since every caller now shares them.
  #parsed(read: MeshReadOptions, listReadCacheMs?: number): ParsedDirectory {
    const value = this.#parse(read, listReadCacheMs);
    // smarty-dev#4250: a displayOnly view is not bound to the payload hash, so it never
    // publishes a malformed-mirror refusal; a bound read (cached or not) reports, deduplicated.
    if (read.displayOnly !== true) for (const entry of value.malformed) this.#reportMalformedMirror(entry);
    // Do not memoize authority: a live/reloaded owner immediately vetoes even with unchanged mesh bytes.
    return value.terminalActors.length ? { ...value, participants: [...value.participants,
      ...value.terminalActors.flatMap(entry => this.#terminalOrParticipant(entry) ?? [])] } : value;
  }

  #terminalOrParticipant(entry: MeshStateEntry): FabricParticipantRecord | undefined {
    if (!isObject(entry.value) || entry.value.stopReason !== "owner-root-gone") return participantFromEntry(entry);
    const terminal = this.options.terminalActor?.(entry);
    if (terminal) return terminal;
    // A newer, unauthenticated observer envelope is not ownership. Resolve the
    // authentic native file immediately, without restoring/rewriting shared state.
    // Raw terminal candidates remain cached; both proof and fallback are read afresh
    // so lease-only changes cannot memoize an authority decision. Normal parsing
    // still requires updatedBy.id === ownerIdentityId, including this fallback.
    const file = readParticipantFile(this.mesh.root, entry.key);
    return file ? participantFromEntry(file) : undefined;
  }

  #parse(read: MeshReadOptions, listReadCacheMs?: number): ParsedDirectory {
    if (typeof this.mesh.stateToken !== "function" || typeof this.mesh.listAllShared !== "function") {
      const merged = mergeParticipantEntries(this.#participantFiles(read, listReadCacheMs), this.mesh.listAll(PARTICIPANT_PREFIX, read));
      return {
        hosts: this.mesh.listAll(HOST_PREFIX, read),
        ...this.#participantsOf(merged.entries),
        shadowed: merged.shadowed.flatMap((entry) => participantFromEntry(entry) ?? []),
        legacySessions: this.mesh.listAll(LEGACY_SESSION_PREFIX, read),
        legacyActors: this.mesh.listAll(LEGACY_ACTOR_PREFIX, read),
      };
    }
    const token = this.mesh.stateToken(read);
    // Freshness is paid once; every namespace below is selected from that exact
    // canonical snapshot, never by issuing four independent fresh parses.
    const snapshot = { snapshot: token };
    // Participant files are a second source (smarty-dev#2004): their listing is the same array
    // while no file changed, so it keys the cache with the state token.
    const files = this.#participantFiles(read, listReadCacheMs);
    const cached = this.#parsedCache;
    if (cached?.token === token && cached.files === files) return cached.value;
    const previous = this.#parsedEntries;
    const next = new Map<string, { source: Readonly<MeshStateEntry>; entry: MeshStateEntry }>();
    const copies = (prefix: string): MeshStateEntry[] => this.mesh.listAllShared(prefix, snapshot).map((shared) => {
      const known = previous.get(shared.key);
      // Journal replay preserves unchanged entry identities. A legacy writer can change
      // values/ownership without advancing version or updatedAt, so those labels alone
      // must not undo a fresh canonical observation. Compare bytes on full-parse fallback.
      const unchanged = known && (known.source === shared || JSON.stringify(known.source) === JSON.stringify(shared));
      const entry = unchanged ? known.entry : deepFreeze(structuredClone(shared) as MeshStateEntry);
      next.set(shared.key, { source: shared, entry });
      return entry;
    });
    const value: ParsedDirectory = {
      hosts: copies(HOST_PREFIX),
      ...((): Pick<ParsedDirectory, "participants" | "terminalActors" | "malformed" | "shadowed"> => {
        const merged = mergeParticipantEntries(files, copies(PARTICIPANT_PREFIX));
        return {
          ...this.#participantsOf(merged.entries),
          shadowed: merged.shadowed.flatMap((entry) => participantFromEntry(entry) ?? []),
        };
      })(),
      legacySessions: copies(LEGACY_SESSION_PREFIX),
      legacyActors: copies(LEGACY_ACTOR_PREFIX),
    };
    this.#parsedEntries = next;
    this.#parsedCache = { token, files, value };
    return value;
  }

  // Valid host records by id, each with its effective expiry: the later of its shared-state
  // lease and its file lease (smarty-dev#816).
  #liveHosts(entries: Iterable<MeshStateEntry>, leases: ReadonlyMap<string, FabricHostLease> = readHostLeases(this.mesh.root)): Map<string, FabricHostRecord & { mirror?: FabricMirrorLease; bridgeStale?: true }> {
    const hosts = new Map<string, FabricHostRecord & { mirror?: FabricMirrorLease; bridgeStale?: true }>();
    for (const entry of entries) {
      const host = hostFromEntry(entry);
      const mirror = host && hostMirrorLease(leases, host);
      if (host) hosts.set(host.id, { ...host, ...hostLiveness(leases, host),
        ...(mirror ? { mirror } : host.remoteHost && (host.mirrorLease === 1 || leases.get(host.id)?.mirrorLease === 1)
          ? { bridgeStale: true as const } : {}) });
    }
    return hosts;
  }

  #mirrorStatus(owner: FabricHostRecord & { mirror?: FabricMirrorLease; bridgeStale?: true }, now: number) {
    return mirroredOwnerRoutingStatus({ ...owner, remoteHost: owner.remoteHost! }, now);
  }

  // A mirrored record never shadows a local target (smarty-dev#2004): published participant IDs,
  // own identity and native host IDs/identities are claims, even after a host lease expires.
  // Only a Main claims its root; an agent/actor's rootId is lineage membership (smarty-dev#17198).
  // ponytail: a mirror that overwrote another local host's child agent is not detectable here;
  // that host's next heartbeat takes its key back, and the bridge never overwrites unmirrored keys.
  #collides(participant: FabricParticipantRecord, hosts: ReadonlyMap<string, FabricHostRecord>, report = true): boolean {
    if (participant.remoteHost === undefined) return false;
    const ids = new Set([participant.id, participant.rootId]);
    let collides = [...ids].some((id) =>
      this.#localRecords.has(id) || id === this.options.identity.id ||
      (this.options.identity.kind === "main" && id === this.options.rootId));
    for (const host of hosts.values()) {
      if (collides) break;
      collides = host.remoteHost === undefined &&
        (ids.has(host.id) || ids.has(host.identity.id) ||
          (host.identity.kind === "main" && ids.has(host.rootId)));
    }
    if (collides && report) this.#reportCollision(participant);
    return collides;
  }

  #reportCollision(participant: FabricParticipantRecord): void {
    this.#reportRefusal(participant.id, participant.remoteHost, "it collides with a local participant", {
      rootId: participant.rootId, ownerHostId: participant.ownerHostId,
    });
  }

  // Check initial registration AND heartbeat renames, using both file and legacy/state peers.
  // This is an alert, not a race-free exclusive claim or a fork-ancestry inference.
  #reportRootCollisions(root: FabricParticipantRecord): void {
    // Advisory only: collision alerts may lag the bounded idle view. They must not bypass
    // coalescing on every heartbeat; ownership/lineage/delivery reads below stay fresh.
    for (const peer of this.list({ scope: "project", kinds: ["root"] })) {
      if (peer.id === root.id || !["idle", "running", "reloading"].includes(peer.status)) continue;
      const sameSession = root.sessionId !== undefined && root.sessionId === peer.sessionId;
      const sameName = root.name !== "main" && root.name === peer.name;
      if (!sameSession && !sameName) continue;
      const collision: FabricRootCollision = {
        reason: sameSession ? "duplicate-session" : "duplicate-name",
        name: root.name,
        ids: [root.id, peer.id].sort(),
        ...(sameSession ? { sessionId: root.sessionId } : {}),
      };
      const key = JSON.stringify([collision.reason, sameSession ? root.sessionId : root.name, collision.ids]);
      if (this.#reportedRootCollisions.has(key) || this.#reportedRootCollisions.size >= 1_000) continue;
      this.#reportedRootCollisions.add(key);
      try { this.options.onRootCollision?.(collision); } catch { /* Advisory must not stop a heartbeat. */ }
      void this.#notifications.enqueue(() => this.mesh.publish({
        topic: ROOT_COLLISION_TOPIC, kind: "alert", from: this.options.identity,
        text: `Duplicate live Fabric root (${collision.reason}): ${collision.name}; ${collision.ids.join(", ")}. Fixture forks must use PI_FABRIC_FIXTURE=1.`,
        data: collision,
      }));
    }
  }

  // A record the bridge marked remoteHost that fails validation: dropped alone, logged once.
  #reportMalformedMirror(entry: MeshStateEntry): void {
    const value = isObject(entry.value) ? entry.value : undefined;
    if (!value || value.remoteHost === undefined) return;
    const id = typeof value.id === "string" ? value.id.slice(0, 200) : entry.key;
    const remoteHost = typeof value.remoteHost === "string" ? value.remoteHost.slice(0, 64) : "(invalid)";
    this.#reportRefusal(id, remoteHost, "it is malformed", {});
  }

  #reportRefusal(id: string, remoteHost: string | undefined, reason: string, data: Record<string, unknown>): void {
    const key = `${id}\0${remoteHost}\0${reason}`;
    if (this.#reportedCollisions.has(key) || this.#reportedCollisions.size >= 1_000) return;
    this.#reportedCollisions.add(key);
    void this.#notifications.enqueue(() => this.mesh.publish({
      topic: MIRROR_COLLISION_TOPIC,
      kind: "refused",
      from: this.options.identity,
      text: `Refused mirrored record ${id} from remote host ${remoteHost}: ${reason}`,
      data: { id, remoteHost, reason, ...data },
    }));
  }

  /** Validated mirror attribution, including expired leases; fresh and without publishing refusals. */
  mirroredControlOwner(
    ownerHostId: string,
    ownerIdentityId: string | undefined,
    targetId: string,
  ): { remoteHost: string; expiresAt: number; mirror?: FabricMirrorLease; bridgeStale?: true; ownerIncarnation?: string } | undefined {
    if (this.#isNats()) return undefined;
    if (!this.options.enabled || ownerIdentityId === undefined) return undefined;
    const read = { fresh: true };
    const entry = this.#participantEntry(keyFor(PARTICIPANT_PREFIX, targetId), read);
    const participant = entry ? participantFromEntry(entry) : undefined;
    if (
      !participant?.remoteHost ||
      participant.id !== targetId ||
      participant.kind !== "root" ||
      participant.rootId !== targetId ||
      participant.ownerHostId !== ownerHostId ||
      participant.ownerIdentityId !== ownerIdentityId
    ) return undefined;
    const leases = readHostLeases(this.mesh.root);
    const hosts = this.#liveHosts(this.mesh.listAll(HOST_PREFIX, read), leases);
    const owner = hosts.get(ownerHostId);
    if (
      !owner ||
      !ownerMatches(participant, owner, this.options.hostId) ||
      owner.identity.id !== ownerIdentityId ||
      owner.rootId !== participant.rootId ||
      !Number.isFinite(owner.expiresAt) ||
      this.#collides(participant, hosts, false)
    ) return undefined;
    const mirror = owner.mirror;
    return { remoteHost: participant.remoteHost, expiresAt: owner.expiresAt,
      ...(participant.ownerIncarnation !== undefined ? { ownerIncarnation: participant.ownerIncarnation } : {}),
      ...(mirror ? { mirror } : owner.bridgeStale ? { bridgeStale: true as const } : {}) };
  }

  #routingOwner(id: string) {
    if (!this.options.enabled) return undefined;
    const target = id === "main" ? this.options.rootId : id;
    const read = { snapshot: this.mesh.stateToken() };
    const entry = this.#participantEntry(keyFor(PARTICIPANT_PREFIX, target), read);
    const participant = entry ? participantFromEntry(entry) : undefined;
    if (!participant || participant.id !== target || participant.remoteHost) return undefined;
    const hostEntry = this.mesh.get(keyFor(HOST_PREFIX, participant.ownerHostId), read);
    const owner = hostEntry ? hostFromEntry(hostEntry) : undefined;
    if (!owner || !ownerMatches(participant, owner, this.options.hostId) ||
      owner.identity.id !== participant.ownerIdentityId || owner.rootId !== participant.rootId) return undefined;
    return { participant, owner, read };
  }

  #sessionEnded(rootId: string, read: MeshReadOptions = {}): boolean {
    const entry = this.mesh.get(keyFor(LINEAGE_CLOSURE_PREFIX, rootId), read);
    const value = entry?.value;
    return !!(isObject(value) && value.format === 1 && value.rootId === rootId &&
      value.ownerHostId === rootId && value.ownerIdentityId === rootId &&
      entry?.updatedBy.id === rootId && entry.updatedBy.kind === "main" &&
      typeof value.closedAt === "number" && Number.isFinite(value.closedAt));
  }

  /** A Main's close certifies its own host, not an independently leased resident.
   * The old root address stays closed even if another host retains its presence. */
  #routeEnded(participant: FabricParticipantRecord, read: MeshReadOptions = {}): boolean {
    return (participant.kind === "root" || participant.residency !== "durable" || participant.ownerHostId === participant.rootId) &&
      this.#sessionEnded(participant.rootId, read);
  }

  /** The closure check get() applies, read fresh, for a record get() did not answer (lastKnown).
   * Unknown (NATS, a disabled mesh) counts as ended: callers only accept a record that is not. */
  routeEnded(participant: FabricParticipantRecord): boolean {
    if (this.#isNats() || !this.options.enabled) return true;
    return this.#routeEnded(participant, { fresh: true });
  }

  /** Capture owner authority once. Subsequent ACK checks only stat/read that owner's file. */
  captureControlOwnerLease(ownerHostId: string, ownerIdentityId: string, targetId: string): (() => number) | undefined {
    if (this.#isNats()) {
      const target = targetId === "main" ? this.options.rootId : targetId;
      const route = this.#natsView().resolve(target);
      if (!route || route.ownerHostId !== ownerHostId || route.ownerIdentityId !== ownerIdentityId || !route.live) return undefined;
      return () => {
        const current = this.#natsView().resolve(target);
        return current?.live && current.machine === route.machine && current.ownerHostId === ownerHostId &&
          current.ownerIdentityId === ownerIdentityId && current.ownerIncarnation === route.ownerIncarnation
          ? Date.now() + PARTICIPANT_HEARTBEAT_MS : 0;
      };
    }
    const record = this.#routingOwner(targetId);
    if (!record || record.owner.id !== ownerHostId || record.owner.identity.id !== ownerIdentityId ||
      this.#routeEnded(record.participant, record.read)) return undefined;
    const { owner } = record;
    return () => {
      const lease = readHostLease(this.mesh.root, owner.id);
      return hostLeaseExpiry(lease ? new Map([[owner.id, lease]]) : new Map(), owner);
    };
  }

  /** Retained presence is not authority to route a long-dead or explicitly ended session. */
  retainedRouteAllowed(id: string): boolean {
    if (this.#isNats()) {
      const target = id === "main" ? this.options.rootId : id;
      return this.#natsView().resolve(target)?.live === true;
    }
    const record = this.#routingOwner(id);
    if (!record) return false;
    const now = (this.options.routingLease?.now ?? Date.now)();
    const expiry = this.captureControlOwnerLease(record.owner.id, record.owner.identity.id, id)?.();
    return expiry !== undefined && now - expiry <= participantLeaseGraceMs(this.options.routingLease?.graceMs);
  }

  /** Recovery is resolution-only: no mesh write, ownership transfer, or delivery retry here. */
  async resolveRoutingLease(id: string, signal?: AbortSignal, budget?: MirrorLeaseWaitBudget): Promise<boolean> {
    signal?.throwIfAborted();
    if (this.#isNats()) return this.#resolveNatsRoutingLease(id, signal);
    const record = this.#routingOwner(id);
    if (!record) return this.#resolveRemoteMirrorLease(id, signal, budget); // Unknown ids retain the ordinary resolver.
    const { participant, owner } = record;
    const options = this.options.routingLease ?? {};
    const now = (options.now ?? Date.now)();
    if (this.#routeEnded(participant, record.read)) return false;
    const snapshot = readHostLeaseSnapshot(this.mesh.root, owner.id);
    const lease = snapshot?.lease;
    const matching = lease && lease.rootId === owner.rootId && lease.identityId === owner.identity.id &&
      (owner.incarnationToken !== undefined ? lease.incarnationToken === owner.incarnationToken :
        (lease.startedAt === undefined || lease.startedAt === owner.startedAt));
    const expiresAt = hostLeaseExpiry(matching ? new Map([[owner.id, lease]]) : new Map(), owner);
    if (expiresAt >= now) return true;
    if (now - expiresAt > participantLeaseGraceMs(options.graceMs) ||
      participant.status === "stopping" || participant.status === "reloading") return false;
    // A direct `.lock` observer (smarty-dev#6477 A1 section 4): a heartbeat writer shows there on
    // the `file` backend only. With state in SQLite this reads the state write-lock signal (L2a).
    const lockWaiting = options.lockWaiting ?? (() => fs.existsSync(path.join(this.mesh.root, ".lock")));
    // A replaced lease file newer than the stored ownership record also explains a cached lapse.
    const advanced = matching && snapshot!.mtimeMs > owner.updatedAt;
    if (!advanced && !lockWaiting() && !this.writeStalled(now)) return true;
    await waitForHostLeaseRenewal(id, () => {
      const current = readHostLease(this.mesh.root, owner.id);
      return hostLeaseExpiry(current ? new Map([[owner.id, current]]) : new Map(), owner);
    }, options);
    // Shared authority and terminal state are rechecked once, not on each file poll.
    const current = this.#participantEntry(keyFor(PARTICIPANT_PREFIX, participant.id), { fresh: true });
    const refreshed = current ? participantFromEntry(current) : undefined;
    const currentOwnerEntry = this.mesh.get(keyFor(HOST_PREFIX, owner.id));
    const currentOwner = currentOwnerEntry ? hostFromEntry(currentOwnerEntry) : undefined;
    return !!refreshed && refreshed.ownerHostId === owner.id && refreshed.ownerIdentityId === owner.identity.id &&
      refreshed.rootId === participant.rootId && !refreshed.remoteHost &&
      !["stopping", "reloading"].includes(refreshed.status) && currentOwner !== undefined &&
      (owner.incarnationToken !== undefined ? currentOwner.incarnationToken === owner.incarnationToken : currentOwner.startedAt === owner.startedAt) &&
      currentOwner.identity.id === owner.identity.id && currentOwner.rootId === owner.rootId && !this.#routeEnded(refreshed);
  }

  async #resolveNatsRoutingLease(id: string, signal?: AbortSignal): Promise<boolean> {
    const view = this.#participantView;
    if (!view || this.#closed || this.#transportLost) throw new DirectoryNotReadyError();
    const target = id === "main" ? this.options.rootId : id;
    const configured = this.options.routingLease?.waitMs ?? 10_000;
    const waitMs = Number.isFinite(configured) ? Math.min(10_000, Math.max(1, configured)) : 10_000;
    const bounded = AbortSignal.any([this.#leaseAbort.signal, AbortSignal.timeout(waitMs), ...(signal ? [signal] : [])]);
    while (!view.ready) await view.whenChanged(target, bounded);
    const initial = view.resolve(target);
    if (!initial || initial.live) return initial?.live === true;
    for (;;) {
      await view.whenChanged(target, bounded);
      if (!view.ready) continue;
      const current = view.resolve(target);
      if (!current) return false;
      if (current.live) return true;
    }
  }

  /** fv2-11703: a validated remote Main mirror, read fresh. Malformed, colliding, missing-owner or
   * mismatched-owner mirrors are "none": they never authorize a wait nor become routable here. */
  #remoteMirrorLease(id: string): RemoteMirrorLease {
    if (!this.options.enabled) return { state: "none" };
    const target = id === "main" ? this.options.rootId : id;
    const read = { fresh: true };
    const entry = this.#participantEntry(keyFor(PARTICIPANT_PREFIX, target), read);
    const participant = entry ? participantFromEntry(entry) : undefined;
    if (!participant?.remoteHost || participant.id !== target || participant.kind !== "root" ||
      participant.rootId !== target) return { state: "none" };
    const hosts = this.#liveHosts(this.mesh.listAll(HOST_PREFIX, read));
    const owner = hosts.get(participant.ownerHostId);
    if (!owner || !ownerMatches(participant, owner, this.options.hostId) ||
      owner.identity.id !== participant.ownerIdentityId || owner.rootId !== participant.rootId ||
      !Number.isFinite(owner.expiresAt) || this.#collides(participant, hosts, false)) return { state: "none" };
    if (participant.status === "stopping" || this.#routeEnded(participant, read)) return { state: "ended" };
    const now = (this.options.routingLease?.now ?? Date.now)();
    const status = this.#mirrorStatus(owner, now);
    return {
      state: status === "lease-stale" ? "lapsed" : status === "live" ? "live" : "expired",
      ownerHostId: owner.id, ownerIdentityId: owner.identity.id, rootId: owner.rootId,
      remoteHost: participant.remoteHost, expiresAt: owner.expiresAt,
      ...(owner.mirror ? { mirror: owner.mirror } : {}),
      ...(owner.bridgeStale || owner.mirrorLease === 1 && !owner.mirror ? { bridgeStale: true as const } : {}),
    };
  }

  /** One source-aware, notification-driven wait. Old/missing required samples are doubt,
   * regardless of their projected deadline; only a fresh source lapse proves root death. */
  async #resolveRemoteMirrorLease(id: string, signal?: AbortSignal, budget?: MirrorLeaseWaitBudget): Promise<boolean> {
    signal?.throwIfAborted();
    const initial = this.#remoteMirrorLease(id);
    if (initial.state === "none" || initial.state === "live") return true;
    if (initial.state === "ended" || initial.state === "lapsed") return false;
    const current = await waitForMirroredLeaseRenewal(this.mesh.root, id, initial, () => {
      const owner = this.#remoteMirrorLease(id);
      return owner.state === "none" || owner.state === "ended" ? undefined : owner;
    }, owner => owner.ownerHostId === initial.ownerHostId && owner.ownerIdentityId === initial.ownerIdentityId &&
      owner.rootId === initial.rootId && owner.remoteHost === initial.remoteHost,
    { ...this.options.routingLease, signal, budget });
    return current !== undefined && current.state === "live";
  }

  lastKnown(id: string, now = Date.now()): { participant: FabricParticipantInfo; lapsedMs: number } | undefined {
    if (this.#isNats()) {
      const target = id === "main" ? this.options.rootId : id;
      const participant = this.#natsView().list({ scope: "project", includeStale: true }).find(candidate => candidate.id === target);
      return participant?.stale ? { participant, lapsedMs: Number.POSITIVE_INFINITY } : undefined;
    }
    if (!this.options.enabled) return undefined;
    const target = id === "main" ? this.options.rootId : id;
    // Fresh: this answers whether a lease lapsed, so a lease file read within the listing's reuse
    // window (smarty-dev#557) must not hide a lapse or removal since. It runs only for an unknown id.
    const participant = this.list({ scope: "project", includeStale: true, fresh: true }, now)
      .find((candidate) => candidate.id === target);
    if (!participant?.stale) return undefined;
    const entry = this.mesh.get(keyFor(HOST_PREFIX, participant.ownerHostId));
    const host = entry ? this.#liveHosts([entry]).get(participant.ownerHostId) : undefined;
    const sameHost = host && host.identity.id === participant.ownerIdentityId && host.rootId === participant.rootId;
    return { participant, lapsedMs: sameHost ? Math.max(0, now - host.expiresAt) : Number.POSITIVE_INFINITY };
  }

  // With the mesh enabled, every local record on it is one this host published at its last
  // refresh, so false is exact for get(id)?.local. A record left by this host's previous run, before its first
  // refresh here, counts as not published; it is removed or republished on that refresh.
  publishes(id: string): boolean {
    if (this.#isNats()) return this.#natsPublished.has(id === "main" ? this.options.rootId : id);
    return this.#localRecords.has(id === "main" ? this.options.rootId : id);
  }

  /**
   * fv2-11107: actor ownership by participant id (owner host === `localHostId`) from a full project list.
   * A fresh read, or a call without `actorIds`, is exactly `list({ scope: "project", fresh })` and never
   * touches the reuse below. A non-fresh (poll) call that names its managed actors reuses the previous
   * non-fresh map only while observeActorOwnership() of exactly those ids holds: their participant
   * record (file and shared), owner host record and lease, root closure, and every lease/reload
   * deadline. Unrelated commits or peer heartbeats therefore do not rebuild the fleet directory. A new
   * managed id, any changed fact or a reached deadline lists afresh. Reuse is armed only when every
   * managed id was answered by its own unmirrored participant record (no mirror collision, no legacy
   * or stale fallback), so the observation covers each input of that answer. A key whose file and
   * shared copies disagree is answered by updatedAt arbitration, which the facts ignore: the strict
   * observation then never validates, so such a poll always lists. Validation, not authority:
   * admission, custody and commit fences call with fresh=true.
   */
  actorOwnership(localHostId: string, options: { fresh?: boolean; actorIds?: Iterable<string> | undefined } = {}): ReadonlyMap<string, boolean> {
    if (this.#isNats()) {
      const ids = options.actorIds ? new Set(options.actorIds) : undefined;
      return new Map(this.#natsView().list({ scope: "project", kinds: ["actor"], includeStale: options.fresh !== true })
        .filter(record => !ids || ids.has(record.id))
        .map(record => [record.id, record.ownerHostId === localHostId] as const));
    }
    const fresh = options.fresh === true;
    const ids = !fresh && options.actorIds ? new Set(options.actorIds) : undefined;
    const cached = this.#pollOwnership;
    if (ids && cached && cached.hostId === localHostId && [...ids].every((id) => cached.ids.has(id))) {
      let valid = false;
      try { cached.observation.scope(ids); valid = cached.observation.unchanged(); } catch { valid = false; }
      // Only the validated (scoped) ids: an unscoped entry of the old map is unobserved and could
      // resurrect ownership that moved; absent ids have no directory opinion.
      if (valid) return new Map([...ids].flatMap((id) => cached.map.has(id) ? [[id, cached.map.get(id)!] as const] : []));
    }
    if (!fresh) this.#pollOwnership = undefined;
    // Observe BEFORE the decision (observeActorOwnership contract): a change in between invalidates.
    let observation: ActorOwnershipObservation | undefined;
    if (ids && ids.size > 0 && this.options.enabled) {
      try {
        observation = observeActorOwnership(this.mesh, ids, ownershipPointReads(this.mesh), {
          strict: { participant: (entry) => participantFromEntry(entry) !== undefined, host: (entry) => hostFromEntry(entry) !== undefined },
        });
      } catch { observation = undefined; }
    }
    const toMap = (listed: readonly FabricParticipantInfo[]): Map<string, boolean> =>
      new Map(listed.map((participant) => [participant.id, participant.ownerHostId === localHostId]));
    const exactFor = (listed: readonly FabricParticipantInfo[], read: MeshReadOptions): boolean => {
      const byId = new Map(listed.map((participant) => [participant.id, participant]));
      let parsed: ParsedDirectory | undefined;
      try { parsed = this.#parse(read, this.options.listReadCacheMs); } catch { parsed = undefined; }
      if (!parsed) return false;
      const own = new Map(parsed.participants.map((participant) => [participant.id, participant]));
      const mirrored = new Set(parsed.shadowed.map((participant) => participant.id));
      return [...ids!].every((id) => {
        const listedRecord = byId.get(id);
        const record = own.get(id);
        return listedRecord !== undefined && !listedRecord.stale && listedRecord.remoteHost === undefined &&
          record !== undefined && record.remoteHost === undefined && record.ownerHostId === listedRecord.ownerHostId &&
          !mirrored.has(id);
      });
    };
    const listed = this.list({ scope: "project", ...(fresh ? { fresh: true } : {}) });
    const map = toMap(listed);
    if (!ids || !observation) return map;
    if (!exactFor(listed, { fresh: false })) return map;
    // The non-fresh list may come from a parse cached BEFORE the observation (listReadCacheMs):
    // its map need not correspond to the observed facts. Arm only from a list read AFTER the
    // observation (fresh), re-checked for exactness on a fresh parse, and only if the observed
    // facts still hold afterwards, so the armed map provably answers the observed inputs.
    // Cost: one extra fresh read per arming (about once per owner lease period), none while armed.
    let armed: Map<string, boolean> | undefined;
    try {
      const freshListed = this.list({ scope: "project", fresh: true });
      if (exactFor(freshListed, { fresh: true }) && observation.unchanged()) armed = toMap(freshListed);
    } catch { armed = undefined; }
    if (!armed) return map;
    this.#pollOwnership = { hostId: localHostId, ids, map: armed, observation };
    return armed;
  }

  get(id: string, now = Date.now(), options: { fresh?: boolean } = {}): FabricParticipantInfo | undefined {
    const target = id === "main" ? this.options.rootId : id;
    if (this.#isNats()) return this.#natsView().get(target);
    // One record and its owner's lease, not a clone of the whole directory: actor ownership
    // checks call this per actor (smarty-dev#784). A missing or stale record takes the full
    // list, which also knows legacy roots, so the answer is the same.
    if (this.options.enabled) {
      const read = { snapshot: this.mesh.stateToken({ fresh: options.fresh === true }) };
      const entry = this.#participantEntry(keyFor(PARTICIPANT_PREFIX, target), read);
      const participant = entry ? participantFromEntry(entry) : undefined;
      if ((participant && this.#routeEnded(participant, read)) ||
        (target.startsWith("session:") && participant?.kind !== "root" && this.#sessionEnded(target, read))) return undefined;
      if (participant?.id === target && (
        participant.remoteHost === undefined ||
        !this.#collides(participant, this.#liveHosts(this.mesh.listAll(HOST_PREFIX, read)))
      )) {
        const hostEntry = this.mesh.get(keyFor(HOST_PREFIX, participant.ownerHostId), read);
        const host = hostEntry ? hostFromEntry(hostEntry) : undefined;
        const lease = host ? readHostLease(this.mesh.root, host.id) : undefined;
        const owner = hostEntry ? this.#liveHosts([hostEntry], lease ? new Map([[lease.id, lease]]) : new Map())
          .get(participant.ownerHostId) : undefined;
        const status = owner?.remoteHost ? this.#mirrorStatus(owner, now) : undefined;
        if (
          owner &&
          ownerMatches(participant, owner, this.options.hostId) &&
          !(participant.status === "reloading" && (participant.reloadUntil ?? 0) < now) &&
          (owner.remoteHost ? status !== "lease-stale" : owner.expiresAt >= now) &&
          owner.identity.id === participant.ownerIdentityId &&
          owner.rootId === participant.rootId
        ) {
          return { ...participant, local: isLocal(participant, this.options.hostId), stale: false,
            ...(status === "bridge-stale" ? { bridgeStale: true as const } : {}) };
        }
      }
    }
    return this.list({ scope: "project", ...(options.fresh ? { fresh: true } : {}) }, now)
      .find((participant) => participant.id === target);
  }

  /**
   * Shared by adoption and delivery. Live, stale and unknown all veto inheritance.
   * Never combine lease-filtered get/lastKnown snapshots: a renewal between them
   * can make both omit the same live root. Raw presence is lease-independent.
   * Only a root-owned clean-close receipt, with no conflicting presence, proves death.
   */
  lineageAlive(rootId: string, _now = Date.now()): boolean {
    // KV absence/TTL is never native lineage-death proof. The canary deliberately
    // vetoes cross-root actor inheritance: F-ACTOR-INHERIT (smarty-dev, issue number pending).
    // A KV-aware raw-presence/native-close fence is follow-on work, not TTL takeover.
    if (this.#isNats()) return true;
    if (!this.options.enabled) return true;
    const target = rootId === "main" ? this.options.rootId : rootId;
    const key = keyFor(PARTICIPANT_PREFIX, target);
    try {
      // true unless ENOENT: suppressed read/stat errors and invalid files veto inheritance.
      if (participantFilePresent(this.mesh.root, key)) return true;
      if (this.mesh.get(key, { fresh: true }) !== undefined) return true;
      // Retained legacy sessions also count regardless of lease or parse validity.
      if (target.startsWith("session:") && this.mesh.get(`${LEGACY_SESSION_PREFIX}${target.slice(8)}`, { fresh: true }) !== undefined) return true;
      const entry = this.mesh.get(keyFor(LINEAGE_CLOSURE_PREFIX, target), { fresh: true });
      const receipt = entry?.value;
      if (!(isObject(receipt) && receipt.format === 1 && receipt.rootId === target &&
        receipt.ownerHostId === target && receipt.ownerIdentityId === target &&
        entry?.updatedBy.id === target && entry.updatedBy.kind === "main" &&
        typeof receipt.closedAt === "number" && Number.isFinite(receipt.closedAt))) return true;
      // Recheck after reading the proof: a file-only publisher may have appeared
      // since the first absence read. Cross-root commits additionally hold the
      // mesh custody lock, which serializes this decision with resumeLineage().
      if (participantFilePresent(this.mesh.root, key) || this.mesh.get(key, { fresh: true }) !== undefined) return true;
      if (target.startsWith("session:") && this.mesh.get(`${LEGACY_SESSION_PREFIX}${target.slice(8)}`, { fresh: true }) !== undefined) return true;
      return this.mesh.get(entry.key, { fresh: true })?.version !== entry.version;
    } catch {
      return true; // Unknown is not positive proof, even if a close receipt exists.
    }
  }

  // A stalled mesh writer (for example a signal-stopped lock holder, smarty-dev#266)
  // stops every host lease from renewing, so peers soon look departed. This reports it;
  // the directory's own reads (get, list, sessions, peers) never throw, because timers,
  // the dashboard and local ownership checks consume them. User-facing listings and
  // "unknown participant" answers turn it into an error instead of an empty answer.
  // Two signals: this host's heartbeat failed on a mesh-lock timeout; or, before any
  // timeout, a peer lease lapsed after this host's last committed heartbeat while that
  // commit is two intervals overdue (or the heartbeat failed). The same lock may be what
  // stopped the peer renewing, so the lapse is not a departure yet. Elapsed wait alone is
  // not a signal: a complete listing of fresh leases stays usable under long contention.
  // ponytail: before the commit is overdue a plain listing can still omit such a peer for
  // up to two intervals; peer-settle, the decision that acts on absence, waits for a later
  // commit instead (confirmedAt). Revisit if another consumer acts on absence.
  writeStalled(now = Date.now()): Error | undefined {
    if (this.#isNats()) {
      return this.#transportLost || !this.#transportLease?.live() ? new Error("Fabric transport host lease is not live") : undefined;
    }
    if (!this.options.enabled || this.#closed) return undefined;
    const error = this.#refreshError;
    if (isMeshLockTimeout(error)) {
      return new Error(
        `Fabric mesh is write-stalled: ${error.message}. Participant leases have not renewed for ` +
          `${Math.round((now - this.#refreshedAt) / 1000)} s, so peer visibility is unknown, not empty.`,
      );
    }
    const confirmed = this.#refreshedAt;
    const unconfirmed = error !== undefined || now - confirmed > 2 * this.#heartbeatMs;
    if (!unconfirmed) return undefined;
    const lapsed = this.#lapsedSince(confirmed, now);
    if (lapsed === 0) return undefined;
    return new Error(
      `Fabric mesh is write-stalled: ${lapsed} peer lease${lapsed === 1 ? "" : "s"} lapsed while this host's ` +
        `heartbeat has not committed for ${((now - confirmed) / 1000).toFixed(1)} s, so peer visibility is unknown, not empty.`,
    );
  }

  /** A cached negative is evidence of absence only while the directory is confirmed fresh. */
  routingUnavailable(now = Date.now()): string | undefined {
    if (this.#isNats()) {
      if (this.#closed) return "participant directory is closed";
      if (this.#transportLost || !this.#transportLease?.live()) return "transport host lease is not live";
      if (!this.#participantView?.ready) return "participant directory snapshots are not ready";
      return undefined;
    }
    if (!this.options.enabled) return undefined;
    if (this.#closed) return "participant directory is closed";
    if (this.#routingError !== undefined) return this.#routingError instanceof Error
      ? this.#routingError.message : String(this.#routingError);
    const confirmed = Math.max(this.#routingReadAt,
      this.#leaseConfirmed && this.#refreshError === undefined ? this.#refreshedAt : 0);
    if (confirmed > 0 && now - confirmed < this.#heartbeatMs * 2) return undefined;
    if (this.#refreshError !== undefined) {
      return this.#refreshError instanceof Error ? this.#refreshError.message : String(this.#refreshError);
    }
    return confirmed === 0 ? "participant directory has no confirmed view"
      : `participant directory view is overdue (${now - confirmed} ms old)`;
  }

  /** Revalidate the canonical routing view without acquiring a state write lock.
   * Never promote this read to confirmedAt/canConsumeMesh: it renews no ownership lease. */
  async refreshRoutingView(): Promise<void> {
    if (this.#isNats()) {
      if (this.#closed) throw new Error("participant directory is closed");
      await this.#waitNatsReady();
      return;
    }
    if (this.#closed) throw new Error("participant directory is closed");
    if (!this.options.enabled) return;
    this.#routingReadAt = 0;
    try {
      // Dashboard reads tolerate damage; routing must not certify it as absence (#2386).
      assertMeshStateReadable(this.mesh.root);
      this.list({ scope: "project", includeStale: true, fresh: true });
      this.#routingReadAt = Date.now();
      this.#routingError = undefined;
    } catch (error) {
      this.#routingError = error;
      throw error;
    }
  }

  /** Resident consumers fail closed on failed/overdue renewal, not just peer visibility.
   * A file-only liveness write is not confirmation that the mesh is writable. */
  #meshAdmitted(now: number): boolean {
    if (this.#isNats()) return !this.#closed && !this.#suspended && !this.#quiescing && !this.#transportRecovering && !this.#transportLost &&
      !!this.#transportLease?.live() && this.#remainingTransportLeaseMs() > 0 && !!this.#participantView?.ready;
    return !this.options.enabled || (!this.#closed && !this.#quiescing && this.#leaseConfirmed &&
      this.#refreshError === undefined && now - this.#refreshedAt < this.#heartbeatMs * 2);
  }

  canConsumeMesh(now = Date.now()): boolean {
    const confirmed = this.#meshAdmitted(now);
    this.controlAdmission(now);
    if (this.#isNats()) return confirmed;
    // A resumed/suspended process (or a forward wall-clock adjustment) must not
    // wait a full heartbeat interval before trying to confirm its lapsed lease.
    // This never grants admission: refresh still needs the real mesh lock.
    // A deferred write already has a single host recovery lane. Idle consumers
    // call this gate as often as every 100 ms; they must not multiply attempts.
    if (!confirmed && !(isMeshWriteDeferred(this.#refreshError) || isMeshLockTimeout(this.#refreshError)) && !this.#closed && !this.#quiescing && this.#timer) {
      this.#runBackground(() => this.refresh());
    }
    return confirmed;
  }

  /** Inspect the actual directory gate plus explicitly supplied host gates. Routing-only
   * reads and NATS refreshError do not acquire admission authority here. */
  controlAdmission(now = Date.now()): FabricControlAdmission {
    const failure = this.#controlAdmissionFailure(now);
    this.#observeControlAdmission(failure, now);
    this.#observeSourceFailures(now);
    this.#scheduleDiagnosticThreshold(now);
    return failure ? { controlAdmission: "gated", controlAdmissionReason: failure.reason }
      : { controlAdmission: "admitted" };
  }

  #controlAdmissionFailure(now: number): { reason: string; alarmReason: string } | undefined {
    if (this.#meshAdmitted(now)) {
      try {
        const reason = this.options.controlAdmissionReason?.()?.trim();
        return reason ? { reason, alarmReason: "additional host control admission gate" } : undefined;
      } catch (error) {
        return { reason: `host control admission inspection failed: ${error instanceof Error ? error.message : String(error)}`,
          alarmReason: "host control admission inspection failed" };
      }
    }
    if (this.#closed) return { reason: "participant directory is closed", alarmReason: "participant directory is closed" };
    if (this.#quiescing) return { reason: "participant directory is quiescing", alarmReason: "participant directory is quiescing" };
    if (this.#isNats()) {
      const reason = this.#suspended ? "participant directory lifecycle is suspended"
        : this.#transportLost ? "transport host lease was lost"
        : this.#transportRecovering ? "transport host lease is recovering"
        : !this.#transportLease?.live() ? "transport host lease is not live"
        : "participant directory snapshots are not ready";
      return { reason, alarmReason: reason };
    }
    if (this.#refreshError !== undefined) return {
      reason: `participant heartbeat refresh failed: ${this.#refreshError instanceof Error ? this.#refreshError.message : String(this.#refreshError)}`,
      alarmReason: "participant heartbeat refresh failed",
    };
    const reason = !this.#leaseConfirmed ? "participant directory has no confirmed heartbeat publication"
      : `participant heartbeat confirmation is overdue (${now - this.#refreshedAt} ms old)`;
    return { reason, alarmReason: !this.#leaseConfirmed ? "participant heartbeat publication is unconfirmed"
      : "participant heartbeat confirmation is overdue" };
  }

  #cancelControlAdmissionTimer(): void {
    if (this.#controlAdmissionTimer) clearTimeout(this.#controlAdmissionTimer);
    this.#controlAdmissionTimer = undefined;
    this.#controlAdmissionDueAt = undefined;
  }

  #controlAlarmOwnerLive(): boolean {
    return this.options.enabled && !this.#closed && !this.#suspended && !this.#quiescing && !this.#superseded && this.#live();
  }

  #observeControlAdmission(failure: { reason: string; alarmReason: string } | undefined, now: number): void {
    if (!this.#controlAlarmOwnerLive()) return; // Retirement is not recovery.
    if (!failure) {
      this.#controlAdmissionIncident = undefined;
      return;
    }
    const incident = this.#controlAdmissionIncident ??= {
      since: now, dedupeKey: `control-admission:${this.options.rootId}:${this.ownerIncarnation}:${randomUUID()}`, alarmed: false,
    };
    if (incident.alarmed) return;
    if (now - incident.since > 60_000) {
      incident.alarmed = true;
      diagnostics.warn("src/topology/participant-directory.ts", `[pi-fabric] control admission gated for more than 60s on ${this.options.hostId}: ${failure.reason}`);
      void this.#notifications.enqueue(async () => {
        const active = (): boolean => this.#controlAlarmOwnerLive() && this.#controlAdmissionIncident === incident &&
          this.controlAdmission().controlAdmission === "gated";
        if (!active()) return;
        await controlAdmissionAlarm(this.mesh, this.options.identity, this.options.rootId, this.options.hostId,
          { since: incident.since, dedupeKey: incident.dedupeKey, reason: failure.alarmReason });
      });
    }
  }

  #observeSourceFailures(now: number): void {
    if (!this.#controlAlarmOwnerLive()) return;
    for (const [source, incident] of this.#sourceFailures) {
      if (incident.alarmed || now - incident.since <= 60_000) continue;
      incident.alarmed = true;
      diagnostics.warn("src/topology/participant-directory.ts", `[pi-fabric] participant source ${incident.source} on ${this.options.hostId} failed for more than 60s: ${incident.reason}`);
      void this.#notifications.enqueue(async () => {
        if (!this.#controlAlarmOwnerLive() || this.#sourceFailures.get(source) !== incident) return;
        await isolatedParticipantSourceAlarm(this.mesh, this.options.identity, this.options.rootId, this.options.hostId, incident);
      });
    }
  }

  #scheduleDiagnosticThreshold(now: number): void {
    let due = Infinity;
    if (this.#controlAlarmOwnerLive()) {
      for (const incident of [this.#controlAdmissionIncident, ...this.#sourceFailures.values()]) {
        if (incident && !incident.alarmed) due = Math.min(due, incident.since + 60_001);
      }
    }
    if (this.#controlAdmissionTimer && this.#controlAdmissionDueAt === due) return;
    this.#cancelControlAdmissionTimer();
    if (!Number.isFinite(due)) return;
    this.#controlAdmissionDueAt = due;
    // Share the existing owned one-shot threshold deadline; no source polling timer.
    this.#controlAdmissionTimer = setTimeout(() => {
      this.#controlAdmissionTimer = undefined;
      this.#controlAdmissionDueAt = undefined;
      this.controlAdmission();
    }, Math.max(1, due - now));
    this.#controlAdmissionTimer.unref?.();
  }

  /** When this host last committed its heartbeat: lapses before it happened on a working mesh. */
  confirmedAt(): number {
    if (this.#isNats()) return this.#natsConfirmedAt;
    return this.#refreshedAt;
  }

  // Peer leases (host leases, and legacy session entries) that lapsed in (since, now].
  #lapsedSince(since: number, now: number): number {
    let lapsed = 0;
    for (const host of this.#liveHosts(this.mesh.listAll(HOST_PREFIX)).values()) {
      // A mirrored lease lapses when its bridge stops, which says nothing about this mesh's lock.
      if (host.id !== this.options.hostId && host.remoteHost === undefined && host.expiresAt > since && host.expiresAt <= now) lapsed += 1;
    }
    for (const entry of this.mesh.listAll(LEGACY_SESSION_PREFIX)) {
      const expiresAt = sessionLiveness(entry, this.mesh.root).expiresAt;
      const id = isObject(entry.value) ? entry.value.id : undefined;
      if (id !== this.options.rootId && expiresAt > since && expiresAt <= now) lapsed += 1;
    }
    return lapsed;
  }

  #natsSelf(now: number): FabricParticipantInfo {
    const existing = this.#localRecords.get(this.options.identity.id);
    if (existing) return { ...existing, local: existing.ownerHostId === this.options.hostId, stale: false };
    const published = this.#participantView?.ready ? this.#participantView.get(this.options.identity.id) : undefined;
    if (published) return published;
    const kind: FabricParticipantKind = this.options.identity.kind === "main" ? "root" : this.options.identity.kind;
    return {
      format: 1, id: this.options.identity.id, kind, rootId: this.options.rootId,
      ownerHostId: this.options.selfOwnerHostId ?? this.options.hostId,
      ownerIdentityId: this.options.selfOwnerIdentityId ?? this.options.identity.id,
      ...(!this.options.selfOwnerHostId ? { ownerIncarnation: this.ownerIncarnation } : {}),
      ...(kind === "root" ? {} : { parentId: this.options.rootId }),
      name: this.options.identity.name, status: "running", runner: "pi", transport: "host",
      capabilities: ["steer", "followUp", "fabric"],
      ...(this.options.identity.sessionId ? { sessionId: this.options.identity.sessionId } : {}),
      startedAt: this.#startedAt, updatedAt: now, controlProtocol: "v1",
      local: (this.options.selfOwnerHostId ?? this.options.hostId) === this.options.hostId, stale: false,
    };
  }

  self(now = Date.now(), read: MeshReadOptions = {}): FabricParticipantInfo {
    if (this.#isNats()) return this.#natsSelf(now);
    const existing =
      this.#localRecords.get(this.options.identity.id) ??
      this.#parsed(read).participants
        .find((participant) => participant.id === this.options.identity.id && participant.remoteHost === undefined);
    if (existing) {
      return {
        ...existing,
        local: existing.ownerHostId === this.options.hostId,
        stale: false,
      };
    }
    const kind: FabricParticipantKind =
      this.options.identity.kind === "main" ? "root" : this.options.identity.kind;
    return {
      format: 1,
      id: this.options.identity.id,
      kind,
      rootId: this.options.rootId,
      ownerHostId: this.options.selfOwnerHostId ?? this.options.hostId,
      ownerIdentityId: this.options.selfOwnerIdentityId ?? this.options.identity.id,
      // A worker's self address belongs to its parent owner, not this runtime.
      ...(!this.options.selfOwnerHostId ? { ownerIncarnation: this.ownerIncarnation } : {}),
      ...(kind === "root" ? {} : { parentId: this.options.rootId }),
      name: this.options.identity.name,
      status: "running",
      runner: "pi",
      transport: "host",
      capabilities: ["steer", "followUp", "fabric"],
      ...(this.options.identity.sessionId ? { sessionId: this.options.identity.sessionId } : {}),
      startedAt: this.#startedAt,
      updatedAt: now,
      controlProtocol: "v1",
      local: (this.options.selfOwnerHostId ?? this.options.hostId) === this.options.hostId,
      stale: false,
    };
  }

  sessions(now = Date.now()): FabricParticipantInfo[] {
    return this.list({ scope: "project", kinds: ["root"] }, now);
  }

  peers(now = Date.now(), options: FabricPeerListOptions = {}): FabricPeerInfo[] {
    const display = options.background === true && options.fresh !== true
      ? options.displayParticipants : undefined;
    return (display ?? this.list({ ...options, scope: "project", kinds: ["root"] }, now))
      .filter((participant) => participant.id !== this.options.rootId)
      .flatMap((participant) => {
        const peer = peerFromParticipant(participant);
        return peer ? [peer] : [];
      });
  }

  root(
    main: FabricMainAgentInfo,
    interactive = true,
    sessionName?: string,
    boundGrant?: { role: string | undefined },
    herdrPane?: string,
    services?: readonly { name: string; version: number }[],
  ): FabricParticipantRecord {
    // Runtime supplies its session_start-bound snapshot. Standalone directories bind once too.
    const role = boundGrant ? boundGrant.role : this.#roleGrant.roleFor(main.sessionId ?? "", main.cwd ?? "");
    const project = main.cwd ? participantProject(main.cwd) : undefined;
    const repository = project ? repositoryOf(project) : undefined;
    return {
      format: 1,
      id: main.id,
      kind: "root",
      rootId: main.id,
      ownerHostId: this.options.hostId,
      ownerIdentityId: this.options.identity.id,
      ownerIncarnation: this.ownerIncarnation,
      name: rootParticipantName(sessionName),
      status: main.status === "running" ? "running" : "idle",
      runner: "pi",
      transport: "host",
      // Format-1 readers reject the whole record on an unknown capability. Keep that
      // vocabulary stable; older readers ignore this optional feature advertisement.
      capabilities: interactive ? ["steer", "followUp", "fabric"] : ["fabric"],
      mainBindings: false, // Cross-process Main setters await a native Pi commit guard.
      interactive,
      ...(main.cwd ? { cwd: main.cwd, projectRoot: process.env.PI_FABRIC_PROJECT_ROOT ?? main.cwd } : {}),
      ...(project ? { project } : {}),
      ...(repository ? { repository } : {}),
      ...(role ? { role } : {}),
      ...(herdrPane ? { herdrPane } : {}),
      ...(services?.length ? { services: services.map(({ name, version }) => ({ name, version })) } : {}),
      ...(main.sessionId ? { sessionId: main.sessionId } : {}),
      ...(main.model ? { model: main.model } : {}),
      ...(main.thinking ? { thinking: main.thinking } : {}),
      startedAt: main.startedAt ?? this.#startedAt,
      updatedAt: main.updatedAt,
      pendingMessages: main.pendingMessages,
      controlProtocol: "v1",
      mainProcess: ownMainProcess(),
    };
  }

  // After a committed heartbeat only: a host that cannot write gains nothing from a sweep.
  #sweepDeadHosts(): void {
    const reap = this.options.reapDeadHosts;
    if (!this.options.enabled || reap === false || this.#closed || this.#quiescing) return;
    // Maintenance cadence is elapsed time, not lease authority. A forward wall
    // clock adjustment must give other live hosts their ordinary renewal turn,
    // not immediately retire their UUIDs before they can observe the new clock.
    const elapsed = performance.now();
    if (elapsed - this.#deadHostSweepAt < (reap?.sweepMs ?? DEAD_HOST_SWEEP_MS)) return;
    this.#deadHostSweepAt = elapsed;
    const now = Date.now();
    void this.#notifications.enqueue(() => {
      // Per-record DIRECTORY_RETENTION_MS uses private receipt/raw owner-death
      // proof lazily at selection and deletion. No public candidate vetoes a sweep.
      return reapDeadHostRecords(this.mesh, this.options.identity, {
        terminalActor: entry => this.options.terminalActor?.(entry),
        withCommitFence: operation => this.#withLeaseFence(operation),
        ownHostId: this.options.hostId,
        now,
        ...(reap?.deadAfterMs !== undefined ? { deadAfterMs: reap.deadAfterMs } : {}),
      });
    });
  }

  async #quiesceNats(reason?: string): Promise<void> {
    if (this.#closed || this.#quiescing) return;
    if (!this.#transportLease?.live()) throw new TransportUnavailableError("NATS host lease is not live");
    this.#quiescing = true;
    this.#cancelControlAdmissionTimer();
    this.#cancelReconnectRefresh();
    if (reason === "reload" && this.options.identity.kind === "main" && this.options.hostId === this.options.rootId) {
      this.#reloadUntil = Date.now() + MAIN_RELOAD_LEASE_MS;
    }
    await Promise.allSettled([...this.#natsPending.values()]);
    await this.#refreshNats(true);
  }

  #closeNats(): Promise<void> {
    if (!this.#natsClosing) this.#natsClosing = this.#finishCloseNats();
    return this.#natsClosing;
  }

  async #finishCloseNats(): Promise<void> {
    // Admission closes before the view withdraws and before the lease writes its clean marker.
    this.#quiescing = true;
    this.#closed = true;
    this.#cancelControlAdmissionTimer();
    this.#leaseAbort.abort();
    this.#cancelReconnectRefresh();
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#heartbeatDueAt = undefined;
    if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
    this.#refreshTimer = undefined;
    this.#refreshScheduled = false;
    await this.#refreshing?.catch(() => undefined);
    await Promise.allSettled([...this.#natsPending.values()]);
    await this.#transportRenewing?.catch(() => undefined);
    this.#transportReconnectUnsubscribe?.();
    this.#transportReconnectUnsubscribe = undefined;
    await this.#notifications.close();
    if (this.#participantView) await this.#participantView.close().catch(error => { this.#refreshError = error; });
    if (this.#transportLease) await this.#transportLease.release(AbortSignal.timeout(5_000)).catch(error => { this.#refreshError = error; });
    if (this.#localProjection) await this.#localProjection.close().catch(error => { this.#refreshError = error; });
    this.#natsPrelaunch.clear();
    this.#natsPending.clear();
    this.#natsPublished.clear();
    this.#natsPublishedAt.clear();
  }

  async quiesce(reason?: string): Promise<void> {
    if (this.#isNats()) {
      await this.#quiesceNats(reason);
      return;
    }
    if (this.#closed || this.#quiescing) return;
    this.#assertLeaseActive();
    this.#quiescing = true;
    this.#cancelControlAdmissionTimer();
    this.#cancelPublicationRetry();
    await this.#publicationRetrying;
    if (reason === "reload" && this.options.identity.kind === "main" && this.options.hostId === this.options.rootId) {
      this.#reloadUntil = Date.now() + MAIN_RELOAD_LEASE_MS;
    }
    try {
      await this.#refreshing?.catch(() => undefined);
      await this.refresh();
    } catch (error) {
      // A reload's shared write can time out under lock load (smarty-dev#6729). The reload
      // goes on regardless (shutdown swallows this), so keep the Main addressable: renew its
      // own lease file, which needs no mesh lock, and let close() keep the root and lease.
      if (this.#reloadUntil !== undefined && this.options.enabled) {
        try { await this.#renewFileLease(); } catch (renewError) {
          if (renewError instanceof FabricHostLeaseSupersededError) throw renewError;
        }
        this.#reloadPublished = true;
      }
      throw error;
    }
    this.#reloadPublished = this.#reloadUntil !== undefined;
  }

  async #resumeNatsLineage(): Promise<void> {
    if (!this.options.enabled || this.options.hostId !== this.options.rootId ||
      this.options.identity.id !== this.options.rootId || this.options.identity.kind !== "main") return;
    const closure = this.mesh.get(keyFor(LINEAGE_CLOSURE_PREFIX, this.options.rootId), { fresh: true });
    if (closure) await this.#withLeaseFence(() => this.mesh.delete({ key: closure.key, ifVersion: closure.version }));
  }

  /** Invalidate death proof under the mesh custody lock BEFORE activation or file publication. */
  async resumeLineage(): Promise<void> {
    if (this.#isNats()) return this.#resumeNatsLineage();
    if (!this.options.enabled || this.options.hostId !== this.options.rootId ||
      this.options.identity.id !== this.options.rootId || this.options.identity.kind !== "main") return;
    const closure = this.mesh.get(keyFor(LINEAGE_CLOSURE_PREFIX, this.options.rootId), { fresh: true });
    // Runtime construction invokes resumption before start(). Claim this new host before
    // invalidating the proof; the deletion is an owned mutation and must keep its fence.
    if (closure && !this.#leaseClaimed) await this.#renewFileLease();
    if (closure) await this.#withLeaseFence(() => this.mesh.delete({ key: closure.key, ifVersion: closure.version }));
  }

  async #closeNatsLineage(): Promise<void> {
    if (this.#closed) return;
    const lease = this.#transportLease;
    try {
      // A lost/suspended NATS lease cannot fence a closure receipt. Shutdown is still
      // idempotent: let the lease TTL expire rather than failing the runtime close path.
      if (lease?.live()) {
        try { await this.#quiesceNats(); }
        catch (error) {
          const code = typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
          const unavailable = code === "FABRIC_TRANSPORT_UNAVAILABLE" || code === "FABRIC_TRANSPORT_UNCERTAIN" ||
            code === "FABRIC_DIRECTORY_NOT_READY";
          if (lease.live() && !lease.recovering() && !this.#transportLost && !unavailable) throw error;
        }
      }
      if (lease?.live() && !lease.recovering() && !this.#transportLost &&
        this.options.hostId === this.options.rootId && this.options.identity.id === this.options.rootId &&
        this.options.identity.kind === "main" && this.#localRecords.get(this.options.rootId)?.kind === "root") {
        try {
          await this.#withLeaseFence(() => this.mesh.put({
            key: keyFor(LINEAGE_CLOSURE_PREFIX, this.options.rootId), identity: this.options.identity,
            value: { format: 1, rootId: this.options.rootId, ownerHostId: this.options.hostId,
              ownerIdentityId: this.options.identity.id, closedAt: Date.now() },
          }));
        } catch (error) {
          if (lease.live() && !lease.recovering() && !this.#transportLost) throw error;
        }
      }
    } finally { await this.#closeNats(); }
  }

  /** Explicit terminal session operation. Disposing/replacing a runtime is NOT lineage closure. */
  async closeLineage(): Promise<void> {
    if (this.#isNats()) return this.#closeNatsLineage();
    if (this.#closed) return;
    if (!this.options.enabled || this.#reloadPublished || this.#superseded) { await this.close(); return; }
    await this.quiesce().catch(() => undefined); // Drain publication before certifying terminal closure.
    if (this.#superseded) { await this.close(); return; }
    // Only the creating Main may certify its terminal close, never a resident/child host.
    if (this.options.hostId === this.options.rootId && this.options.identity.id === this.options.rootId &&
      this.options.identity.kind === "main" && this.#localRecords.get(this.options.rootId)?.kind === "root") {
      await this.#withLeaseFence(() => this.mesh.put({
        key: keyFor(LINEAGE_CLOSURE_PREFIX, this.options.rootId), identity: this.options.identity,
        value: { format: 1, rootId: this.options.rootId, ownerHostId: this.options.hostId,
          ownerIdentityId: this.options.identity.id, closedAt: Date.now() },
      })).catch(() => undefined);
    }
    await this.close();
  }

  async close(): Promise<void> {
    if (this.#isNats()) {
      await this.#closeNats();
      return;
    }
    if (this.#closed) return;
    this.#closed = true;
    this.#cancelControlAdmissionTimer();
    this.#leaseAbort.abort();
    this.#withdrawalPending = true;
    this.#cancelPublicationRetry();
    await this.#publicationRetrying;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#heartbeatDueAt = undefined;
    this.#suspended = false;
    if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
    this.#refreshTimer = undefined;
    this.#refreshScheduled = false;
    await this.#refreshing?.catch(() => undefined);
    await this.#leaseRenewing?.catch(() => undefined);
    await this.#notifications.close();
    if (!this.options.enabled || this.#superseded || !this.#leaseClaimed) return;
    const leaseOwner = this.#leaseOwner();
    const ownsLease = (): boolean => {
      const current = readHostLeaseCurrent(this.mesh.root, this.options.hostId);
      return current !== undefined && sameHostLeaseOwner(current, leaseOwner);
    };
    if (!ownsLease()) {
      try { assertHostLeaseOwner(this.mesh.root, leaseOwner); }
      catch (error) { if (error instanceof FabricHostLeaseSupersededError) this.#markSuperseded(error); }
      return;
    }
    const own = (entry: MeshStateEntry): boolean => {
      if (!ownsLease()) return false;
      const participant = participantFromEntry(entry);
      return participant !== undefined && isLocal(participant, this.options.hostId) &&
        !(this.#reloadPublished && participant.kind === "root" && participant.id === this.options.rootId);
    };
    for (const entry of readParticipantFiles(this.mesh.root, { maxAgeMs: 0 }).filter(own)) {
      await removeParticipantFileIf(this.mesh, entry.key, own, this.#fileLockOptions()).catch(() => undefined);
    }
    // ONE batch (one lock acquisition) for every shared record, reread fresh and retried
    // while the lock is busy: under mesh-lock load, per-entry deletes each timed out and
    // were swallowed, which left this host's entries fresh after a clean exit (#6622).
    // A full close folds its host entry into that batch: a second batch waited its own lock
    // timeouts and pushed a resident's idle exit past its shutdown window (round 3).
    // Every attempt commits under the lease fence while the current UUID still owns the
    // file lease, so a superseded incarnation never withdraws its successor's records.
    const legacySessionKey = this.#legacySessionKey();
    const withdrawHost = !this.#reloadPublished;
    const records = await this.#withdraw(() => {
      const ops: MeshBatchOperation[] = this.mesh.listAll(PARTICIPANT_PREFIX, { fresh: true }).filter(own)
        .map((entry) => ({ kind: "delete" as const, key: entry.key, ifVersion: entry.version, onConflict: "skip" as const }));
      const legacy = legacySessionKey ? this.mesh.get(legacySessionKey, { fresh: true }) : undefined;
      if (legacy?.updatedBy.id === this.options.identity.id && ownsLease()) {
        ops.push({ kind: "delete", key: legacy.key, ifVersion: legacy.version, onConflict: "skip" });
      }
      const hostEntry = withdrawHost ? this.mesh.get(keyFor(HOST_PREFIX, this.options.hostId), { fresh: true }) : undefined;
      const host = hostEntry ? hostFromEntry(hostEntry) : undefined;
      if (hostEntry && host && host.remoteHost === undefined && host.incarnationToken === this.#incarnationToken &&
        host.identity.id === this.options.identity.id) {
        ops.push({ kind: "delete", key: hostEntry.key, ifVersion: hostEntry.version, onConflict: "skip" });
      }
      return ops;
    }, (ops) => this.#withLeaseFence(() => this.mesh.writeBatch({ identity: this.options.identity, ops,
      lockTiming: { operation: "directory-withdraw" },
      beforeCommit: () => { assertHostLeaseOwner(this.mesh.root, leaseOwner); } })));
    // A superseded incarnation owns nothing left to lapse: its successor holds the lease.
    const failed = (reason: string): void => { if (!this.#superseded) this.#withdrawalFailure(reason); };
    // A reload leaves only its root and fixed host lease; the next session_start replaces both.
    // A reload whose withdrawal did not commit keeps the exit hook: its lease is no longer
    // renewed, and an exit drops it, so the leftover entries lapse instead of staying fresh.
    if (this.#reloadPublished) {
      if (!records.ok) { failed(records.reason); return; }
      this.#withdrawn = true;
      this.#withdrawalPending = false;
      this.#removeExitHook();
      return;
    }
    if (!this.#superseded) await removeOwnedHostLease(this.mesh, leaseOwner).catch(() => false);
    // Only a committed withdrawal retires the exit hook.
    if (!records.ok) { failed(records.reason); return; }
    this.#withdrawalPending = false;
    // An owned lease removal that did not take effect keeps the hook: an exit still drops it.
    if (!this.#superseded && ownsLease()) return;
    this.#withdrawn = true;
    this.#removeExitHook();
  }

  #natsRecord(candidate: FabricParticipantRecord): FabricParticipantRecord {
    const {
      task: _task,
      text: _text,
      error: _error,
      ownerIncarnation: _incarnation,
      placementRunId: _placementRunId,
      ...operational
    } = candidate as FabricParticipantRecord & { task?: unknown; text?: unknown; error?: unknown };
    const placementRunId = candidate.kind === "root" && candidate.id === this.options.rootId && candidate.rootId === this.options.rootId
      ? placementRunIdForParticipant(candidate.kind)
      : undefined;
    return {
      ...operational,
      format: 1,
      ownerHostId: this.options.hostId,
      ownerIdentityId: this.options.identity.id,
      ownerIncarnation: this.ownerIncarnation,
      ...(this.#quiescing
        ? this.#reloadUntil !== undefined && candidate.kind === "root" && candidate.id === this.options.rootId
          ? { status: "reloading", reloadUntil: this.#reloadUntil }
          : { capabilities: [], status: "stopping" }
        : {}),
      ...(placementRunId ? { placementRunId } : {}),
      controlProtocol: "v1",
      livenessLeaseFiles: 1,
    };
  }

  #natsChanged(before: FabricParticipantRecord, after: FabricParticipantRecord): boolean {
    if (JSON.stringify({ ...before, ...QUIET_FIELDS }) !== JSON.stringify({ ...after, ...QUIET_FIELDS })) return true;
    return Date.now() - (this.#natsPublishedAt.get(before.id) ?? 0) >= ACTIVITY_REFRESH_MS &&
      JSON.stringify({ ...before, updatedAt: undefined }) !== JSON.stringify({ ...after, updatedAt: undefined });
  }

  async #refreshNats(full: boolean, reconnect?: ReconnectRefresh): Promise<void> {
    const current = (): boolean => !reconnect || this.#reconnectRefresh === reconnect &&
      !this.#suspended && !this.#quiescing && this.#live();
    if (this.#closed || !current()) return;
    const lease = this.#transportLease, view = this.#natsView();
    if (!lease?.live() || this.#transportLost || this.#transportRecovering) throw new TransportUnavailableError("NATS host lease is not live");
    if (this.#refreshing) {
      await this.#refreshing;
      return this.#refreshNats(full, reconnect);
    }
    if (!full) this.#changeRefreshAt = Date.now();
    // Recovering includes a re-created lease whose root re-check has not passed (smarty-dev#15310).
    const assertLive = (): void => {
      if (!current()) throw new ParticipantPublicationChangedError();
      if (this.#closed || this.#transportLost || this.#transportRecovering || !lease.live()) {
        throw new TransportUnavailableError("NATS host lease is not live");
      }
    };
    const select = async (): Promise<void> => {
      assertLive();
      const valid = this.options.preparePublicationFence?.();
      const desired = new Map<string, FabricParticipantRecord>();
      const terminal = new Set<string>();
      for (const snapshot of this.#sources) {
        for (const candidate of this.#snapshot(snapshot)) {
          if (this.#natsPending.has(candidate.id)) continue;
          // Settlement is taken BEFORE quiesce replaces operational status.
          if (NATS_TERMINAL_STATUSES.has(candidate.status) || candidate.stopReason !== undefined) {
            terminal.add(candidate.id);
            continue;
          }
          // Task ids retire permanently; actors retain their native same-id reactivation policy.
          if (candidate.kind === "agent" && (this.#natsRetired.has(candidate.id) || this.#natsAbsent.has(candidate.id))) continue;
          const record = this.#natsRecord(candidate);
          if (record.kind === "actor" && this.options.actorRenewalAllowed && !this.options.actorRenewalAllowed(record)) continue;
          desired.set(record.id, record);
        }
      }
      for (const id of this.#natsAbsent) {
        if (!desired.has(id)) terminal.add(id);
        else this.#natsAbsent.delete(id); // An authorized actor may resume the same id.
      }
      const uncertain = new Set([...this.#sourceOmittedIds.values()].flatMap(ids => [...ids]));
      const observed = new Set(desired.keys());
      for (const [id, record] of this.#natsPrelaunch) {
        if (!desired.has(id) && !terminal.has(id)) desired.set(id, record);
      }
      const commit = async (): Promise<void> => {
        assertLive();
        if (valid && !valid()) throw new ParticipantPublicationChangedError();
        const reconnectGeneration = this.#verifiedReconnectGeneration;
        const forcePresence = !this.#suspended && (!this.#reconnectRefresh || this.#reconnectRefresh === reconnect) &&
          reconnectGeneration > this.#confirmedReconnectGeneration;
        const root = desired.get(this.options.rootId);
        this.#assertRootPublishable(root);
        if (root?.kind === "root" && this.#localRecords.get(root.id)?.kind !== "root") await this.resumeLineage();
        for (const id of terminal) {
          if (this.#natsPending.has(id)) continue;
          assertLive();
          await view.remove(id);
          this.#natsPublished.delete(id);
          this.#natsPublishedAt.delete(id);
          this.#natsPrelaunch.delete(id);
          this.#natsRetired.add(id);
          this.#natsAbsent.delete(id);
        }
        for (const [id, record] of desired) {
          // Hooks reserve synchronously, even if this pass already selected its snapshot.
          if (this.#natsPending.has(id)) continue;
          assertLive();
          if (valid && !valid()) throw new ParticipantPublicationChangedError();
          this.#assertRootPublishable(record);
          if (record.kind === "actor" && this.options.actorRenewalAllowed && !this.options.actorRenewalAllowed(record)) continue;
          const previous = this.#natsPublished.get(id);
          if (forcePresence || !previous || this.#natsChanged(previous, record)) {
            await view.publish(record, forcePresence ? { force: true } : {});
            this.#natsPublished.set(id, record);
            this.#natsPublishedAt.set(id, Date.now());
          }
          // A manager snapshot now contains this successfully launched/queued run.
          if (observed.has(id)) this.#natsPrelaunch.delete(id);
        }
        for (const [id] of [...this.#natsPublished]) {
          if (desired.has(id) || this.#natsPrelaunch.has(id) || this.#natsPending.has(id)) continue;
          assertLive();
          await view.remove(id);
          this.#natsPublished.delete(id);
          this.#natsPublishedAt.delete(id);
          // Omission is not death proof when the source could not observe this id.
          if (!uncertain.has(id)) this.#natsRetired.add(id);
        }
        assertLive();
        this.#localRecords.clear();
        for (const [id, record] of this.#natsPublished) this.#localRecords.set(id, record);
        if (forcePresence) this.#confirmedReconnectGeneration = reconnectGeneration;
        this.#natsConfirmedAt = Date.now();
        this.#leaseConfirmed = true;
        this.#refreshError = undefined;
        this.#routingError = undefined;
      };
      // Retain native registry custody, including removals. No presenceBatch or FILE
      // lease gate participates; actors still validate their native lineage tokens.
      if (valid && this.options.withPublicationFence) await this.options.withPublicationFence(commit);
      else await commit();
    };
    // Deferring selection installs #refreshing before sources/callbacks can re-enter.
    const operation = Promise.resolve().then(async () => {
      await this.#ownIncarnation;
      if (this.options.beforePublication) await this.options.beforePublication();
      if (!this.options.preparePublicationFence && this.options.withPublicationFence) {
        await this.options.withPublicationFence(select);
      } else await select();
    });
    this.#refreshing = operation;
    try { await operation; }
    catch (error) {
      // Cancellation is not an error of the replacement or of another waiter that
      // joined this preparation. Let those waiters reselect under their own token.
      if (!current()) return;
      this.#refreshError = error;
      throw error;
    }
    finally {
      if (this.#refreshing === operation) this.#refreshing = undefined;
      if (this.#refreshAgain) {
        this.#refreshAgain = false;
        this.scheduleRefresh();
      }
      this.controlAdmission();
    }
  }

  // Return the actual shared commit/acquisition time, not the completion time
  // of fallible post-commit file copies. An unchanged change-only refresh proves nothing.
  async #refresh(full: boolean, initialLeaseAt?: number): Promise<number | false> {
    // Claim before capturing a publication generation, but reselect ALL sources
    // afterwards: neither a label's mesh write nor an awaited claim may leave
    // actor ownership observations selected against an older generation.
    let leaseRenewed = initialLeaseAt !== undefined;
    if (this.options.enabled && !this.#leaseClaimed && !leaseRenewed) {
      initialLeaseAt = await this.#renewFileLease();
      leaseRenewed = true;
    }
    if (this.options.enabled && !this.#claimedPeerLabel && !this.#localRecords.get(this.options.rootId)?.label) {
      for (const source of this.#sources) {
        const root = this.#snapshot(source).find(record => record.kind === "root" && record.id === this.options.rootId);
        if (root) { await this.#ensurePeerLabels(new Map([[root.id, { ...root }]])); break; }
      }
    }
    const validPublication = this.options.preparePublicationFence?.();
    const now = Date.now();
    const desired = new Map<string, FabricParticipantRecord>();
    for (const source of this.#sources) {
      for (const candidate of this.#snapshot(source)) {
        const {
          task: _task,
          text: _text,
          error: _error,
          ownerIncarnation: _incarnation,
          ...operational
        } = candidate as
          FabricParticipantRecord & { task?: unknown; text?: unknown; error?: unknown };
        const record: FabricParticipantRecord = {
          ...operational,
          format: 1,
          ownerHostId: this.options.hostId,
          ownerIdentityId: this.options.identity.id,
          ownerIncarnation: this.ownerIncarnation, // override untrusted/cached source epochs
          // A root that is shutting down takes no new steer, and says why (smarty-dev#1113).
          ...(this.#quiescing
            ? this.#reloadUntil !== undefined && candidate.kind === "root" && candidate.id === this.options.rootId
              ? { status: "reloading", reloadUntil: this.#reloadUntil }
              : { capabilities: [], status: "stopping" }
            : {}),
          controlProtocol: "v1",
          livenessLeaseFiles: 1,
        };
        // A cached predecessor (including a removed row or same-root token
        // rotation) is not a publication source, even before successor presence.
        if (record.kind === "actor" && this.options.actorRenewalAllowed && !this.options.actorRenewalAllowed(record)) continue;
        desired.set(record.id, record);
      }
    }
    const candidateRoot = desired.get(this.options.rootId);
    if (this.options.enabled && !this.#quiescing && candidateRoot?.kind === "root") {
      // smarty-dev#7978: only when a participant, lease or this root changed since the last report
      // (or a lease period passed): an unchanged directory can only lose collisions as peers lapse,
      // and every collision it still has was already reported (#reportedRootCollisions).
      const signals = this.#rootCollisionSignals(candidateRoot, now);
      if (signals) {
        this.#reportRootCollisions(candidateRoot);
        this.#rootCollisionPass = signals;
      }
    }
    // No resumed root is activated locally or published to a file while an old
    // death proof survives. Failure aborts this refresh before any root publication.
    // An already-active root cannot terminally close its lineage: only activation needs
    // this fresh authority read, not every idle heartbeat (smarty-dev#4383).
    if (desired.get(this.options.rootId)?.kind === "root" &&
      this.#localRecords.get(this.options.rootId)?.kind !== "root") await this.resumeLineage();
    // Mint before the local cache swap so self() exposes the label too.
    await this.#ensurePeerLabels(desired);
    if (!this.options.enabled) {
      this.#localRecords.clear();
      for (const [id, record] of desired) this.#localRecords.set(id, record);
      return Date.now();
    }

    const root = [...desired.values()].find(
      (participant) => participant.kind === "root" && participant.id === this.options.rootId,
    );
    // Every write of this heartbeat goes into ONE locked state write (smarty-dev#367):
    // each separate put rewrote the whole shared state file under the mesh lock.
    const publication = this.options.publicationBatch?.(full);
    const ops: MeshBatchOperation[] = [...(publication?.ops ?? [])];
    let changed = ops.length > 0;
    // Before the fleet owner's switch to files, the shared state stays the record every runtime
    // reads, and each committed record is also written to its file. After it, records are written
    // only to their files, and this host removes its records from the shared state, and its
    // pre-directory session entry too: every runtime then reads the records (smarty-dev#2004).
    // Pin one exact-on-change snapshot for preparation. This includes migration policy,
    // not pure observation, so it must NOT opt into the background cache floor.
    // CAS/ownership ports below and the post-lock skip check stay fresh.
    const snapshot = this.mesh.stateToken();
    const read = { snapshot };
    // A live pre-capability reader temporarily vetoes that migration.
    // An explicit participant-file policy must not hide our legacy advertisement from
    // an old runtime that joins later. Resume dual publication until that reader lapses.
    const legacyRenewalsRequired = this.#legacyRenewalsRequired(now, snapshot);
    const hostPolicyFilesOnly = fileLeasesOnly(this.mesh.get(LIVENESS_POLICY_KEY, read)?.value);
    const filesOnly = participantFilesOnly(this.mesh.get(LIVENESS_POLICY_KEY, read)?.value) &&
      !legacyRenewalsRequired;
    const legacySessionKey = this.#legacySessionKey();
    let legacyPut: MeshBatchOperation | undefined;
    let legacyChanged = false;
    // 6b15d905's sessions()/peers() prefer native participants and their host file lease.
    // Its raw sessions/ fallback has a fixed 15 s TTL, but is not needed under this policy.
    if (legacySessionKey && (this.#quiescing || filesOnly || hostPolicyFilesOnly)) {
      const legacy = this.mesh.get(legacySessionKey, read);
      if (legacy?.updatedBy.id === this.options.identity.id) {
        ops.push({ kind: "delete", key: legacy.key, ifVersion: legacy.version, onConflict: "skip" });
        changed = true;
      }
    } else if (root && legacySessionKey && root.cwd && root.sessionId) {
      const legacyValue = {
      id: root.id,
      livenessLeaseFiles: 1,
      livenessHostId: this.options.hostId,
      livenessStartedAt: this.#startedAt,
      livenessIncarnationToken: this.#incarnationToken,
      name: rootPeerName(root.name, root.label, root.sessionId),
      ...(root.label ? { label: root.label } : {}),
      kind: "peer",
      status: root.status === "running" ? "running" : "idle",
      runner: "pi",
      transport: "host",
      cwd: root.cwd,
      sessionId: root.sessionId,
      ...(root.model ? { model: root.model } : {}),
      ...(root.thinking ? { thinking: root.thinking } : {}),
      startedAt: root.startedAt,
      updatedAt: now,
      pendingMessages: root.pendingMessages === true,
      local: false,
      };
      const current = this.mesh.get(legacySessionKey, read)?.value;
      if (JSON.stringify({ ...(isObject(current) ? current : {}), updatedAt: undefined }) !==
        JSON.stringify({ ...legacyValue, updatedAt: undefined })) {
        legacyChanged = true;
        changed = true;
      }
      legacyPut = {
        kind: "put",
        key: legacySessionKey,
        value: (leaseAt: number) => ({ ...legacyValue, updatedAt: leaseAt }),
      };
    }

    const ownParticipant = (entry: MeshStateEntry | undefined): FabricParticipantRecord | undefined => {
      const participant = entry && participantFromEntry(entry);
      return participant && isLocal(participant, this.options.hostId) ? participant : undefined;
    };
    // Read-only preparation from the pinned snapshot: cloning every retained actor payload
    // on each idle heartbeat adds fleet-sized allocations and GC work (#2039). Mutations
    // below still use CAS and fresh locked checks; no shared entry is modified here.
    const stateEntries = this.mesh.listAllShared(PARTICIPANT_PREFIX, read);
    const stateByKey = new Map(stateEntries.map((entry) => [entry.key, entry]));
    const fileEntries = readParticipantFiles(this.mesh.root);
    const filesByKey = new Map(fileEntries.map((entry) => [entry.key, entry]));
    const existing = stateEntries.flatMap((entry) => {
      const participant = ownParticipant(entry);
      return participant ? [{ entry, participant }] : [];
    });
    const existingById = new Map(existing.map((item) => [item.participant.id, item]));
    const legacyRoots = new Map(
      this.mesh
        .listAllShared(LEGACY_SESSION_PREFIX, read)
        .flatMap((entry) => {
          const root = legacyRootFromEntry(entry, this.options.rootId, now, this.mesh.root);
          return root ? [[root.id, root] as const] : [];
        }),
    );
    const legacyActorOwners = new Map(
      this.mesh
        .listAllShared(LEGACY_ACTOR_PREFIX, read)
        .flatMap((entry) => {
          const actor = legacyActorFromEntry(entry, legacyRoots, now, this.options.terminalActor);
          return actor ? [[actor.id, actor.ownerIdentityId] as const] : [];
        }),
    );
    // A write needs a change beyond the timestamps that sources stamp on every read (Main's
    // info() sets updatedAt to now) and the activity counters, which ride along at most every
    // ACTIVITY_REFRESH_MS.
    const withoutTime = (value: unknown): string =>
      JSON.stringify({ ...(isObject(value) ? value : {}), ...QUIET_FIELDS });
    const activityOf = (current: FabricParticipantRecord, record: FabricParticipantRecord): boolean =>
      JSON.stringify({ ...current, updatedAt: undefined }) !== JSON.stringify({ ...record, updatedAt: undefined });
    let activity = false;
    // Records whose files to write: now (files only), or once the shared state committed them.
    const fileWrites: FabricParticipantRecord[] = [];
    const activityWrites: FabricParticipantRecord[] = [];
    const statePuts = new Map<string, FabricParticipantRecord>();
    for (const record of desired.values()) {
      const key = keyFor(PARTICIPANT_PREFIX, record.id);
      const current = existingById.get(record.id);
      const currentFile = ownParticipant(filesByKey.get(key));
      // A resident's fence, not its Main's lease, owns these actors. Current readers
      // derive liveness from that host lease, so a heartbeat compares content and does
      // not renew an unchanged row. The compatibility half-life applies only to the
      // host/session rows whose old readers actually use their timestamps (#13645).
      if (filesOnly
        ? !current && currentFile && JSON.stringify(currentFile) === JSON.stringify(record)
        : current && JSON.stringify(current.participant) === JSON.stringify(record)) continue;
      const stateEntry = stateByKey.get(key) ?? (filesOnly ? undefined : this.mesh.get(key, read));
      // Before the switch the shared state arbitrates ownership (its compare-and-swap), so it
      // decides occupancy: a stale file of this host never hides a newer owner there, and never
      // skips the live-owner check below (security pass S1 on #142).
      const occupied = filesOnly ? newer(filesByKey.get(key), stateEntry) : stateEntry ?? filesByKey.get(key);
      const occupiedParticipant = occupied && this.#terminalOrParticipant(occupied);
      const legacyOwner = record.kind === "actor" ? legacyActorOwners.get(record.id) : undefined;
      if (!occupiedParticipant && legacyOwner && legacyOwner !== this.options.identity.id) {
        continue;
      }
      // A live owner elsewhere keeps its key; a mirrored record never outranks a local one.
      if (
        occupiedParticipant &&
        occupiedParticipant.remoteHost === undefined &&
        occupiedParticipant.ownerHostId !== this.options.hostId &&
        this.#ownerLive(occupiedParticipant)
      ) continue;
      if (filesOnly) {
        // A file write does not take the mesh lock, so it does not count as a shared change.
        if (current || !currentFile || withoutTime(currentFile) !== withoutTime(record)) fileWrites.push(record);
        else if (activityOf(currentFile, record)) activityWrites.push(record);
        continue;
      }
      if (!current || withoutTime(current.participant) !== withoutTime(record)) changed = true;
      else if (activityOf(current.participant, record)) activity = true;
      // Liveness belongs to the host lease. A host renewal or another participant's real
      // change must not republish this record just because its source stamped updatedAt.
      else continue;
      statePuts.set(key, record);
      ops.push({
        kind: "put",
        key,
        value: record,
        ...(stateEntry ? { ifVersion: stateEntry.version } : {}),
        // Another host took the key meanwhile: leave it. Any other conflict fails the
        // whole heartbeat (nothing written) so the next one retries.
        onConflict: (latest) => {
          const latestParticipant = latest && this.#terminalOrParticipant(latest);
          return latestParticipant && !isLocal(latestParticipant, this.options.hostId) ? "skip" : "abort";
        },
      });
    }
    // An independently renewed, unchanged actor file may legitimately lead its
    // shared compatibility envelope. No copy (and therefore no key acquisition)
    // is needed; preparing this skip outside custody removes 50 idle key fences.
    const copies = filesOnly ? [] : existing.flatMap(({ entry }) => {
      const file = filesByKey.get(entry.key), participant = participantFromEntry(entry)!;
      return !statePuts.has(entry.key) && desired.has(participant.id) &&
        file?.version !== entry.version && !this.#renewalAhead(file, entry)
        ? [{ key: entry.key, version: entry.version, participant }] : [];
    });
    const actorCopies: Array<{ key: string; version: number }> = [];
    // smarty-dev#8526: renewals of actor files this host already owns run after the registry
    // fence, like actorCopies: the key lock plus a fresh registry-lineage check at the write
    // decision is the receipt adoption shares (it changes lineage holding this key's lock).
    const actorRenewals: Array<() => Promise<void>> = [];
    const renewOutsideFence = (record: FabricParticipantRecord): boolean => !!validPublication &&
      !!this.options.withPublicationFence && !!this.options.actorRenewalAllowed &&
      record.kind === "actor" && record.actorOwnershipToken !== undefined;
    const copyCommitted = async (key: string, version: number, participant: FabricParticipantRecord): Promise<void> => {
      if (validPublication && this.options.actorRenewalAllowed && participant.kind === "actor" && participant.actorOwnershipToken !== undefined) {
        actorCopies.push({ key, version });
      } else await this.#copyCommitted(key, version);
    };
    const commitPrepared = async (): Promise<number | false> => {
      if (validPublication && !validPublication()) throw new ParticipantPublicationChangedError();
      this.#localRecords.clear();
      for (const [id, record] of desired) this.#localRecords.set(id, record);
      // Keep the one host lease renewal outside the shared publication. Change-only
      // refreshes do not enter through refresh(), so renew them here as needed.
      if (this.options.enabled && !leaseRenewed && (!this.#quiescing || this.#reloadUntil !== undefined)) await this.#renewFileLease();
      leaseRenewed = false;
      for (const entry of fileEntries) {
        const participant = ownParticipant(entry);
        if (participant && !desired.has(participant.id)) {
          await this.#retryFile(() => removeParticipantFileIf(this.mesh, entry.key, (current) => ownParticipant(current) !== undefined,
            this.#fileLockOptions()));
        }
      }
      for (const { entry, participant } of existing) {
        // Desired migrations delete their shared copy only after a durable, verified file
        // publication below. A suppressed key failure must keep the last ownership record.
        if (desired.has(participant.id)) continue;
        changed = true;
        ops.push({ kind: "delete", key: entry.key, ifVersion: entry.version, onConflict: "skip" });
      }
      const deferredFileWrites: Array<() => Promise<void>> = [];
      const publishDeferredFiles = async (): Promise<void> => {
        for (const publish of deferredFileWrites) await publish();
      };
      if (filesOnly) {
        if (activityWrites.length > 0 && now - this.#recordsWrittenAt >= ACTIVITY_REFRESH_MS) {
          fileWrites.push(...activityWrites);
          this.#recordsWrittenAt = now;
        }
        // Migrations must publish durable files BEFORE deleting their shared copies;
        // first publication/takeover also keeps its files-first ownership ordering.
        // Already-owned file renewals have no shared copy to remove: defer them until
        // after the batch/confirmation so slow file I/O cannot consume a mesh gap.
        // Both phases still run under the same fresh actor-registry publication fence.
        for (const record of fileWrites) {
          // Under the key's lock, the file as it is now: absent, ours, or its owner gone by a fresh
          // (uncached) read of that owner's liveness. A live owner keeps it (review/astra F1 on #142).
          // The shared state's entry for the key counts too: a runtime that writes only the state may
          // hold it (review/astra round 4 on #142).
          const key = keyFor(PARTICIPANT_PREFIX, record.id);
          const migrating = existingById.get(record.id)?.entry;
          const publish = async (afterFence = false): Promise<void> => {
            const published = await this.#retryFile(() => this.#writeFile(record, (current) => {
              if (afterFence && !this.options.actorRenewalAllowed!(record)) return false;
              const taken = (entry: MeshStateEntry | undefined): boolean => {
                if (!entry || ownParticipant(entry) !== undefined) return false;
                const holder = this.#terminalOrParticipant(entry);
                return holder !== undefined && holder.remoteHost === undefined && this.#ownerLive(holder);
              };
              return !taken(current) && !taken(this.mesh.get(key, { fresh: true }));
            }, migrating !== undefined));
            if (migrating && published === true) {
              changed = true;
              ops.push({ kind: "delete", key, ifVersion: migrating.version, onConflict: "skip" });
            }
          };
          if (migrating || !ownParticipant(filesByKey.get(key))) await publish();
          else if (renewOutsideFence(record)) actorRenewals.push(() => publish(true));
          else deferredFileWrites.push(() => publish());
        }
      }
      for (const copy of copies) await copyCommitted(copy.key, copy.version, copy.participant);
      if (activity && now - this.#recordsWrittenAt >= ACTIVITY_REFRESH_MS) changed = true;
      if (!full && !changed) {
        await publishDeferredFiles();
        return false;                                          // nothing shared to publish
      }
      // Liveness stays in the matching per-host file on every tick. Old directory readers
      // already use that host lease for native sessions, so the explicit policy never
      // renews a legacy session. Without the policy, retain the fixed 7.5 s fallback
      // threshold (10 s on the default 5 s heartbeat) for genuinely state-only readers.
      // Real record/ownership changes above bypass this idle-only decision.
      let renewHost = changed;
      let renewSession = changed || legacyChanged;
      if (!this.#quiescing || this.#reloadUntil !== undefined) {
        const decideRenewal = (leaseAt: number, snapshot?: object): { skip: boolean; host: boolean; session: boolean } => {
          const read = snapshot ? { snapshot } : {};
          const own = this.mesh.get(keyFor(HOST_PREFIX, this.options.hostId), read);
          const host = own && hostFromEntry(own);
          const hostValid = !!host &&
            host.remoteHost === undefined &&
            host.rootId === this.options.rootId &&
            JSON.stringify(host.identity) === JSON.stringify(this.options.identity) &&
            host.incarnationToken === this.#incarnationToken;
          if (!hostValid) return { skip: false, host: true, session: false };
          if (!this.#legacyRenewalsRequired(leaseAt, snapshot)) return { skip: true, host: false, session: false };
          const legacy = root && legacySessionKey ? this.mesh.get(legacySessionKey, read) : undefined;
          // The host retains its configured policy cadence (10 min under hostLeases: files).
          const hostDue = hostPolicyFilesOnly
            ? leaseAt - host.updatedAt >= STATE_LEASE_RENEW_MS
            : host.expiresAt - leaseAt <= this.#leaseMs / 2;
          const sessionDue = !hostPolicyFilesOnly && !!legacyPut &&
            (!legacy || legacy.updatedAt + PARTICIPANT_LEASE_MS - leaseAt <= PARTICIPANT_LEASE_MS / 2);
          return { skip: !hostDue && !sessionDue, host: hostDue, session: sessionDue };
        };
        if (!changed) {
          let decision = decideRenewal(Date.now(), snapshot);
          renewHost ||= decision.host;
          renewSession ||= decision.session;
          if (decision.skip) {
            // The file shows only that this host is alive. A committed heartbeat also certifies
            // that the shared state is writable (confirmedAt; peer-settle relies on it, #24).
            // Nothing is written here, so prefer lock-free evidence of that (smarty-dev#6477 L6);
            // without it, take the lock once without a write: a stalled mesh still stops confirmation.
            let acquiredAt = this.#confirmWithoutLock() ?? 0;
            if (acquiredAt === 0) await this.mesh.confirmWritable(at => { acquiredAt = at; touchConfirmWitness(this.mesh.root); });
            // Re-check after the lock: the threshold may have elapsed while confirming.
            decision = decideRenewal(Date.now());
            if (decision.skip) {
              await publishDeferredFiles();
              return acquiredAt;
            }
            renewHost ||= decision.host;
            renewSession ||= decision.session;
          }
        }
      }

      // A compatibility tick may be due for only the session or only the host. Keep each
      // record at its old cadence, but append both to this one locked batch when both are due.
      if (renewSession && legacyPut) ops.push(legacyPut);

      // Stamp this host's lease at commit time, under the lock: a refresh that
      // outruns its own lease must not publish an already-expired lease, which
      // would make peers — and this host's own list() — read the records it just
      // wrote as stale.
      const priorHost = this.mesh.get(keyFor(HOST_PREFIX, this.options.hostId), { snapshot });
      if (renewHost) ops.push({
        kind: "put",
        key: keyFor(HOST_PREFIX, this.options.hostId),
        ...(priorHost ? { ifVersion: priorHost.version } : {}),
        onConflict: latest => {
          const owner = latest && hostFromEntry(latest);
          if (owner?.identity.id === this.options.identity.id && owner.incarnationToken === this.#incarnationToken) return "abort";
          throw new FabricHostLeaseSupersededError(this.options.hostId, this.options.identity.id, this.#startedAt);
        },
        value: (leaseAt: number): FabricHostRecord => ({
          format: 1,
          livenessLeaseFiles: 1,
          id: this.options.hostId,
          rootId: this.options.rootId,
          identity: this.options.identity,
          startedAt: this.#startedAt,
          incarnationToken: this.#incarnationToken,
          updatedAt: leaseAt,
          expiresAt: this.#reloadUntil ?? leaseAt + this.#leaseMs,
        }),
      });
      // Maintenance is independent of publication: select before custody, then retain exact
      // row-version CAS and the fresh lease condition for each bounded candidate at commit.
      ops.push(...compactExpiredHostRecords({ listAll: prefix => this.mesh.listAllShared(prefix, read) },
        this.mesh.root, this.options.hostId));
      let committedAt = 0;
      const results = await this.#withLeaseFence(() => this.mesh.writeBatch({ identity: this.options.identity, ops,
        lockTiming: { operation: changed || legacyChanged ? "directory-publish" : "directory-renew" },
        beforeCommit: () => {
          assertHostLeaseOwner(this.mesh.root, this.#leaseFenceOwner());
          this.#assertRootPublishable(statePuts.get(keyFor(PARTICIPANT_PREFIX, this.options.rootId)));
        },
        // In-process bookkeeping only (no view, no file): it needs no state custody, so it runs as the
        // commit hook, after COMMIT on every backend. As afterCommit it cost SQLite a second
        // BEGIN IMMEDIATE on every heartbeat (smarty-dev#6477).
        commitOutbox: () => {
          if (this.#keptReloadLease) {
            // Retained reload authority is checked under the SAME gate as this COMMIT.
            // Rotate only after success, before any post-commit file copy or gate release.
            commitHostLeaseUnderCustody(this.mesh.root, this.#fileLease(Date.now()),
              { claim: true, expected: this.#leasePredecessor });
            this.#leaseClaimed = true; this.#keptReloadLease = false;
          }
          committedAt = Date.now(); publication?.committed();
        } }));
      if (!filesOnly) this.#recordsWrittenAt = Date.now();
      await publishDeferredFiles();
      // Each record the shared state committed goes to its file too, for runtimes that read files.
      for (const result of results) {
        if (result.applied && statePuts.has(result.key)) await copyCommitted(result.key, result.version, statePuts.get(result.key)!);
      }
      return committedAt;
    };
    // smarty-dev#8526: the actors this round can write, so the fence locks only their registries.
    // An exact empty set means no actor effects; undefined keeps every registry, as before,
    // when the round also writes a root/legacy record, removes an own record, or carries
    // a batch op not attributed to an actor id. Later additions in commitPrepared are
    // host/session-only; actor removals and migration/copy writes are covered here.
    const fencedActors = ((): ReadonlySet<string> | undefined => {
      const ids = new Set(publication?.actorIds ?? []);
      if (publication && publication.ops.length > 0 && publication.actorIds === undefined) return undefined;
      // Prepared ops are the batch, the statePuts and (only) a legacy session delete.
      if (ops.length > (publication?.ops.length ?? 0) + statePuts.size || legacyChanged || legacyPut !== undefined) return undefined;
      if (existing.some(({ participant }) => !desired.has(participant.id)) ||
        fileEntries.some(entry => { const own = ownParticipant(entry); return own !== undefined && !desired.has(own.id); })) return undefined;
      for (const record of [...statePuts.values(), ...fileWrites, ...activityWrites, ...copies.map(copy => copy.participant)]) {
        if (record.kind !== "actor") return undefined;
        ids.add(record.id);
      }
      return ids;
    })();
    // smarty-dev#8880: a publication retry waits for its admission here, after selection, so only the
    // registry fences stand between the free mesh window and the 50 ms try. The decision stays under the
    // fence: validPublication (registry generations captured before selection) and the commit's CAS
    // refuse anything the wait changed, and a changed preparation re-selects without a second wait.
    // ponytail: the selected source timestamps are at most one admission wait old; host lease expiry
    // is still computed at commit (leaseAt).
    const admit = validPublication && this.options.withPublicationFence ? this.#admitBeforeCommit : undefined;
    if (admit) {
      this.#admitBeforeCommit = undefined;
      let interrupt!: () => void;
      const interrupted = new Promise<void>(resolve => { interrupt = resolve; });
      const admitted = admit();
      this.#admitted = admitted;
      this.#admitting = { interrupt, admitted };
      try { await Promise.race([admitted, interrupted]); }
      finally { this.#admitting = undefined; }
      if (this.#closed) return false;
    }
    const committed = validPublication && this.options.withPublicationFence
      ? await this.options.withPublicationFence(commitPrepared, fencedActors) : await commitPrepared();
    // The shared receipt already committed. Copies recheck that exact receipt
    // AND current registry lineage while holding the actor key; adoption takes
    // the same key before changing lineage. New/changed actor files therefore
    // need no registry custody, keeping first publication of 50 actors bounded.
    for (const renew of actorRenewals) await renew();
    for (const copy of actorCopies) await this.#copyCommitted(copy.key, copy.version);
    return committed;
  }

  // A single-key fault does not abort other keys; a shared mesh timeout unwinds the round.
  // No decision/cleanup is replayed outside its lock: the next refresh rereads that key.
  async #retryFile<T>(operation: () => Promise<T>): Promise<T | undefined> {
    this.#fileWork += 1;
    try {
      if (!this.#quiescing || this.#reloadUntil !== undefined) await this.#renewFileLease();
      return await operation();
    } catch (error) {
      // A busy shared mesh is not a single-key fault. Abort this fenced round,
      // rather than multiplying acquisition attempts while retaining registries.
      if (isMeshWriteDeferred(error) || isMeshLockTimeout(error) || error instanceof FabricHostLeaseSupersededError) throw error;
      return undefined; /* retry this key on the next refresh */
    }
    finally {
      this.#fileWork -= 1;
      // Renew between keys too: already-resolved promises can monopolize microtasks,
      // and the dual-write copies run AFTER the shared host commit. This file-only
      // renewal does not advance confirmedAt or certify shared writability.
      if (!this.#quiescing || this.#reloadUntil !== undefined) await this.#renewFileLease();
    }
  }

  #renewalAhead(current: MeshStateEntry | undefined, committed: MeshStateEntry | undefined): boolean {
    if (!this.options.actorRenewalAllowed || !current || !committed || current.updatedAt <= committed.updatedAt) return false;
    const participant = participantFromEntry(committed), existing = participantFromEntry(current);
    return participant?.kind === "actor" && existing?.ownerHostId === participant.ownerHostId &&
      existing.ownerIdentityId === participant.ownerIdentityId && existing.actorOwnershipToken === participant.actorOwnershipToken &&
      JSON.stringify(existing) === JSON.stringify(participant);
  }

  // The copy is the committed state entry itself, with its own version and commit time, and only
  // while the state still holds exactly that write and it is this host's: a copy delayed past a
  // newer owner's state write is dropped and never looks newer than it (review/astra round 4 and
  // security pass S1 on #142). A failed copy is made again by a later refresh.
  async #copyCommitted(key: string, version: number): Promise<void> {
    await this.#retryFile(() => writeParticipantFileIf(this.mesh, key, current => {
      const committed = this.mesh.get(key, { fresh: true });
      const participant = committed && participantFromEntry(committed);
      // Registry adoption shares this same key receipt. A copy accepted under
      // an earlier registry fence cannot publish after its lineage moved, even
      // before the successor has advertised a participant file.
      if (participant?.kind === "actor" && this.options.actorRenewalAllowed &&
        !this.options.actorRenewalAllowed(participant)) return undefined;
      if (this.#renewalAhead(current, committed)) return undefined;
      if (participant) this.#assertRootPublishable(participant);
      return committed?.version === version && participant && isLocal(participant, this.options.hostId)
        ? committed : undefined;
    }, this.#fileLockOptions()));
  }

  /** smarty-dev#6477 L6: confirms an idle heartbeat without the mesh lock when another commit,
   * not yet behind any receipt of this directory, shows the shared state was writable; returns
   * that commit's time. Otherwise undefined, and the caller takes the lock (confirmWritable)
   * exactly as before. Only a host already admitted by a real lock receipt, with no failed
   * refresh since, may use it: admission, outage recovery and the first confirmation keep the
   * real acquisition, and so does an overdue chain (no confirmation for two heartbeats, as
   * canConsumeMesh/writeStalled count it): lock-free evidence only extends a fresh chain of
   * receipts. The evidence must be newer than the last confirmation and at most one heartbeat old, so a stalled mesh stops confirmation within one interval and falls back to
   * the lock wait (and its timeout/outage path). Like confirmWritable, the next view is
   * canonical: read it fresh now, after the evidence. That read is synchronous file I/O and can
   * itself be slow while the mesh stalls, so every condition is checked again against a clock
   * taken after it: evidence that aged past one heartbeat during the read (or a chain that went
   * overdue) no longer confirms, and the caller takes the lock (review round 2 on #592). */
  #confirmWithoutLock(): number | undefined {
    if (!this.#leaseConfirmed || this.#refreshError !== undefined || this.#closed) return undefined;
    const now = Date.now();
    if (now - this.#refreshedAt >= this.#heartbeatMs * 2) return undefined;
    const latest = latestCommitWitness(this.mesh.root);
    if (latest <= this.#commitWitnessSeen) return undefined;
    const evidence = Math.min(latest, now);
    if (evidence <= this.#refreshedAt || now - evidence > this.#heartbeatMs) return undefined;
    try { this.mesh.stateToken({ fresh: true }); } catch { return undefined; }
    const lease = readHostLeaseCurrent(this.mesh.root, this.options.hostId);
    return lease && sameHostLeaseOwner(lease, this.#leaseOwner()) && this.#witnessHolds(latest, evidence, Date.now()) ? evidence : undefined;
  }

  /** The lock-free confirmation conditions at `at`, a clock reading taken after the fresh read. */
  #witnessHolds(latest: number, evidence: number, at: number): boolean {
    return this.#leaseConfirmed && this.#refreshError === undefined && !this.#closed &&
      at - this.#refreshedAt < this.#heartbeatMs * 2 &&
      latest > this.#commitWitnessSeen && evidence > this.#refreshedAt && at - evidence <= this.#heartbeatMs;
  }

  /** smarty-dev#6729 gate 1: a listing pairs a root with its owner's shared host record, extended
   * only by a lease file of the same incarnation (hostLiveness). Until this incarnation's first
   * shared commit stamps its own host record, that record is still the predecessor's; replacing
   * the predecessor's live reload lease file with this incarnation's (new incarnation token) dropped the
   * reloading Main from every listing until the commit landed, 10-55 s under lock load. Keep it.
   * Its reloadUntil still bounds the listing if this release never commits.
   * The predecessor's own recorded expiry decides, not a relation to this release's #leaseMs: a
   * fixed 180 s reload lease never outlasts leaseMs >= 180 s (or heartbeatMs >= 90 s), which made
   * the guard dead under that config (#663 round 2). Before this incarnation's host record commits,
   * its own lease file pairs with no host record. Keep only a matching predecessor's reload
   * lease, never an ordinary heartbeat or a lease after our own host record has committed.
   * Reload is explicit in new leases; older leases extended the fixed Main session TTL.
   * Neither signal depends on this release's configurable host TTL. */
  #keepsPredecessorReloadLease(at: number): boolean {
    if (this.#leaseConfirmed || this.options.identity.kind !== "main" || this.options.hostId !== this.options.rootId) return false;
    const entry = this.mesh.get(keyFor(HOST_PREFIX, this.options.hostId), { fresh: true });
    const host = entry && hostFromEntry(entry);
    if (!host || host.remoteHost !== undefined || host.incarnationToken === this.#incarnationToken ||
      host.rootId !== this.options.rootId || host.identity.id !== this.options.identity.id) return false;
    const lease = readHostLeaseCurrent(this.mesh.root, this.options.hostId);
    return lease !== undefined && lease.incarnationToken !== undefined && lease.rootId === this.options.rootId &&
      lease.identityId === this.options.identity.id && lease.startedAt !== undefined &&
      (host.incarnationToken !== undefined ? lease.incarnationToken === host.incarnationToken : lease.startedAt === host.startedAt) &&
      (this.#leasePredecessor?.incarnationToken !== undefined ? sameHostLeaseOwner(lease, this.#leasePredecessor) :
        lease.incarnationToken === undefined && lease.startedAt === this.#leasePredecessor?.startedAt) && lease.expiresAt > at &&
      (lease.reloadUntil === lease.expiresAt ||
        // Older releases extended the fixed Main session TTL without an explicit reload marker.
        (lease.reloadUntil === undefined && lease.session?.expiresAt === lease.expiresAt &&
          lease.session.expiresAt !== lease.session.updatedAt + PARTICIPANT_LEASE_MS));
  }

  #assertLeaseActive(): void {
    if (this.#superseded) throw this.#superseded;
  }

  #markSuperseded(error: FabricHostLeaseSupersededError): void {
    this.#superseded = error;
    this.#leaseAbort.abort();
    this.#refreshError = error;
    this.#leaseConfirmed = false;
    this.#routingReadAt = 0;
    if (this.#timer) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#heartbeatDueAt = undefined;
    if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
    this.#refreshTimer = undefined;
    this.#refreshScheduled = false;
    this.#refreshAgain = false;
    this.#cancelPublicationRetry();
    this.#cancelControlAdmissionTimer();
  }

  #leaseOwner(): FabricHostLease {
    return { id: this.options.hostId, rootId: this.options.rootId, identityId: this.options.identity.id,
      incarnationToken: this.#incarnationToken, startedAt: this.#startedAt, updatedAt: 0, expiresAt: 0 };
  }

  #fileLease(leaseAt: number): FabricHostLease {
    const root = this.#localRecords.get(this.options.rootId);
    return {
      id: this.options.hostId,
      rootId: this.options.rootId,
      identityId: this.options.identity.id,
      incarnationToken: this.#incarnationToken,
      startedAt: this.#startedAt,
      // The census record's exact start time: identity is (host, pid, startedAt) (pi-fabric#638).
      writer: meshWriterLeaseRecord(this.mesh.lockProtocol, this.mesh.stateBackend, meshProcessStartedAt),
      ...(this.#reloadUntil !== undefined ? { reloadUntil: this.#reloadUntil } : {}),
      ...(this.options.identity.kind === "main" && root?.sessionId ? {
        session: {
          id: root.sessionId,
          startedAt: root.startedAt,
          updatedAt: leaseAt,
          expiresAt: this.#reloadUntil ?? leaseAt + PARTICIPANT_LEASE_MS,
        },
      } : {}),
      updatedAt: leaseAt,
      expiresAt: this.#reloadUntil ?? leaseAt + this.#leaseMs,
    };
  }

  #leaseFenceOwner(): FabricHostLease {
    return this.#keptReloadLease && !this.#leaseClaimed && this.#leasePredecessor?.incarnationToken
      ? this.#leasePredecessor : this.#leaseOwner();
  }

  #renewFileLease(): Promise<number> {
    if (this.#leaseRenewing) return this.#leaseRenewing;
    const renewing = this.#withLeaseCommit(() => this.#commitFileLease());
    this.#leaseRenewing = renewing.finally(() => { this.#leaseRenewing = undefined; });
    return this.#leaseRenewing;
  }

  async #commitFileLease(): Promise<number> {
    this.#assertLeaseActive();
    if (this.#closed) return Date.now();
    const leaseAt = Date.now();
    if (this.#keepsPredecessorReloadLease(leaseAt)) {
      this.#keptReloadLease = true;
      return leaseAt;
    }
    try { await renewHostLease(this.mesh, this.#fileLease(leaseAt),
      { claim: !this.#leaseClaimed, expected: this.#leasePredecessor, signal: this.#leaseAbort.signal,
        ...this.#leaseLockIncarnation() });
      this.#lastLeaseRenewedAt = leaseAt;
    } catch (error) {
      if (error instanceof FabricHostLeaseSupersededError) this.#markSuperseded(error);
      if (error instanceof HostLeaseLockBusyError) this.#prepareLeaseLock = true;
      throw error;
    }
    this.#leaseClaimed = true;
    this.#keptReloadLease = false;
    return leaseAt;
  }

  /**
   * The root-collision report's change signals now, or undefined when nothing it reads changed since the
   * last report: the bounded list() view's snapshot and participant files, the peer leases, and the
   * local records and root it compares (smarty-dev#7978). Staleness only grows with time, so an unchanged
   * view can report no new collision; a lease period still forces a full pass.
   */
  #rootCollisionSignals(root: FabricParticipantRecord, now: number): (PeerSignals & { local: string }) | undefined {
    const checkedAt = Date.now();
    if (typeof this.mesh.stateToken !== "function" || typeof this.mesh.listAllShared !== "function") {
      return { token: {}, entries: [], files: [], leases: new Map(), local: "", at: now, checkedAt };
    }
    // Exactly the reads list() makes for it (#parsed with the bounded, non-fresh read).
    const read = { fresh: false };
    const token = this.mesh.stateToken(read);
    const files = this.#participantFiles(read, this.options.listReadCacheMs);
    const leases = readHostLeases(this.mesh.root);
    const local = JSON.stringify([root.id, root.name, root.sessionId, ...[...this.#localRecords.values()].map(record =>
      [record.id, record.kind, record.rootId, record.name, record.sessionId, record.status, record.reloadUntil, record.remoteHost])]);
    const last = this.#rootCollisionPass;
    if (last && last.files === files && last.local === local &&
      samePeerLeases(last.leases, leases, this.options.hostId) && now >= last.at &&
      checkedAt >= last.checkedAt && checkedAt - last.checkedAt < this.#leaseMs &&
      this.#samePeerState(last, token, ROOT_COLLISION_PREFIXES)) return undefined;
    return { token, entries: this.#peerEntries(token, ROOT_COLLISION_PREFIXES), files, leases, local, at: now, checkedAt };
  }

  /**
   * No operator switch needed: a live older reader makes us dual-renew again.
   * smarty-dev#7978: one idle heartbeat asks this up to three times, and each answer was a full read of
   * every host, session and participant entry (JSON copies of the pinned snapshot, or for the unpinned
   * call a full SQLite scan). The answer is now read from the canonical snapshot without copies and
   * reused while its change signals are unchanged: a `true` until the live old reader that proved it
   * lapses (expiry needs no change event), a `false` for any later time (expiry only removes readers),
   * and either for at most one lease period.
   */
  #legacyRenewalsRequired(now: number, snapshot?: object): boolean {
    if (typeof this.mesh.stateToken !== "function" || typeof this.mesh.listAllShared !== "function") {
      const read = snapshot ? { snapshot } : {};
      return this.#legacyRenewalsScan(now, readHostLeases(this.mesh.root), (prefix) => this.mesh.listAll(prefix, read),
        this.#participantFiles(read)).required;
    }
    // Unpinned: the current canonical snapshot, the same exact read an unpinned listAll made.
    const token = snapshot ?? this.mesh.stateToken();
    const read = { snapshot: token };
    const files = this.#participantFiles(read);
    const leases = readHostLeases(this.mesh.root);
    const checkedAt = Date.now();
    const memo = this.#legacyRenewals;
    if (memo && memo.files === files && samePeerLeases(memo.leases, leases, this.options.hostId) &&
      checkedAt >= memo.checkedAt && checkedAt - memo.checkedAt < this.#leaseMs &&
      (memo.required ? now <= memo.until : now >= memo.at) &&
      this.#samePeerState(memo, token, LEGACY_RENEWAL_PREFIXES)) return memo.required;
    // Read-only: hostFromEntry/participantFromEntry/isLiveLegacyRootEntry only inspect the entries, and
    // #liveHosts and mergeParticipantEntries build new containers, so the shared entries need no copy.
    const result = this.#legacyRenewalsScan(now, leases, (prefix) => this.mesh.listAllShared(prefix, read), files);
    this.#legacyRenewals = { token, entries: this.#peerEntries(token, LEGACY_RENEWAL_PREFIXES), files, leases,
      at: now, checkedAt, ...result };
    return result.required;
  }

  /** The shared entries of `prefixes` in the snapshot `token`, uncopied (read-only). */
  #peerEntries(token: object, prefixes: readonly string[]): readonly (readonly Readonly<MeshStateEntry>[])[] {
    return prefixes.map((prefix) => this.mesh.listAllShared(prefix, { snapshot: token }));
  }

  /**
   * Whether the snapshot `token` holds the same participant-relevant entries as `signals` (Astra P2 on
   * fd31b1c1): the state token moves on ANY committed change (a fleet commits unrelated keys every
   * second), so it is only the fast path; a moved token re-selects these prefixes and compares the
   * entries by identity. Unchanged, the signals move to the newer token, so the next call is the fast
   * path again and the older snapshot is released.
   */
  #samePeerState(signals: PeerSignals, token: object, prefixes: readonly string[]): boolean {
    if (signals.token === token) return true;
    const entries = this.#peerEntries(token, prefixes);
    if (!sameEntrySelections(signals.entries, entries)) return false;
    signals.token = token;
    signals.entries = entries;
    return true;
  }

  /** The scan behind #legacyRenewalsRequired; `until` is when the old reader that answered true lapses. */
  #legacyRenewalsScan(now: number, leases: ReadonlyMap<string, FabricHostLease>,
    list: (prefix: string) => readonly MeshStateEntry[], files: readonly MeshStateEntry[],
  ): { required: boolean; until: number } {
    const hosts = this.#liveHosts(list(HOST_PREFIX), leases);
    for (const host of hosts.values()) {
      if (host.id !== this.options.hostId && host.remoteHost === undefined &&
        host.expiresAt >= now && host.livenessLeaseFiles !== 1) return { required: true, until: host.expiresAt };
    }
    for (const entry of list(LEGACY_SESSION_PREFIX)) {
      if (entry.updatedBy.id !== this.options.identity.id && isLiveLegacyRootEntry(entry, now, this.mesh.root) &&
        (!isObject(entry.value) || entry.value.livenessLeaseFiles !== 1)) {
        return { required: true, until: sessionLiveness(entry, this.mesh.root).expiresAt };
      }
    }
    for (const entry of mergeParticipantEntries(files, list(PARTICIPANT_PREFIX)).entries) {
      const peer = participantFromEntry(entry);
      const owner = peer && hosts.get(peer.ownerHostId);
      if (peer && peer.ownerHostId !== this.options.hostId && peer.remoteHost === undefined &&
        owner && owner.expiresAt >= now && owner.identity.id === peer.ownerIdentityId &&
        owner.rootId === peer.rootId && peer.livenessLeaseFiles !== 1) return { required: true, until: owner.expiresAt };
    }
    return { required: false, until: Number.POSITIVE_INFINITY };
  }

  // Whether a participant's owner host is live now, by a fresh read: its shared host record (not
  // the store's read cache) with its file lease, or, before its first shared record, its file lease
  // alone, which a host writes before any participant file (review/astra round 2 F1 on #142).
  #ownerLive(participant: FabricParticipantRecord, now = Date.now()): boolean {
    const lease = readHostLease(this.mesh.root, participant.ownerHostId);
    const entry = this.mesh.get(keyFor(HOST_PREFIX, participant.ownerHostId), { fresh: true });
    const host = entry ? hostFromEntry(entry) : undefined;
    if (host) {
      return host.identity.id === participant.ownerIdentityId &&
        hostLeaseExpiry(lease ? new Map([[host.id, lease]]) : new Map(), host) >= now;
    }
    return lease !== undefined && lease.identityId === participant.ownerIdentityId &&
      lease.rootId === participant.rootId && effectiveLiveness(undefined, lease).expiresAt >= now;
  }

  #withLeaseCommit<T>(operation: () => Promise<T>): Promise<T> {
    // Promise handoff, not lock retry: each local operation enters the physical
    // gate exactly once. Actor key writes and shared publication must not report
    // each other's still-releasing async custody as a foreign busy holder.
    const committing = this.#leaseCommitting.then(() => {
      this.#assertLeaseActive();
      return operation();
    });
    this.#leaseCommitting = committing.then(() => undefined, () => undefined);
    return committing;
  }

  #withLeaseFence<T>(operation: () => T | Promise<T>): Promise<T> {
    if (this.#isNats()) return this.#withLeaseCommit(async () => {
      const assertLive = (): void => {
        if (this.#closed || this.#transportLost || !this.#transportLease?.live()) {
          throw new TransportUnavailableError("NATS host lease is not live");
        }
      };
      assertLive();
      const result = await operation();
      assertLive();
      return result;
    });
    return this.#withLeaseCommit(async () => {
      this.#assertLeaseActive();
      try { return await withOwnedHostLease(this.mesh, this.#leaseFenceOwner(), operation,
        { ...this.#leaseLockIncarnation(), ownerAfter: () => this.#leaseFenceOwner() }); }
      catch (error) {
        if (error instanceof FabricHostLeaseSupersededError) this.#markSuperseded(error);
        if (isMeshLockTimeout(error)) this.#prepareLeaseLock = true;
        throw error;
      }
    });
  }

  /** The incarnation host-lease custody records for its holder (custody-lock.ts). A fenced directory passes the one it
   * prepared before any registry fence. An unfenced directory (the session runtime, fabric-runtime-state.ts) never waits on
   * a native read for its lease (a slow probe would lapse it), so it always passed undefined and its gate records had
   * none (pi-fabric#1562 review-fleet-lead P1). It now passes this process's own incarnation once any read has settled
   * (participant files, the mesh lock, custody), still without starting or awaiting one; otherwise none, as before. */
  #leaseLockIncarnation(): { ownIncarnation: string | undefined } {
    return { ownIncarnation: this.options.withPublicationFence ? this.#ownIncarnationValue : knownOwnProcessIncarnation() };
  }

  #fileLockOptions(): ParticipantFileLockOptions {
    return { withCommitFence: operation => this.#withLeaseFence(operation),
      ...(this.options.withPublicationFence
        ? { ownIncarnation: this.#ownIncarnationValue, registryFenced: true } : {}) };
  }

  // A files-only write, decided under the key's lock: stamped now, the time of that decision.
  /** Under the same key lock or state transaction as the write: a root Main never publishes its root
   * participant while an operator actor removal fences that root (smarty-dev#7817). */
  #assertRootPublishable(record: FabricParticipantRecord | undefined): true {
    if (record?.kind === "root" && record.id === this.options.rootId && mainPublicationFenced(this.mesh.root, record.id)) {
      throw new MainPublicationFencedError(record.id);
    }
    return true;
  }

  #writeFile(
    record: FabricParticipantRecord,
    allowed: (current: MeshStateEntry | undefined) => boolean,
    durable = false,
  ): Promise<boolean> {
    const key = keyFor(PARTICIPANT_PREFIX, record.id);
    return writeParticipantFileIf(this.mesh, key, (current) => this.#assertRootPublishable(record) && allowed(current) ? {
      key,
      value: record,
      version: (current?.version ?? 0) + 1,
      updatedAt: Date.now(),
      updatedBy: this.options.identity,
    } : undefined, { durable, ...this.#fileLockOptions() });
  }

  /**
   * Mint missing labels for this host's root participants. Labels persist in
   * the peer's own participant record, so every session computes the same
   * label and retired numbers are never reused even if a peer record dies.
   */
  async #ensurePeerLabels(desired: Map<string, FabricParticipantRecord>): Promise<void> {
    if (!this.options.enabled) return;
    for (const record of desired.values()) {
      if (record.kind !== "root" || record.id !== this.options.rootId || record.label) continue;
      const published = this.#localRecords.get(record.id)?.label ?? this.#claimedPeerLabel;
      if (published) {
        record.label = published;
        continue;
      }
      const existingEntry = this.#participantEntry(keyFor(PARTICIPANT_PREFIX, record.id));
      const existing = existingEntry ? this.#terminalOrParticipant(existingEntry) : undefined;
      // Only this mesh's own record keeps a label: a mirror at the key never names a local root.
      if (existing?.label && existing.remoteHost === undefined) {
        record.label = existing.label;
        continue;
      }
      const seq = await this.#claimPeerSeq();
      if (seq !== undefined) record.label = this.#claimedPeerLabel = `${peerLabelPrefix(record.cwd)}-${seq}`;
    }
  }

  /** CAS-claim the next project-wide peer sequence number. ifVersion 0 creates. */
  async #claimPeerSeq(): Promise<number | undefined> {
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const entry = this.mesh.get(PEER_SEQ_KEY);
      const current =
        entry && isObject(entry.value) &&
        typeof entry.value.next === "number" &&
        Number.isInteger(entry.value.next) &&
        entry.value.next >= 1
          ? entry.value.next
          : 0;
      try {
        await this.#withLeaseFence(() => this.mesh.put({
          key: PEER_SEQ_KEY,
          value: { format: 1, next: current + 1 },
          identity: this.options.identity,
          ifVersion: entry?.version ?? 0,
        }));
        return current + 1;
      } catch (error) {
        // Only lost CAS races retry; offline/auth failures propagate.
        if (!/compare-and-swap/.test(error instanceof Error ? error.message : String(error))) {
          throw error;
        }
      }
    }
    return undefined;
  }

  #participantFiles(read: MeshReadOptions, listReadCacheMs?: number): readonly MeshStateEntry[] {
    // The resident's legacy list TTL belongs only to this observation. Mutation/
    // migration preparation calls without it, and fresh ownership lists bypass it.
    const maxAgeMs = read.fresh ? 0 : read.background ? this.mesh.backgroundReadCacheMs
      : listReadCacheMs ?? this.mesh.readCacheMs;
    return readParticipantFiles(this.mesh.root, { maxAgeMs });
  }

  #participantsOf(entries: readonly MeshStateEntry[]): Pick<ParsedDirectory, "participants" | "terminalActors" | "malformed"> {
    const participants: FabricParticipantRecord[] = [];
    const terminalActors: MeshStateEntry[] = [];
    const malformed: MeshStateEntry[] = [];
    for (const entry of entries) {
      // Normal parsing keeps owner-writer equality. Terminal exceptions are checked
      // by the lazy trusted host callback on EVERY read, never in this parse cache.
      const participant = participantFromEntry(entry);
      if (participant) participants.push(participant);
      else if (isObject(entry.value) && entry.value.stopReason === "owner-root-gone") terminalActors.push(entry);
      else if (isObject(entry.value) && entry.value.remoteHost !== undefined) malformed.push(entry);
    }
    return { participants, terminalActors, malformed };
  }

  #participantEntry(key: string, read: MeshReadOptions = {}): MeshStateEntry | undefined {
    const file = readParticipantFile(this.mesh.root, key), state = this.mesh.get(key, read);
    // Only authenticated terminal state may displace a native owner-written file.
    const candidate = state && isObject(state.value) && state.value.stopReason === "owner-root-gone" &&
      !this.options.terminalActor?.(state) ? undefined : state;
    return newer(file, candidate);
  }

  #legacySessionKey(): string | undefined {
    if (this.options.identity.kind !== "main") return undefined;
    return `sessions/${this.options.identity.sessionId ?? this.options.identity.id}`;
  }
}
