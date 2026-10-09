import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { MeshBackgroundQueue, MeshBackgroundRetry } from "../core/atomic-write.js";
import { participantProject, ParticipantRoleGrant, repositoryOf } from "./project-identity.js";
import type { FabricMainAgentInfo } from "../main-agent.js";
import { assertMeshStateReadable, MeshStore, meshProcessStartedAt, type MeshBatchOperation, type MeshIdentity, type MeshStateEntry, type MeshReadOptions } from "../mesh/store.js";
import type {
  FabricHostRecord,
  FabricParticipantInfo,
  FabricParticipantKind,
  FabricParticipantListOptions,
  FabricParticipantRecord,
  FabricParticipantSource,
  FabricPeerInfo,
} from "./types.js";

import { reapDeadHostRecords } from "./host-reaper.js";
import { compactExpiredHostRecords } from "./host-record-compaction.js";
import { withStateFence } from "../mesh/commit-outbox.js";
import { effectiveLiveness } from "./liveness.js";
import { isLiveLegacyRootEntry, sessionLiveness, LEGACY_ROOT_LEASE_MS as PARTICIPANT_LEASE_MS } from "./legacy-root-liveness.js";
import {
  fileLeasesOnly,
  hostLeaseExpiry,
  hostLiveness,
  LIVENESS_POLICY_KEY,
  readHostLease,
  readHostLeaseCurrent,
  readHostLeaseSnapshot,
  participantLeaseGraceMs,
  waitForHostLeaseRenewal,
  type RoutingLeaseWaitOptions,
  readHostLeases,
  removeHostLease,
  STATE_LEASE_RENEW_MS,
  writeHostLease,
  meshWriterLeaseRecord,
} from "./host-leases.js";
import { peerLabelPrefix } from "./peer-settle.js";
import { rootParticipantName } from "./participant-name.js";
import { ownProcessIncarnation } from "../core/atomic-write.js";
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
/** Project-scoped monotonic counter backing Linear-style peer labels. Never shrinks. */
const PEER_SEQ_KEY = "topology/peer-seq";
const HOST_PREFIX = "topology/hosts/";
// Root-owned clean-close receipts survive record cleanup. Absence alone (including a
// lease-based reaper's cleanup) is not positive evidence that a lineage ended.
const LINEAGE_CLOSURE_PREFIX = "topology/lineage-closures/";
const LEGACY_SESSION_PREFIX = "sessions/";
const LEGACY_ACTOR_PREFIX = "actors/";
const PARTICIPANT_HEARTBEAT_MS = 5_000;
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
/**
 * A stopped actor refuses every message and advertises no routing capability, so a
 * lease-unaware router gains nothing from a fresh envelope; current readers take its
 * liveness from the owner's host lease. Its envelope is written when it changes (the
 * stop) and otherwise renewed only this rarely, far inside the 6 h dead-host window,
 * instead of once per heartbeat inside the locked shared write (smarty-dev#6729).
 */
export const STOPPED_ACTOR_RENEW_MS = 60 * 60 * 1_000;
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

const isMeshLockTimeout = (error: unknown): error is Error =>
  error instanceof Error &&
  ((error as Error & { code?: unknown }).code === "FABRIC_MESH_LOCK_TIMEOUT" ||
    error.message.startsWith("Timed out waiting for the Fabric mesh lock"));

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const participantKind = (value: unknown): FabricParticipantKind | undefined =>
  value === "root" || value === "agent" || value === "actor" ? value : undefined;

const transports = new Set([
  "host",
  "auto",
  "process",
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

const participantFromEntry = (entry: MeshStateEntry): FabricParticipantRecord | undefined => {
  if (!isObject(entry.value) || entry.value.format !== 1) return undefined;
  const value = entry.value as Partial<FabricParticipantRecord> & Record<string, unknown>;
  const kind = participantKind(value.kind);
  if (
    !kind ||
    (value.interactive !== undefined && typeof value.interactive !== "boolean") ||
    !remoteHostValid(value.remoteHost) ||
    // Optional fields that consumers read as strings (peer cards, labels, leader selection):
    // a malformed one drops this record alone, never the listing (smarty-dev#2045).
    !optionalStrings(value, ["sessionId", "cwd", "label", "role", "project", "projectRoot", "repository", "model", "thinking", "parentId", "actorOwnershipToken"]) ||
    // v1 of the bridge mirrors root presence only; remote agents and actors come in v2.
    (value.remoteHost !== undefined && kind !== "root") ||
    typeof value.id !== "string" ||
    entry.key !== keyFor(PARTICIPANT_PREFIX, value.id) ||
    typeof value.rootId !== "string" ||
    typeof value.ownerHostId !== "string" ||
    typeof value.ownerIdentityId !== "string" ||
    entry.updatedBy.id !== value.ownerIdentityId ||
    typeof value.name !== "string" ||
    typeof value.status !== "string" ||
    (value.runner !== "pi" && value.runner !== "claude" && value.runner !== "veda") ||
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
    typeof value.rootId !== "string" ||
    !isObject(value.identity) ||
    typeof value.identity.id !== "string" ||
    typeof value.identity.name !== "string" ||
    entry.updatedBy.id !== value.identity.id ||
    (value.identity.kind !== "main" &&
      value.identity.kind !== "agent" &&
      value.identity.kind !== "actor") ||
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
  (owner.remoteHost === participant.remoteHost && !(owner.remoteHost !== undefined && owner.id === localHostId));

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

const legacyActorFromEntry = (
  entry: MeshStateEntry,
  roots: Map<string, FabricParticipantInfo>,
): FabricParticipantInfo | undefined => {
  if (!isObject(entry.value)) return undefined;
  const value = entry.value;
  if (
    typeof value.id !== "string" ||
    typeof value.name !== "string" ||
    (value.runner !== "pi" && value.runner !== "claude" && value.runner !== "veda") ||
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
      ...(value.runner === "pi" ? (["fabric"] as const) : []),
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
  /** Resident owners keep actor envelope timestamps fresh for lease-unaware routing readers. */
  renewActorParticipants?: boolean;
  /** Existing actor file renewal only; validate the last known lineage against
   * an atomic registry snapshot at the per-key write decision. Never admits keys. */
  actorRenewalAllowed?: (record: FabricParticipantRecord) => boolean;
  /** Prepare outside custody; validate exact registry generations inside the
   * existing publication fence before any ownership/presence write. */
  preparePublicationFence?: () => () => boolean;
  /** Explicit legacy list observation TTL, without a floor. Authority must request fresh. */
  listReadCacheMs?: number;
  /** Registry -> mesh/key order for shared publication. Accepted actor file copies
   * may follow outside custody with exact token validation under their key lock. */
  withPublicationFence?: <T>(publish: () => Promise<T>) => Promise<T>;
  /** Wait for mesh admission WITHOUT registry custody, then retry under a fresh fence.
   * Resident hosts supply a bounded FIFO wait; no snapshot/commit receipt crosses it. */
  waitForPublicationRetry?: () => Promise<void>;
  /** Extra presence writes share this host's one fenced heartbeat acquisition. */
  publicationBatch?: (full: boolean) => { ops: MeshBatchOperation[]; committed: () => void };
  hostId: string;
  rootId: string;
  identity: MeshIdentity;
  onRootCollision?: (collision: FabricRootCollision) => void;
  selfOwnerHostId?: string;
  selfOwnerIdentityId?: string;
  heartbeatMs?: number;
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
  /**
   * Session lifecycle token (smarty-dev#5962). False once the owner's extension ctx retired
   * (reload, session replacement): background ticks skip quietly until the owner rebinds.
   * Explicit refresh()/quiesce() calls still publish from the owner's last live snapshot.
   */
  live?: () => boolean;
}

export type ParticipantSnapshotSource = () => FabricParticipantRecord[];

export class ParticipantDirectory implements FabricParticipantSource {
  readonly #backgroundRefresh = new MeshBackgroundRetry("participant heartbeat/change refresh");
  readonly #notifications = new MeshBackgroundQueue("participant refusal/reap");
  readonly #sources = new Set<ParticipantSnapshotSource>();
  readonly #startedAt = Date.now();
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
  readonly #reportedCollisions = new Set<string>();
  readonly #reportedRootCollisions = new Set<string>();
  #timer: NodeJS.Timeout | undefined;
  #closed = false;
  #refreshing: Promise<void> | undefined;
  #actorRenewing: Promise<void> | undefined;
  /** Last independent file renewal of each stopped actor (see STOPPED_ACTOR_RENEW_MS). */
  readonly #stoppedRenewedAt = new Map<string, number>();
  /** A successful sequence claim survives discarded preparations for this root. */
  #claimedPeerLabel: string | undefined;
  #refreshScheduled = false;
  #refreshAgain = false;
  #refreshTimer: ReturnType<typeof setTimeout> | undefined;
  #publicationRetryTimer: ReturnType<typeof setTimeout> | undefined;
  #publicationRetrying: Promise<void> | undefined;
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
  #deadHostSweepAt = Date.now();
  #presencePassAt = Date.now();
  #quiescing = false;
  #reloadUntil: number | undefined;
  #reloadPublished = false;
  /** When a committed write last carried this host's participant records. */
  #recordsWrittenAt = 0;

  constructor(
    readonly mesh: MeshStore,
    readonly options: ParticipantDirectoryOptions,
  ) {
    // Prepare the participant-file lock receipt before any publication fence. A cold
    // Darwin/Windows native identity read must never be awaited while registries are held.
    this.#ownIncarnation = options.withPublicationFence
      ? ownProcessIncarnation().then((value) => {
          this.#ownIncarnationValue = value;
          return value;
        }, () => undefined)
      : Promise.resolve(undefined);
    this.#heartbeatMs = Math.max(100, options.heartbeatMs ?? PARTICIPANT_HEARTBEAT_MS);
    this.#leaseMs = Math.max(this.#heartbeatMs * 2, options.leaseMs ?? PARTICIPANT_LEASE_MS);
  }

  /** A retired owner lifecycle skips background work; a throwing token is retired too. */
  #live(): boolean {
    try { return this.options.live?.() ?? true; } catch { return false; }
  }

  registerSource(source: ParticipantSnapshotSource): () => void {
    this.#sources.add(source);
    if (this.#timer) this.scheduleRefresh();
    return () => {
      this.#sources.delete(source);
      if (this.#timer) this.scheduleRefresh();
    };
  }

  async start(): Promise<void> {
    if (this.#timer) return;
    // Restarting this directory is activation too, even if its last local view held a root.
    if (this.#closed) this.#localRecords.delete(this.options.rootId);
    this.#closed = false;
    this.#refreshError = undefined;
    this.#publicationRetryDelay = 750;
    this.#routingReadAt = 0;
    this.#leaseConfirmed = false;
    this.#refreshedAt = Date.now();
    if (this.options.enabled) {
      // Start before the initial publish: its per-key work can contend too. The
      // timer also retries a failed initial publish so the host can join later.
      this.#timer = setInterval(() => {
        if (this.#closed || !this.#live()) return;
        void this.#renewActors();
        // The retry runner coalesces shared publication, not independent liveness.
        // Renew through per-key waits even when run() skips an in-flight refresh;
        // shared-lock-only waits still lapse and confirmation still needs its lock.
        try {
          if (this.#refreshing && this.#fileWork > 0 && !this.#quiescing) this.#renewFileLease();
        } catch (error) {
          this.#refreshError = error;
          this.#routingReadAt = 0;
          this.#backgroundRefresh.failure(error);
          return;
        }
        if (!this.#publicationRetryTimer && !this.#publicationRetrying) {
          void this.#backgroundRefresh.run(() => this.refresh(), false);
        }
      }, this.#heartbeatMs);
      this.#timer.unref();
    }
    await this.refresh();
  }

  // Publishes changed local records soon: at once after a quiet second, otherwise at the
  // end of that second (one write for a burst of changes).
  scheduleRefresh(): void {
    if (this.#closed || isMeshLockTimeout(this.#refreshError)) return;
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
      void this.#backgroundRefresh.run(() => this.#runRefresh(false), false);
    };
    const wait = this.#changeRefreshAt + CHANGE_REFRESH_MIN_MS - Date.now();
    if (wait <= 0) {
      queueMicrotask(run);
      return;
    }
    this.#refreshTimer = setTimeout(run, wait);
    this.#refreshTimer.unref?.();
  }

  /** Actor mutations publish changes, not a full heartbeat or a lock-outage retry. */
  async refreshPresence(): Promise<void> {
    if (isMeshLockTimeout(this.#refreshError)) return;
    // A setter may finish saving after an in-flight heartbeat selected its source.
    // Wait for that round, then flush the pending revision under a fresh registry
    // fence. Coalescing with it could acknowledge presence it never committed.
    while (this.#refreshing) {
      try { await this.#refreshing; }
      catch (error) {
        if (isMeshLockTimeout(error)) return;
        throw error;
      }
    }
    if (isMeshLockTimeout(this.#refreshError)) return;
    // Do not call refresh(): mutation callers must not masquerade as automatic
    // heartbeats. The next full round still renews every actor envelope (#5128)
    // and its host lease in the same single batch.
    await this.#runRefresh(false);
  }

  /** A heartbeat: publishes the records and renews this host's lease. */
  async refresh(): Promise<void> {
    if (this.#closed) return;
    // Independent file renewal must not delay shared admission or let an
    // in-flight change failure disappear before a heartbeat joins its receipt.
    const renewing = this.#renewActors();
    const join = (publication: Promise<void>): Promise<void> =>
      Promise.all([renewing, publication]).then(() => undefined);
    if (this.#refreshing) {
      // A key waiter is still a live host. Do not turn this into a mesh-lock bypass:
      // confirmation remains gated on the shared write; shared-lock-only waits still lapse.
      if (this.#fileWork > 0 && this.options.enabled && !this.#quiescing) this.#renewFileLease();
      if (this.#refreshingFull) return join(this.#refreshing);
      // A change-only refresh may skip its write; renew the lease right after it.
      return join(this.#refreshing.catch(error => {
        if (isMeshLockTimeout(error)) throw error;
      }).then(() => this.refresh()));
    }
    return join(this.#runRefresh(true));
  }

  async #runRefresh(full: boolean): Promise<void> {
    if (this.#closed) return;
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
      if (this.#prepareFileLocks) {
        await prepareParticipantFileLocks(this.mesh);
        this.#prepareFileLocks = false;
        if (this.#closed) return false;
      }
      if (!this.options.preparePublicationFence) return this.options.withPublicationFence
        ? this.options.withPublicationFence(() => this.#refresh(full)) : this.#refresh(full);
      // A changed preparation drops every selected operation. Re-select only after
      // the old registry/mesh fences have unwound; no snapshot crosses a retry wait.
      for (let attempt = 0; ; attempt++) {
        try { return await this.#refresh(full); }
        catch (error) {
          if (!(error instanceof ParticipantPublicationChangedError) || attempt >= 4 || this.#closed) throw error;
          await new Promise(resolve => setTimeout(resolve, 0));
        }
      }
    });
    const settled = operation.then(() => undefined);
    settled.catch(() => undefined);                            // awaiters still see a failure
    this.#refreshing = settled;
    this.#refreshingFull = full;
    let lockTimedOut = false;
    try {
      const committed = await operation;
      if (!committed) return;
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
        try { this.#renewFileLease(); } catch { /* the next heartbeat renews it */ }
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
      if (this.#leaseConfirmed && !this.#closed && !this.#quiescing) {
        try { this.#renewFileLease(); } catch { /* Preserve the prior lease on file failure too. */ }
      }
      this.#refreshError = error;
      this.#routingReadAt = 0;
      if (error instanceof ParticipantFileLockBusyError) this.#prepareFileLocks = true;
      lockTimedOut = isMeshLockTimeout(error);
      if (lockTimedOut) {
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
        if (!lockTimedOut) this.scheduleRefresh();
      }
      if (lockTimedOut) this.#schedulePublicationRetry();
    }
  }

  #cancelPublicationRetry(): void {
    if (this.#publicationRetryTimer) clearTimeout(this.#publicationRetryTimer);
    this.#publicationRetryTimer = undefined;
  }

  #schedulePublicationRetry(): void {
    if (!this.options.waitForPublicationRetry || !this.#timer || this.#closed || this.#quiescing ||
      !isMeshLockTimeout(this.#refreshError) || this.#publicationRetryTimer || this.#publicationRetrying) return;
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
      await this.options.waitForPublicationRetry!();
      if (this.#closed || this.#quiescing || !isMeshLockTimeout(this.#refreshError)) return;
      // Release mesh BEFORE taking registries. Re-select every actor under fresh
      // registry custody, preserving #504/#531 and the registry -> mesh lock order.
      await this.refresh();
    } catch (error) {
      if (!isMeshLockTimeout(error)) this.#refreshError = error;
      this.#backgroundRefresh.failure(error);
    }
  }

  list(
    options: FabricParticipantListOptions = {},
    now = Date.now(),
  ): FabricParticipantInfo[] {
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
      const owner = hosts.get(participant.ownerHostId);
      if (!ownerMatches(participant, owner, this.options.hostId)) continue;
      if (this.#collides(participant, hosts)) {
        refused.push(participant.id);
        continue;
      }
      const stale =
        !owner ||
        owner.expiresAt < now ||
        (participant.status === "reloading" && (participant.reloadUntil ?? 0) < now) ||
        owner.identity.id !== participant.ownerIdentityId ||
        owner.rootId !== participant.rootId;
      if (stale && !options.includeStale) continue;
      const local = isLocal(participant, this.options.hostId);
      if (options.scope === "local" && !local) continue;
      if (options.scope === "lineage" && participant.rootId !== this.options.rootId) continue;
      if (options.kinds && !options.kinds.includes(participant.kind)) continue;
      byId.set(participant.id, { ...participant, local, stale });
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
          const actor = legacyActorFromEntry(entry, legacyRoots);
          if (actor && !byId.has(actor.id)) byId.set(actor.id, actor);
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
    return value;
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
      ...((): Pick<ParsedDirectory, "participants" | "malformed" | "shadowed"> => {
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
  #liveHosts(entries: Iterable<MeshStateEntry>): Map<string, FabricHostRecord> {
    const leases = readHostLeases(this.mesh.root);
    const hosts = new Map<string, FabricHostRecord>();
    for (const entry of entries) {
      const host = hostFromEntry(entry);
      if (host) hosts.set(host.id, { ...host, ...hostLiveness(leases, host) });
    }
    return hosts;
  }

  // A mirrored record never shadows a local target (smarty-dev#2004): it is refused when its id
  // or root is a participant this host publishes, this host's own root or identity, or any
  // unmirrored host's id, root or identity on this mesh.
  // ponytail: a mirror that overwrote another local host's child agent is not detectable here;
  // that host's next heartbeat takes its key back, and the bridge never overwrites unmirrored keys.
  #collides(participant: FabricParticipantRecord, hosts: ReadonlyMap<string, FabricHostRecord>, report = true): boolean {
    if (participant.remoteHost === undefined) return false;
    const ids = new Set([participant.id, participant.rootId]);
    let collides = [...ids].some((id) =>
      this.#localRecords.has(id) || id === this.options.rootId || id === this.options.identity.id);
    for (const host of hosts.values()) {
      if (collides) break;
      collides = host.remoteHost === undefined &&
        (ids.has(host.id) || ids.has(host.rootId) || ids.has(host.identity.id));
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
  ): { remoteHost: string; expiresAt: number } | undefined {
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
    const hosts = this.#liveHosts(this.mesh.listAll(HOST_PREFIX, read));
    const owner = hosts.get(ownerHostId);
    if (
      !owner ||
      !ownerMatches(participant, owner, this.options.hostId) ||
      owner.identity.id !== ownerIdentityId ||
      owner.rootId !== participant.rootId ||
      !Number.isFinite(owner.expiresAt) ||
      this.#collides(participant, hosts, false)
    ) return undefined;
    return { remoteHost: participant.remoteHost, expiresAt: owner.expiresAt };
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

  /** Capture owner authority once. Subsequent ACK checks only stat/read that owner's file. */
  captureControlOwnerLease(ownerHostId: string, ownerIdentityId: string, targetId: string): (() => number) | undefined {
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
    const record = this.#routingOwner(id);
    if (!record) return false;
    const now = (this.options.routingLease?.now ?? Date.now)();
    const expiry = this.captureControlOwnerLease(record.owner.id, record.owner.identity.id, id)?.();
    return expiry !== undefined && now - expiry <= participantLeaseGraceMs(this.options.routingLease?.graceMs);
  }

  /** Recovery is resolution-only: no mesh write, ownership transfer, or delivery retry here. */
  async resolveRoutingLease(id: string): Promise<boolean> {
    const record = this.#routingOwner(id);
    if (!record) return true; // Unknown/mirrored ids retain the ordinary resolver and bridge rules.
    const { participant, owner } = record;
    const options = this.options.routingLease ?? {};
    const now = (options.now ?? Date.now)();
    if (this.#routeEnded(participant, record.read)) return false;
    const snapshot = readHostLeaseSnapshot(this.mesh.root, owner.id);
    const lease = snapshot?.lease;
    const matching = lease && lease.rootId === owner.rootId && lease.identityId === owner.identity.id &&
      (lease.startedAt === undefined || lease.startedAt === owner.startedAt);
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
      !["stopping", "reloading"].includes(refreshed.status) && currentOwner?.startedAt === owner.startedAt &&
      currentOwner.identity.id === owner.identity.id && currentOwner.rootId === owner.rootId && !this.#routeEnded(refreshed);
  }

  lastKnown(id: string, now = Date.now()): { participant: FabricParticipantInfo; lapsedMs: number } | undefined {
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
    return this.#localRecords.has(id === "main" ? this.options.rootId : id);
  }

  get(id: string, now = Date.now(), options: { fresh?: boolean } = {}): FabricParticipantInfo | undefined {
    const target = id === "main" ? this.options.rootId : id;
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
        const owner = hostEntry ? hostFromEntry(hostEntry) : undefined;
        const lease = owner ? readHostLease(this.mesh.root, owner.id) : undefined;
        if (
          owner &&
          ownerMatches(participant, owner, this.options.hostId) &&
          !(participant.status === "reloading" && (participant.reloadUntil ?? 0) < now) &&
          hostLeaseExpiry(lease ? new Map([[owner.id, lease]]) : new Map(), owner) >= now &&
          owner.identity.id === participant.ownerIdentityId &&
          owner.rootId === participant.rootId
        ) {
          return { ...participant, local: isLocal(participant, this.options.hostId), stale: false };
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

  /** Revalidate once under the state write lock, without waiting for a long heartbeat write.
   * Never promote this read to confirmedAt/canConsumeMesh: it renews no ownership lease.
   * A state operation, not exclusive(): the backend picks the lock (smarty-dev#6477 R3). */
  async refreshRoutingView(): Promise<void> {
    if (this.#closed) throw new Error("participant directory is closed");
    if (!this.options.enabled) return;
    this.#routingReadAt = 0;
    try {
      await withStateFence(this.mesh, this.options.identity, () => {
        // MeshStore reads are intentionally tolerant for dashboards. Routing absence is
        // stronger: a damaged canonical state must never be confirmed as an empty view.
        assertMeshStateReadable(this.mesh.root);
        this.list({ scope: "project", includeStale: true, fresh: true });
        this.#routingReadAt = Date.now();
        this.#routingError = undefined;
      }, 250);
    } catch (error) {
      this.#routingError = error;
      throw error;
    }
  }

  /** Resident consumers fail closed on failed/overdue renewal, not just peer visibility.
   * A file-only liveness write is not confirmation that the mesh is writable. */
  canConsumeMesh(now = Date.now()): boolean {
    const confirmed = !this.options.enabled || (!this.#closed && !this.#quiescing && this.#leaseConfirmed &&
      this.#refreshError === undefined && now - this.#refreshedAt < this.#heartbeatMs * 2);
    // A resumed/suspended process (or a forward wall-clock adjustment) must not
    // wait a full heartbeat interval before trying to confirm its lapsed lease.
    // This never grants admission: refresh still needs the real mesh lock.
    // A typed lock timeout already has a single host recovery lane. Idle consumers
    // call this gate as often as every 100 ms; they must not multiply attempts.
    if (!confirmed && !isMeshLockTimeout(this.#refreshError) && !this.#closed && !this.#quiescing && this.#timer) {
      void this.#backgroundRefresh.run(() => this.refresh(), false);
    }
    return confirmed;
  }

  /** When this host last committed its heartbeat: lapses before it happened on a working mesh. */
  confirmedAt(): number {
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

  self(now = Date.now(), read: MeshReadOptions = {}): FabricParticipantInfo {
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

  peers(now = Date.now(), options: FabricParticipantListOptions = {}): FabricPeerInfo[] {
    return this.list({ ...options, scope: "project", kinds: ["root"] }, now)
      .filter((participant) => participant.id !== this.options.rootId)
      .flatMap((participant) => {
        const peer = peerFromParticipant(participant);
        return peer ? [peer] : [];
      });
  }

  root(main: FabricMainAgentInfo, interactive = true, sessionName?: string, boundGrant?: { role: string | undefined }): FabricParticipantRecord {
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
      ...(main.sessionId ? { sessionId: main.sessionId } : {}),
      ...(main.model ? { model: main.model } : {}),
      ...(main.thinking ? { thinking: main.thinking } : {}),
      startedAt: main.startedAt ?? this.#startedAt,
      updatedAt: main.updatedAt,
      pendingMessages: main.pendingMessages,
      controlProtocol: "v1",
    };
  }

  // After a committed heartbeat only: a host that cannot write gains nothing from a sweep.
  #sweepDeadHosts(): void {
    const reap = this.options.reapDeadHosts;
    if (!this.options.enabled || reap === false || this.#closed || this.#quiescing) return;
    const now = Date.now();
    if (now - this.#deadHostSweepAt < (reap?.sweepMs ?? DEAD_HOST_SWEEP_MS)) return;
    this.#deadHostSweepAt = now;
    void this.#notifications.enqueue(() => reapDeadHostRecords(this.mesh, this.options.identity, {
      ownHostId: this.options.hostId,
      now,
      ...(reap?.deadAfterMs !== undefined ? { deadAfterMs: reap.deadAfterMs } : {}),
    }));
  }

  async quiesce(reason?: string): Promise<void> {
    if (this.#closed || this.#quiescing) return;
    this.#quiescing = true;
    this.#cancelPublicationRetry();
    await this.#publicationRetrying;
    if (reason === "reload" && this.options.identity.kind === "main" && this.options.hostId === this.options.rootId) {
      this.#reloadUntil = Date.now() + MAIN_RELOAD_LEASE_MS;
    }
    try {
      await this.#actorRenewing;
      await this.#refreshing?.catch(() => undefined);
      await this.refresh();
    } catch (error) {
      // A reload's shared write can time out under lock load (smarty-dev#6729). The reload
      // goes on regardless (shutdown swallows this), so keep the Main addressable: renew its
      // own lease file, which needs no mesh lock, and let close() keep the root and lease.
      if (this.#reloadUntil !== undefined && this.options.enabled) {
        try { this.#renewFileLease(); } catch { /* close() still keeps the last lease */ }
        this.#reloadPublished = true;
      }
      throw error;
    }
    this.#reloadPublished = this.#reloadUntil !== undefined;
  }

  /** Invalidate death proof under the mesh custody lock BEFORE activation or file publication. */
  async resumeLineage(): Promise<void> {
    if (!this.options.enabled || this.options.hostId !== this.options.rootId ||
      this.options.identity.id !== this.options.rootId || this.options.identity.kind !== "main") return;
    const closure = this.mesh.get(keyFor(LINEAGE_CLOSURE_PREFIX, this.options.rootId), { fresh: true });
    if (closure) await this.mesh.delete({ key: closure.key, ifVersion: closure.version });
  }

  /** Explicit terminal session operation. Disposing/replacing a runtime is NOT lineage closure. */
  async closeLineage(): Promise<void> {
    await this.close();
    if (!this.options.enabled || this.#reloadPublished) return;
    // Only the creating Main may certify its terminal close, never a resident/child host.
    if (this.options.hostId === this.options.rootId && this.options.identity.id === this.options.rootId &&
      this.options.identity.kind === "main" && this.#localRecords.get(this.options.rootId)?.kind === "root") {
      await this.mesh.put({
        key: keyFor(LINEAGE_CLOSURE_PREFIX, this.options.rootId), identity: this.options.identity,
        value: { format: 1, rootId: this.options.rootId, ownerHostId: this.options.hostId,
          ownerIdentityId: this.options.identity.id, closedAt: Date.now() },
      }).catch(() => undefined);
    }
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#cancelPublicationRetry();
    await this.#publicationRetrying;
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = undefined;
    if (this.#refreshTimer) clearTimeout(this.#refreshTimer);
    this.#refreshTimer = undefined;
    this.#refreshScheduled = false;
    await this.#actorRenewing;
    await this.#refreshing?.catch(() => undefined);
    await this.#notifications.close();
    if (!this.options.enabled) return;
    const own = (entry: MeshStateEntry): boolean => {
      const participant = participantFromEntry(entry);
      return participant !== undefined && isLocal(participant, this.options.hostId) &&
        !(this.#reloadPublished && participant.kind === "root" && participant.id === this.options.rootId);
    };
    await Promise.allSettled(readParticipantFiles(this.mesh.root, { maxAgeMs: 0 }).filter(own)
      .map((entry) => removeParticipantFileIf(this.mesh, entry.key, own, this.#fileLockOptions())));
    const owned = this.mesh.listAll(PARTICIPANT_PREFIX).filter(own);
    await Promise.allSettled(owned.map((entry) => this.mesh.delete({ key: entry.key, ifVersion: entry.version })));
    const legacySessionKey = this.#legacySessionKey();
    if (legacySessionKey) {
      const legacy = this.mesh.get(legacySessionKey);
      if (legacy?.updatedBy.id === this.options.identity.id) {
        await this.mesh.delete({ key: legacy.key, ifVersion: legacy.version }).catch(() => undefined);
      }
    }
    // A reload leaves only its root and fixed host lease; the next session_start replaces both.
    if (this.#reloadPublished) return;
    removeHostLease(this.mesh.root, this.options.hostId);
    const hostEntry = this.mesh.get(keyFor(HOST_PREFIX, this.options.hostId));
    if (hostEntry && hostFromEntry(hostEntry)?.remoteHost === undefined) {
      await this.mesh.delete({ key: hostEntry.key, ifVersion: hostEntry.version }).catch(() => undefined);
    }
  }

  // Return the actual shared commit/acquisition time, not the completion time
  // of fallible post-commit file copies. An unchanged change-only refresh proves nothing.
  async #refresh(full: boolean): Promise<number | false> {
    // Claim before capturing a publication generation, but reselect ALL sources
    // afterwards: neither a label's mesh write nor an awaited claim may leave
    // actor ownership observations selected against an older generation.
    if (this.options.enabled && !this.#claimedPeerLabel && !this.#localRecords.get(this.options.rootId)?.label) {
      for (const source of this.#sources) {
        const root = source().find(record => record.kind === "root" && record.id === this.options.rootId);
        if (root) { await this.#ensurePeerLabels(new Map([[root.id, { ...root }]])); break; }
      }
    }
    const validPublication = this.options.preparePublicationFence?.();
    const now = Date.now();
    const desired = new Map<string, FabricParticipantRecord>();
    for (const source of this.#sources) {
      for (const candidate of source()) {
        const { task: _task, text: _text, error: _error, ...operational } = candidate as
          FabricParticipantRecord & { task?: unknown; text?: unknown; error?: unknown };
        const record: FabricParticipantRecord = {
          ...operational,
          format: 1,
          ownerHostId: this.options.hostId,
          ownerIdentityId: this.options.identity.id,
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
      this.#reportRootCollisions(candidateRoot);
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
          const actor = legacyActorFromEntry(entry, legacyRoots);
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
      // A resident's fence, not its Main's lease, owns these actors. External routing
      // readers still use participant envelope freshness; retain actor activity time
      // in the value, and do not turn change-only refreshes into heartbeats (#5128).
      // Shared compatibility readers still need their heartbeat envelopes;
      // files-only actors use the independent lane instead of duplicate writes.
      // A stopped actor is renewed only when its envelope is old (STOPPED_ACTOR_RENEW_MS);
      // its stop and any later change still publish through the change check below.
      const renewedAt = (filesOnly ? filesByKey.get(key) : current?.entry)?.updatedAt;
      const renewActor = full && this.options.renewActorParticipants === true &&
        (!filesOnly || !this.options.actorRenewalAllowed) && !this.#quiescing &&
        record.kind === "actor" && record.rootId === this.options.rootId &&
        (record.status !== "stopped" || renewedAt === undefined || now - renewedAt >= STOPPED_ACTOR_RENEW_MS);
      if (!renewActor && (filesOnly
        ? !current && currentFile && JSON.stringify(currentFile) === JSON.stringify(record)
        : current && JSON.stringify(current.participant) === JSON.stringify(record))) continue;
      const stateEntry = stateByKey.get(key) ?? (filesOnly ? undefined : this.mesh.get(key, read));
      // Before the switch the shared state arbitrates ownership (its compare-and-swap), so it
      // decides occupancy: a stale file of this host never hides a newer owner there, and never
      // skips the live-owner check below (security pass S1 on #142).
      const occupied = filesOnly ? newer(filesByKey.get(key), stateEntry) : stateEntry ?? filesByKey.get(key);
      const occupiedParticipant = occupied && participantFromEntry(occupied);
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
        if (renewActor || current || !currentFile || withoutTime(currentFile) !== withoutTime(record)) fileWrites.push(record);
        else if (activityOf(currentFile, record)) activityWrites.push(record);
        continue;
      }
      if (renewActor || !current || withoutTime(current.participant) !== withoutTime(record)) changed = true;
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
          const latestParticipant = latest && participantFromEntry(latest);
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
    const copyCommitted = async (key: string, version: number, participant: FabricParticipantRecord): Promise<void> => {
      if (validPublication && this.options.actorRenewalAllowed && participant.kind === "actor" && participant.actorOwnershipToken !== undefined) {
        actorCopies.push({ key, version });
      } else await this.#copyCommitted(key, version);
    };
    const commitPrepared = async (): Promise<number | false> => {
      if (validPublication && !validPublication()) throw new ParticipantPublicationChangedError();
      this.#localRecords.clear();
      for (const [id, record] of desired) this.#localRecords.set(id, record);
      // Renew before ANY per-key cleanup/write/copy, including migration and retry copies.
      // Heartbeat calls keep renewing while #retryFile is waiting on a contended key.
      if (!this.#quiescing || this.#reloadUntil !== undefined) this.#renewFileLease();
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
          const publish = async (): Promise<void> => {
            const published = await this.#retryFile(() => this.#writeFile(record, (current) => {
              const taken = (entry: MeshStateEntry | undefined): boolean => {
                if (!entry || ownParticipant(entry) !== undefined) return false;
                const holder = participantFromEntry(entry);
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
          else deferredFileWrites.push(publish);
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
            host.startedAt === this.#startedAt;
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
            decision = decideRenewal(this.#renewFileLease());
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
      if (renewHost) ops.push({
        kind: "put",
        key: keyFor(HOST_PREFIX, this.options.hostId),
        value: (leaseAt: number): FabricHostRecord => ({
          format: 1,
          livenessLeaseFiles: 1,
          id: this.options.hostId,
          rootId: this.options.rootId,
          identity: this.options.identity,
          startedAt: this.#startedAt,
          updatedAt: leaseAt,
          expiresAt: this.#reloadUntil ?? leaseAt + this.#leaseMs,
        }),
      });
      let committedAt = 0;
      const results = await this.mesh.writeBatch({ identity: this.options.identity, ops,
        prepare: view => compactExpiredHostRecords(view, this.mesh.root, this.options.hostId),
        afterCommit: () => { committedAt = Date.now(); publication?.committed(); } });
      if (!filesOnly) this.#recordsWrittenAt = Date.now();
      await publishDeferredFiles();
      // Each record the shared state committed goes to its file too, for runtimes that read files.
      for (const result of results) {
        if (result.applied && statePuts.has(result.key)) await copyCommitted(result.key, result.version, statePuts.get(result.key)!);
      }
      return committedAt;
    };
    const committed = validPublication && this.options.withPublicationFence
      ? await this.options.withPublicationFence(commitPrepared) : await commitPrepared();
    // The shared receipt already committed. Copies recheck that exact receipt
    // AND current registry lineage while holding the actor key; adoption takes
    // the same key before changing lineage. New/changed actor files therefore
    // need no registry custody, keeping first publication of 50 actors bounded.
    for (const copy of actorCopies) await this.#copyCommitted(copy.key, copy.version);
    return committed;
  }

  // A single-key fault does not abort other keys; a shared mesh timeout unwinds the round.
  // No decision/cleanup is replayed outside its lock: the next refresh rereads that key.
  async #retryFile<T>(operation: () => Promise<T>): Promise<T | undefined> {
    this.#fileWork += 1;
    try {
      if (!this.#quiescing || this.#reloadUntil !== undefined) this.#renewFileLease();
      return await operation();
    } catch (error) {
      // A busy shared mesh is not a single-key fault. Abort this fenced round,
      // rather than multiplying acquisition attempts while retaining registries.
      if (isMeshLockTimeout(error)) throw error;
      return undefined; /* retry this key on the next refresh */
    }
    finally {
      this.#fileWork -= 1;
      // Renew between keys too: already-resolved promises can monopolize microtasks,
      // and the dual-write copies run AFTER the shared host commit. This file-only
      // renewal does not advance confirmedAt or certify shared writability.
      if (!this.#quiescing || this.#reloadUntil !== undefined) this.#renewFileLease();
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
      return committed?.version === version && participant && isLocal(participant, this.options.hostId)
        ? committed : undefined;
    }, this.#fileLockOptions()));
  }

  /** Independent liveness lane: use ONLY the last committed local actor set,
   * renew ONLY an existing matching ownership token, and recheck registry custody
   * at the write decision. No creation, takeover, cleanup, shared commit, or
   * confirmedAt advancement is permitted here. A registry waiter cannot stall it. */
  #renewActors(): Promise<void> {
    if (this.#actorRenewing) return this.#actorRenewing;
    if (!this.options.enabled || !this.options.renewActorParticipants || !this.options.actorRenewalAllowed ||
      !this.#leaseConfirmed || this.#closed || this.#quiescing) return Promise.resolve();
    const startedAt = Date.now();
    const owned = [...this.#localRecords.values()].filter(record => record.kind === "actor" &&
      record.rootId === this.options.rootId && record.actorOwnershipToken !== undefined);
    for (const id of this.#stoppedRenewedAt.keys()) {
      if (this.#localRecords.get(id)?.status !== "stopped") this.#stoppedRenewedAt.delete(id);
    }
    // Stopped actors: once per instance, then every STOPPED_ACTOR_RENEW_MS (smarty-dev#6729).
    const records = owned.filter(record => record.status !== "stopped" ||
      startedAt - (this.#stoppedRenewedAt.get(record.id) ?? -Infinity) >= STOPPED_ACTOR_RENEW_MS);
    const work = (async () => {
      this.#renewFileLease();
      for (const record of records) {
        if (this.#closed || this.#quiescing) return;
        if (record.status === "stopped") this.#stoppedRenewedAt.set(record.id, startedAt);
        const key = keyFor(PARTICIPANT_PREFIX, record.id);
        try {
          await this.mesh.withTryLock(() => writeParticipantFileIf(this.mesh, key, current => {
            const participant = current && participantFromEntry(current);
            if (!current || !participant || participant.remoteHost !== undefined ||
              participant.ownerHostId !== this.options.hostId || participant.ownerIdentityId !== this.options.identity.id ||
              participant.rootId !== record.rootId || participant.actorOwnershipToken !== record.actorOwnershipToken ||
              !this.options.actorRenewalAllowed!(record)) return undefined;
            return { ...current, version: current.version + 1, updatedAt: Date.now() };
          }, { ownIncarnation: this.#ownIncarnationValue, registryFenced: true }), 0);
        } catch (error) {
          if (error instanceof ParticipantFileLockBusyError) this.#prepareFileLocks = true;
          // One busy key never stops siblings. Native recovery, when needed,
          // happens in the ordinary preparation lane outside every registry.
        }
      }
    })();
    this.#actorRenewing = work.finally(() => { this.#actorRenewing = undefined; });
    // Timer callers are fire-and-forget but close/quiesce join this exact lane.
    this.#actorRenewing.catch(() => undefined);
    return this.#actorRenewing;
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
    return this.#witnessHolds(latest, evidence, Date.now()) ? evidence : undefined;
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
   * the predecessor's live reload lease file with this incarnation's (new startedAt) dropped the
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
    if (!host || host.remoteHost !== undefined || host.startedAt === this.#startedAt ||
      host.rootId !== this.options.rootId || host.identity.id !== this.options.identity.id) return false;
    const lease = readHostLeaseCurrent(this.mesh.root, this.options.hostId);
    return lease !== undefined && lease.rootId === this.options.rootId &&
      lease.identityId === this.options.identity.id && lease.startedAt !== undefined &&
      lease.startedAt === host.startedAt && lease.startedAt < this.#startedAt && lease.expiresAt > at &&
      (lease.reloadUntil === lease.expiresAt ||
        // Older releases extended the fixed Main session TTL without an explicit reload marker.
        (lease.reloadUntil === undefined && lease.session?.expiresAt === lease.expiresAt &&
          lease.session.expiresAt !== lease.session.updatedAt + PARTICIPANT_LEASE_MS));
  }

  #renewFileLease(): number {
    const leaseAt = Date.now();
    if (this.#keepsPredecessorReloadLease(leaseAt)) {
      this.#keptReloadLease = true;
      return leaseAt;
    }
    const root = this.#localRecords.get(this.options.rootId);
    writeHostLease(this.mesh.root, {
      id: this.options.hostId,
      rootId: this.options.rootId,
      identityId: this.options.identity.id,
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
    });
    this.#keptReloadLease = false;
    return leaseAt;
  }

  /** No operator switch needed: a live older reader makes us dual-renew again. */
  #legacyRenewalsRequired(now: number, snapshot?: object): boolean {
    const read = snapshot ? { snapshot } : {};
    const hosts = this.#liveHosts(this.mesh.listAll(HOST_PREFIX, read));
    for (const host of hosts.values()) {
      if (host.id !== this.options.hostId && host.remoteHost === undefined &&
        host.expiresAt >= now && host.livenessLeaseFiles !== 1) return true;
    }
    for (const entry of this.mesh.listAll(LEGACY_SESSION_PREFIX, read)) {
      if (entry.updatedBy.id !== this.options.identity.id && isLiveLegacyRootEntry(entry, now, this.mesh.root) &&
        (!isObject(entry.value) || entry.value.livenessLeaseFiles !== 1)) return true;
    }
    for (const entry of this.#participantEntries(read)) {
      const peer = participantFromEntry(entry);
      const owner = peer && hosts.get(peer.ownerHostId);
      if (peer && peer.ownerHostId !== this.options.hostId && peer.remoteHost === undefined &&
        owner && owner.expiresAt >= now && owner.identity.id === peer.ownerIdentityId &&
        owner.rootId === peer.rootId && peer.livenessLeaseFiles !== 1) return true;
    }
    return false;
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

  #fileLockOptions(): ParticipantFileLockOptions {
    return this.options.withPublicationFence
      ? { ownIncarnation: this.#ownIncarnationValue, registryFenced: true } : {};
  }

  // A files-only write, decided under the key's lock: stamped now, the time of that decision.
  #writeFile(
    record: FabricParticipantRecord,
    allowed: (current: MeshStateEntry | undefined) => boolean,
    durable = false,
  ): Promise<boolean> {
    const key = keyFor(PARTICIPANT_PREFIX, record.id);
    return writeParticipantFileIf(this.mesh, key, (current) => allowed(current) ? {
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
      const existing = existingEntry ? participantFromEntry(existingEntry) : undefined;
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
        await this.mesh.put({
          key: PEER_SEQ_KEY,
          value: { format: 1, next: current + 1 },
          identity: this.options.identity,
          ifVersion: entry?.version ?? 0,
        });
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

  // Participant entries from their files and the shared state, the later of each key's two.
  #participantEntries(read: { fresh?: boolean; snapshot?: object } = {}): MeshStateEntry[] {
    return this.#mergedEntries(read).entries;
  }

  #mergedEntries(read: { fresh?: boolean; snapshot?: object } = {}): ReturnType<typeof mergeParticipantEntries> {
    return mergeParticipantEntries(this.#participantFiles(read), this.mesh.listAll(PARTICIPANT_PREFIX, read));
  }

  #participantFiles(read: MeshReadOptions, listReadCacheMs?: number): readonly MeshStateEntry[] {
    // The resident's legacy list TTL belongs only to this observation. Mutation/
    // migration preparation calls without it, and fresh ownership lists bypass it.
    const maxAgeMs = read.fresh ? 0 : read.background ? this.mesh.backgroundReadCacheMs
      : listReadCacheMs ?? this.mesh.readCacheMs;
    return readParticipantFiles(this.mesh.root, { maxAgeMs });
  }

  #participantsOf(entries: readonly MeshStateEntry[]): Pick<ParsedDirectory, "participants" | "malformed"> {
    const participants: FabricParticipantRecord[] = [];
    const malformed: MeshStateEntry[] = [];
    for (const entry of entries) {
      const participant = participantFromEntry(entry);
      if (participant) participants.push(participant);
      else if (isObject(entry.value) && entry.value.remoteHost !== undefined) malformed.push(entry);
    }
    return { participants, malformed };
  }

  #participantEntry(key: string, read: MeshReadOptions = {}): MeshStateEntry | undefined {
    return newer(readParticipantFile(this.mesh.root, key), this.mesh.get(key, read));
  }

  #legacySessionKey(): string | undefined {
    if (this.options.identity.kind !== "main") return undefined;
    return `sessions/${this.options.identity.sessionId ?? this.options.identity.id}`;
  }
}
